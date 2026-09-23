# marchilink — Flink-inspired stream processor MVP

Educational stream-processing engine in Go. Same spirit as
[marchilogs](../marchilogs) and [marchiq](../marchiq): **one learning goal per
slice**, single process, HTTP-first observability, Docker runnable, tests that
prove the core loop.

**Not a Flink clone.** We borrow the **dataflow model**: event time, watermarks,
keyed state, and Chandy–Lamport checkpoints — not the JVM, TaskManagers, slot
scheduling, or the SQL/Table stack.

The narrative completes the trilogy: marchiq is the durable log (Kafka role),
marchilink is the stateful compute (Flink role). By the final slice, marchilink
consumes marchiq topics with exactly the semantics of Flink's Kafka connector:
**offsets committed only when a checkpoint completes.**

---

## Learning goal

After the MVP you can explain, with a running binary and on-disk files:

1. Why stream processing needs **event time + watermarks** — you cannot wait
   forever for stragglers, so progress is a *heuristic with a contract*.
2. How **keyBy + keyed state** makes stateful computation parallel, and why
   state (not compute) is the hard part of streaming.
3. How **Chandy–Lamport barrier alignment** produces a consistent snapshot of a
   running dataflow *without stopping the world* — the mechanism behind
   Flink's exactly-once state semantics.
4. Why a checkpoint alone is not end-to-end exactly-once: you also need
   **source replay** + an **idempotent or transactional sink**.
5. What **backpressure** is and why every push-based dataflow needs flow
   control (bounded channels = poor man's credit-based flow control).

---

## Flink concepts we keep (and what we drop)

| Flink idea | marchilink v0 | Deferred |
|---|---|---|
| DataStream API | fluent Go API: `Map/Filter/KeyBy/Window/AddSink` | SQL, Table API, DataSet |
| JobGraph | in-process DAG of operators | distributed TaskManagers, RPC |
| Operator chaining | same-goroutine function composition | slot sharing groups |
| keyBy | hash partition across N goroutines | key groups, rescaling |
| Watermarks | bounded out-of-orderness generator | idle-source detection, custom strategies |
| Windows | tumbling + sliding, event-time | session, global, custom triggers/evictors |
| Keyed state | `ValueState/ListState/MapState`, in-memory | RocksDB backend, async state access |
| Checkpoints | aligned barriers, synchronous snapshot to disk | unaligned checkpoints, incremental |
| Savepoints | named, manually triggered checkpoints | savepoint format compat across versions |
| Backpressure | bounded Go channels (blocking = signal) | credit-based network flow control |
| Exactly-once | checkpoint + source replay + 2PC/idempotent sink | transactional external systems |
| Restart | fixed-delay restart from latest checkpoint | failure-rate strategies, region failover |
| Parallelism | `GOMAXPROCS`-bounded goroutines | slots, TaskManager process model |

**Explicit non-goals (v0):**

- Distributed execution (no JobManager/TaskManager split, no network shuffle)
- High availability (no ZooKeeper/Kubernetes leader election)
- SQL / Table API / CEP / ML libraries
- RocksDB or any embedded LSM state backend
- Web dashboard (HTTP JSON endpoints are enough)
- Exactly-once into external transactional systems (we do it into marchiq,
  which we control)

---

## Core loop (the whole system in one picture)

```text
marchiq topic "events"                marchilink job (single process)
        |                                   |
        |  GET /fetch?group=flink&topic=... |
        |<----------------------------------|  Source (offsets in state)
        |   records + watermarks            |
        |---------------------------------->+--> Map/Filter (stateless)
        |                                   |--> KeyBy(key) --hash--> N parallel subtasks
        |                                   |       |
        |                                   |       v
        |                                   |   Window(tumbling 5s) + ValueState
        |                                   |       |  fires when watermark passes window end
        |                                   |       v
        |                                   |   Sink (stdout / file / marchiq topic)
        |                                   |
        |        === checkpoint barrier flows with the stream ===
        |                                   |  every operator snapshots state,
        |                                   |  source records in-flight offsets
        |  POST /commit (offsets)           |
        |<----------------------------------|  ONLY after checkpoint N completes
```

**The one-sentence thesis:** a stream processor is *a dataflow graph where time
and consistency are first-class citizens* — watermarks govern when results are
emitted; barriers govern how state survives failure.

---

## Execution model (job graph, not actors)

A job is a **DAG of operators**. Each operator has parallelism `p`; each
parallel instance is a goroutine wired to its inputs by **bounded channels**.

```text
Source(1) --> Map(2) --> KeyBy --> Window(4) --> Sink(1)
              ^ chained: source+map may run in one goroutine
```

- **Bounded channels are the backpressure mechanism.** A slow window operator
  fills the channel; the source blocks. No unbounded queues, no drops.
- **Chaining** fuses consecutive single-parallelism-preserving operators into
  one goroutine (Flink does exactly this to avoid serialization hops).
- **keyBy** hashes the key to one of the downstream subtasks: same key, same
  goroutine, same state instance. This is the entire scalability argument.

---

## Time model (the heart of Flink)

Every record carries an **event timestamp**. Sources (or a timestamp assigner)
attach it; watermarks flow as **control records in the same stream**.

```text
record{ts=12:00:01}  record{ts=12:00:04}  WM(12:00:00)  record{ts=11:59:58} <- late!
```

- **Watermark(t)** = "I promise no more records with ts <= t" (bounded
  out-of-orderness: `WM = max_ts_seen - bound`).
- An operator with several inputs takes the **min** of its input watermarks —
  the stream is only as timely as its slowest partition.
- Windows fire when `watermark >= window.end`. Late records are dropped (v0)
  or sent to a **side output** (later slice).
- Processing-time windows exist only as a teaching contrast; event time is
  the default.

---

## State + checkpoint storage (on-disk)

State is **keyed**: it lives inside an operator, scoped to the current key.
Checkpoints persist it with the same atomic-publish protocol as marchiq
(tmp → sync → rename → sync dir — durability habits transfer).

```text
{dataDir}/
  checkpoints/
    {jobId}/
      chk-000017/
        _metadata.json              # checkpoint id, timestamp, operator list, status
        source-0.state              # source: marchiq group offsets in flight
        window-2/
          keygroup-0.state          # keyed state shard (gob/JSON lines)
          keygroup-1.state
      chk-000018/
        ...
      LATEST                        # pointer file: last completed checkpoint
  savepoints/
    {name}/                         # same layout, manually triggered, never auto-deleted
```

**Checkpoint protocol (Chandy–Lamport, aligned):**

1. Coordinator (in-process) tells every source: inject **barrier N** after the
   current record; source snapshots its offsets *at the barrier position*.
2. Barrier flows downstream like a record. An operator with one input snapshots
   when the barrier passes. An operator with several inputs **aligns**: it
   blocks the fast channel until the barrier arrives on all inputs, then
   snapshots. (Blocking = the price of alignment; unaligned checkpoints are the
   optimization we defer.)
3. When the sink acks barrier N, the coordinator marks chk-N complete and only
   then lets the source **commit offsets to marchiq**.

Recovery: on restart, load the latest completed checkpoint, restore every
operator's state, rewind the marchiq group offsets to the snapshot, resume.
Records between checkpoint and crash are **replayed** — state was rolled back
too, so each record is applied exactly once.

---

## API

Jobs are **Go code** (like Flink jobs are Java code) — there is no job DSL in
v0. HTTP is for observability and control only.

```go
job := marchilink.NewJob("wordcount")
job.Source(marchiqSource("events", "flink-group")).
    Filter(nonEmpty).
    KeyBy(func(e Event) string { return e.Key }).
    Window(marchilink.Tumbling(5 * time.Second)).
    Sum("count").
    Sink(stdoutSink{})
job.Run() // blocks; checkpoints every -checkpointInterval
```

| Method | Path | Purpose |
|---|---|---|
| GET | `/healthz` | liveness |
| GET | `/jobs` | running job graph (operators, parallelism, edges) |
| GET | `/jobs/{id}/watermarks` | current watermark per operator |
| GET | `/jobs/{id}/state` | keyed state size per operator (debug) |
| GET | `/checkpoints` | checkpoint history (id, duration, size, status) |
| POST | `/savepoints/{name}` | trigger a named savepoint |
| GET | `/` | help text |

---

## MVP slices (build order)

Each slice = branch + tests + `docker compose` still works.

### Slice 0 — skeleton

- `go mod`, `cmd/marchilink/main.go`, `runtime/` + `api/` packages
- `Event{Key, Value, Timestamp}`, `Source`/`Sink` interfaces
- A generator source → map → stdout sink pipeline that runs

**Done when:** `go test ./...` green, Docker image builds, pipeline prints
transformed events.

### Slice 1 — job graph + keyBy + backpressure

- DAG builder from the fluent API; operator parallelism; bounded channels
- Chaining of consecutive compatible operators
- keyBy hash-routes records to subtasks

**Tests:** word count over a scripted source; same key always lands on the same
subtask; a blocked sink eventually blocks the source (backpressure visible as
channel-full, asserted with a probe).

### Slice 2 — event time + watermarks

- Timestamp assigner + bounded out-of-orderness watermark generator at sources
- Watermark propagation: per-input tracking, min across inputs
- Watermarks observable at `GET /jobs/{id}/watermarks`

**Tests:** out-of-order scripted stream → watermarks advance monotonically and
never overtake `max_ts - bound`; multi-input operator takes the min.

### Slice 3 — windows

- Tumbling + sliding **event-time** windows, per-key
- Fire when watermark passes window end; late records counted and dropped
- Window results emitted downstream as records

**Tests:** scripted out-of-order input produces exactly the expected windowed
sums; each window fires exactly once; late record is accounted as dropped.

### Slice 4 — keyed state

- `ValueState/ListState/MapState` interfaces, in-memory backend
- State scoped to (operator, key); accessible in window/operator functions
- `GET /jobs/{id}/state` shows per-operator key counts

**Tests:** running-count-per-key job survives a million keys without cross-key
bleed; dedup job filters already-seen ids.

### Slice 5 — checkpoints (the crown jewel)

- Barrier injection at sources; barrier alignment at multi-input operators
- Synchronous snapshot to `{dataDir}/checkpoints/...`, atomic publish, LATEST
  pointer
- Restart-from-checkpoint recovery with source rewind

**Tests:** running-count job, kill -9 mid-stream, restart from checkpoint →
final counts equal a no-crash run **exactly** (no double-apply, no loss).

### Slice 6 — marchiq source/sink + end-to-end exactly-once

- `marchiqSource`: group fetch (slice-4 API); offsets snapshotted in
  checkpoints; committed to marchiq **only on checkpoint completion**
- `marchiqSink`: produce results back to a topic
- Demo: `events` → windowed count → `counts` topic

**Tests:** crash between "state snapshotted" and "offsets committed" → after
restart, marchiq consumer of the output topic sees no duplicates; marchiq group
lag returns to 0.

### Slice 7 — polish (optional before "MVP done")

- Savepoints (named, manual) + resume-from-savepoint flag
- Allowed lateness + late-record side output
- Session windows
- `GET /debug/throughput` (records/s per operator, watermark lag)

---

## Consistency decisions (document early)

Write `runtime/CONSISTENCY.md` in slice 5:

| Choice | Behavior | Cost |
|---|---|---|
| Aligned barriers | simplest exactly-once state | fast inputs stall during alignment |
| Synchronous snapshots | simple, correct | checkpoint pauses the operator |
| Commit offsets after checkpoint | no duplicates in state | up to one interval of replay on crash |
| At-least-once sink (default) | may duplicate output on recovery | sink must dedup or tolerate |

Contrast with marchiq: the queue guarantees **durability of bytes**; the
processor guarantees **consistency of derived state**. Exactly-once end-to-end
= checkpointed state + source replay + idempotent/transactional sink, and each
piece is useless without the others.

---

## Observability (minimal)

| Signal | v0 |
|---|---|
| Watermark per operator | `GET /jobs/{id}/watermarks` |
| Checkpoint history | `GET /checkpoints` (id, duration, bytes, status) |
| Backpressure | channel occupancy per edge (debug endpoint) |
| Log | window fire, checkpoint start/complete, barrier alignment stalls |

---

## Project layout (target)

```text
marchilink/
  PLAN.md                 # this file
  go.mod
  Dockerfile
  docker-compose.yml      # marchiq + marchilink together
  cmd/
    marchilink/main.go    # runtime + HTTP observability
    demo/                 # wordcount / windowed demo job
  api/                    # fluent DataStream API (user-facing)
  runtime/
    graph.go              # DAG, chaining, channels
    operator.go           # operator lifecycle
    watermark.go
    window.go
    state.go              # keyed state backend
    checkpoint.go         # barriers, alignment, snapshots
    source_marchiq.go
    sink_marchiq.go
    CONSISTENCY.md
  runtime/*_test.go
```

Module path: `github.com/imarchuang/marchilink` (mirror marchiq). Go 1.22, zero
third-party dependencies.

---

## Demo script (graduation bar)

```bash
# terminal 1 — the queue
cd marchiq && docker compose up

# terminal 2 — the processor: 5s tumbling window count per key
cd marchilink && go run ./cmd/demo -job windowed-count \
  -source "localhost:9092/topics/events" -window 5s -checkpoint 10s

# terminal 3 — produce out-of-order events
./scripts/produce-events.sh localhost:9092 events 100 --out-of-order 0.2

# watch: windows fire as watermarks advance; checkpoints complete every 10s
curl localhost:9081/jobs/windowed-count/watermarks
curl localhost:9081/checkpoints

# kill -9 the processor mid-stream, restart it
# PASS: counts in the output topic have no duplicates and no gaps;
#       marchiq group offsets resume from the last completed checkpoint
```

---

## Relation to sibling projects

| Project | Role in the story | Difference |
|---|---|---|
| **marchilogs** | append-only log storage, query by time | many readers scan; no derived state |
| **marchiq** | durable partitioned queue | moves bytes; knows nothing about time semantics |
| **marchilink** | stateful stream compute | consumes the queue; watermarks + checkpoints |
| **marchimetrics** (planned) | numeric samples | could be a marchilink sink later |

marchilink is the right third system: marchiq taught *durability of the log*,
marchilink teaches *consistency of computation over the log*.

---

## Open decisions (defaults for v0)

| Question | Default | Revisit when |
|---|---|---|
| Job definition | Go code, no DSL | SQL/Table far later, if ever |
| Watermark strategy | bounded out-of-orderness, 5s bound | idle sources hurt demos |
| Checkpoint interval | 10s | alignment stalls dominate |
| State serialization | gob per keygroup file | format evolution / savepoint compat |
| Late records | drop + count | side output in slice 7 |
| Barrier alignment | aligned only | unaligned if stalls hurt |

---

## Success criteria ("MVP done")

- [ ] Fluent Go API: source → keyBy → event-time window → sink
- [ ] Watermarks advance correctly through out-of-order input
- [ ] Keyed state survives kill -9 via checkpoints; counts match a no-crash run
- [ ] marchiq source commits offsets only on checkpoint completion
- [ ] End-to-end demo: no duplicates in the output topic across a crash
- [ ] `go test ./...` + Docker demo script documented
- [ ] `runtime/CONSISTENCY.md` explains barriers and why checkpoint ≠ e2e exactly-once

---

## After MVP (not now)

1. Unaligned checkpoints + incremental (RocksDB) state backend
2. Key-group rescaling (change parallelism across a savepoint)
3. Two-phase-commit sink to an external system (file sink with pending commits)
4. Distributed runtime: split operators across processes with a real network
   shuffle — at which point you finally understand why Flink needs a JobManager

Start implementation at **slice 0** on branch `feat/skeleton`.
