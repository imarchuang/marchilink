# Consistency decisions

How marchilink keeps derived state consistent across failures, and what each
choice costs.

## The mechanism

Checkpoints follow Chandy-Lamport, aligned:

1. The coordinator asks the source to inject a **barrier N** after the current
   record. The barrier carries the source offset of the *next* record.
2. The barrier flows downstream like a record. Each subtask snapshots its state
   the moment the barrier passes — no waiting, because each subtask has a
   single input channel in this topology.
3. When every subtask has acked barrier N, the coordinator writes the
   checkpoint atomically (tmp → sync → rename → sync dir) and updates LATEST.

Recovery loads LATEST: every subtask's state is restored, and the source is
rewound to the barrier's offset. Records between the checkpoint and the crash
are replayed — but state was rolled back too, so each is applied exactly once.

## Choices and costs

| Choice | Behavior | Cost |
|---|---|---|
| Aligned barriers | simplest exactly-once state | fast inputs stall during alignment (not visible here: single-input subtasks) |
| Synchronous snapshots | simple, correct | subtask pauses while its state is serialized |
| Commit offsets after checkpoint | no duplicates in state | up to one interval of replay on crash |
| At-least-once sink (default) | may duplicate output on recovery | sink must dedup or tolerate |
| Full (non-incremental) snapshots | every checkpoint writes all state | I/O grows with state size |
| JSON serialization | human-readable, debuggable | larger and slower than gob/Protobuf |

## Why a checkpoint alone is not end-to-end exactly-once

A checkpoint guarantees the *processor's internal state* is applied exactly
once. The output topic is a separate system: if the job crashes after the sink
wrote a record but before the checkpoint completed, recovery replays and the
sink writes it again.

End-to-end exactly-once needs three pieces, each useless without the others:

1. **Source replay** — the input log can be rewound (marchiq offsets).
2. **Checkpointed state** — derived state rolls back to the same point.
3. **Idempotent or transactional sink** — replayed output does not duplicate.

marchilink v0 provides (1) and (2). For (3), the marchiq sink in slice 6
commits offsets only on checkpoint completion, which makes the *input* side
exactly-once; true transactional output is deferred.

## Contrast with marchiq

marchiq guarantees **durability of bytes**: once a record is in the log, it is
not lost. marchilink guarantees **consistency of derived state**: what you
computed from those bytes survives crashes. The queue knows nothing about
watermarks or windows; the processor knows nothing about replication. The
contract between them is the offset — checkpointed here, committed there only
after the checkpoint completes.
