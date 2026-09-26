# How Flink checkpoints TB-scale state every minute

marchilink snapshots by deep-copying all state on every checkpoint. That is
fine at megabyte scale. This note explains how Flink gets the same guarantee
at terabyte scale, on a 30–60 second interval, without stopping the stream.

## The problem

A naive checkpoint does three things synchronously: pause processing, copy all
state, write it to durable storage. At TB scale each step is fatal on its own:

- copying 1 TB of heap state takes minutes and needs a second terabyte of
  memory during the copy;
- writing 1 TB per minute means ~17 GB/s of sustained I/O, per job;
- pausing the stream for that long destroys latency and backpressures every
  source.

Yet production Flink jobs do exactly this. The trick is that checkpoint cost
is made proportional to **how much state changed since last time**, not **how
much state exists**.

## Enabler 1: immutable files (RocksDB state backend)

State lives in RocksDB, an embedded LSM-tree KV store — one instance per
subtask:

    write → memtable (in-memory, mutable)
          → flush → SST file (on-disk, immutable once written)
          → compaction (merges old SSTs into new ones, deletes the old)

Two properties do all the work:

1. **SST files are immutable.** An update appends a new record; it never edits
   an old file. So "the state at barrier N" is just a set of files that
   cannot change under you.
2. **All disk writes are sequential.** Flushes and compactions are bulk
   sequential I/O, which disks and object stores like.

## Enabler 2: split "pin" from "persist"

When a barrier reaches a subtask, the snapshot runs in two phases.

**Synchronous (milliseconds, processing paused):**

1. Force-flush the memtable, so all state is in SST files.
2. Take a native RocksDB snapshot: pin the current file list. Compaction may
   not delete pinned files until the snapshot is released. This is O(1) — no
   data is copied.

Processing resumes immediately. New writes go to a fresh memtable; the pinned
SSTs are frozen by construction.

**Asynchronous (seconds, processing running):**

3. A background thread uploads the pinned SST files to durable storage
   (S3/HDFS), skipping any file already uploaded by an earlier checkpoint.
4. The subtask acks with a handle: metadata plus file references. The
   checkpoint completes when every subtask's upload has finished.

Contrast with marchilink: the subtask deep-copies state synchronously (the
pause), then the coordinator writes JSON after the acks. Flink moves both the
copy and the write off the processing thread.

## Enabler 3: incremental checkpoints

SSTs are immutable and uniquely identified, so a checkpoint only needs files
created since the last one — new flushes and compaction outputs. Previously
uploaded files are referenced, not re-uploaded. This changes the cost
equation:

    full checkpoint:        I/O ∝ total state size          (TB per minute — impossible)
    incremental checkpoint: I/O ∝ state churn per interval  (GBs per minute — fine)

Example: 2 TB of state across 200 subtasks is 10 GB each. If 1% of state
changes per minute, each subtask uploads ~100 MB per checkpoint, in the
background, while records keep flowing.

The price is a **recovery chain**: restore fetches a baseline plus a chain of
deltas, and garbage-collecting old files needs reference counting across
checkpoints.

## Enabler 4: parallelism

There is no single 2 TB snapshot. State is sharded by key across subtasks;
each subtask snapshots its own slice independently and concurrently.
Aggregate checkpoint bandwidth scales with the cluster, not with one machine.

## What still hurts

| Problem | Cause | Mitigation |
|---|---|---|
| Write amplification | forced flush per checkpoint makes many small SSTs | larger memtables, rate-limit checkpoint interval |
| Compaction pressure | pinned files delay reclamation | RocksDB tuning, spacing checkpoints |
| Barrier alignment stalls | a backpressured channel delays the barrier, so the snapshot waits | **unaligned checkpoints**: the barrier overtakes queued records and the in-flight data is snapshotted too |
| Upload tail latency | the checkpoint completes only when the slowest upload finishes | more parallelism, faster storage, changelog backend |

The **changelog state backend** goes further: it continuously writes a durable
log of state changes, so the per-checkpoint materialization is tiny and
completion time is predictable.

## The heap variant

For state that fits in memory, the HashMapStateBackend plays the same game at
object granularity: when a snapshot starts, the first write to each state
entry copies that entry (copy-on-write); the snapshot thread serializes the
old versions asynchronously. Same principle: writers detour, the snapshot
reads a frozen view.

## The one-sentence version

> marchilink freezes state with a photocopier: deep-copy everything, pause
> while copying. Flink writes state onto immutable pages, so freezing is a
> bookmark — "don't delete these files" — and the photocopying (upload)
> happens in the background, only for pages written since last time.

## Further reading

- CONSISTENCY.md — what marchilink actually does, and what each choice costs
- Flink docs: *State Backends*, *Checkpointing*, *Unaligned Checkpoints*
- RocksDB wiki: *Snapshots*, *Compaction*
