# Ownership, publication, and shutdown

## SPSC slot protocol

A slot starts with `sequence = slot_index`. Producer position `p` may write when the slot sequence equals `p`. It moves the record into the slot and publishes `p + 1` with release ordering. Consumer position `c` may read when the sequence equals `c + 1` with acquire ordering. After moving the record out, it releases the slot with `sequence = c + capacity`.

The producer's acquire load of that reclamation sequence prevents reuse before the consumer finishes accessing the non-atomic string. The consumer's acquire load of publication makes the complete string move visible. Initial slot stores are relaxed because construction happens before thread creation. Producer and consumer positions have only one owner each and require no atomic accesses. Unsigned positions wrap modulo 2^64, and power-of-two capacity preserves slot indexing across wrap.

Positions and slots are aligned to 64 bytes to reduce false sharing on common CPUs. This is a platform assumption, not a universal cache-line guarantee. Padding increases memory use. The implementation does not cache opposite-side indices or use architecture-specific fences.

## Close protocol

The sole SPSC producer publishes its final record before release-storing `closed`. The consumer first tries to pop. If empty and an acquire load sees `closed`, it **tries to pop again** before declaring the queue drained. This matters: the first empty check can race with the final publication. The acquire of close makes that publication happen-before the second check.

The locked queue uses the same mutex for push, close and pop. A push either precedes close and is accepted, or follows it and is rejected. Closing wakes waiting consumers. Consumers continue draining records after closure.

The engine owns its writer thread. Sink exceptions are captured on that thread and published through an atomic failure flag; exception storage is read only after join. Failed sinks are not retried because side effects may already have happened. Queue entries are destroyed with the engine. Blocking producers check failure on each retry, so a dead writer does not leave them spinning forever.

## Resource and lifetime boundaries

The queue is bounded in record count and each accepted record is bounded in size. Caller-owned strings, transient parameters, allocator overhead and sink buffers are additional memory. `submit` takes ownership by value and may throw on allocation before acceptance. The queue's `try_push` leaves its argument untouched on full/closed results.

The caller must keep the sink and its referenced objects alive through close/destruction. Close calls must be serialized. SPSC callers must obey the single producer rule even during shutdown; there is no hidden producer mutex. A synchronous sink that never returns cannot be safely interrupted by portable C++.
