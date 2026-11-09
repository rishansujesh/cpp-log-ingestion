# Validation record

Validated locally on Linux with GCC 14.2.0 and C++20 on 2026-11-09:

- Release build and the queue/engine test executable: passed.
- AddressSanitizer plus UndefinedBehaviorSanitizer build: passed.
- ThreadSanitizer build: passed.
- Ordered 200,000-record stress tests for each backend, four-producer locked queue, full/closed queue contracts, drainage, repeated close, invalid capacity, writer exceptions, rejected oversize records, and blocked-producer failure recovery: passed.
- CLI newline roundtrip: byte-for-byte match.
- Google Benchmark 1.9.1: built and executed; [raw measurements](measurements/README.md).

Checks use explicit throwing assertions, so Release builds retain the assertions. CTest has a 60-second timeout to turn deadlocks into failures. Sanitizers were run in separate builds. Clang and the GitHub-hosted CI matrix are configured but were not executed locally; GitHub publication requires repository-creation access. This record describes local evidence and is not a claim that hosted CI has passed.
