# Validation record

Validated locally on Linux with GCC 14.2.0 and C++20 on 2026-11-09:

- Release build and the queue/engine test executable: passed.
- AddressSanitizer plus UndefinedBehaviorSanitizer build: passed.
- ThreadSanitizer build: passed.
- Ordered 200,000-record stress tests for each backend, four-producer locked queue, full/closed queue contracts, drainage, repeated close, invalid capacity, writer exceptions, rejected oversize records, and blocked-producer failure recovery: passed.
- CLI newline roundtrip: byte-for-byte match.
- Google Benchmark 1.9.1: built and executed; [raw measurements](measurements/README.md).

Checks use explicit throwing assertions, so Release builds retain the assertions. CTest has a 60-second timeout to turn deadlocks into failures. Sanitizers were run in separate builds. The initial [GitHub Actions run](https://github.com/rishansujesh/cpp-log-ingestion/actions/runs/42703736516) passed all seven jobs: GCC and Clang with no sanitizer, AddressSanitizer/UndefinedBehaviorSanitizer, and ThreadSanitizer, plus the benchmark build and smoke run. Each compiler job also passed the CLI roundtrip.
