# Changelog

## 0.1.0 (unreleased)

- `Mongo::Async::Pool.install!` opts the MongoDB driver into:
  - `Mongo::Async::Pool::ConnectionPool`: an event-loop native connection pool, with a
    stock-pool fallback for checkouts outside the owner thread's event loop,
    and first-come-first-served waiting.
  - `Mongo::Async::Pool::SocketExtension`: socket waits through the fiber scheduler
    instead of `Kernel.select` (which Async runs on a new thread per call).
  - `Mongo::Async::Pool::SafeFinalizer`: a stock-pool finalizer that can't raise.
- Behaves like `Mongo::Server::ConnectionPool` (events, close reasons, errors,
  clear/pause/populate/close semantics); passes all 32 official CMAP spec tests.
- `ConnectionPool#take_peak_in_use`: the most connections in use at once since
  the last call, tracked only once first called.
- Puma vs Falcon benchmark in `bench/`, with an HTML report.
