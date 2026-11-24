# mongo-async-pool

Lets [Falcon](https://github.com/socketry/falcon)'s fiber concurrency reach
MongoDB.

Puma serves concurrent requests with a pool of threads, one request per
thread. Falcon runs each request in a fiber on
[Async](https://github.com/socketry/async)'s event loop, so one process can
serve many requests at once without a thread for each, and a request can run
several queries concurrently. But the
[MongoDB Ruby driver](https://github.com/mongodb/mongo-ruby-driver) was built
for threads, and under Falcon it gives most of that back:

- **Every socket wait starts a thread.** `Mongo::Socket` waits with
  `Kernel.select`, which under a fiber scheduler goes through `io_select`, and
  Async implements that by running `IO.select` on a new thread. The driver
  waits on nearly every read, so each query costs a thread, and Falcon ends up
  no faster than Puma, with worse tail latency under load.
- **The pool wasn't built for fibers.** Waiting for a connection, establishing
  one and maintaining the pool all go through threads (a populator per server),
  and a fiber that's cancelled mid-query (a client disconnects, a
  `with_timeout` fires) returns its connection with the unread reply still on
  the socket, so the next query on it gets someone else's reply
  (`Mongo::Error::UnexpectedResponse`).

mongo-async-pool makes the driver fiber-native without forking it: socket waits go
through the event loop, and a connection pool that runs on fibers replaces the
stock one. It changes nothing until you call `Mongo::Async::Pool.install!`, and it's
a drop-in replacement: it passes all of the official MongoDB connection pool
spec tests.

With the database 2ms away, one Falcon process with mongo-async-pool serves
46–60% more requests than a 16-thread Puma process from 64 to 256 clients, and
four Falcon processes serve 13–52% more than four Puma processes, on half to
two thirds of the CPU. The farther away the database, the bigger the gap: at
5ms, one process serves 3× as much. Falcon with the stock driver gets some of
that concurrency, but its latency falls apart under load. See
[Benchmarks](#benchmarks).

## Installation

```ruby
# Gemfile
gem "mongo-async-pool", require: "mongo/async/pool/install"
```

or install explicitly at boot, before any `Mongo::Client` connects:

```ruby
require "mongo/async/pool"

Mongo::Async::Pool.install!                 # pool + socket extension (where supported)
Mongo::Async::Pool.install!(socket: false)  # just the pool
Mongo::Async::Pool.install!(pool: false)    # just the socket extension
```

`install!` is process-wide, idempotent, and can't be undone. It checks that the
driver still has the internals mongo-async-pool relies on and raises
`Mongo::Async::Pool::IncompatibleDriverError` (naming what's missing) if not.

It's safe to install in any process: connections are only taken from the
async pool by fibers running in an Async task; everything else (Puma threads,
Sidekiq, scripts) transparently uses the driver's stock pool.

## What it changes

### `Mongo::Async::Pool::ConnectionPool`

Replaces `Mongo::Server::ConnectionPool` for every server, except in
load-balanced deployments (see [Load-balanced deployments](#load-balanced-deployments)).

- **Event-loop native.** Checkout, check-in and waiting never block the
  reactor. Waiting for a connection, establishing one and maintaining the pool
  all happen on fibers.
- **Cancellation safe.** If anything other than a `Mongo::Error` (e.g.
  `Async::Stop`, `Timeout::Error`) interrupts an operation, the connection is
  discarded instead of returned to the pool.
- **No pool threads.** A transient maintenance fiber keeps `min_pool_size`
  connections, retires idle (`max_idle_time`) and stale connections, and
  interrupts in-use connections when the driver detects that the server went
  away (so fibers blocked on a dead server get a retryable `PoolClearedError`).
  The driver's own background threads only flip flags and wake it.
- **Fast checkouts.** Idle connections are handed out without waiting behind
  connections being established; at most `max_connecting` connections are
  established at once, and they count toward `max_pool_size`. The most
  recently used connection is reused first.
- **Fork aware.** Connections inherited from a parent process are never reused.
- **Thread-safe fallback.** The pool is owned by one thread: the first that
  checks out from an Async task. Checkouts from other threads, or from outside
  an Async task, go to a stock pool created on first use, which shares the
  async pool's generation counter so the driver's server monitoring stays
  consistent. In a forked Falcon worker, every request uses the async pool.

See [Sizing the connection pool](#sizing-the-connection-pool) for how to
choose `max_pool_size` under Falcon.

It honors the driver's pool options: `max_pool_size`, `min_pool_size`,
`wait_queue_timeout`, `max_idle_time`, `max_connecting`, and client-side
operation timeouts (`timeoutMS`). It publishes the standard CMAP monitoring
events, and `pool.stats` returns a summary:

```ruby
client.cluster.servers.first.pool.stats
# => {state: :ready, idle: 7, in_use: 1, connecting: 0, max_size: 20, min_size: 0, maintenance: true, fallback: nil}
```

`in_use` is a snapshot, so a sampler polling it misses short bursts.
`pool.take_peak_in_use` returns the most connections in use at once since its
last call. Tracking starts with the first call, so a pool that's never measured
doesn't do it.

### `Mongo::Async::Pool::SocketExtension`

Prepended to `Mongo::Socket`. When a fiber scheduler is active, reads and
write-waits use `IO#wait_readable` / `IO#wait_writable`, which the scheduler
handles in its event loop; otherwise (e.g. the driver's monitor threads) the
stock code runs. Connection establishment still uses `IO.select`; it only
happens when a pool opens a connection.

It also makes fewer system calls per round trip than the stock code: after
writing a request it waits for the reply rather than trying a read that would
find nothing yet, a small reply's header and body come from one read, and reads
and writes skip the scheduler's `io_read` / `io_write` hooks (`recv_nonblock`
and `write_nonblock`). A query then takes one write, one read and one wait,
rather than one write and three reads.

The stock read also watches a pipe that lets the pool wake readers blocked on a
dead server. The extension relies on closing the socket instead, which wakes a
fiber blocked on it only on Ruby 3.5+ (`Fiber::Scheduler#fiber_interrupt`). So
`install!` enables it automatically only there; on Ruby 3.4 you can force it
with `install!(socket: true)`, and readers on a dead server are then only woken
by `socketTimeoutMS`.

### `Mongo::Async::Pool::SafeFinalizer`

Installed with the pool. The stock pool's finalizer stops at the first
connection that fails to disconnect (e.g. `Errno::EBADF` after a fork, or an
`Interrupt`), and an exception escaping a finalizer surfaces in whatever code
the GC interrupted. This one disconnects each connection independently and
never raises.

## Benchmarks

`bundle exec ruby bench/run.rb` compares Puma + stock mongo, Falcon + stock
mongo and Falcon + mongo-async-pool as the number of concurrent clients grows
from 1 to 1,024, at 1 process and at 4 processes. It writes a self-contained
HTML report plotting requests/s, latency and CPU against the number of clients;
see [bench/](bench/README.md).

Puma runs with 4, 8 or 16 threads and a database connection per thread, as Ruby
apps usually do, so its concurrency is its thread count. Falcon runs a fiber
per request with 64 connections per process, so its concurrency is its
connection pool. Measured on an Apple M3 Ultra, Ruby 4.0.6 + YJIT, MongoDB
8.0.8 on localhost (Docker), mongo 2.26.0, Puma 8.0.2, Falcon 0.57.0.
Requests/s:

| 1 process | Clients | Puma, 4 threads | Puma, 8 threads | Puma, 16 threads | Falcon + stock mongo | Falcon + mongo-async-pool |
|---|--:|--:|--:|--:|--:|--:|
| `/find` | 64 | 5,765 | 5,750 | 5,851 | 6,281 | **9,094** |
| `/find` | 256 | 5,704 | 6,024 | 6,087 | 5,346 | **8,248** |
| `/find?remote=1` | 64 | 1,326 | 2,872 | 5,541 | 5,476 | **8,862** |
| `/find?remote=1` | 256 | 1,313 | 2,873 | 5,522 | 4,987 | **8,052** |
| `/fanout?remote=1` | 64 | 260 | 573 | 1,182 | 1,043 | **1,872** |
| `/fanout?remote=1` | 256 | 265 | 587 | 1,238 | 745 | **1,325** |

| 4 processes | Clients | Puma, 4 threads | Puma, 8 threads | Puma, 16 threads | Falcon + stock mongo | Falcon + mongo-async-pool |
|---|--:|--:|--:|--:|--:|--:|
| `/find` | 64 | 13,485 | 13,747 | 13,518 | 14,957 | **19,253** |
| `/find` | 256 | 13,429 | 13,975 | 14,083 | 16,502 | **24,604** |
| `/find?remote=1` | 64 | 5,974 | 10,776 | 12,650 | 12,268 | **14,322** |
| `/find?remote=1` | 256 | 5,993 | 11,081 | 13,482 | 14,082 | **20,426** |
| `/fanout?remote=1` | 64 | 1,220 | 2,297 | 2,958 | 2,855 | **3,697** |
| `/fanout?remote=1` | 256 | 1,217 | 2,296 | 3,005 | 2,783 | **4,139** |

`/find` is one indexed query against the local database; `?remote=1` adds 2ms
to every database round trip (a fast query on a database in another
availability zone; see [Database distance](#database-distance) for 1 and 5ms),
and `/fanout` runs 5 queries per request, concurrently in fibers under Falcon
and one after another under Puma.

- **On one process, Falcon + mongo-async-pool serves 36–60% more requests than
  the best Puma configuration** from 64 to 256 clients, on the local database
  and with it 2ms away, with a lower p99 (37ms against 49ms at 256 clients,
  2ms away). At 2ms, 16 threads are enough to keep one Puma process busy: it's
  limited by CPU (the GVL), at about 5,500 req/s, close to its 5,900–6,100 on
  the local database. With 4 or 8 threads it's limited by its threads, at about
  1,300 and 2,900 req/s however many clients are waiting.
- **Fibers alone aren't enough.** Falcon with the stock driver gets past
  Puma's thread cap, but falls off as load grows, with tail latency measured
  in hundreds of milliseconds to seconds (p99 124ms at 256 clients, 2ms away,
  against 37ms with mongo-async-pool; 1.2s at 1,024 clients on the local
  database).
- **At four processes, Falcon + mongo-async-pool serves 40–75% more than the
  best Puma configuration on the local database, and 13–52% more with it 2ms
  away**, from 64 clients up, on half to three quarters of the CPU (2.6–3.9
  cores against 5.0–5.4) and with a p99 as low or lower.
- **Five queries per request**, run concurrently in fibers, take 3.8ms
  (median, 1 client) against 20–22ms one after another on Puma. Under load,
  four Falcon processes serve 23–38% more than Puma from 64 clients up, and one
  serves 58% more at 64 clients. At 256 clients one process is about level with
  Puma: with 1,280 queries waiting for 64 connections it spends more of its
  time in GC, and varied between 1,300 and 1,700 req/s across runs.
- **One Falcon process has limits**: it takes every request at once, each in a
  fiber, on one core, so at 1,024 clients a single process falls behind Puma
  on the local database (5,385 against 5,920 req/s). With four processes it
  stays ahead. Run several Falcon workers, as you would Puma workers.
- **Where the gains come from** (one process, `/find`, 256 clients): the
  socket extension alone takes Falcon from 5,346 to 7,700 req/s, the pool alone
  cuts p99 from 96ms to 69ms, and together they give 8,248 req/s at 38ms p99.
  With five queries per request, the socket extension alone serves the most
  (1,498 against 1,325 req/s), but with a p99 of 358ms against 228ms.
- **Warm up, or size the heap.** The numbers are measured after a warmup at
  1,024 clients. With many requests in flight, each GC scans every fiber's
  stack, so until the heap has grown, one Falcon + mongo-async-pool process
  spends much of its time collecting: at 256 clients it served about 5,800
  req/s on `/find` before the heap had grown, against 8,200 after (pre-sizing
  it with `RUBY_GC_HEAP_0_INIT_SLOTS` and friends does the same). Puma and
  Falcon with the stock driver weren't affected.
- Without a database (`/ping`), one Falcon process serves about 48,000–51,000
  req/s against Puma's 38,000–39,000, and four about 154,000 against
  91,000–93,000.

### Database distance

How much the async pool helps depends on how long each query keeps the app
waiting: the network round trip (within a cloud region, about 0.1–0.5ms inside
an availability zone and 0.5–2ms across zones) plus however long the query
runs. Your APM's median query duration is a good estimate. Requests/s on
`/find?remote=1` at 256 clients, with 1, 2 and 5ms added to each round trip:

| | Processes | 1ms | 2ms | 5ms |
|---|--:|--:|--:|--:|
| Puma, 16 threads | 1 | 5,590 | 5,522 | 2,636 |
| Falcon + stock mongo | 1 | 4,948 | 4,987 | 4,981 |
| Falcon + mongo-async-pool | 1 | **7,718** | **8,052** | **7,906** |
| Puma, 16 threads | 4 | 13,761 | 13,482 | 10,754 |
| Falcon + stock mongo | 4 | 15,798 | 14,082 | 15,204 |
| Falcon + mongo-async-pool | 4 | **23,546** | **20,426** | **21,130** |

Up to about 2ms, 16 threads keep Puma busy and the GVL is its limit; at 5ms the
threads are, and one process serves half as much. Falcon's 64 connections per
process cover the wait at every distance, so Falcon + mongo-async-pool serves
38–46% more than Puma on one process at 1–2ms and 3.0× as much at 5ms, and
52–96% more on four processes.

### Same number of connections

With the same number of database connections per process (Puma with a thread
per connection), the database concurrency is equal and the difference is
fibers and the driver. Requests/s at 256 clients, database 2ms away for
`remote`, Puma → Falcon + mongo-async-pool:

| Connections per process | `/find`, 1 process | `/find?remote=1`, 1 process | `/fanout?remote=1`, 1 process | `/find?remote=1`, 4 processes |
|--:|--:|--:|--:|--:|
| 4 | 5,704 → **7,908** | **1,313** → 1,204 | **265** → 236 | 5,993 → **6,039** |
| 8 | 6,024 → **8,303** | **2,873** → 2,806 | **587** → 483 | 11,081 → **11,342** |
| 16 | 6,087 → **8,164** | 5,522 → **5,850** | **1,238** → 992 | 13,482 → **16,162** |
| 64 | 5,556 → **8,248** | 5,093 → **8,052** | **1,441** → 1,325 | 11,792 → **20,426** |

- **CPU-bound work is faster on Falcon + mongo-async-pool at every pool size**:
  34–48% on one process and 12–84% on four, at 256 clients.
- **When the connections are the limit, it's close to a tie**: with 4 or 8
  connections and the database 2ms away, throughput is about connections ÷
  round trip on either server, within 9% of each other, with Falcon using less
  CPU. With five queries per request on one process, Falcon serves 11–20% less
  than Puma at 4–16 connections.
- **The difference is what more connections cost.** For Puma each one is a
  thread, and at 64 threads one process is no faster than at 16 (5,093 req/s
  against 5,522, on 1.2 cores), against Falcon's 8,052 on 1.0 core (58% more);
  at four processes, 11,792 req/s on 5.5 cores against 20,426 on 3.2 (73%
  more). Fibers make connections cheap, so Falcon can run enough of them to
  keep up with a database that's a few milliseconds away.

Numbers vary between runs by a few percent, and by up to about 10% for one
Falcon process with many requests in flight; the report has every
measurement, including p50/p90/p99 latency, CPU and memory.

## Sizing the connection pool

Under Puma, the thread count limits how many queries a process runs at once,
whatever `max_pool_size` says. Under Falcon, every request has its own fiber,
so `max_pool_size` is that limit, and it should be sized for the database
rather than for the number of clients:

- Each process needs about **queries per second × query round trip**
  connections: a process doing 2,000 queries/s against a database 2ms away
  keeps about 4 busy; at 5ms, about 10; on localhost at 0.5ms, about 1.
- Leave headroom for bursts, but more connections than the database can serve
  at once don't add throughput. They queue inside the database, raising its
  CPU and your tail latency. In the benchmarks (`/find`, 256 clients), four
  Falcon processes with 64 connections each kept about 40 queries in flight in
  a local MongoDB and used about 17 of its cores; with 16 each, they served
  about 15% fewer requests with 8 in flight, on 9.5 cores and with a lower p99
  (21ms against 26ms), still 47% more than four 16-thread Puma processes (2
  in flight, 2.7 cores).
- Count connections across all processes and servers: that total is what the
  database sees.
- The driver's default `max_pool_size` is 20 per server; requests beyond it
  wait in the pool's queue (`wait_queue_timeout`, 10s by default), first come
  first served.

## Compatibility

| | |
|---|---|
| Ruby | 3.4+ (socket extension auto-enabled on 3.5+) |
| mongo | 2.24 – 2.26 tested; checked for compatibility at install |
| async | 2.24+ |
| MongoDB topologies | standalone, replica set, sharded; [load-balanced](#load-balanced-deployments) servers keep the stock pool |

`Mongo::Async::Pool::ConnectionPool` is a drop-in replacement for
`Mongo::Server::ConnectionPool`: the same public methods and options, the same
CMAP monitoring events (in the same order, with the same `ConnectionClosed`
reasons), the same errors, and the same semantics for clearing, pausing,
populating and closing.

- **Spec compliance:** it passes all 32 of the official
  [CMAP spec tests](https://github.com/mongodb/specifications/tree/master/source/connection-monitoring-and-pooling)
  (as vendored by the Ruby driver), run with the specs' "threads" as Async
  tasks. The same runner passes the stock pool, which checks the runner
  itself (`CMAP_POOL=stock`).
- **Failure handling:** integration specs check, through the driver and with
  fail points, that network and state-change errors clear the pool and retries
  succeed, that ordinary command errors leave it alone, and that blocked
  connections time out waiting checkouts and then recover.

The pool's state is only touched by its owner thread's fibers. In Falcon's
threaded or hybrid modes, the first reactor thread uses the async pool and the
others use the fallback stock pool.

### Load-balanced deployments

When the driver connects in load-balanced mode (`loadBalanced=true`, e.g. a
MongoDB Atlas sharded cluster behind a private endpoint, or mongos behind a
layer 4 load balancer), servers keep the driver's stock
`Mongo::Server::ConnectionPool`. Everything works exactly as it does without
mongo-async-pool, but the async pool's improvements don't apply to those
connections.

In this mode the load balancer may route each connection to a different
mongos, so the driver pins a connection to a cursor (for its `getMore`s) or to
a transaction until it finishes, tracks connection generations per backend
service, and clears the pool per service. Supporting that in the async pool
needs connection-specific checkouts, waiting for a particular pinned connection
to be returned, and per-service clearing and interruption, all of which need a
load-balanced test environment to verify. The stock pool already implements it
correctly.

| | Other topologies | Load-balanced |
|---|---|---|
| Connection pool | `Mongo::Async::Pool::ConnectionPool` | `Mongo::Server::ConnectionPool` |
| Socket waits through the fiber scheduler (`SocketExtension`) | ✓ | ✓ |
| Safe stock-pool finalizer (`SafeFinalizer`) | ✓ (fallback pool) | ✓ |
| Connections discarded when a fiber is cancelled mid-operation | ✓ | ✗ a connection can be returned with an unread reply, and the next operation on it fails |
| Fair (first come, first served) waiting with one wakeup per release | ✓ | stock behavior |
| Pool maintenance (`min_pool_size`, idle and stale connections) | maintenance fiber | populator thread per server |
| In-use connections interrupted when the server goes away | ✓ | ✓ (by the populator thread) |

To check which pool a server uses:

```ruby
client.cluster.servers.map { |server| server.pool.class }
# => [Mongo::Server::ConnectionPool] in load-balanced mode
```

## Development

```bash
bundle install
docker run -d --rm --name mongo-async-pool-test -p 27017:27017 mongo:8 --setParameter enableTestCommands=1
bundle exec rspec          # MONGODB_URI, default mongodb://localhost:27017/mongo_async_pool_test
bundle exec rubocop
MONGO_RUBY_DRIVER_VERSION="~> 2.24.0" bundle update mongo && bundle exec rspec
```

The CMAP and failure specs need a server with test commands enabled (for fail
points) and are skipped without one; the integration specs need any MongoDB.

## License

MIT
