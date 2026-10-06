*This project has been created as part of the 42 curriculum by mtaranti, jiezhang.*

# treenity — Message Queue System

## Description

treenity is a message queue broker, in the spirit of Kafka: producers publish
`key:value` messages to named **topics**, and consumers **subscribe** to a
topic (optionally filtering by key prefix) to receive them asynchronously,
resuming from a committed offset across reconnections.

It ships as two executables built from one Makefile:

- **`server`** — holds every topic's message log and consumer index in
  memory, routes produced records to the matching subscribers, and persists
  nothing to disk (an in-memory broker for the lifetime of the process).
- **`client`** — a single binary exposing five subcommands:
  `create`, `list`, `produce`, `subscribe`, `info`.

Key features:
- Topic creation/listing, text (`key:body`) and raw binary message formats.
- Prefix-filtered subscriptions with offset tracking: a returning subscriber
  resumes where it left off, or jumps to an explicit `--offset`.
- One dedicated thread pair (reader + worker) per topic, so a slow or stuck
  topic never blocks another.
- Graceful shutdown on `SIGINT`/`SIGTERM`: every subscribed consumer gets a
  sentinel message and a bounded window to disconnect cleanly before the
  server exits.
- A manually implemented hash map (client index) and a trie (consumer
  prefix index) — no `std::unordered_map` used for either.

## Instructions

### Prerequisites

- Linux (POSIX message queues — `mqueue.h` — are not available on Windows/macOS).
- `g++` with C++17 support, `make`.
- `libgtest-dev` and `pkg-config` (only needed for `make test`).

The provided `.devcontainer/devcontainer.json` (Ubuntu 24.04) installs all of
the above automatically if you open the project in a dev container.

### Build

```sh
make        # builds ./server and ./client
make re     # clean + build
make clean  # remove build artifacts
```

### Run

```sh
./server &
# prints its IPC identifier on stdout, e.g.:
# /treenity.server.1337

./client /treenity.server.1337 create user_events
./client /treenity.server.1337 list

./client /treenity.server.1337 subscribe user_events client0 --prefix user.create &

./client /treenity.server.1337 produce user_events
# then type, one per line (Ctrl+D to end):
# user.create:{"user":"reach","age":25}
# user.update:{"user":"norminet","age":42}

./client /treenity.server.1337 info client0
```

`produce` and `subscribe` also accept `--raw` for the binary
`[size:int32][bytes]...` framing instead of text lines. See the subject for
the exact wire format.

### Tests

```sh
make test
```

Builds `tests/*.cpp` against gtest and runs the resulting binary. If
`libgtest-dev`/`pkg-config` are missing, `test` reports that instead of
failing the whole build.

## Architecture

**Threading model.** The server's main thread runs a single dispatch loop
(`ppoll` + `mq_receive` on the main request queue) that handles every
management request (`create`, `list`, `register`, `produce-start`, `info`,
`ack`, `disconnect`) and routes topic-specific work to the right `Topic`.
Each `Topic` owns exactly two threads:
- a **reader** thread blocked on `mq_receive` on that topic's own data
  queue, turning each wire-format `ProduceRecord` into a work item;
- a **worker** thread that drains a `std::queue` of work items
  (`PRODUCE` / `ADD_CONSUMER` / `REMOVE_CONSUMER`) one at a time, guarded by
  a `std::mutex` + `std::condition_variable`.

Funnelling all mutation of a topic's message log and consumer index through
that single worker thread means the log itself only needs a
`std::shared_mutex` for the reader/writer split against the *delivery* path
(`get`/`size` take a shared lock, `append` takes a unique one) — no locking
is needed between the reader and worker threads themselves, since the work
queue already serializes them.

**Synchronization.** The client registry (hash map) is wrapped in a
`std::shared_mutex` for the same reason: `info` reads it concurrently with
writes from `register`/`ack`/`disconnect` on the main thread.

**Graceful shutdown.** Signal handlers for `SIGINT`/`SIGTERM` only set a
`volatile sig_atomic_t` flag — no unsafe calls inside the handler. The main
thread blocks both signals up front and passes the original mask to
`ppoll`, so a signal raised between the "is shutdown requested?" check and
the blocking wait is never lost. On shutdown the server stops every topic
(each drains its queue, then sends a `SHUTDOWN` sentinel over every
subscribed consumer's dedicated reply queue — the only way to unblock a
consumer stuck in a blocking read on a one-way queue), then keeps serving
only `CONSUMER_ACK`/`DISCONNECT` on the main queue until every active
consumer has disconnected, or a 4s soft deadline / 5s hard `alarm()` is hit.

## IPC Choice

**Mechanism: POSIX message queues** (`mq_open`/`mq_send`/`mq_receive`).
Chosen over System V message queues and named pipes because:
- Each message is received as a discrete, already-delimited unit (unlike a
  FIFO, which is a byte stream and would force us to invent our own framing
  on top), which matches treenity's "one struct per message" protocol
  exactly.
- `mq_open` returns a file descriptor accepted by `poll`/`ppoll`, so the
  server can wait on the main queue and a signal mask in a single syscall;
  SysV message queues (`msgget`/`msgrcv`) have no equivalent descriptor to
  poll on.
- Queues are named by path-like strings rather than a numeric key derived
  from `ftok()`, so advertising the main queue's name (printed on `server`'s
  stdout) and deriving a client's reply-queue name by string concatenation
  is direct and race-free.

**Bidirectional communication.** POSIX message queues are one-way: the
process that calls `mq_open(O_CREAT | O_RDONLY, ...)` owns the read end, and
every other process opens the same name `O_WRONLY`. treenity uses three
queues to get both directions across three different traffic patterns:

1. **Main request queue** (`/treenity.server.<pid>`) — the server is the
   only reader; every client writes every request to it.
2. **Per-client reply queue** (`/treenity.server.<pid>.<client_id>`) —
   created and owned by the client, read only by the client; the server
   opens it `O_WRONLY` to deliver responses, `CONSUMER_MESSAGE`s, and the
   `SHUTDOWN` sentinel.
3. **Per-topic data queue** (`/treenity.topic.<pid>.<topic_name>`) — created
   by the `Topic`; once a producer's `PRODUCE_START` handshake succeeds on
   the main queue, every following record is written straight here,
   fire-and-forget, so a high-throughput producer never contends with the
   main queue's single dispatch loop.

## Data Structures

**Client registry (hash map, `src/server/HashMap.hpp`).** A manually
implemented hash map — no `std::unordered_map` — using **separate chaining**
for collision resolution: each bucket is a singly linked list of nodes
(`std::unique_ptr<Node>`). The hash function is **FNV-1a**, chosen for being
simple, fast, and giving a good-enough distribution for short ASCII
identifiers (client ids, topic names) without needing a cryptographic hash.
The table doubles its bucket count whenever the load factor would exceed
`0.75` on the next insert, so lookup stays close to O(1) amortized
regardless of how many clients register. `ClientRegistry` wraps it behind a
`std::shared_mutex` so `info` (a reader) never blocks another `info`, while
still being safely serialized against `register`/`ack`/`disconnect` writers.

**Prefix-matching structure (trie, `src/server/PrefixIndex.hpp`).** A
**trie (prefix tree)** over the prefixes registered by a topic's consumers.
`match(key)` walks the trie one character of the message key at a time and
collects every node crossed along the way — so a consumer registered on
`"user"` matches `"user"` and `"user.login"` but not `"admin"`, in **O(key
length)** rather than scanning every consumer on every produced message.
Empty-prefix ("subscribe to everything") consumers never touch the trie at
all: they're kept in a separate list and always match, per the subject's
"Direct Addition" rule. A trie was chosen over, e.g., a balanced BST of
prefixes because prefix membership is exactly what a trie is built for —
checking "does any registered prefix match this key" falls out of the walk
for free, with no per-node string comparison needed.

The trie's child links use `std::map`, and its id-to-consumer lookup reuses the
hand-written hash map above, so `std::unordered_map` is not used anywhere. Both 
structures are owned by the server; `ClientRegistry`/`PrefixIndex` are
implemented by `jiezhang`/`mtaranti` respectively but called from the
server's dispatch loop and topic worker threads — see `treenity_technical_notes.md`
for the interface history.

## Testing

`make test` compiles everything under `tests/` against **Google Test**
(`libgtest-dev`, located via `pkg-config gtest gtest_main`), links it
directly against `src/server/PrefixIndex.cpp` (no mocking — the real trie
implementation is exercised), and runs the resulting `test_runner` binary.

Coverage:
- **`tests/PrefixIndexTest.cpp`** — exact match, prefix match, no-match
  (including a case that shares a letter but diverges before the end of a
  registered prefix), the empty-prefix wildcard, multiple consumers sharing
  one prefix, removal (including of an unknown client, and that it only
  affects the targeted client), re-subscribing replacing a previous prefix,
  `all()`, and matching against an empty key.
- **`tests/HashMapTest.cpp`** — insert/find/erase/upsert, duplicate insert
  rejection, and — using a `ConstantHash` that forces every key into the
  same bucket — collisions resolved correctly under chaining (insert, find,
  and erase all individually correct, not just "something is in there"),
  plus automatic resizing under load with every key remaining findable
  afterwards.

## Resources

- POSIX message queues: `man 7 mq_overview`, `man 2 mq_open`, `man 3
  mq_send`, `man 3 mq_receive`
- FNV-1a hash: <https://www.isthe.com/chongo/tech/comp/fnv/>
- Trie / prefix tree: standard CS data structure, see e.g. *Introduction to
  Algorithms* (CLRS) or any algorithms reference on prefix trees
- Google Test: <https://google.github.io/googletest/>

**AI usage.** Claude (Anthropic) was used by `mtaranti` for: implementing
the entire `client` executable (`src/client/`) against the `ipc_protocol.h`
contract and the behavioural notes left in `treenity_technical_notes.md`;
replacing the placeholder linear-scan `PrefixIndex` with the trie
implementation described above; writing the `gtest` suite under `tests/`;
wiring `make test` and the devcontainer's `libgtest-dev` dependency; and
drafting this README. Every piece was reviewed line by line and validated
with end-to-end integration tests run against the real `server` binary
(create/list/produce/subscribe/info, prefix filtering, offset resume, raw
binary mode, and graceful shutdown via `SIGINT`) before being committed —
see the git history for the resulting commits.

**AI usage (`jiezhang`).** Claude (Anthropic) was used as a tutor and code reviewer for the server side, for which `jiezhang` had no prior C++ experience. For each piece (`Server`, `Topic`, the hash map behind `ClientRegistry`, the shutdown sequence) Claude explained the design and the C++/POSIX concepts, proposed the code, and asked comprehension questions. `jiezhang` entered the code, built it, fixed the compile errors, ran small throw-away test programs against it, and committed each step on its own branch. Claude also read `mtaranti`'s client and trie, ran them against the server, and pointed out issues that the two of us then discussed.
