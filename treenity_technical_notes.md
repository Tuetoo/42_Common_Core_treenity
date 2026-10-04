# treenity — Technical Design Notes

*Living document tracking role split and shared technical decisions. Last updated: 2026-10-04 (server complete; shutdown contract and queue names changed).*

---

## Language

**C++** (C++17, for `std::shared_mutex`)

---

## Role Split

### 👤 Person A — Server & Concurrency
**Focus:** server core, thread-safety, server-side IPC

**Tasks:**
- Project setup: folder structure, Makefile (`all`, `clean`, `re`, `test`)
- IPC implementation (server side): main endpoint, accepting client connections
- Topic management: Topic struct, in-memory message storage, incrementing 32-bit offset
- Thread-per-topic model: reader thread + worker thread per topic
- Synchronization: mutex/shared_mutex for safe concurrent access to shared topic data
- Graceful shutdown: SIGINT/SIGTERM handling, wait for consumers to commit (max 5s), sentinel message to unblock consumers
- Server responsibilities: topic create/list, client registration, message routing to consumers
- Main queue dispatch loop: single reader/router reading the main request queue and routing to handlers
- **Status: done and merged** (Server, Topic, shutdown wait).
- **Now in progress: `ClientRegistry` (manual hashmap) and `PrefixIndex` (efficient prefix structure).** Originally Person B's list; Person A is writing them with guided help. Person B to confirm. Person B still owns the gtest tests for prefix matching.

### 👤 Person B — Client, Data Structures & Protocol
**Focus:** CLI client, message formats, tests

**Tasks:**
- Client executable: subcommand parsing (`create`, `list`, `produce`, `subscribe`, `info`) — must match CLI spec exactly
- Message formats: text mode (`key:body`) and `--raw` binary mode (little-endian, int32 sizes)
- Client-side offset handling (commit = last offset + 1, resume, custom `--offset`)
- Client's own response queue: creation and cleanup (`mq_unlink`) on disconnect/exit
- Unit tests (Google Test) for the hashmap and prefix matching, and a real `make test`
- Local validation of client id / topic name (see Client contract below)

### 🤝 Shared work
- Review and finalize `ipc_protocol.h` together (frozen contract below)
- README.md (split sections, joint review)
- Final integration testing with `ft_aquarium` / `ft_fish`
- Git: branch + merge only after the other person has reviewed the code

---

## IPC Protocol — Frozen Contract

**Mechanism:** POSIX Message Queues

### Architecture (three queues)
- Server creates `/treenity.server.<pid>` as the main request queue.
- Each client creates and owns `/treenity.server.<pid>.<client_id>` as its dedicated response/consumer queue.
- Each topic has a data queue `/treenity.topic.<pid>.<topic_name>`, created and unlinked by the server's `Topic`. **Changed:** it used to be `/treenity.server.<pid>.topic.<name>`, which could collide with a client whose id is `topic.<name>`.
- **Client → Server:** all requests (create, list, produce start, register, info, ACK, disconnect) go through the main queue.
- **Producer → Topic:** after `PRODUCE_START` succeeds, records go straight to the topic data queue (path returned in the response).
- **Server → Client:** responses, delivered messages and shutdown signals go through the client's dedicated queue.

### Queue limits
- `mq_maxmsg` / `mq_msgsize` are set explicitly at `mq_open` with `O_CREAT`.
- Message struct size accounts for the 1024-byte max key+body payload plus protocol metadata.

### Queue ownership & cleanup
- A client unlinks its own response queue (`mq_close` + `mq_unlink`) on normal disconnect and on shutdown.
- The server only opens a client's queue to write to it (`O_NONBLOCK`); it never unlinks it.
- The server unlinks the main queue and all topic data queues on exit.

### Request/response correlation
- Every request carries a `request_id`; responses echo it back.
- Consumer messages are asynchronous events, not tied to a request/response cycle.

### Produce flow
- Producer sends one `PRODUCE_START` over the main queue; server replies OK (with the topic data queue path) or an error (topic not found → exit code 2).
- Then records are streamed to the topic data queue as fire-and-forget messages.

### Main queue dispatch
- One dispatch loop reads the main queue (`ppoll` + `mq_receive`) and calls the handler for each message type. Topic threads never read the main queue.
- The server never trusts IPC fields: strings are read with `strnlen`, ids are validated against `^[a-zA-Z0-9_.-]{1,32}$`.

### Register / offset handling
- Consumer ACK contains the *next* offset (`received_offset + 1`). The server stores it as the next wanted offset.
- `REGISTER_CLIENT` has `has_offset` / `requested_offset` to tell "no `--offset`" from explicit `--offset 0`.
- Returning subscriber without `--offset` resumes from the stored offset. Returning subscriber without `--prefix` gets no filter (current behaviour).
- The server replies OK **before** replaying history, so the response always arrives before the first message.
- A second `REGISTER_CLIENT` for an active client id is rejected as `duplicate client name`.

### Shutdown (changed)
- Signal handlers only set a flag. The main thread blocks SIGINT/SIGTERM and waits in `ppoll` with the original mask, so no signal is lost.
- On SIGINT/SIGTERM the server stops all topics. Each topic drains its work, then sends a `SHUTDOWN` message to every subscribed consumer.
- Then the server keeps serving **only `CONSUMER_ACK` and `DISCONNECT`** on the main queue. Any other request gets `IPC_ERROR "server is shutting down"`.
- The server exits as soon as every active consumer has sent `DISCONNECT`, or after 4 seconds. `alarm(5)` is the hard stop (`_exit(0)`). The main queue is always unlinked.

### Error handling
- Protocol errors are returned as status + error code and mapped by the client to exit codes: general = 1, topic/client = 2, IPC = 3.

---

## Client contract (what Person B must do)

1. **Validate ids locally.** Client id and topic name must match `^[a-zA-Z0-9_.-]{1,32}$`. If not, print an error and exit 1. The server silently drops requests with an invalid `client_id` because it has no queue to reply to.
2. **On `SHUTDOWN` (subscriber):** send the final `CONSUMER_ACK`, then `DISCONNECT`, then `mq_unlink` own queue and exit 0. If the client skips `DISCONNECT`, the server waits the full 4 seconds.
3. **Tolerate `IPC_ERROR "server is shutting down"`** as a normal reply during shutdown (do not crash or hang).
4. **Topic data queue name** is now `/treenity.topic.<pid>.<name>`. Do not build it by hand; use `topic_data_queue_name()` from `ipc_protocol.h`, or use the path returned by `PRODUCE_START`.

---

## Known limitations (for the defense)

- Delivery to consumers is non-blocking: a consumer that is too slow can lose messages.
- A subscriber that crashes without `DISCONNECT` stays "active", so its name is rejected as a duplicate.
- `LIST_TOPICS` is capped at 512 bytes (whole names only).
- Queue names are `/treenity.server.<pid>`-style, not the subject's `/tmp/...` example (POSIX mq names cannot contain extra slashes; the subject says "e.g.").