# treenity — Technical Design Notes

*Living document tracking role split and shared technical decisions.*

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
- Thread-per-topic model: at least one dedicated thread/routine per topic for message processing
- Synchronization: mutex/shared_mutex for safe concurrent access to shared topic data
- Graceful shutdown: SIGINT/SIGTERM handling, 5s timeout, mechanism to unblock consumers stuck reading their channel (sentinel message)
- Server responsibilities: topic create/list, client registration, message routing to consumers
- Main queue dispatch loop: single reader/router component reading the main request queue and routing to the correct handler/topic thread

**Topics to learn/review:**
- IPC mechanisms in C/C++ (POSIX MQ chosen — see below)
- Why/when bidirectional IPC needs two channels
- `std::thread`, thread lifecycle, joining vs detaching
- `std::mutex`, `std::lock_guard`, `std::unique_lock`
- `std::shared_mutex` (C++17) for reader/writer locking
- `std::condition_variable` for inter-thread signaling
- Signal handling in C++ (`sigaction`, safe SIGINT/SIGTERM handling)
- RAII for resource cleanup
- Basic race condition / deadlock awareness
- Makefile basics
- `mq_timedreceive` for deadline-bound reads

### 👤 Person B — Client, Data Structures & Protocol
**Focus:** CLI client, custom data structures, message formats

**Tasks:**
- Client executable: subcommand parsing (`create`, `list`, `produce`, `subscribe`, `info`) — must match CLI spec exactly
- Custom hashmap for client metadata (manual, explicit collision resolution)
- Prefix-matching data structure (e.g. trie) + matching logic
- Message formats: text mode (`key:body`) and `--raw` binary mode (little-endian, int32 sizes)
- Client-side offset handling (commit = last offset + 1, resume, custom `--offset`)
- Client's own response queue: creation and cleanup (`mq_unlink`) on disconnect/exit
- Unit tests (Google Test) for prefix matching

**Topics to learn/review:**
- Hashmap design from scratch (hash function, load factor, resizing, chaining vs open addressing)
- Trie / prefix-matching structures
- Binary data handling in C++ (`reinterpret_cast`, endianness)
- CLI argument parsing (manual or CLI11/cxxopts)
- Google Test basics
- Client-side signal handling and clean disconnection
- Regex for ID/topic name validation
- Reading stdin record-by-record without partial records at EOF

### 🤝 Shared work (do together before splitting)
- Review and finalize `ipc_protocol.h` together (frozen contract below)
- README.md (split sections, joint review)
- Final integration testing with `ft_aquarium` / `ft_fish`
- Git: branch + merge only after the other person has reviewed the code

---

## IPC Protocol — Frozen Contract

**Mechanism:** POSIX Message Queues

### Architecture
- Server creates `/treenity.server.<pid>` as the main request queue
- Each client creates and owns `/treenity.server.<pid>.<client_id>` as its dedicated response/consumer queue
- **Client → Server:** all requests (create, list, produce, register, info, ACK, disconnect) go through the main queue
- **Server → Client:** responses, delivered messages, and shutdown signals go through the client's dedicated queue

### Queue limits
- `mq_maxmsg` / `mq_msgsize` are set explicitly at `mq_open` with `O_CREAT` (default system limits are not relied upon)
- Message struct size accounts for the 1024-byte max key+body payload plus protocol metadata (type, request_id, offset, sizes)

### Queue ownership & cleanup
- The client that creates its own response queue is responsible for `mq_close` and `mq_unlink` on it, on both normal disconnect and shutdown
- The server only opens the client's response queue to write to it; the server never unlinks a client's queue

### Request/response correlation
- Every request carries a `request_id`; responses echo it back
- Consumer messages are asynchronous events, not tied to a request/response cycle

### Produce flow
- Producer sends a single `PRODUCE_START` request (with `request_id`) over the main queue to validate the target topic exists
- Server responds with ACK or error (topic not found → mapped to exit code 2)
- Once acknowledged, subsequent data records are streamed as fire-and-forget messages (no per-record ack), to avoid throttling throughput on the main queue

### Main queue dispatch
- A single dispatch loop on the server reads the main queue and routes each message to the correct handler or topic thread based on message type and topic name
- Topic threads never read the main queue directly

### Offset handling
- Consumer ACK contains the *next* offset (`received_offset + 1`)
- Server only updates the stored consumer offset after receiving the ACK
- `REGISTER_CLIENT` includes `has_offset` / `requested_offset` fields to distinguish "no `--offset` given" from explicit `--offset 0`

### Shutdown
- Signal handlers only set a shutdown flag; actual cleanup happens in normal server code (not inside the handler)
- Server's main loop uses `mq_timedreceive` to enforce the 5-second shutdown deadline
- On SIGINT/SIGTERM, server sends a `SHUTDOWN` sentinel to every subscribed consumer over their dedicated queue before exiting

### Error handling
- Protocol errors are returned by the server (status + error code in the response) and mapped by the client to subject exit codes: general = 1, topic/client = 2, IPC = 3
