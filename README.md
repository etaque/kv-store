# kv-store

In-memory key-value store accessible over TCP. Nothing production-grade, this is just a toy project to play with Rust.

## Features

- RESP (REdis Serialization Protocol) compatible — works with `redis-cli` and any Redis client library
- `GET`, `SET`, `DEL`, and `PING` commands
- TTL support via `SET key value EX seconds`
- Multi-client support (threaded, `Arc<Mutex>`)
- Write-ahead log (WAL) for persistence across restarts
- Background garbage collection for expired entries

## Usage

```bash
cargo build --release
cargo run
```

Connect with `redis-cli`:

```bash
redis-cli -p 7878
```

## Protocol

Commands are sent as RESP arrays of bulk strings. Responses use standard RESP types.

### SET

```
> SET key value
+OK

> SET key value EX 60
+OK
```

### GET

```
> GET key
$5
value

> GET missing
$-1
```

### DEL

```
> DEL key
:1

> DEL missing
:0
```

### PING

```
> PING
+PONG
```

Unrecognized or malformed commands return `-ERR <message>`.

## Testing

```bash
cargo test
```
