# kv-store

In-memory key-value store accessible over TCP. Nothing production-grade, this is just a toy project to play with Rust.

## Features

- Text protocol with `GET`, `SET`, and `DEL` commands
- TTL support via `SET key value EX seconds`
- Multi-client support (threaded, `Arc<Mutex>`)
- Write-ahead log (WAL) for persistence across restarts
- Background garbage collection for expired entries

## Usage

```bash
cargo build --release
cargo run
```

Connect with `nc`:

```bash
nc localhost 7878
```

## Protocol

### SET

```
SET key value
OK

SET key value EX 60
OK
```

### GET

```
GET key
value

GET missing
(empty line)
```

### DEL

```
DEL key
OK
```

Unrecognized or malformed commands return `ERR <message>`.

## Testing

```bash
cargo test
```
