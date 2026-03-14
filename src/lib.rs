use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, PartialEq)]
pub enum Command {
    Get {
        key: String,
    },
    Set {
        key: String,
        value: String,
        ttl: Option<u64>,
    },
    Del {
        key: String,
    },
    Ping,
    Command,
    CommandDocs,
}

struct Store {
    data: HashMap<String, Entry>,
}

struct Entry {
    value: String,
    expires_at: Option<SystemTime>,
}

impl Store {
    fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    fn get(&self, key: &str) -> Option<&str> {
        self.data.get(key).map(|v| v.value.as_str())
    }

    fn set(&mut self, key: String, value: String, expires_at: Option<SystemTime>) {
        self.data.insert(key, Entry { value, expires_at });
    }

    fn del(&mut self, key: &str) -> bool {
        self.data.remove(key).is_some()
    }

    fn gc(&mut self) {
        self.data
            .retain(|_key, entry| entry.expires_at.is_none_or(|t| t > SystemTime::now()))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum KvError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Parse error: {0}")]
    Parse(String),
    #[error("WAL parse error: {0}")]
    WalParse(String),
    #[error("End of file")]
    Eof,
}

pub fn run_server(listener: TcpListener) -> Result<(), KvError> {
    let store = Arc::new(Mutex::new(Store::new()));
    let wal_path = Path::new("./kv.log");

    wal_replay(wal_path, &mut store.lock().unwrap())?;

    let gc_store = store.clone();
    thread::spawn(move || {
        loop {
            thread::sleep(Duration::from_millis(1000));
            gc_store.lock().unwrap().gc();
        }
    });

    for stream in listener.incoming() {
        let stream = stream?;
        let store = store.clone();
        thread::spawn(move || {
            handle_client(stream, store, wal_path)
                .unwrap_or_else(|e| eprintln!("Client error: {}", e));
        });
    }
    Ok(())
}

fn handle_client(
    stream: TcpStream,
    store: Arc<Mutex<Store>>,
    wal_path: &Path,
) -> Result<(), KvError> {
    let mut writer = stream.try_clone()?;
    let mut reader = BufReader::new(stream);

    loop {
        let args = match read_resp_array(&mut reader) {
            Err(KvError::Eof) => break,
            other => other?,
        };

        let response = match parse_args(&args) {
            Ok(Command::Set { key, value, ttl }) => {
                let expires_at = ttl.map(|ttl| SystemTime::now() + Duration::from_secs(ttl));
                store.lock().unwrap().set(key, value, expires_at);
                wal_append(wal_path, &args)?;
                resp_simple("OK")
            }
            Ok(Command::Get { key }) => match store.lock().unwrap().get(&key) {
                Some(value) => resp_bulk(&value.to_string()),
                None => resp_null(),
            },
            Ok(Command::Del { key }) => {
                wal_append(wal_path, &args)?;
                if store.lock().unwrap().del(&key) {
                    resp_integer(1)
                } else {
                    resp_integer(0)
                }
            }
            Ok(Command::Ping) => resp_simple("PONG"),
            Ok(Command::Command) => resp_bulk(""),
            Ok(Command::CommandDocs) => resp_array(&[]),
            Err(KvError::Parse(msg)) => resp_error(&msg),
            Err(e) => resp_error(&e.to_string()),
        };

        writer.write_all(response.as_bytes())?;
        writer.flush()?;
    }
    Ok(())
}

fn read_resp_array<R: Read>(reader: &mut BufReader<R>) -> Result<Vec<String>, KvError> {
    let mut line = String::new();
    if reader.read_line(&mut line)? == 0 {
        return Err(KvError::Eof);
    };

    let line = line.trim_end();
    let count: usize = line
        .strip_prefix('*')
        .ok_or_else(|| KvError::Parse("expected array".into()))?
        .parse()
        .map_err(|_| KvError::Parse("invalid array length".into()))?;
    let mut args = Vec::with_capacity(count);
    for _ in 0..count {
        let mut header = String::new();
        reader.read_line(&mut header)?;
        let len: usize = header
            .trim_end()
            .strip_prefix('$')
            .ok_or_else(|| KvError::Parse("expected bulk string".into()))?
            .parse()
            .map_err(|_| KvError::Parse("invalid bulk length".into()))?;
        let mut buf = vec![0u8; len + 2];
        reader.read_exact(&mut buf)?;
        buf.truncate(len);
        args.push(String::from_utf8_lossy(&buf).into_owned());
    }
    Ok(args)
}

fn parse_args(args: &[String]) -> Result<Command, KvError> {
    match args.iter().as_slice() {
        [cmd, key] if cmd == "GET" => Ok(Command::Get {
            key: key.to_string(),
        }),
        [cmd, key, value, rest @ ..] if cmd == "SET" => {
            let ttl: Option<u64> = match rest.iter().as_slice() {
                [ex, secs] if ex == "EX" => secs
                    .parse()
                    .map(Some)
                    .map_err(|_| KvError::Parse(format!("invalid TTL: {}", secs)))?,
                _ => None,
            };

            Ok(Command::Set {
                key: key.to_string(),
                value: value.to_string(),
                ttl,
            })
        }
        [cmd, key] if cmd == "DEL" => Ok(Command::Del {
            key: key.to_string(),
        }),
        [cmd] if cmd == "PING" => Ok(Command::Ping),
        [cmd, ..] if cmd == "COMMAND" => Ok(Command::Command),
        _ => Err(KvError::Parse("unknown command".to_string())),
    }
}

fn wal_replay(path: &Path, store: &mut Store) -> Result<(), KvError> {
    if !path.exists() {
        return Ok(());
    }

    let mut reader = BufReader::new(File::open(path)?);

    loop {
        let args = match read_resp_array(&mut reader) {
            Err(KvError::Eof) => break,
            other => other?,
        };
        match parse_args(&args) {
            Ok(Command::Set { key, value, ttl }) => {
                let expires_at = ttl.map(|ts| UNIX_EPOCH + Duration::from_secs(ts));
                store.set(key, value, expires_at);
            }
            Ok(Command::Del { key }) => {
                store.del(&key);
            }
            Ok(_) => {
                eprintln!("WAL error: unexpected command, skipping");
            }
            Err(e) => {
                eprintln!("WAL error: {}", e);
            }
        }
    }

    Ok(())
}

fn resp_simple(s: &str) -> String {
    format!("+{}\r\n", s)
}

fn resp_error(s: &str) -> String {
    format!("-ERR {}\r\n", s)
}

fn resp_integer(n: i64) -> String {
    format!(":{}\r\n", n)
}

fn resp_bulk(s: &str) -> String {
    format!("${}\r\n{}\r\n", s.len(), s)
}

fn resp_array(arr: &[String]) -> String {
    let mut res = format!("*{}\r\n", arr.len());
    for s in arr {
        res += &resp_bulk(s);
    }
    res
}

fn resp_null() -> String {
    "$-1\r\n".into()
}

fn wal_append(path: &Path, args: &[String]) -> Result<(), KvError> {
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    write!(file, "{}", resp_array(args))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- parse_args ---

    fn args(strs: &[&str]) -> Vec<String> {
        strs.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn parse_get() {
        assert_eq!(
            parse_args(&args(&["GET", "foo"])).unwrap(),
            Command::Get {
                key: "foo".to_string()
            }
        );
    }

    #[test]
    fn parse_set() {
        assert_eq!(
            parse_args(&args(&["SET", "foo", "bar"])).unwrap(),
            Command::Set {
                key: "foo".to_string(),
                value: "bar".to_string(),
                ttl: None
            }
        );
    }

    #[test]
    fn parse_set_with_ttl() {
        assert_eq!(
            parse_args(&args(&["SET", "foo", "bar", "EX", "30"])).unwrap(),
            Command::Set {
                key: "foo".to_string(),
                value: "bar".to_string(),
                ttl: Some(30)
            }
        );
    }

    #[test]
    fn parse_set_with_invalid_ttl() {
        assert!(parse_args(&args(&["SET", "foo", "bar", "EX", "notnum"])).is_err());
    }

    #[test]
    fn parse_del() {
        assert_eq!(
            parse_args(&args(&["DEL", "foo"])).unwrap(),
            Command::Del {
                key: "foo".to_string()
            }
        );
    }

    #[test]
    fn parse_ping() {
        assert_eq!(parse_args(&args(&["PING"])).unwrap(), Command::Ping);
    }

    #[test]
    fn parse_missing_args() {
        assert!(parse_args(&args(&["GET"])).is_err());
        assert!(parse_args(&args(&["SET"])).is_err());
        assert!(parse_args(&args(&["SET", "foo"])).is_err());
        assert!(parse_args(&args(&["DEL"])).is_err());
    }

    #[test]
    fn parse_unknown_command() {
        assert!(parse_args(&args(&["NOPE"])).is_err());
        assert!(parse_args(&args(&[])).is_err());
    }

    // --- Store ---

    #[test]
    fn store_get_set_del() {
        let mut store = Store::new();
        assert_eq!(store.get("key"), None);

        store.set("key".to_string(), "val".to_string(), None);
        assert_eq!(store.get("key"), Some("val"));

        assert!(store.del("key"));
        assert_eq!(store.get("key"), None);
    }

    #[test]
    fn store_del_nonexistent() {
        let mut store = Store::new();
        assert!(!store.del("nope"));
    }

    #[test]
    fn store_overwrite() {
        let mut store = Store::new();
        store.set("k".to_string(), "v1".to_string(), None);
        store.set("k".to_string(), "v2".to_string(), None);
        assert_eq!(store.get("k"), Some("v2"));
    }

    #[test]
    fn store_cleanup_expires_old_entries() {
        let mut store = Store::new();
        store.set("keep".to_string(), "val".to_string(), None);
        store.set(
            "expire".to_string(),
            "val".to_string(),
            Some(SystemTime::now() - Duration::from_secs(1)),
        );

        store.gc();

        assert_eq!(store.get("keep"), Some("val"));
        assert_eq!(store.get("expire"), None);
    }

    #[test]
    fn store_cleanup_keeps_fresh_entries() {
        let mut store = Store::new();
        store.set(
            "fresh".to_string(),
            "val".to_string(),
            Some(SystemTime::now() + Duration::from_secs(60)),
        );
        store.gc();
        assert_eq!(store.get("fresh"), Some("val"));
    }

    #[test]
    fn wal_replay_restores_state() {
        let dir = std::env::temp_dir();
        let path = dir.join("test_wal_replay.log");

        let far_future = (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600)
            .to_string();

        let wal = [
            resp_array(&args(&["SET", "k1", "v1"])),
            resp_array(&args(&["SET", "k2", "v2", "EX", "0"])),
            resp_array(&args(&["SET", "k3", "v3", "EX", &far_future])),
            resp_array(&args(&["DEL", "k1"])),
        ]
        .join("");
        std::fs::write(&path, wal).unwrap();

        let mut store = Store::new();
        wal_replay(&path, &mut store).unwrap();
        store.gc();

        assert_eq!(store.get("k1"), None); // deleted
        assert_eq!(store.get("k2"), None); // expired
        assert_eq!(store.get("k3"), Some("v3")); // still fresh

        std::fs::remove_file(&path).ok();
    }
}
