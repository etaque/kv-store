use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
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

fn parse_wal_command(line: &str) -> Result<Command, KvError> {
    let mut parts = line.split_whitespace();
    match parts.next() {
        Some("SET") => match (parts.next(), parts.next()) {
            (Some(key), Some(value)) => {
                let ttl = match parts.next() {
                    Some(ts) => ts
                        .parse()
                        .map(Some)
                        .map_err(|_| KvError::Parse(format!("invalid TTL: {}", ts)))?,
                    None => None,
                };
                Ok(Command::Set {
                    key: key.to_string(),
                    value: value.to_string(),
                    ttl,
                })
            }
            _ => Err(KvError::WalParse(format!("malformed SET: {}", line))),
        },
        Some("DEL") => match parts.next() {
            Some(key) => Ok(Command::Del {
                key: key.to_string(),
            }),
            None => Err(KvError::WalParse(format!("malformed DEL: {}", line))),
        },
        _ => Err(KvError::WalParse(format!("unknown command: {}", line))),
    }
}

fn handle_client(
    stream: TcpStream,
    store: Arc<Mutex<Store>>,
    wal_path: &Path,
) -> Result<(), KvError> {
    let mut writer = stream.try_clone()?;
    let reader = BufReader::new(stream);

    for line in reader.lines() {
        let line = line?;
        let response = match parse_command(&line) {
            Ok(Command::Set { key, value, ttl }) => {
                let expires_at = ttl.map(|ttl| SystemTime::now() + Duration::from_secs(ttl));
                store.lock().unwrap().set(key, value, expires_at);
                wal_append(wal_path, &line)?;
                "OK".to_string()
            }
            Ok(Command::Get { key }) => match store.lock().unwrap().get(&key) {
                Some(value) => value.to_string(),
                None => "".to_string(),
            },
            Ok(Command::Del { key }) => {
                store.lock().unwrap().del(&key);
                wal_append(wal_path, &line)?;
                "OK".to_string()
            }
            Err(KvError::Parse(msg)) => format!("ERR {}", msg),
            Err(_) => "ERR".to_string(),
        };
        writeln!(writer, "{}", response)?;
        writer.flush()?;
    }
    Ok(())
}

fn parse_command(input: &str) -> Result<Command, KvError> {
    let mut parts = input.split_whitespace();
    match parts.next() {
        Some("GET") => match parts.next() {
            Some(key) => Ok(Command::Get {
                key: key.to_string(),
            }),
            None => Err(KvError::Parse("invalid GET command: GET key".to_string())),
        },
        Some("SET") => match (parts.next(), parts.next()) {
            (Some(key), Some(value)) => {
                let ttl: Option<u64> = match (parts.next(), parts.next()) {
                    (Some("EX"), None) => Err(KvError::Parse("missing TTL value".to_string()))?,
                    (Some("EX"), Some(secs)) => secs
                        .parse()
                        .map(Some)
                        .map_err(|_| KvError::Parse(format!("invalid TTL: {}", secs)))?,
                    (None, None) => None,
                    _ => Err(KvError::Parse("unexpected argument".to_string()))?,
                };
                Ok(Command::Set {
                    key: key.to_string(),
                    value: value.to_string(),
                    ttl,
                })
            }
            _ => Err(KvError::Parse(
                "invalid SET command: SET key value [EX ttl]".to_string(),
            )),
        },
        Some("DEL") => match parts.next() {
            Some(key) => Ok(Command::Del {
                key: key.to_string(),
            }),
            _ => Err(KvError::Parse("invalid DEL command: DEL key".to_string())),
        },
        _ => Err(KvError::Parse("unknown command".to_string())),
    }
}

fn wal_replay(path: &Path, store: &mut Store) -> Result<(), KvError> {
    if !path.exists() {
        return Ok(());
    }
    let reader = BufReader::new(File::open(path)?);
    for (i, line) in reader.lines().enumerate() {
        let line = line?;
        match parse_wal_command(&line) {
            Ok(Command::Set { key, value, ttl }) => {
                let expires_at = ttl.map(|ts| UNIX_EPOCH + Duration::from_secs(ts));
                store.set(key, value, expires_at);
            }
            Ok(Command::Del { key }) => {
                store.del(&key);
            }
            Ok(_) => {
                eprintln!("WAL line {}: unexpected command, skipping", i + 1);
            }
            Err(e) => {
                eprintln!("WAL line {}: {}", i + 1, e);
            }
        }
    }
    Ok(())
}

fn wal_append(path: &Path, line: &str) -> Result<(), KvError> {
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    writeln!(file, "{}", line)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- parse_command ---

    #[test]
    fn parse_get() {
        assert_eq!(
            parse_command("GET foo").unwrap(),
            Command::Get {
                key: "foo".to_string()
            }
        );
    }

    #[test]
    fn parse_set() {
        assert_eq!(
            parse_command("SET foo bar").unwrap(),
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
            parse_command("SET foo bar EX 30").unwrap(),
            Command::Set {
                key: "foo".to_string(),
                value: "bar".to_string(),
                ttl: Some(30)
            }
        );
    }

    #[test]
    fn parse_set_with_invalid_ttl() {
        assert!(parse_command("SET foo bar EX notnum").is_err());
    }

    #[test]
    fn parse_set_with_missing_ttl() {
        assert!(parse_command("SET foo bar EX").is_err());
    }

    #[test]
    fn parse_del() {
        assert_eq!(
            parse_command("DEL foo").unwrap(),
            Command::Del {
                key: "foo".to_string()
            }
        );
    }

    #[test]
    fn parse_missing_args() {
        assert!(parse_command("GET").is_err());
        assert!(parse_command("SET").is_err());
        assert!(parse_command("SET foo").is_err());
        assert!(parse_command("DEL").is_err());
    }

    #[test]
    fn parse_unknown_command() {
        assert!(parse_command("PING").is_err());
        assert!(parse_command("").is_err());
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

        let far_future = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600;

        let wal = format!("SET k1 v1\nSET k2 v2 0\nSET k3 v3 {far_future}\nDEL k1\n");
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
