use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

fn start_server() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    thread::spawn(move || {
        kv_store::run_server(listener).unwrap();
    });
    port
}

/// Encode args as a RESP array of bulk strings
fn resp_encode(args: &[&str]) -> Vec<u8> {
    let mut buf = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        buf.extend(format!("${}\r\n{}\r\n", arg.len(), arg).as_bytes());
    }
    buf
}

/// Read one RESP response, returning the raw payload (without the type prefix or trailing \r\n)
fn read_resp(reader: &mut BufReader<TcpStream>) -> String {
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    let line = line.trim_end_matches("\r\n").trim_end_matches('\n');

    match line.as_bytes().first() {
        Some(b'+') => line[1..].to_string(),
        Some(b'-') => line[1..].to_string(),
        Some(b':') => line[1..].to_string(),
        Some(b'$') => {
            let len: i64 = line[1..].parse().unwrap();
            if len < 0 {
                return "".to_string(); // null bulk string
            }
            let len = len as usize;
            let mut buf = vec![0u8; len + 2]; // +2 for \r\n
            reader.read_exact(&mut buf).unwrap();
            buf.truncate(len);
            String::from_utf8(buf).unwrap()
        }
        _ => panic!("unexpected RESP response: {}", line),
    }
}

fn send_command(stream: &mut TcpStream, reader: &mut BufReader<TcpStream>, args: &[&str]) -> String {
    stream.write_all(&resp_encode(args)).unwrap();
    stream.flush().unwrap();
    read_resp(reader)
}

fn connect(port: u16) -> (TcpStream, BufReader<TcpStream>) {
    let stream = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    let reader = BufReader::new(stream.try_clone().unwrap());
    (stream, reader)
}

#[test]
fn set_and_get() {
    let port = start_server();
    let (mut stream, mut reader) = connect(port);

    assert_eq!(send_command(&mut stream, &mut reader, &["SET", "name", "emilien"]), "OK");
    assert_eq!(send_command(&mut stream, &mut reader, &["GET", "name"]), "emilien");
}

#[test]
fn get_missing_key() {
    let port = start_server();
    let (mut stream, mut reader) = connect(port);

    assert_eq!(send_command(&mut stream, &mut reader, &["GET", "missing"]), "");
}

#[test]
fn del_key() {
    let port = start_server();
    let (mut stream, mut reader) = connect(port);

    send_command(&mut stream, &mut reader, &["SET", "key", "val"]);
    assert_eq!(send_command(&mut stream, &mut reader, &["DEL", "key"]), "1");
    assert_eq!(send_command(&mut stream, &mut reader, &["GET", "key"]), "");
}

#[test]
fn ping() {
    let port = start_server();
    let (mut stream, mut reader) = connect(port);

    assert_eq!(send_command(&mut stream, &mut reader, &["PING"]), "PONG");
}

#[test]
fn unknown_command() {
    let port = start_server();
    let (mut stream, mut reader) = connect(port);

    let resp = send_command(&mut stream, &mut reader, &["NOPE"]);
    assert!(resp.starts_with("ERR"));
}

#[test]
fn invalid_args() {
    let port = start_server();
    let (mut stream, mut reader) = connect(port);

    let resp = send_command(&mut stream, &mut reader, &["GET"]);
    assert!(resp.starts_with("ERR"));
}

#[test]
fn shared_state_across_clients() {
    let port = start_server();

    let (mut s1, mut r1) = connect(port);
    let (mut s2, mut r2) = connect(port);

    send_command(&mut s1, &mut r1, &["SET", "shared", "hello"]);
    assert_eq!(send_command(&mut s2, &mut r2, &["GET", "shared"]), "hello");
}

#[test]
fn multiple_concurrent_writers() {
    let port = start_server();

    let mut handles = vec![];
    for i in 0..10 {
        handles.push(thread::spawn(move || {
            let (mut stream, mut reader) = connect(port);
            let resp = send_command(&mut stream, &mut reader, &["SET", &format!("key{i}"), &format!("val{i}")]);
            assert_eq!(resp, "OK");
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let (mut stream, mut reader) = connect(port);
    for i in 0..10 {
        assert_eq!(
            send_command(&mut stream, &mut reader, &["GET", &format!("key{i}")]),
            format!("val{i}")
        );
    }
}
