use std::io::{BufRead, BufReader, Write};
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

fn send_command(stream: &mut TcpStream, reader: &mut BufReader<TcpStream>, cmd: &str) -> String {
    writeln!(stream, "{}", cmd).unwrap();
    stream.flush().unwrap();
    let mut response = String::new();
    reader.read_line(&mut response).unwrap();
    response.trim().to_string()
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

    assert_eq!(
        send_command(&mut stream, &mut reader, "SET name emilien"),
        "OK"
    );
    assert_eq!(
        send_command(&mut stream, &mut reader, "GET name"),
        "emilien"
    );
}

#[test]
fn get_missing_key() {
    let port = start_server();
    let (mut stream, mut reader) = connect(port);

    assert_eq!(send_command(&mut stream, &mut reader, "GET missing"), "");
}

#[test]
fn del_key() {
    let port = start_server();
    let (mut stream, mut reader) = connect(port);

    send_command(&mut stream, &mut reader, "SET key val");
    assert_eq!(send_command(&mut stream, &mut reader, "DEL key"), "OK");
    assert_eq!(send_command(&mut stream, &mut reader, "GET key"), "");
}

#[test]
fn unknown_command() {
    let port = start_server();
    let (mut stream, mut reader) = connect(port);

    let resp = send_command(&mut stream, &mut reader, "PING");
    assert!(resp.starts_with("ERR"));
}

#[test]
fn invalid_args() {
    let port = start_server();
    let (mut stream, mut reader) = connect(port);

    let resp = send_command(&mut stream, &mut reader, "GET");
    assert!(resp.starts_with("ERR"));
}

#[test]
fn shared_state_across_clients() {
    let port = start_server();

    let (mut s1, mut r1) = connect(port);
    let (mut s2, mut r2) = connect(port);

    send_command(&mut s1, &mut r1, "SET shared hello");
    assert_eq!(send_command(&mut s2, &mut r2, "GET shared"), "hello");
}

#[test]
fn multiple_concurrent_writers() {
    let port = start_server();

    let mut handles = vec![];
    for i in 0..10 {
        handles.push(thread::spawn(move || {
            let (mut stream, mut reader) = connect(port);
            let resp = send_command(&mut stream, &mut reader, &format!("SET key{i} val{i}"));
            assert_eq!(resp, "OK");
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let (mut stream, mut reader) = connect(port);
    for i in 0..10 {
        assert_eq!(
            send_command(&mut stream, &mut reader, &format!("GET key{i}")),
            format!("val{i}")
        );
    }
}
