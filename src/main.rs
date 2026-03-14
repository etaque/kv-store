use std::net::TcpListener;

fn main() -> Result<(), kv_store::KvError> {
    let listener = TcpListener::bind("127.0.0.1:6379")?;
    kv_store::run_server(listener)
}
