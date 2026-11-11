//! Helpers shared by the integration tests that start a real `rocky serve`.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

/// Whether `rocky serve` on `port` answers `GET /api/v1/health` with `200`.
///
/// Never panics. A readiness loop must treat every failure as "not up yet".
/// The weekly coverage run on 2026-11-09 failed in a loop that did not: its
/// bare `TcpStream::connect` probe succeeded, then the health request's own
/// connect was refused and panicked. One way a loopback connect succeeds
/// with no server listening is a self-connect, when the ephemeral source
/// port the OS picks equals the target port. Only a real HTTP `200` status
/// line counts as ready here, so no such connect can end the wait.
pub fn serve_health_ok(port: u16) -> bool {
    let Ok(mut stream) = TcpStream::connect(("127.0.0.1", port)) else {
        return false;
    };
    if stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .is_err()
    {
        return false;
    }
    if stream
        .write_all(
            format!("GET /api/v1/health HTTP/1.0\r\nHost: 127.0.0.1:{port}\r\n\r\n").as_bytes(),
        )
        .is_err()
    {
        return false;
    }
    let mut raw = Vec::new();
    if stream.read_to_end(&mut raw).is_err() {
        return false;
    }
    let text = String::from_utf8_lossy(&raw);
    let status = text.lines().next().unwrap_or("");
    status.starts_with("HTTP/") && status.contains(" 200")
}
