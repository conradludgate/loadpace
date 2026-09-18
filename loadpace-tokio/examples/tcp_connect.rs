//! Connect to the cheaper of two destinations, with a local dispatch/IO deadline.
//! Run with: cargo run -p loadpace-tokio --example tcp_connect -- ADDR1 ADDR2

use loadpace::{Completion, EndpointConfig, PairChoice};
use loadpace_tokio::Endpoint;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::timeout;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addresses = std::env::args()
        .skip(1)
        .map(|address| address.parse())
        .collect::<Result<Vec<SocketAddr>, _>>()?;
    let [first_address, second_address] = addresses.as_slice() else {
        return Err(io::Error::other("supply exactly two socket addresses").into());
    };
    let config = EndpointConfig::new(Duration::from_millis(20), 32);
    // A long-lived client retains these handles and reuses them across attempts.
    let first = Endpoint::new(config.clone());
    let second = Endpoint::new(config);
    let (choice, reservation) = first
        .try_reserve_pair(&second)
        .map_err(|error| io::Error::other(format!("admission rejected: {error:?}")))?;
    let address = match choice {
        PairChoice::First => first_address,
        PairChoice::Second => second_address,
    };
    let stream = timeout(
        Duration::from_secs(2),
        reservation.run(
            || TcpStream::connect(address),
            |result| match result {
                Ok(_) => Completion::Success,
                Err(error) if error.kind() == io::ErrorKind::TimedOut => Completion::Abandoned,
                Err(_) => Completion::Failure,
            },
        ),
    )
    .await??;
    println!("connected to {}", stream.peer_addr()?);
    Ok(())
}
