extern crate df_forum_backend;
extern crate futures_channel;
extern crate futures_util;
extern crate tokio;
extern crate tokio_tungstenite;

static PORT: u32 = 5050;

mod connection;

#[tokio::main]
async fn main() -> Result<(), connection::HandlerError> {
    df_forum_backend::init_logger();
    let addr = "0.0.0.0:".to_owned() + &PORT.to_string();

    connection::establish(addr).await
}
