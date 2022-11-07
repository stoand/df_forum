// #![feature(test)]
// extern crate test;
extern crate df_forum_frontend;
extern crate env_logger;
extern crate log;
extern crate serde;
extern crate serde_derive;
extern crate serde_json;

pub mod dataflows;
pub mod forum_minimal;
pub mod operators;

use std::io::Write;

pub fn init_logger() {
    env_logger::builder()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();
}
