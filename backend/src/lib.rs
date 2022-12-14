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
use std::sync::Once;

static INIT: Once = Once::new();

pub fn init_logger() {
    INIT.call_once(|| {
        env_logger::builder()
            .format(|buf, record| {
                writeln!(
                    buf,
                    "[\x1b[93m{}\x1b[0m]: {}",
                    record.level(),
                    record.args()
                )
            })
            .init();
    });
}
