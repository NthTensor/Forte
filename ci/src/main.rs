//! CI script used for Forte.
//!
//! Copied from the bevy CI tool.

mod ci;
mod commands;
mod prepare;

pub use self::ci::*;
pub use self::prepare::*;

fn main() {
    argh::from_env::<CI>().run();
}
