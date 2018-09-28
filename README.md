ttv - train, test, validation sets for large files
==================================================

ttv is a command line tool for splitting large files up into chunks suitable for train/test/validation splits for machine learning. It arose from the need to split files that were too large to fit into memory to split, and the desire to do it in a clean way.

Currently ttv requires a nightly version of the Rust toolchain (it uses features of Rust 2018, and the `int_to_from_bytes` nightly feature). This will probably be relaxed soon.

Installation
------------

Build using `cargo build --release` to get a binary at `./target/release/ttv`. Copy this into your path to use it.

Usage
-----

Currently usage is very simple. Run `ttv --help` to get help, or infer what you can from one of these examples:

    $ ttv split <INPUT FILE> --split=train=9000 --split=test=1000
    $ ttv split <INPUT FILE> --split=train=0.65 --split=validation=0.15 --split=test=0.15 --split=some-other-split=0.05

Note that right now the input file MUST be compressed using gzip! Support for uncompressed files will be added very shortly.

Development
-----------

You'll need a recent version of the Rust nightly toolchain and Cargo. Then just hack away as normal.
