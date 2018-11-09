ttv - create train, test, validation sets
=========================================

ttv is a command line tool for splitting large files up into chunks suitable for train/test/validation splits for machine learning. It arose from the need to split files that were too large to fit into memory to split, and the desire to do it in a clean way.

`ttv` requires Rust 2018.

Installation
------------

Build using `cargo build --release` to get a binary at `./target/release/ttv`. Copy this into your path to use it.

Usage
-----

Run `ttv --help` to get help, or infer what you can from one of these examples:

    # Split CSV file into two sets of a fixed number of rows
    $ ttv split data.csv --rows=train=9000 --rows=test=1000

    # Accepts gzipped data. Shorthand argument version. As many splits as you like!
    $ ttv split data.csv.gz --rows=train=65000,validation=15000,test=15000

    # Alternatively, specify proportion-based splits.
    $ ttv split data.csv --props=train=0.8,test=0.2

    # When using proportions, include the total rows to get a progress bar
    $ ttv split data.csv --props=train=0.8,test=0.2 --total-rows=1234

    # Accepts data from stdin, compressed or not (must give a filename)
    $ cat data.csv | ttv split --rows=test=10000,train=90000 --output-prefix data
    $ cat data.csv.gz | ttv split --rows=test=10000,train=90000 --output-prefix data

    # Using pigz for faster decompression
    $ pigz -dc data.csv.gz | ttv split --prop=test=0.1,train=0.9 --chunk-size 5000 --output-prefix data

    # Split outputs into chunks for faster writing/reading later
    $ ttv split data.csv.gz --rows=test=100000,train=900000 --chunk-size 5000

    # Write outputs uncompressed
    $ ttv split data.csv.gz --prop=test=0.5,train=0.5 --uncompressed

    # Reproducible splits using seed
    $ ttv split data.csv.gz --prop=test=0.5,train=0.5 --chunk-size 1000 --seed 5330

Development
-----------

You'll need a recent version of the Rust nightly toolchain and Cargo. Then just hack away as normal.
