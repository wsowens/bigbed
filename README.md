# bigbed - a bigbed crate for Rust

## Background
The BigBed file format is the binary, randomly accessible of the popular BED file format.
The BED-like files are tab-delimited files that store genomic data in the following manner:
```
chrom   start   stop
chr1    100     1000
chr1    3000    3200
chr2    200     230
```
For more information on these file formats, consult [the UCSC Genome Browser's page on the subject](https://genome.ucsc.edu/FAQ/FAQformat.html#format1).

For more information on the BigBed format, please refer to the 2010 paper from Kent et al., *BigWig and BigBed: enabling browsing of large distributed datasets* (doi: [10.1093/bioinformatics/btq351](https://dx.doi.org/10.1093%2Fbioinformatics%2Fbtq351)).

This repository contains code for both a Rust crate for dealing with BigBed files and a cross-platform version of `bigBedToBed`-the utility provided by Kent et al. that converts BigBed files to BED files.

## Requirements
This project relies on functions in the Rust `std` crate version 1.32 or higher.
(You can check your version of rustc / cargo with `cargo --version`.)
If you have not updated to at least version 1.32, run `rustup` like so:
```
rustup update
```
If you did not install Rust through rustup (i.e. you installed Rust via a package manager), I recommend removing your current installation and installing via `rustup`.
If you don't have `rustup`, you can consult [the Rust website](https://www.rust-lang.org/tools/install).

### Note to Windows Users
Windows users may experience an error along the lines of

> error: linker `link.exe` not found

If that is the case, make sure that you have all the correct build tools installed. 
(Consult this issue: [https://github.com/rust-lang/rust/issues/43039]() for more instructions.)


## Getting Started

This crate features two main components:
- a library for manipulating BigBed files
- an example binary (`rbb`) that replicates the functionality of UCSC's `bigBedToBed`

### Building the library
To build the `bigbed` library with full optimizations, run the following command:
```
cargo build --release
```
All library files will be located in `target/release`.

### Building the example binary
To build the example binary (with full optimizations), run the following command:
```
cargo build --features binary --release --bin rbb
```
The `rbb` binary will be available in `target/release/rbb`.
(Note: Windows users may find the executable named `rbb.exe` instead of just `rbb`.)

## Testing
### Testing with built-in testcases
This crate includes built-in testcases to ensure that all functions are running properly.
To run these testcases, execute the following command:
```
cargo test
```

### Testing wth provided test files
This repository includes several corresponding BED and bigbed files.
For instance, the files `test/beds/long.bed` and `test/bigbeds/long.bb` contain the same data, but in BED and BigBed formats, respectively.

To run a manual test, execute the following commands:
```sh
# build the binary and run on long.bed
cargo run --features=binary test/bigbeds/long.bb test-long.bed

# compare test output to expected output
# this should produce no output if the program works
diff -q test-long.bed test/beds/long.bed
```

You can repeat this process with other provided files in `test/` or with files of your own. 

## License

This crate is licensed under GPL-3.0.
Please see the [LICENSE](./LICENSE) for details.
