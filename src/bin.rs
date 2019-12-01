#[macro_use]
extern crate clap;
extern crate bigbed;
mod error;

use clap::{App, Arg, crate_version};
use crate::bigbed::BigBed;
use crate::bigbed::error::Error::*;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Write};
use std::process::exit;

fn main() {
    let matches = App::new("rbb")
        .version(crate_version!())
        .arg(
            Arg::with_name("input.bb")
                .help("BigBed file to convert")
                .index(1)
                .required(true)
        )
        .arg(
            Arg::with_name("output.bed")
                .help("Path for output BED file")
                .index(2)
        )
        .get_matches();
    
    let output: BufWriter<Box<dyn Write>> = BufWriter::new(
        match matches.value_of("output.bb") {
            None => Box::new(io::stdout()),
            Some(name) => {
                match File::create(name) {
                    Err(err) => {
                        eprintln!("{}", err);
                        exit(1);
                    },
                    Ok(file) => {
                        Box::new(file)
                    }
                }
            }
        }
    );
    // this will always work, since input is required arg
    let filename = matches.value_of("input.bb").unwrap();
    match File::open(filename) {
        Err(err) => {
            eprintln!("{}", err);
            eprintln!("Could not open file: {}", filename);
        }
        Ok(file) => {
            let result = BigBed::from_file(BufReader::new(file));
            match result {
                Ok(mut bigbed) => {
                    let result = bigbed.to_bed(None, None, None, None, output);
                    if let Err(err) = result {
                        eprintln!("{}", err);
                        // provide helpful follow-ups on specific errors
                        match err {
                            BadChrom(chr) | BadKey(chr, _) => {
                                eprintln!("This chromosome ('{}') may not be in the file.", chr);
                            }
                            _ => {}
                        }
                    }
                }
                Err(err) => {
                    // provide helpful follow-ups on specific errors
                    match err {
                        IOError(_) => {
                            eprintln!("Could not open file '{}' due to the following error:\n{}.", filename, err);
                        }
                        BadSig{expected, received} => {
                            eprintln!("{}", err);
                            eprintln!("Is '{}' a BigBed file?", filename);
                        }
                        _ => {
                            eprintln!("{}", err)
                        }
                    }
                }
            }
        }
    }
}