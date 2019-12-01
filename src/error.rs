use std::io;
use std::fmt;

#[derive(Debug)]
pub struct IOErrorWrapper(io::Error);

impl PartialEq for IOErrorWrapper {
    fn eq(&self, _other: &IOErrorWrapper) -> bool {
        false
    }
    fn ne(&self, _other: &IOErrorWrapper) -> bool {
        true
    }
}

#[derive(Debug, PartialEq)]
pub enum Error {
    IOError(IOErrorWrapper),
    DecompressError,
    BadSig{expected: [u8; 4], received: [u8; 4]},
    BadChrom(String),
    BadKey(String, usize),
    ConversionError(std::num::TryFromIntError),
    Misc(&'static str)
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Error {
        Error::IOError(IOErrorWrapper(e))
    }
}

impl From<&'static str> for Error {
    fn from(e: &'static str) -> Error {
        Error::Misc(e)
    }
}

impl From<flate2::DecompressError> for Error {
    fn from(_e: flate2::DecompressError) -> Error {
        Error::DecompressError
    }
}

impl From<std::num::TryFromIntError> for Error {
    fn from(e: std::num::TryFromIntError) -> Error {
        Error::ConversionError(e)
    }
}

impl fmt::Display for Error {
    
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::IOError(wrapped_io_err) => write!(f, "IOError: {}", wrapped_io_err.0),
            Error::DecompressError => write!(f, "Decompression error!"),
            Error::BadSig{expected, received} => write!(f, "Bad file signature. Expected \"{:?}\", Received \"{:?}\" ", expected, received),
            Error::BadChrom(chr) => write!(f, "Chromosome \"{}\" not found", chr),
            Error::BadKey(key, size) => write!(f, "Chromosome \"{}\" not found (Exceeds max key size: {})", key, size),
            Error::ConversionError(convert_err) => write!(f, "{}", convert_err),
            Error::Misc(msg) => write!(f, "{}", msg),
        }
    }
}