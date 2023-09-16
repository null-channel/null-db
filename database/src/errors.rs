use std::io::Error;

pub enum NullDbReadError {
    ValueNotFound,
    ValueDeleted,
    IOError(Error),
    Corrupted,
}

impl std::fmt::Display for NullDbReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            _ => write!(f, "This is a terrible error, fix me please")
        }
    }
}
