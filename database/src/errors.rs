use std::io::Error;

pub enum NullDbReadError {
    ValueNotFound,
    ValueDeleted,
    IOError(Error),
}