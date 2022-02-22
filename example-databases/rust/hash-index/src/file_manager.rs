use bytes::{Bytes, BytesMut, BufMut};
use std::{
    fs::File,
    io::{
        self,
        Error
    }
};

pub struct FileManager {
    pub byte_offset: u64,
    file: String,
}

impl FileManager {
    pub fn new(mut file: String) -> Result<Self, Error> {
        Ok(FileManager {
            byte_offset: 0,
            file: file,
        })
    }

    // Writes data to disk. returns offset if sucsess
    pub fn write_data(key: String, data: Bytes) -> Result<u64,Error> {
        return Err(Error::last_os_error());
    }

    // returns data at offset
    pub fn get_data(index: u64) -> Result<Bytes, Error> {
        let mut buf = BytesMut::with_capacity(1024);
        buf.put(&b"not implemented"[..]);
        return Ok(buf.freeze());
    }
}