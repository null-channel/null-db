use std::{
    fs::{
        File,
        write,
        copy
    },
    io::{
        self,
        Error
    }
};
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::thread;

fn main() {

    for i in 0..100 {
        thread::spawn(move || loop {
            println!("Loop:{}",i);
            let mut file = OpenOptions::new()
                .write(true)
                .append(true)
                .create(true)
                .open("file.txt")
                .unwrap();  
        
            if let Err(e) = writeln!(file,"{}:{}",i, "This should be all one line. hopefully we don't corrupt it\n".to_string()) {
                eprintln!("Couldn't write to file: {}", e);
            }
        });
    }

}
