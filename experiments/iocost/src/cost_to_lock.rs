use std::error::Error;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::fs::OpenOptions;
use std::io;
use std::time::{SystemTime, UNIX_EPOCH};



fn old_test() {
    let itterations = 100;
    let start = SystemTime::now();
    for x in 0..itterations {
        copy_main_segment().unwrap();
    }
    copy_main_segment().unwrap();
    let duration = start.elapsed().unwrap();
    println!("Time elapsed in copilot_function() is: {:?}", duration);

    let start2 = SystemTime::now();
    for x in 0..itterations {
        copy_main_segment_human().unwrap();
    }
    let duration2 = start2.elapsed().unwrap();
    println!("Time elapsed in marek_function() is: {:?}", duration2);
}

extern crate test;

// IO Time
// How long does it take to copy the main segment?
fn copy_main_segment() -> Result<(), Box<dyn Error>> {
    let mut reader = BufReader::new(File::open("null.database")?);
    let mut writer = BufWriter::new(File::create("null.database.compacted")?);
    let mut buffer = [0; 4048];
    loop {
        let bytes_read = reader.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        writer.write_all(&buffer[..bytes_read])?;
    }
    Ok(())
}

fn copy_main_segment_human() -> Result<(), Box<dyn Error>> {
    std::fs::copy("null.database", "null.database.compacted").unwrap();
    Ok(())
}

pub fn add_two(a: i32) -> i32 {
    a + 2
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use test::Bencher;
    use std::sync::RwLock;

    #[bench]
    fn bench_lock_write(b: &mut Bencher) {

        let mut lock = RwLock::new(0);
        b.iter(|| {
            let mut guard = lock.write().unwrap();
        });
    }
    #[bench]
    fn bench_lock_read(b: &mut Bencher) {

        let mut lock = RwLock::new(0);
        b.iter(|| {
            let mut guard = lock.read().unwrap();
        });
    }
    #[bench]
    fn bench_do_nothing(b: &mut Bencher) {

        b.iter(|| {
        });
    }

}