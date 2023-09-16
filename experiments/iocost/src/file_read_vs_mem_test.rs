

extern crate test;
use rand::distributions::{Alphanumeric};
use rand::{thread_rng, Rng};
use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;
use std::io::Write;

pub fn add_two(a: i32) -> i32 {
    a + 2
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, fs::{File, OpenOptions}, io};

    use super::*;
    use test::Bencher;

    #[bench]
    fn bench_read_memory_from_hashmap(b: &mut Bencher) {

        let mut data = HashMap::new();
        data.insert("aa".to_owned(), "aaaaaaaaaa".to_owned());
        for x in 0..100000 {
            data.insert(get_random_string(2), get_random_string(10));
        }
        b.iter(|| data.get("aa"));
    }


    #[bench]
    fn bench_read_disk_no_search(b: &mut Bencher) {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open("./testfile".to_owned()).unwrap();

        
        for x in 0..50000 {
            writeln!(file, "{}", get_random_string(10));
        }

        writeln!(file, "{}", "aaaaaaaaaa");

        for x in 0..50000 {
            writeln!(file, "{}", get_random_string(10));
        }
        b.iter(|| {
            if let Ok(_) = read_lines("./testfile") {
                // Consumes the iterator, returns an (Optional) String
                return;
            }

        });
    }

    #[bench]
    fn bench_read_disk_from_file(b: &mut Bencher) {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open("./testfile".to_owned()).unwrap();

        
        for x in 0..50000 {
            writeln!(file, "{}", get_random_string(10));
        }

        writeln!(file, "{}", "aaaaaaaaaa");

        for x in 0..50000 {
            writeln!(file, "{}", get_random_string(10));
        }
        b.iter(|| {
            if let Ok(lines) = read_lines("./testfile") {
                // Consumes the iterator, returns an (Optional) String
                for line in lines {
                    if let Ok(line_value) = line {
                        if line_value == "aaaaaaaaaa" {
                            return;
                        }
                    }
                }
            }
        });
    }

    fn get_random_string(length: usize) -> String {
        let chars: Vec<u8> = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(length)
            .collect();
        let s = std::str::from_utf8(&chars).unwrap().to_string();
        return s;
    }

    // The output is wrapped in a Result to allow matching on errors
    // Returns an Iterator to the Reader of the lines of the file.
    fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
    where P: AsRef<Path>, {
        let file = File::open(filename)?;
        Ok(io::BufReader::new(file).lines())
    }
}