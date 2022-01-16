use std::thread;
use std::time::Duration;
use std::sync::mpsc;
use std::thread;
use std::collections::HashMap;
use std::collections::HashSet;

fn start_compaction() {

    let (tx, rx) = mpsc::channel();
    thread::spawn(move || loop {
        println!("Suspending...");
        match rx.recv() {
            Ok(_) => {
                println!("compacting!!!");
                thread::sleep(Duration::from_millis(5000));
            }
            Err(_) => {
                println!("Terminating.");
                break;
            }
        }
    });

}

fn compactor() {
    let paths = fs::read_dir("./").unwrap();

    let dbfiles = paths.flat_map(|x| {
        if get_extension_from_filename(x) == Some("nullpack") {
            return Some(x);
        }
        return None;
    })
    .collect::<Vec<String>>()
    .sort();

    let mut data_set = HashSet::new();

    for file_path in dbfiles {

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file_path)
            .expect("db pack file doesn't exist.");


        reader.build_index();
        reader.eof();
        while let Some(line) = reader.prev_line().unwrap() {
            
        }
        let lines = BufReader::new(file).lines();
        for line in lines {
            let split = line.split(":").collect::<Vec<&str>>();
                if split.len() == 2 {
                    if split[0] == key {

                    }
                }
            }
            data_set.insert(line)
        }
        if data_set.len() > 64 {

        }
        .collect::<Vec<String>>().join("\n");

    }
 
}

fn get_extension_from_filename(filename: &str) -> Option<&str> {
    Path::new(filename)
        .extension()
        .and_then(OsStr::to_str)
}