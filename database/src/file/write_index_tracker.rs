// Everything related to the write index tracker
// must be thread-safe, as it will be shared between
// multiple threads.
//
// YOU MUST NOT USE LOCKS OR MUTEXES.

use std::{
    f64::consts::E,
    fs::File,
    path::PathBuf,
    sync::atomic::{AtomicBool, AtomicU64},
};

use actix_web::error::ErrorGone;
use tokio::runtime::TryCurrentError;

// https://users.rust-lang.org/t/struct-contains-a-u32-u64-guarantees-that-struct-is-aligned-to-32-64-bits/25548/2
#[repr(align(8))]
pub struct WriteIndexTracker {
    //TODO: deal with the main file
    file_path_ring_buffer: Vec<PathBuf>,
    working_directory: PathBuf,
    buffer_size: usize,

    // United File and byte index
    indexes: AtomicU64,
    atomic_lock: AtomicBool,
}

// If we need to store more then a u32, we can manually bit shift the values
// Align 8 so we can actually use bit shifting magic
#[repr(C, align(8))]
union Indexes {
    value: u64,
    // file, byte offset
    parts: (u32, u8),
}

impl Indexes {
    fn new(value: u64) -> Self {
        Indexes { value }
    }

    fn file_index(&self) -> u8 {
        unsafe { self.parts.1 }
    }

    fn byte_index_add(&mut self, value: u32) {
        unsafe {
            self.parts.0 += value;
        }
    }

    fn byte_index(&self) -> u32 {
        unsafe { self.parts.0 }
    }

    fn parse_from_u64(value: u64) -> Self {
        Indexes { value }
    }
}

pub struct IndexResult {
    pub index: usize,
    pub file: PathBuf,
}

fn shift_file_index(value: u8) -> u64 {
    (value >> 32) as u64
}

impl WriteIndexTracker {
    /// new creates a new WriteIndexTracker.
    pub fn new(working_directory: PathBuf) -> Self {
        WriteIndexTracker {
            file_path_ring_buffer: Vec::new(),
            working_directory,
            buffer_size: 10,
            indexes: AtomicU64::new(0),
        }
    }

    pub fn get_write_index(&self, size: usize) -> IndexResult {
        let indexes = Indexes::new(
            self.indexes
                .fetch_add(shift_file_index(size), std::sync::atomic::Ordering::SeqCst),
        );

        let file = self.file_path_ring_buffer[indexes.file_index() as usize].clone();
        let index = indexes.byte_index() as usize;
        IndexResult { index, file }
    }

    fn next_segment(&mut self) -> Result<(), WriteIndexTrackerError> {
        //is this horrifying?
        while self
            .atomic_lock
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::Acquire,
                std::sync::atomic::Ordering::Relaxed,
            )
            .is_err()
        {}

        let mut current_index = self.indexes.load(std::sync::atomic::Ordering::Relaxed);
        let mut new_index = current_index + 1;

        if new_index >= self.buffer_size {
            new_index = 0;
        }

        self.current_file_index.compare_exchange(
            current_index,
            new_index,
            std::sync::atomic::Ordering::SeqCst,
            std::sync::atomic::Ordering::SeqCst,
        );

        return Err(WriteIndexTrackerError::AlreadyDone);

        let mut next_index = self
            .current_file_index
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;

        let next_file = create_next_segment_file(&self.working_directory)?;

        self.file_path_ring_buffer[next_index] = next_file;

        //TODO: Can I release sooner?
        self.atomic_lock
            .store(false, std::sync::atomic::Ordering::Release);

        Ok(())
    }
}

fn create_next_segment_file(working_directory: &PathBuf) -> Result<PathBuf, io::Error> {
    let time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("now is before epoch")
        .as_millis();
    let seg_file = working_directory.join(format!("0-{time}.{LOG_SEGMENT_EXT}"));
    let _file = File::create(&seg_file)?;
    Ok(seg_file)
}

pub enum WriteIndexTrackerError {
    FileOutOfSpace,
    AlreadyDone,
}
