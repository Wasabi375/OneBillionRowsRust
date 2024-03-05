use std::{
    fs::File,
    io::{stdout, Read, Seek, Write},
    ops::Deref,
    path::{Path, PathBuf},
    str::{from_utf8, from_utf8_unchecked},
    thread,
};

use clap::Parser;
use crossbeam::channel::{bounded, Receiver, Sender};
use hashbrown::HashMap;

#[derive(Debug, Parser)]
struct Args {
    input: PathBuf,

    #[arg(short, long, default_value_t = 8)]
    threads: usize,
}

fn main() {
    let args = Args::parse();

    calculate(args, stdout());
}

#[derive(Debug, Default)]
struct CityEntry {
    min: f32,
    max: f32,
    sum: f32,
    count: usize,
}

const BLOCK_SIZE: usize = 4096;
struct StrBuffer {
    raw_data: Box<[u8; BLOCK_SIZE]>,
    size: usize,
}

impl StrBuffer {
    fn read_from<R: Read + Seek>(read: &mut R) -> Option<Self> {
        let mut raw_data = Box::new([0u8; BLOCK_SIZE]);

        let full_size = read
            .read(raw_data.as_mut())
            .expect("Failed to read TextChunk");

        if full_size == 0 {
            return None;
        }

        let raw_data_slice = &raw_data[0..full_size];
        let last_nl = full_size - 1 - raw_data_slice.iter().rev().position(|&c| c == b'\n')
            .expect("TextChunk must contain at least 1 nl.
                    This is an implementation specific requirement and not part of the challenge spec");
        assert_eq!(raw_data[last_nl], b'\n');

        let str_data_slice = &raw_data[0..=last_nl];
        assert_eq!(*str_data_slice.last().unwrap(), b'\n');

        from_utf8(str_data_slice).expect("Expected utf8 data");
        read.seek(std::io::SeekFrom::Current(
            -((full_size - last_nl - 1) as i64),
        ))
        .expect("Seek to after last nl failed");

        Some(StrBuffer {
            raw_data,
            size: last_nl + 1,
        })
    }
}

impl Deref for StrBuffer {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        let chunk = &self.raw_data[0..self.size];
        // Safety: we check that this is a valid utf8 str when we create the TextChunk
        unsafe { from_utf8_unchecked(chunk) }
    }
}

fn produce_text_chunks(in_path: &Path, sender: Sender<StrBuffer>) {
    let mut file = File::open(in_path).expect("could not open input file");

    loop {
        match StrBuffer::read_from(&mut file) {
            Some(chunk) => sender.send(chunk).expect("Failed to send TextChunk"),
            None => break,
        }
    }
}

fn process_lines(chunks: Receiver<StrBuffer>) -> HashMap<String, CityEntry> {
    let mut result = HashMap::<String, CityEntry>::new();
    loop {
        let chunk = match chunks.recv() {
            Ok(chunk) => chunk,
            Err(_) => break,
        };

        for line in chunk.lines() {
            let mut parts = line.split(';');
            let city = parts.next().expect("Expected city name");
            let value = &parts.next().expect("Expected value");
            let value: f32 = value.parse().expect("expected float value");

            // TODO switch to hashbrown maps and use raw-entry api
            // this crate is the implementation in the std-lib, but provides access to nightly
            // features (without nightly) such as the raw entry api as well as the inline-more
            // feature-flag that should improve performance but reduce compilation speed
            if !result.contains_key(city) {
                result.insert(city.to_string(), CityEntry::default());
            }

            let entry = result.get_mut(city).unwrap();
            entry.sum += value;
            entry.count += 1;
            if entry.max < value {
                entry.max = value;
            }
            if entry.min > value {
                entry.min = value;
            }
        }
    }
    result
}

fn calculate<OWrite: Write>(args: Args, mut output: OWrite) {
    let (chunk_sender, chunk_receiver) = bounded(10);

    thread::spawn(move || produce_text_chunks(&args.input, chunk_sender));

    let mut partial_result_handles = Vec::with_capacity(args.threads);
    for _ in 0..args.threads {
        let chunk_receiver = chunk_receiver.clone();
        let handle = thread::spawn(move || process_lines(chunk_receiver));
        partial_result_handles.push(handle);
    }

    let result = combine_results(partial_result_handles);

    write!(output, "{{").expect("failed to write output");

    let mut cities: Vec<_> = result.keys().collect();
    cities.sort_unstable();
    let mut first = true;
    for name in cities {
        if !first {
            write!(output, ", ").expect("failed to write output");
        } else {
            first = false;
        }
        let city = &result[name];
        write!(
            output,
            "{}={:.1}/{:.1}/{:.1}",
            name,
            city.min,
            city.sum / city.count as f32,
            city.max
        )
        .expect("failed to write output");
    }
    write!(output, "}}").expect("failed to write output");
}

fn combine_results(
    partial_result_handles: Vec<thread::JoinHandle<HashMap<String, CityEntry>>>,
) -> HashMap<String, CityEntry> {
    let mut result = HashMap::new();
    for handle in partial_result_handles {
        let partial = match handle.join() {
            Ok(p) => p,
            Err(_) => panic!("process lines failed"),
        };

        for (p_city, p_data) in partial.into_iter() {
            result
                .entry(p_city)
                .and_modify(|full_data: &mut CityEntry| {
                    full_data.sum += p_data.sum;
                    full_data.count += p_data.count;
                    if full_data.min > p_data.min {
                        full_data.min = p_data.min;
                    }
                    if full_data.max < p_data.max {
                        full_data.max = p_data.max;
                    }
                })
                .or_insert(p_data);
        }
    }
    result
}

#[cfg(test)]
mod test {
    use std::{
        fs::File,
        io::{Read, Seek},
        path::PathBuf,
        str::from_utf8,
    };

    use crate::{calculate, Args, StrBuffer};

    fn check(in_path: PathBuf, expected_path: PathBuf) {
        let mut result = Vec::new();

        let args = Args {
            input: in_path,
            threads: 1,
        };

        calculate(args, &mut result);

        let mut expected = Vec::new();
        let mut file = File::open(expected_path).unwrap();
        file.read_to_end(&mut expected).unwrap();

        assert_eq!(from_utf8(&expected).unwrap(), from_utf8(&result).unwrap());
    }

    #[test]
    fn check_against_test_data() {
        check(
            PathBuf::from("data/test.txt"),
            PathBuf::from("data/test_res.txt"),
        );
    }

    #[test]
    #[ignore]
    fn check_against_full_data() {
        check(
            PathBuf::from("data/all_cities.txt"),
            PathBuf::from("data/all_cities_res.txt"),
        );
    }

    #[test]
    #[ignore]
    fn check_against_cities400_data() {
        check(
            PathBuf::from("data/cities_400.txt"),
            PathBuf::from("data/cities_400_res.txt"),
        );
    }

    #[test]
    fn check_read_str_buffer() {
        let mut file =
            File::open(PathBuf::from("data/test.txt")).expect("could not open input file");
        let mut full_data = String::new();
        loop {
            match StrBuffer::read_from(&mut file) {
                Some(chunk) => full_data.push_str(&chunk),
                None => break,
            }
        }

        file.seek(std::io::SeekFrom::Start(0)).unwrap();
        let mut expected = String::new();
        file.read_to_string(&mut expected).unwrap();

        assert_eq!(expected, full_data);
    }

    #[test]
    fn check_read_single_str_buffer() {
        let mut file =
            File::open(PathBuf::from("data/all_cities.txt")).expect("could not open input file");
        let _chunk = StrBuffer::read_from(&mut file).unwrap();
        let mut b = [0u8];
        file.read(&mut b).unwrap();
        assert_ne!(b[0], b'\n');
    }
}
