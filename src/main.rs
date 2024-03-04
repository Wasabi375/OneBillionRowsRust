use std::{
    collections::HashMap,
    fs::File,
    io::{stdout, Read, Write},
    path::{Path, PathBuf},
    thread,
};

use clap::Parser;
use crossbeam::channel::{bounded, Receiver, Sender};

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

fn produce_lines(in_path: &Path, sender: Sender<Box<str>>) {
    let mut file = File::open(in_path).expect("could not open input file");

    let mut text_data = String::new();
    file.read_to_string(&mut text_data)
        .expect("input is not valid utf8");
    for line in text_data.lines() {
        sender.send(line.into()).expect("send line failed");
    }
}

fn process_lines(lines: Receiver<Box<str>>) -> HashMap<String, CityEntry> {
    let mut result = HashMap::<String, CityEntry>::new();
    loop {
        let line = match lines.recv() {
            Ok(line) => line,
            Err(_) => break,
        };

        let mut parts = line.split(';');
        let city = parts.next().expect("Expected city name");
        let value: f32 = parts
            .next()
            .expect("Expected value")
            .parse()
            .expect("expected float value");

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
    result
}

fn calculate<OWrite: Write>(args: Args, mut output: OWrite) {
    let (line_sender, line_receiver) = bounded(100);

    thread::spawn(move || produce_lines(&args.input, line_sender));

    let mut partial_result_handles = Vec::with_capacity(args.threads);
    for _ in 0..args.threads {
        let line_receiver = line_receiver.clone();
        let handle = thread::spawn(move || process_lines(line_receiver));
        partial_result_handles.push(handle);
    }

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

#[cfg(test)]
mod test {
    use std::{fs::File, io::Read, path::PathBuf};

    use crate::{calculate, Args};

    fn check(in_path: PathBuf, expected_path: PathBuf) {
        let mut result = Vec::new();

        let args = Args {
            input: in_path,
            threads: 4,
        };

        calculate(args, &mut result);

        let mut expected = Vec::new();
        let mut file = File::open(expected_path).unwrap();
        file.read_to_end(&mut expected).unwrap();

        assert_eq!(expected, result);
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
}
