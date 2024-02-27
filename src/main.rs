use std::{
    collections::HashMap,
    fs::File,
    io::{stdout, Read, Write},
    path::{Path, PathBuf},
};

use clap::Parser;

#[derive(Debug, Parser)]
struct Args {
    input: PathBuf,
}

fn main() {
    let args = Args::parse();

    calculate(&args.input, stdout());
}

#[derive(Debug, Default)]
struct CityEntry {
    min: f32,
    max: f32,
    sum: f32,
    count: usize,
}

fn calculate<OWrite: Write>(in_path: &Path, mut output: OWrite) {
    let mut file = File::open(in_path).expect("could not open input file");

    let mut result = HashMap::<&str, CityEntry>::new();

    let mut text_data = String::new();
    file.read_to_string(&mut text_data)
        .expect("input is not valid utf8");
    for line in text_data.lines() {
        let mut parts = line.split(';');
        let city = parts.next().expect("Expected city name");
        let value: f32 = parts
            .next()
            .expect("Expected value")
            .parse()
            .expect("expected float value");

        let entry = result.entry(city).or_default();
        entry.sum += value;
        entry.count += 1;
        if entry.max < value {
            entry.max = value;
        }
        if entry.min > value {
            entry.min = value;
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

    use crate::calculate;

    fn check(in_path: &PathBuf, expected_path: &PathBuf) {
        let mut result = Vec::new();

        calculate(in_path, &mut result);

        let mut expected = Vec::new();
        let mut file = File::open(expected_path).unwrap();
        file.read_to_end(&mut expected).unwrap();

        assert_eq!(expected, result);
    }

    #[test]
    fn check_against_test_data() {
        check(
            &PathBuf::from("data/test.txt"),
            &PathBuf::from("data/test_res.txt"),
        );
    }

    #[test]
    #[ignore]
    fn check_against_full_data() {
        check(
            &PathBuf::from("data/all_cities.txt"),
            &PathBuf::from("data/all_cities_res.txt"),
        );
    }

    #[test]
    #[ignore]
    fn check_against_cities400_data() {
        check(
            &PathBuf::from("data/cities_400.txt"),
            &PathBuf::from("data/cities_400_res.txt"),
        );
    }
}
