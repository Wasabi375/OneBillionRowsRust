use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    fs::{create_dir_all, File},
    io::{BufWriter, Write},
    path::PathBuf,
};

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use console::Term;
use rand::{distributions::Alphanumeric, seq::SliceRandom, Rng, SeedableRng};
use rand_distr::{Binomial, Distribution};

#[derive(Debug, ValueEnum, Clone, Copy)]
enum ArgPreset {
    Full,
    Cities400,
    Test,
}

#[derive(Debug, Parser)]
struct Args {
    /// The number of lines to generate
    #[arg(short, long, default_value_t = 1_000_000_000)]
    line_count: usize,

    /// The number of cities to generate data for
    ///
    /// It is not guaranteed that all cities are used.
    /// Each row uses a random city, therefor there is a chance
    /// especially for small line counts that now all cities are used.
    #[arg(short, long, default_value_t = 10_000)]
    city_count: usize,

    /// The median length of the generated city names.
    ///
    /// The length follows a binomial distribution with a std-deviation of 0.3.
    #[arg(short, long, default_value_t = 5)]
    city_len: usize,

    /// The highest integer value that is generated (exclusive).
    /// This ignores the fractional digits. So a max_value of 99 with 1 fractional
    /// digit can generate a true max value of 99.9
    #[arg(long = "max", default_value_t = 99)]
    max_value: i32,

    /// The lowest integer value that is generated (exclusive)
    /// This ignores the fractional digits. So a min_value of -99 with 1 fractional
    /// digit can generate a true min value of -99.9
    #[arg(long = "min", default_value_t = -99)]
    min_value: i32,

    /// Number of fractional digits in the generated values
    #[arg(short, long, default_value_t = 1)]
    fractional_digit: u8,

    /// The output filename. Default is data.txt
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// The output filename for the expected result of the 1 Billion Row challenge
    /// given the data created.
    ///
    /// This can be used to generate test data to verify an implementation.
    #[arg(short, long)]
    result_output: Option<PathBuf>,

    /// A number of predefined arguments for easy data generation
    ///
    /// This will override all arguments except for the output files.
    /// It will however change the default for the output files.
    #[arg(short, long, value_enum)]
    preset: Option<ArgPreset>,
}

impl ArgPreset {
    fn output(&self) -> PathBuf {
        PathBuf::from(match self {
            ArgPreset::Full => "data/all_cities.txt",
            ArgPreset::Cities400 => "data/citeis_400.txt",
            ArgPreset::Test => "data/test.txt",
        })
    }

    fn result_output(&self) -> PathBuf {
        PathBuf::from(match self {
            ArgPreset::Full => "data/all_cities_res.txt",
            ArgPreset::Cities400 => "data/citeis_400_res.txt",
            ArgPreset::Test => "data/test_res.txt",
        })
    }

    fn city_count(&self) -> usize {
        match self {
            ArgPreset::Full => 10_000,
            ArgPreset::Cities400 => 400,
            ArgPreset::Test => 10,
        }
    }

    fn city_len(&self) -> usize {
        5
    }

    fn line_count(&self) -> usize {
        match self {
            ArgPreset::Full => 1_000_000_000,
            ArgPreset::Cities400 => 1_000_000_000,
            ArgPreset::Test => 1_000,
        }
    }

    fn min_value(&self) -> i32 {
        -99
    }

    fn max_value(&self) -> i32 {
        99
    }

    fn fractional_digit(&self) -> u8 {
        1
    }
}

fn main() -> Result<()> {
    let mut args = Args::parse();

    match args.preset {
        Some(preset) => {
            args.output
                .get_or_insert(preset.output())
                .parent()
                .map(|parent| create_dir_all(parent).context("Could not create output parent dir"));

            args.result_output
                .get_or_insert(preset.result_output())
                .parent()
                .map(|parent| create_dir_all(parent).context("Could not create output parent dir"));

            args.city_count = preset.city_count();
            args.city_len = preset.city_len();
            args.line_count = preset.line_count();
            args.min_value = preset.min_value();
            args.max_value = preset.max_value();
            args.fractional_digit = preset.fractional_digit();
        }
        None => {}
    }

    let mut rng = rand::rngs::StdRng::from_entropy();
    println!("generating cities ...");
    let cities = generate_cities(args.city_count, args.city_len, &mut rng);

    let generator = Generator::new(
        &cities,
        args.min_value,
        args.max_value,
        args.fractional_digit,
        rng,
    );

    let file = File::create(args.output.unwrap_or_else(|| PathBuf::from("data.txt")))
        .context("failed to create output file")?;
    let mut writer = BufWriter::new(file);

    let mut results = if args.result_output.is_some() {
        Some(HashMap::<String, CityResult>::with_capacity(
            args.city_count,
        ))
    } else {
        None
    };
    println!("generating rows...");

    let term = Term::stdout();
    for (i, row) in generator.take(args.line_count).enumerate() {
        if i % 10_000 == 0 && args.line_count > 10_000_000 {
            let _ = term.clear_last_lines(1);
            println!("generating rows {}/{}", i, args.line_count);
        }

        if let Some(results) = results.as_mut() {
            if let Some(old) = results.get_mut(row.city) {
                let value = row.value();

                old.count += 1;
                old.total += value;

                if old.min > value {
                    old.min = value;
                }
                if old.max < value {
                    old.max = value
                }
            } else {
                results.entry(row.city.to_owned()).or_insert_with(|| {
                    let value = row.value();
                    CityResult {
                        name: row.city.to_owned(),
                        count: 1,
                        total: value,
                        min: value,
                        max: value,
                    }
                });
            }
        }

        writeln!(&mut writer, "{row}").context("failed to write data")?;
    }
    drop(writer);

    if let Some(result_file) = args.result_output {
        println!("calculating result data");
        let file = File::create(result_file).context("failed to create result output fiel")?;
        let mut result_file = BufWriter::new(file);
        write!(result_file, "{{").context("failed to write result file")?;

        let results = results.unwrap();

        let mut sorted = cities.into_vec();
        sorted.sort_unstable();
        let mut first = true;
        for city in sorted.iter().filter_map(|name| results.get(name.as_str())) {
            if !first {
                write!(result_file, ", ").context("failed to write result file")?;
            } else {
                first = false;
            }
            write!(
                result_file,
                "{}={:.4$}/{:.4$}/{:.4$}",
                city.name,
                city.min,
                city.total / city.count as f32,
                city.max,
                args.fractional_digit as usize
            )
            .context("failed to write result file")?;
        }
        write!(result_file, "}}").context("failed to write result file")?;
    }

    println!("done");
    Ok(())
}

#[derive(Debug)]
struct CityResult {
    name: String,
    count: usize,
    total: f32,
    min: f32,
    max: f32,
}

fn generate_city<R: Rng>(distribution: Binomial, rng: &mut R) -> String {
    let len = distribution.sample(rng).min(100);
    let result: String = rng
        .sample_iter(Alphanumeric)
        .map(char::from)
        .take(len as usize)
        .collect();

    assert!(result.bytes().len() <= 100);
    result
}

fn generate_cities<R: Rng>(count: usize, city_len: usize, rng: &mut R) -> Box<[String]> {
    let mut cities = HashSet::with_capacity(count);

    let name_len_dist = Binomial::new(city_len as u64, 0.3).unwrap();

    while cities.len() != count {
        cities.insert(generate_city(name_len_dist, rng));
    }

    let mut result = Vec::with_capacity(count);

    for city in cities.into_iter() {
        result.push(city);
    }

    result.into()
}

struct Generator<'a, R> {
    cities: &'a [String],
    min: i32,
    max: i32,
    fraction_max: usize,
    rng: R,
}

impl<'a, R> Generator<'a, R> {
    fn new(cities: &'a [String], min: i32, max: i32, fraction_digits: u8, rng: R) -> Self {
        let fraction_max = 10usize.pow(fraction_digits.into()) - 1;

        Self {
            cities,
            min,
            max,
            fraction_max,
            rng,
        }
    }
}

#[derive(Debug)]
struct Row<'a> {
    city: &'a str,
    int_value: i32,
    fraction: Option<u32>,
}

impl Display for Row<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(fract_value) = self.fraction {
            write!(f, "{};{}.{}", self.city, self.int_value, fract_value)
        } else {
            write!(f, "{};{}", self.city, self.int_value)
        }
    }
}

impl Row<'_> {
    fn value(&self) -> f32 {
        if let Some(fraction) = self.fraction {
            // TODO is this the best I can come up with
            format!("{}.{}", self.int_value, fraction).parse().unwrap()
        } else {
            self.int_value as f32
        }
    }
}

impl<'a, R: Rng> Iterator for Generator<'a, R> {
    type Item = Row<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let city = self.cities.choose(&mut self.rng)?;

        let int_value = self.rng.gen_range(self.min..=self.max);

        if self.fraction_max > 0 {
            let fract_value = self.rng.gen_range(0..=self.fraction_max) as u32;
            Some(Row {
                city,
                int_value,
                fraction: Some(fract_value),
            })
        } else {
            Some(Row {
                city,
                int_value,
                fraction: None,
            })
        }
    }
}
