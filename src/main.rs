use clap::{Parser, clap_derive::ArgEnum};
use csv::{StringRecord, StringRecordsIter};
use hdrhistogram::{Histogram, serialization::{V2DeflateSerializer, Serializer}};
use thiserror::Error;
use std::{io, num::ParseIntError, fs::File, collections::HashMap};

#[derive(Error, Debug)]
enum Error {
    #[error("io error {0}")]
    IO(io::Error),

    #[error("histogram parameter error {0}")]
    HistError(hdrhistogram::CreationError),

    #[error("error reading CSV {0}")]
    Csv(csv::Error),

    #[error("invalid input: {0}")]
    UserError(String),

    #[error("illegal value in file")]
    ParseIntError(ParseIntError),
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::IO(err)
    }
}

impl From<hdrhistogram::CreationError> for Error {
    fn from(err: hdrhistogram::CreationError) -> Self {
        Error::HistError(err)
    }
}

impl From<csv::Error> for Error {
    fn from(err: csv::Error) -> Self {
        Error::Csv(err)
    }
}

impl From<ParseIntError> for Error {
    fn from(err: ParseIntError) -> Self {
        Error::ParseIntError(err)
    }
}

impl From<String> for Error {
    fn from(str: String) -> Self {
        Error::UserError(str)
    }
}

type Result<T> = std::result::Result<T, Error>;

#[derive(ArgEnum, Debug, Clone)]
enum OOBRule {
    Error,
    Drop,
    Saturate
}

#[derive(Parser, Debug)]
#[clap(author, version, about)]
struct Args {
    #[clap(long, value_parser)]
    fname: String,
    #[clap(long, value_parser)]
    lhs_column: String,
    #[clap(long, value_parser)]
    rhs_column: String,
    #[clap(long, value_parser, default_value_t = 30000)]
    max_value: u64,
    #[clap(long, value_parser, default_value_t = 2)]
    sigfigs: u8,

    // optional arguments for multi-file measures
    #[clap(long, value_parser)]
    rhs_fname: Option<String>,
    #[clap(long, value_parser)]
    join_column: Option<String>,

    #[clap(long, arg_enum, default_value_t = OOBRule::Error)]
    oob: OOBRule,
}

fn some_or_missing_col<T>(maybe: Option<T>, col: &str) -> Result<T> {
    if let Some(v) = maybe {
        Result::Ok(v)
    } else {
        Result::Err(Error::UserError(format!("{} is not a valid column", col)))
    }
}

fn serialize(hist: Histogram<u64>) -> String {
    let mut ser = V2DeflateSerializer::new();
    let mut buf: Vec<u8> = Vec::new();
    ser.serialize(&hist, &mut buf).unwrap();
    base64::encode(buf)
}

struct JoinRHS<'a> {
    reader: StringRecordsIter<'a, File>,
    join_idx: usize,
    ooo: HashMap<String, StringRecord>,
}

impl <'a> JoinRHS<'a> {
    fn new(reader: StringRecordsIter<'a, File>, join_idx: usize) -> JoinRHS<'a> {
        JoinRHS { reader: reader, join_idx, ooo: HashMap::new() }
    }

    fn take(&mut self, join_key: String) -> Result<Option<StringRecord>> {
        if let Some(record) = self.ooo.remove(&join_key) {
            Result::Ok(Option::Some(record))
        } else {
            while let Some(record) = self.reader.next() {
                let record = record?;
                if let Some(record_key) = record.get(self.join_idx) {
                    if record_key == join_key {
                        return Result::Ok(Option::Some(record))
                    } else {
                        self.ooo.insert(record_key.into(), record);
                    }
                }
            }
            Result::Ok(Option::None)
        }
    }
}

impl Args {
    fn to_hist(&self) -> Result<Histogram<u64>> {
        if let Some(rhs_fname) = self.rhs_fname.as_ref() {
            let join_cname = self.join_column.as_ref().ok_or(Error::UserError("join column not supplied".into()))?;
            self.dual_file_to_hist(rhs_fname, join_cname)
        } else {
            self.single_file_to_hist()
        }
    }

    fn single_file_to_hist(&self) -> Result<Histogram<u64>> {
        let mut hist : Histogram<u64> = Histogram::new_with_max(self.max_value, self.sigfigs)?;
        let mut csv = csv::Reader::from_path(&self.fname)?;
        let header = csv.headers()?;
        let lhs_idx = header.iter().position(|name| { name == &self.lhs_column});
        let rhs_idx = header.iter().position(|name| { name == &self.rhs_column});

        let lhs_idx = some_or_missing_col(lhs_idx, &self.lhs_column)?;
        let rhs_idx = some_or_missing_col(rhs_idx, &self.rhs_column)?;

        let records = csv.records();
        for record in records {
            let record = record?;
            let lhs = record.get(lhs_idx);
            let rhs = record.get(rhs_idx);
            if let Some(lhs) = lhs {
                if let Some(rhs) = rhs {
                    let lhs = lhs.parse::<i64>()?;
                    let rhs = rhs.parse::<i64>()?;
                    let v = lhs - rhs;

                    match self.oob {
                        OOBRule::Error => {
                            hist.record(v as u64).map_err(|err| {Error::UserError(format!("could not record {}", err))})?;
                        },

                        OOBRule::Saturate => {
                            hist.saturating_record(v as u64)
                        },

                        OOBRule::Drop => {
                            if v >= 0 && v < self.max_value as i64 {
                                hist.record(v as u64).map_err(|err| {Error::UserError(format!("could not record {}", err))})?;
                            }
                        }
                    }
                    
                }
            }
        }

        Result::Ok(hist)
    }

    fn dual_file_to_hist(&self, rhs_fname: &String, join_column: &String) -> Result<Histogram<u64>> {
        let mut hist : Histogram<u64> = Histogram::new_with_max(self.max_value, self.sigfigs)?;
        let mut lhs_csv = csv::Reader::from_path(&self.fname)?;
        let lhs_header = lhs_csv.headers()?;
        let lhs_idx = lhs_header.iter().position(|name| { name == &self.lhs_column});
        let lhs_join_idx = lhs_header.iter().position(|name| { name == join_column});

        let mut rhs_csv = csv::Reader::from_path(&rhs_fname)?;
        let rhs_header = rhs_csv.headers()?;
        let rhs_idx = rhs_header.iter().position(|name| { name == &self.rhs_column});
        let rhs_join_idx = rhs_header.iter().position(|name| { name == join_column});

        let lhs_join_idx = some_or_missing_col(lhs_join_idx, &join_column)?;
        let rhs_join_idx = some_or_missing_col(rhs_join_idx, &join_column)?;
        let lhs_idx = some_or_missing_col(lhs_idx, &self.lhs_column)?;
        let rhs_idx = some_or_missing_col(rhs_idx, &self.rhs_column)?;
        let mut rhs_csv = JoinRHS::new(rhs_csv.records(), rhs_join_idx);

        let lhs_records = lhs_csv.records();
        for lhs_record in lhs_records {
            let lhs_record = lhs_record?;
            let lhs = lhs_record.get(lhs_idx);
            let join_value = if let Some(value) = lhs_record.get(lhs_join_idx) {
                value
            } else {
                continue
            };

            let rhs = if let Some(rhs_record) = rhs_csv.take(join_value.into())? {
                rhs_record.get(rhs_idx).map(|v| { v.to_string() })
            } else {
                // rhs join record did not exist
                Option::None
            };

            if let Some(lhs) = lhs {
                if let Some(rhs) = rhs {
                    let lhs = lhs.parse::<i64>()?;
                    let rhs = rhs.parse::<i64>()?;
                    let v = lhs - rhs;

                    match self.oob {
                        OOBRule::Error => {
                            if v > 0 {
                                hist.record(v as u64).map_err(|err| {Error::UserError(format!("could not record {}", err))})?;
                            }
                        },

                        OOBRule::Saturate => {
                            if v > 0 {
                                hist.saturating_record(v as u64)
                            }
                        },

                        OOBRule::Drop => {
                            if v >= 0 && v < self.max_value as i64 {
                                hist.record(v as u64).map_err(|err| {Error::UserError(format!("could not record {}", err))})?;
                            }
                        }
                    }
                    
                }
            }
        }

        Result::Ok(hist)
    }
}

fn main() {
    let args = Args::parse();
    let hist = args.to_hist().unwrap();

    println!("{}", serialize(hist));
}
