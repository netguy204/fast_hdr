use clap::{Parser, clap_derive::ArgEnum};
use csv::{StringRecord};
use flate2::read::GzDecoder;
use hdrhistogram::{Histogram, serialization::{V2DeflateSerializer, Serializer}};
use thiserror::Error;
use std::{io, num::ParseIntError, collections::HashMap, fs::File, ops::Deref};

#[derive(Error, Debug)]
pub enum Error {
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

fn serialize(hist: Histogram<u64>) -> String {
    let mut ser = V2DeflateSerializer::new();
    let mut buf: Vec<u8> = Vec::new();
    ser.serialize(&hist, &mut buf).unwrap();
    base64::encode(buf)
}

pub struct Measurement {
    primary: u64,
    rhs: Option<u64>,
    join: Option<String>,
}

struct Reader<T>{
    reader: csv::Reader<T>,
    primary_idx: usize,
    rhs_idx: Option<usize>,
    join_idx: Option<usize>,
}

fn get_str(record: &StringRecord, idx: usize) -> Result<&str> {
    record.get(idx).ok_or_else(|| {
        Error::UserError(format!("{} is not a valid index in {:?}", idx, record))
    })
}

fn get(record: &StringRecord, idx: usize) -> Result<u64> {
    let value = get_str(record, idx);
    let value = value.and_then(|value| {
        value.parse::<u64>().map_err(|err| {err.into()})
    });
    value
}

type MeasurementIterator = dyn Iterator<Item = Result<Measurement>>;


pub fn new_reader(fname: &str, primary_cname: &str, rhs_cname: Option<&str>, join_cname: Option<&str>) -> Result<Box<MeasurementIterator>> {
    let result: Result<Box<dyn Iterator<Item = Result<Measurement>>>> = if fname.ends_with(".gz") {
        new_compressed_reader(fname, primary_cname, rhs_cname, join_cname).map(|r| { Box::new(r) as Box<MeasurementIterator> })
    } else {
        new_uncompressed_reader(fname, primary_cname, rhs_cname, join_cname).map(|r| { Box::new(r) as Box<MeasurementIterator> })
    };
    result
}

fn new_uncompressed_reader(fname: &str, primary_cname: &str, rhs_cname: Option<&str>, join_cname: Option<&str>) -> Result<Reader<File>> {
    let mut csv = csv::Reader::from_path(fname)?;
    let header = csv.headers()?;
    let primary_idx = header.iter().position(|name| { name == primary_cname});
    let primary_idx = match primary_idx {
        Option::None => {
            return Result::Err(Error::UserError(format!("{} is not a column in {}", primary_cname, fname)));
        },
        Option::Some(value) => value,
    };
    let rhs_idx = rhs_cname.and_then(|rhs_cname| {
        header.iter().position(|name| { name == rhs_cname})
    });
    let join_idx = join_cname.and_then(|join_cname| {
        header.iter().position(|name| { name == join_cname })
    });

    Result::Ok(Reader {
        reader: csv,
        primary_idx,
        rhs_idx,
        join_idx,
    })
}

fn new_compressed_reader(fname: &str, primary_cname: &str, rhs_cname: Option<&str>, join_cname: Option<&str>) -> Result<Reader<GzDecoder<File>>> {
    let file = File::open(fname)?;
    let file = GzDecoder::new(file);
    let mut csv = csv::Reader::from_reader(file);
    let header = csv.headers()?;
    let primary_idx = header.iter().position(|name| { name == primary_cname});
    let primary_idx = match primary_idx {
        Option::None => {
            return Result::Err(Error::UserError(format!("{} is not a column in {}", primary_cname, fname)));
        },
        Option::Some(value) => value,
    };
    let rhs_idx = rhs_cname.and_then(|rhs_cname| {
        header.iter().position(|name| { name == rhs_cname})
    });
    let join_idx = join_cname.and_then(|join_cname| {
        header.iter().position(|name| { name == join_cname })
    });

    Result::Ok(Reader {
        reader: csv,
        primary_idx,
        rhs_idx,
        join_idx,
    })
}


impl <T: io::Read> Iterator for Reader<T> {
    type Item = Result<Measurement>;

    fn next(&mut self) -> Option<Self::Item> {
        let row = self.reader.records().next();
        match row {
            Option::Some(csv::Result::Err(err)) => {
                return Option::Some(Result::Err(err.into()))
            }
            Option::Some(csv::Result::Ok(value)) => {
                let lhs = get(&value, self.primary_idx);
                let rhs = self.rhs_idx.map(|rhs_idx| { 
                    get(&value, rhs_idx)
                });
                let join = self.join_idx.map(|join_idx| { 
                    get_str(&value, join_idx)
                });
                // abort on required column read error
                let rhs = match rhs {
                    Option::None => Option::None,
                    Option::Some(Result::Ok(v)) => Option::Some(v),
                    Option::Some(Result::Err(err)) => {
                        return Option::Some(Result::Err(err));
                    }
                };
                let join = match join {
                    Option::None => Option::None,
                    Option::Some(Result::Ok(v)) => Option::Some(v),
                    Option::Some(Result::Err(err)) => {
                        return Option::Some(Result::Err(err));
                    }
                };

                Option::Some(lhs.map(|lhs| {
                    Measurement{
                        primary: lhs,
                        rhs: rhs,
                        join: join.map(|join| { join.to_string() }),
                    }
                }))
            }
            Option::None => Option::None
        }
    }
}



struct JoinRHS {
    reader: Box<MeasurementIterator>,
    ooo: HashMap<String, Measurement>,
}

impl JoinRHS {
    fn new(reader: Box<MeasurementIterator>) -> JoinRHS {
        JoinRHS { reader, ooo: HashMap::new() }
    }

    fn take(&mut self, join_key: String) -> Result<Option<Measurement>> {
        if let Some(record) = self.ooo.remove(&join_key) {
            Result::Ok(Option::Some(record))
        } else {
            while let Some(record) = self.reader.next() {
                let record = record?;
                let key = record.join.clone().unwrap();
                if key == join_key {
                    return Result::Ok(Option::Some(record))
                } else {
                    self.ooo.insert(key, record);
                }
            }
            Result::Ok(Option::None)
        }
    }
}

impl Args {
    fn to_hist(&self) -> Result<Histogram<u64>> {
        if let Some(_) = self.rhs_fname.as_ref() {
            self.join_column.as_ref().ok_or(Error::UserError("join column not supplied".into()))?;
            self.dual_file_to_hist()
        } else {
            self.single_file_to_hist()
        }
    }

    fn single_file_to_hist(&self) -> Result<Histogram<u64>> {
        let mut hist : Histogram<u64> = Histogram::new_with_max(self.max_value, self.sigfigs)?;
        let records = new_reader(
            &self.fname,
            &self.lhs_column,
            Option::Some(&self.rhs_column),
            Option::None,
        )?;
        for record in records {
            let record = record?;
            let lhs = record.primary;
            let rhs = record.rhs;

            if let Some(rhs) = rhs {
                let lhs = lhs as i64;
                let rhs = rhs as i64;
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

        Result::Ok(hist)
    }

    fn dual_file_to_hist(&self) -> Result<Histogram<u64>> {
        let mut hist : Histogram<u64> = Histogram::new_with_max(self.max_value, self.sigfigs)?;
        let lhs_records = new_reader(
            &self.fname,
            &self.lhs_column,
            Option::None,
            self.join_column.as_ref().map(|c| { c.deref() })
        )?;
        let rhs_records = new_reader(
            &self.rhs_fname.as_ref().unwrap(),
            &self.rhs_column,
            Option::None,
            self.join_column.as_ref().map(|c| { c.deref() })
        )?;
        let mut rhs_csv = JoinRHS::new(rhs_records);

        for lhs_record in lhs_records {
            let lhs_record = lhs_record?;
            let lhs = lhs_record.primary;
            let join_value = if let Some(value) = lhs_record.join {
                value
            } else {
                continue
            };

            let rhs = rhs_csv.take(join_value.into())?;
            let rhs = rhs.map(|rhs| { rhs.primary });

            if let Some(rhs) = rhs {
                let lhs = lhs as i64;
                let rhs = rhs as i64;
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

        Result::Ok(hist)
    }
}

fn main() {
    let args = Args::parse();
    let hist = args.to_hist().unwrap();

    println!("{}", serialize(hist));
}
