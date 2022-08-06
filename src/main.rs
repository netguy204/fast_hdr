use clap::{Parser, clap_derive::ArgEnum};
use hdrhistogram::{Histogram, serialization::{V2DeflateSerializer, Serializer}};
use thiserror::Error;
use std::{io, num::ParseIntError};

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

fn some_or_err<T, FN>(maybe: Option<T>, err: FN) -> Result<T> 
where FN: Fn() -> Error {
    if let Some(v) = maybe {
        Result::Ok(v)
    } else {
        Result::Err(err())
    }
}

fn serialize(hist: Histogram<u64>) -> String {
    let mut ser = V2DeflateSerializer::new();
    let mut buf: Vec<u8> = Vec::new();
    ser.serialize(&hist, &mut buf).unwrap();
    base64::encode(buf)
}

impl Args {
    fn to_hist(&self) -> Result<Histogram<u64>> {
        let mut hist : Histogram<u64> = Histogram::new_with_max(self.max_value, self.sigfigs)?;
        let mut csv = csv::Reader::from_path(&self.fname)?;
        let header = csv.headers()?;
        let lhs_idx = header.iter().position(|name| { name == &self.lhs_column});
        let rhs_idx = header.iter().position(|name| { name == &self.rhs_column});

        let lhs_idx = some_or_err(lhs_idx, || {Error::UserError(format!("{} does not exist in {:?}", self.lhs_column, header))})?;
        let rhs_idx = some_or_err(rhs_idx, || {Error::UserError(format!("{} does not exist in {:?}", self.rhs_column, header))})?;

        let records = csv.records();
        for record in records {
            let record = record?;
            let lhs = record.get(lhs_idx);
            let rhs = record.get(rhs_idx);
            if let Some(lhs) = lhs {
                if let Some(rhs) = rhs {
                    let lhs = lhs.parse::<u64>()?;
                    let rhs = rhs.parse::<u64>()?;

                    match self.oob {
                        OOBRule::Error => {
                            hist.record(lhs - rhs).map_err(|err| {Error::UserError(format!("could not record {}", err))})?;
                        },

                        OOBRule::Saturate => {
                            hist.saturating_record(lhs - rhs)
                        },

                        OOBRule::Drop => {
                            let v = lhs - rhs;
                            if v < self.max_value {
                                hist.record(v).map_err(|err| {Error::UserError(format!("could not record {}", err))})?;
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
