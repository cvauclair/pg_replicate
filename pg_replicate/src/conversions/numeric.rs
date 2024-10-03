// This code is heavily inspired by the work from Diesel.
// See Diesel's `PgValue` to `PgNumeric` type conversion [implementation](https://github.com/diesel-rs/diesel/blob/381be195688db339fe2927e49bc818ab86754dd9/diesel/src/pg/types/floats/mod.rs#L16)
// and Diesel's `PgNumeric` to `BigDecimal` type conversion [implementation](https://github.com/diesel-rs/diesel/blob/381be195688db339fe2927e49bc818ab86754dd9/diesel/src/pg/types/numeric.rs#L40)
use std::error::Error;

use bigdecimal::{
    num_bigint::{BigInt, BigUint, Sign},
    BigDecimal,
};

use byteorder::{NetworkEndian, ReadBytesExt};
use tokio_postgres::types::{FromSql, Type};

/// representation
pub enum PgNumeric {
    /// A positive number
    Positive {
        /// How many digits come before the decimal point?
        weight: i16,
        /// How many significant digits are there?
        scale: u16,
        /// The digits in this number, stored in base 10000
        digits: Vec<i16>,
    },
    /// A negative number
    Negative {
        /// How many digits come before the decimal point?
        weight: i16,
        /// How many significant digits are there?
        scale: u16,
        /// The digits in this number, stored in base 10000
        digits: Vec<i16>,
    },
    /// Not a number
    NaN,
}

#[derive(Debug, Clone, Copy)]
struct InvalidNumericSign(u16);
impl ::std::fmt::Display for InvalidNumericSign {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        f.write_str("sign for numeric field was not one of 0, 0x4000, 0xC000")
    }
}
impl Error for InvalidNumericSign {}

impl<'a> TryFrom<&'a PgNumeric> for BigDecimal {
    type Error = Box<dyn Error + Send + Sync>;

    fn try_from(numeric: &'a PgNumeric) -> Result<Self, Self::Error> {
        let (sign, weight, scale, digits) = match *numeric {
            PgNumeric::Positive {
                weight,
                scale,
                ref digits,
            } => (Sign::Plus, weight, scale, digits),
            PgNumeric::Negative {
                weight,
                scale,
                ref digits,
            } => (Sign::Minus, weight, scale, digits),
            PgNumeric::NaN => {
                return Err(Box::from("NaN is not (yet) supported in BigDecimal"))
            }
        };

        let mut result = BigUint::default();
        let count = i64::try_from(digits.len())?;
        for digit in digits {
            result *= BigUint::from(10_000u64);
            result += BigUint::from(u64::try_from(*digit)?);
        }
        // First digit got factor 10_000^(digits.len() - 1), but should get 10_000^weight
        let correction_exp = 4 * (i64::from(weight) - count + 1);
        let result = BigDecimal::new(BigInt::from_biguint(sign, result), -correction_exp)
            .with_scale(i64::from(scale));
        Ok(result)
    }
}

impl<'a> FromSql<'a> for PgNumeric {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let mut bytes = raw.clone();
        let digit_count = bytes.read_u16::<NetworkEndian>()?;
        let mut digits = Vec::with_capacity(digit_count as usize);
        let weight = bytes.read_i16::<NetworkEndian>()?;
        let sign = bytes.read_u16::<NetworkEndian>()?;
        let scale = bytes.read_u16::<NetworkEndian>()?;
        for _ in 0..digit_count {
            digits.push(bytes.read_i16::<NetworkEndian>()?);
        }

        match sign {
            0 => Ok(PgNumeric::Positive {
                weight,
                scale,
                digits,
            }),
            0x4000 => Ok(PgNumeric::Negative {
                weight,
                scale,
                digits,
            }),
            0xC000 => Ok(PgNumeric::NaN),
            invalid => Err(Box::new(InvalidNumericSign(invalid))),
        }
    }
    
    fn accepts(ty: &Type) -> bool {
        match *ty {
            Type::NUMERIC => true,
            _ => false,
        }
    }

    
}
