use crate::error::Result;
use serde::Deserialize;

pub fn from_str<'a, T>(_s: &'a str) -> Result<T>
where
    T: Deserialize<'a>,
{
    todo!()
}
pub struct Deserializer<'de> {
    // This string starts with the input data and characters are truncated off
    // the beginning as data is parsed.
    _input: &'de str,
}
