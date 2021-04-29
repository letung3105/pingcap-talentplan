use crate::error::Result;
use serde::Serialize;

pub fn to_string<T>(_value: &T) -> Result<String>
where
    T: Serialize,
{
    todo!()
}
pub struct Serializer {
    _output: String,
}
