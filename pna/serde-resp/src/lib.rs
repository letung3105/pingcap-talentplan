mod de;
mod error;
mod ser;

pub use de::{from_str, Deserializer};
pub use error::{Error, Result};
pub use ser::{to_string, Serializer};

#[cfg(tests)]
mod tests {
    #[test]
    fn test_build() {
        println!("It works!");
    }
}
