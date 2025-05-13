use serde::{ Deserialize, Deserializer, Serialize, Serializer };
use serde::de::{ self, Visitor };
use std::fmt;
use tracing::Level;

/// Serialize `tracing::Level` to a string
pub fn serialize_level<S>(level: &Level, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer
{
    let level_str = match *level {
        Level::TRACE => "trace",
        Level::DEBUG => "debug",
        Level::INFO => "info",
        Level::WARN => "warn",
        Level::ERROR => "error",
    };

    serializer.serialize_str(level_str)
}

/// Deserialize `tracing::Level` from a string
pub fn deserialize_level<'de, D>(deserializer: D) -> Result<Level, D::Error>
    where D: Deserializer<'de>
{
    struct LevelVisitor;

    impl<'de> Visitor<'de> for LevelVisitor {
        type Value = Level;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str(
                "a string representing a log level (trace, debug, info, warn, error)"
            )
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> where E: de::Error {
            match value.to_lowercase().as_str() {
                "trace" => Ok(Level::TRACE),
                "debug" => Ok(Level::DEBUG),
                "info" => Ok(Level::INFO),
                "warn" => Ok(Level::WARN),
                "error" => Ok(Level::ERROR),
                _ => Err(E::custom(format!("unknown log level: {}", value))),
            }
        }
    }

    deserializer.deserialize_str(LevelVisitor)
}
