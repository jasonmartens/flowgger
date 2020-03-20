use serde_json::de;
use serde_json::error::Error::Syntax;
use serde_json::error::ErrorCode;
use serde_json::value::Value;

use crate::flowgger::config::Config;
use crate::flowgger::record::{Record, SDValue, SEVERITY_MAX, StructuredData};
use crate::flowgger::utils;

use super::Decoder;

#[derive(Clone)]
pub struct JsonDecoder;

impl JsonDecoder {
    /// The JSON decoder doesn't support any configuration, the config is passed as an argument
    /// just to respect the interface
    pub fn new(_config: &Config) -> JsonDecoder {
        JsonDecoder
    }
}

impl Decoder for JsonDecoder {
    /// Implements decode from a JSON formatted text line to a Record object
    ///
    /// # Parameters
    /// - `line`: A string slice containing JSON
    ///
    /// # Returns
    /// A `Result` that contain:
    ///
    /// - `Ok`: A record containing all the line parsed as a Record data struct
    /// - `Err`: if there was any error parsing the line, that could be missing values, bad json or wrong
    /// types associated with specific fields
    fn decode(&self, line: &str) -> Result<Record, &'static str> {
        let mut sd = StructuredData::new(None);
        let mut ts = 0.0;
        let mut hostname = None;
        let mut msg = None;
        let full_msg = None;
        let mut severity = None;

        let obj = match de::from_str(line) {
            x @ Ok(_) => x,
            Err(Syntax(ErrorCode::InvalidUnicodeCodePoint, ..)) => {
                de::from_str(&line.replace('\n', r"\n"))
            }
            x => x,
        };
        let obj: Value = obj.or(Err("Unable to parse as a JSON object"))?;
        let obj = obj.as_object().ok_or("Empty JSON input")?;
        for (key, value) in obj {
            match key.as_ref() {
                "timestamp" => ts = value.as_f64().unwrap_or( utils::PreciseTimestamp::now().as_f64())
                ,
                "host" => {
                    hostname = Some(
                        value
                            .as_str()
                            .ok_or("host name must be a string")?
                            .to_owned(),
                    )
                }
                "message" => {
                    msg = Some(
                        value
                            .as_str()
                            .ok_or("message must be a string")?
                            .to_owned(),
                    )
                }
                "level" => {
                    let severity_given = value.as_u64().ok_or("Invalid severity level")?;
                    if severity_given > u64::from(SEVERITY_MAX) {
                        return Err("Invalid severity level (too high)");
                    }
                    severity = Some(severity_given as u8)
                }
                name => {
                    let sd_value: SDValue = match *value {
                        Value::String(ref value) => SDValue::String(value.to_owned()),
                        Value::Bool(value) => SDValue::Bool(value),
                        Value::F64(value) => SDValue::F64(value),
                        Value::I64(value) => SDValue::I64(value),
                        Value::U64(value) => SDValue::U64(value),
                        Value::Null => SDValue::Null,
                        _ => return Err("Invalid value type in structured data"),
                    };
                    let name = if name.starts_with('_') {
                        name.to_owned()
                    } else {
                        format!("_{}", name)
                    };
                    sd.pairs.push((name, sd_value));
                }
            }
        }
        let record = Record {
            ts,
            hostname: hostname.unwrap_or(String::from("unknown")),
            facility: None,
            severity,
            appname: None,
            procid: None,
            msgid: None,
            sd: if sd.pairs.is_empty() { None } else { Some(sd) },
            msg,
            full_msg,
        };
        Ok(record)
    }
}

#[cfg(test)]
mod test {
    use crate::flowgger::record::SEVERITY_MAX;

    use super::*;

    #[test]
    fn test_json_decoder() {
        let msg = r#"{"version":"1.1", "host": "example.org","short_message": "A short message that helps you identify what is going on", "full_message": "Backtrace here\n\nmore stuff", "timestamp": 1385053862.3072, "level": 1, "_user_id": 9001, "_some_info": "foo", "_some_env_var": "bar"}"#;
        let res = JsonDecoder.decode(msg).unwrap();
        assert!(res.ts == 1_385_053_862.307_2);
        assert!(res.hostname == "example.org");
        assert!(res.msg.unwrap() == "A short message that helps you identify what is going on");
        assert!(res.full_msg.unwrap() == "Backtrace here\n\nmore stuff");
        assert!(res.severity.unwrap() == 1);

        let sd = res.sd.unwrap();
        let pairs = sd.pairs;
        assert!(pairs
            .iter()
            .cloned()
            .any(|(k, v)| if let SDValue::U64(v) = v {
                k == "_user_id" && v == 9001
            } else {
                false
            }));
        assert!(pairs
            .iter()
            .cloned()
            .any(|(k, v)| if let SDValue::String(v) = v {
                k == "_some_info" && v == "foo"
            } else {
                false
            }));
        assert!(pairs
            .iter()
            .cloned()
            .any(|(k, v)| if let SDValue::String(v) = v {
                k == "_some_env_var" && v == "bar"
            } else {
                false
            }));
    }

    #[test]
    #[should_panic(expected = "Invalid value type in structured data")]
    fn test_json_decoder_bad_key() {
        let msg = r#"{"some_key": []}"#;
        let _res = JsonDecoder.decode(&msg).unwrap();
    }

    #[test]
    #[should_panic(expected = "Invalid timestamp")]
    fn test_json_decoder_bad_timestamp() {
        let msg = r#"{"timestamp": "a string not a timestamp", "host": "anhostname"}"#;
        let _res = JsonDecoder.decode(&msg).unwrap();
    }

    #[test]
    #[should_panic(expected = "Invalid JSON input, unable to parse as a JSON object")]
    fn test_json_decoder_invalid_input() {
        let _res = JsonDecoder.decode("{some_key = \"some_value\"}").unwrap();
    }

    #[test]
    #[should_panic(expected = "Invalid severity level (too high)")]
    fn test_json_decoder_severity_to_high() {
        let _res = JsonDecoder
            .decode(format!("{{\"level\": {}}}", SEVERITY_MAX + 1).as_str())
            .unwrap();
    }
}
