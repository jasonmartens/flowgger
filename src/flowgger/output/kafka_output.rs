extern crate openssl;

use std::thread;
use std::io::{stderr, Write};
use std::process::exit;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::Receiver;
use std::time::Duration;

use kafka::client::KafkaClient;
use kafka::producer::{Compression, Producer, Record, RequiredAcks};
use log::{debug, error, info};

use crate::flowgger::config::Config;
use crate::flowgger::merger::Merger;

use super::Output;
use super::super::kafka::client::SecurityConfig;

use self::openssl::ssl::{SslConnector, SslFiletype, SslMethod, SslVerifyMode};


const KAFKA_DEFAULT_ACKS: i16 = 0;
const KAFKA_DEFAULT_COALESCE: usize = 1;
const KAFKA_DEFAULT_COMPRESSION: &str = "none";
const KAFKA_DEFAULT_THREADS: u32 = 1;
const KAFKA_DEFAULT_TIMEOUT: u64 = 60_000;

pub struct KafkaOutput {
    config: KafkaConfig,
    threads: u32,
}

#[derive(Clone)]
struct KafkaConfig {
    acks: i16,
    brokers: Vec<String>,
    topic: String,
    timeout: Duration,
    coalesce: usize,
    compression: Compression,
    ssl_cert_path: Option<String>,
    ssl_key_path: Option<String>,
    ssl_ca_cert_path: Option<String>,
    ssl_host_verify: bool,
}

struct KafkaWorker<'a> {
    arx: Arc<Mutex<Receiver<Vec<u8>>>>,
    producer: Producer,
    config: KafkaConfig,
    queue: Vec<Record<'a, (), Vec<u8>>>,
}

impl<'a> KafkaWorker<'a> {
    fn new(arx: Arc<Mutex<Receiver<Vec<u8>>>>, config: KafkaConfig) -> KafkaWorker<'a> {
        let acks = match config.acks {
            -1 => RequiredAcks::All,
            0 => RequiredAcks::None,
            1 => RequiredAcks::One,
            _ => panic!("Unsupported value for kafka_acks"),
        };

        // ~ If certificates are specified, configure SSL
        let producer =
            if let (Some(ccert), Some(ckey)) = (config.ssl_cert_path.clone(), config.ssl_key_path.clone()) {
                let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();

                debug!("loading cert-file={}, key-file={}", ccert, ckey);
                builder.set_cipher_list("DEFAULT").unwrap();
                builder.set_verify(SslVerifyMode::PEER); // TODO: This is only for testing!
                builder.set_certificate_file(ccert, SslFiletype::PEM).unwrap();
                builder.set_private_key_file(ckey, SslFiletype::PEM).unwrap();
                builder.check_private_key().unwrap();


                if let Some(ca_cert_path) = config.ssl_ca_cert_path.clone() {
                    debug!("Setting ca certificates to {}", ca_cert_path);
                    builder.set_ca_file(ca_cert_path).unwrap();
                } else {
                    builder.set_default_verify_paths().unwrap();
                }

                let connector = builder.build();

                let mut client = KafkaClient::new_secure(
                    config.brokers.clone(),
                    SecurityConfig::new(connector).with_hostname_verification(config.ssl_host_verify),
                );
                client.set_client_id("log_producer".into());

                debug!("Kafka ssl client hosts: {:?}, config: {:?}", config.brokers.clone(), client);

                let producer_builder = Producer::from_client(client)
                    .with_required_acks(acks)
                    .with_ack_timeout(config.timeout)
                    .with_compression(config.compression);
                producer_builder
            } else {
                info!("Connecting to non-ssl Kafka at {:?}", config.brokers.clone());
                let producer_builder = Producer::from_hosts(config.brokers.clone())
                    .with_required_acks(acks)
                    .with_ack_timeout(config.timeout)
                    .with_compression(config.compression);
                producer_builder
            };

        let producer = match producer.create() {
            Ok(producer) => producer,
            Err(e) => {
                error!("Unable to connect to Kafka: [{}]", e);
                exit(1);
            }
        };
        let queue = Vec::with_capacity(config.coalesce);
        KafkaWorker {
            arx,
            producer,
            config,
            queue,
        }
    }

    fn run_nocoalesce(&'a mut self) {
        loop {
            let bytes = match { self.arx.lock().unwrap().recv() } {
                Ok(line) => line,
                Err(_) => return,
            };
            debug!("sending to kafka: {}", String::from_utf8(bytes.clone()).unwrap());
            match self
                .producer
                .send(&Record::from_value(&self.config.topic, bytes))
            {
                Ok(_) => {}
                Err(e) => {
                    println!("Kafka not responsive: [{}]", e);
                    exit(1);
                }
            }
        }
    }

    fn run_coalesce(&'a mut self) {
        loop {
            let bytes = match { self.arx.lock().unwrap().recv() } {
                Ok(line) => line,
                Err(_) => return,
            };
            debug!("sending to kafka: {}", String::from_utf8(bytes.clone()).unwrap());
            let message = Record {
                key: (),
                partition: -1,
                topic: &self.config.topic,
                value: bytes,
            };
            let queue = &mut self.queue;
            queue.push(message);
            if queue.len() >= self.config.coalesce {
                match self.producer.send_all(queue) {
                    Ok(_) => {}
                    Err(e) => {
                        println!("Kafka not responsive: [{}]", e);
                        exit(1);
                    }
                }
                queue.clear();
            }
        }
    }

    fn run(&'a mut self) {
        if self.config.coalesce <= 1 {
            self.run_nocoalesce()
        } else {
            self.run_coalesce()
        }
    }
}

impl KafkaOutput {
    pub fn new(config: &Config) -> KafkaOutput {
        let acks = config
            .lookup("output.kafka_acks")
            .map_or(KAFKA_DEFAULT_ACKS, |x| {
                x.as_integer()
                    .expect("output.kafka_acks must be a 16-bit integer") as i16
            });
        let brokers = config
            .lookup("output.kafka_brokers")
            .expect("output.kafka_brokers is required")
            .as_array()
            .expect("Invalid list of Kafka brokers");
        let brokers = brokers
            .iter()
            .map(|x| {
                x.as_str()
                    .expect("output.kafka_brokers must be a list of strings")
                    .to_owned()
            })
            .collect();
        let topic = config
            .lookup("output.kafka_topic")
            .expect("output.kafka_topic must be a string")
            .as_str()
            .expect("output.kafka_topic must be a string")
            .to_owned();
        let timeout = Duration::from_millis(config.lookup("output.kafka_timeout").map_or(
            KAFKA_DEFAULT_TIMEOUT,
            |x| {
                x.as_integer()
                    .expect("output.kafka_timeout must be a 64-bit integer") as u64
            },
        ));
        let threads = config
            .lookup("output.kafka_threads")
            .map_or(KAFKA_DEFAULT_THREADS, |x| {
                x.as_integer()
                    .expect("output.kafka_threads must be a 32-bit integer") as u32
            });
        let coalesce = config
            .lookup("output.kafka_coalesce")
            .map_or(KAFKA_DEFAULT_COALESCE, |x| {
                x.as_integer()
                    .expect("output.kafka_coalesce must be a size integer") as usize
            });
        let compression = match config
            .lookup("output.kafka_compression")
            .map_or(KAFKA_DEFAULT_COMPRESSION, |x| {
                x.as_str()
                    .expect("output.kafka_compresion must be a string")
            })
            .to_lowercase()
            .as_ref()
        {
            "none" => Compression::NONE,
            "gzip" => Compression::GZIP,
            "snappy" => Compression::SNAPPY,
            _ => panic!("Unsupported compression method"),
        };
        let ssl_cert_path = config
            .lookup("output.kafka_ssl_cert_path")
            .map(|x| x.as_str()
                .expect("output.kafka_ssl_cert_path must be a string")
            ).map(|x| String::from(x));

        let ssl_key_path = config
            .lookup("output.kafka_ssl_key_path")
            .map(|x| x.as_str()
                .expect("output.kafka_ssl_key_path must be a string")
            ).map(|x| String::from(x));
        let ssl_ca_cert_path = config
            .lookup("output.kafka_ssl_ca_cert_path")
            .map(|x| x.as_str()
                .expect("output.kafka_ssl_ca_cert_path must be a string")
            ).map(|x| String::from(x));
        let ssl_host_verify = config
            .lookup("output.kafka_ssl_host_verify")
            .map_or(true, |x| x.as_bool()
                .expect("output.kafka_ssl_host_verify must be a bool"),
            );
        let kafka_config = KafkaConfig {
            acks,
            brokers,
            topic,
            timeout,
            coalesce,
            compression,
            ssl_cert_path,
            ssl_key_path,
            ssl_ca_cert_path,
            ssl_host_verify,
        };
        KafkaOutput {
            config: kafka_config,
            threads,
        }
    }
}

impl Output for KafkaOutput {
    fn start(&self, arx: Arc<Mutex<Receiver<Vec<u8>>>>, merger: Option<Box<dyn Merger>>) {
        if merger.is_some() {
            let _ = writeln!(stderr(), "Output framing is ignored with the Kafka output");
        }
        for _ in 0..self.threads {
            let arx = Arc::clone(&arx);
            let config = self.config.clone();
            thread::spawn(move || {
                let mut worker = KafkaWorker::new(arx, config);
                worker.run();
            });
        }
    }
}
