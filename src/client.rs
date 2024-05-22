use tonic::transport::Channel;
use tonic::Request;

use rmp_serde::decode::from_read_ref;
use serde_json::{json, to_string, to_string_pretty, Value};
use clap::{Arg, Command};

use datastore::datastore_client::DatastoreClient;
use datastore::{GetRequest, QueryRequest, SetRequest};

use base64::{engine::general_purpose, Engine as _};

pub mod datastore {
    tonic::include_proto!("datastore");
}

const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_PORT: &str = "7777";

async fn get(
    client: &mut DatastoreClient<Channel>,
    key: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let request = GetRequest { key: key.clone() };
    let response = client.get(Request::new(request)).await?;
    let item = response.into_inner();

    if let Some(item) = item.item {
        // deserialize messagepack into serde_json::Value
        match from_read_ref::<_, Value>(&item.value) {
            Ok(value) => {
                if let Ok(json_str) = to_string_pretty(&value) {
                    println!("{}", json_str);
                } else {
                    println!("Error formatting JSON");
                }
            }
            Err(e) => {
                println!(
                    "[key: {}] Failed to deserialize MessagePack data: {:?}",
                    item.key, e
                );
            }
        }
    } else {
        println!("No item found for key: {}", key);
    }

    Ok(())
}

async fn set(
    client: &mut DatastoreClient<Channel>,
    key: String,
    value: Vec<u8>,
    ttl: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let encoded_value = general_purpose::STANDARD.encode(&value);
    let request = SetRequest {
        key,
        value: encoded_value.into(),
        options: Some(datastore::SetOptions {
            preserve_history: true,
            ttl,
        }),
    };
    let response = client.set(Request::new(request)).await?;
    println!(
        "Set operation successful: {}",
        response.into_inner().success
    );
    Ok(())
}

async fn query(
    client: &mut DatastoreClient<Channel>,
    key: String,
    history_count: Option<i64>,
) -> Result<(), Box<dyn std::error::Error>> {
    let options = history_count.map(|count| datastore::GetOptions {
        history_count: Some(count),
    });

    let request = QueryRequest {key, options};
    let response = client.query(Request::new(request)).await?;

    let items = response.into_inner().items;
    let mut results = Vec::new();

    for item in items {
        // deserialize messagepack into serde_json::Value
        let value = match from_read_ref::<_, Value>(&item.value) {
            Ok(value) => value,
            Err(_) => json!({"error": "Failed to deserialize MessagePack data"}),
        };

        results.push(json!({
            "key": item.key,
            "value": value
        }));
    }

    // serialize results as json and return!
    if let Ok(json_str) = to_string(&results) {
        println!("{}", json_str);
    } else {
        println!("Error formatting JSON");
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cmd = Command::new("rs-datastore client")
        .version("0.1.0")
        .author("Sr. Rust Developer")
        .arg_required_else_help(true)
        .subcommand_required(true)
        .subcommand(Command::new("get")
            .about("gets a value for a key")
            .arg(Arg::new("key").required(true)))
        .subcommand(Command::new("set")
            .about("sets a value for a key with optional ttl")
            .arg(Arg::new("key").required(true))
            .arg(Arg::new("value").required(true))
            .arg(Arg::new("ttl").required(false).value_parser(clap::value_parser!(i64))))
        .subcommand(Command::new("query")
            .about("queries a key with optional history count")
            .arg(Arg::new("key").required(true))
            .arg(Arg::new("history_count").required(false).value_parser(clap::value_parser!(i64))))
        .get_matches();

    // Retrieve host and port from environment or use default values
    let host = std::env::var("REMOTE_HOST").unwrap_or_else(|_| DEFAULT_HOST.to_string());
    let port = std::env::var("REMOTE_PORT").unwrap_or_else(|_| DEFAULT_PORT.to_string());
    let endpoint = format!("http://{}:{}", host, port);

    let mut client = DatastoreClient::connect(endpoint).await?;

    match cmd.subcommand() {
        Some(("get", sub_matches)) => {
            let key = sub_matches.get_one::<String>("key").unwrap();
            get(&mut client, key.to_string()).await?;
        },
        Some(("set", sub_matches)) => {
            let key = sub_matches.get_one::<String>("key").unwrap();
            let value = sub_matches.get_one::<String>("value").unwrap().as_bytes().to_vec();
            let ttl = sub_matches.get_one::<String>("ttl")
                        .and_then(|v| v.parse::<i64>().ok())
                        .unwrap_or(0);
            set(&mut client, key.to_string(), value, ttl).await?;
        },
        Some(("query", sub_matches)) => {
            let key = sub_matches.get_one::<String>("key").unwrap();
            let history_count = sub_matches.get_one::<String>("history_count")
                                  .and_then(|v| v.parse::<i64>().ok());
            query(&mut client, key.to_string(), history_count).await?;
        },
        _ => unreachable!(),
    }

    Ok(())
}
