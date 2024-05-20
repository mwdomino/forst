use tonic::transport::Channel;
use tonic::Request;

use rmp_serde::decode::from_read_ref;
use serde_json::{json, to_string, to_string_pretty, Value};

use datastore::datastore_client::DatastoreClient;
use datastore::{GetRequest, QueryRequest, SetRequest};

use base64::{engine::general_purpose, Engine as _};

pub mod datastore {
    tonic::include_proto!("datastore");
}

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
) -> Result<(), Box<dyn std::error::Error>> {
    let request = QueryRequest {
        key,
        options: Some(datastore::GetOptions {
            history_count: 0, // TODO: make this dynamic
        }),
    };
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
    let mut args: Vec<String> = std::env::args().collect();
    args.remove(0);

    if args.is_empty() || args.len() < 2 {
        println!("Usage: client <command> <key> [value] [ttl]");
        return Ok(());
    }

    let command = &args[0];
    let keys = args[1].clone();
    let mut client = DatastoreClient::connect("http://127.0.0.1:7777").await?;

    match command.as_str() {
        "get" => {
            get(&mut client, keys).await?;
        }
        "set" if args.len() > 2 => {
            let value = args[2].as_bytes().to_vec();
            let ttl = if args.len() > 3 {
                args[3].parse::<i64>().unwrap_or(0) // Attempt to parse TTL if provided
            } else {
                0 // Default TTL if not provided
            };
            set(&mut client, keys, value, ttl).await?;
        }
        "query" => {
            query(&mut client, keys).await?;
        }
        _ => println!("invalid command"),
    }

    Ok(())
}
