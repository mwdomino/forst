use tonic::transport::Channel;
use tonic::Request;

use bincode; // Ensure bincode is added to Cargo.toml
use chrono::{DateTime, Utc}; // Ensure chrono is added to Cargo.toml

use rs_datastore::nestedmap::Item;

use datastore::datastore_client::DatastoreClient;
use datastore::{GetRequest, QueryRequest, SetRequest};

use base64::{engine::general_purpose, Engine as _};

pub mod datastore {
    tonic::include_proto!("nestedmap");
}

async fn get(
    client: &mut DatastoreClient<Channel>,
    keys: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let request = GetRequest { keys };
    let response = client.get(Request::new(request)).await?;
    let item_bytes = response.into_inner().item;

    // Deserialize the item_bytes into an Item struct
    let item: Item = bincode::deserialize(&item_bytes)?;

    // Converting SystemTime to DateTime<Utc>
    let datetime: DateTime<Utc> = DateTime::from(item.timestamp);
    let timestamp_str = datetime.format("%Y-%m-%d %H:%M:%S").to_string();

    // Decode the value assuming it is UTF-8 text; handle possible errors
    let value_str = general_purpose::STANDARD
        .decode(&item.value)
        .map_err(|_| "Base64 decode error")
        .and_then(|bytes| String::from_utf8(bytes).map_err(|_| "Invalid UTF-8"))
        .unwrap_or_else(|e| e.to_string());

    println!(
        "[{}][id:{}][key:{}] {}",
        timestamp_str, item.id, item.key, value_str
    );

    Ok(())
}

async fn set(
    client: &mut DatastoreClient<Channel>,
    keys: String,
    value: Vec<u8>,
    ttl: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let encoded_value = general_purpose::STANDARD.encode(&value);
    let request = SetRequest {
        keys,
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
    keys: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let request = QueryRequest { keys };
    let response = client.query(Request::new(request)).await?;

    let items_bytes = response.into_inner().items; // Assuming items is Vec<u8> of serialized Item structs

    for item_bytes in items_bytes {
        // Deserialize the item_bytes into an Item struct
        let item: Item = bincode::deserialize(&item_bytes)
            .map_err(|e| format!("Failed to deserialize item: {}", e))?;

        // Convert SystemTime to DateTime<Utc>
        let datetime: DateTime<Utc> = DateTime::from(item.timestamp);
        let timestamp_str = datetime.format("%Y-%m-%d %H:%M:%S").to_string();

        // Decode the base64 value and convert to a UTF-8 string
        let value_str = general_purpose::STANDARD
            .decode(&item.value)
            .map_err(|_| "Base64 decode error")
            .and_then(|bytes| String::from_utf8(bytes).map_err(|_| "Invalid UTF-8"))
            .unwrap_or_else(|e| e.to_string());

        println!(
            "[{}][id:{}][key:{}] {}",
            timestamp_str, item.id, item.key, value_str
        );
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
    let mut client = DatastoreClient::connect("http://127.0.0.1:50051").await?;

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
