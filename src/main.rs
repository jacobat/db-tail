use postgres::{Client, NoTls};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

fn main() {
    let mut client = Client::connect(
        "dbname=message_store host=localhost user=message_store",
        NoTls,
    )
    .expect("Could not connect to database");
    let mut cursor: i64 = 0;

    loop {
        for row in client.query("SELECT global_position, position, id, data, metadata, stream_name FROM message_store.messages WHERE global_position > $1", &[&cursor]).expect("Could not execute query") {
            let global_position: i64 = row.get(0);
            let position: i64 = row.get(1);
            let id: Uuid = row.get(2);
            let data: Value = row.get(3);
            let pretty_data = format_json(data);
            let metadata: Option<Value> = row.get(4);
            let pretty_metadata = metadata.clone().map( |v| format_json(v) );
            let stream_name: &str = row.get(5);
            cursor = global_position;


            println!("[ Pos: {}/{} ] <> [ {} ] <> [ ID: {} ]", global_position, position, stream_name, id);
            println!("| Data     | {}", pretty_data);
            println!("| Metadata | {}", pretty_metadata.unwrap_or("Null".to_string()));
            println!();
        }
    }
    ()
}

fn format_json(value: Value) -> String {
    value
        .as_object()
        .unwrap()
        .into_iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join(" :: ")
}
