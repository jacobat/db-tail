use chrono::NaiveDateTime;
use clap::Parser;
use colored_json::{Color, ColoredFormatter, CompactFormatter, Styler};
use postgres::{Client, NoTls};
use serde_json::Value;
use std::{thread, time};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    /// Stream name filter
    #[arg(short, long)]
    stream_name_filter: Option<String>,
}

fn main() {
    let args = Args::parse();

    let mut client = Client::connect(
        "dbname=message_store host=localhost user=message_store",
        NoTls,
    )
    .expect("Could not connect to database");
    let mut cursor: i64 = 0;

    let f = ColoredFormatter::with_styler(
        CompactFormatter {},
        Styler {
            key: Color::Green.foreground(),
            string_value: Color::Blue.bold(),
            ..Default::default()
        },
    );

    loop {
        let row = client
            .query_opt(
                "SELECT global_position FROM message_store.messages WHERE global_position = $1",
                &[&cursor],
            )
            .expect("Could not execute query");

        if cursor > 0 && row.is_none() {
            cursor = 0;
            println!("Database was reset, starting over");
        }

        for row in client.query("SELECT global_position, position, id, data, metadata, stream_name, type, time FROM message_store.messages WHERE global_position > $1 ORDER BY global_position", &[&cursor]).expect("Could not execute query") {
            let global_position: i64 = row.get(0);
            let position: i64 = row.get(1);
            let data: Value = row.get(3);
            let pretty_data = f.clone().to_colored_json_auto(&data);
            let metadata: Option<Value> = row.get(4);
            let pretty_metadata = f.clone().to_colored_json_auto(&metadata);
            let stream_name: &str = row.get(5);
            let event_type: &str = row.get(6);
            let time: NaiveDateTime = row.get(7);
            cursor = global_position;

            if let Some(filter) = &args.stream_name_filter {
                if !stream_name.contains(filter) {
                    continue;
                }
            }

            println!("- [ Global Position: {} ] --------------------------",global_position);
            println!("| Stream name | {}", stream_name);
            println!("| Position    | {}", position);
            println!("| Type        | {}", event_type);
            println!("| Time        | {}", time);
            println!("| Data        | {}", pretty_data.unwrap_or("Null".to_string()));
            println!("| Metadata    | {}", pretty_metadata.unwrap_or("Null".to_string()));
        }
        let duration = time::Duration::from_millis(50);

        thread::sleep(duration);
    }
}
