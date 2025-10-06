use chrono::NaiveDateTime;
use clap::Parser;
use colored::Colorize;
use colored_json::{Color, ColoredFormatter, CompactFormatter, Styler};
use colorhash::{ColorHash, Rgb};
use postgres::{Client, NoTls};
use serde_json::Value;
use std::{thread, time};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    /// Stream name filter
    #[arg(short, long)]
    stream_name_filter: Option<Vec<String>>,
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
            let stream_name_color = ColorHash::new().rgb(stream_name.split("-").next().unwrap_or("default"));
            let event_type: &str = row.get(6);
            let time: NaiveDateTime = row.get(7);
            cursor = global_position;

            if let Some(filters) = &args.stream_name_filter {
                if !filters.iter().any(|filter| stream_name.contains(filter)) {
                    continue;
                }
            }

            println!();
            println!("- [ Global Position: {} ] --------------------------",global_position);
            print!("| Stream name | ");
            println_colored(stream_name, &stream_name_color);
            // println!("| Stream name | {}", colorize(stream_name, stream_name_color));
            print!("| Position    | ");
            println_colored(&position.to_string(), &stream_name_color);
            print!("| Type        | ");
            println_colored(event_type, &stream_name_color);
            print!("| Time        | ");
            println_colored(&time.to_string(), &stream_name_color);
            println!("| Data        | {}", pretty_data.unwrap_or("Null".to_string()));
            println!("| Metadata    | {}", pretty_metadata.unwrap_or("Null".to_string()));
            println!("----------------------------------------------------");
        }
        let duration = time::Duration::from_millis(50);

        thread::sleep(duration);
    }
}

fn println_colored(string: &str, stream_name_color: &Rgb) {
    println!("{}", colorize(string, stream_name_color));
}

fn colorize(string: &str, stream_name_color: &Rgb) -> colored::ColoredString {
    string.truecolor(
        make_light(stream_name_color.red()),
        make_light(stream_name_color.green()),
        make_light(stream_name_color.blue()),
    )
}

fn make_light(color: f64) -> u8 {
    ((color / 2.0) + 128.0) as u8
}
