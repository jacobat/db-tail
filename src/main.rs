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

    /// Range of global positions to display
    #[arg(short, long)]
    range: Option<Vec<String>>,

    /// Keep monitoring table for new records
    #[arg(short, long, default_value_t = false)]
    follow: bool,
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

    let ranges: Option<Vec<(i64, i64)>> = args
        .range
        .map(|r| r.iter().map(|s| parse_range(s)).collect());

    if let Some(ranges) = ranges {
        let query = ranges_query(ranges.clone());
        let params = range_params(&ranges);
        let rows = client
            .query(&query, params.as_slice())
            .expect("Could not execute query");
        for row in rows {
            print_row(&row, &f, &args.stream_name_filter);
        }
    } else {
        loop {
            let row = client
                .query_opt(
                    "SELECT global_position FROM message_store.messages WHERE global_position = $1",
                    &[&cursor],
                )
                .expect("Could not execute query");

            if cursor > 0 && row.is_none() {
                cursor = 0;
                print!("{}[2J", 27 as char);
                println!("Database was reset, starting over");
            }

            for row in client.query("SELECT global_position, position, id, data, metadata, stream_name, type, time FROM message_store.messages WHERE global_position > $1 ORDER BY global_position", &[&cursor]).expect("Could not execute query") {
            print_row(&row, &f, &args.stream_name_filter);
            cursor = row.get(0);
        }
            if !args.follow {
                break;
            }
            let duration = time::Duration::from_millis(50);

            thread::sleep(duration);
        }
    }
}

fn print_row(
    row: &postgres::Row,
    f: &ColoredFormatter<CompactFormatter>,
    stream_name_filter: &Option<Vec<String>>,
) {
    let global_position: i64 = row.get(0);
    let position: i64 = row.get(1);
    let data: Value = row.get(3);
    let pretty_data = f.clone().to_colored_json_auto(&data);
    let metadata: Option<Value> = row.get(4);
    let pretty_metadata = f.clone().to_colored_json_auto(&metadata);
    let stream_name: &str = row.get(5);
    let stream_name_color =
        ColorHash::new().rgb(stream_name.split("-").next().unwrap_or("default"));
    let event_type: &str = row.get(6);
    let time: NaiveDateTime = row.get(7);

    if let Some(filters) = stream_name_filter {
        if !filters.iter().any(|filter| stream_name.contains(filter)) {
            return;
        }
    }

    println!();
    println!(
        "- [ Global Position: {} ] --------------------------",
        global_position
    );
    print!("| Stream name | ");
    println_colored(stream_name, &stream_name_color);
    // println!("| Stream name | {}", colorize(stream_name, stream_name_color));
    print!("| Position    | ");
    println_colored(&position.to_string(), &stream_name_color);
    print!("| Type        | ");
    println_colored(event_type, &stream_name_color);
    print!("| Time        | ");
    println_colored(&time.to_string(), &stream_name_color);
    println!(
        "| Data        | {}",
        pretty_data.unwrap_or("Null".to_string())
    );
    println!(
        "| Metadata    | {}",
        pretty_metadata.unwrap_or("Null".to_string())
    );
    println!("----------------------------------------------------");
}

fn ranges_query(ranges: Vec<(i64, i64)>) -> String {
    let query: String = ranges
        .iter()
        .enumerate()
        .map(|(i, _)| {
            let a = 2 * i + 1;
            let b = 2 * i + 2;
            format!("(global_position >= ${} AND global_position <= ${})", a, b)
        })
        .collect::<Vec<String>>()
        .join(" OR ");

    "SELECT global_position, position, id, data, metadata, stream_name, type, time FROM message_store.messages WHERE ".to_string() + &query
}

fn range_params(ranges: &[(i64, i64)]) -> Vec<&(dyn postgres::types::ToSql + Sync)> {
    ranges
        .iter()
        .flat_map(|(start, end)| {
            vec![
                start as &(dyn postgres::types::ToSql + Sync),
                end as &(dyn postgres::types::ToSql + Sync),
            ]
        })
        .collect::<Vec<&(dyn postgres::types::ToSql + Sync)>>()
}

fn parse_range(range: &str) -> (i64, i64) {
    let parts: Vec<&str> = range.split('-').collect();
    if parts.len() != 2 {
        panic!("Invalid range format, expected M-N");
    }
    let start: i64 = parts[0].parse().expect("Invalid start of range");
    let end: i64 = parts[1].parse().expect("Invalid end of range");
    if start > end {
        panic!("Invalid range, start must be less than or equal to end");
    }
    (start, end)
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
