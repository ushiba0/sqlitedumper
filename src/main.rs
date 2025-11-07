use clap::Parser;

// Constants and command line options.
const DB_DEFAULT_FILENAME: &str = "appliance_stats.sqlite";
const DUMP_OUTPUT_DIR_DEFAULT: &str = "sqlite_dump";

#[derive(Debug, Parser)]
struct CommandArguments {
    /// SQLite database file.
    #[clap(short, long, default_value = DB_DEFAULT_FILENAME)]
    file: String,

    /// Log level. One of trace, debug, info, wanr, error.
    #[clap(short, long, default_value = "NONE")]
    log: String,

    /// Output directory.
    #[clap(short, long, default_value = DUMP_OUTPUT_DIR_DEFAULT)]
    dir: String,
}

fn set_loglevel(loglevel: &str) {
    unsafe {
        std::env::set_var("RUST_LOG", loglevel);
    }
}

fn handle_cmd_args() -> Result<CommandArguments, Box<dyn std::error::Error>> {
    let cli_commands = CommandArguments::parse();
    set_loglevel("INFO");

    match cli_commands.log.to_lowercase().as_str() {
        "error" | "warn" | "info" | "debug" | "trace" | "none" => {
            set_loglevel(&cli_commands.log.to_uppercase())
        }
        other => return Err(format!("Invalid log level '{other}'").into()),
    }

    env_logger::init();
    log::debug!("{cli_commands:?}");
    Ok(cli_commands)
}

async fn dump_table(table_name: &str, dump_dir: &str) -> Result<(), Box<dyn std::error::Error>> {
    log::info!("Dumping table {table_name}");
    let start_time = std::time::Instant::now();

    let file = {
        let dir = std::path::Path::new(dump_dir);
        let path = dir.join(format!("{table_name}.csv"));
        tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .await?
            .into_std()
            .await
    };
    let conn = create_db_connection_ro()?;
    let query = format!("SELECT * FROM '{}'", table_name);
    let mut stmt = conn.prepare(&query)?;
    let column_count = stmt.column_count();
    let mut column_name: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
    let mut csv_writer = csv::Writer::from_writer(file);
    let  timestamp_position= column_name.iter().position(|name|name == "sm_timestamp" || name == "timestamp");

    if let Some(pos) = timestamp_position {
        column_name.insert(pos+1, "timestamp_parsed".to_string());
    }    

    // Write header;
    csv_writer.write_record(&column_name)?;

    log::info!("Column name: {column_name:?}");
    let mut rows = stmt.query([])?;

    while let Some(row) = rows.next()? {
        for i in 0..column_count {
            use rusqlite::types::Type;
            let val = row.get_ref(i)?;
            let txt = match val.data_type() {
                Type::Text => val.as_str()?.to_string(),
                Type::Integer => val.as_i64()?.to_string(),
                Type::Real => val.as_f64()?.to_string(),
                Type::Blob => format!("{:?}", val.as_blob()?),
                Type::Null => "null".to_string(),
            };
            csv_writer.write_field(&txt)?;
            if let Some(pos) = timestamp_position {
                if pos == i {
                    use chrono::prelude::*;
                    let ts = txt.parse::<i64>()?;
                    let datetime = Utc.timestamp_opt(ts, 0).unwrap();
                    let a =     datetime.to_rfc3339_opts(SecondsFormat::Secs, true);
                    csv_writer.write_field(&a)?;
                }
            }
        }
        csv_writer.write_record(None::<&[u8]>)?; // 改行
    }

    drop(rows);
    drop(stmt);
    match conn.close() {
        Ok(()) => {}
        Err((_, err)) => {
            log::error!("Error while closing db connection. {err}");
        }
    }

    log::info!(
        "Table {table_name} dump completed. Elapsed {} ms",
        start_time.elapsed().as_millis()
    );
    Ok(())
}

fn create_db_connection_ro() -> Result<rusqlite::Connection, Box<dyn std::error::Error>> {
    let conn = rusqlite::Connection::open_with_flags(
        "appliance_stats.sqlite",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    Ok(conn)
}

fn get_tables() -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let conn = create_db_connection_ro()?;
    let mut stmt = conn.prepare(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';",
    )?;
    let table_names_row = stmt.query_map([], |row| row.get::<_, String>(0))?;
    let mut table_names = Vec::new();
    for table_name_res in table_names_row {
        match table_name_res {
            Ok(table_name) => {
                log::info!("Found table name {table_name}");
                table_names.push(table_name);
            }
            Err(e) => log::error!("⚠️ Error while getting table name {e}"),
        }
    }

    drop(stmt);
    match conn.close() {
        Ok(()) => {}
        Err((_, err)) => {
            log::error!("Error while closing db connection. {err}");
        }
    }
    Ok(table_names)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let start_time = std::time::Instant::now();
    let cli_commands = handle_cmd_args()?;
    let table_names = get_tables()?;

    // ダンプ先ディレクトリ作成
    let dump_path = std::path::Path::new(&cli_commands.dir);
    tokio::fs::create_dir_all(dump_path).await?;

    let mut joinhandles = Vec::new();

    for tbl_name in table_names.iter() {
        let table_name = tbl_name.to_string();
        let dump_dir = cli_commands.dir.clone();
        let jh = tokio::spawn(async move {
            let result = dump_table(&table_name, &dump_dir).await;
            match result {
                Ok(()) => {}
                Err(e) => log::error!("Error while handling table {table_name}. {e}"),
            }
        });
        log::debug!("Thread {} spawned.", tbl_name);
        joinhandles.push(jh);
    }

    for handle in joinhandles {
        match handle.await {
            Ok(_) => {}
            Err(e) => {
                eprintln!("Error {e:?}");
            }
        }
    }

    log::info!(
        "Dump {} completed. Elapsed {} ms",
        cli_commands.file,
        start_time.elapsed().as_millis()
    );
    Ok(())
}
