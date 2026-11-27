use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use meala_core::{push_log_to_server, IngestOptions, TelemetryStore};

#[derive(Parser)]
#[command(
    name = "meala-rs",
    version,
    about = "Native Rust CLI for Meala UAV flight log normalization (MAVLink/CSV -> Parquet + SQLite) and server sync."
)]
struct Cli {
    /// Base data directory containing meala_flights.db and parquet_store/
    #[arg(long, default_value = "./data")]
    data_dir: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Seed the local SQLite + Parquet store with realistic demo flights
    Seed,

    /// Ingest a .tlog or .csv flight log into canonical Parquet + SQLite
    Ingest {
        /// Path to .tlog or .csv log file
        file: PathBuf,

        #[arg(long)]
        flight_id: String,

        #[arg(long, default_value = "TailSitter-VTOL-01")]
        vehicle: String,

        #[arg(long, default_value = "BAT-6S-001")]
        battery: String,

        #[arg(long, default_value = "ArduPilot")]
        stack: String,

        #[arg(long, default_value = "Brandon H.")]
        pilot: String,

        #[arg(long, default_value_t = 15.0)]
        temp_c: f64,

        #[arg(long, default_value = "")]
        notes: String,
    },

    /// Query indexed flights from the local SQLite metadata store
    Query {
        #[arg(long)]
        stack: Option<String>,

        #[arg(long)]
        max_temp: Option<f64>,

        #[arg(long, default_value_t = false)]
        alerts: bool,
    },

    /// Inspect summary telemetry statistics from a flight's Apache Parquet file
    Inspect {
        flight_id: String,
    },

    /// Upload a raw flight log directly to a self-hosted mealadev/meala server
    Push {
        file: PathBuf,

        #[arg(long, default_value = "http://localhost:8000")]
        server: String,

        #[arg(long, env = "ORG_API_KEY")]
        api_key: String,

        #[arg(long, default_value = "TailSitter-VTOL-01")]
        vehicle: String,

        #[arg(long, default_value = "BAT-6S-001")]
        battery: String,

        #[arg(long, default_value = "ArduPilot")]
        stack: String,

        #[arg(long, default_value = "Brandon H.")]
        pilot: String,

        #[arg(long, default_value_t = 15.0)]
        temp_c: f64,

        #[arg(long, default_value = "")]
        notes: String,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Seed => {
            let store = TelemetryStore::open(&cli.data_dir)?;
            let flights = store.seed_demo_store()?;
            println!(
                "Seeded {} flights into {}",
                flights.len(),
                cli.data_dir.display()
            );
        }
        Commands::Ingest {
            file,
            flight_id,
            vehicle,
            battery,
            stack,
            pilot,
            temp_c,
            notes,
        } => {
            let store = TelemetryStore::open(&cli.data_dir)?;
            let opts = IngestOptions {
                flight_id,
                vehicle_uid: vehicle,
                battery_uid: battery,
                autopilot_type: stack,
                pilot,
                takeoff_time: None,
                ambient_temp_c: temp_c,
                notes,
            };
            let rec = store.ingest_file(&file, &opts)?;
            println!(
                "Ingested {} -> {} (min_v={:.2}V, max_vibe={:.2}, alert={})",
                rec.flight_id, rec.parquet_path, rec.min_battery_v, rec.max_vibe_z, rec.had_alert
            );
        }
        Commands::Query {
            stack,
            max_temp,
            alerts,
        } => {
            let store = TelemetryStore::open(&cli.data_dir)?;
            let flights = store.query_flights(stack.as_deref(), max_temp, alerts)?;
            println!(
                "{:<16} {:<20} {:<10} {:>7} {:>8} {:>7}",
                "FLIGHT_ID", "VEHICLE", "STACK", "TEMP_C", "MIN_VBAT", "ALERT"
            );
            for f in flights {
                println!(
                    "{:<16} {:<20} {:<10} {:>6.1}° {:>7.2}V {:>7}",
                    f.flight_id,
                    f.vehicle_uid,
                    f.autopilot_type,
                    f.ambient_temp_c,
                    f.min_battery_v,
                    if f.had_alert { "YES" } else { "NO" }
                );
            }
        }
        Commands::Inspect { flight_id } => {
            let store = TelemetryStore::open(&cli.data_dir)?;
            let stats = store.inspect_series(&flight_id)?;
            println!("{}", serde_json::to_string_pretty(&stats)?);
        }
        Commands::Push {
            file,
            server,
            api_key,
            vehicle,
            battery,
            stack,
            pilot,
            temp_c,
            notes,
        } => {
            let opts = IngestOptions {
                flight_id: String::new(),
                vehicle_uid: vehicle,
                battery_uid: battery,
                autopilot_type: stack,
                pilot,
                takeoff_time: None,
                ambient_temp_c: temp_c,
                notes,
            };
            let resp = push_log_to_server(&server, &api_key, &file, &opts)?;
            println!("Server response: {}", resp);
        }
    }

    Ok(())
}
