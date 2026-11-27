use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use arrow::array::{Array, Float32Array, Float64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::Utc;
use mavlink::ardupilotmega::MavMessage;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};

pub const CANONICAL_COLUMNS: [&str; 12] = [
    "timestamp_ms",
    "lat",
    "lon",
    "alt_m",
    "roll_deg",
    "pitch_deg",
    "yaw_deg",
    "climb_rate_mps",
    "vbat_v",
    "curr_a",
    "vibe_z",
    "throttle_pct",
];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryRow {
    pub timestamp_ms: f64,
    pub lat: f64,
    pub lon: f64,
    pub alt_m: f32,
    pub roll_deg: f32,
    pub pitch_deg: f32,
    pub yaw_deg: f32,
    pub climb_rate_mps: f32,
    pub vbat_v: f32,
    pub curr_a: f32,
    pub vibe_z: f32,
    pub throttle_pct: f32,
}

impl Default for TelemetryRow {
    fn default() -> Self {
        Self {
            timestamp_ms: 0.0,
            lat: 0.0,
            lon: 0.0,
            alt_m: 0.0,
            roll_deg: 0.0,
            pitch_deg: 0.0,
            yaw_deg: 0.0,
            climb_rate_mps: 0.0,
            vbat_v: 0.0,
            curr_a: 0.0,
            vibe_z: 0.0,
            throttle_pct: 0.0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlightRecord {
    pub flight_id: String,
    pub vehicle_uid: String,
    pub battery_uid: Option<String>,
    pub autopilot_type: String,
    pub pilot: Option<String>,
    pub takeoff_time: String,
    pub duration_sec: i64,
    pub takeoff_lat: f64,
    pub takeoff_lon: f64,
    pub min_battery_v: f64,
    pub max_vibe_z: f64,
    pub ambient_temp_c: f64,
    pub had_alert: bool,
    pub notes: String,
    pub parameters_json: String,
    pub parquet_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestOptions {
    pub flight_id: String,
    pub vehicle_uid: String,
    pub battery_uid: String,
    pub autopilot_type: String,
    pub pilot: String,
    pub takeoff_time: Option<String>,
    pub ambient_temp_c: f64,
    pub notes: String,
}

impl Default for IngestOptions {
    fn default() -> Self {
        Self {
            flight_id: "FLT-0001".to_string(),
            vehicle_uid: "TailSitter-VTOL-01".to_string(),
            battery_uid: "BAT-6S-001".to_string(),
            autopilot_type: "ArduPilot".to_string(),
            pilot: "Brandon H.".to_string(),
            takeoff_time: None,
            ambient_temp_c: 15.0,
            notes: String::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeriesStats {
    pub flight_id: String,
    pub sample_count: usize,
    pub duration_sec: f64,
    pub min_vbat_v: f32,
    pub max_vbat_v: f32,
    pub max_alt_m: f32,
    pub max_vibe_z: f32,
    pub mean_vibe_z: f32,
}

pub fn canonical_arrow_schema() -> Schema {
    Schema::new(vec![
        Field::new("timestamp_ms", DataType::Float64, false),
        Field::new("lat", DataType::Float64, false),
        Field::new("lon", DataType::Float64, false),
        Field::new("alt_m", DataType::Float32, false),
        Field::new("roll_deg", DataType::Float32, false),
        Field::new("pitch_deg", DataType::Float32, false),
        Field::new("yaw_deg", DataType::Float32, false),
        Field::new("climb_rate_mps", DataType::Float32, false),
        Field::new("vbat_v", DataType::Float32, false),
        Field::new("curr_a", DataType::Float32, false),
        Field::new("vibe_z", DataType::Float32, false),
        Field::new("throttle_pct", DataType::Float32, false),
    ])
}

pub fn enforce_canonical_rows(mut rows: Vec<TelemetryRow>) -> Vec<TelemetryRow> {
    rows.sort_by(|a, b| {
        a.timestamp_ms
            .partial_cmp(&b.timestamp_ms)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    if let Some(first) = rows.first() {
        let t0 = first.timestamp_ms;
        for r in &mut rows {
            r.timestamp_ms -= t0;
        }
    }
    rows
}

pub struct TelemetryStore {
    pub db_path: PathBuf,
    pub parquet_dir: PathBuf,
    conn: Connection,
}

impl TelemetryStore {
    pub fn open<P: AsRef<Path>>(base_dir: P) -> Result<Self> {
        let base = base_dir.as_ref();
        fs::create_dir_all(base)?;
        let parquet_dir = base.join("parquet_store");
        fs::create_dir_all(&parquet_dir)?;
        let db_path = base.join("meala_flights.db");
        let conn = Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS flights (
                flight_id TEXT PRIMARY KEY,
                vehicle_uid TEXT NOT NULL,
                battery_uid TEXT,
                autopilot_type TEXT NOT NULL,
                pilot TEXT,
                takeoff_time TEXT NOT NULL,
                duration_sec INTEGER DEFAULT 0,
                takeoff_lat REAL DEFAULT 0.0,
                takeoff_lon REAL DEFAULT 0.0,
                min_battery_v REAL DEFAULT 0.0,
                max_vibe_z REAL DEFAULT 0.0,
                ambient_temp_c REAL DEFAULT 15.0,
                had_alert INTEGER DEFAULT 0,
                notes TEXT DEFAULT '',
                parameters_json TEXT DEFAULT '{}',
                parquet_path TEXT NOT NULL
            );",
        )?;
        Ok(Self {
            db_path,
            parquet_dir,
            conn,
        })
    }

    pub fn ingest_rows(
        &self,
        rows: Vec<TelemetryRow>,
        params_map: &HashMap<String, f64>,
        opts: &IngestOptions,
    ) -> Result<FlightRecord> {
        let canonical = enforce_canonical_rows(rows);
        if canonical.is_empty() {
            return Err(anyhow!("Cannot ingest empty telemetry stream"));
        }

        let pq_path = self.parquet_dir.join(format!("{}.parquet", opts.flight_id));
        write_canonical_parquet(&pq_path, &canonical)?;

        let min_v = canonical
            .iter()
            .filter(|r| r.vbat_v > 1.0)
            .map(|r| r.vbat_v as f64)
            .fold(f64::INFINITY, f64::min);
        let min_battery_v = if min_v.is_finite() {
            (min_v * 100.0).round() / 100.0
        } else {
            0.0
        };

        let max_vibe = canonical
            .iter()
            .map(|r| r.vibe_z as f64)
            .fold(0.0_f64, f64::max);
        let max_vibe_z = (max_vibe * 100.0).round() / 100.0;

        let duration_sec = if canonical.len() > 1 {
            ((canonical.last().unwrap().timestamp_ms - canonical.first().unwrap().timestamp_ms)
                / 1000.0)
                .round() as i64
        } else {
            0
        };

        let takeoff_lat = canonical[0].lat;
        let takeoff_lon = canonical[0].lon;
        let had_alert =
            min_battery_v < 20.0 || max_vibe_z > 25.0 || canonical.iter().any(|r| r.vbat_v == 0.0);

        let takeoff_time = opts
            .takeoff_time
            .clone()
            .unwrap_or_else(|| Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string());
        let parameters_json = serde_json::to_string(params_map)?;
        let pq_str = pq_path.to_string_lossy().to_string();

        self.conn.execute(
            "INSERT OR REPLACE INTO flights (
                flight_id, vehicle_uid, battery_uid, autopilot_type, pilot,
                takeoff_time, duration_sec, takeoff_lat, takeoff_lon,
                min_battery_v, max_vibe_z, ambient_temp_c, had_alert,
                notes, parameters_json, parquet_path
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
            params![
                opts.flight_id,
                opts.vehicle_uid,
                opts.battery_uid,
                opts.autopilot_type,
                opts.pilot,
                takeoff_time,
                duration_sec,
                takeoff_lat,
                takeoff_lon,
                min_battery_v,
                max_vibe_z,
                opts.ambient_temp_c,
                if had_alert { 1 } else { 0 },
                opts.notes,
                parameters_json,
                pq_str,
            ],
        )?;

        Ok(FlightRecord {
            flight_id: opts.flight_id.clone(),
            vehicle_uid: opts.vehicle_uid.clone(),
            battery_uid: Some(opts.battery_uid.clone()),
            autopilot_type: opts.autopilot_type.clone(),
            pilot: Some(opts.pilot.clone()),
            takeoff_time,
            duration_sec,
            takeoff_lat,
            takeoff_lon,
            min_battery_v,
            max_vibe_z,
            ambient_temp_c: opts.ambient_temp_c,
            had_alert,
            notes: opts.notes.clone(),
            parameters_json,
            parquet_path: pq_str,
        })
    }

    pub fn ingest_file<P: AsRef<Path>>(&self, path: P, opts: &IngestOptions) -> Result<FlightRecord> {
        let p = path.as_ref();
        let ext = p
            .extension()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_lowercase();

        let (rows, params_map) = match ext.as_str() {
            "csv" => (parse_csv_file(p)?, HashMap::new()),
            "tlog" => parse_mavlink_tlog(p)?,
            _ => {
                return Err(anyhow!(
                    "Unsupported file extension '.{}' (supported: .tlog, .csv)",
                    ext
                ))
            }
        };

        self.ingest_rows(rows, &params_map, opts)
    }

    pub fn query_flights(
        &self,
        autopilot_type: Option<&str>,
        max_temp_c: Option<f64>,
        only_alerts: bool,
    ) -> Result<Vec<FlightRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT flight_id, vehicle_uid, battery_uid, autopilot_type, pilot,
                    takeoff_time, duration_sec, takeoff_lat, takeoff_lon,
                    min_battery_v, max_vibe_z, ambient_temp_c, had_alert,
                    notes, parameters_json, parquet_path
             FROM flights
             ORDER BY takeoff_time DESC",
        )?;

        let iter = stmt.query_map([], |row| {
            let alert_int: i64 = row.get(12)?;
            Ok(FlightRecord {
                flight_id: row.get(0)?,
                vehicle_uid: row.get(1)?,
                battery_uid: row.get(2)?,
                autopilot_type: row.get(3)?,
                pilot: row.get(4)?,
                takeoff_time: row.get(5)?,
                duration_sec: row.get(6)?,
                takeoff_lat: row.get(7)?,
                takeoff_lon: row.get(8)?,
                min_battery_v: row.get(9)?,
                max_vibe_z: row.get(10)?,
                ambient_temp_c: row.get(11)?,
                had_alert: alert_int != 0,
                notes: row.get(13)?,
                parameters_json: row.get(14)?,
                parquet_path: row.get(15)?,
            })
        })?;

        let mut results = Vec::new();
        for item in iter {
            let rec = item?;
            if let Some(stack) = autopilot_type {
                if !rec.autopilot_type.eq_ignore_ascii_case(stack) {
                    continue;
                }
            }
            if let Some(max_t) = max_temp_c {
                if rec.ambient_temp_c > max_t {
                    continue;
                }
            }
            if only_alerts && !rec.had_alert {
                continue;
            }
            results.push(rec);
        }
        Ok(results)
    }

    pub fn inspect_series(&self, flight_id: &str) -> Result<SeriesStats> {
        let pq_path = self.parquet_dir.join(format!("{}.parquet", flight_id));
        if !pq_path.exists() {
            return Err(anyhow!("Parquet file not found for {}", flight_id));
        }
        let rows = read_canonical_parquet(&pq_path)?;
        let n = rows.len();
        let duration_sec = if n > 1 {
            (rows.last().unwrap().timestamp_ms - rows.first().unwrap().timestamp_ms) / 1000.0
        } else {
            0.0
        };
        let min_vbat_v = rows
            .iter()
            .filter(|r| r.vbat_v > 1.0)
            .map(|r| r.vbat_v)
            .fold(f32::INFINITY, f32::min);
        let max_vbat_v = rows.iter().map(|r| r.vbat_v).fold(0.0_f32, f32::max);
        let max_alt_m = rows.iter().map(|r| r.alt_m).fold(0.0_f32, f32::max);
        let max_vibe_z = rows.iter().map(|r| r.vibe_z).fold(0.0_f32, f32::max);
        let mean_vibe_z = if n > 0 {
            rows.iter().map(|r| r.vibe_z).sum::<f32>() / (n as f32)
        } else {
            0.0
        };

        Ok(SeriesStats {
            flight_id: flight_id.to_string(),
            sample_count: n,
            duration_sec,
            min_vbat_v: if min_vbat_v.is_finite() { min_vbat_v } else { 0.0 },
            max_vbat_v,
            max_alt_m,
            max_vibe_z,
            mean_vibe_z,
        })
    }

    pub fn seed_demo_store(&self) -> Result<Vec<FlightRecord>> {
        let existing = self.query_flights(None, None, false)?;
        if !existing.is_empty() {
            return Ok(existing);
        }

        let samples = [
            (
                "FLT-2019-0114",
                "TailSitter-VTOL-01",
                "BAT-6S-002",
                "ArduPilot",
                "Fiona M.",
                "2019-01-14T09:42:00Z",
                -21.5,
                true,
                101_u64,
                "Cold weather electrolytic capacitor voltage spike failure.",
                HashMap::from([("BATT_CRT_VOLT".into(), 21.0), ("INS_ACCEL_FILTER".into(), 20.0)]),
            ),
            (
                "FLT-2019-1219",
                "TailSitter-VTOL-01",
                "BAT-6S-005",
                "ArduPilot",
                "Tim R.",
                "2019-12-19T10:15:00Z",
                -20.2,
                true,
                202_u64,
                "Repeated winter shutdown before Christmas.",
                HashMap::from([("BATT_CRT_VOLT".into(), 21.0), ("INS_ACCEL_FILTER".into(), 20.0)]),
            ),
            (
                "FLT-2026-0208",
                "TailSitter-VTOL-02",
                "BAT-6S-011",
                "PX4",
                "Brandon H.",
                "2026-02-08T14:10:00Z",
                -17.0,
                false,
                303_u64,
                "Upgraded to solid polymer ESC capacitors. Nominal flight.",
                HashMap::from([("BAT1_V_CRIT".into(), 21.0), ("MC_ROLL_P".into(), 6.5)]),
            ),
            (
                "FLT-2026-0514",
                "Mapper-Quad-04",
                "BAT-4S-019",
                "ArduPilot",
                "Brandon H.",
                "2026-05-14T11:20:00Z",
                18.4,
                false,
                404_u64,
                "PID tuning flight; roll/pitch solid, yaw needs slightly more D.",
                HashMap::from([("ATC_RAT_RLL_P".into(), 0.135), ("ATC_RAT_YAW_D".into(), 0.004)]),
            ),
        ];

        for (fid, veh, bat, stack, pilot, dt, temp, is_cold, seed, notes, params_map) in samples {
            let rows = generate_synthetic_flight(seed, is_cold);
            let opts = IngestOptions {
                flight_id: fid.into(),
                vehicle_uid: veh.into(),
                battery_uid: bat.into(),
                autopilot_type: stack.into(),
                pilot: pilot.into(),
                takeoff_time: Some(dt.into()),
                ambient_temp_c: temp,
                notes: notes.into(),
            };
            self.ingest_rows(rows, &params_map, &opts)?;
        }

        self.query_flights(None, None, false)
    }
}

pub fn generate_synthetic_flight(seed: u64, cold_failure: bool) -> Vec<TelemetryRow> {
    let n = 500_usize;
    let mut state = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut next_unit = || -> f64 {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        ((state >> 11) as f64) / ((1_u64 << 53) as f64) - 0.5
    };

    let mut rows = Vec::with_capacity(n);
    let mut lat = 43.7764_f64;
    let mut lon = -79.3472_f64;

    for i in 0..n {
        let frac = (i as f64) / ((n - 1) as f64);
        let t_ms = frac * 420_000.0;
        lat += next_unit() * 0.00005;
        lon += next_unit() * 0.00005;

        let alt = if i < 100 {
            120.0 * (i as f32 / 100.0)
        } else if i < 400 {
            120.0 + (next_unit() as f32) * 2.0
        } else {
            let rem = (500 - i) as f32 / 100.0;
            if cold_failure {
                (120.0 * rem).powf(1.03)
            } else {
                120.0 * rem
            }
        };

        let mut vbat = 25.2_f32 - (2.8_f32 * frac as f32) + (next_unit() as f32) * 0.08;
        let mut throttle = 54.0_f32 + (next_unit() as f32) * 3.5;

        if cold_failure {
            if i >= n - 20 {
                vbat = 0.0;
                throttle = 0.0;
            } else if i >= n - 75 {
                let drop_frac = (i - (n - 75)) as f32 / 55.0;
                vbat = 21.6 - (3.5 * drop_frac);
            }
        }

        let vibe_base = if cold_failure { 28.2_f32 } else { 11.5_f32 };
        let vibe_z = vibe_base + (next_unit() as f32) * 3.5;
        let climb_rate = if i < 100 {
            1.4
        } else if i < 400 {
            (next_unit() as f32) * 0.4
        } else {
            -1.4
        };

        rows.push(TelemetryRow {
            timestamp_ms: t_ms,
            lat,
            lon,
            alt_m: alt,
            roll_deg: (next_unit() as f32) * 6.0,
            pitch_deg: 2.0 + (next_unit() as f32) * 4.0,
            yaw_deg: ((frac * 180.0) % 360.0) as f32,
            climb_rate_mps: climb_rate,
            vbat_v: vbat,
            curr_a: throttle * 0.45,
            vibe_z,
            throttle_pct: throttle,
        });
    }

    enforce_canonical_rows(rows)
}

pub fn parse_csv_file<P: AsRef<Path>>(path: P) -> Result<Vec<TelemetryRow>> {
    let mut rdr = csv::Reader::from_path(path)?;
    let mut rows = Vec::new();
    for rec in rdr.deserialize() {
        let row: TelemetryRow = rec?;
        rows.push(row);
    }
    Ok(enforce_canonical_rows(rows))
}

pub fn parse_mavlink_tlog<P: AsRef<Path>>(path: P) -> Result<(Vec<TelemetryRow>, HashMap<String, f64>)> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut rows = Vec::new();
    let mut params_map = HashMap::new();
    let mut state = TelemetryRow::default();
    let mut ts_buf = [0_u8; 8];

    loop {
        // MAVLink .tlog files prepend an 8-byte big-endian microsecond timestamp before each frame
        if reader.read_exact(&mut ts_buf).is_err() {
            break;
        }
        let ts_us = u64::from_be_bytes(ts_buf);
        let ts_ms = (ts_us as f64) / 1000.0;

        let Ok((_header, msg)) = mavlink::read_v2_msg::<MavMessage, _>(&mut reader) else {
            break;
        };

        match msg {
            MavMessage::ATTITUDE(d) => {
                state.roll_deg = d.roll.to_degrees();
                state.pitch_deg = d.pitch.to_degrees();
                state.yaw_deg = d.yaw.to_degrees().rem_euclid(360.0);
            }
            MavMessage::GLOBAL_POSITION_INT(d) => {
                state.lat = (d.lat as f64) / 1e7;
                state.lon = (d.lon as f64) / 1e7;
                state.alt_m = (d.alt as f32) / 1000.0;
                state.climb_rate_mps = -(d.vz as f32) / 100.0;
            }
            MavMessage::SYS_STATUS(d) => {
                state.vbat_v = (d.voltage_battery as f32) / 1000.0;
                state.curr_a = (d.current_battery as f32) / 100.0;
            }
            MavMessage::VIBRATION(d) => {
                state.vibe_z = d.vibration_z;
            }
            MavMessage::VFR_HUD(d) => {
                state.climb_rate_mps = d.climb;
                state.throttle_pct = d.throttle as f32;
            }
            MavMessage::PARAM_VALUE(d) => {
                let name = String::from_utf8_lossy(&d.param_id)
                    .trim_end_matches(' ')
                    .to_string();
                if !name.is_empty() {
                    params_map.insert(name, d.param_value as f64);
                }
                continue;
            }
            _ => {}
        }

        if ts_ms > 0.0 {
            let mut row = state.clone();
            row.timestamp_ms = ts_ms;
            rows.push(row);
        }
    }

    if rows.is_empty() {
        return Err(anyhow!("No telemetry frames extracted from .tlog stream"));
    }

    Ok((enforce_canonical_rows(rows), params_map))
}

pub fn write_canonical_parquet<P: AsRef<Path>>(path: P, rows: &[TelemetryRow]) -> Result<()> {
    let schema = Arc::new(canonical_arrow_schema());
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Float64Array::from(
                rows.iter().map(|r| r.timestamp_ms).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter().map(|r| r.lat).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter().map(|r| r.lon).collect::<Vec<_>>(),
            )),
            Arc::new(Float32Array::from(
                rows.iter().map(|r| r.alt_m).collect::<Vec<_>>(),
            )),
            Arc::new(Float32Array::from(
                rows.iter().map(|r| r.roll_deg).collect::<Vec<_>>(),
            )),
            Arc::new(Float32Array::from(
                rows.iter().map(|r| r.pitch_deg).collect::<Vec<_>>(),
            )),
            Arc::new(Float32Array::from(
                rows.iter().map(|r| r.yaw_deg).collect::<Vec<_>>(),
            )),
            Arc::new(Float32Array::from(
                rows.iter().map(|r| r.climb_rate_mps).collect::<Vec<_>>(),
            )),
            Arc::new(Float32Array::from(
                rows.iter().map(|r| r.vbat_v).collect::<Vec<_>>(),
            )),
            Arc::new(Float32Array::from(
                rows.iter().map(|r| r.curr_a).collect::<Vec<_>>(),
            )),
            Arc::new(Float32Array::from(
                rows.iter().map(|r| r.vibe_z).collect::<Vec<_>>(),
            )),
            Arc::new(Float32Array::from(
                rows.iter().map(|r| r.throttle_pct).collect::<Vec<_>>(),
            )),
        ],
    )?;

    let file = File::create(path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

pub fn read_canonical_parquet<P: AsRef<Path>>(path: P) -> Result<Vec<TelemetryRow>> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;
    let mut out = Vec::new();

    for batch_res in reader {
        let batch = batch_res?;
        let ts = batch
            .column_by_name("timestamp_ms")
            .context("missing timestamp_ms")?
            .as_any()
            .downcast_ref::<Float64Array>()
            .context("invalid timestamp_ms type")?;
        let lat = batch
            .column_by_name("lat")
            .context("missing lat")?
            .as_any()
            .downcast_ref::<Float64Array>()
            .context("invalid lat type")?;
        let lon = batch
            .column_by_name("lon")
            .context("missing lon")?
            .as_any()
            .downcast_ref::<Float64Array>()
            .context("invalid lon type")?;
        let alt = batch
            .column_by_name("alt_m")
            .context("missing alt_m")?
            .as_any()
            .downcast_ref::<Float32Array>()
            .context("invalid alt_m type")?;
        let roll = batch
            .column_by_name("roll_deg")
            .context("missing roll_deg")?
            .as_any()
            .downcast_ref::<Float32Array>()
            .context("invalid roll_deg type")?;
        let pitch = batch
            .column_by_name("pitch_deg")
            .context("missing pitch_deg")?
            .as_any()
            .downcast_ref::<Float32Array>()
            .context("invalid pitch_deg type")?;
        let yaw = batch
            .column_by_name("yaw_deg")
            .context("missing yaw_deg")?
            .as_any()
            .downcast_ref::<Float32Array>()
            .context("invalid yaw_deg type")?;
        let climb = batch
            .column_by_name("climb_rate_mps")
            .context("missing climb_rate_mps")?
            .as_any()
            .downcast_ref::<Float32Array>()
            .context("invalid climb_rate_mps type")?;
        let vbat = batch
            .column_by_name("vbat_v")
            .context("missing vbat_v")?
            .as_any()
            .downcast_ref::<Float32Array>()
            .context("invalid vbat_v type")?;
        let curr = batch
            .column_by_name("curr_a")
            .context("missing curr_a")?
            .as_any()
            .downcast_ref::<Float32Array>()
            .context("invalid curr_a type")?;
        let vibe = batch
            .column_by_name("vibe_z")
            .context("missing vibe_z")?
            .as_any()
            .downcast_ref::<Float32Array>()
            .context("invalid vibe_z type")?;
        let thr = batch
            .column_by_name("throttle_pct")
            .context("missing throttle_pct")?
            .as_any()
            .downcast_ref::<Float32Array>()
            .context("invalid throttle_pct type")?;

        for i in 0..batch.num_rows() {
            out.push(TelemetryRow {
                timestamp_ms: ts.value(i),
                lat: lat.value(i),
                lon: lon.value(i),
                alt_m: alt.value(i),
                roll_deg: roll.value(i),
                pitch_deg: pitch.value(i),
                yaw_deg: yaw.value(i),
                climb_rate_mps: climb.value(i),
                vbat_v: vbat.value(i),
                curr_a: curr.value(i),
                vibe_z: vibe.value(i),
                throttle_pct: thr.value(i),
            });
        }
    }

    Ok(out)
}

pub fn push_log_to_server<P: AsRef<Path>>(
    server_url: &str,
    api_key: &str,
    file_path: P,
    opts: &IngestOptions,
) -> Result<String> {
    let endpoint = format!("{}/api/v1/flights/upload", server_url.trim_end_matches('/'));
    let form = reqwest::blocking::multipart::Form::new()
        .file("file", file_path.as_ref())?
        .text("vehicle_uid", opts.vehicle_uid.clone())
        .text("battery_uid", opts.battery_uid.clone())
        .text("autopilot_type", opts.autopilot_type.clone())
        .text("pilot", opts.pilot.clone())
        .text("ambient_temp_c", opts.ambient_temp_c.to_string())
        .text("notes", opts.notes.clone());

    let client = reqwest::blocking::Client::new();
    let res = client
        .post(&endpoint)
        .header("X-API-Key", api_key)
        .multipart(form)
        .send()?;

    let status = res.status();
    let body = res.text()?;
    if !status.is_success() {
        return Err(anyhow!("Server upload failed ({}): {}", status, body));
    }
    Ok(body)
}
