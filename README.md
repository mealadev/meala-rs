# Meala Rust (`mealadev/meala-rs`)

**Zero-runtime, single-binary Rust CLI and edge ingestion library for the [Meala](https://meala.dev) UAV flight log ecosystem.**

Designed for companion computers (Raspberry Pi, NVIDIA Jetson, CubePilot carrier boards) and ground-control laptops, `meala-rs` parses MAVLink `.tlog` and `.csv` telemetry streams into the exact same **SQLite + Snappy Apache Parquet** canonical schema shared across [`mealadev/meala`](https://github.com/mealadev/meala), [`mealadev/meala-jupyter`](https://github.com/mealadev/meala-jupyter), and [`mealadev/meala-julia`](https://github.com/mealadev/meala-julia).

---

## Features

* **100% Canonical Schema Interoperability:** Reads and writes `meala_flights.db` (SQLite) and Snappy-compressed `parquet_store/*.parquet` (`Float64`/`Float32` SI-unit columns) natively via `arrow`, `parquet`, and `rusqlite`.
* **Native MAVLink `.tlog` Parser:** Extracts `ATTITUDE`, `GLOBAL_POSITION_INT`, `SYS_STATUS`, `VIBRATION`, `VFR_HUD`, and onboard `PARAM_VALUE` parameters using `rust-mavlink` with zero Python overhead.
* **Edge-to-Server Sync (`meala-rs push`):** Uploads flight logs straight to a self-hosted `mealadev/meala` server (`POST /api/v1/flights/upload`) with `X-API-Key` authentication.
* **Sub-Millisecond CLI Queries:** Filter flights by autopilot stack, ambient temperature, or anomaly flags directly from the terminal.

---

## Build & Quickstart

```bash
git clone https://github.com/mealadev/meala-rs.git
cd meala-rs
cargo test
cargo build --release
```

### CLI Examples

```bash
# Seed local demo store
./target/release/meala-rs seed

# Query cold-weather anomaly flights
./target/release/meala-rs query --max-temp -18 --alerts

# Ingest a MAVLink .tlog file into local Parquet + SQLite
./target/release/meala-rs ingest flight_042.tlog   --flight-id FLT-2026-0601   --vehicle TailSitter-VTOL-01   --stack ArduPilot   --temp-c -12.4

# Inspect Parquet summary statistics
./target/release/meala-rs inspect FLT-2026-0601

# Push a flight log directly to a self-hosted mealadev/meala server
./target/release/meala-rs push flight_042.tlog   --server http://localhost:8000   --api-key change_me_org_ingestion_key
```

---

## License

MIT — see [meala.dev](https://meala.dev) for full cross-language architecture documentation.
