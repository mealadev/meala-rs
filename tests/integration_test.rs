use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

use meala_core::{read_canonical_parquet, TelemetryStore};

fn temp_store_dir(suffix: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("meala_rs_{}_{}", suffix, nanos))
}

#[test]
fn test_seed_query_and_parquet_roundtrip() {
    let dir = temp_store_dir("roundtrip");
    let store = TelemetryStore::open(&dir).expect("open store");

    let seeded = store.seed_demo_store().expect("seed demo flights");
    assert_eq!(seeded.len(), 4);

    let cold_alerts = store
        .query_flights(None, Some(-18.0), true)
        .expect("query cold alerts");
    assert_eq!(cold_alerts.len(), 2);
    assert!(cold_alerts.iter().all(|f| f.ambient_temp_c <= -18.0 && f.had_alert));

    let stats = store
        .inspect_series("FLT-2019-0114")
        .expect("inspect parquet series");
    assert_eq!(stats.sample_count, 500);
    assert_eq!(stats.min_vbat_v, 0.0);

    let pq_path = dir.join("parquet_store").join("FLT-2019-0114.parquet");
    let rows = read_canonical_parquet(&pq_path).expect("read parquet");
    assert_eq!(rows.len(), 500);
    assert_eq!(rows[0].timestamp_ms, 0.0);

    let _ = fs::remove_dir_all(&dir);
}
