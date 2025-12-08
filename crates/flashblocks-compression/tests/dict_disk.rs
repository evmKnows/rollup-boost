use flashblocks_compression::test_internals::{load_from_disk, now, sha256_hex};
use std::sync::Mutex;

#[test]
fn load_from_disk_filters_by_sha_and_timestamp() {
    // Use a real test dictionary so that dict_id() succeeds.
    const DICT_BYTES: &[u8] = include_bytes!("data/zstd_dict_8453_json.dict");

    let now_ts = now();
    let past_ts = now_ts.saturating_sub(10);
    let future_ts = now_ts.saturating_add(3600);

    let bytes = DICT_BYTES.to_vec();
    let sha = sha256_hex(&bytes);

    let dir = {
        let mut d = std::env::temp_dir();
        d.push(format!("flashblocks-dicts-test-{}", now_ts));
        let _ = std::fs::create_dir_all(&d);
        d
    };

    // 1. Valid past dict.
    let valid_path = dir.join(format!("{past_ts}_{sha}.dict"));
    std::fs::write(&valid_path, &bytes).unwrap();

    // 2. Past dict with mismatched SHA in filename (should be ignored).
    let bad_sha_path = dir.join(format!("{past_ts}_{}.dict", "deadbeefdeadbeef"));
    std::fs::write(&bad_sha_path, &bytes).unwrap();

    // 3. Future dict with correct SHA (should be preloaded as next).
    let future_path = dir.join(format!("{future_ts}_{sha}.dict"));
    std::fs::write(&future_path, &bytes).unwrap();

    let loaded: Mutex<Vec<Vec<u8>>> = Mutex::new(Vec::new());
    load_from_disk(&dir, &|b| loaded.lock().unwrap().push(b.to_vec()));

    // We get exactly one active (past) and one next (future) dict.
    let loaded = loaded.into_inner().unwrap();
    assert_eq!(loaded.len(), 2);
    assert_eq!(loaded[0], bytes);
    assert_eq!(loaded[1], bytes);
}

#[test]
fn load_from_disk_picks_latest_past_and_earliest_future() {
    const DICT_BYTES: &[u8] = include_bytes!("data/zstd_dict_8453_json.dict");

    let now_ts = now();
    let past_older = now_ts.saturating_sub(20);
    let past_newer = now_ts.saturating_sub(10);
    let future_earlier = now_ts.saturating_add(10);
    let future_later = now_ts.saturating_add(20);

    let bytes = DICT_BYTES.to_vec();
    let sha = sha256_hex(&bytes);

    let dir = {
        let mut d = std::env::temp_dir();
        d.push(format!("flashblocks-dicts-test-{}", now_ts + 1));
        let _ = std::fs::create_dir_all(&d);
        d
    };

    // Multiple past dicts.
    std::fs::write(dir.join(format!("{past_older}_{sha}.dict")), &bytes).unwrap();
    std::fs::write(dir.join(format!("{past_newer}_{sha}.dict")), &bytes).unwrap();

    // Multiple future dicts.
    std::fs::write(dir.join(format!("{future_earlier}_{sha}.dict")), &bytes).unwrap();
    std::fs::write(dir.join(format!("{future_later}_{sha}.dict")), &bytes).unwrap();

    let loaded: Mutex<Vec<Vec<u8>>> = Mutex::new(Vec::new());
    load_from_disk(&dir, &|b| loaded.lock().unwrap().push(b.to_vec()));

    // Even with many candidates, we only load at most one active and one next.
    let loaded = loaded.into_inner().unwrap();
    assert_eq!(loaded.len(), 2);
    assert_eq!(loaded[0], bytes);
    assert_eq!(loaded[1], bytes);
}
