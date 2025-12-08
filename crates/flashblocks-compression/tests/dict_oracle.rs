use std::{net::SocketAddr, sync::Arc};

use axum::{
    Router,
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::Response,
    routing::{get, head},
};
use flashblocks_compression::{StreamDecoder, StreamEncoder, dict_id};
use tokio::net::TcpListener;

const TEST_JSON: &str = r#"{"id":1,"method":"test","params":[]}"#;

#[derive(Clone)]
struct OracleDict {
    alg: &'static str,
    dict_id: u32,
    sha: String,
    bytes: &'static [u8],
}

#[derive(Clone)]
struct OracleState {
    active: OracleDict,
    next: Option<OracleDict>,
}

async fn oracle_head(State(state): State<Arc<tokio::sync::Mutex<OracleState>>>) -> Response {
    let state = state.lock().await;
    let mut headers = HeaderMap::new();
    headers.insert(
        "Flashblocks-Active-Compression-Alg",
        HeaderValue::from_static("zstd"),
    );
    headers.insert(
        "Flashblocks-Active-Compression-Dict-Id",
        HeaderValue::from_str(&state.active.dict_id.to_string()).unwrap(),
    );
    headers.insert(
        "Flashblocks-Active-Compression-Dict-Sha",
        HeaderValue::from_str(&state.active.sha).unwrap(),
    );
    if let Some(next) = &state.next {
        headers.insert(
            "Flashblocks-Next-Compression-Alg",
            HeaderValue::from_static(next.alg),
        );
        headers.insert(
            "Flashblocks-Next-Compression-Dict-Id",
            HeaderValue::from_str(&next.dict_id.to_string()).unwrap(),
        );
        headers.insert(
            "Flashblocks-Next-Compression-Dict-Sha",
            HeaderValue::from_str(&next.sha).unwrap(),
        );
        headers.insert(
            "Flashblocks-Next-Compression-Activation-Time",
            HeaderValue::from_static("1"),
        );
    }
    let mut response = Response::builder()
        .status(StatusCode::OK)
        .body(Body::empty())
        .unwrap();
    *response.headers_mut() = headers;
    response
}

async fn oracle_get_root(State(state): State<Arc<tokio::sync::Mutex<OracleState>>>) -> Response {
    let state = state.lock().await;
    Response::builder()
        .status(StatusCode::OK)
        .body(Body::from(state.active.bytes.to_vec()))
        .unwrap()
}

async fn oracle_get_sha(
    State(state): State<Arc<tokio::sync::Mutex<OracleState>>>,
    Path(sha): Path<String>,
) -> Response {
    let state = state.lock().await;
    if state.active.sha == sha {
        return Response::builder()
            .status(StatusCode::OK)
            .body(Body::from(state.active.bytes.to_vec()))
            .unwrap();
    }
    if let Some(next) = &state.next
        && next.sha == sha {
            return Response::builder()
                .status(StatusCode::OK)
                .body(Body::from(next.bytes.to_vec()))
                .unwrap();
        }
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Body::empty())
        .unwrap()
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(bytes)
        .iter()
        .fold(String::with_capacity(64), |mut s, b| {
            use std::fmt::Write;
            let _ = write!(&mut s, "{b:02x}");
            s
        })
}

async fn start_oracle() -> (String, Arc<tokio::sync::Mutex<OracleState>>) {
    let dict_s = include_bytes!("data/zstd_dict_8453_json.dict");
    let dict_l = include_bytes!("data/zstd_dict_8453_json_XL.dict");
    let id_s = dict_id(dict_s).unwrap().get();
    let id_l = dict_id(dict_l).unwrap().get();

    let state = Arc::new(tokio::sync::Mutex::new(OracleState {
        active: OracleDict {
            alg: "zstd",
            dict_id: id_s,
            sha: sha256_hex(dict_s),
            bytes: dict_s,
        },
        next: Some(OracleDict {
            alg: "zstd",
            dict_id: id_l,
            sha: sha256_hex(dict_l),
            bytes: dict_l,
        }),
    }));

    let router = Router::new()
        .route("/", head(oracle_head).get(oracle_get_root))
        .route("/{sha}", get(oracle_get_sha))
        .with_state(state.clone());

    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.ok();
    });

    (format!("http://{}", addr), state)
}

// Oracle variant that advertises a dict id that does not match the bytes it
// serves. This exercises that the loader validates *both* the SHA256 and the
// zstd dict_id header before accepting and caching a dictionary.
async fn start_oracle_dict_id_mismatch() -> (String, Arc<tokio::sync::Mutex<OracleState>>) {
    let dict_s = include_bytes!("data/zstd_dict_8453_json.dict");
    let dict_l = include_bytes!("data/zstd_dict_8453_json_XL.dict");

    let id_s = dict_id(dict_s).unwrap().get();

    // Intentionally broken: advertise id_s, but actually serve dict_l (and its SHA).
    let state = Arc::new(tokio::sync::Mutex::new(OracleState {
        active: OracleDict {
            alg: "zstd",
            dict_id: id_s,
            sha: sha256_hex(dict_l),
            bytes: dict_l,
        },
        next: None,
    }));

    let router = Router::new()
        .route("/", head(oracle_head).get(oracle_get_root))
        .route("/{sha}", get(oracle_get_sha))
        .with_state(state.clone());

    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.ok();
    });

    (format!("http://{}", addr), state)
}

// Oracle variant that announces a next algorithm (zstd) *without* a dictionary.
// This exercises that the loader tolerates a Next-Alg header with no
// Next-Dict-* headers and continues to function using the active dict only.
async fn start_oracle_next_alg_without_dict() -> (String, Arc<tokio::sync::Mutex<OracleState>>) {
    let dict_s = include_bytes!("data/zstd_dict_8453_json.dict");
    let id_s = dict_id(dict_s).unwrap().get();

    let state = Arc::new(tokio::sync::Mutex::new(OracleState {
        active: OracleDict {
            alg: "zstd",
            dict_id: id_s,
            sha: sha256_hex(dict_s),
            bytes: dict_s,
        },
        next: None,
    }));

    async fn oracle_head_next_alg_only(
        State(state): State<Arc<tokio::sync::Mutex<OracleState>>>,
    ) -> Response {
        let state = state.lock().await;
        let mut headers = HeaderMap::new();
        headers.insert(
            "Flashblocks-Active-Compression-Alg",
            HeaderValue::from_static("zstd"),
        );
        headers.insert(
            "Flashblocks-Active-Compression-Dict-Id",
            HeaderValue::from_str(&state.active.dict_id.to_string()).unwrap(),
        );
        headers.insert(
            "Flashblocks-Active-Compression-Dict-Sha",
            HeaderValue::from_str(&state.active.sha).unwrap(),
        );
        // Announce a next algorithm without any associated dict metadata.
        headers.insert(
            "Flashblocks-Next-Compression-Alg",
            HeaderValue::from_static("zstd"),
        );
        let mut response = Response::builder()
            .status(StatusCode::OK)
            .body(Body::empty())
            .unwrap();
        *response.headers_mut() = headers;
        response
    }

    let router = Router::new()
        .route("/", head(oracle_head_next_alg_only).get(oracle_get_root))
        .route("/{sha}", get(oracle_get_sha))
        .with_state(state.clone());

    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.ok();
    });

    (format!("http://{}", addr), state)
}

#[tokio::test]
async fn test_rolling_dictionary_update() {
    let (oracle_url, _state) = start_oracle().await;

    // Manual dict loading test (oracle endpoints work)
    let encoder = StreamEncoder::new();
    let decoder = StreamDecoder::new();

    let dict_bytes = include_bytes!("data/zstd_dict_8453_json.dict");
    encoder.add_dict(dict_bytes).unwrap();
    decoder.add_dict(dict_bytes).unwrap();

    let compressed = encoder.encode(TEST_JSON.as_bytes()).unwrap();
    let decompressed = decoder.try_decode(&compressed).unwrap();
    assert_eq!(String::from_utf8(decompressed).unwrap(), TEST_JSON);

    // Verify oracle endpoint responds correctly
    let client = reqwest::Client::new();
    let resp = client.head(&oracle_url).send().await.unwrap();
    assert!(resp.status().is_success());
    assert!(
        resp.headers()
            .contains_key("Flashblocks-Active-Compression-Alg")
    );
}

#[tokio::test]
async fn test_loader_preloads_active_and_next_dicts() {
    let (oracle_url, _state) = start_oracle().await;

    // Use a throwaway directory under /tmp for dict storage.
    let mut dir = std::env::temp_dir();
    dir.push(format!(
        "flashblocks-dicts-test-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();

    // Start a decoder wired to the oracle + storage; loader runs in background.
    let decoder = StreamDecoder::new()
        .maybe_dict_storage(Some(dir.as_path()))
        .maybe_dict_oracle(Some(&oracle_url));

    // Allow some time for the initial HEAD + GETs to complete.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Build an encoder with the same two dictionaries as the oracle (S and L).
    let dict_s = include_bytes!("data/zstd_dict_8453_json.dict");
    let dict_l = include_bytes!("data/zstd_dict_8453_json_XL.dict");

    let encoder = StreamEncoder::new();
    let id_s = encoder.add_dict(dict_s).unwrap();
    let id_l = encoder.add_dict(dict_l).unwrap();

    // Encode once with S as active, once with L as active.
    encoder.set_active(id_s);
    let compressed_s = encoder.encode(TEST_JSON.as_bytes()).unwrap();

    encoder.set_active(id_l);
    let compressed_l = encoder.encode(TEST_JSON.as_bytes()).unwrap();

    // Decoder should handle both dict IDs because loader preloaded active and next.
    assert_eq!(
        decoder.try_decode(&compressed_s).unwrap(),
        TEST_JSON.as_bytes()
    );
    assert_eq!(
        decoder.try_decode(&compressed_l).unwrap(),
        TEST_JSON.as_bytes()
    );
}

#[tokio::test]
async fn test_loader_tolerates_next_alg_without_dict() {
    let (oracle_url, _state) = start_oracle_next_alg_without_dict().await;

    // Use a throwaway directory under /tmp for dict storage.
    let mut dir = std::env::temp_dir();
    dir.push(format!(
        "flashblocks-dicts-test-next-alg-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();

    // Start a decoder wired to the oracle + storage; loader runs in background.
    let decoder = StreamDecoder::new()
        .maybe_dict_storage(Some(dir.as_path()))
        .maybe_dict_oracle(Some(&oracle_url));

    // Allow some time for the initial HEAD + GETs to complete.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Build an encoder with the active dictionary only.
    let dict_s = include_bytes!("data/zstd_dict_8453_json.dict");
    let encoder = StreamEncoder::new();
    let id_s = encoder.add_dict(dict_s).unwrap();

    // Encode with the active dict.
    encoder.set_active(id_s);
    let compressed = encoder.encode(TEST_JSON.as_bytes()).unwrap();

    // Decoder should handle this dict ID even though the oracle announced a
    // "next" algorithm without dict metadata.
    assert_eq!(
        decoder.try_decode(&compressed).unwrap(),
        TEST_JSON.as_bytes()
    );
}

#[tokio::test]
async fn test_loader_rejects_dict_with_mismatched_id() {
    let (oracle_url, _state) = start_oracle_dict_id_mismatch().await;

    // Use a throwaway directory under /tmp for dict storage.
    let mut dir = std::env::temp_dir();
    dir.push(format!(
        "flashblocks-dicts-test-id-mismatch-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();

    // Start a decoder wired to the oracle + storage; loader runs in background.
    let _decoder = StreamDecoder::new()
        .maybe_dict_storage(Some(dir.as_path()))
        .maybe_dict_oracle(Some(&oracle_url));

    // Allow some time for the initial HEAD + GET to complete.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // The loader should *not* cache a dictionary whose advertised id does not
    // match the zstd dict_id in the bytes. That means no files are written to
    // the storage directory.
    let entries = std::fs::read_dir(&dir).unwrap();
    assert_eq!(entries.count(), 0);
}
