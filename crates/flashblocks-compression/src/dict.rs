use crate::zstd::dict_id;
use reqwest::{Client, header::HeaderMap, retry};
use sha2::{Digest, Sha256};
use std::{
    fs,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tracing::{debug, info, warn};

const POLL_INTERVAL: Duration = Duration::from_secs(4 * 60 * 60);
const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);

pub fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .fold(String::with_capacity(64), |mut s, b| {
            use std::fmt::Write;
            let _ = write!(s, "{b:02x}");
            s
        })
}

pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

trait Headers {
    fn str(&self, k: &str) -> Option<&str>;
    fn u32(&self, k: &str) -> Option<u32> {
        self.str(k)?.parse().ok()
    }
    fn u64(&self, k: &str) -> Option<u64> {
        self.str(k)?.parse().ok()
    }
}
impl Headers for HeaderMap {
    fn str(&self, k: &str) -> Option<&str> {
        self.get(k)?.to_str().ok()
    }
}

pub(crate) fn start_loader<F>(url: String, dir: PathBuf, on_dict: F)
where
    F: Fn(&[u8]) + Send + Sync + 'static,
{
    tokio::spawn(async move { run_loader(url, dir, on_dict).await });
}

async fn run_loader<F>(url: String, dir: PathBuf, on_dict: F)
where
    F: Fn(&[u8]) + Send + Sync + 'static,
{
    let _ = fs::create_dir_all(&dir);
    let client = {
        let host = url
            .parse::<reqwest::Url>()
            .ok()
            .and_then(|u| u.host_str().map(|h| h.to_owned()));
        match host {
            Some(h) => {
                let policy = retry::for_host(h).max_retries_per_request(3);
                Client::builder()
                    .retry(policy)
                    .build()
                    .unwrap_or_else(|_| Client::new())
            }
            None => Client::new(),
        }
    };
    // Oracle is primary (with timeout); disk is fallback
    match tokio::time::timeout(STARTUP_TIMEOUT, poll(&client, &url, &dir, &on_dict)).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            warn!(%e, "oracle failed, loading from disk");
            load_from_disk(&dir, &on_dict);
        }
        Err(_) => {
            warn!("oracle timeout, loading from disk");
            load_from_disk(&dir, &on_dict);
        }
    }
    loop {
        tokio::time::sleep(POLL_INTERVAL).await;
        if let Err(e) = poll(&client, &url, &dir, &on_dict).await {
            warn!(%e, "poll failed");
        }
    }
}

pub fn load_from_disk<F: Fn(&[u8])>(dir: &PathBuf, on_dict: &F) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    let now = now();
    let mut active: Option<(u64, PathBuf)> = None;
    let mut next: Option<(u64, PathBuf)> = None;

    for e in entries.flatten() {
        let path = e.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let Some((ts_str, sha)) = name.strip_suffix(".dict").and_then(|s| s.split_once('_')) else {
            continue;
        };
        let Ok(ts) = ts_str.parse::<u64>() else {
            continue;
        };
        let Ok(bytes) = fs::read(&path) else {
            continue;
        };
        let computed = sha256_hex(&bytes);
        if !sha.eq_ignore_ascii_case(&computed) {
            warn!(?path, %sha, %computed, "cached dict sha mismatch, skipping");
            continue;
        }
        if dict_id(&bytes).is_none() {
            warn!(?path, "cached dict has invalid header, skipping");
            continue;
        }
        if ts <= now {
            match active {
                None => active = Some((ts, path.clone())),
                Some((cur_ts, _)) if ts > cur_ts => active = Some((ts, path.clone())),
                _ => {}
            }
        } else {
            match next {
                None => next = Some((ts, path.clone())),
                Some((cur_ts, _)) if ts < cur_ts => next = Some((ts, path.clone())),
                _ => {}
            }
        }
    }

    if let Some((_, path)) = active
        && let Ok(bytes) = fs::read(&path) {
            on_dict(&bytes);
            info!(path = ?path, "loaded active dict from disk");
        }

    if let Some((ts, path)) = next
        && let Ok(bytes) = fs::read(&path) {
            on_dict(&bytes);
            info!(path = ?path, ts, "preloaded next dict from disk");
        }
}

async fn poll<F: Fn(&[u8])>(
    client: &Client,
    url: &str,
    dir: &PathBuf,
    on_dict: &F,
) -> eyre::Result<()> {
    let resp = client.head(url).send().await?;
    if !resp.status().is_success() {
        eyre::bail!("HEAD {}", resp.status());
    }
    let head = resp.headers();

    let uses_dict = |k: &str| {
        head.str(k)
            .map(|s| matches!(s.to_ascii_lowercase().as_str(), "dcz" | "zstd"))
            .unwrap_or(false)
    };

    let active = if uses_dict("Flashblocks-Active-Compression-Alg") {
        match (
            head.u32("Flashblocks-Active-Compression-Dict-Id"),
            head.str("Flashblocks-Active-Compression-Dict-Sha"),
        ) {
            (Some(id), Some(sha)) => Some((id, sha.to_owned())),
            _ => None,
        }
    } else {
        None
    };

    let next = if uses_dict("Flashblocks-Next-Compression-Alg") {
        match (
            head.u32("Flashblocks-Next-Compression-Dict-Id"),
            head.str("Flashblocks-Next-Compression-Dict-Sha"),
            head.u64("Flashblocks-Next-Compression-Activation-Time"),
        ) {
            (Some(id), Some(sha), Some(ts)) => Some((id, sha.to_owned(), ts)),
            _ => None,
        }
    } else {
        None
    };

    let mut load_next = true;

    if let (Some((active_id, active_sha)), Some((next_id, next_sha, _))) =
        (active.as_ref(), next.as_ref())
    {
        if active_id == next_id && !active_sha.eq_ignore_ascii_case(next_sha) {
            warn!(
                active_id,
                active_sha = %active_sha,
                next_sha = %next_sha,
                "dict id clash between active and next from oracle, skipping next"
            );
            load_next = false;
        }
        if active_sha.eq_ignore_ascii_case(next_sha) {
            // Same dictionary advertised twice; no need to preload twice.
            load_next = false;
        }
    }

    if let Some((id, sha)) = active {
        fetch(client, url, dir, on_dict, 0, id, &sha).await;
    }
    if load_next
        && let Some((id, sha, ts)) = next {
            fetch(
                client,
                &format!("{}/{sha}", url.trim_end_matches('/')),
                dir,
                on_dict,
                ts,
                id,
                &sha,
            )
            .await;
        }
    Ok(())
}

async fn fetch<F: Fn(&[u8])>(
    client: &Client,
    url: &str,
    dir: &PathBuf,
    on_dict: &F,
    ts: u64,
    id: u32,
    sha: &str,
) {
    let path = dir.join(format!("{ts}_{sha}.dict"));
    // Use cached if available
    if let Ok(bytes) = fs::read(&path) {
        let computed = sha256_hex(&bytes);
        let id_ok = !dict_id(&bytes).map(|i| i.get() != id).unwrap_or(true);

        if sha.eq_ignore_ascii_case(&computed) && id_ok {
            on_dict(&bytes);
            if ts <= now() {
                debug!(%sha, "loaded from cache");
            } else {
                debug!(%sha, ts, "preloaded from cache");
            }
            return;
        }

        warn!(
            ?path,
            %sha,
            %computed,
            id,
            "cached dict failed validation, refetching"
        );
        let _ = fs::remove_file(&path);
    }
    // Fetch from oracle
    let bytes = match client.get(url).send().await {
        Ok(r) => match r.bytes().await {
            Ok(b) => b,
            Err(e) => return warn!(%e, "read failed"),
        },
        Err(e) => return warn!(%e, "GET failed"),
    };

    let computed = sha256_hex(&bytes);
    if !sha.eq_ignore_ascii_case(&computed) {
        return warn!(%sha, %computed, "sha mismatch");
    }

    if dict_id(&bytes).map(|i| i.get() != id).unwrap_or(true) {
        return warn!("dict id mismatch");
    }

    let _ = fs::write(&path, &bytes);
    on_dict(&bytes);

    if ts <= now() {
        info!(%sha, "installed");
    } else {
        info!(%sha, ts, "preloaded");
    }
}
