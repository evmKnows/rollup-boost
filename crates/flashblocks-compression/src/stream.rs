use crate::{dict, zstd::ZstdDecoder, zstd::ZstdEncoder};
use std::{
    io::{Read, Write},
    num::NonZeroU32,
    path::{Path, PathBuf},
    sync::Arc,
};

/// Trait for zstd contexts that can load dictionaries.
pub trait DictStore: Default + Send + Sync + 'static {
    fn add_dict(&self, bytes: &[u8]) -> eyre::Result<NonZeroU32>;
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CompressionAlg {
    #[default]
    None,
    Br,
    Zstd,
    Dcz,
}

/// Generic stream processor with dictionary oracle support.
/// Reads `DICT_ORACLE` and `DICT_STORAGE` env vars by default, overridable via builder methods.
pub struct DictStream<T> {
    pub(crate) zstd: Arc<T>,
    oracle_url: Option<String>,
    storage_path: Option<PathBuf>,
}

impl<T: DictStore> Default for DictStream<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: DictStore> DictStream<T> {
    pub fn new() -> Self {
        Self {
            zstd: Arc::new(T::default()),
            oracle_url: None,
            storage_path: None,
        }
    }

    pub fn with_dict_oracle(mut self, url: &str) -> Self {
        self.oracle_url = Some(url.into());
        self.start();
        self
    }

    pub fn with_dict_storage(mut self, path: &Path) -> Self {
        self.storage_path = Some(path.into());
        self
    }

    pub fn maybe_dict_oracle(self, url: Option<&str>) -> Self {
        match url {
            Some(u) => self.with_dict_oracle(u),
            None => self,
        }
    }

    pub fn maybe_dict_storage(self, path: Option<&Path>) -> Self {
        match path {
            Some(p) => self.with_dict_storage(p),
            None => self,
        }
    }

    pub fn add_dict(&self, bytes: &[u8]) -> eyre::Result<NonZeroU32> {
        self.zstd.add_dict(bytes)
    }

    pub fn start(&self) {
        if self.oracle_url.is_none() {
            return;
        }
        let url = self.oracle_url.clone().unwrap();
        let dir = self
            .storage_path
            .clone()
            .unwrap_or_else(|| PathBuf::from("/data/flashblocks-dicts"));
        let zstd = self.zstd.clone();
        dict::start_loader(url, dir, move |bytes| {
            let _ = zstd.add_dict(bytes);
        });
    }
}

pub type StreamEncoder = DictStream<ZstdEncoder>;

impl StreamEncoder {
    pub fn set_active(&self, id: NonZeroU32) {
        self.zstd.set_active(id)
    }

    pub fn encode(&self, data: &[u8]) -> eyre::Result<Vec<u8>> {
        self.zstd.encode(data)
    }

    pub fn encode_brotli(&self, data: &[u8]) -> eyre::Result<Vec<u8>> {
        let mut out = Vec::new();
        brotli::CompressorWriter::new(&mut out, 4096, 5, 22).write_all(data)?;
        Ok(out)
    }

    pub fn compress_with(&self, data: &[u8], alg: CompressionAlg) -> eyre::Result<Vec<u8>> {
        match alg {
            CompressionAlg::Br => self.encode_brotli(data),
            CompressionAlg::Zstd | CompressionAlg::Dcz => self.encode(data),
            CompressionAlg::None => Ok(data.to_vec()),
        }
    }
}

pub type StreamDecoder = DictStream<ZstdDecoder>;

impl StreamDecoder {
    /// Decode bytes. Detection: cleartext (`{` = 0x7B) → zstd → brotli → passthrough.
    ///
    /// Does not collide:
    ///   - zstd: first byte is 0x28: 0x28 < 0x7B
    ///   - brotli: first byte is windo bits: 0x10 < 0x3B < 0x7B
    pub fn try_decode(&self, bytes: &[u8]) -> eyre::Result<Vec<u8>> {
        if bytes.first() == Some(&b'{') {
            return Ok(bytes.to_vec());
        }
        if ZstdDecoder::is_zstd(bytes) {
            return self.zstd.decode(bytes);
        }
        let mut dec = Vec::new();
        if brotli::Decompressor::new(bytes, 4096)
            .read_to_end(&mut dec)
            .is_ok()
            && !dec.is_empty()
        {
            return Ok(dec);
        }
        Ok(bytes.to_vec())
    }
}
