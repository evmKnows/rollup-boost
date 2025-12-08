use crate::DictStore;
use std::{collections::HashMap, num::NonZeroU32, sync::Mutex};

const ZSTD_MAGICNUMBER: u32 = 0xFD2FB528;
const MAX_DICT_SIZE: usize = 2 << 20;
const MIN_DICT_SIZE: usize = 1 << 10;
const MAX_DECODE_SIZE: usize = 16 << 20;
const ZSTD_LEVEL: i32 = 2;

#[inline]
fn validate_dict(bytes: &[u8]) -> eyre::Result<NonZeroU32> {
    if bytes.len() > MAX_DICT_SIZE {
        eyre::bail!("dict too large");
    }
    if bytes.len() < MIN_DICT_SIZE {
        eyre::bail!("dict too small");
    }
    let id = dict_id(bytes).ok_or_else(|| eyre::eyre!("invalid dict"))?;
    Ok(id)
}

impl DictStore for ZstdEncoder {
    fn add_dict(&self, bytes: &[u8]) -> eyre::Result<NonZeroU32> {
        let id = validate_dict(bytes)?;
        let cdict = zstd_safe::CDict::create(bytes, ZSTD_LEVEL);
        let mut dicts = self.dicts.lock().unwrap();
        let mut active = self.active.lock().unwrap();
        dicts.insert(id, cdict);
        if active.is_none() {
            *active = Some(id);
        }
        Ok(id)
    }
}

#[inline]
pub fn dict_id(b: &[u8]) -> Option<NonZeroU32> {
    zstd_safe::get_dict_id_from_dict(b)
}

/// Parses a zstd frame header and returns the optional dictionary id used by the frame.
///
/// Frame header (subset):
///   offset  size  description
///   0..4    4     MAGIC = 0x28 B5 2F FD (ZSTD_MAGICNUMBER, LE)
///   4       1     frame header byte; dictIDFlag in 2 LSBs
///   5..9    4     dict_id (u32 LE), present if dictIDFlag != 0
#[inline]
pub fn frame_dict_id(b: &[u8]) -> Option<Option<NonZeroU32>> {
    if b.len() <= 8 {
        return None;
    }
    if u32::from_le_bytes([b[0], b[1], b[2], b[3]]) != ZSTD_MAGICNUMBER {
        return None;
    }
    let id = ((b[4] & 0x03) != 0)
        .then(|| NonZeroU32::new(u32::from_le_bytes([b[5], b[6], b[7], b[8]])))
        .flatten();
    Some(id)
}

/// Zstd encoder with optional dictionaries and a fixed compression level.
pub struct ZstdEncoder {
    cctx: Mutex<zstd_safe::CCtx<'static>>,
    dicts: Mutex<HashMap<NonZeroU32, zstd_safe::CDict<'static>>>,
    active: Mutex<Option<NonZeroU32>>,
}

impl Default for ZstdEncoder {
    fn default() -> Self {
        Self::new()
    }
}

impl ZstdEncoder {
    pub fn new() -> Self {
        let mut cctx = zstd_safe::CCtx::default();
        cctx.set_parameter(zstd_safe::CParameter::CompressionLevel(ZSTD_LEVEL))
            .ok();
        Self {
            cctx: Mutex::new(cctx),
            dicts: Mutex::new(HashMap::new()),
            active: Mutex::new(None),
        }
    }

    pub fn set_active(&self, id: NonZeroU32) {
        *self.active.lock().unwrap() = Some(id);
    }

    pub fn encode(&self, data: &[u8]) -> eyre::Result<Vec<u8>> {
        let mut cctx = self.cctx.lock().unwrap();
        let mut out = vec![0u8; zstd_safe::compress_bound(data.len())];
        let dicts = self.dicts.lock().unwrap();
        let active = self.active.lock().unwrap();
        let n = match active.and_then(|id| dicts.get(&id)) {
            Some(cdict) => cctx
                .compress_using_cdict(&mut out[..], data, cdict)
                .map_err(|e| eyre::eyre!("{e}"))?,
            None => cctx
                .compress(&mut out[..], data, ZSTD_LEVEL)
                .map_err(|e| eyre::eyre!("{e}"))?,
        };
        out.truncate(n);
        Ok(out)
    }
}

pub struct ZstdDecoder {
    dctx: Mutex<zstd_safe::DCtx<'static>>,
    dicts: Mutex<HashMap<NonZeroU32, zstd_safe::DDict<'static>>>,
}

impl Default for ZstdDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl DictStore for ZstdDecoder {
    fn add_dict(&self, bytes: &[u8]) -> eyre::Result<NonZeroU32> {
        let id = validate_dict(bytes)?;
        self.dicts
            .lock()
            .unwrap()
            .insert(id, zstd_safe::DDict::create(bytes));
        Ok(id)
    }
}

impl ZstdDecoder {
    pub fn new() -> Self {
        Self {
            dctx: Mutex::new(zstd_safe::DCtx::default()),
            dicts: Mutex::new(HashMap::new()),
        }
    }

    pub fn is_zstd(bytes: &[u8]) -> bool {
        frame_dict_id(bytes).is_some()
    }

    pub fn decode(&self, bytes: &[u8]) -> eyre::Result<Vec<u8>> {
        let frame_id = frame_dict_id(bytes).ok_or_else(|| eyre::eyre!("not zstd"))?;
        let mut dctx = self.dctx.lock().unwrap();
        let mut out = vec![0u8; MAX_DECODE_SIZE];
        let dicts = self.dicts.lock().unwrap();
        let n = match frame_id {
            Some(id) => {
                let ddict = dicts
                    .get(&id)
                    .ok_or_else(|| eyre::eyre!("dict {} not loaded", id))?;
                dctx.decompress_using_ddict(&mut out[..], bytes, ddict)
                    .map_err(|e| eyre::eyre!("{e}"))?
            }
            None => dctx
                .decompress(&mut out[..], bytes)
                .map_err(|e| eyre::eyre!("{e}"))?,
        };
        out.truncate(n);
        Ok(out)
    }
}
