use flashblocks_compression::{StreamDecoder, StreamEncoder};

const TEST_JSON: &str = r#"{"id":1,"method":"test","params":[]}"#;

#[test]
fn test_try_decode_cleartext() {
    let decoder = StreamDecoder::new();
    assert_eq!(
        decoder.try_decode(TEST_JSON.as_bytes()).unwrap(),
        TEST_JSON.as_bytes()
    );
}

#[test]
fn test_try_decode_passthrough() {
    let decoder = StreamDecoder::new();
    let data = b"  \n  {json}";
    assert_eq!(decoder.try_decode(data).unwrap(), data.as_slice());
}

#[test]
fn test_encode_decode_zstd() {
    let encoder = StreamEncoder::new();
    let decoder = StreamDecoder::new();
    let compressed = encoder.encode(TEST_JSON.as_bytes()).unwrap();
    assert!(!compressed.is_empty());
    assert_eq!(compressed[0..4], [0x28, 0xb5, 0x2f, 0xfd]);
    assert_eq!(
        decoder.try_decode(&compressed).unwrap(),
        TEST_JSON.as_bytes()
    );
}

#[test]
fn test_encode_decode_brotli() {
    let encoder = StreamEncoder::new();
    let decoder = StreamDecoder::new();
    let compressed = encoder.encode_brotli(TEST_JSON.as_bytes()).unwrap();
    assert!(!compressed.is_empty());
    assert_eq!(
        decoder.try_decode(&compressed).unwrap(),
        TEST_JSON.as_bytes()
    );
}

#[test]
fn test_dict_encode_decode() {
    let test_data = include_str!("data/flashblock_8453.json");
    let dict = include_bytes!("data/zstd_dict_8453_json.dict");

    let encoder = StreamEncoder::new();
    let decoder = StreamDecoder::new();

    let id = encoder.add_dict(dict).unwrap();
    decoder.add_dict(dict).unwrap();

    let compressed = encoder.encode(test_data.as_bytes()).unwrap();
    println!(
        "Compressed {} -> {} bytes",
        test_data.len(),
        compressed.len()
    );

    assert_eq!(compressed[4] & 0x03, 3);
    let frame_dict_id =
        u32::from_le_bytes([compressed[5], compressed[6], compressed[7], compressed[8]]);
    assert_eq!(frame_dict_id, id.get());

    assert_eq!(
        decoder.try_decode(&compressed).unwrap(),
        test_data.as_bytes()
    );
}

#[test]
fn test_multiple_dicts() {
    let dict_s = include_bytes!("data/zstd_dict_8453_json.dict");
    let dict_l = include_bytes!("data/zstd_dict_8453_json_XL.dict");

    let encoder = StreamEncoder::new();
    let decoder = StreamDecoder::new();

    let id_s = encoder.add_dict(dict_s).unwrap();
    let id_l = encoder.add_dict(dict_l).unwrap();
    decoder.add_dict(dict_s).unwrap();
    decoder.add_dict(dict_l).unwrap();

    // First dict becomes active
    let compressed1 = encoder.encode(TEST_JSON.as_bytes()).unwrap();
    assert_eq!(
        decoder.try_decode(&compressed1).unwrap(),
        TEST_JSON.as_bytes()
    );

    // Switch to second dict
    encoder.set_active(id_l);
    let compressed2 = encoder.encode(TEST_JSON.as_bytes()).unwrap();
    assert_eq!(
        decoder.try_decode(&compressed2).unwrap(),
        TEST_JSON.as_bytes()
    );

    // Decoder handles both
    assert_eq!(
        decoder.try_decode(&compressed1).unwrap(),
        TEST_JSON.as_bytes()
    );

    let _ = id_s;
}

#[test]
fn test_oversized_dict_rejected() {
    // MAX_DICT_SIZE is 2 << 20; use a buffer slightly larger than that.
    let oversized = vec![0u8; (2 << 20) + 1];
    let encoder = StreamEncoder::new();
    assert!(encoder.add_dict(&oversized).is_err());

    let decoder = StreamDecoder::new();
    assert!(decoder.add_dict(&oversized).is_err());
}

#[test]
fn test_too_small_dict_rejected() {
    // A dictionary that is smaller than MIN_DICT_SIZE (1 KiB) should be rejected.
    // Use a valid-looking header but keep it tiny to exercise the lower bound.
    let small = vec![0u8; 16];

    let encoder = StreamEncoder::new();
    assert!(encoder.add_dict(&small).is_err());

    let decoder = StreamDecoder::new();
    assert!(decoder.add_dict(&small).is_err());
}

#[test]
fn test_zstd_dict_decode_is_independent_of_compression_level() {
    // This test ensures that a decoder using a dictionary created from
    // bytes trained/used at level 2 can successfully decode frames that
    // were compressed with the same dict bytes but a different level
    // (e.g. level 3). In other words, the decompression side does not
    // depend on the encoder's compression level.

    // Plain zstd at level 3 should still be decodable by StreamDecoder.
    {
        use zstd_safe::{CCtx, CParameter};

        let data = TEST_JSON.as_bytes();
        let mut cctx = CCtx::default();
        let _ = cctx.set_parameter(CParameter::CompressionLevel(3));

        let mut out = vec![0u8; zstd_safe::compress_bound(data.len())];
        let n = cctx.compress(&mut out[..], data, 3).unwrap();
        out.truncate(n);

        let decoder = StreamDecoder::new();
        assert_eq!(decoder.try_decode(&out).unwrap(), data);
    }

    // Now do the same but with a dictionary, using level 3 on the encoder
    // side, while the decoder uses our normal Zstd decoder + dict.
    {
        use zstd_safe::{CCtx, CDict, CParameter};

        let dict = include_bytes!("data/zstd_dict_8453_json.dict");
        let data = TEST_JSON.as_bytes();

        let mut cctx = CCtx::default();
        let _ = cctx.set_parameter(CParameter::CompressionLevel(3));
        let cdict = CDict::create(dict, 3);

        let mut out = vec![0u8; zstd_safe::compress_bound(data.len())];
        let n = cctx
            .compress_using_cdict(&mut out[..], data, &cdict)
            .unwrap();
        out.truncate(n);

        let decoder = StreamDecoder::new();
        decoder.add_dict(dict).unwrap();
        assert_eq!(decoder.try_decode(&out).unwrap(), data);
    }
}

#[test]
fn test_stream_decoder_handles_mixed_compression_sequence() {
    use zstd_safe::{CCtx, CDict, CParameter};

    let data = TEST_JSON.as_bytes();

    let encoder = StreamEncoder::new();
    let decoder = StreamDecoder::new();

    // Prepare two dictionaries.
    let dict_a = include_bytes!("data/zstd_dict_8453_json.dict");
    let dict_b = include_bytes!("data/zstd_dict_8453_json_XL.dict");

    // Load both dicts into decoder once.
    decoder.add_dict(dict_a).unwrap();
    decoder.add_dict(dict_b).unwrap();

    // 1. Uncompressed JSON (passthrough).
    let clear = data.to_vec();

    // 2. Brotli via StreamEncoder helper.
    let brotli1 = encoder.encode_brotli(data).unwrap();

    // 3. Zstd (level 2, no dict).
    let zstd_l2 = encoder.encode(data).unwrap();

    // 4. Zstd (level 3, no dict) using raw zstd_safe.
    let zstd_l3 = {
        let mut cctx = CCtx::default();
        let _ = cctx.set_parameter(CParameter::CompressionLevel(3));
        let mut out = vec![0u8; zstd_safe::compress_bound(data.len())];
        let n = cctx.compress(&mut out[..], data, 3).unwrap();
        out.truncate(n);
        out
    };

    // 5. Zstd with dict A.
    let zstd_dict_a = {
        let mut cctx = CCtx::default();
        let _ = cctx.set_parameter(CParameter::CompressionLevel(2));
        let cdict = CDict::create(dict_a, 2);
        let mut out = vec![0u8; zstd_safe::compress_bound(data.len())];
        let n = cctx
            .compress_using_cdict(&mut out[..], data, &cdict)
            .unwrap();
        out.truncate(n);
        out
    };

    // 6. Zstd with dict B.
    let zstd_dict_b = {
        let mut cctx = CCtx::default();
        let _ = cctx.set_parameter(CParameter::CompressionLevel(2));
        let cdict = CDict::create(dict_b, 2);
        let mut out = vec![0u8; zstd_safe::compress_bound(data.len())];
        let n = cctx
            .compress_using_cdict(&mut out[..], data, &cdict)
            .unwrap();
        out.truncate(n);
        out
    };

    // 7. Another Brotli payload.
    let brotli2 = encoder.encode_brotli(data).unwrap();

    // 8. Another plain zstd payload.
    let zstd_again = encoder.encode(data).unwrap();

    let sequence = [
        clear,
        brotli1,
        zstd_l2,
        zstd_l3,
        zstd_dict_a,
        zstd_dict_b,
        brotli2,
        zstd_again,
    ];

    for chunk in &sequence {
        let decoded = decoder.try_decode(chunk).unwrap();
        assert_eq!(decoded, data);
    }
}
