//! # Randomized robustness tests for the two parsing fuzz targets (LLD §12 rows 1–2)
//!
//! `cargo fuzz` needs a nightly toolchain and libFuzzer, neither of which this Windows/MSVC
//! development environment carries — so these are the LLD's stated fallback: deterministic
//! seeded randomized tests over the same two attack surfaces, runnable inside the ordinary
//! `cargo test` gate on every platform.
//!
//! * **multipart splitter**: random valid bodies chunked at random split points must reassemble
//!   byte-for-byte; random garbage must never panic, never grow the buffer unboundedly, and only
//!   ever fail with the splitter's own error types.
//! * **streams XML**: random mutations of a real document and raw random soup must parse to
//!   `Ok`/`Err` — never a panic — under the structural caps.
//!
//! The RNG is a seeded xorshift: every failure is reproducible from the printed iteration seed.

use mtconnect_adapter::mtconnect::multipart::{MultipartReader, Part};
use mtconnect_adapter::mtconnect::xml::{parse_document, parse_errors, parse_streams};
use mtconnect_adapter::mtconnect::MtcError;

const CURRENT_2_7: &str = include_str!("fixtures/current_2.7.xml");
const ERRORS_2_7: &str = include_str!("fixtures/errors_out_of_range_2.7.xml");

/// A tiny deterministic xorshift64* generator — no dependency, reproducible failures.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n.max(1) as u64) as usize
    }
    fn chance(&mut self, percent: u64) -> bool {
        self.next() % 100 < percent
    }
}

// =================================================================================================
// Multipart splitter
// =================================================================================================

/// A random part body. Deliberately hostile: may contain CR, LF, `-`, and boundary-like runs —
/// legal inside a length-declared body.
fn random_body(rng: &mut Rng, max_len: usize) -> Vec<u8> {
    let len = rng.below(max_len);
    (0..len)
        .map(|_| match rng.below(6) {
            0 => b'\r',
            1 => b'\n',
            2 => b'-',
            _ => b'a' + (rng.below(26) as u8),
        })
        .collect()
}

/// A random body safe for a part WITHOUT a Content-length: it must not contain the delimiter,
/// which excluding `-` guarantees.
fn random_delimited_body(rng: &mut Rng, max_len: usize) -> Vec<u8> {
    let len = rng.below(max_len);
    (0..len).map(|_| b'a' + (rng.below(26) as u8)).collect()
}

#[test]
fn random_multipart_bodies_reassemble_across_random_chunk_splits() {
    for seed in 0..300u64 {
        let mut rng = Rng::new(seed + 1);
        let boundary: String = (0..1 + rng.below(24))
            .map(|_| char::from(b'A' + rng.below(26) as u8))
            .collect();

        // Build 1..5 parts, remembering the expected bodies.
        let mut wire = Vec::new();
        let mut expected: Vec<Vec<u8>> = Vec::new();
        if rng.chance(30) {
            wire.extend_from_slice(b"random preamble\r\n");
        }
        for _ in 0..1 + rng.below(4) {
            let declared = rng.chance(70);
            let body = if declared {
                random_body(&mut rng, 400)
            } else {
                random_delimited_body(&mut rng, 400)
            };
            wire.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
            wire.extend_from_slice(b"Content-type: text/xml\r\n");
            if declared {
                wire.extend_from_slice(format!("Content-length: {}\r\n", body.len()).as_bytes());
            }
            wire.extend_from_slice(b"\r\n");
            wire.extend_from_slice(&body);
            wire.extend_from_slice(b"\r\n");
            expected.push(body);
        }
        wire.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());

        // Feed it in random chunks.
        let mut reader = MultipartReader::from_content_type(
            &format!("multipart/x-mixed-replace;boundary={boundary}"),
            4096,
        )
        .unwrap();
        let mut got: Vec<Part> = Vec::new();
        let mut offset = 0;
        while offset < wire.len() {
            let take = (1 + rng.below(64)).min(wire.len() - offset);
            reader
                .push(&wire[offset..offset + take])
                .unwrap_or_else(|e| {
                    panic!("seed {seed}: push refused a valid body: {e}");
                });
            offset += take;
            loop {
                match reader.next_part() {
                    Ok(Some(part)) => got.push(part),
                    Ok(None) => break,
                    Err(e) => panic!("seed {seed}: valid body failed to frame: {e}"),
                }
            }
        }
        let got_bodies: Vec<&[u8]> = got.iter().map(|p| p.body.as_slice()).collect();
        let want: Vec<&[u8]> = expected.iter().map(Vec::as_slice).collect();
        assert_eq!(
            got_bodies, want,
            "seed {seed}: bodies must reassemble byte-for-byte"
        );
        assert!(
            reader.is_finished(),
            "seed {seed}: the terminator must be recognized"
        );
    }
}

#[test]
fn random_garbage_never_panics_the_splitter_and_never_grows_its_buffer_unboundedly() {
    const CAP: usize = 512;
    for seed in 0..300u64 {
        let mut rng = Rng::new(seed + 7_000);
        let mut reader =
            MultipartReader::from_content_type("multipart/mixed; boundary=FUZZ", CAP).unwrap();
        for _ in 0..40 {
            let chunk: Vec<u8> = (0..rng.below(200))
                .map(|_| (rng.next() & 0xff) as u8)
                .collect();
            if reader.push(&chunk).is_err() {
                // The bounded accumulator refused — that IS the guarantee. Start over.
                reader.reset();
                continue;
            }
            loop {
                match reader.next_part() {
                    Ok(Some(_)) => {} // garbage that happened to frame — fine
                    Ok(None) => break,
                    Err(MtcError::Multipart(_) | MtcError::TooLarge { .. }) => {
                        reader.reset();
                        break;
                    }
                    Err(other) => panic!("seed {seed}: unexpected error type {other:?}"),
                }
            }
            assert!(
                reader.buffered().len()
                    <= CAP + mtconnect_adapter::mtconnect::multipart::MAX_PART_HEADER_BYTES,
                "seed {seed}: the buffer must stay bounded"
            );
        }
    }
}

// =================================================================================================
// Streams XML
// =================================================================================================

/// Mutate a document: random single-byte replacements, insertions, and deletions.
fn mutate(rng: &mut Rng, base: &str) -> Vec<u8> {
    let mut bytes = base.as_bytes().to_vec();
    for _ in 0..1 + rng.below(12) {
        if bytes.is_empty() {
            break;
        }
        let at = rng.below(bytes.len());
        match rng.below(3) {
            0 => bytes[at] = (rng.next() & 0xff) as u8,
            1 => bytes.insert(at, (rng.next() & 0xff) as u8),
            _ => {
                bytes.remove(at);
            }
        }
    }
    bytes
}

#[test]
fn mutated_streams_documents_parse_or_fail_but_never_panic() {
    for seed in 0..400u64 {
        let mut rng = Rng::new(seed + 40_000);
        let base = if rng.chance(70) {
            CURRENT_2_7
        } else {
            ERRORS_2_7
        };
        let mutated = mutate(&mut rng, base);
        let Ok(text) = std::str::from_utf8(&mutated) else {
            continue;
        };
        // Ok or Err are both acceptable outcomes; a panic is the only failure.
        let _ = parse_streams(text);
        let _ = parse_errors(text);
        let _ = parse_document(text);
    }
}

#[test]
fn truncated_streams_documents_never_panic() {
    for seed in 0..100u64 {
        let mut rng = Rng::new(seed + 90_000);
        let cut = rng.below(CURRENT_2_7.len());
        let truncated = &CURRENT_2_7[..cut];
        let _ = parse_streams(truncated);
        let _ = parse_document(truncated);
    }
}

#[test]
fn random_ascii_soup_never_panics_the_parsers() {
    for seed in 0..300u64 {
        let mut rng = Rng::new(seed + 130_000);
        let soup: String = (0..rng.below(600))
            .map(|_| {
                // Bias toward XML structure characters so the tokenizer gets past byte one.
                match rng.below(10) {
                    0 => '<',
                    1 => '>',
                    2 => '/',
                    3 => '"',
                    4 => '&',
                    5 => ';',
                    6 => '=',
                    _ => char::from(b' ' + rng.below(95) as u8),
                }
            })
            .collect();
        let _ = parse_streams(&soup);
        let _ = parse_errors(&soup);
        let _ = parse_document(&soup);
    }
}
