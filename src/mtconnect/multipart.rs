//! # Multipart framing (LLD §6) — the seam the streaming reader is built on
//!
//! An `interval`-driven `/sample` response is one long multipart body: a boundary, two headers, a
//! document, repeat. Two content types occur in the field and **both are accepted**:
//! `multipart/x-mixed-replace` (what the standard specifies) and `multipart/mixed` (what cppagent
//! 2.7.0.12 actually sends — verified live, HLD §2).
//!
//! What lives here today is the part every caller already needs: reading the boundary off the
//! content type, and a bounded accumulator that refuses to buffer more than `maxDocumentBytes`
//! while a part is still incomplete. The incremental part splitter that consumes this buffer lands
//! with the streaming state machine ([`super::stream`]); its shape — [`Part`] in, boundary and cap
//! out — is fixed here so the two halves cannot drift.

use super::error::MtcError;

/// The standard's streaming content type.
pub const X_MIXED_REPLACE: &str = "multipart/x-mixed-replace";
/// What cppagent 2.7 sends instead — accepted deliberately, not by accident.
pub const MIXED: &str = "multipart/mixed";

/// One complete part: its headers (lower-cased names) and its body bytes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Part {
    pub content_type: Option<String>,
    /// The part's own declared length, when it carried one. Trusted, but capped.
    pub content_length: Option<usize>,
    pub body: Vec<u8>,
}

impl Part {
    /// The part body as text.
    ///
    /// # Errors
    /// [`MtcError::Xml`] when the body is not valid UTF-8.
    pub fn text(&self) -> Result<&str, MtcError> {
        std::str::from_utf8(&self.body)
            .map_err(|_| MtcError::Xml("multipart body is not valid UTF-8".into()))
    }
}

/// Whether this content type is a multipart stream this client reads.
#[must_use]
pub fn is_multipart(content_type: &str) -> bool {
    let base = content_type.split(';').next().unwrap_or_default().trim().to_ascii_lowercase();
    base == X_MIXED_REPLACE || base == MIXED
}

/// The `boundary` parameter of a multipart content type, unquoted.
///
/// Returns `None` when the type is not a multipart this client reads, or declares no boundary —
/// both of which are stream-fatal rather than something to guess at.
#[must_use]
pub fn boundary_from_content_type(content_type: &str) -> Option<String> {
    if !is_multipart(content_type) {
        return None;
    }
    for param in content_type.split(';').skip(1) {
        let (key, value) = param.split_once('=')?;
        if key.trim().eq_ignore_ascii_case("boundary") {
            let value = value.trim().trim_matches('"');
            if value.is_empty() {
                return None;
            }
            return Some(value.to_string());
        }
    }
    None
}

/// A bounded accumulator for an in-flight multipart body.
///
/// It owns the two invariants the splitter must not violate: the boundary it is framing against,
/// and the cap on how much may be held while a part is incomplete. A body that runs past the cap is
/// a [`MtcError::TooLarge`] — the stream is dropped and re-established rather than buffered.
#[derive(Debug)]
pub struct MultipartReader {
    boundary: String,
    max_part_bytes: usize,
    buffer: Vec<u8>,
}

impl MultipartReader {
    /// Build a reader from a response's content type.
    ///
    /// # Errors
    /// [`MtcError::Multipart`] when the content type is not a multipart stream, or declares no
    /// boundary.
    pub fn from_content_type(content_type: &str, max_part_bytes: usize) -> Result<Self, MtcError> {
        let boundary = boundary_from_content_type(content_type).ok_or_else(|| {
            MtcError::Multipart(format!("no usable multipart boundary in `{content_type}`"))
        })?;
        Ok(Self { boundary, max_part_bytes, buffer: Vec::new() })
    }

    /// The boundary this reader frames against.
    #[must_use]
    pub fn boundary(&self) -> &str {
        &self.boundary
    }

    /// How much is buffered while the current part is incomplete.
    #[must_use]
    pub fn buffered(&self) -> &[u8] {
        &self.buffer
    }

    /// Append a chunk from the transport.
    ///
    /// # Errors
    /// [`MtcError::TooLarge`] when the incomplete part would exceed the cap.
    pub fn push(&mut self, chunk: &[u8]) -> Result<(), MtcError> {
        if self.buffer.len() + chunk.len() > self.max_part_bytes {
            return Err(MtcError::TooLarge { limit: self.max_part_bytes });
        }
        self.buffer.extend_from_slice(chunk);
        Ok(())
    }

    /// Drop everything buffered — the stream is being re-established, so a half-part from the old
    /// connection must never be framed against the new one.
    pub fn reset(&mut self) {
        self.buffer.clear();
    }

    /// Take the buffered bytes, leaving the reader empty (the splitter's entry point).
    #[must_use]
    pub fn take_buffer(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn both_observed_content_types_are_accepted() {
        // The standard's type, and the one cppagent 2.7 actually sends.
        assert!(is_multipart("multipart/x-mixed-replace;boundary=--------------------------"));
        assert!(is_multipart("multipart/mixed; boundary=abc"));
        assert!(is_multipart("MULTIPART/MIXED; boundary=abc"), "case is not semantics");
        assert!(!is_multipart("application/xml"));
        assert!(!is_multipart("multipart/form-data; boundary=abc"));
    }

    #[test]
    fn the_boundary_is_read_off_the_content_type() {
        assert_eq!(
            boundary_from_content_type("multipart/x-mixed-replace;boundary=--------------------------"),
            Some("--------------------------".to_string())
        );
        assert_eq!(
            boundary_from_content_type("multipart/mixed; boundary=\"quoted-boundary\""),
            Some("quoted-boundary".to_string())
        );
        assert_eq!(boundary_from_content_type("multipart/mixed"), None, "no boundary declared");
        assert_eq!(boundary_from_content_type("multipart/mixed; boundary="), None);
        assert_eq!(boundary_from_content_type("application/xml; boundary=x"), None);
    }

    #[test]
    fn a_reader_needs_a_multipart_content_type_with_a_boundary() {
        let r = MultipartReader::from_content_type("multipart/mixed; boundary=xyz", 1024).unwrap();
        assert_eq!(r.boundary(), "xyz");
        assert!(r.buffered().is_empty());

        for bad in ["application/xml", "multipart/mixed", ""] {
            assert!(
                matches!(MultipartReader::from_content_type(bad, 1024), Err(MtcError::Multipart(_))),
                "`{bad}` cannot be framed"
            );
        }
    }

    #[test]
    fn the_accumulator_is_bounded_and_resettable() {
        let mut r = MultipartReader::from_content_type("multipart/mixed; boundary=x", 8).unwrap();
        r.push(b"1234").unwrap();
        r.push(b"5678").unwrap();
        assert_eq!(r.buffered(), b"12345678");
        // One byte past the cap: the stream is dropped rather than buffered.
        assert!(matches!(r.push(b"9"), Err(MtcError::TooLarge { limit: 8 })));

        assert_eq!(r.take_buffer(), b"12345678".to_vec());
        assert!(r.buffered().is_empty(), "taking the buffer empties it");

        r.push(b"abc").unwrap();
        r.reset();
        assert!(r.buffered().is_empty(), "a re-established stream starts clean");
    }

    #[test]
    fn a_part_body_is_text_only_when_it_really_is_text() {
        let part = Part { body: b"<MTConnectStreams/>".to_vec(), ..Part::default() };
        assert_eq!(part.text().unwrap(), "<MTConnectStreams/>");
        let part = Part { body: vec![0xff, 0xfe], ..Part::default() };
        assert!(matches!(part.text(), Err(MtcError::Xml(_))));
        assert_eq!(Part::default().content_length, None);
        assert_eq!(Part::default().content_type, None);
    }
}
