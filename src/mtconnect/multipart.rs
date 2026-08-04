//! # Multipart framing (LLD §6) — the seam the streaming reader is built on
//!
//! An `interval`-driven `/sample` response is one long multipart body: a boundary, two headers, a
//! document, repeat. Two content types occur in the field and **both are accepted**:
//! `multipart/x-mixed-replace` (what the standard specifies) and `multipart/mixed` (what cppagent
//! 2.7.0.12 actually sends — verified live, HLD §2).
//!
//! [`MultipartReader`] is the whole framing path: the boundary read off the content type, a
//! bounded accumulator that refuses to hold more than `maxDocumentBytes` while a part is
//! incomplete, and the incremental splitter ([`MultipartReader::next_part`]) that turns transport
//! chunks — split anywhere, including inside a boundary marker — into complete [`Part`]s. A part's
//! own `Content-length` is trusted but capped; a part without one is delimited by the next
//! boundary; junk between parts is skipped, never buffered. Anything that cannot be framed is a
//! [`MtcError::Multipart`]/[`MtcError::TooLarge`], which the streaming state machine answers by
//! dropping and re-establishing the stream (recovery ladder 1).

use super::error::MtcError;

/// Cap on one part's header block — two headers and slack, not a document.
pub const MAX_PART_HEADER_BYTES: usize = 8 * 1024;

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
    let base = content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
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

/// Where the splitter is inside the multipart grammar. The consumed prefix of the buffer is
/// drained as each state completes, so the state always describes the buffer's *start*.
#[derive(Debug, Clone, PartialEq, Eq)]
enum SplitState {
    /// Scanning for the next `--<boundary>` marker; everything before it is preamble or
    /// between-part junk and is discarded, never buffered.
    Seek,
    /// A marker was just consumed: decide terminator (`--`) vs the start of a part.
    AfterMarker,
    /// Reading the part's headers until the blank line.
    Headers,
    /// Reading the part's body.
    Body {
        content_type: Option<String>,
        content_length: Option<usize>,
    },
}

/// A bounded incremental splitter for an in-flight multipart body.
///
/// It owns the invariants the streaming reader depends on: the boundary it frames against, and the
/// cap on how much may be held while a part is incomplete. A part that runs past the cap is a
/// [`MtcError::TooLarge`] — the stream is dropped and re-established rather than buffered.
#[derive(Debug)]
pub struct MultipartReader {
    boundary: String,
    max_part_bytes: usize,
    buffer: Vec<u8>,
    state: SplitState,
    finished: bool,
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
        Ok(Self {
            boundary,
            max_part_bytes,
            buffer: Vec::new(),
            state: SplitState::Seek,
            finished: false,
        })
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

    /// Whether the terminating `--<boundary>--` marker was seen. A finished body yields no further
    /// parts; anything after the terminator is epilogue and is discarded.
    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    /// Append a chunk from the transport.
    ///
    /// # Errors
    /// [`MtcError::TooLarge`] when the incomplete part would exceed the cap (plus the bounded
    /// header allowance).
    pub fn push(&mut self, chunk: &[u8]) -> Result<(), MtcError> {
        if self.buffer.len() + chunk.len() > self.max_part_bytes + MAX_PART_HEADER_BYTES {
            return Err(MtcError::TooLarge {
                limit: self.max_part_bytes,
            });
        }
        self.buffer.extend_from_slice(chunk);
        Ok(())
    }

    /// Drop everything buffered — the stream is being re-established, so a half-part from the old
    /// connection must never be framed against the new one.
    pub fn reset(&mut self) {
        self.buffer.clear();
        self.state = SplitState::Seek;
        self.finished = false;
    }

    /// Take the buffered bytes, leaving the reader empty (diagnosis and tests).
    #[must_use]
    pub fn take_buffer(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.buffer)
    }

    /// The next complete part, or `None` when more bytes are needed (call [`Self::push`] and try
    /// again). Call in a loop after every push — one chunk can complete several parts.
    ///
    /// # Errors
    /// [`MtcError::Multipart`] for unframeable input (a boundary line that never ends, a header
    /// block past its cap, a header line with no `:`); [`MtcError::TooLarge`] when a part's
    /// declared or delimited body exceeds the cap. Both are stream-fatal: the caller drops and
    /// re-establishes (recovery ladder 1).
    pub fn next_part(&mut self) -> Result<Option<Part>, MtcError> {
        loop {
            if self.finished {
                self.buffer.clear();
                return Ok(None);
            }
            match self.state.clone() {
                SplitState::Seek => {
                    let marker = format!("--{}", self.boundary).into_bytes();
                    match find(&self.buffer, &marker) {
                        Some(i) => {
                            self.buffer.drain(..i + marker.len());
                            self.state = SplitState::AfterMarker;
                        }
                        None => {
                            // Junk is discarded, not buffered: keep only the tail that could be a
                            // partial marker split across chunks.
                            let keep = (marker.len() - 1).min(self.buffer.len());
                            let cut = self.buffer.len() - keep;
                            if cut > 0 {
                                self.buffer.drain(..cut);
                            }
                            return Ok(None);
                        }
                    }
                }
                SplitState::AfterMarker => {
                    if self.buffer.len() < 2 {
                        return Ok(None);
                    }
                    if self.buffer.starts_with(b"--") {
                        self.finished = true;
                        self.buffer.clear();
                        return Ok(None);
                    }
                    if self.buffer.starts_with(b"\r\n") {
                        self.buffer.drain(..2);
                    } else if self.buffer[0] == b'\n' {
                        self.buffer.drain(..1);
                    } else {
                        return Err(MtcError::Multipart(
                            "boundary marker not followed by a line break or terminator".into(),
                        ));
                    }
                    self.state = SplitState::Headers;
                }
                SplitState::Headers => {
                    let Some((end, sep)) = find_blank_line(&self.buffer) else {
                        if self.buffer.len() > MAX_PART_HEADER_BYTES {
                            return Err(MtcError::Multipart(format!(
                                "part headers exceed {MAX_PART_HEADER_BYTES} bytes"
                            )));
                        }
                        return Ok(None);
                    };
                    let (content_type, content_length) = parse_part_headers(&self.buffer[..end])?;
                    if let Some(len) = content_length {
                        if len > self.max_part_bytes {
                            return Err(MtcError::TooLarge {
                                limit: self.max_part_bytes,
                            });
                        }
                    }
                    self.buffer.drain(..end + sep);
                    self.state = SplitState::Body {
                        content_type,
                        content_length,
                    };
                }
                SplitState::Body {
                    content_type,
                    content_length,
                } => {
                    match content_length {
                        // The declared length is trusted (a body may contain boundary-looking
                        // bytes) but was capped above.
                        Some(len) => {
                            if self.buffer.len() < len {
                                return Ok(None);
                            }
                            let body: Vec<u8> = self.buffer.drain(..len).collect();
                            self.state = SplitState::Seek;
                            return Ok(Some(Part {
                                content_type,
                                content_length: Some(len),
                                body,
                            }));
                        }
                        // No declared length: the body runs to the next boundary line.
                        None => {
                            let delim = format!("\n--{}", self.boundary).into_bytes();
                            let Some(i) = find(&self.buffer, &delim) else {
                                if self.buffer.len() > self.max_part_bytes + delim.len() {
                                    return Err(MtcError::TooLarge {
                                        limit: self.max_part_bytes,
                                    });
                                }
                                return Ok(None);
                            };
                            let end = if i > 0 && self.buffer[i - 1] == b'\r' {
                                i - 1
                            } else {
                                i
                            };
                            let body: Vec<u8> = self.buffer[..end].to_vec();
                            // Consume through the newline; the marker itself is re-found by Seek.
                            self.buffer.drain(..=i);
                            self.state = SplitState::Seek;
                            let len = body.len();
                            if len > self.max_part_bytes {
                                return Err(MtcError::TooLarge {
                                    limit: self.max_part_bytes,
                                });
                            }
                            return Ok(Some(Part {
                                content_type,
                                content_length: None,
                                body,
                            }));
                        }
                    }
                }
            }
        }
    }
}

/// First occurrence of `needle` in `haystack`.
fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// The end of the header block and the separator width: `\r\n\r\n` (4) or `\n\n` (2), whichever
/// comes first.
fn find_blank_line(buf: &[u8]) -> Option<(usize, usize)> {
    let crlf = find(buf, b"\r\n\r\n").map(|i| (i, 4));
    let lf = find(buf, b"\n\n").map(|i| (i, 2));
    match (crlf, lf) {
        (Some(a), Some(b)) => Some(if a.0 <= b.0 { a } else { b }),
        (a, b) => a.or(b),
    }
}

/// Parse a part's header block into the two headers this client reads. Unknown headers are
/// ignored; a line without a `:` is unframeable.
fn parse_part_headers(head: &[u8]) -> Result<(Option<String>, Option<usize>), MtcError> {
    let text = std::str::from_utf8(head)
        .map_err(|_| MtcError::Multipart("part headers are not valid UTF-8".into()))?;
    let mut content_type = None;
    let mut content_length = None;
    for line in text.split('\n') {
        let line = line.trim_end_matches('\r');
        if line.trim().is_empty() {
            continue;
        }
        let Some((name, value)) = line.split_once(':') else {
            return Err(MtcError::Multipart(format!(
                "malformed part header line `{line}`"
            )));
        };
        match name.trim().to_ascii_lowercase().as_str() {
            "content-type" => content_type = Some(value.trim().to_string()),
            "content-length" => {
                content_length = Some(value.trim().parse::<usize>().map_err(|_| {
                    MtcError::Multipart(format!("unparseable Content-length `{}`", value.trim()))
                })?);
            }
            _ => {}
        }
    }
    Ok((content_type, content_length))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn both_observed_content_types_are_accepted() {
        // The standard's type, and the one cppagent 2.7 actually sends.
        assert!(is_multipart(
            "multipart/x-mixed-replace;boundary=--------------------------"
        ));
        assert!(is_multipart("multipart/mixed; boundary=abc"));
        assert!(
            is_multipart("MULTIPART/MIXED; boundary=abc"),
            "case is not semantics"
        );
        assert!(!is_multipart("application/xml"));
        assert!(!is_multipart("multipart/form-data; boundary=abc"));
    }

    #[test]
    fn the_boundary_is_read_off_the_content_type() {
        assert_eq!(
            boundary_from_content_type(
                "multipart/x-mixed-replace;boundary=--------------------------"
            ),
            Some("--------------------------".to_string())
        );
        assert_eq!(
            boundary_from_content_type("multipart/mixed; boundary=\"quoted-boundary\""),
            Some("quoted-boundary".to_string())
        );
        assert_eq!(
            boundary_from_content_type("multipart/mixed"),
            None,
            "no boundary declared"
        );
        assert_eq!(
            boundary_from_content_type("multipart/mixed; boundary="),
            None
        );
        assert_eq!(
            boundary_from_content_type("application/xml; boundary=x"),
            None
        );
    }

    #[test]
    fn a_reader_needs_a_multipart_content_type_with_a_boundary() {
        let r = MultipartReader::from_content_type("multipart/mixed; boundary=xyz", 1024).unwrap();
        assert_eq!(r.boundary(), "xyz");
        assert!(r.buffered().is_empty());

        for bad in ["application/xml", "multipart/mixed", ""] {
            assert!(
                matches!(
                    MultipartReader::from_content_type(bad, 1024),
                    Err(MtcError::Multipart(_))
                ),
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
        // The buffer cap is the part cap plus the bounded header allowance — one byte past it and
        // the stream is dropped rather than buffered.
        let filler = vec![b'z'; MAX_PART_HEADER_BYTES + 1];
        assert!(matches!(
            r.push(&filler),
            Err(MtcError::TooLarge { limit: 8 })
        ));

        assert_eq!(r.take_buffer(), b"12345678".to_vec());
        assert!(r.buffered().is_empty(), "taking the buffer empties it");

        r.push(b"abc").unwrap();
        r.reset();
        assert!(
            r.buffered().is_empty(),
            "a re-established stream starts clean"
        );
    }

    // --- the splitter -----------------------------------------------------------------------

    /// One cppagent-shaped part: marker line, two headers, body of the declared length.
    fn part_bytes(boundary: &str, body: &str) -> Vec<u8> {
        format!(
            "--{boundary}\r\nContent-type: text/xml\r\nContent-length: {}\r\n\r\n{body}\r\n",
            body.len()
        )
        .into_bytes()
    }

    fn drain(r: &mut MultipartReader) -> Vec<Part> {
        let mut out = Vec::new();
        while let Some(p) = r.next_part().unwrap() {
            out.push(p);
        }
        out
    }

    #[test]
    fn a_whole_body_splits_into_its_parts_under_both_content_types() {
        for ct in [
            "multipart/x-mixed-replace;boundary=B",
            "multipart/mixed; boundary=B",
        ] {
            let mut r = MultipartReader::from_content_type(ct, 1024).unwrap();
            let mut body = part_bytes("B", "<one/>");
            body.extend_from_slice(&part_bytes("B", "<two/>"));
            body.extend_from_slice(b"--B--\r\n");
            r.push(&body).unwrap();
            let parts = drain(&mut r);
            assert_eq!(parts.len(), 2, "{ct}");
            assert_eq!(parts[0].text().unwrap(), "<one/>");
            assert_eq!(parts[0].content_type.as_deref(), Some("text/xml"));
            assert_eq!(parts[0].content_length, Some(6));
            assert_eq!(parts[1].text().unwrap(), "<two/>");
            assert!(r.is_finished(), "the terminator ends the stream");
        }
    }

    #[test]
    fn a_part_split_anywhere_across_chunks_still_frames() {
        // Byte-at-a-time is the worst possible chunking: every boundary, header and body byte
        // arrives alone. The framing must not care.
        let mut whole = part_bytes("bound", "<a>1</a>");
        whole.extend_from_slice(&part_bytes("bound", "<b>2</b>"));
        let mut r =
            MultipartReader::from_content_type("multipart/mixed; boundary=bound", 1024).unwrap();
        let mut parts = Vec::new();
        for byte in whole {
            r.push(&[byte]).unwrap();
            parts.extend(drain(&mut r));
        }
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].text().unwrap(), "<a>1</a>");
        assert_eq!(parts[1].text().unwrap(), "<b>2</b>");
    }

    #[test]
    fn preamble_and_junk_between_parts_are_skipped_never_buffered() {
        let mut r =
            MultipartReader::from_content_type("multipart/mixed; boundary=B", 1024).unwrap();
        r.push(b"some preamble the sender should not have written\r\n")
            .unwrap();
        r.push(&part_bytes("B", "<ok/>")).unwrap();
        r.push(b"junk between parts").unwrap();
        r.push(&part_bytes("B", "<ok2/>")).unwrap();
        let parts = drain(&mut r);
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].text().unwrap(), "<ok/>");
        assert_eq!(parts[1].text().unwrap(), "<ok2/>");

        // Endless junk with no boundary is discarded down to a partial-marker tail, so a chatty
        // but boundary-less stream cannot grow the buffer.
        let mut r = MultipartReader::from_content_type("multipart/mixed; boundary=B", 64).unwrap();
        for _ in 0..1000 {
            r.push(b"noise ").unwrap();
            assert!(r.next_part().unwrap().is_none());
        }
        assert!(r.buffered().len() < 8, "junk does not accumulate");
    }

    #[test]
    fn a_declared_length_is_trusted_so_a_body_may_contain_boundary_bytes() {
        let body = "<x>--B inside</x>";
        let mut r =
            MultipartReader::from_content_type("multipart/mixed; boundary=B", 1024).unwrap();
        r.push(&part_bytes("B", body)).unwrap();
        let parts = drain(&mut r);
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].text().unwrap(), body);
    }

    #[test]
    fn a_missing_content_length_falls_back_to_boundary_delimiting() {
        let mut r =
            MultipartReader::from_content_type("multipart/mixed; boundary=B", 1024).unwrap();
        r.push(b"--B\r\nContent-type: text/xml\r\n\r\n<no-length/>\r\n--B\r\n")
            .unwrap();
        r.push(b"Content-type: text/xml\r\nContent-length: 5\r\n\r\n<t/>x\r\n")
            .unwrap();
        let parts = drain(&mut r);
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].text().unwrap(), "<no-length/>");
        assert_eq!(parts[0].content_length, None);
        assert_eq!(parts[1].text().unwrap(), "<t/>x");
    }

    #[test]
    fn an_oversize_part_is_refused_declared_or_not() {
        // Declared: refused from the header alone, before any body arrives.
        let mut r = MultipartReader::from_content_type("multipart/mixed; boundary=B", 16).unwrap();
        r.push(b"--B\r\nContent-length: 1000\r\n\r\n").unwrap();
        assert!(matches!(
            r.next_part(),
            Err(MtcError::TooLarge { limit: 16 })
        ));

        // Undeclared: refused when the delimited body runs past the cap without a boundary.
        let mut r = MultipartReader::from_content_type("multipart/mixed; boundary=B", 16).unwrap();
        r.push(b"--B\r\nContent-type: text/xml\r\n\r\n").unwrap();
        r.push(&[b'x'; 32]).unwrap();
        assert!(matches!(
            r.next_part(),
            Err(MtcError::TooLarge { limit: 16 })
        ));
    }

    #[test]
    fn malformed_framing_is_an_error_not_a_guess() {
        // A marker line that continues with garbage instead of a line break or terminator.
        let mut r =
            MultipartReader::from_content_type("multipart/mixed; boundary=B", 1024).unwrap();
        r.push(b"--Bgarbage here").unwrap();
        assert!(matches!(r.next_part(), Err(MtcError::Multipart(_))));

        // A header line with no colon.
        let mut r =
            MultipartReader::from_content_type("multipart/mixed; boundary=B", 1024).unwrap();
        r.push(b"--B\r\nnot a header line\r\n\r\nbody").unwrap();
        assert!(matches!(r.next_part(), Err(MtcError::Multipart(_))));

        // An unparseable Content-length.
        let mut r =
            MultipartReader::from_content_type("multipart/mixed; boundary=B", 1024).unwrap();
        r.push(b"--B\r\nContent-length: lots\r\n\r\n").unwrap();
        assert!(matches!(r.next_part(), Err(MtcError::Multipart(_))));

        // A header block that never ends.
        let mut r =
            MultipartReader::from_content_type("multipart/mixed; boundary=B", 1 << 20).unwrap();
        r.push(b"--B\r\n").unwrap();
        r.push(&vec![b'h'; MAX_PART_HEADER_BYTES + 2]).unwrap();
        assert!(matches!(r.next_part(), Err(MtcError::Multipart(_))));
    }

    #[test]
    fn bare_lf_line_endings_are_tolerated() {
        // Some proxies rewrite CRLF; the grammar survives it.
        let mut r =
            MultipartReader::from_content_type("multipart/mixed; boundary=B", 1024).unwrap();
        r.push(b"--B\nContent-type: text/xml\nContent-length: 4\n\n<a/>\n")
            .unwrap();
        let parts = drain(&mut r);
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].text().unwrap(), "<a/>");
    }

    #[test]
    fn the_terminator_ends_the_stream_and_the_epilogue_is_ignored() {
        let mut r =
            MultipartReader::from_content_type("multipart/mixed; boundary=B", 1024).unwrap();
        r.push(&part_bytes("B", "<last/>")).unwrap();
        r.push(b"--B--\r\nepilogue nobody reads").unwrap();
        let parts = drain(&mut r);
        assert_eq!(parts.len(), 1);
        assert!(r.is_finished());
        r.push(b"more epilogue").unwrap();
        assert!(
            r.next_part().unwrap().is_none(),
            "a finished stream yields nothing"
        );
        assert!(r.buffered().is_empty());

        // Reset clears the terminal state too (a re-established stream starts clean).
        r.reset();
        assert!(!r.is_finished());
    }

    #[test]
    fn a_part_body_is_text_only_when_it_really_is_text() {
        let part = Part {
            body: b"<MTConnectStreams/>".to_vec(),
            ..Part::default()
        };
        assert_eq!(part.text().unwrap(), "<MTConnectStreams/>");
        let part = Part {
            body: vec![0xff, 0xfe],
            ..Part::default()
        };
        assert!(matches!(part.text(), Err(MtcError::Xml(_))));
        assert_eq!(Part::default().content_length, None);
        assert_eq!(Part::default().content_type, None);
    }
}
