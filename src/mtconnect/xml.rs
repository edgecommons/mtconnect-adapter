//! # Namespace-tolerant MTConnect XML parsing (LLD §6)
//!
//! One pull parser ([`quick_xml`]) behind three document parsers — [`parse_devices`],
//! [`parse_streams`], [`parse_errors`] — plus the shared [`MtcHeader`].
//!
//! ## Why local names, and nothing else
//!
//! Every MTConnect schema version puts its elements in a *different* namespace URI
//! (`urn:mtconnect.org:MTConnectStreams:1.3` … `:2.7`). Matching qualified names would make the
//! parser version-locked; matching **local names** makes one parser serve 1.3 through 2.7 (and read
//! a document with no namespace at all, which agents in the wild do emit). The namespace URI is
//! still *captured* — as [`NsVersion`] — because the version is worth reporting and a document
//! below the 1.3 floor is worth refusing.
//!
//! ## XXE-inert by construction
//!
//! `quick-xml` resolves no entities and fetches no external resources. A `<!DOCTYPE>` is a token
//! this parser ignores, and a `&whatever;` general reference is surfaced as its own event: the five
//! predefined entities and numeric character references are expanded here, and **anything else is
//! dropped and counted** ([`XmlDoc::unresolved_entities`]). There is no code path from a document to
//! the filesystem or the network.
//!
//! ## Bounded
//!
//! Depth ([`MAX_DEPTH`]), attribute count ([`MAX_ATTRIBUTES`]), attribute length
//! ([`MAX_ATTR_VALUE_BYTES`]) and total element count ([`MAX_NODES`]) are capped, so a hostile or
//! broken document costs a bounded amount of memory before it is refused. The byte size of the
//! document itself is capped upstream by `maxDocumentBytes` in [`super::client`] — but bytes alone
//! do not bound the tree, which is why [`MAX_NODES`] exists.

use std::collections::BTreeMap;

use quick_xml::Reader;
use quick_xml::events::Event;

use super::error::MtcError;
use super::model::Category;

/// Maximum element nesting depth.
pub const MAX_DEPTH: usize = 64;
/// Maximum number of attributes on one element.
pub const MAX_ATTRIBUTES: usize = 64;
/// Maximum length of one attribute value, in bytes.
pub const MAX_ATTR_VALUE_BYTES: usize = 64 * 1024;
/// Maximum number of elements in one document.
///
/// `maxDocumentBytes` bounds the *bytes* read off the socket; it does not bound the DOM those
/// bytes expand into. One `<a/>` costs four bytes on the wire and an [`XmlElem`] — a `String`
/// name, a `BTreeMap`, a `String` text and a `Vec` — in memory, a 25–50× amplification. A 16 MiB
/// document of millions of empty elements therefore stays inside the byte cap while consuming
/// hundreds of megabytes of heap. Real MTConnect content (an observation carries identity
/// attributes and a value) stays orders of magnitude below this ceiling.
pub const MAX_NODES: usize = 250_000;

/// The namespace-URI prefix every MTConnect schema version shares. A declaration that does not
/// start with it is some other vocabulary (`xmlns:xsi`, a vendor extension) and carries no
/// MTConnect version.
const MTCONNECT_NS_PREFIX: &str = "urn:mtconnect.org:";

/// The lowest MTConnect schema version this client parses.
pub const MIN_VERSION: NsVersion = NsVersion { major: 1, minor: 3 };
/// The highest MTConnect schema version this client was written against. Newer documents still
/// parse (local-name matching is forward-compatible) but are flagged by [`NsVersion::is_known`].
pub const MAX_KNOWN_VERSION: NsVersion = NsVersion { major: 2, minor: 7 };

// =================================================================================================
// The generic element tree
// =================================================================================================

/// One element, reduced to what MTConnect documents actually carry: a local name, its attributes,
/// its text, and its children. Attributes are held **sorted** (`BTreeMap`) — that is what makes the
/// probe digest in [`super::model`] canonical without a second pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct XmlElem {
    /// The element's local name — never a prefixed/qualified name.
    pub name: String,
    /// Attributes by local-or-prefixed name, sorted.
    pub attrs: BTreeMap<String, String>,
    /// The element's own character data, trimmed. Empty when it has none.
    pub text: String,
    pub children: Vec<XmlElem>,
}

impl XmlElem {
    #[must_use]
    pub fn attr(&self, key: &str) -> Option<&str> {
        self.attrs.get(key).map(String::as_str)
    }

    /// The first direct child with this local name.
    #[must_use]
    pub fn child(&self, name: &str) -> Option<&XmlElem> {
        self.children.iter().find(|c| c.name == name)
    }

    /// Every direct child with this local name.
    pub fn children_named<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a XmlElem> {
        self.children.iter().filter(move |c| c.name == name)
    }
}

/// A parsed document: its root element, the MTConnect schema version it declared (when any —
/// default or prefixed declaration alike), and the count of general references that were **not**
/// expanded (the XXE-inertness signal).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XmlDoc {
    pub root: XmlElem,
    pub ns_version: Option<NsVersion>,
    pub unresolved_entities: u64,
}

/// An MTConnect schema version, taken from the document's MTConnect namespace declaration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct NsVersion {
    pub major: u16,
    pub minor: u16,
}

impl NsVersion {
    /// Whether this version is one the client was written against (1.3–2.7). A newer document still
    /// parses — local-name matching is forward-compatible — but the flag lets the runtime say so.
    #[must_use]
    pub fn is_known(self) -> bool {
        self >= MIN_VERSION && self <= MAX_KNOWN_VERSION
    }
}

impl std::fmt::Display for NsVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}

/// Extract the version from an MTConnect namespace URI
/// (`urn:mtconnect.org:MTConnectDevices:1.3` → `1.3`). Returns `None` for anything else — including
/// a colon-bearing URI from another vocabulary, which must never be read as an MTConnect version.
#[must_use]
pub fn parse_ns_version(uri: &str) -> Option<NsVersion> {
    if !uri.starts_with(MTCONNECT_NS_PREFIX) {
        return None;
    }
    let tail = uri.rsplit(':').next()?;
    let mut parts = tail.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next().unwrap_or("0").parse().ok()?;
    Some(NsVersion { major, minor })
}

/// Which MTConnect document this is, by root local name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DocKind {
    Devices,
    Streams,
    Errors,
    /// A root element this client does not serve, kept verbatim for the error message.
    Unknown(String),
}

/// Classify a document by its root element's local name — the streaming reader's dispatch (a part
/// may be a Streams document or an Errors document, and it must not guess).
#[must_use]
pub fn document_kind(root_name: &str) -> DocKind {
    match root_name {
        "MTConnectDevices" => DocKind::Devices,
        "MTConnectStreams" => DocKind::Streams,
        "MTConnectError" | "MTConnectErrors" => DocKind::Errors,
        other => DocKind::Unknown(other.to_string()),
    }
}

// =================================================================================================
// The header
// =================================================================================================

/// The `<Header>` every MTConnect document carries. Every field but `instance_id` is optional
/// because the three document types populate different subsets (a Devices header has no sequence
/// window; an Errors header may carry only the agent identity).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MtcHeader {
    /// The agent's incarnation. A change means the agent restarted and every sequence is void.
    pub instance_id: u64,
    pub buffer_size: Option<u64>,
    pub first_sequence: Option<u64>,
    pub last_sequence: Option<u64>,
    pub next_sequence: Option<u64>,
    /// The agent's reported version string (e.g. `2.7.0.12`).
    pub version: Option<String>,
    pub sender: Option<String>,
    pub creation_time: Option<String>,
}

impl MtcHeader {
    fn from_elem(e: &XmlElem) -> Self {
        let num = |k: &str| e.attr(k).and_then(|v| v.parse::<u64>().ok());
        Self {
            instance_id: num("instanceId").unwrap_or(0),
            buffer_size: num("bufferSize"),
            first_sequence: num("firstSequence"),
            last_sequence: num("lastSequence"),
            next_sequence: num("nextSequence"),
            version: e.attr("version").map(str::to_string),
            sender: e.attr("sender").map(str::to_string),
            creation_time: e.attr("creationTime").map(str::to_string),
        }
    }
}

// =================================================================================================
// Document parsers
// =================================================================================================

/// A parsed `MTConnectDevices` document: the header, the version, and each `<Device>` subtree
/// verbatim (the tree [`super::model`] projects and digests).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DevicesDoc {
    pub header: MtcHeader,
    pub ns_version: Option<NsVersion>,
    pub devices: Vec<XmlElem>,
    /// Elements the parser did not recognize but did not drop — the forward-compatibility measure.
    pub unknown_elements: u64,
}

impl DevicesDoc {
    /// The `<Device>` with this uuid.
    #[must_use]
    pub fn device(&self, uuid: &str) -> Option<&XmlElem> {
        self.devices.iter().find(|d| d.attr("uuid") == Some(uuid))
    }
}

/// One `<DeviceStream>` of an `MTConnectStreams` document, already demultiplexed by device uuid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceStream {
    pub uuid: String,
    pub name: Option<String>,
    /// Every observation element, tagged with the category of the group that held it.
    pub entries: Vec<StreamEntry>,
}

/// One observation element plus the category its enclosing group declared (`<Samples>`,
/// `<Events>`, `<Condition>`) — decoded into an [`super::observations::Observation`] against the
/// device model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamEntry {
    pub category: Category,
    pub elem: XmlElem,
    /// The `Component`/`ComponentStream` name that held it, for diagnosis.
    pub component: Option<String>,
}

/// A parsed `MTConnectStreams` document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsDoc {
    pub header: MtcHeader,
    pub ns_version: Option<NsVersion>,
    pub device_streams: Vec<DeviceStream>,
    pub unknown_elements: u64,
}

impl StreamsDoc {
    /// Whether this document carried no observations at all — the agent's **heartbeat** document,
    /// which proves liveness without data.
    #[must_use]
    pub fn is_heartbeat(&self) -> bool {
        self.device_streams.iter().all(|d| d.entries.is_empty())
    }
}

/// One `<Error>` of an `MTConnectError(s)` document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentErrorEntry {
    pub code: String,
    pub message: String,
}

/// A parsed `MTConnectError(s)` document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorsDoc {
    pub header: MtcHeader,
    pub ns_version: Option<NsVersion>,
    pub errors: Vec<AgentErrorEntry>,
    pub unknown_elements: u64,
}

impl ErrorsDoc {
    /// The `OUT_OF_RANGE` error's reported `firstSequence`, when this document is the buffer-overrun
    /// error (recovery ladder 2). The value comes from the header, which the standard requires the
    /// agent to send with the error.
    #[must_use]
    pub fn out_of_range(&self) -> Option<u64> {
        self.errors
            .iter()
            .any(|e| e.code.eq_ignore_ascii_case("OUT_OF_RANGE"))
            .then(|| self.header.first_sequence.unwrap_or(0))
    }
}

/// Structural element names of a Devices document. Anything else inside a `<Device>` subtree — a
/// vendor extension, a 2.8 element — is retained in the tree (so the digest still sees it) and
/// counted as unknown.
const KNOWN_DEVICE_ELEMENTS: &[&str] = &[
    "Description",
    "DataItems",
    "DataItem",
    "Components",
    "Compositions",
    "Composition",
    "Configuration",
    "Filters",
    "Filter",
    "Constraints",
    "Minimum",
    "Maximum",
    "Value",
    "InitialValue",
    "Source",
    "References",
    "DataItemRef",
    "ComponentRef",
    "Definition",
    "EntryDefinitions",
    "CellDefinitions",
    "Relationships",
    "SolidModel",
    "SensorConfiguration",
    "Specifications",
    "CoordinateSystems",
    "Motion",
];

/// Parse an `MTConnectDevices` document.
///
/// # Errors
/// [`MtcError::Xml`] for malformed XML, a cap breach, or a root element that is not
/// `MTConnectDevices`; [`MtcError::UnsupportedVersion`] below the 1.3 floor.
pub fn parse_devices(xml: &str) -> Result<DevicesDoc, MtcError> {
    let doc = parse_document(xml)?;
    expect_root(&doc.root, "MTConnectDevices")?;

    let header = doc
        .root
        .child("Header")
        .map(MtcHeader::from_elem)
        .unwrap_or_default();
    let mut unknown = doc.unresolved_entities;
    let mut devices = Vec::new();
    for child in &doc.root.children {
        match child.name.as_str() {
            "Header" => {}
            "Devices" => {
                for d in child.children_named("Device") {
                    unknown += count_unknown_device_elements(d, false);
                    devices.push(d.clone());
                }
                unknown += child.children.iter().filter(|c| c.name != "Device").count() as u64;
            }
            _ => unknown += 1,
        }
    }
    Ok(DevicesDoc {
        header,
        ns_version: doc.ns_version,
        devices,
        unknown_elements: unknown,
    })
}

/// Count elements inside a `<Device>` subtree the parser does not structurally recognize.
/// `in_components` marks the children of a `<Components>` element: component *types* are an open
/// set by design (a vendor may ship `<Vise>`), so they are never "unknown".
fn count_unknown_device_elements(e: &XmlElem, in_components: bool) -> u64 {
    let mut n = 0;
    for c in &e.children {
        let known = in_components || KNOWN_DEVICE_ELEMENTS.contains(&c.name.as_str());
        if !known {
            n += 1;
        }
        n += count_unknown_device_elements(c, c.name == "Components");
    }
    n
}

/// Parse an `MTConnectStreams` document (a `/current` snapshot, a `/sample` window, or a heartbeat).
///
/// # Errors
/// [`MtcError::Xml`] for malformed XML, a cap breach, or a wrong root;
/// [`MtcError::UnsupportedVersion`] below the 1.3 floor.
pub fn parse_streams(xml: &str) -> Result<StreamsDoc, MtcError> {
    streams_from_doc(parse_document(xml)?)
}

/// Build a [`StreamsDoc`] from an already-tokenized document — the streaming reader's path, where
/// the root had to be sniffed first ([`document_kind`]) and tokenizing twice would double the cost
/// of every part.
///
/// # Errors
/// [`MtcError::Xml`] when the root is not `MTConnectStreams`.
pub fn streams_from_doc(doc: XmlDoc) -> Result<StreamsDoc, MtcError> {
    expect_root(&doc.root, "MTConnectStreams")?;

    let header = doc
        .root
        .child("Header")
        .map(MtcHeader::from_elem)
        .unwrap_or_default();
    let mut unknown = doc.unresolved_entities;
    let mut device_streams = Vec::new();

    for child in &doc.root.children {
        match child.name.as_str() {
            "Header" => {}
            "Streams" => {
                for ds in &child.children {
                    if ds.name != "DeviceStream" {
                        unknown += 1;
                        continue;
                    }
                    let mut entries = Vec::new();
                    collect_component_streams(ds, &mut entries, &mut unknown);
                    device_streams.push(DeviceStream {
                        uuid: ds.attr("uuid").unwrap_or_default().to_string(),
                        name: ds.attr("name").map(str::to_string),
                        entries,
                    });
                }
            }
            _ => unknown += 1,
        }
    }

    Ok(StreamsDoc {
        header,
        ns_version: doc.ns_version,
        device_streams,
        unknown_elements: unknown,
    })
}

fn collect_component_streams(e: &XmlElem, out: &mut Vec<StreamEntry>, unknown: &mut u64) {
    for cs in &e.children {
        if cs.name != "ComponentStream" {
            *unknown += 1;
            continue;
        }
        let component = cs
            .attr("name")
            .or_else(|| cs.attr("component"))
            .map(str::to_string);
        for group in &cs.children {
            let category = match group.name.as_str() {
                "Samples" => Category::Sample,
                "Events" => Category::Event,
                "Condition" => Category::Condition,
                // A group this client does not know: skipped, counted, never guessed at.
                _ => {
                    *unknown += 1;
                    continue;
                }
            };
            for obs in &group.children {
                out.push(StreamEntry {
                    category,
                    elem: obs.clone(),
                    component: component.clone(),
                });
            }
        }
    }
}

/// Parse an `MTConnectError`/`MTConnectErrors` document.
///
/// # Errors
/// [`MtcError::Xml`] for malformed XML, a cap breach, or a wrong root;
/// [`MtcError::UnsupportedVersion`] below the 1.3 floor.
pub fn parse_errors(xml: &str) -> Result<ErrorsDoc, MtcError> {
    errors_from_doc(parse_document(xml)?)
}

/// Build an [`ErrorsDoc`] from an already-tokenized document (see [`streams_from_doc`]).
///
/// # Errors
/// [`MtcError::Xml`] when the root is not an `MTConnectError(s)` element.
pub fn errors_from_doc(doc: XmlDoc) -> Result<ErrorsDoc, MtcError> {
    let root = &doc.root;
    if document_kind(&root.name) != DocKind::Errors {
        return Err(MtcError::Xml(format!(
            "expected an MTConnectError(s) document, got `{}`",
            root.name
        )));
    }

    let header = root
        .child("Header")
        .map(MtcHeader::from_elem)
        .unwrap_or_default();
    let mut unknown = doc.unresolved_entities;
    let mut errors = Vec::new();
    let mut push = |e: &XmlElem| {
        errors.push(AgentErrorEntry {
            code: e.attr("errorCode").unwrap_or("UNKNOWN").to_string(),
            message: e.text.clone(),
        });
    };

    for child in &root.children {
        match child.name.as_str() {
            "Header" => {}
            // 1.x puts a single <Error> directly under the root; 2.x wraps them in <Errors>.
            "Error" => push(child),
            "Errors" => {
                for e in &child.children {
                    if e.name == "Error" {
                        push(e);
                    } else {
                        unknown += 1;
                    }
                }
            }
            _ => unknown += 1,
        }
    }

    Ok(ErrorsDoc {
        header,
        ns_version: doc.ns_version,
        errors,
        unknown_elements: unknown,
    })
}

fn expect_root(root: &XmlElem, want: &str) -> Result<(), MtcError> {
    if root.name == want {
        Ok(())
    } else {
        Err(MtcError::Xml(format!(
            "expected a `{want}` document, got `{}`",
            root.name
        )))
    }
}

// =================================================================================================
// The tokenizer
// =================================================================================================

/// Parse any XML document into the generic [`XmlElem`] tree, capturing the MTConnect namespace
/// version and counting unexpanded general references.
///
/// # Errors
/// [`MtcError::Xml`] for malformed XML, an empty document, or a depth/attribute/node cap breach;
/// [`MtcError::UnsupportedVersion`] when the declared namespace version is below the 1.3 floor.
pub fn parse_document(xml: &str) -> Result<XmlDoc, MtcError> {
    let mut reader = Reader::from_str(xml);
    // Text is NOT trimmed per event: an element's characters can arrive in several events (a
    // predefined entity splits them), and trimming each piece would eat the spaces between them.
    // The accumulated text is trimmed once, when the element closes.
    reader.config_mut().trim_text(false);
    reader.config_mut().check_end_names = true;
    reader.config_mut().expand_empty_elements = false;

    let mut stack: Vec<XmlElem> = Vec::new();
    let mut root: Option<XmlElem> = None;
    let mut ns_uri: Option<String> = None;
    let mut unresolved_entities = 0u64;
    // A running total across the whole document, not a per-level count: the amplification
    // [`MAX_NODES`] guards against is flat, not deep, and the depth cap already bounds nesting.
    let mut nodes = 0usize;

    loop {
        match reader.read_event() {
            Err(e) => return Err(MtcError::Xml(format!("{e}"))),
            Ok(Event::Eof) => break,
            Ok(Event::Start(s)) => {
                push_element(
                    &mut stack,
                    element_from_start(&s, &mut ns_uri, &mut unresolved_entities)?,
                    &mut nodes,
                )?;
            }
            Ok(Event::Empty(s)) => {
                // An empty element opens and closes in one event.
                push_element(
                    &mut stack,
                    element_from_start(&s, &mut ns_uri, &mut unresolved_entities)?,
                    &mut nodes,
                )?;
                close_element(&mut stack, &mut root);
            }
            Ok(Event::End(_)) => close_element(&mut stack, &mut root),
            Ok(Event::Text(t)) => {
                if let Some(top) = stack.last_mut() {
                    let decoded = t.decode().map_err(|e| MtcError::Xml(format!("{e}")))?;
                    push_text(
                        &mut top.text,
                        &unescape_inert(&decoded, &mut unresolved_entities),
                    );
                }
            }
            Ok(Event::CData(c)) => {
                if let Some(top) = stack.last_mut() {
                    let decoded = String::from_utf8_lossy(c.as_ref()).into_owned();
                    push_text(&mut top.text, &decoded);
                }
            }
            Ok(Event::GeneralRef(r)) => {
                // The XXE seam: an entity reference is resolved ONLY if it is one of the five
                // predefined entities or a numeric character reference. Everything else — every
                // DTD-declared and every external entity — is dropped and counted.
                let raw = r.decode().map_err(|e| MtcError::Xml(format!("{e}")))?;
                let resolved = resolve_reference(&raw);
                if resolved.is_none() {
                    unresolved_entities += 1;
                }
                if let Some(top) = stack.last_mut() {
                    // An unresolvable reference stays **literal** — visible for diagnosis, and
                    // never a value fetched from disk or the network.
                    top.text
                        .push_str(&resolved.unwrap_or_else(|| format!("&{raw};")));
                }
            }
            // Declarations, DTDs, comments and processing instructions carry no data this client
            // reads. A `<!DOCTYPE>` is deliberately inert: nothing here resolves it.
            Ok(_) => {}
        }
    }

    if !stack.is_empty() {
        return Err(MtcError::Xml(
            "document ended inside an open element".into(),
        ));
    }
    let root = root.ok_or_else(|| MtcError::Xml("document has no root element".into()))?;

    let ns_version = ns_uri.as_deref().and_then(parse_ns_version);
    if let Some(v) = ns_version {
        if v < MIN_VERSION {
            return Err(MtcError::UnsupportedVersion(v.to_string()));
        }
    }
    Ok(XmlDoc {
        root,
        ns_version,
        unresolved_entities,
    })
}

fn push_element(
    stack: &mut Vec<XmlElem>,
    elem: XmlElem,
    nodes: &mut usize,
) -> Result<(), MtcError> {
    if stack.len() + 1 > MAX_DEPTH {
        return Err(MtcError::Xml(format!(
            "element nesting exceeds {MAX_DEPTH}"
        )));
    }
    *nodes += 1;
    if *nodes > MAX_NODES {
        return Err(MtcError::Xml(format!("element count exceeds {MAX_NODES}")));
    }
    stack.push(elem);
    Ok(())
}

fn close_element(stack: &mut Vec<XmlElem>, root: &mut Option<XmlElem>) {
    if let Some(mut done) = stack.pop() {
        if !done.text.is_empty() {
            let trimmed = done.text.trim();
            if trimmed.len() != done.text.len() {
                done.text = trimmed.to_string();
            }
        }
        match stack.last_mut() {
            Some(parent) => parent.children.push(done),
            None => *root = Some(done),
        }
    }
}

fn element_from_start(
    s: &quick_xml::events::BytesStart<'_>,
    ns_uri: &mut Option<String>,
    unresolved_entities: &mut u64,
) -> Result<XmlElem, MtcError> {
    let name = local_name(s.name().as_ref());
    let mut attrs = BTreeMap::new();
    let mut count = 0usize;

    for attr in s.attributes().with_checks(false) {
        let attr = attr.map_err(|e| MtcError::Xml(format!("attribute: {e}")))?;
        count += 1;
        if count > MAX_ATTRIBUTES {
            return Err(MtcError::Xml(format!(
                "element `{name}` exceeds {MAX_ATTRIBUTES} attributes"
            )));
        }
        if attr.value.len() > MAX_ATTR_VALUE_BYTES {
            return Err(MtcError::Xml(format!(
                "attribute value on `{name}` exceeds {MAX_ATTR_VALUE_BYTES} bytes"
            )));
        }
        let key = String::from_utf8_lossy(attr.key.as_ref()).into_owned();
        let value = unescape_inert(&String::from_utf8_lossy(&attr.value), unresolved_entities);
        // The version floor must not be bypassable by writing the same declaration with a prefix.
        // A DEFAULT declaration (`xmlns=`) and a PREFIXED one (`xmlns:m=`) are equally binding, and
        // an agent that qualifies its elements — `<m:MTConnectStreams xmlns:m="…:1.1">` — is
        // declaring exactly the version the floor exists to refuse. Foreign declarations
        // (`xmlns:xsi=`, a vendor extension) are skipped rather than latched, so the FIRST
        // MTConnect declaration wins wherever it appears in the attribute list.
        if ns_uri.is_none()
            && (key == "xmlns" || key.starts_with("xmlns:"))
            && value.starts_with(MTCONNECT_NS_PREFIX)
        {
            *ns_uri = Some(value.clone());
        }
        attrs.insert(key, value);
    }

    Ok(XmlElem {
        name,
        attrs,
        text: String::new(),
        children: Vec::new(),
    })
}

/// The local part of a possibly-prefixed name (`m:DataItem` → `DataItem`).
fn local_name(raw: &[u8]) -> String {
    let s = String::from_utf8_lossy(raw);
    match s.rsplit(':').next() {
        Some(local) => local.to_string(),
        None => s.into_owned(),
    }
}

fn push_text(dest: &mut String, text: &str) {
    dest.push_str(text);
}

/// Expand only what XML itself defines: the five predefined entities and numeric character
/// references. An unknown entity is left **literal** (never fetched, never expanded) and counted.
fn unescape_inert(raw: &str, dropped: &mut u64) -> String {
    if !raw.contains('&') {
        return raw.to_string();
    }
    let mut out = String::with_capacity(raw.len());
    let mut rest = raw;
    while let Some(i) = rest.find('&') {
        out.push_str(&rest[..i]);
        rest = &rest[i..];
        match rest.find(';') {
            Some(end) => {
                let body = &rest[1..end];
                match resolve_reference(body) {
                    Some(text) => out.push_str(&text),
                    None => {
                        *dropped += 1;
                        out.push('&');
                        out.push_str(body);
                        out.push(';');
                    }
                }
                rest = &rest[end + 1..];
            }
            None => break,
        }
    }
    out.push_str(rest);
    out
}

/// Resolve one reference body (`amp`, `#65`, `#x41`). `None` for anything else — which is exactly
/// what makes a DTD-declared or external entity inert.
fn resolve_reference(body: &str) -> Option<String> {
    match body {
        "amp" => Some("&".into()),
        "lt" => Some("<".into()),
        "gt" => Some(">".into()),
        "quot" => Some("\"".into()),
        "apos" => Some("'".into()),
        _ => {
            let digits = body.strip_prefix('#')?;
            let code = match digits
                .strip_prefix('x')
                .or_else(|| digits.strip_prefix('X'))
            {
                Some(hex) => u32::from_str_radix(hex, 16).ok()?,
                None => digits.parse::<u32>().ok()?,
            };
            char::from_u32(code).map(|c| c.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DEVICES_2_7: &str = include_str!("../../tests/fixtures/devices_2.7.xml");
    const DEVICES_1_3: &str = include_str!("../../tests/fixtures/devices_1.3.xml");
    const DEVICES_1_7: &str = include_str!("../../tests/fixtures/devices_1.7.xml");
    const DEVICES_2_0: &str = include_str!("../../tests/fixtures/devices_2.0.xml");
    const CURRENT_2_7: &str = include_str!("../../tests/fixtures/current_2.7.xml");
    const HEARTBEAT_2_7: &str = include_str!("../../tests/fixtures/heartbeat_2.7.xml");
    const ERRORS_2_7: &str = include_str!("../../tests/fixtures/errors_out_of_range_2.7.xml");
    const XXE: &str = include_str!("../../tests/fixtures/xxe_devices.xml");
    const UNKNOWN: &str = include_str!("../../tests/fixtures/unknown_elements_2.7.xml");

    #[test]
    fn the_namespace_version_is_captured_across_1_3_to_2_7() {
        for (xml, want) in [
            (DEVICES_1_3, NsVersion { major: 1, minor: 3 }),
            (DEVICES_1_7, NsVersion { major: 1, minor: 7 }),
            (DEVICES_2_0, NsVersion { major: 2, minor: 0 }),
            (DEVICES_2_7, NsVersion { major: 2, minor: 7 }),
        ] {
            let doc = parse_devices(xml).unwrap();
            assert_eq!(doc.ns_version, Some(want), "version capture");
            assert!(want.is_known());
            assert_eq!(doc.devices.len(), 1, "one Device per fixture");
        }
    }

    #[test]
    fn every_version_yields_the_same_local_name_projection() {
        // The point of local-name matching: four namespaces, one parser, identical structure.
        for xml in [DEVICES_1_3, DEVICES_1_7, DEVICES_2_0, DEVICES_2_7] {
            let doc = parse_devices(xml).unwrap();
            let d = doc.device("OKUMA.123456").expect("the fixture's device");
            assert_eq!(d.attr("name"), Some("OKUMA-CNC"));
            let items = d.child("DataItems").unwrap();
            assert!(
                items
                    .children_named("DataItem")
                    .any(|i| i.attr("id") == Some("avail"))
            );
        }
    }

    #[test]
    fn a_prefixed_document_parses_by_local_name_and_still_declares_its_version() {
        let xml = r#"<m:MTConnectDevices xmlns:m="urn:mtconnect.org:MTConnectDevices:2.7">
              <m:Header instanceId="7" version="2.7.0.12" sender="agent"/>
              <m:Devices><m:Device uuid="U" name="N" id="d"/></m:Devices>
            </m:MTConnectDevices>"#;
        let doc = parse_devices(xml).unwrap();
        assert_eq!(doc.header.instance_id, 7);
        assert_eq!(doc.devices[0].attr("uuid"), Some("U"));
        // A PREFIXED declaration binds exactly as a default one does: the version is captured, so
        // the 1.3 floor cannot be bypassed by qualifying the elements.
        assert_eq!(doc.ns_version, Some(NsVersion { major: 2, minor: 7 }));
    }

    #[test]
    fn a_prefixed_declaration_below_the_floor_is_refused_like_a_default_one() {
        // The C-4 regression: `xmlns:m=` used to leave `ns_version` unset, and the floor check only
        // fires on a version it has — so a 1.1 document walked straight through.
        let old = r#"<m:MTConnectDevices xmlns:m="urn:mtconnect.org:MTConnectDevices:1.1">
              <m:Header instanceId="1"/>
              <m:Devices><m:Device uuid="U" name="N" id="d"/></m:Devices>
            </m:MTConnectDevices>"#;
        assert!(matches!(parse_devices(old), Err(MtcError::UnsupportedVersion(v)) if v == "1.1"));

        // Streams and Errors documents go through the same tokenizer, so they inherit the fix.
        let streams = r#"<m:MTConnectStreams xmlns:m="urn:mtconnect.org:MTConnectStreams:1.2">
              <m:Header instanceId="1"/><m:Streams/>
            </m:MTConnectStreams>"#;
        assert!(matches!(
            parse_streams(streams),
            Err(MtcError::UnsupportedVersion(_))
        ));
    }

    #[test]
    fn the_first_mtconnect_declaration_wins_over_foreign_namespaces() {
        // A real agent declares the schema-instance namespace first. Latching the first `xmlns:*`
        // of ANY vocabulary would read `2001/XMLSchema-instance` as the MTConnect version.
        let xml = r#"<MTConnectDevices xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                xmlns="urn:mtconnect.org:MTConnectDevices:2.7"
                xsi:schemaLocation="urn:mtconnect.org:MTConnectDevices:2.7 MTConnectDevices_2.7.xsd">
              <Header instanceId="1"/><Devices><Device uuid="U" name="N" id="d"/></Devices>
            </MTConnectDevices>"#;
        let doc = parse_devices(xml).unwrap();
        assert_eq!(doc.ns_version, Some(NsVersion { major: 2, minor: 7 }));

        // A document that declares NO MTConnect namespace still parses, with no version — agents in
        // the wild do emit namespace-less documents.
        let bare = r#"<MTConnectDevices xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
              <Header instanceId="1"/><Devices><Device uuid="U" name="N" id="d"/></Devices>
            </MTConnectDevices>"#;
        assert_eq!(parse_devices(bare).unwrap().ns_version, None);
    }

    #[test]
    fn the_node_cap_bounds_the_tree_a_document_of_legal_size_can_build() {
        // C-5: `maxDocumentBytes` bounds the wire, not the DOM. Four bytes per element on the wire
        // becomes an XmlElem in memory, so the byte cap alone lets a document of empty elements
        // amplify 25-50x. The node cap is what makes the tree bounded.
        let flat = format!("<r>{}</r>", "<a/>".repeat(MAX_NODES));
        let err = parse_document(&flat).expect_err("the node cap refuses it");
        assert!(
            matches!(&err, MtcError::Xml(m) if m.contains("element count exceeds")),
            "got {err:?}"
        );

        // Exactly at the cap is fine: the refusal is for exceeding it, not reaching it.
        let at_cap = format!("<r>{}</r>", "<a/>".repeat(MAX_NODES - 1));
        assert_eq!(
            parse_document(&at_cap).unwrap().root.children.len(),
            MAX_NODES - 1
        );

        // A realistic document is orders of magnitude below the ceiling.
        assert!(parse_devices(DEVICES_2_7).is_ok());
        assert!(parse_streams(CURRENT_2_7).is_ok());
    }

    #[test]
    fn the_header_carries_the_sequence_window() {
        let doc = parse_streams(CURRENT_2_7).unwrap();
        assert_eq!(doc.header.instance_id, 1_749_000_000);
        assert_eq!(doc.header.buffer_size, Some(131_072));
        assert_eq!(doc.header.first_sequence, Some(1));
        assert_eq!(doc.header.last_sequence, Some(41));
        assert_eq!(doc.header.next_sequence, Some(42));
        assert_eq!(doc.header.version.as_deref(), Some("2.7.0.12"));
        assert_eq!(doc.header.sender.as_deref(), Some("mtc-agent"));
        assert!(doc.header.creation_time.is_some());
    }

    #[test]
    fn a_streams_document_demultiplexes_by_device_and_tags_each_category() {
        let doc = parse_streams(CURRENT_2_7).unwrap();
        assert_eq!(
            doc.device_streams.len(),
            2,
            "one stream serves many devices"
        );
        let cnc = doc
            .device_streams
            .iter()
            .find(|d| d.uuid == "OKUMA.123456")
            .unwrap();
        assert!(
            cnc.entries.iter().any(
                |e| e.category == Category::Sample && e.elem.attr("dataItemId") == Some("Xabs")
            )
        );
        assert!(
            cnc.entries.iter().any(
                |e| e.category == Category::Event && e.elem.attr("dataItemId") == Some("avail")
            )
        );
        let cond = cnc
            .entries
            .iter()
            .find(|e| e.category == Category::Condition)
            .expect("a Condition entry");
        assert_eq!(
            cond.elem.name, "Fault",
            "the condition STATE is the element name"
        );
        assert_eq!(cond.component.as_deref(), Some("X"));
        assert!(!doc.is_heartbeat());
    }

    #[test]
    fn an_empty_streams_document_is_the_agents_heartbeat() {
        let doc = parse_streams(HEARTBEAT_2_7).unwrap();
        assert!(doc.is_heartbeat(), "liveness without data");
        assert_eq!(doc.header.next_sequence, Some(42));
    }

    #[test]
    fn an_errors_document_surfaces_out_of_range_with_the_buffer_floor() {
        let doc = parse_errors(ERRORS_2_7).unwrap();
        assert_eq!(doc.errors.len(), 1);
        assert_eq!(doc.errors[0].code, "OUT_OF_RANGE");
        assert!(doc.errors[0].message.contains("out of range"));
        assert_eq!(doc.out_of_range(), Some(153), "the agent's firstSequence");

        // A non-range agent error is not a buffer overrun.
        let other = parse_errors(
            r#"<MTConnectError xmlns="urn:mtconnect.org:MTConnectError:2.7">
                 <Header instanceId="1"/>
                 <Errors><Error errorCode="UNAUTHORIZED">nope</Error></Errors>
               </MTConnectError>"#,
        )
        .unwrap();
        assert_eq!(other.errors[0].code, "UNAUTHORIZED");
        assert_eq!(other.out_of_range(), None);
    }

    #[test]
    fn a_1_x_error_document_puts_error_directly_under_the_root() {
        let doc = parse_errors(
            r#"<MTConnectError xmlns="urn:mtconnect.org:MTConnectError:1.3">
                 <Header instanceId="1" firstSequence="9"/>
                 <Error errorCode="OUT_OF_RANGE">'from' is out of range</Error>
               </MTConnectError>"#,
        )
        .unwrap();
        assert_eq!(doc.errors.len(), 1);
        assert_eq!(doc.out_of_range(), Some(9));
    }

    #[test]
    fn an_external_entity_is_never_resolved_and_is_counted() {
        // The XXE fixture declares a SYSTEM entity pointing at a local file and references it in
        // both an attribute and element text. Neither is expanded: the parser has no code path to
        // the filesystem, and the reference is left literal and counted.
        let doc = parse_devices(XXE).unwrap();
        assert!(
            doc.unknown_elements >= 1,
            "the unresolved reference is counted"
        );
        let d = &doc.devices[0];
        let text = format!("{:?}", d);
        assert!(
            !text.contains("root:x:0:0"),
            "no /etc/passwd content anywhere in the tree"
        );
        assert!(!text.contains("BEGIN PRIVATE KEY"));
        assert!(text.contains("&xxe;"), "the reference stays literal");
        // A DOCTYPE is inert, not an error: the document still parses.
        assert_eq!(d.attr("uuid"), Some("XXE.1"));
    }

    #[test]
    fn unknown_elements_are_skipped_and_counted_rather_than_failing_the_parse() {
        let devices = parse_devices(UNKNOWN).unwrap();
        // A vendor extension inside a Device subtree, and a stray element under <Devices>.
        assert_eq!(devices.unknown_elements, 3);
        // The known structure is untouched.
        let d = devices.device("OKUMA.123456").unwrap();
        assert_eq!(d.child("DataItems").unwrap().children.len(), 2);

        // Component *types* are an open set - a vendor component is not "unknown".
        let with_vendor = r#"<MTConnectDevices xmlns="urn:mtconnect.org:MTConnectDevices:2.7">
              <Header instanceId="1"/>
              <Devices><Device uuid="U" name="N" id="d">
                <Components><Vise id="v1"><DataItems>
                  <DataItem id="v-pos" category="SAMPLE" type="POSITION"/>
                </DataItems></Vise></Components>
              </Device></Devices>
            </MTConnectDevices>"#;
        assert_eq!(parse_devices(with_vendor).unwrap().unknown_elements, 0);

        // In a Streams document an unknown GROUP is skipped and counted; the known ones survive.
        let streams = r#"<MTConnectStreams xmlns="urn:mtconnect.org:MTConnectStreams:2.7">
              <Header instanceId="1" nextSequence="2"/>
              <Streams><DeviceStream uuid="U" name="N">
                <ComponentStream component="Device" name="N" componentId="d">
                  <Events><Availability dataItemId="avail" sequence="1"
                     timestamp="2026-07-27T10:00:00Z">AVAILABLE</Availability></Events>
                  <Futures><Whatever dataItemId="w" sequence="2"
                     timestamp="2026-07-27T10:00:00Z">1</Whatever></Futures>
                </ComponentStream>
              </DeviceStream></Streams>
            </MTConnectStreams>"#;
        let s = parse_streams(streams).unwrap();
        assert_eq!(s.unknown_elements, 1, "the unknown group");
        assert_eq!(
            s.device_streams[0].entries.len(),
            1,
            "the known group still delivered"
        );
    }

    #[test]
    fn a_wrong_root_element_is_refused_per_document_type() {
        assert!(matches!(parse_devices(CURRENT_2_7), Err(MtcError::Xml(_))));
        assert!(matches!(parse_streams(DEVICES_2_7), Err(MtcError::Xml(_))));
        assert!(matches!(parse_errors(DEVICES_2_7), Err(MtcError::Xml(_))));
        assert_eq!(document_kind("MTConnectStreams"), DocKind::Streams);
        assert_eq!(document_kind("MTConnectDevices"), DocKind::Devices);
        assert_eq!(document_kind("MTConnectError"), DocKind::Errors);
        assert_eq!(document_kind("MTConnectErrors"), DocKind::Errors);
        assert_eq!(document_kind("html"), DocKind::Unknown("html".into()));
    }

    #[test]
    fn malformed_documents_are_errors_not_panics() {
        assert!(
            parse_document("").is_err(),
            "an empty body is not a document"
        );
        assert!(parse_document("<a><b></a>").is_err(), "mismatched end tag");
        assert!(parse_document("<a>").is_err(), "truncated document");
        assert!(parse_document("not xml at all").is_err());
    }

    #[test]
    fn structural_caps_bound_a_hostile_document() {
        let deep = format!(
            "{}{}",
            "<a>".repeat(MAX_DEPTH + 2),
            "</a>".repeat(MAX_DEPTH + 2)
        );
        assert!(
            matches!(parse_document(&deep), Err(MtcError::Xml(_))),
            "depth cap"
        );

        let attrs: String = (0..MAX_ATTRIBUTES + 5)
            .map(|i| format!(" a{i}=\"1\""))
            .collect();
        assert!(
            parse_document(&format!("<a{attrs}/>")).is_err(),
            "attribute-count cap"
        );

        let long = "x".repeat(MAX_ATTR_VALUE_BYTES + 1);
        assert!(
            parse_document(&format!("<a v=\"{long}\"/>")).is_err(),
            "attribute-length cap"
        );
    }

    #[test]
    fn a_version_below_the_1_3_floor_is_refused() {
        let old = r#"<MTConnectDevices xmlns="urn:mtconnect.org:MTConnectDevices:1.1">
              <Header instanceId="1"/><Devices><Device uuid="U" name="N" id="d"/></Devices>
            </MTConnectDevices>"#;
        assert!(matches!(parse_devices(old), Err(MtcError::UnsupportedVersion(v)) if v == "1.1"));
    }

    #[test]
    fn a_version_above_the_known_window_still_parses_and_is_flagged() {
        // Forward-compatibility is the whole point of local-name matching: a 2.8 agent is read,
        // and the version is reported as one this client was not written against.
        let future = r#"<MTConnectDevices xmlns="urn:mtconnect.org:MTConnectDevices:2.8">
              <Header instanceId="1"/><Devices><Device uuid="U" name="N" id="d"/></Devices>
            </MTConnectDevices>"#;
        let doc = parse_devices(future).unwrap();
        let v = doc.ns_version.unwrap();
        assert_eq!(v.to_string(), "2.8");
        assert!(!v.is_known());
    }

    #[test]
    fn version_parsing_reads_the_urn_tail_only() {
        assert_eq!(
            parse_ns_version("urn:mtconnect.org:MTConnectStreams:1.3"),
            Some(NsVersion { major: 1, minor: 3 })
        );
        assert_eq!(
            parse_ns_version("urn:mtconnect.org:MTConnectStreams:2"),
            Some(NsVersion { major: 2, minor: 0 })
        );
        assert_eq!(parse_ns_version("http://example.com/ns"), None);
        assert_eq!(parse_ns_version("urn:x:y:not-a-version"), None);
        // Only an MTConnect URN carries an MTConnect version: an arbitrary colon-bearing URI whose
        // tail happens to look numeric is not one.
        assert_eq!(parse_ns_version("urn:example:thing:2.7"), None);
        assert_eq!(
            parse_ns_version("http://www.w3.org/2001/XMLSchema-instance"),
            None
        );
    }

    #[test]
    fn predefined_entities_and_character_references_are_expanded() {
        let xml = r#"<MTConnectStreams xmlns="urn:mtconnect.org:MTConnectStreams:2.7">
              <Header instanceId="1"/>
              <Streams><DeviceStream uuid="U" name="N">
                <ComponentStream component="Controller" name="C" componentId="c">
                  <Events><Message dataItemId="msg" sequence="1" timestamp="2026-07-27T10:00:00Z"
                    nativeCode="A&amp;B">tool &lt;5&gt; &#65;</Message></Events>
                </ComponentStream>
              </DeviceStream></Streams>
            </MTConnectStreams>"#;
        let doc = parse_streams(xml).unwrap();
        let e = &doc.device_streams[0].entries[0].elem;
        assert_eq!(e.text, "tool <5> A");
        assert_eq!(e.attr("nativeCode"), Some("A&B"));
    }

    #[test]
    fn cdata_and_split_text_become_one_trimmed_string() {
        let xml = r#"<MTConnectDevices xmlns="urn:mtconnect.org:MTConnectDevices:2.7">
              <Header instanceId="1"/>
              <Devices><Device uuid="U" name="N" id="d">
                <Description model="M"><![CDATA[a & b]]></Description>
              </Device></Devices>
            </MTConnectDevices>"#;
        let doc = parse_devices(xml).unwrap();
        assert_eq!(doc.devices[0].child("Description").unwrap().text, "a & b");
    }

    #[test]
    fn the_element_helpers_navigate_by_local_name() {
        let doc = parse_devices(DEVICES_2_7).unwrap();
        let d = &doc.devices[0];
        assert!(d.child("DataItems").is_some());
        assert!(d.child("Nope").is_none());
        assert_eq!(d.children_named("Components").count(), 1);
        assert_eq!(d.attr("uuid"), Some("OKUMA.123456"));
        assert_eq!(d.attr("nope"), None);
    }
}
