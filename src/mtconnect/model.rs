//! # The probe projection: device model, browse tree, and content digest (LLD §6 / §7)
//!
//! A `/probe` answer is a whole agent's device catalog. [`ProbeModel::from_devices`] projects **one**
//! device out of it into three things the rest of the adapter needs:
//!
//! 1. `items` — every `DataItem` by id, with the metadata a published `signal.address` carries
//!    (category, type, subType, units, component path). This is what a configured signal compiles
//!    against, and a missing id is a *named* failure rather than a silently dropped signal.
//! 2. `tree` — a pre-ordered [`BrowseNode`] projection: Device, its own data items, then each
//!    component subtree. `sb/browse` pages this straight out of the cache, so the address space is
//!    browsable while the agent is unreachable.
//! 3. `raw_digest` — a sha256 over the canonicalized `Device` subtree. It is the browse cursor's
//!    `viewGeneration` and the drift detector: the same model always digests the same, a changed
//!    model never digests the same, and neither depends on how the agent formatted its XML.

use std::collections::HashMap;

use sha2::{Digest, Sha256};

use super::error::MtcError;
use super::xml::{DevicesDoc, NsVersion, XmlElem};

/// An MTConnect observation category.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Category {
    Sample,
    Event,
    Condition,
}

impl Category {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sample => "SAMPLE",
            Self::Event => "EVENT",
            Self::Condition => "CONDITION",
        }
    }

    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_uppercase().as_str() {
            "SAMPLE" => Some(Self::Sample),
            "EVENT" => Some(Self::Event),
            "CONDITION" => Some(Self::Condition),
            _ => None,
        }
    }
}

/// A `DataItem/@representation`. `Value` is the default (a scalar); the rest change the *shape* of
/// the published value, which is why the decoder needs them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Repr {
    #[default]
    Value,
    TimeSeries,
    DataSet,
    Table,
    Discrete,
}

impl Repr {
    #[must_use]
    pub fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_uppercase().as_str() {
            "TIME_SERIES" => Self::TimeSeries,
            "DATA_SET" => Self::DataSet,
            "TABLE" => Self::Table,
            "DISCRETE" => Self::Discrete,
            _ => Self::Value,
        }
    }

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Value => "VALUE",
            Self::TimeSeries => "TIME_SERIES",
            Self::DataSet => "DATA_SET",
            Self::Table => "TABLE",
            Self::Discrete => "DISCRETE",
        }
    }
}

/// Everything a published `signal.address` and the observation decoder need about one data item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataItemMeta {
    pub id: String,
    pub name: Option<String>,
    pub category: Category,
    pub type_: String,
    pub sub_type: Option<String>,
    pub units: Option<String>,
    pub native_units: Option<String>,
    /// `Axes/Linear[X]` — the component chain holding this item, empty for a device-level item.
    pub component_path: String,
    pub representation: Repr,
}

/// The device itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceNode {
    pub uuid: String,
    pub name: Option<String>,
    pub id: Option<String>,
    /// The device's declared `mtconnectVersion`, else the document's namespace version.
    pub mtconnect_version: Option<String>,
}

/// What a [`BrowseNode`] is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeKind {
    Device,
    Component,
    DataItem,
}

impl NodeKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Device => "DEVICE",
            Self::Component => "COMPONENT",
            Self::DataItem => "DATA_ITEM",
        }
    }
}

/// One entry of the browse projection. Ids are stable and round-trippable:
/// `mtc:/component/<path>` for the device and its components, `mtc:/item/<dataItemId>` for data
/// items — the same `dataItemId` a signal binds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrowseNode {
    pub id: String,
    pub parent_id: Option<String>,
    pub kind: NodeKind,
    pub name: Option<String>,
    /// The MTConnect element type (`Linear`, `POSITION`, …).
    pub type_name: String,
    pub sub_type: Option<String>,
    pub category: Option<Category>,
    pub units: Option<String>,
    pub depth: usize,
}

/// The immutable projection of one device's probe, shared by `Arc` and swapped wholesale when the
/// model changes (never patched in place — a half-updated model is worse than a stale one).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeModel {
    /// sha256 over the canonicalized `Device` subtree.
    pub raw_digest: [u8; 32],
    pub device: DeviceNode,
    pub items: HashMap<String, DataItemMeta>,
    pub tree: Vec<BrowseNode>,
    /// The namespace version of the probe document this was built from.
    pub ns_version: Option<NsVersion>,
}

/// The browse id of the device root (an empty component path).
pub const ROOT_NODE_ID: &str = "mtc:/component/";

impl ProbeModel {
    /// Project the device with this uuid out of a parsed `/probe` document.
    ///
    /// # Errors
    /// [`MtcError::NoSuchDevice`] when the probe has no device with that uuid.
    pub fn from_devices(doc: &DevicesDoc, device_uuid: &str) -> Result<Self, MtcError> {
        let device_elem = doc
            .device(device_uuid)
            .ok_or_else(|| MtcError::NoSuchDevice(device_uuid.to_string()))?;
        Ok(Self::from_device_element(device_elem, doc.ns_version))
    }

    /// Project one already-selected `<Device>` element.
    #[must_use]
    pub fn from_device_element(device_elem: &XmlElem, ns_version: Option<NsVersion>) -> Self {
        let device = DeviceNode {
            uuid: device_elem.attr("uuid").unwrap_or_default().to_string(),
            name: device_elem.attr("name").map(str::to_string),
            id: device_elem.attr("id").map(str::to_string),
            mtconnect_version: device_elem
                .attr("mtconnectVersion")
                .map(str::to_string)
                .or_else(|| ns_version.map(|v| v.to_string())),
        };

        let mut items = HashMap::new();
        let mut tree = Vec::new();
        tree.push(BrowseNode {
            id: ROOT_NODE_ID.to_string(),
            parent_id: None,
            kind: NodeKind::Device,
            name: device.name.clone(),
            type_name: "Device".to_string(),
            sub_type: None,
            category: None,
            units: None,
            depth: 0,
        });
        walk(device_elem, "", ROOT_NODE_ID, 1, &mut items, &mut tree);

        Self {
            raw_digest: digest_of(device_elem),
            device,
            items,
            tree,
            ns_version,
        }
    }

    /// The digest as the `sha256:<hex>` string `sb/status` and the browse `viewGeneration` publish.
    #[must_use]
    pub fn digest_hex(&self) -> String {
        let mut s = String::with_capacity(7 + 64);
        s.push_str("sha256:");
        for b in self.raw_digest {
            s.push_str(&format!("{b:02x}"));
        }
        s
    }

    /// The metadata for one data item id.
    #[must_use]
    pub fn item(&self, data_item_id: &str) -> Option<&DataItemMeta> {
        self.items.get(data_item_id)
    }

    /// The `signal.address` object for a data item (HLD §5.3) — round-trippable, non-secret, and
    /// stable across reconnects. `agent_id` and the device uuid come from configuration, so they are
    /// passed in rather than guessed.
    #[must_use]
    pub fn address_of(&self, agent_id: &str, data_item_id: &str) -> Option<serde_json::Value> {
        let m = self.item(data_item_id)?;
        Some(serde_json::json!({
            "protocol": "mtconnect",
            "agentId": agent_id,
            "deviceUuid": self.device.uuid,
            "dataItemId": m.id,
            "category": m.category.as_str(),
            "type": m.type_,
            "subType": m.sub_type,
            "componentPath": m.component_path,
        }))
    }
}

/// Walk a component subtree in pre-order: this element's own data items first, then each child
/// component (depth-first, document order).
fn walk(
    elem: &XmlElem,
    path: &str,
    parent_id: &str,
    depth: usize,
    items: &mut HashMap<String, DataItemMeta>,
    tree: &mut Vec<BrowseNode>,
) {
    if let Some(data_items) = elem.child("DataItems") {
        for di in data_items.children_named("DataItem") {
            let Some(id) = di.attr("id") else { continue };
            let Some(category) = di.attr("category").and_then(Category::parse) else { continue };
            let meta = DataItemMeta {
                id: id.to_string(),
                name: di.attr("name").map(str::to_string),
                category,
                type_: di.attr("type").unwrap_or_default().to_string(),
                sub_type: di.attr("subType").map(str::to_string),
                units: di.attr("units").map(str::to_string),
                native_units: di.attr("nativeUnits").map(str::to_string),
                component_path: path.to_string(),
                representation: di.attr("representation").map(Repr::parse).unwrap_or_default(),
            };
            tree.push(BrowseNode {
                id: format!("mtc:/item/{id}"),
                parent_id: Some(parent_id.to_string()),
                kind: NodeKind::DataItem,
                name: meta.name.clone(),
                type_name: meta.type_.clone(),
                sub_type: meta.sub_type.clone(),
                category: Some(category),
                units: meta.units.clone(),
                depth,
            });
            items.insert(id.to_string(), meta);
        }
    }

    if let Some(components) = elem.child("Components") {
        for c in &components.children {
            let segment = match c.attr("name") {
                Some(name) => format!("{}[{}]", c.name, name),
                None => c.name.clone(),
            };
            let child_path =
                if path.is_empty() { segment } else { format!("{path}/{segment}") };
            let node_id = format!("mtc:/component/{child_path}");
            tree.push(BrowseNode {
                id: node_id.clone(),
                parent_id: Some(parent_id.to_string()),
                kind: NodeKind::Component,
                name: c.attr("name").map(str::to_string),
                type_name: c.name.clone(),
                sub_type: None,
                category: None,
                units: None,
                depth,
            });
            walk(c, &child_path, &node_id, depth + 1, items, tree);
        }
    }
}

/// The canonical serialization the digest is taken over: pre-order (element local name, then its
/// attributes in sorted order), for the `Device` subtree **only**. Whitespace, attribute order and
/// namespace prefixes cannot change it; a renamed element, a changed attribute, or an added data
/// item always does.
fn canonicalize(e: &XmlElem, out: &mut Vec<u8>) {
    out.extend_from_slice(e.name.as_bytes());
    out.push(0x1f);
    for (k, v) in &e.attrs {
        // Namespace declarations and schema hints are document plumbing, not model content.
        if k == "xmlns" || k.starts_with("xmlns:") || k.ends_with(":schemaLocation") {
            continue;
        }
        out.extend_from_slice(k.as_bytes());
        out.push(b'=');
        out.extend_from_slice(v.as_bytes());
        out.push(0x1f);
    }
    out.push(0x1e);
    for c in &e.children {
        canonicalize(c, out);
    }
    out.push(0x1d);
}

fn digest_of(device_elem: &XmlElem) -> [u8; 32] {
    let mut buf = Vec::new();
    canonicalize(device_elem, &mut buf);
    let mut hasher = Sha256::new();
    hasher.update(&buf);
    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mtconnect::xml::parse_devices;

    const DEVICES_2_7: &str = include_str!("../../tests/fixtures/devices_2.7.xml");
    const DEVICES_1_7: &str = include_str!("../../tests/fixtures/devices_1.7.xml");

    fn model() -> ProbeModel {
        let doc = parse_devices(DEVICES_2_7).unwrap();
        ProbeModel::from_devices(&doc, "OKUMA.123456").unwrap()
    }

    #[test]
    fn a_probe_projects_the_named_device_only() {
        let m = model();
        assert_eq!(m.device.uuid, "OKUMA.123456");
        assert_eq!(m.device.name.as_deref(), Some("OKUMA-CNC"));
        assert_eq!(m.device.id.as_deref(), Some("okuma"));
        assert_eq!(m.device.mtconnect_version.as_deref(), Some("2.7"));
        assert_eq!(m.ns_version, Some(NsVersion { major: 2, minor: 7 }));

        let doc = parse_devices(DEVICES_2_7).unwrap();
        assert!(matches!(
            ProbeModel::from_devices(&doc, "NOT.THERE"),
            Err(MtcError::NoSuchDevice(u)) if u == "NOT.THERE"
        ));
    }

    #[test]
    fn a_device_without_its_own_version_falls_back_to_the_namespace() {
        let doc = parse_devices(
            r#"<MTConnectDevices xmlns="urn:mtconnect.org:MTConnectDevices:1.7">
                 <Header instanceId="1"/>
                 <Devices><Device uuid="U" name="N" id="d"/></Devices>
               </MTConnectDevices>"#,
        )
        .unwrap();
        let m = ProbeModel::from_devices(&doc, "U").unwrap();
        assert_eq!(m.device.mtconnect_version.as_deref(), Some("1.7"));
        // No data items, but the device root is always a browse node.
        assert!(m.items.is_empty());
        assert_eq!(m.tree.len(), 1);
        assert_eq!(m.tree[0].id, ROOT_NODE_ID);
    }

    #[test]
    fn every_data_item_carries_the_metadata_a_signal_address_publishes() {
        let m = model();
        let x = m.item("Xabs").expect("Xabs");
        assert_eq!(x.category, Category::Sample);
        assert_eq!(x.type_, "POSITION");
        assert_eq!(x.sub_type.as_deref(), Some("ACTUAL"));
        assert_eq!(x.units.as_deref(), Some("MILLIMETER"));
        assert_eq!(x.native_units.as_deref(), Some("MILLIMETER"));
        assert_eq!(x.component_path, "Axes/Linear[X]", "the HLD §5.3 component path");
        assert_eq!(x.representation, Repr::Value);
        assert_eq!(x.name.as_deref(), Some("Xabs"));

        // A device-level item has an empty component path.
        assert_eq!(m.item("avail").unwrap().component_path, "");
        // Representations are read, not assumed.
        assert_eq!(m.item("Xfreq").unwrap().representation, Repr::TimeSeries);
        assert_eq!(m.item("tool-offsets").unwrap().representation, Repr::DataSet);
        assert_eq!(m.item("Xtravel").unwrap().category, Category::Condition);
        assert_eq!(m.item("execution").unwrap().category, Category::Event);
        assert!(m.item("nope").is_none());
    }

    #[test]
    fn the_published_address_round_trips_the_binding() {
        let m = model();
        let addr = m.address_of("line-a-agent", "Xabs").unwrap();
        assert_eq!(addr["protocol"], "mtconnect");
        assert_eq!(addr["agentId"], "line-a-agent");
        assert_eq!(addr["deviceUuid"], "OKUMA.123456");
        assert_eq!(addr["dataItemId"], "Xabs");
        assert_eq!(addr["category"], "SAMPLE");
        assert_eq!(addr["type"], "POSITION");
        assert_eq!(addr["subType"], "ACTUAL");
        assert_eq!(addr["componentPath"], "Axes/Linear[X]");
        assert!(m.address_of("line-a-agent", "nope").is_none());
    }

    #[test]
    fn the_browse_tree_is_pre_ordered_device_then_items_then_components() {
        let m = model();
        let ids: Vec<&str> = m.tree.iter().map(|n| n.id.as_str()).collect();
        assert_eq!(
            ids,
            vec![
                "mtc:/component/",
                "mtc:/item/avail",
                "mtc:/item/asset-changed",
                "mtc:/component/Axes",
                "mtc:/component/Axes/Linear[X]",
                "mtc:/item/Xabs",
                "mtc:/item/Xload",
                "mtc:/item/Xfreq",
                "mtc:/item/Xtravel",
                "mtc:/component/Axes/Rotary[C]",
                "mtc:/item/Sspeed",
                "mtc:/component/Controller",
                "mtc:/item/estop",
                "mtc:/component/Controller/Path[P1]",
                "mtc:/item/execution",
                "mtc:/item/program",
                "mtc:/item/part-count",
                "mtc:/item/tool-offsets",
                "mtc:/item/Ppos",
                "mtc:/item/logic",
            ]
        );

        // Parentage and depth make the hierarchical browse mode possible without re-walking.
        let x_axis = m.tree.iter().find(|n| n.id == "mtc:/component/Axes/Linear[X]").unwrap();
        assert_eq!(x_axis.parent_id.as_deref(), Some("mtc:/component/Axes"));
        assert_eq!(x_axis.kind, NodeKind::Component);
        assert_eq!(x_axis.type_name, "Linear");
        assert_eq!(x_axis.name.as_deref(), Some("X"));
        assert_eq!(x_axis.depth, 2);

        let xabs = m.tree.iter().find(|n| n.id == "mtc:/item/Xabs").unwrap();
        assert_eq!(xabs.parent_id.as_deref(), Some("mtc:/component/Axes/Linear[X]"));
        assert_eq!(xabs.kind, NodeKind::DataItem);
        assert_eq!(xabs.category, Some(Category::Sample));
        assert_eq!(xabs.units.as_deref(), Some("MILLIMETER"));
        assert_eq!(xabs.depth, 3);
        assert_eq!(m.tree[0].kind, NodeKind::Device);
        assert_eq!(m.tree[0].depth, 0);
    }

    #[test]
    fn the_digest_is_deterministic_and_independent_of_formatting() {
        let a = model().raw_digest;
        let b = model().raw_digest;
        assert_eq!(a, b, "same input, same digest");

        // Reformatting the document — whitespace, attribute order, a namespace prefix — must not
        // change the model, so it must not change the digest.
        let reordered = DEVICES_2_7
            .replace(
                r#"<DataItem category="SAMPLE" id="Xabs" name="Xabs" nativeUnits="MILLIMETER" subType="ACTUAL" type="POSITION" units="MILLIMETER"/>"#,
                "<DataItem type=\"POSITION\"   units=\"MILLIMETER\"\n  subType=\"ACTUAL\" nativeUnits=\"MILLIMETER\" name=\"Xabs\" id=\"Xabs\" category=\"SAMPLE\"/>",
            );
        let doc = parse_devices(&reordered).unwrap();
        let m = ProbeModel::from_devices(&doc, "OKUMA.123456").unwrap();
        assert_eq!(m.raw_digest, a, "attribute order and whitespace are not model content");

        // The digest covers the Device subtree ONLY: a different agent header, or another device
        // appearing in the same probe, must not move it.
        let other_header = DEVICES_2_7.replace("instanceId=\"1749000000\"", "instanceId=\"42\"");
        let doc = parse_devices(&other_header).unwrap();
        assert_eq!(ProbeModel::from_devices(&doc, "OKUMA.123456").unwrap().raw_digest, a);
    }

    #[test]
    fn a_changed_model_always_changes_the_digest() {
        let base = model().raw_digest;
        for changed in [
            // an added data item
            DEVICES_2_7.replace(
                r#"<DataItem category="EVENT" id="avail" name="avail" type="AVAILABILITY"/>"#,
                r#"<DataItem category="EVENT" id="avail" name="avail" type="AVAILABILITY"/><DataItem category="EVENT" id="extra" type="BLOCK"/>"#,
            ),
            // a changed attribute
            DEVICES_2_7.replace(r#"units="MILLIMETER""#, r#"units="INCH""#),
            // a renamed component
            DEVICES_2_7.replace(r#"<Rotary id="c-axis" name="C">"#, r#"<Rotary id="c-axis" name="C2">"#),
            // a removed data item
            DEVICES_2_7.replace(
                r#"<DataItem category="SAMPLE" id="Xload" name="Xload" type="LOAD" units="PERCENT"/>"#,
                "",
            ),
        ] {
            let doc = parse_devices(&changed).unwrap();
            let m = ProbeModel::from_devices(&doc, "OKUMA.123456").unwrap();
            assert_ne!(m.raw_digest, base, "a model change must move the digest");
        }
    }

    #[test]
    fn the_digest_renders_as_the_sha256_prefixed_hex_status_and_cursors_publish() {
        let hex = model().digest_hex();
        assert!(hex.starts_with("sha256:"));
        assert_eq!(hex.len(), 7 + 64);
        assert!(hex[7..].chars().all(|c| c.is_ascii_hexdigit()));
        // Two probes of the same device from different schema versions are NOT the same model
        // (their elements differ), and the digest says so rather than pretending otherwise.
        let doc = parse_devices(DEVICES_1_7).unwrap();
        let older = ProbeModel::from_devices(&doc, "OKUMA.123456").unwrap();
        assert_ne!(older.digest_hex(), hex);
    }

    #[test]
    fn a_data_item_without_an_id_or_category_is_skipped_rather_than_guessed_at() {
        let doc = parse_devices(
            r#"<MTConnectDevices xmlns="urn:mtconnect.org:MTConnectDevices:2.7">
                 <Header instanceId="1"/>
                 <Devices><Device uuid="U" name="N" id="d"><DataItems>
                   <DataItem type="AVAILABILITY" category="EVENT"/>
                   <DataItem id="no-category" type="AVAILABILITY"/>
                   <DataItem id="ok" type="AVAILABILITY" category="EVENT"/>
                 </DataItems></Device></Devices>
               </MTConnectDevices>"#,
        )
        .unwrap();
        let m = ProbeModel::from_devices(&doc, "U").unwrap();
        assert_eq!(m.items.len(), 1);
        assert!(m.item("ok").is_some());
    }

    #[test]
    fn categories_and_representations_parse_case_insensitively_and_default_safely() {
        assert_eq!(Category::parse("sample"), Some(Category::Sample));
        assert_eq!(Category::parse(" EVENT "), Some(Category::Event));
        assert_eq!(Category::parse("Condition"), Some(Category::Condition));
        assert_eq!(Category::parse("nope"), None);
        assert_eq!(Category::Sample.as_str(), "SAMPLE");
        assert_eq!(Category::Event.as_str(), "EVENT");
        assert_eq!(Category::Condition.as_str(), "CONDITION");

        assert_eq!(Repr::parse("TIME_SERIES"), Repr::TimeSeries);
        assert_eq!(Repr::parse("data_set"), Repr::DataSet);
        assert_eq!(Repr::parse("TABLE"), Repr::Table);
        assert_eq!(Repr::parse("DISCRETE"), Repr::Discrete);
        assert_eq!(Repr::parse("whatever-2.8-adds"), Repr::Value, "unknown means scalar");
        assert_eq!(Repr::TimeSeries.as_str(), "TIME_SERIES");
        assert_eq!(Repr::Value.as_str(), "VALUE");
        assert_eq!(Repr::DataSet.as_str(), "DATA_SET");
        assert_eq!(Repr::Table.as_str(), "TABLE");
        assert_eq!(Repr::Discrete.as_str(), "DISCRETE");
        assert_eq!(NodeKind::Device.as_str(), "DEVICE");
        assert_eq!(NodeKind::Component.as_str(), "COMPONENT");
        assert_eq!(NodeKind::DataItem.as_str(), "DATA_ITEM");
    }
}
