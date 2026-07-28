//! # Observations: decoding one streamed value (LLD §2, HLD §5.3)
//!
//! An MTConnect observation is an element whose *name* is the observation type (`<Position>`,
//! `<Availability>`, `<Fault>`), whose attributes carry the identity (`dataItemId`, `sequence`,
//! `timestamp`) and whose text is the value. This module turns one such element into an
//! [`Observation`] — protocol-shaped, with no EdgeCommons vocabulary anywhere: the mapping onto a
//! `Reading`/`Sample` happens above the seam, in `device.rs`.
//!
//! Three value shapes exist, and the difference matters to a consumer:
//!
//! * a **scalar** (or an array, for `TIME_SERIES` and the `_3D` units) — an ordinary value;
//! * **`UNAVAILABLE`** — the agent has no value; published as a BAD null, never as `0` or `""`;
//! * a **condition state** — `Normal`/`Warning`/`Fault`/`Unavailable` is *itself* the value, with
//!   the native code and text riding as extras.

use serde_json::Value;
use smallvec::SmallVec;

use super::model::{Category, DataItemMeta, Repr};
use super::xml::{StreamEntry, XmlElem};

/// The sentinel text an agent sends when it has no value for a data item.
pub const UNAVAILABLE: &str = "UNAVAILABLE";

/// EVENT-category types whose value is a number even though the data item declares no units.
const NUMERIC_EVENT_TYPES: &[&str] = &[
    "PART_COUNT",
    "LINE",
    "LINE_NUMBER",
    "TOOL_NUMBER",
    "SEQUENCE_NUMBER",
    "PATH_FEEDRATE_OVERRIDE",
    "ROTARY_VELOCITY_OVERRIDE",
];

/// The state of a CONDITION-category data item.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CondState {
    Normal,
    Warning,
    Fault,
    Unavailable,
}

impl CondState {
    /// The condition state is the observation element's own name.
    #[must_use]
    pub fn parse(element: &str) -> Option<Self> {
        match element.trim().to_ascii_uppercase().as_str() {
            "NORMAL" => Some(Self::Normal),
            "WARNING" => Some(Self::Warning),
            "FAULT" => Some(Self::Fault),
            "UNAVAILABLE" => Some(Self::Unavailable),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "NORMAL",
            Self::Warning => "WARNING",
            Self::Fault => "FAULT",
            Self::Unavailable => "UNAVAILABLE",
        }
    }

    /// How bad this state is, so a signal bound to several conditions can take the worst.
    #[must_use]
    pub fn severity(self) -> u8 {
        match self {
            Self::Normal => 0,
            Self::Warning => 1,
            Self::Unavailable => 2,
            Self::Fault => 3,
        }
    }
}

/// One observation's value.
#[derive(Debug, Clone, PartialEq)]
pub enum ObsValue {
    /// An ordinary JSON-native value: number, string, boolean, array (`TIME_SERIES`/`_3D`) or
    /// object (`DATA_SET`/`TABLE`).
    Scalar(Value),
    /// The agent reported `UNAVAILABLE` — there is no value.
    Unavailable,
    /// A condition state, which IS the value for a CONDITION-category data item.
    Condition(CondState),
}

/// One parsed observation from a Streams document.
#[derive(Debug, Clone, PartialEq)]
pub struct Observation {
    pub data_item_id: String,
    /// The agent's monotonic sequence number — the once-only ordering key across reconnects.
    pub sequence: u64,
    /// The agent's capture timestamp, verbatim (RFC3339). Published as `serverTs`.
    pub timestamp: String,
    pub value: ObsValue,
    /// Additive protocol fields, in a stable order: `resetTriggered`, `duration`, `nativeCode`, …
    pub extras: SmallVec<[(&'static str, Value); 4]>,
    /// The observation element's local name (`Position`, `Fault`, …).
    pub element: String,
    /// The data item's `name` attribute, when the agent sent one.
    pub name: Option<String>,
    pub sub_type: Option<String>,
    pub category: Category,
}

impl Observation {
    /// The value of one extra, for callers that need a single field.
    #[must_use]
    pub fn extra(&self, key: &str) -> Option<&Value> {
        self.extras.iter().find(|(k, _)| *k == key).map(|(_, v)| v)
    }

    /// Whether the agent reported no value (either an `UNAVAILABLE` text or an unavailable
    /// condition state).
    #[must_use]
    pub fn is_unavailable(&self) -> bool {
        matches!(
            self.value,
            ObsValue::Unavailable | ObsValue::Condition(CondState::Unavailable)
        )
    }

    /// The condition state, when this observation is one.
    #[must_use]
    pub fn condition_state(&self) -> Option<CondState> {
        match self.value {
            ObsValue::Condition(s) => Some(s),
            _ => None,
        }
    }
}

/// Decode one stream entry against the device model.
///
/// `meta` is the data item's probe metadata when the model knows it; an observation for a data item
/// the model does not carry still decodes (a stream may run ahead of a re-probe) — it is simply
/// decoded conservatively, as text.
///
/// Returns `None` only when the element carries no `dataItemId`, which is the one thing that makes
/// an observation unusable.
#[must_use]
pub fn decode(entry: &StreamEntry, meta: Option<&DataItemMeta>) -> Option<Observation> {
    let elem = &entry.elem;
    let data_item_id = elem.attr("dataItemId")?.to_string();
    let sequence = elem.attr("sequence").and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
    let timestamp = elem.attr("timestamp").unwrap_or_default().to_string();
    let repr = meta.map_or_else(
        || elem.attr("representation").map(Repr::parse).unwrap_or_default(),
        |m| m.representation,
    );

    let mut extras: SmallVec<[(&'static str, Value); 4]> = SmallVec::new();
    let value = match entry.category {
        Category::Condition => {
            let state = CondState::parse(&elem.name).unwrap_or(CondState::Unavailable);
            for (attr, key) in [
                ("nativeCode", "nativeCode"),
                ("nativeSeverity", "nativeSeverity"),
                ("qualifier", "qualifier"),
            ] {
                if let Some(v) = elem.attr(attr) {
                    extras.push((key, Value::String(v.to_string())));
                }
            }
            if !elem.text.is_empty() {
                extras.push(("conditionText", Value::String(elem.text.clone())));
            }
            ObsValue::Condition(state)
        }
        category => {
            for (attr, key) in [
                ("resetTriggered", "resetTriggered"),
                ("duration", "duration"),
                ("nativeCode", "nativeCode"),
                ("sampleRate", "sampleRate"),
                ("statistic", "statistic"),
            ] {
                if let Some(v) = elem.attr(attr) {
                    extras.push((key, numeric_or_string(v)));
                }
            }
            decode_value(elem, category, repr, meta)
        }
    };

    Some(Observation {
        data_item_id,
        sequence,
        timestamp,
        value,
        extras,
        element: elem.name.clone(),
        name: elem.attr("name").map(str::to_string),
        sub_type: elem
            .attr("subType")
            .map(str::to_string)
            .or_else(|| meta.and_then(|m| m.sub_type.clone())),
        category: meta.map_or(entry.category, |m| m.category),
    })
}

fn decode_value(
    elem: &XmlElem,
    category: Category,
    repr: Repr,
    meta: Option<&DataItemMeta>,
) -> ObsValue {
    // DATA_SET / TABLE carry their value in <Entry> children, so their emptiness is structural.
    if matches!(repr, Repr::DataSet | Repr::Table) {
        if elem.text.trim().eq_ignore_ascii_case(UNAVAILABLE) {
            return ObsValue::Unavailable;
        }
        return ObsValue::Scalar(decode_entries(elem, repr));
    }

    let text = elem.text.trim();
    if text.eq_ignore_ascii_case(UNAVAILABLE) {
        return ObsValue::Unavailable;
    }
    if text.is_empty() {
        return ObsValue::Scalar(Value::String(String::new()));
    }

    match category {
        // A sample is numeric by definition: one number, or a vector (TIME_SERIES, `*_3D` units).
        Category::Sample => match numeric_vector(text) {
            Some(v) => ObsValue::Scalar(v),
            None => ObsValue::Scalar(Value::String(text.to_string())),
        },
        // An event is a string/enum verbatim — unless the model says this type is numeric.
        Category::Event => {
            let numeric_type = meta.is_some_and(|m| {
                m.units.is_some() || NUMERIC_EVENT_TYPES.contains(&m.type_.as_str())
            });
            match (numeric_type, parse_number(text)) {
                (true, Some(n)) => ObsValue::Scalar(n),
                _ => ObsValue::Scalar(Value::String(text.to_string())),
            }
        }
        Category::Condition => ObsValue::Scalar(Value::String(text.to_string())),
    }
}

/// `<Entry key="T1">12.5</Entry>` children → a JSON object. A `removed="true"` entry becomes an
/// explicit `null`, so a consumer can tell "gone" from "never present". `TABLE` entries carry
/// `<Cell key=…>` children and nest one level deeper.
fn decode_entries(elem: &XmlElem, repr: Repr) -> Value {
    let mut out = serde_json::Map::new();
    for entry in elem.children_named("Entry") {
        let Some(key) = entry.attr("key") else { continue };
        if entry.attr("removed").is_some_and(|v| v.eq_ignore_ascii_case("true")) {
            out.insert(key.to_string(), Value::Null);
            continue;
        }
        let value = if repr == Repr::Table {
            let mut cells = serde_json::Map::new();
            for cell in entry.children_named("Cell") {
                if let Some(ck) = cell.attr("key") {
                    cells.insert(ck.to_string(), numeric_or_string(&cell.text));
                }
            }
            Value::Object(cells)
        } else {
            numeric_or_string(&entry.text)
        };
        out.insert(key.to_string(), value);
    }
    Value::Object(out)
}

/// One number, or a whitespace-separated vector of numbers (`TIME_SERIES`, `MILLIMETER_3D`).
/// `None` when any token is not numeric — a sample whose text is not a number stays a string
/// rather than being coerced.
fn numeric_vector(text: &str) -> Option<Value> {
    let mut tokens = text.split_whitespace();
    let first = tokens.next()?;
    let first = parse_number(first)?;
    let mut rest: Vec<Value> = Vec::new();
    for t in tokens {
        rest.push(parse_number(t)?);
    }
    if rest.is_empty() {
        Some(first)
    } else {
        let mut all = Vec::with_capacity(rest.len() + 1);
        all.push(first);
        all.extend(rest);
        Some(Value::Array(all))
    }
}

fn parse_number(text: &str) -> Option<Value> {
    if let Ok(i) = text.parse::<i64>() {
        return Some(Value::from(i));
    }
    let f = text.parse::<f64>().ok()?;
    if f.is_finite() {
        serde_json::Number::from_f64(f).map(Value::Number)
    } else {
        None
    }
}

fn numeric_or_string(text: &str) -> Value {
    let t = text.trim();
    parse_number(t).unwrap_or_else(|| Value::String(t.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mtconnect::model::ProbeModel;
    use crate::mtconnect::xml::{parse_devices, parse_streams, DeviceStream};
    use serde_json::json;

    const DEVICES_2_7: &str = include_str!("../../tests/fixtures/devices_2.7.xml");
    const CURRENT_2_7: &str = include_str!("../../tests/fixtures/current_2.7.xml");

    fn model() -> ProbeModel {
        ProbeModel::from_devices(&parse_devices(DEVICES_2_7).unwrap(), "OKUMA.123456").unwrap()
    }

    fn cnc_stream() -> DeviceStream {
        parse_streams(CURRENT_2_7)
            .unwrap()
            .device_streams
            .into_iter()
            .find(|d| d.uuid == "OKUMA.123456")
            .unwrap()
    }

    fn decoded() -> Vec<Observation> {
        let m = model();
        cnc_stream()
            .entries
            .iter()
            .filter_map(|e| decode(e, m.item(e.elem.attr("dataItemId").unwrap_or_default())))
            .collect()
    }

    fn by_id(obs: &[Observation], id: &str) -> Observation {
        obs.iter().find(|o| o.data_item_id == id).expect("observation").clone()
    }

    #[test]
    fn identity_sequence_and_timestamp_come_off_the_observation_itself() {
        let obs = decoded();
        let x = by_id(&obs, "Xabs");
        assert_eq!(x.sequence, 37, "the once-only ordering key");
        assert_eq!(x.timestamp, "2026-07-27T10:00:04.250000Z", "the agent's capture stamp");
        assert_eq!(x.element, "Position");
        assert_eq!(x.name.as_deref(), Some("Xabs"));
        assert_eq!(x.sub_type.as_deref(), Some("ACTUAL"));
        assert_eq!(x.category, Category::Sample);
    }

    #[test]
    fn a_sample_decodes_as_a_number_and_a_vector_sample_as_an_array() {
        let obs = decoded();
        assert_eq!(by_id(&obs, "Xabs").value, ObsValue::Scalar(json!(123.456)));
        // MILLIMETER_3D: three numbers, published as an array rather than a mangled string.
        assert_eq!(by_id(&obs, "Ppos").value, ObsValue::Scalar(json!([10.5, 20.25, 30])));
        // TIME_SERIES: the whole window, with its sample rate as an extra.
        let ts = by_id(&obs, "Xfreq");
        assert_eq!(ts.value, ObsValue::Scalar(json!([0.1, 0.2, 0.35, 0.4])));
        assert_eq!(ts.extra("sampleRate"), Some(&json!(1000)));
    }

    #[test]
    fn unavailable_is_its_own_value_never_a_zero() {
        let obs = decoded();
        let load = by_id(&obs, "Xload");
        assert_eq!(load.value, ObsValue::Unavailable);
        assert!(load.is_unavailable());
        assert!(!by_id(&obs, "Xabs").is_unavailable());
        // Case is the agent's business, not ours.
        assert_eq!(
            decode_value(
                &XmlElem { text: "unavailable".into(), ..XmlElem::default() },
                Category::Sample,
                Repr::Value,
                None
            ),
            ObsValue::Unavailable
        );
    }

    #[test]
    fn events_stay_verbatim_unless_the_model_says_the_type_is_numeric() {
        let obs = decoded();
        assert_eq!(by_id(&obs, "execution").value, ObsValue::Scalar(json!("ACTIVE")));
        // A program name that looks like a number must NOT become one.
        assert_eq!(by_id(&obs, "program").value, ObsValue::Scalar(json!("O1234-PART-A")));
        // PART_COUNT is numeric by type, with no units declared.
        assert_eq!(by_id(&obs, "part-count").value, ObsValue::Scalar(json!(42)));
        assert_eq!(by_id(&obs, "avail").value, ObsValue::Scalar(json!("AVAILABLE")));
    }

    #[test]
    fn a_condition_state_is_the_value_and_its_native_code_rides_as_an_extra() {
        let obs = decoded();
        let fault = by_id(&obs, "Xtravel");
        assert_eq!(fault.value, ObsValue::Condition(CondState::Fault));
        assert_eq!(fault.condition_state(), Some(CondState::Fault));
        assert_eq!(fault.extra("nativeCode"), Some(&json!("ALM-1041")));
        assert_eq!(fault.extra("nativeSeverity"), Some(&json!("2")));
        assert_eq!(fault.extra("qualifier"), Some(&json!("HIGH")));
        assert_eq!(
            fault.extra("conditionText"),
            Some(&json!("X axis travel limit exceeded")),
            "the operator-facing text"
        );
        assert_eq!(fault.category, Category::Condition);

        // A Normal condition with no text carries no condition extras at all.
        let normal = by_id(&obs, "logic");
        assert_eq!(normal.value, ObsValue::Condition(CondState::Normal));
        assert!(normal.extras.is_empty());
        assert!(!normal.is_unavailable());
    }

    #[test]
    fn reset_triggered_and_duration_ride_as_extras() {
        let obs = decoded();
        let s = by_id(&obs, "Sspeed");
        assert_eq!(s.value, ObsValue::Scalar(json!(1200)));
        assert_eq!(s.extra("resetTriggered"), Some(&json!("MANUAL")));
        assert_eq!(s.extra("duration"), Some(&json!(1.5)));
        assert_eq!(s.extra("nope"), None);
        // Extras keep a stable order: reset, duration, nativeCode, sampleRate, statistic.
        let keys: Vec<&str> = s.extras.iter().map(|(k, _)| *k).collect();
        assert_eq!(keys, vec!["resetTriggered", "duration"]);
    }

    #[test]
    fn a_data_set_decodes_into_an_object_and_removals_into_explicit_nulls() {
        let obs = decoded();
        assert_eq!(
            by_id(&obs, "tool-offsets").value,
            ObsValue::Scalar(json!({ "T1": 12.5, "T2": 7.25 }))
        );

        let removed = parse_streams(
            r#"<MTConnectStreams xmlns="urn:mtconnect.org:MTConnectStreams:2.7">
                 <Header instanceId="1"/>
                 <Streams><DeviceStream uuid="U" name="N"><ComponentStream component="Path" name="P" componentId="p">
                   <Events><ToolOffsetDataSet dataItemId="tool-offsets" sequence="9"
                       timestamp="2026-07-27T10:00:00Z" count="2">
                     <Entry key="T1">3</Entry><Entry key="T2" removed="true"/>
                   </ToolOffsetDataSet></Events>
                 </ComponentStream></DeviceStream></Streams>
               </MTConnectStreams>"#,
        )
        .unwrap();
        let m = model();
        let o = decode(&removed.device_streams[0].entries[0], m.item("tool-offsets")).unwrap();
        assert_eq!(o.value, ObsValue::Scalar(json!({ "T1": 3, "T2": null })));
    }

    #[test]
    fn a_table_nests_its_cells_one_level_deeper() {
        let doc = parse_streams(
            r#"<MTConnectStreams xmlns="urn:mtconnect.org:MTConnectStreams:2.7">
                 <Header instanceId="1"/>
                 <Streams><DeviceStream uuid="U" name="N"><ComponentStream component="Path" name="P" componentId="p">
                   <Events><WorkOffsetTable dataItemId="wt" representation="TABLE" sequence="4"
                       timestamp="2026-07-27T10:00:00Z">
                     <Entry key="G54"><Cell key="X">1.5</Cell><Cell key="Y">-2</Cell></Entry>
                   </WorkOffsetTable></Events>
                 </ComponentStream></DeviceStream></Streams>
               </MTConnectStreams>"#,
        )
        .unwrap();
        // No model entry: the representation is read off the observation itself.
        let o = decode(&doc.device_streams[0].entries[0], None).unwrap();
        assert_eq!(o.value, ObsValue::Scalar(json!({ "G54": { "X": 1.5, "Y": -2 } })));
    }

    #[test]
    fn an_observation_for_an_unmodelled_data_item_still_decodes_conservatively() {
        // A stream can run ahead of a re-probe. The observation is kept (as text), never dropped.
        let doc = parse_streams(CURRENT_2_7).unwrap();
        let mazak = doc.device_streams.iter().find(|d| d.uuid == "MAZAK.999").unwrap();
        let o = decode(&mazak.entries[0], None).unwrap();
        assert_eq!(o.data_item_id, "m-avail");
        assert_eq!(o.value, ObsValue::Scalar(json!("AVAILABLE")));
    }

    #[test]
    fn an_element_without_a_data_item_id_is_the_one_undecodable_case() {
        let entry = StreamEntry {
            category: Category::Event,
            elem: XmlElem { name: "Availability".into(), ..XmlElem::default() },
            component: None,
        };
        assert!(decode(&entry, None).is_none());
    }

    #[test]
    fn a_missing_sequence_or_timestamp_degrades_rather_than_dropping_the_value() {
        let mut elem = XmlElem { name: "Execution".into(), ..XmlElem::default() };
        elem.attrs.insert("dataItemId".into(), "execution".into());
        elem.text = "READY".into();
        let o = decode(&StreamEntry { category: Category::Event, elem, component: None }, None)
            .unwrap();
        assert_eq!(o.sequence, 0);
        assert_eq!(o.timestamp, "");
        assert_eq!(o.value, ObsValue::Scalar(json!("READY")));
    }

    #[test]
    fn condition_states_parse_from_the_element_name_and_order_by_severity() {
        assert_eq!(CondState::parse("Normal"), Some(CondState::Normal));
        assert_eq!(CondState::parse("WARNING"), Some(CondState::Warning));
        assert_eq!(CondState::parse("fault"), Some(CondState::Fault));
        assert_eq!(CondState::parse("Unavailable"), Some(CondState::Unavailable));
        assert_eq!(CondState::parse("Nonsense"), None);
        assert!(CondState::Fault.severity() > CondState::Unavailable.severity());
        assert!(CondState::Unavailable.severity() > CondState::Warning.severity());
        assert!(CondState::Warning.severity() > CondState::Normal.severity());
        assert_eq!(CondState::Normal.as_str(), "NORMAL");
        assert_eq!(CondState::Warning.as_str(), "WARNING");
        assert_eq!(CondState::Fault.as_str(), "FAULT");
        assert_eq!(CondState::Unavailable.as_str(), "UNAVAILABLE");

        // An unrecognized condition element is treated as Unavailable, never as Normal: a state
        // this client cannot read must not look healthy.
        let mut elem = XmlElem { name: "Whatever".into(), ..XmlElem::default() };
        elem.attrs.insert("dataItemId".into(), "c1".into());
        let o = decode(&StreamEntry { category: Category::Condition, elem, component: None }, None)
            .unwrap();
        assert_eq!(o.value, ObsValue::Condition(CondState::Unavailable));
        assert!(o.is_unavailable());
    }

    #[test]
    fn number_parsing_keeps_integers_integral_and_refuses_nonsense() {
        assert_eq!(parse_number("42"), Some(json!(42)));
        assert_eq!(parse_number("-7"), Some(json!(-7)));
        assert_eq!(parse_number("1.5"), Some(json!(1.5)));
        assert_eq!(parse_number("1e3"), Some(json!(1000.0)));
        assert_eq!(parse_number("NaN"), None, "not a publishable JSON number");
        assert_eq!(parse_number("inf"), None);
        assert_eq!(parse_number("O1234"), None);
        assert_eq!(numeric_or_string(" 5 "), json!(5));
        assert_eq!(numeric_or_string("ACTIVE"), json!("ACTIVE"));
        assert_eq!(numeric_vector("1 2 x"), None, "a partly numeric vector is not a vector");
        assert_eq!(numeric_vector(""), None);
    }

    #[test]
    fn an_empty_sample_text_is_an_empty_string_not_a_dropped_observation() {
        let elem = XmlElem { name: "Position".into(), ..XmlElem::default() };
        assert_eq!(
            decode_value(&elem, Category::Sample, Repr::Value, None),
            ObsValue::Scalar(json!(""))
        );
        // A non-numeric sample text survives as a string rather than being coerced to 0.
        let elem = XmlElem { name: "Position".into(), text: "HOME".into(), ..XmlElem::default() };
        assert_eq!(
            decode_value(&elem, Category::Sample, Repr::Value, None),
            ObsValue::Scalar(json!("HOME"))
        );
        // An UNAVAILABLE data set is unavailable, not an empty object.
        let elem = XmlElem { name: "X".into(), text: UNAVAILABLE.into(), ..XmlElem::default() };
        assert_eq!(
            decode_value(&elem, Category::Event, Repr::DataSet, None),
            ObsValue::Unavailable
        );
    }
}
