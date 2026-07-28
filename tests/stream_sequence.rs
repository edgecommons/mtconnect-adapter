//! # Virtual-clock sequence tests for the streaming ladders (LLD §12 row 3)
//!
//! [`AgentRuntime::drive_stream`] is driven with a **scripted** [`ChunkSource`] under tokio's
//! paused clock, so every timing assertion is exact: a heartbeat window is `2 × heartbeatMs` to
//! the millisecond, not "about". The scripted source is cancel-safe on purpose — `select!` drops
//! and recreates the `next_chunk` future every iteration, and a script that lost a step to that
//! would make the tests (and nothing else) flaky.
//!
//! What lives here: heartbeat expiry math, liveness refresh, the three ladder *exits*
//! (`OUT_OF_RANGE`, `instanceId` change, and every ladder-1 reason), the undecodable-part
//! tolerance, and the snapshot∩stream dedupe overlap. The outer state machine that consumes these
//! exits (reconnect, snapshot republish, degradation to polling) is exercised end-to-end against
//! a live socket in `tests/stream_acquisition.rs`.

use std::collections::VecDeque;
use std::time::Duration;

use mtconnect_adapter::mtconnect::config::{parse_agents, AgentCredentials};
use mtconnect_adapter::mtconnect::multipart::MultipartReader;
use mtconnect_adapter::mtconnect::{
    AgentCtl, AgentRuntime, ChunkSource, InstanceEvent, MtcError, StreamExit,
    MAX_CONSECUTIVE_UNDECODABLE,
};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::mpsc;

const HEARTBEAT_2_7: &str = include_str!("fixtures/heartbeat_2.7.xml");
const CURRENT_2_7: &str = include_str!("fixtures/current_2.7.xml");
const ERRORS_2_7: &str = include_str!("fixtures/errors_out_of_range_2.7.xml");

const BOUNDARY: &str = "MTC-TEST-BOUNDARY";
const HEARTBEAT_MS: u32 = 10_000;

// =================================================================================================
// The scripted chunk source
// =================================================================================================

enum Step {
    /// Let this much (virtual) time pass before the next step.
    Wait(Duration),
    /// Deliver these bytes.
    Chunk(Vec<u8>),
    /// End of stream.
    Eof,
    /// A transport failure.
    Fail,
    /// Never answer again (the stream is stalled but TCP has not noticed).
    Hang,
}

struct Script {
    steps: VecDeque<Step>,
    /// The absolute deadline of an in-progress `Wait`, kept across cancellation: `select!` drops
    /// this future every time another branch wins, and the wait must resume, not restart.
    wait_until: Option<tokio::time::Instant>,
}

impl Script {
    fn new(steps: Vec<Step>) -> Self {
        Self { steps: steps.into(), wait_until: None }
    }
}

impl ChunkSource for Script {
    async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>, MtcError> {
        loop {
            match self.steps.front() {
                None => return Ok(None),
                Some(Step::Wait(d)) => {
                    let deadline =
                        *self.wait_until.get_or_insert_with(|| tokio::time::Instant::now() + *d);
                    tokio::time::sleep_until(deadline).await;
                    self.wait_until = None;
                    self.steps.pop_front();
                }
                Some(Step::Chunk(_)) => {
                    let Some(Step::Chunk(bytes)) = self.steps.pop_front() else { unreachable!() };
                    return Ok(Some(bytes));
                }
                Some(Step::Eof) => return Ok(None),
                Some(Step::Fail) => return Err(MtcError::Transport("scripted failure".into())),
                Some(Step::Hang) => {
                    std::future::pending::<()>().await;
                    unreachable!("pending never resolves");
                }
            }
        }
    }
}

/// One multipart part carrying this document, cppagent-shaped.
fn part(body: &str) -> Vec<u8> {
    format!(
        "--{BOUNDARY}\r\nContent-type: text/xml\r\nContent-length: {}\r\n\r\n{body}\r\n",
        body.len()
    )
    .into_bytes()
}

fn reader() -> MultipartReader {
    MultipartReader::from_content_type(
        &format!("multipart/x-mixed-replace;boundary={BOUNDARY}"),
        1 << 20,
    )
    .unwrap()
}

fn runtime() -> Arc<AgentRuntime> {
    let cfg = parse_agents(&json!({ "agents": [{
        // An unroutable port: these tests never want a real HTTP round-trip to succeed.
        "id": "seq-agent", "url": "http://127.0.0.1:9", "requestTimeoutMs": 250,
        "heartbeatMs": HEARTBEAT_MS
    }] }))
    .unwrap()
    .remove(0);
    AgentRuntime::new(cfg, &AgentCredentials::default()).unwrap()
}

/// A minimal Streams document for the OKUMA device: one `Xabs` sample per `(sequence, value)`.
fn streams_doc(instance_id: u64, next: u64, samples: &[(u64, f64)]) -> String {
    let obs: String = samples
        .iter()
        .map(|(seq, v)| {
            format!(
                "<Position dataItemId=\"Xabs\" name=\"Xabs\" sequence=\"{seq}\" \
                 timestamp=\"2026-07-27T10:00:00Z\">{v}</Position>"
            )
        })
        .collect();
    format!(
        "<MTConnectStreams xmlns=\"urn:mtconnect.org:MTConnectStreams:2.7\">\
           <Header instanceId=\"{instance_id}\" bufferSize=\"131072\" nextSequence=\"{next}\"/>\
           <Streams><DeviceStream uuid=\"OKUMA.123456\" name=\"OKUMA-CNC\">\
             <ComponentStream component=\"Linear\" name=\"X\" componentId=\"x\">\
               <Samples>{obs}</Samples>\
             </ComponentStream>\
           </DeviceStream></Streams>\
         </MTConnectStreams>"
    )
}

async fn drive(rt: &Arc<AgentRuntime>, steps: Vec<Step>) -> StreamExit {
    let (_tx, mut ctl) = mpsc::channel::<AgentCtl>(4);
    rt.drive_stream(&mut Script::new(steps), &mut reader(), &mut ctl).await
}

// =================================================================================================
// Heartbeat expiry math (ladder 1)
// =================================================================================================

#[tokio::test(start_paused = true)]
async fn total_silence_expires_at_exactly_twice_the_heartbeat() {
    let rt = runtime();
    let started = tokio::time::Instant::now();
    let exit = drive(&rt, vec![Step::Hang]).await;
    assert!(matches!(exit, StreamExit::HeartbeatMissed), "{exit:?}");
    assert_eq!(
        started.elapsed(),
        Duration::from_millis(u64::from(HEARTBEAT_MS) * 2),
        "the window is 2 x heartbeatMs, to the millisecond"
    );
}

#[tokio::test(start_paused = true)]
async fn every_part_refreshes_the_deadline_and_silence_after_the_last_one_expires() {
    let rt = runtime();
    let hb = Duration::from_millis(u64::from(HEARTBEAT_MS));
    let started = tokio::time::Instant::now();
    // Three heartbeat documents, each within the window, then silence: the stream lives exactly
    // 1.5hb + 1.5hb + 2hb.
    let exit = drive(
        &rt,
        vec![
            Step::Chunk(part(HEARTBEAT_2_7)),
            Step::Wait(hb * 3 / 2),
            Step::Chunk(part(HEARTBEAT_2_7)),
            Step::Wait(hb * 3 / 2),
            Step::Chunk(part(HEARTBEAT_2_7)),
            Step::Hang,
        ],
    )
    .await;
    assert!(matches!(exit, StreamExit::HeartbeatMissed), "{exit:?}");
    assert_eq!(started.elapsed(), hb * 3 / 2 + hb * 3 / 2 + hb * 2);
}

#[tokio::test(start_paused = true)]
async fn a_part_split_across_chunks_with_a_gap_still_counts_once_complete() {
    let rt = runtime();
    let hb = Duration::from_millis(u64::from(HEARTBEAT_MS));
    let bytes = part(HEARTBEAT_2_7);
    let (a, b) = bytes.split_at(bytes.len() / 2);
    // The first half arrives, then a pause longer than half the window, then the rest: the part
    // completes at 1.2hb and refreshes the deadline, so expiry lands at 1.2hb + 2hb.
    let started = tokio::time::Instant::now();
    let exit = drive(
        &rt,
        vec![
            Step::Chunk(a.to_vec()),
            Step::Wait(hb * 6 / 5),
            Step::Chunk(b.to_vec()),
            Step::Hang,
        ],
    )
    .await;
    assert!(matches!(exit, StreamExit::HeartbeatMissed), "{exit:?}");
    assert_eq!(started.elapsed(), hb * 6 / 5 + hb * 2);
}

// =================================================================================================
// Ladder exits
// =================================================================================================

#[tokio::test(start_paused = true)]
async fn an_out_of_range_error_document_exits_ladder_two_with_the_agents_floor() {
    let rt = runtime();
    rt.ingest_streams(CURRENT_2_7, false).unwrap(); // position the cursor (next = 42)
    let exit = drive(&rt, vec![Step::Chunk(part(ERRORS_2_7)), Step::Hang]).await;
    match exit {
        StreamExit::OutOfRange { first_sequence } => assert_eq!(first_sequence, 153),
        other => panic!("expected OutOfRange, got {other:?}"),
    }
}

#[tokio::test(start_paused = true)]
async fn an_instance_change_in_a_streams_part_exits_ladder_three() {
    let rt = runtime();
    let mut handle = rt.attach("OKUMA.123456");
    rt.ingest_streams(CURRENT_2_7, false).unwrap(); // instance 1749000000
    while handle.rx.try_recv().is_ok() {}

    let restarted = streams_doc(1_749_999_999, 4, &[(3, 9.5)]);
    let exit = drive(&rt, vec![Step::Chunk(part(&restarted)), Step::Hang]).await;
    assert!(matches!(exit, StreamExit::InstanceChanged), "{exit:?}");
    assert!(rt.needs_resync(), "the model is suspect until re-probed");
    // The restarted agent's low sequence was NOT discarded as stale: the state reset first.
    let events: Vec<InstanceEvent> = std::iter::from_fn(|| handle.rx.try_recv().ok()).collect();
    assert!(
        events.iter().any(|e| matches!(e, InstanceEvent::Snapshot(obs)
            if obs.iter().any(|o| o.sequence == 3))),
        "fresh post-restart observations are published, not deduped: {events:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn an_instance_change_on_an_error_document_beats_out_of_range() {
    // A restarted agent may greet a stale `from` with OUT_OF_RANGE from its NEW incarnation.
    // Ladder 3 supersedes ladder 2: the model is suspect, not just the cursor.
    let rt = runtime();
    rt.ingest_streams(CURRENT_2_7, false).unwrap();
    let errors = ERRORS_2_7.replace("instanceId=\"1749000000\"", "instanceId=\"1749999999\"");
    let exit = drive(&rt, vec![Step::Chunk(part(&errors)), Step::Hang]).await;
    assert!(matches!(exit, StreamExit::InstanceChanged), "{exit:?}");
    assert!(rt.needs_resync());
}

#[tokio::test(start_paused = true)]
async fn a_non_range_agent_error_document_is_liveness_not_an_exit() {
    let rt = runtime();
    let hb = Duration::from_millis(u64::from(HEARTBEAT_MS));
    let errors = r#"<MTConnectError xmlns="urn:mtconnect.org:MTConnectError:2.7">
         <Header instanceId="1749000000"/>
         <Errors><Error errorCode="INTERNAL_ERROR">transient agent hiccup</Error></Errors>
       </MTConnectError>"#;
    rt.ingest_streams(CURRENT_2_7, false).unwrap();
    let started = tokio::time::Instant::now();
    let exit = drive(
        &rt,
        vec![Step::Wait(hb * 3 / 2), Step::Chunk(part(errors)), Step::Hang],
    )
    .await;
    // The error document refreshed the deadline (it proves the agent is alive), so expiry lands
    // at 1.5hb + 2hb — not at 2hb.
    assert!(matches!(exit, StreamExit::HeartbeatMissed), "{exit:?}");
    assert_eq!(started.elapsed(), hb * 3 / 2 + hb * 2);
}

#[tokio::test(start_paused = true)]
async fn transport_failure_and_end_of_stream_are_ladder_one_exits() {
    let rt = runtime();
    let exit = drive(&rt, vec![Step::Chunk(part(HEARTBEAT_2_7)), Step::Fail]).await;
    assert!(matches!(exit, StreamExit::TransportLost(MtcError::Transport(_))), "{exit:?}");

    let exit = drive(&rt, vec![Step::Chunk(part(HEARTBEAT_2_7)), Step::Eof]).await;
    assert!(matches!(exit, StreamExit::EndOfStream), "{exit:?}");

    // The multipart terminator is a deliberate end of stream too.
    let exit = drive(
        &rt,
        vec![Step::Chunk(part(HEARTBEAT_2_7)), Step::Chunk(format!("--{BOUNDARY}--\r\n").into_bytes()), Step::Hang],
    )
    .await;
    assert!(matches!(exit, StreamExit::EndOfStream), "{exit:?}");
}

#[tokio::test(start_paused = true)]
async fn an_oversize_part_drops_the_stream() {
    let rt = runtime();
    let huge = format!(
        "--{BOUNDARY}\r\nContent-type: text/xml\r\nContent-length: {}\r\n\r\n",
        (1 << 20) + 1
    );
    let exit = drive(&rt, vec![Step::Chunk(huge.into_bytes()), Step::Hang]).await;
    assert!(matches!(exit, StreamExit::Malformed(MtcError::TooLarge { .. })), "{exit:?}");
}

// =================================================================================================
// Undecodable-part tolerance (LLD §9: dropped + counted; 3 consecutive → stream drop)
// =================================================================================================

#[tokio::test(start_paused = true)]
async fn two_bad_parts_are_tolerated_when_a_good_one_follows() {
    let rt = runtime();
    let before = rt.parse_counters();
    let exit = drive(
        &rt,
        vec![
            Step::Chunk(part("not xml at all")),
            Step::Chunk(part("<Wrong/>")),
            Step::Chunk(part(HEARTBEAT_2_7)),
            Step::Chunk(part("junk again")),
            Step::Chunk(part("<html>an error page</html>")),
            Step::Eof,
        ],
    )
    .await;
    assert!(matches!(exit, StreamExit::EndOfStream), "the stream survived: {exit:?}");
    let counters = rt.parse_counters();
    assert_eq!(counters.parse_errors - before.parse_errors, 4, "every bad part is counted");
}

#[tokio::test(start_paused = true)]
async fn three_consecutive_undecodable_parts_drop_the_stream() {
    let rt = runtime();
    let steps = (0..MAX_CONSECUTIVE_UNDECODABLE)
        .map(|i| Step::Chunk(part(&format!("garbage {i}"))))
        .chain([Step::Hang])
        .collect();
    let exit = drive(&rt, steps).await;
    assert!(matches!(exit, StreamExit::Malformed(MtcError::Xml(_))), "{exit:?}");
}

// =================================================================================================
// Dedupe overlap: /current snapshot ∩ stream window (LLD §5)
// =================================================================================================

#[tokio::test(start_paused = true)]
async fn the_snapshot_and_the_stream_overlap_without_duplicates() {
    let rt = runtime();
    let mut handle = rt.attach("OKUMA.123456");
    // The /current snapshot published Xabs @ seq 37 (and set next = 42).
    rt.ingest_streams(CURRENT_2_7, false).unwrap();
    while handle.rx.try_recv().is_ok() {}

    // The stream resumes from an overlapping window: seq 37 again (already published) and 43
    // (new). Only the new one may reach the instance.
    let overlap = streams_doc(1_749_000_000, 44, &[(37, 123.456), (43, 124.0)]);
    let exit = drive(&rt, vec![Step::Chunk(part(&overlap)), Step::Eof]).await;
    assert!(matches!(exit, StreamExit::EndOfStream), "{exit:?}");

    let events: Vec<InstanceEvent> = std::iter::from_fn(|| handle.rx.try_recv().ok()).collect();
    let published: Vec<u64> = events
        .iter()
        .filter_map(|e| match e {
            InstanceEvent::Snapshot(obs) => Some(obs.iter().map(|o| o.sequence)),
            _ => None,
        })
        .flatten()
        .collect();
    assert_eq!(published, vec![43], "the overlap was deduped per data item: {events:?}");

    // A DIFFERENT data item's older-but-unseen sequence still publishes (the per-item floor).
    let other = streams_doc(1_749_000_000, 45, &[(40, 1.0)])
        .replace("dataItemId=\"Xabs\" name=\"Xabs\"", "dataItemId=\"Xload\" name=\"Xload\"");
    let exit = drive(&rt, vec![Step::Chunk(part(&other)), Step::Eof]).await;
    assert!(matches!(exit, StreamExit::EndOfStream), "{exit:?}");
    let events: Vec<InstanceEvent> = std::iter::from_fn(|| handle.rx.try_recv().ok()).collect();
    assert!(
        events.iter().any(|e| matches!(e, InstanceEvent::Snapshot(obs)
            if obs.iter().any(|o| o.data_item_id == "Xload" && o.sequence == 40))),
        "a lower sequence on another data item is not stale: {events:?}"
    );
}

// =================================================================================================
// Control-channel behavior inside a live stream
// =================================================================================================

#[tokio::test(start_paused = true)]
async fn shutdown_and_reconnect_control_requests_end_the_stream() {
    let rt = runtime();
    let (tx, mut ctl) = mpsc::channel::<AgentCtl>(4);
    tx.send(AgentCtl::Shutdown).await.unwrap();
    let exit = rt.drive_stream(&mut Script::new(vec![Step::Hang]), &mut reader(), &mut ctl).await;
    assert!(matches!(exit, StreamExit::Shutdown), "{exit:?}");

    let (tx, mut ctl) = mpsc::channel::<AgentCtl>(4);
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    tx.send(AgentCtl::Reconnect { reply: reply_tx }).await.unwrap();
    let exit = rt.drive_stream(&mut Script::new(vec![Step::Hang]), &mut reader(), &mut ctl).await;
    assert!(matches!(exit, StreamExit::CtlReconnect), "{exit:?}");
    // No devices attached: the re-probe had nothing to do and answered Ok.
    assert!(reply_rx.await.unwrap().is_ok());
}
