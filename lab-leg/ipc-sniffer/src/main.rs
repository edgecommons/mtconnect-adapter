//! # IPC sniffer — the independent witness for the GREENGRASS deployed release leg
//!
//! A HARNESS, not product code. A SECOND, separate Greengrass component that subscribes to
//! `ecv1/#` over real Greengrass IPC local pub/sub and prints every publication it receives. It
//! proves that what `mtconnect-adapter` publishes actually leaves the process and arrives at
//! another component through the Nucleus — not merely that the publishing runtime returned `Ok`.
//!
//! Greengrass IPC exposes no raw-bytes subscription (unlike the local-MQTT wire gate, which reads
//! the bytes off the broker and decodes them with `prost` independently). The library's own codec
//! therefore decodes the envelope here. The independence that remains — and it is the point — is
//! that this is a different process, a different component identity, and a different IPC
//! connection from the one that published.
//!
//! Nothing in `src/**` of the component is touched or re-declared.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use edgecommons::messaging::Message;
use edgecommons::prelude::*;

const COMPONENT_NAME: &str = "com.mbreissi.edgecommons.MtcIpcSniffer";

/// Prints one line per delivery. `SNIFF <seq> <topic> <name> <json>` is greppable out of the
/// Greengrass log wrapper.
struct Printer {
    seq: AtomicU64,
}

#[async_trait]
impl MessageHandler for Printer {
    async fn handle(&self, topic: String, message: Message) {
        let n = self.seq.fetch_add(1, Ordering::Relaxed) + 1;
        let body = serde_json::to_string(&message.body).unwrap_or_else(|e| format!("<{e}>"));
        let identity = message
            .identity
            .as_ref()
            .map_or_else(|| "-".to_string(), |i| i.path().to_string());
        println!(
            "SNIFF #{n} topic={topic} name={} identity={identity} body={body}",
            message.header.name
        );
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let gg = EdgeCommonsBuilder::new(COMPONENT_NAME)
        .args(std::env::args_os())
        .build()
        .await?;

    let filter = std::env::var("SNIFF_FILTER").unwrap_or_else(|_| "ecv1/#".to_string());
    let messaging = gg.messaging()?;
    messaging
        .subscribe(
            &filter,
            Arc::new(Printer {
                seq: AtomicU64::new(0),
            }),
            1024,
            1,
        )
        .await?;
    println!("SNIFFER READY filter={filter}");

    gg.shutdown_signal().await;
    println!("SNIFFER stopping");
    // Always unsubscribe before exit, or the leaked subscription outlives the process.
    messaging.unsubscribe(&filter).await.ok();
    println!("SNIFFER stopped");
    Ok(())
}
