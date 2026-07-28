//! # MtconnectAdapter — entry point
//!
//! An AWS IoT Greengrass v2 component built on the `edgecommons` Rust library.
//! Initializes the runtime from the standard CLI contract (`-c`/`--platform`/`--transport`/`-t`),
//! then hands control to [`app::App`]. The component runs until a shutdown signal
//! (Ctrl-C / SIGTERM); dropping the [`edgecommons::EdgeCommons`] runtime then releases
//! all resources (RAII).
//!
//! ## Running locally (HOST platform, MQTT transport, against a local MQTT broker)
//! ```bash
//! cargo run -- \
//!   --platform HOST --transport MQTT ./test-configs/standalone-messaging.json \
//!   -c FILE ./test-configs/config.json \
//!   -t my-thing
//! ```

use mtconnect_adapter::supervisor;
use edgecommons::prelude::*;

/// The component's full name (matches `recipe.yaml` / `gdk-config.json`).
const COMPONENT_NAME: &str = "com.mbreissi.edgecommons.MtconnectAdapter";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let gg = EdgeCommonsBuilder::new(COMPONENT_NAME)
        .args(std::env::args_os())
        .build()
        .await?;

    tracing::info!(
        component = gg.component_name(),
        identity = %gg.config().identity().path(),
        "MtconnectAdapter starting"
    );

    let app = supervisor::App::new(&gg)?;
    app.run(&gg).await?;

    tracing::info!("MtconnectAdapter stopped");
    Ok(())
}
