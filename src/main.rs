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

use edgecommons::config::{ConfigurationValidationPhase, ConfigurationValidationResult};
use edgecommons::prelude::*;
use mtconnect_adapter::reload::{self, Verdict};
use mtconnect_adapter::supervisor;

/// The component's full name (matches `recipe.yaml` / `gdk-config.json`).
const COMPONENT_NAME: &str = "com.mbreissi.edgecommons.MtconnectAdapter";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let gg = EdgeCommonsBuilder::new(COMPONENT_NAME)
        .args(std::env::args_os())
        // The pre-commit reload gate (LLD §8). It runs BEFORE a candidate becomes the active
        // configuration, so a change that only a restart can apply — a re-pointed agent, an added
        // or removed instance — is refused with `RESTART_REQUIRED` and the component keeps running
        // on its last-good configuration instead of being half-applied. The verdict itself is
        // pure and unit-tested (`mtconnect_adapter::reload::classify`).
        .configuration_validator("MtconnectReload", |candidate, current, phase| {
            let current = match phase {
                ConfigurationValidationPhase::Initial => None,
                ConfigurationValidationPhase::Reload => current,
            };
            Ok(match reload::classify(&candidate, current.as_ref()) {
                Verdict::Accept => ConfigurationValidationResult::accept(),
                Verdict::Reject { code, message } => {
                    ConfigurationValidationResult::reject(code, message)
                }
            })
        })?
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
