# Container image for a edgecommons Rust component, for the KUBERNETES (and HOST/Docker) platform.
# Requires the edgecommons crate resolvable (cargo git dep on the private repo, or crates.io);
# see docs/platform/DESIGN-packaging.md §13.
#
# Multi-stage: stage 1 compiles the standalone release binary against the edgecommons crate;
# stage 2 is a slim glibc runtime that carries only the binary, run as a non-root user.
#
# Build (the cargo git dep needs network + git auth to fetch the private edgecommons repo —
# pass a GITHUB_TOKEN or mount an SSH agent):
#   docker build -t <image> .
# Then push to your registry (or `kind load docker-image <image>` for a local cluster) and set
# `image:` in k8s/deployment.yaml.

# ---- stage 1: build -------------------------------------------------------------------------
# The toolchain floor is set by the locked dependency graph, not by this crate's `rust-version`:
# `time` needs 1.88 and the `icu`/`idna` chain needs 1.86, so an older base cannot build Cargo.lock.
FROM rust:1.88-slim-bookworm AS build

# Resolve the private edgecommons git dependency using the system git (honours GITHUB_TOKEN / SSH).
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true

# git is needed for cargo to fetch the git dependency.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy the crate manifest (+ lockfile if present) and sources, then build the release binary.
COPY Cargo.toml ./
COPY Cargo.lock* ./
COPY src ./src

# `metrics-prometheus` is what makes the KUBERNETES profile's default metric target real: that
# profile selects `prometheus` on :9090, which k8s/deployment.yaml maps as the `metrics` port.
# Without the feature the target falls back to a log file the read-only root filesystem refuses,
# and the pod serves no metrics at all.
RUN cargo build --release --bin mtconnect-adapter --features metrics-prometheus

# ---- stage 2: runtime -----------------------------------------------------------------------
# debian:bookworm-slim has glibc (the binary is glibc-linked) and is small.
FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /build/target/release/mtconnect-adapter /usr/local/bin/component

# Run as a non-root, unprivileged user (matches the Deployment's runAsNonRoot securityContext).
USER 65532:65532

# No default args: with --platform auto the library auto-detects KUBERNETES (or HOST) and
# defaults its config source / transport / identity accordingly. Override via the Deployment's
# args: if you need an explicit platform/transport/config-source.
ENTRYPOINT ["/usr/local/bin/component"]
