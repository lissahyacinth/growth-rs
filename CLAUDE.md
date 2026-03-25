# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

GrowthRS is a Kubernetes cluster autoscaler operator written in Rust. It allows users to declare node pools via CRDs and automatically provisions nodes to satisfy unschedulable pod demand. Providers include KWOK (for testing) and Hetzner (production).

## Build Commands

The Rust project lives in the `growthrs/` subdirectory.

```bash
# Build
cargo build --manifest-path growthrs/Cargo.toml

# Run
cargo run --manifest-path growthrs/Cargo.toml

# Test (unit + E2E)
just test

# Unit tests only (no cluster needed)
just test-unit

# E2E tests against a live KWOK cluster
just test-e2e

# E2E through toxiproxy (network chaos)
just test-e2e-toxi

# Run a single test
cargo test --manifest-path growthrs/Cargo.toml <test_name>

# Check (faster than build, no codegen)
cargo check --manifest-path growthrs/Cargo.toml
```

**Note:** Edition 2024 requires a nightly Rust toolchain.

## Architecture

### Core Domain (`offering/`)

`Offering` is the central type connecting providers to the scheduler. Each offering pairs an `InstanceType` (provider-opaque string) with `Resources` (cpu, memory_mib, ephemeral_storage_gib, gpu, gpu_model). Memory is in MiB to avoid fractional-GiB rounding. Newtype wrappers (`Region`, `Zone`, `InstanceType`) prevent accidental string swaps.

The module is split into:
- `mod.rs` — Core types (`Offering`, `Resources`, `PodResources`, `Location`, `AffinityConstraint`, etc.)
- `consts.rs` — Label constants (`POOL_LABEL`, `INSTANCE_TYPE_LABEL`, `MANAGED_BY_SELECTOR`, `GPU_PRODUCT_LABEL`)
- `helper.rs` — Kubernetes quantity parsing

### Optimiser (`optimiser/`)

Greedy bin-packing solver that matches Kubernetes scheduling semantics (filter → score → reserve → bind). Key types:
- `BoundedOffering` — Instance type with max instances and topology labels
- `ExistingNode` — Pre-seeded capacity for in-flight NodeRequests
- `PotentialNode` — Nodes the solver decided to create
- `PlacementSolution` — Result enum (`AllPlaced`, `NoDemands`, `IncompletePlacement`)

Split into:
- `mod.rs` — Core solver logic
- `affinity.rs` — Pod affinity/anti-affinity constraint evaluation

### Provider Interface (`providers/provider.rs`)

The `Provider` enum dispatches to concrete implementations with four methods:
- `offerings()` → what instance types are available
- `create(node_id, offering, config, provider_config)` → provision a node and return its `NodeId`
- `delete(node_id)` → remove a node
- `status(node_id)` → query infrastructure-level VM status

Provider-specific configuration is resolved by the **controller** and passed as `ProviderCreateConfig` — providers are pure execution engines. `InstanceConfig` holds provider-agnostic config (labels).

Errors go through `ProviderError` (creation failure, deletion failure, join timeout, offering unavailable, missing config, unknown provider, or internal).

Current implementations:
- **KWOK** (`providers/kwok.rs`) — Creates fake Kubernetes nodes via the API. Offerings mirror Hetzner's current lineup (CX, CPX, CAX, CCX series) plus fictional GPU instances for testing.
- **Fake** (`providers/fake/`) — Deterministic in-memory provider for testing. Behavior-scripting via queued `CreateBehavior`, `DeleteBehavior`, `StatusBehavior`, and `OfferingsBehavior` lets tests script exact sequences of successes, failures, and delays. Split into `mod.rs` (implementation) and `types.rs` (behavior types).
- **Hetzner** (`providers/hetzner/`) — Production provider using the Hetzner Cloud API via the `hcloud` crate. Split into `mod.rs` (API operations) and `config.rs` (`HetznerCreateConfig` type). Configured via `HCLOUD_TOKEN` env var.

### Configuration (`config.rs`)

Environment-based configuration via `ControllerContext`:
- `GROWTH_PROVIDER` — Provider name (kwok/fake/hetzner)
- `GROWTH_PROVISIONING_TIMEOUT` — Timeout in seconds
- `GROWTH_COOLING_DURATION` — Scale-down idle duration
- `GROWTH_REMOVAL_ATTEMPTS` — Max delete retries
- `GROWTH_UNMET_TTL` — Unmet NodeRequest lifetime
- `HCLOUD_TOKEN` — Hetzner API token

### Clock (`clock.rs`)

Wall-clock abstraction (`Clock` trait with `SystemClock` implementation). Enables deterministic testing via time injection.

### CRDs

Four Custom Resource Definitions drive the system:

- **NodePool** (`growth.vettrdev.com/v1alpha1`) — Declares scaling pools with available server types and scaling limits. Pods opt into pools via a `nodeSelector` label.
- **NodeRequest** (`growth.vettrdev.com/v1alpha1`) — Tracks individual node provisioning requests through a state machine: `Pending → Provisioning → Ready | Unmet | Deprovisioning`.
- **NodeRemovalRequest** (`growth.vettrdev.com/v1alpha1`) — Tracks node scale-down through: `Pending → Deprovisioning | CouldNotRemove`. Implemented in `crds/node_removal_request.rs` (types) and `controller/node_removal/` (reconciler, idle-node detection, helpers).
- **HetznerNodeClass** (`growth.vettrdev.com/v1alpha1`) — Provider-specific instance configuration for Hetzner. Declares OS image, SSH keys, and user-data template with variable substitution from Secrets. Defined in `crds/hetzner_node_class.rs`.

User-data templating (`crds/user_data.rs`) supports cloud-init templates via ConfigMap references with dynamic variable substitution (`REGION`, `LOCATION`, `INSTANCE_TYPE`, `NODE_LABELS`) and custom variables from Secrets.

### Controller (`controller/`)

The controller orchestration runs five concurrent watchers via `tokio::select!`:
1. **Pod watcher** (`pods/`) — Finds unschedulable pods, runs the optimiser, creates NodeRequests
2. **Node request provisioning** (`node_requests/`) — Advances NodeRequests through the state machine by communicating with providers. Resolves provider-specific config (e.g. HetznerNodeClass + user-data) before calling provider.
3. **Node ready watcher** (`node/`) — Watches for nodes transitioning to Ready
4. **Idle node scanner** (`node_removal/`) — Detects idle nodes and creates NodeRemovalRequests
5. **Node removal processor** (`node_removal/`) — Executes scale-down via provider deletion

Key modules:
- `errors.rs` — `ControllerError`, `ControllerStreamError`, `ConfigError` types
- `pods/helpers.rs` — Pod-related helper functions
- `pods/mod.rs` — Pod reconciliation with `UnconfirmedCreates` for resource-based deduplication (write-ahead buffer tracking node capacity between NodeRequest creation and API list confirmation)

### Key Dependencies

- `kube` v3 (with `runtime` and `derive` features) — Kubernetes controller runtime
- `k8s-openapi` (with `latest` and `schemars` features) — Kubernetes API types
- `schemars` v1 — JSON Schema generation for CRDs
- `hcloud` v0.25.0 (with `rustls-tls` feature) — Hetzner Cloud API client
- `fail` v0.5 (with `failpoints` feature) — Fault injection for testing
- `envconfig` v0.11.1 — Environment-based configuration

### Binaries

- `growthrs` (default) — Main controller
- `test_pod` (feature-gated `testing`) — Create/delete test pods and NodePools
- `hetzner_node` — Direct Hetzner node management CLI

## Code Conventions

Rust imports are grouped: std, then external crates, then `crate::` — separated by blank lines.

## Design Documents

- `rfc/GrowthRS.md` — Full design RFC covering CRD specs, the autoscaler loop algorithm, provider strategy, and scale-down logic
- `rfc/Provider.md` — Planned RFC for the provider interface

## Testing

### Test Environment

A KWOK-based test cluster is used for development:
1. Set up a local cluster with Kind, K3s, or Rancher
2. Install KWOK onto the cluster (see README.md)

### Chaos Testing

The justfile includes toxiproxy-based chaos tests:
- `test-e2e-latency` — Added network latency
- `test-e2e-reset` — TCP connection resets
- `test-e2e-slow` — Bandwidth throttling
- `test-e2e-flaky` — Intermittent packet slicing
