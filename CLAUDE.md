# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

GrowthRS is a Kubernetes cluster autoscaler operator written in Rust. It allows users to declare priority-ordered node pools via CRDs and automatically provisions nodes to satisfy unschedulable pod demand. Providers include KWOK (for testing) and Hetzner (production).

## Build Commands

The Rust project lives in the `growthrs/` subdirectory.

```bash
# Build
cargo build --manifest-path growthrs/Cargo.toml

# Run
cargo run --manifest-path growthrs/Cargo.toml

# Test
cargo test --manifest-path growthrs/Cargo.toml

# Run a single test
cargo test --manifest-path growthrs/Cargo.toml <test_name>

# Check (faster than build, no codegen)
cargo check --manifest-path growthrs/Cargo.toml
```

**Note:** Edition 2024 requires a nightly Rust toolchain.

## Architecture

### Core Domain (`offering.rs`)

`Offering` is the central type connecting providers to the scheduler. Each offering pairs an `InstanceType` (provider-opaque string) with `Resources` (cpu, memory_mib, ephemeral_storage_gib, gpu, gpu_model). Memory is in MiB to avoid fractional-GiB rounding. Newtype wrappers (`Region`, `Zone`, `InstanceType`) prevent accidental string swaps.

### Provider Interface (`providers/provider.rs`)

The `Provider` trait has two methods:
- `offerings(region)` → what instance types are available
- `create(offering, config)` → provision a node and return its `NodeId`

Errors go through `ProviderError` (creation failure, join timeout, offering unavailable, missing config, or internal). Providers are responsible for the node joining the cluster or failing loudly.

Current implementations:
- **KWOK** (`providers/kwok.rs`) — Creates fake Kubernetes nodes via the API. Offerings mirror Hetzner's current lineup (CX, CPX, CAX, CCX series) plus fictional GPU instances for testing.
- **Hetzner** — Production provider, not yet implemented.

### CRDs

Two Custom Resource Definitions drive the system:

- **NodeGroupWithPriority** (`growth/v1alpha1`) — Declares scaling pools with priority ordering. Higher priority pools are attempted first. Multiple server types at the same priority are interchangeable.
- **NodeRequest** (`growth/v1alpha1`) — Tracks individual node provisioning requests through a state machine: `Pending → Provisioning → Ready | Unmet`.

### Autoscaler Reconciliation Loop

The operator runs a continuous loop:
1. Find unschedulable pods and match them to `NodeGroupWithPriority` resources
2. Compare resource demand against existing `Pending`/`Provisioning` NodeRequests
3. Create new NodeRequests for any shortfall
4. Advance NodeRequests through the state machine by communicating with providers
5. Handle scale-down by tainting idle nodes and removing them after a TTL

### Key Dependencies

- `kube` v3 (with `runtime` and `derive` features) — Kubernetes controller runtime
- `k8s-openapi` (with `latest` and `schemars` features) — Kubernetes API types
- `schemars` v1 — JSON Schema generation for CRDs

## Code Conventions

Rust imports are grouped: std, then external crates, then `crate::` — separated by blank lines.

## Design Documents

- `rfc/GrowthRS.md` — Full design RFC covering CRD specs, the autoscaler loop algorithm, provider strategy, and scale-down logic
- `rfc/Provider.md` — Planned RFC for the provider interface

## Testing Environment

A KWOK-based test cluster is used for development:
1. Set up a local cluster with Kind, K3s, or Rancher
2. Install KWOK onto the cluster (see README.md)
