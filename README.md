## Getting Started with GrowthRS

1. Setup a cluster compatible with KWOK, i.e. [Kind](https://kind.sigs.k8s.io/docs/user/quick-start/) or [K3s](https://docs.k3s.io/installation), Rancher etc.
2. Install [KWOK onto the cluster](https://kwok.sigs.k8s.io/docs/user/kwok-in-cluster/)
3. Run Growth via `cargo run` to start the autoscaler controller.

## Testing

### Unit tests

```bash
cargo test --manifest-path growthrs/Cargo.toml
```

### Integration tests

Integration tests run against a live Kubernetes cluster with KWOK installed.
By default they use the `default` kubeconfig context.

Set `KUBE_CONTEXT` to target a different context:

```bash
KUBE_CONTEXT=rancher-desktop cargo test --manifest-path growthrs/Cargo.toml
```

You can also interpose a TCP proxy (e.g. toxiproxy) by setting `KUBE_PROXY_URL`:

```bash
KUBE_PROXY_URL=https://127.0.0.1:16443 cargo test --manifest-path growthrs/Cargo.toml
```
