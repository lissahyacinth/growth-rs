set shell := ["bash", "-c"]

# Defaults to not interfere with K3s, which we assume developers are running locally
image := "ghcr.io/lissahyacinth/growthrs"
image_tag := "latest"

toxi_image := "growthrs-toxiproxy"
toxi_container := "growthrs-toxiproxy"
toxi_port := "16443"
toxi_admin := "8474"
toxi_api := "http://127.0.0.1:8474"
toxi_listen := "0.0.0.0:16443"
toxi_upstream := env("KUBE_HOST", "host.docker.internal:6443")

# Generate CRD YAML manifests from Rust types
gen-crds:
    cargo run --manifest-path growthrs/Cargo.toml --bin gen_crds > deploy/crds.yaml

# Default: run unit tests (no cluster needed)
test-unit:
    cargo test --manifest-path growthrs/Cargo.toml --lib

# E2E tests against a live KWOK cluster (direct connection)
test-e2e:
    cargo test --manifest-path growthrs/Cargo.toml --features failpoints e2e

# Build the toxiproxy image locally
toxi-build:
    docker build -t {{toxi_image}} deploy/toxiproxy

# Start toxiproxy container with a k8s-api proxy (idempotent)
toxi-start: toxi-build
    docker rm -f {{toxi_container}} 2>/dev/null || true
    docker run -d --add-host=host.docker.internal:host-gateway --name {{toxi_container}} -p {{toxi_port}}:{{toxi_port}} -p {{toxi_admin}}:{{toxi_admin}} {{toxi_image}}
    sleep 1
    curl -sf -X POST {{toxi_api}}/proxies -d '{"name":"k8s-api","listen":"{{toxi_listen}}","upstream":"{{toxi_upstream}}"}'

# Stop and remove the toxiproxy container
toxi-stop:
    docker rm -f {{toxi_container}} 2>/dev/null || true

# Inspect the running proxy and its toxics
toxi-list:
    curl -s {{toxi_api}}/proxies/k8s-api | python -m json.tool 2>/dev/null || curl -s {{toxi_api}}/proxies/k8s-api

# E2E with added latency (default 3000ms)
test-e2e-latency latency_ms="3000" $KUBE_PROXY_URL="https://127.0.0.1:16443":
    just toxi-start
    curl -sf -X POST {{toxi_api}}/proxies/k8s-api/toxics -d '{"type":"latency","attributes":{"latency":{{latency_ms}}}}'
    @echo "=== toxic: {{latency_ms}}ms latency ==="
    cargo test --manifest-path growthrs/Cargo.toml --features toxi e2e; \
      status=$?; just toxi-stop; exit $status

# E2E with connection resets (proxy severs TCP after timeout ms)
test-e2e-reset timeout_ms="0" $KUBE_PROXY_URL="https://127.0.0.1:16443":
    just toxi-start
    curl -sf -X POST {{toxi_api}}/proxies/k8s-api/toxics -d '{"type":"reset_peer","attributes":{"timeout":{{timeout_ms}}}}'
    @echo "=== toxic: reset_peer timeout={{timeout_ms}}ms ==="
    cargo test --manifest-path growthrs/Cargo.toml --features toxi e2e; \
      status=$?; just toxi-stop; exit $status

# E2E with bandwidth limit (default 1 KB/s)
test-e2e-slow rate_kb="1" $KUBE_PROXY_URL="https://127.0.0.1:16443":
    just toxi-start
    curl -sf -X POST {{toxi_api}}/proxies/k8s-api/toxics -d '{"type":"bandwidth","attributes":{"rate":{{rate_kb}}}}'
    @echo "=== toxic: bandwidth {{rate_kb}} KB/s ==="
    cargo test --manifest-path growthrs/Cargo.toml --features toxi e2e; \
      status=$?; just toxi-stop; exit $status

# E2E with intermittent failures (slice_average bytes before reset)
test-e2e-flaky avg_bytes="1024" $KUBE_PROXY_URL="https://127.0.0.1:16443":
    just toxi-start
    curl -sf -X POST {{toxi_api}}/proxies/k8s-api/toxics -d '{"type":"slicer","attributes":{"average_size":{{avg_bytes}},"size_variation":64,"delay":10}}'
    @echo "=== toxic: slicer avg={{avg_bytes}} bytes ==="
    cargo test --manifest-path growthrs/Cargo.toml --features toxi e2e; \
      status=$?; just toxi-stop; exit $status

# Run E2E both ways: direct then proxied
test-e2e-full proxy_url="https://127.0.0.1:16443":
    @echo "=== E2E direct ==="
    just test-e2e
    @echo "=== E2E through toxiproxy ==="
    just test-e2e-toxi {{proxy_url}}

# Nuke all growth-managed resources from the cluster
nuke:
    cargo run --manifest-path growthrs/Cargo.toml --features testing --bin nuke

# Run locally with KWOK provider (no real infrastructure)
run-local $GROWTH_PROVIDER="kwok" $GROWTH_PROVISIONING_TIMEOUT="300" $GROWTH_COOLING_DURATION="15" $GROWTH_REMOVAL_ATTEMPTS="5" $GROWTH_UNMET_TTL="120" $RUST_LOG="growthrs=debug":
    cargo run --bin growthrs --manifest-path growthrs/Cargo.toml

# Build the controller image
image-build tag=image_tag:
    docker build -t {{image}}:{{tag}} .

# Build and push the controller image to GHCR
image-push tag=image_tag: (image-build tag)
    docker push {{image}}:{{tag}}

# Lint and template-render the Helm chart
helm-lint:
    helm lint deploy/chart
    helm template growth deploy/chart

# All tests (unit + E2E direct)
test:
    just test-unit
    just test-e2e
