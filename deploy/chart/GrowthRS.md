# GrowthRS Controller — Helm Chart

## Install

```bash
helm install growth deploy/chart
```

## Hetzner setup

1. Create the Hetzner API token secret:

```bash
kubectl create secret generic hcloud-token --from-literal=token=<YOUR_HCLOUD_TOKEN>
```

2. Create the cloud-init ConfigMap from the example template:

```bash
kubectl create configmap k3s-cloud-init \
  --from-file=cloud-init.sh=deploy/chart/examples/cloud-init.sh
```

3. Create the k3s join secret:

```bash
kubectl create secret generic k3s-join \
  --from-literal=url=https://<API_SERVER_IP>:6443 \
  --from-literal=token=<K3S_TOKEN>
```

4. Apply the example HetznerNodeClass and NodePool (edit first):

```bash
kubectl apply -f deploy/chart/examples/nodeclass.yaml
kubectl apply -f deploy/chart/examples/nodepool.yaml
```

## Umbrella chart (recommended for production)

The GrowthRS chart deploys the controller only. Cluster-specific resources
(HetznerNodeClass, NodePool, Secrets, ConfigMaps) are best managed in your own
umbrella chart that depends on GrowthRS. This keeps the operator chart
generic and your infrastructure config in one place.

```
my-cluster/
├── Chart.yaml
├── values.yaml
└── templates/
    ├── cloud-init-configmap.yaml
    ├── hetzner-secrets.yaml
    ├── nodeclass.yaml
    └── nodepool.yaml
```

**Chart.yaml:**

```yaml
apiVersion: v2
name: my-cluster
version: 0.1.0
dependencies:
  - name: growth
    version: "0.1.0"
    repository: "file://../../deploy/chart"   # or your chart repo URL
```

**values.yaml** (overrides passed to the growth subchart):

```yaml
growth:
  image:
    tag: "v0.2.0"
  logLevel: "growthrs=debug"
```

**templates/cloud-init-configmap.yaml:**

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: k3s-cloud-init
data:
  cloud-init.sh: |
    # Paste or .Files.Get your cloud-init template here.
    # See deploy/chart/examples/cloud-init.sh for a working k3s example.
```

**templates/nodeclass.yaml:**

```yaml
apiVersion: growth.vettrdev.com/v1alpha1
kind: HetznerNodeClass
metadata:
  name: k3s-agent
spec:
  image: ubuntu-24.04
  sshKeyNames:
    - my-ssh-key
  userData:
    templateRef:
      name: k3s-cloud-init
      namespace: {{ .Release.Namespace }}
      key: cloud-init.sh
    variables:
      - name: K3S_URL
        secretRef:
          name: k3s-join
          namespace: {{ .Release.Namespace }}
          key: url
      - name: K3S_TOKEN
        secretRef:
          name: k3s-join
          namespace: {{ .Release.Namespace }}
          key: token
```

**templates/nodepool.yaml:**

```yaml
apiVersion: growth.vettrdev.com/v1alpha1
kind: NodePool
metadata:
  name: default
spec:
  serverTypes:
    - name: cpx22
      max: 3
  locations:
    - region: nbg1
  labels:
    growth.vettrdev.com/pool: default
  nodeClassRef:
    name: k3s-agent
```

Then:

```bash
helm dependency update my-cluster/
helm install my-cluster my-cluster/
```

This deploys the controller, CRDs, RBAC, and all your cluster-specific
resources in a single `helm install`.

## Configuration

| Value | Default | Description |
|---|---|---|
| `image.repository` | `ghcr.io/lissahyacinth/growthrs` | Container image |
| `image.tag` | `latest` | Image tag |
| `image.port` | `8080` | Health endpoint port |
| `provider` | `hetzner` | Provider (`hetzner` or `kwok`) |
| `provisioningTimeout` | `300` | Node creation timeout (seconds) |
| `coolingDuration` | `15` | Idle time before scale-down (seconds) |
| `removalAttempts` | `5` | Max deletion retries |
| `unmetTtl` | `120` | Unmet NodeRequest lifetime (seconds) |
| `logLevel` | `growthrs=info` | RUST_LOG filter |
| `resources.requests.cpu` | `100m` | CPU request |
| `resources.requests.memory` | `128Mi` | Memory request |
| `resources.limits.cpu` | `500m` | CPU limit |
| `resources.limits.memory` | `256Mi` | Memory limit |
| `hcloud.tokenSecretName` | `hcloud-token` | Secret name for Hetzner API token |
| `hcloud.tokenSecretKey` | `token` | Key within the secret |

## CRDs

CRDs are in the `crds/` directory and installed automatically on first `helm install`.
They are **not** deleted on `helm uninstall` (Helm 3 convention).

To regenerate CRDs from Rust source types:

```bash
just gen-crds
```

## Lint

```bash
just helm-lint
```
