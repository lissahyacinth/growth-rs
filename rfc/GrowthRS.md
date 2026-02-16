# Growth 

## Building an Autoscaler

Build will happen in two phases

1. Autoscaler allows users to control scaling decisions via declaring NodeGroupsWithPriority that match to their pods.
2. Autoscaler continuously optimises a cluster based on available nodes from providers.

Initial provider and machine availability will be;
| Provider | Machines |
| -------- | -------  |
| KWOK     | N/A      |
| Hetzner  | CX31     |
| Hetzner  | CX32     |
| Hetzner  | CX33     |

As Hetzner is very quick and cheap for machines. A separate RFC is being written for the Provider interface, as no existing Provider for Hetzner in Rust exists that can spin up new nodes on demand. KWOK (Kubernetes With Out Kubelet) will also be used for testing.

### Flexible Node Groups with Pool
```yaml
apiVersion: growth/v1alpha1
kind: NodeGroupWithPriority
spec:
  pools:
    - provider: hetzner
      serverType: [CAX11]
      priority: 90
    - provider: hetzner
      serverType: [CX31, CX32]
      priority: 50
```

Which effectively says to prefer CAX11 if available, then use CX31 as backup. No knowledge exists for whether this node is the most appropriate. CX31 and CX32 being in the same array would be the same as writing 

```yaml
...
    - provider: hetzner
      serverType: [CX31]
      priority: 50
    - provider: hetzner
      serverType: [CX32]
      priority: 50
```

### Autoscaler Loop

```yaml
apiVersion: growth/v1alpha1
kind: NodeRequest
metadata:
  ownerReferences: [NodeGroupWithPriority]
spec:
  requirements:
    cpu: "4"
    memory: "8Gi"
status:
  phase: Provisioning  # Pending -> Provisioning -> Ready / Unmet
  currentPool: gcp-g2-standard-8
  attempts:
    - pool: hetzner-gex44
      result: InsufficientCapacity
      timestamp: ...
    - pool: gcp-g2-standard-8
      result: Provisioning
      timestamp: ...
    - pool: gcp-g2-standard-16
      result: Pending
      timestamp: ...
```

1. Find Unschedulable Pods.
2. Match the pods to `NodeGroupWithPriority`.
  1. If multiple match, log error in operator. 
3. Match pods to existing `NodeRequest`s in state `Pending`/`Provisioning`. (Assume `Ready` won't match against current Pod Demand, and `Provisioning` can't accept pods yet.)
  1. Match is performed on Resource Requirements only, with other constraints left to future.
  2. Sum resource demand from matched pods.
  3. Sum expected capacity from existing `Pending`/`Provisioning` `NodeRequest`s.
4. If demand exceeds planned capacity, create `NodeRequest`s to cover the shortfall.
5. For each `NodeRequest` in state `Pending`.
  1. Attempt the current pool, updating the timestamp.
  2. If provider accepts request, update state to `Provisioning` and add a prospective NodeID.
  3. If all pools are `Unmet`, update `NodeRequest` to `Unmet`.
6. For each `NodeRequest` in state `Provisioning`
  1. Check prospective Node state. If Node becomes `Ready`, update `NodeRequest` to `Ready` and add labels matching the `NodeGroupWithPriority` that created it. 
  2. After a known `ReadinessWait`, if Node does not become `Ready`, update attempts and attempt to remove Node. (This might be a provider detail for them to deal with.)
  3. If attempts exceeds maxAttempts, updated to failed, reset `NodeRequest` to `Pending `,
7. For each `NodeRequest` in state `Ready`/`Unmet`.
  1. If now - timestamp exceeds TTL, delete.
8. For each `Node` with a `NodeGroupWithPriority` label that has no pods (Ignoring DaemonSets etc.), mark the node as `ScaleDownAt: {FutureTime}` and taint it.
9. For each `Node` with a `NodeGroupWithPriority` label where the current time has passed the `ScaleDownTime`, remove it.
