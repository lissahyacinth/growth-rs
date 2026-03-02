# Growth 

## Building an Autoscaler

Build will happen in two phases

1. Autoscaler allows users to control scaling decisions via declaring `NodePool`s that match to their pods.
2. Autoscaler continuously optimises a cluster based on available nodes from providers.

Initial provider and machine availability will be;
| Provider | Machines |
| -------- | -------  |
| KWOK     | N/A      |
| Hetzner  | CX31     |
| Hetzner  | CX32     |
| Hetzner  | CX33     |

As Hetzner is very quick and cheap for machines. A separate RFC is being written for the Provider interface, as no existing Provider for Hetzner in Rust exists that can spin up new nodes on demand. KWOK (Kubernetes With Out Kubelet) will also be used for testing.

### Flexible Node Groups with NodePool
We define two example pools - one default with a range of CPU capacity, and one GPU pool.

```yaml
apiVersion: growth.vettrdev.com/v1alpha1
kind: NodePool
metadata:
  name: default
spec:
  serverTypes:
    - name: CAX11
      max: 10
    - name: CAX21
      # MAYBE feature - pre-warm to guarantee 1 even without load
      min: 1
      max: 5
    - name: CAX31
      max: 1

---

apiVersion: growth.vettrdev.com/v1alpha1
kind: NodePool
metadata:
  name: gpu
spec:
  serverTypes:
    - name: GEX44
      max: 5
```

Pods can *opt* into a NodeGroup. 
```yaml
apiVersion: v1
kind: Pod
metadata:
  name: nginx
spec:
  nodeSelector:
    growth.vettrdev.com/pool: default
  containers:
  - name: nginx
    image: nginx:1.14.2
    ports:
    - containerPort: 80
```

If a `NodePool` with the name `default` exists, Pods are considered `opt-in` by default. Otherwise, pods have to explicitly `opt-in` by providing a nodeSelector that targets an existing `NodePool`.

### Autoscaler Loop

```yaml
apiVersion: growth.vettrdev.com/v1alpha1
kind: NodeRequest
metadata:
  # NodeRequests cannot exist without a NodePool, as otherwise we can't know what we're managing.
  name: default-pool-{uuid}
  ownerReferences: [NodePool]
spec:
  targetOffering: hetzner-cax11
status:
  phase: Provisioning  # Pending -> Provisioning -> Ready / Unmet / Deprovisioning
  lastTransitionTime: 2026-01-01 21:07:52:00Z
```

#### Event Examples
```yaml
events:
- at: 2026-01-01 21:07:52:00Z
  name: nodeRequested
- at: 2026-01-01 21:08:00:00Z
  name: nodeRequestFailed
  reason: "No capacity"
- at: 2026-01-01 21:10:00:00Z
  name: nodeProvisioned
- at: 2026-01-01 21:11:00:00Z
  name: nodeLabelled
```      


```yaml
apiVersion: growth.vettrdev.com/v1alpha1
kind: NodeRemovalRequest
metadata:
  name: default-pool-{uuid}
status:
  phase: CouldNotRemove  # Pending -> Deprovisioning / CouldNotRemove
  removalAttempts: 3
  lastTransitionTime: 2026-01-01 21:07:52:00Z
```

#### Example Events
```yaml
  events:
    - at: 2026-01-01 21:07:52:00Z
      name: Deletion requested
    - at: 2026-01-01 21:08:00:00Z
      name: Deletion failed
      reason: "Insufficient permission..."
```

--- Handle New Demand
1. Find Unschedulable Pods (filtering DaemonSets), label as `Demand`s.
2. Match the pods to `NodePool`. The `NodePool` offers various machines, termed `Offering`s.
  1. Match pod to `NodePool` if the pod has a `nodeSelector` targeting a specific `NodePool`.
  2. If pod doesn't request a `NodePool`, and a `default` `NodePool` exists, use that.
  3. If pod doesn't request a `NodePool`, and no `default` exists, create error event.
  4. If pod requests a `NodePool`, and it doesn't exist, create error event.
3. Retrieve all `NodeRequest`s and build in-flight state:
  1. Collect claimed pod UIDs from all non-`Unmet` NRs (Pending, Provisioning, Ready, Deprovisioning). Pods with these UIDs are excluded from demand so we never double-provision.
  2. Count Pending/Provisioning NRs per pool per instance type. Combined with actual node counts (from `managed-by=growth` labelled nodes), these form the "occupied slots" used to enforce `max` limits.
  3. `Unmet` NRs release their claimed pod UIDs back to the demand pool — the solver will attempt to place them on alternative offerings.
4. An in-memory `PendingClaims` cache bridges API lag: pod UIDs are recorded immediately after NR creation and drained once the API list reflects them. This prevents duplicate NRs across back-to-back reconciliation loops.
5. Provide the Solver with unclaimed `Demand`s, `Offering`s, and occupied counts.
6. The Solver provides a PlacementSolution.
  1. If the PlacementSolution is `AllPlaced`, create a `NodeRequest` for each potential node.
  2. If the PlacementSolution is `NoDemands`, do nothing.
  3. If the PlacementSolution is `IncompletePlacement`, create `NodeRequest`s where possible, but also emit events indicating what could not be scheduled and why.
  4. (Planned) Track consecutive `IncompletePlacement` results per unschedulable pod. If a pod has been unplaceable for N consecutive loops, apply exponential backoff to how often it is included in demand. After reaching a backoff ceiling, mark the pod as `BackOff` — exclude it from demand calculations and emit a `BackOff` event on the pod. This prevents the controller from spinning against an exhausted pool. `BackOff` pods are re-evaluated when an offering recovers from `Unmet` (i.e. its TTL expires and a subsequent provision succeeds).

--- Separately, let's handle existing NodeRequests
Currently we face an issue here - PlacementSolutions don't offer backups if a node isn't available. We could replan in this case, but it could cause a LOT of replanning for other nodes.

We can't easily track this in state. Fallbacks require replanning with the solver, and tracking pod -> node relationships. What's 'simpler' is removing a NodeType from the pool for N time, and all replans do not include that node. We can achieve that with the Unmet.

1. For each `NodeRequest` in state `Pending`.
  1. Request the node to be created from the provider.
  2. If provider accepts request, update state to `Provisioning` and add a prospective NodeID.
  3. If provider doesn't have capacity, update the state to `Unmet`. Pods linked to this demand will get new nodes provisioned on the next loop.
2. For each `NodeRequest` in state `Provisioning`
  1. Check prospective Node status via the provider. If provider reports `Failed` or `NotFound`, update to `Unmet`.
  2. If, after a known `provisioning_timeout`, the Node does not become `Ready`, call `provider.delete()` and update state to `Deprovisioning`. If the delete call fails, stay in `Provisioning` and requeue for retry.
3. For each `NodeRequest` in state `Ready`, await further changes. (Planned: TTL-based cleanup.)
4. For each `NodeRequest` in state `Unmet`, await further changes. (Planned: TTL-based cleanup to release the slot.)
5. For each `NodeRequest` in state `Deprovisioning`, poll the provider. Once the provider confirms the node is gone (`NotFound`), delete the NodeRequest CRD. Otherwise, requeue for retry. (Planned: stall detection and alerting.)

--- Now, let's handle Node Removal and ScaleDown.
A periodic idle-node scanner runs alongside the per-object NRR reconciler.

**Idle Node Scanner** (runs every 30s):
1. List all Growth-managed nodes (labelled `managed-by=growth`) and all pods.
2. Detect idle nodes — nodes with no running pods (ignoring DaemonSets, mirror pods, and pods in terminal phase). Respect `NodePool` `min` limits per (pool, instance_type) to avoid scaling below declared minimums.
3. For each newly idle node, create a `NodeRemovalRequest` in `Pending` state and annotate the node as a removal candidate.
4. For each existing NRR (in `Pending` phase) whose node is no longer idle, cancel the NRR: remove annotations and delete the NRR.

**NodeRemovalRequest Reconciler** (per-object state machine: `Pending → Deprovisioning / CouldNotRemove`):
1. `Pending` — Cooling-off period. At each requeue, verify the node is still idle. If pods have appeared, cancel the NRR. Once the cooling-off duration elapses, apply a `NoSchedule` taint (`growth.vettrdev.com/scale-down: NoSchedule`), call `provider.delete()`, and transition to `Deprovisioning` with `removalAttempts: 1`. The taint **must** be `NoSchedule` to prevent the Kubernetes scheduler from placing new pods onto a node that is about to be removed.
2. `Deprovisioning` — Poll the provider for node status.
  1. If `NotFound`, delete the Kubernetes Node object and the NRR.
  2. If still present, retry `provider.delete()` and increment `removalAttempts`.
  3. If `removalAttempts` exceeds the configured maximum, transition to `CouldNotRemove`.
4. `CouldNotRemove` — Terminal failure state. The node remains tainted and the operator must investigate manually. Emit an event on the NRR so that monitoring can alert on stuck nodes.
