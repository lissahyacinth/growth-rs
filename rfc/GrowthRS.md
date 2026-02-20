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
We define two example pools - one default with a range of CPU capacity, and one GPU pool.

```yaml
apiVersion: growth/v1alpha1
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

apiVersion: growth/v1alpha1
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
    growth.dev/pool: default
  containers:
  - name: nginx
    image: nginx:1.14.2
    ports:
    - containerPort: 80
```

If a `NodePool` with the name `default` exists, Pods are considered `opt-in` by default. Otherwise, pods have to explicitly `opt-in` by providing a nodeSelector that targets an existing `NodePool`.

### Autoscaler Loop

```yaml
apiVersion: growth/v1alpha1
kind: NodeRequest
metadata:
  # NodeRequests cannot exist without a NodePool, as otherwise we can't know what we're managing.
  name: default-pool-{uuid}
  ownerReferences: [NodePool]
spec:
  targetOffering: hetzner-cax11
status:
  phase: Provisioning  # Pending -> Provisioning -> Ready / Unmet
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
apiVersion: growth/v1alpha1
kind: NodeRemovalRequest
metadata:
  name: default-pool-{uuid}
status:
  phase: CouldNotRemove  # Pending -> Deprovisioning, CouldNotRemove
  events:
    - at: 2026-01-01 21:07:52:00Z
      name: Deletion requested
    - at: 2026-01-01 21:08:00:00Z
      name: Deletion failed
      reason: "Insufficient permission..."
```


--- Handle New Demand
1. Find Unschedulable Pods, label as `Demand`s.
2. Match the pods to `NodePool`. The `NodePool` offers various machines, termed `Offering`s. 
  1. Match pod to `NodePool` if the pod asks for a specific `NodePool`. 
  2. If pod doesn't request a `NodePool`, and a `default` `NodePool` exists, use that.
  3. If pod doesn't request a `NodePool`, and no `default` exists, create error event.
  4. If pod requests a `NodePool`, and it doesn't exist, create error event. 
3. Retrieve `NodeRequest`s in state `Pending`/`Provisioning`. (Assume `Ready` won't match against current Pod Demand, and `Provisioning` can't accept pods yet.)
4. Retrieve `NodeRequest`s in state `Unmet` - these act as ways to tell the solver we don't believe these nodes are currently available, and should plan around them.
5. Provide the Solver with `Demand`s, `Offering`s, and existing `NodeRequests` smuggled as `Offering`s. 
6. The Solver provides a PlacementSolution. 
  1. If the PlacementSolution is `AllPlaced`, create a `NodeRequest` for each potential node.
  2. If the PlacementSolution is `NoDemands`, do nothing.
  3. If the PlacementSolution is `IncompletePlacement`, create `NodeRequest`s where possible, but also emit events indicating what could not be scheduled and why.
  4. Track consecutive `IncompletePlacement` results per unschedulable pod. If a pod has been unplaceable for N consecutive loops, apply exponential backoff to how often it is included in demand. After reaching a backoff ceiling, mark the pod as `BackOff` — exclude it from demand calculations and emit a `BackOff` event on the pod. This prevents the controller from spinning against an exhausted pool. `BackOff` pods are re-evaluated when an offering recovers from `Unmet` (i.e. its TTL expires and a subsequent provision succeeds).

--- Separately, let's handle existing NodeRequests
Currently we face an issue here - PlacementSolutions don't offer backups if a node isn't available. We could replan in this case, but it could cause a LOT of replanning for other nodes. 

We can't easily track this in state. Fallbacks require replanning with the solver, and tracking pod -> node relationships. What's 'simpler' is removing a NodeType from the pool for N time, and all replans do not include that node. We can achieve that with the Unmet.

1. For each `NodeRequest` in state `Pending`.
  1. Request the node to be created from the provider.
  2. If provider accepts request, update state to `Provisioning` and add a prospective NodeID.
  3. If provider doesn't have capacity, update the state to `Unmet` with a known TTL. Pods linked to this demand will get new nodes provisioned on the next loop.
2. For each `NodeRequest` in state `Provisioning`
  1. Check prospective Node state. If Node becomes `Ready`, update `NodeRequest` to `Ready`
  2. If, after a known `ReadinessWait`, the Node does not become `Ready`, update state to `Deprovisioning`.
3. For each `NodeRequest` in state `Ready`/`Unmet`, if now - timestamp exceeds TTL, delete. (TTL for `Unmet` differs to TTL for `Ready`.)
4. For each `NodeRequest` in state `Deprovisioning`, Create a `NodeRemovalRequest`, and remove the `NodeRequest`.

--- Now, let's handle Node Removal and ScaleDown.
1. For each `Node` with a `NodePool` label that has no pods (Ignoring DaemonSets etc.), mark the node as `ScaleDownAt: {FutureTime}` and apply a `NoSchedule` taint (`growth.dev/scale-down: NoSchedule`). The taint **must** be `NoSchedule` to prevent the Kubernetes scheduler from placing new pods onto a node that is about to be removed.
2. For each `Node` with a `NodePool` label where the current time has passed the `ScaleDownTime`, verify the node still has no running pods (excluding DaemonSets), then create a `NodeRemovalRequest` object with state `Pending`. If pods have been scheduled since the taint was applied (e.g. via tolerations), remove the taint and cancel the scale-down.
3. For each `NodeRemovalRequest` with state `Pending`, request deletion from the Provider and update the state to `Deprovisioning`. 
4. For each `NodeRemovalRequest` with state `Deprovisioning`
  1. Check state of Node on Provider, if deleted, verify the node has no running pods before finalising removal. If pods are present (e.g. via tolerations), cancel the removal, remove the `NoSchedule` taint, and delete the `NodeRemovalRequest`. Otherwise, remove the `NodeRemovalRequest` and free the node.
  2. Request deletion from the provider, edit `NodeRemovalRequest` to increment `removalAttempt` and set `removeAttemptedAt` to now. 
  3. If now - `removeAttemptedAt` exceeds a TTL, re-attempt deletion. 
  4. If `removalAttempt` exceeds a total count, update state to `RemovalFailed`. Emit an event on the `NodeRemovalRequest` and the associated `Node` so that monitoring can alert on stuck nodes. `RemovalFailed` is a terminal state — the node remains tainted and the operator must investigate manually. A periodic retry (with long backoff, e.g. hours) may be added in future to handle transient provider issues.
