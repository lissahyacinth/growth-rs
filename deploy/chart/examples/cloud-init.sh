#cloud-config
#
# GrowthRS k3s agent cloud-init.
#
# cloud-config directives (ssh_pwauth, chpasswd, ssh_authorized_keys) run
# during early cloud-init stages — before sshd accepts connections — avoiding
# the Hetzner "Password change required but no TTY available" issue.
#
# Static vars (resolved at operator startup from env vars):
#   {{ K3S_TOKEN }}      — k3s join token
#   {{ SSH_PUBLIC_KEY }} — public key for SSH access
#   {{ LB_IP }}          — load balancer / API server IP
#   {{ K3S_VERSION }}    — k3s version to install
#
# Dynamic vars (resolved per-node by the operator):
#   {{ REGION }}         — Hetzner region (e.g. eu-central)
#   {{ LOCATION }}       — Hetzner location (e.g. nbg1)
#   {{ INSTANCE_TYPE }}  — server type (e.g. cpx22)
#   {{ NODE_LABELS }}    — extra --node-label flags (pool, NodeRequest, instance-type)
#   {{ NODE_TAINTS }}    — extra --register-with-taints flags (startup taint)

ssh_pwauth: false
chpasswd:
  expire: false

ssh_authorized_keys:
  - {{ SSH_PUBLIC_KEY }}

write_files:
  - path: /root/growthrs-bootstrap.sh
    permissions: "0755"
    content: |
      #!/bin/bash
      set -euo pipefail
      exec > >(tee -a /var/log/growthrs-bootstrap.log) 2>&1

      echo "=== GrowthRS k3s agent bootstrap ==="

      # Wait for private network interface (Hetzner attaches it async).
      # Interface name varies by server generation:
      #   ens10   — CX*1, CCX*1 (older Intel)
      #   enp7s0  — CPX, CAX, CX*2, CCX*2/3 (AMD/ARM/newer Intel)
      MAX_ATTEMPTS=30
      DELAY=10
      PRIVATE_IP=""
      NET_IF=""

      for i in $(seq 1 $MAX_ATTEMPTS); do
        for iface in enp7s0 ens10; do
          PRIVATE_IP=$(ip -4 addr show "$iface" 2>/dev/null | grep -oP '(?<=inet\s)\d+\.\d+\.\d+\.\d+' || true)
          if [ -n "$PRIVATE_IP" ]; then
            NET_IF="$iface"
            break 2
          fi
        done
        echo "Waiting for private network interface... (attempt $i/$MAX_ATTEMPTS)"
        sleep $DELAY
      done

      if [ -z "$PRIVATE_IP" ]; then
        echo "ERROR: Timeout waiting for private network interface"
        echo "Available interfaces:"
        ip -4 addr show
        exit 1
      fi
      echo "Private network IP: $PRIVATE_IP on $NET_IF (attempt $i)"

      echo "Installing k3s agent..."
      curl -sfL https://get.k3s.io | \
        INSTALL_K3S_VERSION="{{ K3S_VERSION }}" \
        K3S_URL="https://{{ LB_IP }}:6443" \
        K3S_TOKEN="{{ K3S_TOKEN }}" \
        INSTALL_K3S_EXEC="agent" \
        sh -s - \
          --node-ip="$PRIVATE_IP" \
          --node-label=topology.kubernetes.io/region={{ REGION }} \
          --node-label=topology.kubernetes.io/zone={{ LOCATION }} \
          --node-label=node.kubernetes.io/instance-type={{ INSTANCE_TYPE }} \
          {{ NODE_LABELS }} \
          {{ NODE_TAINTS }}

      echo "k3s agent bootstrap complete"

runcmd:
  - [bash, /root/growthrs-bootstrap.sh]
