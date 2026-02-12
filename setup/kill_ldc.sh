#!/bin/bash
#
# kill_ldc.sh
#
# Kills all ldc processes on all cluster nodes.
#
# Usage:
#   ./kill_ldc.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/env.sh"

ADMIN_USER="${USERNAME_RAW}"
ADMIN_KEY="/users/${ADMIN_USER}/.ssh/id_rsa"

NODES=(
    "hp058.utah.cloudlab.us"
    "hp199.utah.cloudlab.us"
    "hp182.utah.cloudlab.us"
    "hp180.utah.cloudlab.us"
    "hp166.utah.cloudlab.us"
    "hp070.utah.cloudlab.us"
    "hp185.utah.cloudlab.us"
    "hp177.utah.cloudlab.us"
    "hp042.utah.cloudlab.us"
    "hp179.utah.cloudlab.us"
    "hp073.utah.cloudlab.us"
    "hp196.utah.cloudlab.us"
)

NODE_NAMES=(
    "node0" "node1" "node2" "node3" "node4" "node5"
    "node6" "node7" "node8" "node9" "node10" "node11"
)

SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=10 -o BatchMode=yes"

log() {
    echo "[$(date '+%H:%M:%S')] $*"
}

log "Killing all ldc processes on all ${#NODES[@]} nodes..."

for idx in "${!NODES[@]}"; do
    host="${NODES[$idx]}"
    node_name="${NODE_NAMES[$idx]}"

    log "  ${node_name} (${host})..."
    ssh ${SSH_OPTS} -i "${ADMIN_KEY}" "${ADMIN_USER}@${host}" \
        "sudo pkill -9 ldc 2>/dev/null; echo '    Killed'" \
        || log "    No ldc processes on ${node_name}"
done

log "=== Finished ==="
