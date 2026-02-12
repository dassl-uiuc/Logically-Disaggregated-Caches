#!/bin/bash
#
# remove_cluster_users.sh
#
# Removes user accounts from all cluster nodes.
#
# Usage:
#   ./remove_cluster_users.sh <username>            # Remove a single user
#   ./remove_cluster_users.sh <num_users> [prefix]  # Remove prefix1..prefixN

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

SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=10 -o BatchMode=yes"

log() {
    echo "[$(date '+%H:%M:%S')] $*"
}

# Build list of usernames to remove
USERNAMES=()
if [[ "$1" =~ ^[0-9]+$ ]]; then
    NUM_USERS="$1"
    PREFIX="${2:-user}"
    for i in $(seq 1 "${NUM_USERS}"); do
        USERNAMES+=("${PREFIX}${i}")
    done
else
    USERNAMES+=("$1")
fi

echo "Will remove users: ${USERNAMES[*]}"
echo "From ${#NODES[@]} nodes"
read -p "Are you sure? [y/N] " confirm
if [[ "${confirm}" != "y" && "${confirm}" != "Y" ]]; then
    echo "Aborted."
    exit 0
fi

for host in "${NODES[@]}"; do
    log "Processing ${host}..."
    for USERNAME in "${USERNAMES[@]}"; do
        ssh ${SSH_OPTS} -i "${ADMIN_KEY}" "${ADMIN_USER}@${host}" "sudo bash -s" <<REMOTE_SCRIPT
            if id "${USERNAME}" &>/dev/null; then
                # Kill any running processes
                pkill -u ${USERNAME} 2>/dev/null || true
                # Remove user and home directory
                userdel -r ${USERNAME} 2>/dev/null || true
                # Remove sudoers file
                rm -f /etc/sudoers.d/${USERNAME}
                # Remove /mydata workspace
                rm -rf /mydata/${USERNAME}
                echo "  Removed ${USERNAME}"
            else
                echo "  ${USERNAME} not found, skipping"
            fi
REMOTE_SCRIPT
    done
done

# Clean up local keys
KEY_DIR="${SCRIPT_DIR}/generated_keys"
for USERNAME in "${USERNAMES[@]}"; do
    if [ -d "${KEY_DIR}/${USERNAME}" ]; then
        rm -rf "${KEY_DIR}/${USERNAME}"
        log "Removed local keys for ${USERNAME}"
    fi
done

log "Done. Users removed from all nodes."
