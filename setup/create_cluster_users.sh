#!/bin/bash
#
# create_cluster_users.sh
#
# Creates N user accounts across all CloudLab cluster nodes with:
#   - Passwordless sudo access
#   - SSH key-based authentication (generates id_rsa per user)
#   - Distributes public keys to all nodes
#
# Usage:
#   ./create_cluster_users.sh <num_users> [username_prefix]
#
# Examples:
#   ./create_cluster_users.sh 5              # Creates user1..user5
#   ./create_cluster_users.sh 3 reviewer     # Creates reviewer1..reviewer3
#
# Output:
#   - SSH keys saved to ./generated_keys/<username>/
#   - A credentials summary file: ./generated_keys/credentials.txt
#
# Prerequisites:
#   - Run from a machine that can SSH into all cluster nodes (e.g., as Khombal2)
#   - Your SSH key must be set up for the ADMIN_USER on all nodes

set -euo pipefail

# ─── Configuration ───────────────────────────────────────────────────────────

# Source env.sh to get the admin username and identity file
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/env.sh"

ADMIN_USER="${USERNAME_RAW}"
ADMIN_KEY="/users/${ADMIN_USER}/.ssh/id_rsa"

# All cluster node hostnames (parsed from cloudlab_machines.xml or hardcoded)
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

# ─── Argument Parsing ────────────────────────────────────────────────────────

if [ $# -lt 1 ]; then
    echo "Usage: $0 <num_users> [username_prefix]"
    echo "  num_users       - Number of user accounts to create (e.g., 5)"
    echo "  username_prefix - Optional prefix (default: 'user'). Users will be named prefix1, prefix2, ..."
    exit 1
fi

NUM_USERS="$1"
PREFIX="${2:-user}"
KEY_DIR="${SCRIPT_DIR}/generated_keys"

mkdir -p "${KEY_DIR}"

# ─── Helper Functions ────────────────────────────────────────────────────────

run_on_node() {
    local host="$1"
    shift
    ssh ${SSH_OPTS} -i "${ADMIN_KEY}" "${ADMIN_USER}@${host}" "$@"
}

run_on_node_sudo() {
    local host="$1"
    shift
    ssh ${SSH_OPTS} -i "${ADMIN_KEY}" "${ADMIN_USER}@${host}" "sudo bash -c '$*'"
}

log() {
    echo "[$(date '+%H:%M:%S')] $*"
}

# ─── Step 1: Generate SSH Key Pairs Locally ──────────────────────────────────

log "=== Step 1: Generating SSH key pairs for ${NUM_USERS} users ==="

USERNAMES=()
for i in $(seq 1 "${NUM_USERS}"); do
    USERNAME="${PREFIX}${i}"
    USERNAMES+=("${USERNAME}")
    USER_KEY_DIR="${KEY_DIR}/${USERNAME}"
    mkdir -p "${USER_KEY_DIR}"

    if [ -f "${USER_KEY_DIR}/id_rsa" ]; then
        log "  Key already exists for ${USERNAME}, skipping generation"
    else
        log "  Generating SSH keypair for ${USERNAME}..."
        ssh-keygen -t rsa -b 4096 -f "${USER_KEY_DIR}/id_rsa" -N "" -C "${USERNAME}@ldc-cluster" -q
        log "  Created ${USER_KEY_DIR}/id_rsa"
    fi
done

# ─── Step 2: Create Users on All Nodes ───────────────────────────────────────

log "=== Step 2: Creating users on all ${#NODES[@]} nodes ==="

for idx in "${!NODES[@]}"; do
    host="${NODES[$idx]}"
    node_name="${NODE_NAMES[$idx]}"
    log "  Setting up ${node_name} (${host})..."

    for USERNAME in "${USERNAMES[@]}"; do
        USER_KEY_DIR="${KEY_DIR}/${USERNAME}"
        PUB_KEY=$(cat "${USER_KEY_DIR}/id_rsa.pub")

        # Create user, home dir, sudo access, and SSH authorized_keys in one SSH call
        run_on_node "${host}" "sudo bash -s" <<REMOTE_SCRIPT
            # Create user if not exists
            if ! id "${USERNAME}" &>/dev/null; then
                useradd -m -s /bin/bash -d /users/${USERNAME} "${USERNAME}"
                echo "    [${node_name}] Created user ${USERNAME}"
            else
                echo "    [${node_name}] User ${USERNAME} already exists"
            fi

            # Grant passwordless sudo
            echo "${USERNAME} ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/${USERNAME}
            chmod 440 /etc/sudoers.d/${USERNAME}

            # Set up SSH directory and authorized_keys
            mkdir -p /users/${USERNAME}/.ssh
            chmod 700 /users/${USERNAME}/.ssh

            # Add public key (idempotent - check if already present)
            touch /users/${USERNAME}/.ssh/authorized_keys
            if ! grep -qF "${PUB_KEY}" /users/${USERNAME}/.ssh/authorized_keys 2>/dev/null; then
                echo "${PUB_KEY}" >> /users/${USERNAME}/.ssh/authorized_keys
            fi

            chmod 600 /users/${USERNAME}/.ssh/authorized_keys
            chown -R ${USERNAME}:${USERNAME} /users/${USERNAME}/.ssh

            # Create an SSH config for the user to access other cluster nodes
            cat > /users/${USERNAME}/.ssh/config <<'SSHCONF'
Host *
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null
    LogLevel ERROR
SSHCONF
            chmod 600 /users/${USERNAME}/.ssh/config
            chown ${USERNAME}:${USERNAME} /users/${USERNAME}/.ssh/config
REMOTE_SCRIPT
    done

    log "  Done with ${node_name}"
done

# ─── Step 3: Distribute Private Keys to All Nodes ────────────────────────────
# This allows users to SSH between cluster nodes once they're logged in.

log "=== Step 3: Distributing private keys across nodes (for inter-node SSH) ==="

for idx in "${!NODES[@]}"; do
    host="${NODES[$idx]}"
    node_name="${NODE_NAMES[$idx]}"

    for USERNAME in "${USERNAMES[@]}"; do
        USER_KEY_DIR="${KEY_DIR}/${USERNAME}"

        # Copy the private key to the user's .ssh on this node
        scp ${SSH_OPTS} -i "${ADMIN_KEY}" \
            "${USER_KEY_DIR}/id_rsa" \
            "${ADMIN_USER}@${host}:/tmp/${USERNAME}_id_rsa"

        run_on_node "${host}" "sudo bash -s" <<REMOTE_SCRIPT
            cp /tmp/${USERNAME}_id_rsa /users/${USERNAME}/.ssh/id_rsa
            chmod 600 /users/${USERNAME}/.ssh/id_rsa
            chown ${USERNAME}:${USERNAME} /users/${USERNAME}/.ssh/id_rsa
            rm -f /tmp/${USERNAME}_id_rsa
REMOTE_SCRIPT
    done

    log "  Distributed keys to ${node_name}"
done

# ─── Step 4: Give users access to /mydata ────────────────────────────────────

log "=== Step 4: Granting /mydata access on all nodes ==="

for idx in "${!NODES[@]}"; do
    host="${NODES[$idx]}"
    node_name="${NODE_NAMES[$idx]}"

    for USERNAME in "${USERNAMES[@]}"; do
        run_on_node "${host}" "sudo bash -s" <<REMOTE_SCRIPT
            # Create a workspace for the user under /mydata
            mkdir -p /mydata/${USERNAME}
            chown ${USERNAME}:${USERNAME} /mydata/${USERNAME}
REMOTE_SCRIPT
    done

    log "  Granted /mydata access on ${node_name}"
done

# ─── Step 5: Generate Credentials File ──────────────────────────────────────

CREDS_FILE="${KEY_DIR}/credentials.txt"

log "=== Step 5: Writing credentials to ${CREDS_FILE} ==="

cat > "${CREDS_FILE}" <<HEADER
============================================================
  LDC Cluster Access Credentials
  Generated: $(date)
============================================================

CLUSTER NODES:
HEADER

for idx in "${!NODES[@]}"; do
    echo "  ${NODE_NAMES[$idx]}: ${NODES[$idx]}" >> "${CREDS_FILE}"
done

cat >> "${CREDS_FILE}" <<MIDDLE

CONNECT INSTRUCTIONS:
  ssh -i <path_to_id_rsa> <username>@<hostname>
  Example: ssh -i ./id_rsa user1@hp058.utah.cloudlab.us

  Once on a node, you can SSH to any other node:
  ssh <other_hostname>

  You have sudo access (passwordless) on all nodes.
  Your workspace is at /mydata/<username>/ on each node.

------------------------------------------------------------
USER CREDENTIALS:
------------------------------------------------------------
MIDDLE

for USERNAME in "${USERNAMES[@]}"; do
    cat >> "${CREDS_FILE}" <<ENTRY

Username: ${USERNAME}
Private Key: ${KEY_DIR}/${USERNAME}/id_rsa
SSH Command (node0): ssh -i ${KEY_DIR}/${USERNAME}/id_rsa ${USERNAME}@${NODES[0]}
ENTRY
done

cat >> "${CREDS_FILE}" <<FOOTER

============================================================
SECURITY NOTES:
  - Each user has a unique SSH keypair
  - Share only the id_rsa (private key) + username with the user
  - To revoke access, run: ./remove_cluster_users.sh <username>
============================================================
FOOTER

log ""
log "=== DONE ==="
log "Created ${NUM_USERS} users (${USERNAMES[*]}) across ${#NODES[@]} nodes"
log ""
log "Keys and credentials saved to: ${KEY_DIR}/"
log "Credentials summary: ${CREDS_FILE}"
log ""
log "To share with a user, give them:"
log "  1. Their private key:  ${KEY_DIR}/<username>/id_rsa"
log "  2. Their username"
log "  3. Node hostnames from the credentials file"
