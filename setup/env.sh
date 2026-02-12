#!/bin/bash

# Shared configuration for all setup scripts.
# Edit the USERNAME below and all scripts will pick it up.

USERNAME_RAW="reviewer1"

PROGRAM_PATH="sudo python3 setup.py"
IDENTITY_FILE="--identity_file /users/${USERNAME_RAW}/.ssh/id_rsa"
USERNAME="--username ${USERNAME_RAW}"
GIT_SSH_KEY_PATH="--git_ssh_key_path /users/${USERNAME_RAW}/.ssh/id_rsa"
NUM_SERVERS="--num_servers 3"
GIT_BRANCH="EUROSYS"
