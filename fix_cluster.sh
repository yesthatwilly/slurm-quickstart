#!/bin/bash
set -euo pipefail

# --- Guard: don't run this wrapper as root ---
if [ "${EUID:-$(id -u)}" -eq 0 ]; then
  echo "ERROR: Do not run this script as root."
  echo "Run as the normal user 'rocky'"
  exit 1
fi

echo "=== Step 05: Update slurm.conf ==="
bash ./05_update_slurmconf

echo "=== Step 06: Enable slurmdbd (runs as sudo) ==="
sudo bash ./06_enable_slurmdbd

echo "=== Step 07: Enable slurmctld (runs as sudo) ==="
sudo bash ./07_enable_slurmctld

echo "=== Step 08: Deploy compute nodes (runs as sudo) ==="
sudo bash ./08_deploy_compute
