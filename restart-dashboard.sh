#!/bin/bash
set -euo pipefail

VPS_HOST="root@64.23.224.233"
REMOTE_DIR="/root/LeadGreed-Bot"

ssh "$VPS_HOST" "cd $REMOTE_DIR && pkill -f 'python dashboard.py' || true && nohup ./venv/bin/python dashboard.py > dashboard.log 2>&1 < /dev/null &"

echo "Dashboard restarted on $VPS_HOST"
