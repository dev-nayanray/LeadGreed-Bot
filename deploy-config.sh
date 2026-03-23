#!/bin/bash
# Deploy config to VPS
scp config.py root@64.23.224.233:/root/LeadGreed-Bot/
ssh root@64.23.224.233 "cd /root/LeadGreed-Bot && systemctl restart leadgreed-bot"
echo "✅ Config deployed & restarted"
