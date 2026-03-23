#!/bin/bash
# Deploy config + fix deps + restart
scp config.py root@64.23.224.233:/root/LeadGreed-Bot/
ssh root@64.23.224.233 "
cd /root/LeadGreed-Bot &&
cp ~/LeadGreed-Bot/action_log.py . || true &&
. venv/bin/activate &&
pip install --upgrade anthropic httpx[http2] playwright &&
playwright install chromium &&
systemctl restart leadgreed-bot &&
systemctl status leadgreed-bot --no-pager -l
"
echo "✅ Deployed config, fixed deps, restarted bot"
