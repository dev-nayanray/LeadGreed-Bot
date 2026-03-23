#!/bin/bash
# Deploy config + fix deps + restart
scp config.py root@64.23.224.233:/root/LeadGreed-Bot/
ssh root@64.23.224.233 "
cd /root/LeadGreed-Bot &&
git pull origin main &&
scp ~/LeadGreed-Bot/*.py ~/LeadGreed-Bot/*.service . || true &&
mv *.service /etc/systemd/system/ || true &&
systemctl daemon-reload &&
. venv/bin/activate &&
pip install --upgrade anthropic==0.34.1 httpx==0.27.0 playwright python-telegram-bot &&
playwright install --with-deps chromium &&
systemctl enable leadgreed-bot leadgreed-dashboard &&
systemctl restart leadgreed-bot leadgreed-dashboard &&
journalctl -u leadgreed-bot -f --no-pager -l | head -20
"
echo "✅ Deployed config, fixed deps, restarted bot"
