#!/bin/bash
# Deploy config + fix deps + restart
scp config.py root@64.23.224.233:/root/LeadGreed-Bot/
ssh root@64.23.224.233 "
cd /root/LeadGreed-Bot &&
git pull origin main &&
scp ~/LeadGreed-Bot/*.py ~/LeadGreed-Bot/*.service ecosystem.config.js . || true &&
mv *.service /etc/systemd/system/ || true &&
systemctl daemon-reload &&
. venv/bin/activate &&
pip install --upgrade anthropic==0.34.1 httpx==0.27.0 playwright python-telegram-bot &&
playwright install --with-deps chromium &&
pm2 delete autob2026-bot autob2026-dashboard || true && pm2 start ecosystem.config.js && pm2 save &&
systemctl restart leadgreed-bot leadgreed-dashboard &&
pm2 status && pm2 logs LeadGreed-Bot --lines 20
"
echo "✅ Deployed config, fixed deps, restarted bot"
