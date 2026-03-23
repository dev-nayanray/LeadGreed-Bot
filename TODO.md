# LeadGreed-Bot Deployment TODO

## Status: [IN PROGRESS] ⏳

### 1. [DONE] ✅ Create TODO.md
### 2. [MANUAL] 🚀 Deploy Commands (Copy-paste to PowerShell/Git Bash)
   ```
   scp config.py root@64.23.224.233:/root/LeadGreed-Bot/
   scp *.py *.service root@64.23.224.233:/root/LeadGreed-Bot/
   ssh root@64.23.224.233 "cd /root/LeadGreed-Bot && git pull origin main && mv *.service /etc/systemd/system/ && systemctl daemon-reload && . venv/bin/activate && pip install --upgrade anthropic==0.34.1 httpx==0.27.0 playwright python-telegram-bot && playwright install --with-deps chromium && systemctl enable leadgreed-bot leadgreed-dashboard && systemctl restart leadgreed-bot leadgreed-dashboard && journalctl -u leadgreed-bot -f --no-pager -l | head -20"
   ```
   **Status: WAITING USER INPUT**

### 3. [PENDING] 🔍 Verify services
   ```
   ssh root@64.23.224.233
   systemctl status leadgreed-bot leadgreed-dashboard
   ```

### 4. [PENDING] 🧪 Test bot & dashboard
   - Send Telegram test command
   - Dashboard: http://64.23.224.233:5000

### 5. [PENDING] 🧹 Cleanup old PM2 processes (optional)
   ```
   pm2 delete autob2026-bot autob2026-dashboard
   ```

**Run `bash deploy-config.sh` next.**

