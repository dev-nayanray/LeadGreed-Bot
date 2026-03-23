# LeadGreed-Bot Fresh Deployment TODO

## Status: [IN PROGRESS] 🚀 DEPLOYING...

### ✅ 1. Files Updated
- [x] requirements.txt → anthropic==0.34.1 httpx==0.27.0 (fixes TypeError)
- [x] TODO.md → Fresh plan created

### 🔄 2. Execute Deploy (Auto)
```
bash deploy-config.sh
```
**Expected**: Copies code/config, pip upgrade, kills old PM2 autob2026-*, restarts leadgreed-bot/dashboard services

### ⏳ 3. [PENDING] Verify Services
```
ssh root@64.23.224.233 "
echo '=== Systemd Status ==='
systemctl status leadgreed-bot leadgreed-dashboard --no-pager

echo '=== PM2 Status ==='
pm2 status

echo '=== Bot Logs (last 20) ==='
journalctl -u leadgreed-bot -f -l --no-pager | head -20
"
```

### ⏳ 4. [PENDING] Test
- [ ] Telegram: `/start` → "👋 Hi! I'm the LeadGreed CRM bot."
- [ ] Command: "Nexus FR hours" → No Anthropic error
- [ ] Dashboard: `http://64.23.224.233:5000` → Live logs/stats

### ⏳ 5. [PENDING] Monitor
```
ssh root@64.23.224.233 "journalctl -u leadgreed-bot -f"
```
**Success**: "Bot started ✅" + No 'proxies' errors + No OOM

**Next**: Reply with deploy output → I'll verify + complete TODO.md

