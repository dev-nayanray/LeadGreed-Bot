# OOM + Service Fixes

- [ ] VPS: `scp *.service /root/LeadGreed-Bot/`
- [ ] VPS: `mv *.service /etc/systemd/system/`
- [ ] VPS: `systemctl daemon-reload`
- [ ] VPS: `systemctl enable leadgreed-bot leadgreed-dashboard`
- [ ] VPS: `systemctl restart leadgreed-bot leadgreed-dashboard`
- [ ] Test: Telegram "Helios Indonesia cap" → full response no OOM
- [ ] Dashboard: http://64.23.224.233:5000 works, logs table visible

