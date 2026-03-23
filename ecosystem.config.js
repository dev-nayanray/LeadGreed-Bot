module.exports = {
  apps: [{
    name: 'LeadGreed-Bot',
    script: 'main.py',
    interpreter: 'python3',
    cwd: '/root/LeadGreed-Bot',
    env: {
      PLAYWRIGHT_BROWSERS_PATH: '/tmp/playwright',
      PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD: 1
    },
    max_memory_restart: '3G',
    error_file: '/root/LeadGreed-Bot/bot.log',
    out_file: '/root/LeadGreed-Bot/bot.out.log',
    log_file: '/root/LeadGreed-Bot/bot.pm2.log',
    time: true
  }, {
    name: 'LeadGreed-Dashboard',
    script: 'dashboard.py',
    interpreter: 'python3',
    cwd: '/root/LeadGreed-Bot',
    instances: 1,
    max_memory_restart: '512M',
    error_file: '/root/LeadGreed-Bot/dashboard.log',
    out_file: '/root/LeadGreed-Bot/dashboard.out.log',
    log_file: '/root/LeadGreed-Bot/dashboard.pm2.log',
    time: true
  }]
};

