#!/usr/bin/env python3
"""
dashboard.py — Дашборд для CRM-бота
Запуск: python dashboard.py (порт 5000)
"""

import json
from flask import Flask, render_template_string, jsonify
from action_log import get_recent_actions, get_stats, get_status

app = Flask(__name__)

DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>LeadGreed Bot — Dashboard</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=DM+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }

        :root {
            --bg: #0a0a0f;
            --surface: #12121a;
            --surface-2: #1a1a26;
            --border: #2a2a3a;
            --text: #e4e4ef;
            --text-dim: #7a7a8f;
            --accent: #6c5ce7;
            --accent-glow: rgba(108, 92, 231, 0.15);
            --green: #2ed573;
            --green-bg: rgba(46, 213, 115, 0.1);
            --red: #ff4757;
            --red-bg: rgba(255, 71, 87, 0.1);
            --yellow: #ffa502;
            --yellow-bg: rgba(255, 165, 2, 0.1);
            --blue: #45aaf2;
        }

        body {
            font-family: 'DM Sans', sans-serif;
            background: var(--bg);
            color: var(--text);
            min-height: 100vh;
        }

        /* Header */
        .header {
            padding: 28px 40px;
            border-bottom: 1px solid var(--border);
            display: flex;
            align-items: center;
            justify-content: space-between;
            background: var(--surface);
        }
        .header-left {
            display: flex;
            align-items: center;
            gap: 14px;
        }
        .logo {
            width: 38px;
            height: 38px;
            background: var(--accent);
            border-radius: 10px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 18px;
            font-weight: 700;
            color: white;
            box-shadow: 0 0 20px var(--accent-glow);
        }
        .header h1 {
            font-size: 20px;
            font-weight: 600;
            letter-spacing: -0.3px;
        }
        .header h1 span { color: var(--text-dim); font-weight: 400; }
        .header-right {
            display: flex;
            align-items: center;
            gap: 20px;
        }
        .status-pill {
            display: flex;
            align-items: center;
            gap: 8px;
            padding: 6px 14px;
            border-radius: 20px;
            font-size: 13px;
            font-weight: 500;
        }
        .status-pill.online {
            background: var(--green-bg);
            color: var(--green);
            border: 1px solid rgba(46, 213, 115, 0.2);
        }
        .status-pill.offline {
            background: var(--red-bg);
            color: var(--red);
            border: 1px solid rgba(255, 71, 87, 0.2);
        }
        .status-dot {
            width: 8px; height: 8px;
            border-radius: 50%;
            background: currentColor;
            animation: pulse 2s ease-in-out infinite;
        }
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.4; }
        }
        .refresh-btn {
            background: var(--surface-2);
            border: 1px solid var(--border);
            color: var(--text-dim);
            padding: 8px 16px;
            border-radius: 8px;
            cursor: pointer;
            font-size: 13px;
            font-family: inherit;
            transition: all 0.2s;
        }
        .refresh-btn:hover {
            border-color: var(--accent);
            color: var(--text);
            background: var(--accent-glow);
        }

        /* Main content */
        .main {
            max-width: 1280px;
            margin: 0 auto;
            padding: 32px 40px;
        }

        /* Stats grid */
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 16px;
            margin-bottom: 32px;
        }
        .stat-card {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 20px 24px;
            transition: border-color 0.2s;
        }
        .stat-card:hover { border-color: var(--accent); }
        .stat-label {
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: 1px;
            color: var(--text-dim);
            margin-bottom: 8px;
        }
        .stat-value {
            font-family: 'JetBrains Mono', monospace;
            font-size: 32px;
            font-weight: 600;
        }
        .stat-value.green { color: var(--green); }
        .stat-value.red { color: var(--red); }
        .stat-value.blue { color: var(--blue); }

        /* Status bar */
        .status-bar {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 16px;
            margin-bottom: 32px;
        }
        .status-item {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 16px 20px;
            display: flex;
            align-items: center;
            gap: 12px;
        }
        .status-icon {
            width: 36px; height: 36px;
            border-radius: 8px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 16px;
            flex-shrink: 0;
        }
        .status-icon.purple { background: var(--accent-glow); }
        .status-icon.green-bg { background: var(--green-bg); }
        .status-icon.blue-bg { background: rgba(69, 170, 242, 0.1); }
        .status-info .label { font-size: 11px; color: var(--text-dim); text-transform: uppercase; letter-spacing: 0.8px; }
        .status-info .value { font-size: 14px; font-weight: 500; margin-top: 2px; }
        .status-info .value.mono { font-family: 'JetBrains Mono', monospace; font-size: 13px; }

        /* Log section */
        .log-section {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 12px;
            overflow: hidden;
        }
        .log-header {
            padding: 18px 24px;
            border-bottom: 1px solid var(--border);
            display: flex;
            align-items: center;
            justify-content: space-between;
        }
        .log-header h2 {
            font-size: 15px;
            font-weight: 600;
            display: flex;
            align-items: center;
            gap: 8px;
        }
        .log-count {
            font-family: 'JetBrains Mono', monospace;
            font-size: 11px;
            background: var(--surface-2);
            padding: 3px 8px;
            border-radius: 4px;
            color: var(--text-dim);
        }
        .log-filters {
            display: flex;
            gap: 6px;
        }
        .filter-btn {
            background: none;
            border: 1px solid var(--border);
            color: var(--text-dim);
            padding: 5px 12px;
            border-radius: 6px;
            cursor: pointer;
            font-size: 12px;
            font-family: inherit;
            transition: all 0.15s;
        }
        .filter-btn:hover, .filter-btn.active {
            border-color: var(--accent);
            color: var(--text);
            background: var(--accent-glow);
        }

        /* Log table */
        .log-table {
            width: 100%;
            border-collapse: collapse;
        }
        .log-table th {
            text-align: left;
            padding: 12px 20px;
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 0.8px;
            color: var(--text-dim);
            background: var(--surface-2);
            border-bottom: 1px solid var(--border);
            font-weight: 500;
        }
        .log-table td {
            padding: 12px 20px;
            font-size: 13px;
            border-bottom: 1px solid rgba(42, 42, 58, 0.5);
            vertical-align: top;
        }
        .log-table tr:last-child td { border-bottom: none; }
        .log-table tr:hover { background: rgba(108, 92, 231, 0.03); }
        .log-table .time {
            font-family: 'JetBrains Mono', monospace;
            font-size: 12px;
            color: var(--text-dim);
            white-space: nowrap;
        }

        /* Action badge */
        .action-badge {
            display: inline-block;
            padding: 3px 10px;
            border-radius: 4px;
            font-size: 11px;
            font-family: 'JetBrains Mono', monospace;
            font-weight: 600;
            white-space: nowrap;
        }
        .action-badge.get { background: rgba(69, 170, 242, 0.1); color: var(--blue); }
        .action-badge.add { background: var(--green-bg); color: var(--green); }
        .action-badge.change { background: var(--yellow-bg); color: var(--yellow); }
        .action-badge.toggle { background: var(--accent-glow); color: #a29bfe; }
        .action-badge.close { background: var(--red-bg); color: var(--red); }
        .action-badge.error { background: var(--red-bg); color: var(--red); }

        /* Status badges */
        .status-badge {
            display: inline-flex;
            align-items: center;
            gap: 5px;
            font-size: 12px;
            font-weight: 500;
        }
        .status-badge.success { color: var(--green); }
        .status-badge.error { color: var(--red); }
        .status-badge.pending { color: var(--yellow); }

        .td-details {
            max-width: 300px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
            color: var(--text-dim);
            font-size: 12px;
        }
        .td-result {
            max-width: 250px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
            font-size: 12px;
        }
        .td-command {
            max-width: 200px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
            color: var(--text-dim);
            font-family: 'JetBrains Mono', monospace;
            font-size: 11px;
        }

        .empty-state {
            text-align: center;
            padding: 60px 20px;
            color: var(--text-dim);
        }
        .empty-state .icon { font-size: 40px; margin-bottom: 12px; }
        .empty-state p { font-size: 14px; }

        /* Auto-refresh indicator */
        .auto-refresh {
            font-size: 11px;
            color: var(--text-dim);
            display: flex;
            align-items: center;
            gap: 6px;
        }
        .auto-refresh .dot {
            width: 6px; height: 6px;
            background: var(--green);
            border-radius: 50%;
            animation: pulse 3s ease-in-out infinite;
        }

        @media (max-width: 900px) {
            .stats-grid { grid-template-columns: repeat(2, 1fr); }
            .status-bar { grid-template-columns: 1fr; }
            .main { padding: 20px; }
            .header { padding: 20px; }
        }
    </style>
</head>
<body>
    <div class="header">
        <div class="header-left">
            <div class="logo">LG</div>
            <h1>LeadGreed Bot <span>Dashboard</span></h1>
        </div>
        <div class="header-right">
            <div class="auto-refresh">
                <div class="dot"></div>
                Обновление каждые 30с
            </div>
            <div id="botStatus" class="status-pill online">
                <div class="status-dot"></div>
                <span>Online</span>
            </div>
            <button class="refresh-btn" onclick="loadData()">⟳ Обновить</button>
        </div>
    </div>

    <div class="main">
        <!-- Stats -->
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-label">Сегодня</div>
                <div class="stat-value blue" id="statToday">—</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Всего</div>
                <div class="stat-value" id="statTotal">—</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Успешных</div>
                <div class="stat-value green" id="statSuccess">—</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Ошибок</div>
                <div class="stat-value red" id="statErrors">—</div>
            </div>
        </div>

        <!-- Status info -->
        <div class="status-bar">
            <div class="status-item">
                <div class="status-icon purple">🤖</div>
                <div class="status-info">
                    <div class="label">Бот запущен</div>
                    <div class="value mono" id="statusStarted">—</div>
                </div>
            </div>
            <div class="status-item">
                <div class="status-icon green-bg">🔑</div>
                <div class="status-info">
                    <div class="label">Последний логин CRM</div>
                    <div class="value mono" id="statusLogin">—</div>
                </div>
            </div>
            <div class="status-item">
                <div class="status-icon blue-bg">⚡</div>
                <div class="status-info">
                    <div class="label">Последнее действие</div>
                    <div class="value mono" id="statusLast">—</div>
                </div>
            </div>
        </div>

        <!-- Log -->
        <div class="log-section">
            <div class="log-header">
                <h2>
                    Лог действий
                    <span class="log-count" id="logCount">0</span>
                </h2>
                <div class="log-filters">
                    <button class="filter-btn active" onclick="setFilter('all', this)">Все</button>
                    <button class="filter-btn" onclick="setFilter('success', this)">✅ Успех</button>
                    <button class="filter-btn" onclick="setFilter('error', this)">❌ Ошибки</button>
                    <button class="filter-btn" onclick="setFilter('get', this)">🔍 Запросы</button>
                </div>
            </div>
            <div id="logContent">
                <div class="empty-state">
                    <div class="icon">📋</div>
                    <p>Загрузка...</p>
                </div>
            </div>
        </div>
    </div>

    <script>
        let allActions = [];
        let currentFilter = 'all';

        function getActionClass(action) {
            if (action.startsWith('get_')) return 'get';
            if (action.startsWith('add_')) return 'add';
            if (action.startsWith('change_')) return 'change';
            if (action.startsWith('close_')) return 'close';
            if (action === 'toggle_broker') return 'toggle';
            return 'change';
        }

        function getStatusBadge(status) {
            if (status === 'success') return '<span class="status-badge success">✅ OK</span>';
            if (status === 'error') return '<span class="status-badge error">❌ Ошибка</span>';
            return '<span class="status-badge pending">⏳ ...</span>';
        }

        function escapeHtml(str) {
            if (!str) return '—';
            const div = document.createElement('div');
            div.textContent = str;
            return div.innerHTML;
        }

        function renderTable(actions) {
            if (!actions.length) {
                return `<div class="empty-state">
                    <div class="icon">📋</div>
                    <p>Нет записей</p>
                </div>`;
            }

            let html = `<table class="log-table">
                <thead>
                    <tr>
                        <th>Время</th>
                        <th>Действие</th>
                        <th>Брокер</th>
                        <th>Детали</th>
                        <th>Статус</th>
                        <th>Результат</th>
                        <th>Команда</th>
                    </tr>
                </thead>
                <tbody>`;

            actions.forEach(a => {
                html += `<tr>
                    <td class="time">${escapeHtml(a.timestamp)}</td>
                    <td><span class="action-badge ${getActionClass(a.action)}">${escapeHtml(a.action)}</span></td>
                    <td>${escapeHtml(a.broker_id)}</td>
                    <td class="td-details" title="${escapeHtml(a.details)}">${escapeHtml(a.details)}</td>
                    <td>${getStatusBadge(a.status)}</td>
                    <td class="td-result" title="${escapeHtml(a.result)}">${escapeHtml(a.result)}</td>
                    <td class="td-command" title="${escapeHtml(a.user_command)}">${escapeHtml(a.user_command)}</td>
                </tr>`;
            });

            html += '</tbody></table>';
            return html;
        }

        function setFilter(filter, btn) {
            currentFilter = filter;
            document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            applyFilter();
        }

        function applyFilter() {
            let filtered = allActions;
            if (currentFilter === 'success') filtered = allActions.filter(a => a.status === 'success');
            else if (currentFilter === 'error') filtered = allActions.filter(a => a.status === 'error');
            else if (currentFilter === 'get') filtered = allActions.filter(a => a.action.startsWith('get_'));

            document.getElementById('logContent').innerHTML = renderTable(filtered);
            document.getElementById('logCount').textContent = filtered.length;
        }

        async function loadData() {
            try {
                const resp = await fetch('/api/data');
                const data = await resp.json();

                // Stats
                document.getElementById('statToday').textContent = data.stats.today;
                document.getElementById('statTotal').textContent = data.stats.total;
                document.getElementById('statSuccess').textContent = data.stats.success;
                document.getElementById('statErrors').textContent = data.stats.errors;

                // Status
                const st = data.status;
                document.getElementById('statusStarted').textContent = st.bot_started || '—';
                document.getElementById('statusLogin').textContent = st.last_login || '—';
                document.getElementById('statusLast').textContent = st.last_action || '—';

                // Bot online/offline
                const pill = document.getElementById('botStatus');
                if (st.bot_started) {
                    pill.className = 'status-pill online';
                    pill.querySelector('span').textContent = 'Online';
                } else {
                    pill.className = 'status-pill offline';
                    pill.querySelector('span').textContent = 'Offline';
                }

                // Actions
                allActions = data.actions;
                applyFilter();

            } catch (e) {
                console.error('Failed to load:', e);
            }
        }

        // Initial load + auto-refresh
        loadData();
        setInterval(loadData, 30000);
    </script>
</body>
</html>
"""


@app.route("/")
def dashboard():
    return render_template_string(DASHBOARD_HTML)


@app.route("/api/data")
def api_data():
    stats = get_stats()
    actions = get_recent_actions(100)

    # Статус бота
    bot_started = get_status("bot_started")
    last_login = get_status("last_login")
    last_action = get_status("last_action")

    return jsonify({
        "stats": stats,
        "actions": actions,
        "status": {
            "bot_started": bot_started["value"] if bot_started else None,
            "last_login": last_login["value"] if last_login else None,
            "last_action": last_action["value"] if last_action else None,
        }
    })


if __name__ == "__main__":
    print("🚀 Dashboard: http://localhost:5000")
    app.run(host="0.0.0.0", port=5000, debug=False)
