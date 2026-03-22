"""
action_log.py — Логирование действий бота в SQLite
"""

import sqlite3
import datetime
import threading
import os

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot_actions.db")

_local = threading.local()


def _get_conn() -> sqlite3.Connection:
    """Получить connection для текущего потока."""
    if not hasattr(_local, "conn") or _local.conn is None:
        _local.conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        _local.conn.row_factory = sqlite3.Row
    return _local.conn


def init_db():
    """Создать таблицы если не существуют."""
    conn = _get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS action_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            action TEXT NOT NULL,
            broker_id TEXT,
            details TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            result TEXT,
            user_command TEXT
        );

        CREATE TABLE IF NOT EXISTS bot_status (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_log_timestamp ON action_log(timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_log_action ON action_log(action);
    """)
    conn.commit()


def log_action(action: str, broker_id: str = None, details: str = None,
               status: str = "pending", result: str = None, user_command: str = None) -> int:
    """Записать действие в лог. Возвращает ID записи."""
    conn = _get_conn()
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cur = conn.execute(
        "INSERT INTO action_log (timestamp, action, broker_id, details, status, result, user_command) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (now, action, broker_id, details, status, result, user_command)
    )
    conn.commit()
    return cur.lastrowid


def update_action(log_id: int, status: str, result: str = None):
    """Обновить статус действия."""
    conn = _get_conn()
    conn.execute(
        "UPDATE action_log SET status = ?, result = ? WHERE id = ?",
        (status, result, log_id)
    )
    conn.commit()


def set_status(key: str, value: str):
    """Установить значение статуса бота."""
    conn = _get_conn()
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn.execute(
        "INSERT OR REPLACE INTO bot_status (key, value, updated_at) VALUES (?, ?, ?)",
        (key, value, now)
    )
    conn.commit()


def get_status(key: str) -> dict:
    """Получить значение статуса."""
    conn = _get_conn()
    row = conn.execute("SELECT value, updated_at FROM bot_status WHERE key = ?", (key,)).fetchone()
    if row:
        return {"value": row["value"], "updated_at": row["updated_at"]}
    return None


def get_recent_actions(limit: int = 50) -> list:
    """Получить последние действия."""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT * FROM action_log ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()
    return [dict(r) for r in rows]


def get_stats() -> dict:
    """Статистика за сегодня и всего."""
    conn = _get_conn()
    today = datetime.datetime.now().strftime("%Y-%m-%d")

    total = conn.execute("SELECT COUNT(*) as c FROM action_log").fetchone()["c"]
    today_count = conn.execute(
        "SELECT COUNT(*) as c FROM action_log WHERE timestamp LIKE ?", (f"{today}%",)
    ).fetchone()["c"]
    success = conn.execute(
        "SELECT COUNT(*) as c FROM action_log WHERE status = 'success'"
    ).fetchone()["c"]
    errors = conn.execute(
        "SELECT COUNT(*) as c FROM action_log WHERE status = 'error'"
    ).fetchone()["c"]

    return {
        "total": total,
        "today": today_count,
        "success": success,
        "errors": errors,
    }


# Инициализация при импорте
init_db()
