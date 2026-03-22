#!/usr/bin/env python3
"""
LeadGreed CRM Bot
Управляй своей CRM через Telegram
"""

import asyncio
import atexit
import datetime
import json
import logging
import re
import signal
from typing import Optional

import anthropic
from playwright.async_api import async_playwright, Page, Browser, BrowserContext
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, filters, ContextTypes
)

# ─────────────────────────────────────────
#  НАСТРОЙКИ — заполни перед запуском!
# ─────────────────────────────────────────
from config import (
    CRM_URL, CRM_EMAIL, CRM_PASSWORD,
    TELEGRAM_TOKEN, ANTHROPIC_API_KEY, ALLOWED_CHAT_ID
)
import action_log as alog
# ─────────────────────────────────────────

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO
)
log = logging.getLogger(__name__)

# ══════════════════════════════════════════
#  СОСТОЯНИЕ БРАУЗЕРА
# ══════════════════════════════════════════

_playwright = None
_browser: Optional[Browser] = None
_context: Optional[BrowserContext] = None
_page: Optional[Page] = None

# Хранилище ожидающих подтверждения команд
pending: dict = {}


# ══════════════════════════════════════════
#  AI — разбор команды пользователя
# ══════════════════════════════════════════

SYSTEM_PROMPT = """
Ты парсер команд для бота управления CRM LeadGreed.
Получаешь команду на русском языке и возвращаешь ТОЛЬКО JSON — без пояснений, без markdown.

Возможные action:
- change_hours  — изменить часы работы брокера
- add_hours     — добавить часы для новой страны
- close_days    — закрыть конкретные дни (убрать галочки) для страны
- add_revenue   — добавить прайс/выплату для страны брокера
- toggle_broker — включить / выключить брокера
- add_affiliate_revenue — добавить прайс/выплату для аффилиата
- get_affiliate_revenue — узнать прайс аффилиата для страны
- get_broker_revenue   — узнать прайс брокера для страны
- get_hours            — узнать текущие часы работы брокера для страны
- unknown              — команда непонятна

Структура JSON (общая):
{
  "action": "change_hours",
  "broker_ids": ["32", "Test Broker"],
  "hours": {"start": "10:00", "end": "19:00"},
  "days_to_keep": ["all"],
  "days_to_close": [],
  "countries": ["all"],
  "country_hours": [],
  "countries_days": [],
  "schedule_groups": [],
  "skip_missing": false,
  "country_revenues": [],
  "queries": [],
  "affiliate_id": null,
  "no_traffic": true,
  "active": null,
  "amount": null
}

Для action = "add_hours" используй поле country_hours (список стран с индивидуальными часами):
{
  "action": "add_hours",
  "broker_ids": ["32"],
  "country_hours": [
    {"country": "Belgium", "start": "10:00", "end": "19:00"},
    {"country": "Brazil",  "start": "15:00", "end": "23:00"},
    {"country": "Slovenia","start": "09:00", "end": "21:00"}
  ],
  "days_to_keep": ["Monday","Tuesday","Wednesday","Thursday","Friday"],
  "no_traffic": true
}
Для add_hours с одной страной — тоже country_hours, список из одного элемента.

Правила:
- Если пользователь не уточняет дни — по умолчанию days_to_keep: ["Monday","Tuesday","Wednesday","Thursday","Friday"] (только рабочие дни)
- Если не указана страна — countries: ["all"]
- no_traffic по умолчанию true
- Названия стран могут быть написаны с опечатками, на русском, или как ISO код (2 буквы). Всегда переводи в английское полное название.
- ВАЖНО: Никогда не склеивай имя брокера/аффилиата и страну в одно поле. ISO коды (DE, FR, ES...) и названия стран (германия, испания...) — это ВСЕГДА countries, а не часть broker_ids. Пример: "легион де" → broker_ids: ["Legion"], countries: ["Germany"], а НЕ broker_ids: ["Legion DE"].
- Названия брокеров и аффилиатов могут быть написаны кириллицей — транслитерируй в латиницу. Примеры: "мн"→"MN", "нексус"→"Nexus", "марси"→"Marsi", "фара"→"Farah", "капитан"→"Capitan", "ройбис"→"RoiBees", "финтрикс"→"Fintrix". Общее правило транслитерации: м→M, н→N, к→K, с→S, р→R и т.д. Сохраняй регистр как в оригинальном названии если известно, иначе используй Title Case. Примеры: "белигия"→"Belgium", "аргентина"→"Argentina", "KE"→"Kenya", "NG"→"Nigeria", "DE"→"Germany", "UK"→"United Kingdom", "IT"→"Italy", "FR"→"France", "ES"→"Spain", "PL"→"Poland", "RO"→"Romania", "HU"→"Hungary", "CZ"→"Czech Republic", "PT"→"Portugal", "GR"→"Greece", "SE"→"Sweden", "NO"→"Norway", "FI"→"Finland", "DK"→"Denmark", "NL"→"Netherlands", "BE"→"Belgium", "AT"→"Austria", "CH"→"Switzerland", "TR"→"Turkey", "IL"→"Israel", "AE"→"United Arab Emirates", "SA"→"Saudi Arabia", "ZA"→"South Africa", "EG"→"Egypt", "MA"→"Morocco", "GH"→"Ghana", "TZ"→"Tanzania", "UG"→"Uganda", "ET"→"Ethiopia", "IN"→"India", "PK"→"Pakistan", "BD"→"Bangladesh", "ID"→"Indonesia", "TH"→"Thailand", "VN"→"Vietnam", "PH"→"Philippines", "MY"→"Malaysia", "SG"→"Singapore", "JP"→"Japan", "KR"→"South Korea", "CN"→"China", "AU"→"Australia", "NZ"→"New Zealand", "CA"→"Canada", "MX"→"Mexico", "CO"→"Colombia", "PE"→"Peru", "CL"→"Chile", "VE"→"Venezuela", "EC"→"Ecuador", "BO"→"Bolivia", "PY"→"Paraguay", "UY"→"Uruguay", "CR"→"Costa Rica", "DO"→"Dominican Republic", "GT"→"Guatemala", "HN"→"Honduras", "SV"→"El Salvador", "NI"→"Nicaragua", "PA"→"Panama", "CU"→"Cuba", "US"→"United States", "BR"→"Brazil", "AR"→"Argentina", "UA"→"Ukraine", "RU"→"Russia", "BY"→"Belarus", "KZ"→"Kazakhstan", "UZ"→"Uzbekistan", "AZ"→"Azerbaijan", "GE"→"Georgia", "AM"→"Armenia", "MD"→"Moldova", "LT"→"Lithuania", "LV"→"Latvia", "EE"→"Estonia", "BG"→"Bulgaria", "HR"→"Croatia", "RS"→"Serbia", "SK"→"Slovakia", "SI"→"Slovenia", "BA"→"Bosnia and Herzegovina", "AL"→"Albania", "MK"→"North Macedonia", "ME"→"Montenegro"
- "завтра", "послезавтра" и т.д. переводи в название дня на английском (Monday/Tuesday/...)
- Сегодняшняя дата и день будут переданы в запросе
- Для close_days с НЕСКОЛЬКИМИ странами используй поле countries_days:
  "countries_days": [
    {"country": "Brazil",   "days_to_close": ["Thursday"]},
    {"country": "Slovenia", "days_to_close": ["Thursday"]}
  ]
  Если одна страна — тоже countries_days, список из одного элемента.
  "days_to_close" на верхнем уровне оставляй пустым.
- Для close_days: days_to_close = список дней которые нужно закрыть
- ВАЖНО: Правило различения брокера и аффилиата при запросе прайса:
  • Просто число + страны + "прайс/price" → ЭТО АФФИЛИАТ (get_affiliate_revenue)
    Примеры: "28 прайс испания", "159 DE price", "28 франция прайс"
  • Имя (текст) + страны + "прайс/price" → ЭТО БРОКЕР (get_broker_revenue)
    Примеры: "Nexus DE price", "финтрикс франция прайс", "Marsi прайс ES"
  • Явное указание "брокер/broker" или "афф/aff" → следуй указанию
    Примеры: "какой прайс у брокера 32", "прайс аффа 159"
  По умолчанию: если ID — просто число без слова "брокер" → считай аффилиатом
- Для get_affiliate_revenue: affiliate_id = ID аффилиата, countries = список стран для проверки
  Примеры: "159 DE price", "159 price DE ES", "какой прайс у аффа 159 для германии", "28 прайс испания италия"
  → {"action": "get_affiliate_revenue", "affiliate_id": "159", "broker_ids": ["159"], "countries": ["Germany"]}
- Для get_broker_revenue: broker_ids = список брокеров, countries = список стран
  Примеры: "Nexus DE price", "финтрикс франция прайс", "какой прайс у брокера 32 для германии"
  → {"action": "get_broker_revenue", "broker_ids": ["Nexus"], "countries": ["Germany"]}
- Для get_hours: broker_ids = список брокеров, countries = список стран
  Ключевые слова: "часы", "дай часы", "wh", "WH", "working hours", "расписание"
  Примеры:
  • "дай часы Nexus FR" → {"action": "get_hours", "broker_ids": ["Nexus"], "countries": ["France"]}
  • "wh Nexus FR" → {"action": "get_hours", "broker_ids": ["Nexus"], "countries": ["France"]}
  • "Nexus FR WH" → {"action": "get_hours", "broker_ids": ["Nexus"], "countries": ["France"]}
  • "часы Capitan DE ES" → {"action": "get_hours", "broker_ids": ["Capitan"], "countries": ["Germany", "Spain"]}
  • "расписание MN Франция" → {"action": "get_hours", "broker_ids": ["MN"], "countries": ["France"]}
  • Многострочный формат:
    "скажи часы
     легион
     де"
    → {"action": "get_hours", "broker_ids": ["Legion"], "countries": ["Germany"]}
  ВАЖНО: ISO коды (DE, FR, ES...) и названия стран — это ВСЕГДА страны, а НЕ часть имени брокера.
  Никогда не склеивай имя брокера и страну в одну строку.
  Если страна не указана — countries: ["all"] (показать все страны)

Если пользователь запрашивает прайсы для НЕСКОЛЬКИХ объектов (аффов и/или брокеров) сразу — используй поле "queries":
{
  "action": "get_prices",
  "broker_ids": [],
  "queries": [
    {"type": "affiliate", "id": "159",   "countries": ["Germany"]},
    {"type": "broker",    "id": "Nexus", "countries": ["Germany"]}
  ]
}
Пример:
  "159 DE price
   Nexus DE price"
→ {"action": "get_prices", "broker_ids": ["159"], "queries": [
    {"type": "affiliate", "id": "159",   "countries": ["Germany"]},
    {"type": "broker",    "id": "Nexus", "countries": ["Germany"]}
  ]}
- Для add_affiliate_revenue: affiliate_id = ID аффилиата (число), country_revenues = список {country, amount}
  Формат может быть любым из:
  • "добавь прайс для аффа 159 / FR 1300 / ES 1500"
  • "aff 159 / FR 1300 / ES 1500"
  • Просто число на первой строке + строки "ISO сумма" на следующих:
    159
    FR 1000
    ES 1100
  • Число + несколько ISO кодов на первой строке + сумма на второй (одна сумма для всех стран):
    159 CO CL
    650
    (159 — аффилиат, CO и CL — страны, 650 — сумма для каждой)
  • Смешанный формат:
    159
    MX 650
    CO 700
  В любом случае: если первая строка начинается с числа и не содержит часов (HH:MM) — это add_affiliate_revenue.
  Одна сумма на несколько стран → country_revenues с одинаковым amount для каждой страны.
  → {"action": "add_affiliate_revenue", "affiliate_id": "159", "broker_ids": ["159"],
     "country_revenues": [{"country": "Colombia", "amount": 650}, {"country": "Chile", "amount": 650}]}
  Отличие от брокера: для брокера пишут имя или ID с названием, для аффа — просто число

Формат "прайс-листа" для add_revenue с несколькими странами:
Пользователь может прислать:
  FR 1300$ cpa
  ES 1500$ cpa
или:
  FR 1300
  ES 1500

Правила разбора:
- ISO код страны → переводи в полное название
- Число → сумма прайса
- $ — игнорируй
- cpa / crg / тип сделки — игнорируй
- Используй поле "country_revenues" — список объектов {country, amount}
- "amount" должен быть числом (без $)

Пример:
  "добавь прайс для Marsi cpa / FR 1300$ cpa / ES 1500$ cpa"
→ {
    "action": "add_revenue",
    "broker_ids": ["Marsi cpa"],
    "country_revenues": [
      {"country": "France", "amount": 1300},
      {"country": "Spain",  "amount": 1500}
    ],
    "countries": [],
    "amount": null
  }

Формат "лид-формы":
Пользователь может прислать задачу в таком формате:
  17 CA EN CRG today
  Capitan 10 cap 17:00-00:00 gmt+2

Правила разбора:
- Первая строка содержит метаданные. Разбирай так:
  • Числа в начале (17) — affiliate id, ИГНОРИРУЙ
  • ISO коды стран (CA, DE, BR...) — переводи в полное название → country
  • Языки (EN, RU, PL...) — ИГНОРИРУЙ (это язык деска, не страна)
  • Тип сделки (CPA, CRG) — ИГНОРИРУЙ пока
  • "today" / "сегодня" → days_to_keep: [название сегодняшнего дня]
  • "tomorrow" / "завтра" → days_to_keep: [название завтрашнего дня]
  • Название дня (Monday, Saturday...) → days_to_keep: [этот день]
- Вторая строка содержит брокера и часы:
  • Первое слово/фраза до числа — имя брокера → broker_ids
  • "N cap" — лимит, ИГНОРИРУЙ пока
  • HH:MM-HH:MM или HH:MM–HH:MM — часы работы → hours start/end
  • "00:00" в конце времени означает полночь — оставляй как "00:00"
  • gmt+N / UTC+N — часовой пояс, ИГНОРИРУЙ (CRM сам управляет таймзоной)
- Итоговый action: "add_hours" если страна одна, days_to_keep = конкретный день
- skip_missing: НЕ используй для lead-формы, вместо этого используй "requested_day"
- "requested_day" — конкретный день который запросил пользователь (например "Thursday", "Saturday").
  Если день не указан — оставляй "requested_day": null.
  Бот сам решит что делать:
  • день указан + будний → создаст/обновит с Пн–Пт
  • день указан + выходной → только этот день
  • день не указан → рабочие дни (Пн–Пт) по умолчанию

Пример:
  "17 CA EN CRG today / Capitan 10 cap 17:00-00:00 gmt+2"  (сегодня четверг)
→ {
    "action": "add_hours",
    "broker_ids": ["Capitan"],
    "country_hours": [{"country": "Canada", "start": "17:00", "end": "00:00"}],
    "days_to_keep": ["Thursday"],
    "requested_day": "Thursday",
    "no_traffic": true,
    "skip_missing": false
  }

Формат "desk-расписания":
Пользователь может прислать расписание в таком формате:
  JP desk 07:30-12:30
  GEO: JP
  EN desk 11:00-17:00
  GEO: BE FI NL NZ AU
  SUNDAY OFF (или OFF, или выходной)

Правила разбора:
- Строка "X desk HH:MM-HH:MM" или "X desk HH:MM–HH:MM" — название деска и часы работы
- Строка "GEO: XX YY ZZ" — страны (ISO коды) к которым применяются часы предыдущего деска. Переводи каждый код в полное английское название
- Строка "SUNDAY OFF" / "OFF" / "выходной" — означает что воскресенье закрыто
- Строка "SATURDAY OFF" — суббота закрыта
- Если написано "поставь на выходные" — days_to_keep: ["Saturday","Sunday"], но корректируй с учётом OFF-строк (если SUNDAY OFF — убирай Sunday из days_to_keep)
- Если написано "поставь на субботу" — days_to_keep: ["Saturday"]
- Если написано "поставь на воскресенье" — days_to_keep: ["Sunday"]
- Генерируй country_hours: один элемент на каждую страну из GEO с часами соответствующего деска
- Слово "desk" игнорируй — оно не несёт смысла для CRM
- Если пользователь задаёт РАЗНЫЕ часы для разных групп дней (например, рабочие 10-19 и суббота 10-15), используй поле "schedule_groups":
  [
    {"days": ["Monday","Tuesday","Wednesday","Thursday","Friday"], "start": "10:00", "end": "19:00"},
    {"days": ["Saturday"], "start": "10:00", "end": "15:00"}
  ]
  "воскресенье закрыто" = не включать Sunday ни в одну группу.
  В этом случае country_hours заполни по первой группе (рабочие дни), а schedule_groups — всеми группами.
  Пример: "рабочие 10-19, суббота 10-15, воскресенье закрыто"
  → schedule_groups: [
      {"days": ["Monday","Tuesday","Wednesday","Thursday","Friday"], "start": "10:00", "end": "19:00"},
      {"days": ["Saturday"], "start": "10:00", "end": "15:00"}
    ]
- Для desk-формата всегда ставь "skip_missing": true — не нужно добавлять страны которых нет у брокера, только обновлять существующие

Пример команды: "поставь часы для этих стран на субботу: JP desk 07:30-12:30 / GEO: JP / EN desk 11:00-17:00 / GEO: BE FI NL"
Результат:
{
  "action": "add_hours",
  "broker_ids": ["32"],
  "country_hours": [
    {"country": "Japan",       "start": "07:30", "end": "12:30"},
    {"country": "Belgium",     "start": "11:00", "end": "17:00"},
    {"country": "Finland",     "start": "11:00", "end": "17:00"},
    {"country": "Netherlands", "start": "11:00", "end": "17:00"}
  ],
  "days_to_keep": ["Saturday"],
  "no_traffic": true
}

- Возвращай ТОЛЬКО JSON
"""

def parse_command(text: str) -> dict:
    """Понять команду пользователя через Claude (синхронная — вызывать через run_in_executor)."""
    today = datetime.datetime.now()
    day_names = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    today_name = day_names[today.weekday()]
    tomorrow_name = day_names[(today.weekday()+1) % 7]
    day_after_tomorrow = day_names[(today.weekday()+2) % 7]

    enriched = (
        f"Сегодня {today.strftime('%Y-%m-%d')}, {today_name}. "
        f"Завтра: {tomorrow_name}. Послезавтра: {day_after_tomorrow}.\n\n"
        f"Команда: {text}"
    )

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY, timeout=60.0)
    resp = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=4000,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": enriched}]
    )
    raw = resp.content[0].text.strip()
    log.info(f"parse_command raw response ({len(raw)} chars): {raw[:500]}")
    raw = re.sub(r"^```[a-z]*\n?", "", raw)
    raw = re.sub(r"\n?```$", "", raw)
    try:
        result = json.loads(raw)
        log.info(f"parse_command result: action={result.get('action')}, brokers={result.get('broker_ids')}, country_hours count={len(result.get('country_hours', []))}")
        return result
    except Exception as e:
        log.error(f"parse_command JSON parse error: {e}\nRaw: {raw[:300]}")
        return {"action": "unknown"}


# ══════════════════════════════════════════
#  БРАУЗЕР — вспомогательные функции
# ══════════════════════════════════════════

async def get_page() -> Page:
    """Вернуть активную страницу, при необходимости залогиниться."""
    global _playwright, _browser, _context, _page

    if _browser is None or not _browser.is_connected():
        log.info("Запускаю браузер...")
        _playwright = await async_playwright().start()
        _browser = await _playwright.chromium.launch(headless=True)
        _context = await _browser.new_context(viewport={"width": 1440, "height": 900})
        _page = await _context.new_page()
        await do_login()
    else:
        # Проверяем что сессия не истекла
        if "login" in _page.url.lower():
            log.info("Сессия истекла, перелогиниваюсь...")
            await do_login()

    return _page


async def do_login():
    """Войти в CRM."""
    await _page.goto(CRM_URL, wait_until="domcontentloaded", timeout=60000)
    await _page.wait_for_timeout(1000)

    # Если уже на дашборде — всё хорошо
    if "login" not in _page.url.lower() and "dashboard" in _page.url.lower():
        log.info("Уже авторизован.")
        return

    log.info("Авторизуюсь...")
    await _page.wait_for_selector("input[type='password']", timeout=10000)

    username_input = await _page.query_selector(
        "input[placeholder*='Username' i], input[placeholder*='email' i], "
        "input[type='email'], input[name='email'], input[name='username'], input[type='text']"
    )
    if username_input:
        await username_input.click()
        await username_input.fill(CRM_EMAIL)

    password_input = await _page.query_selector("input[type='password']")
    if password_input:
        await password_input.click()
        await password_input.fill(CRM_PASSWORD)

    await _page.wait_for_timeout(500)

    login_btn = await _page.query_selector(
        "button[type='submit'], input[type='submit'], button:has-text('LOG IN'), button:has-text('Login')"
    )
    if login_btn:
        await login_btn.click()
    else:
        await password_input.press("Enter")

    try:
        await _page.wait_for_url(lambda url: "login" not in url, timeout=10000)
    except Exception:
        pass

    await _page.wait_for_timeout(1000)
    log.info(f"После логина URL: {_page.url}")

    if "login" in _page.url.lower():
        raise Exception("Авторизация не удалась — проверь логин и пароль в config.py")

    log.info("Авторизация прошла успешно.")
    alog.set_status("last_login", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


async def find_and_open_broker(page: Page, broker_id: str) -> Optional[str]:
    """
    Найти брокера и вернуть его base path (/clients/ID).
    Возвращает None если брокер не найден.
    """
    broker_id = str(broker_id).strip()

    # Если ID числовой — идём напрямую, без поиска
    if broker_id.isdigit():
        base = f"/clients/{broker_id}"
        test_url = f"{CRM_URL.rstrip('/')}{base}/settings"
        await page.goto(test_url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(800)
        current = page.url
        log.info(f"После перехода URL: {current}")
        # Считаем успехом если нас не выкинуло на логин или список
        if "login" not in current and "/clients?" not in current:
            return base
        return None

    # Если имя — ищем через поиск
    await page.goto(f"{CRM_URL.rstrip('/')}/clients?search=", wait_until="domcontentloaded", timeout=60000)
    await page.wait_for_timeout(2000)

    search = None
    for selector in [
        "input[placeholder='Search a broker...']",
        "input.form-control[type='text']",
        "input[type='text']",
    ]:
        try:
            search = await page.wait_for_selector(selector, timeout=3000)
            if search:
                break
        except Exception:
            continue

    if not search:
        return None

    # Очищаем поле и вводим имя с задержкой — fill() не всегда триггерит фильтрацию
    await search.click(click_count=3)
    await page.keyboard.press("Backspace")
    await page.wait_for_timeout(500)
    await search.type(broker_id, delay=80)
    await page.wait_for_timeout(800)

    # Ждём пока таблица отфильтруется — количество строк должно стабилизироваться
    prev_count = -1
    stable_checks = 0
    for _ in range(20):
        await page.wait_for_timeout(400)
        cur_count = await page.evaluate("() => document.querySelectorAll('table tr').length")
        if cur_count == prev_count and cur_count > 0:
            stable_checks += 1
            if stable_checks >= 2:  # Два подряд одинаковых — таблица точно стабилизировалась
                break
        else:
            stable_checks = 0
        prev_count = cur_count

    # Собираем все строки таблицы с именем брокера и ссылкой на settings
    rows = await page.evaluate("""(query) => {
        const results = [];
        document.querySelectorAll("table tr").forEach(row => {
            const link = row.querySelector("a[href*='/clients/'][href*='/settings']") ||
                         row.querySelector("a.btn-primary");
            if (!link) return;
            const tds = row.querySelectorAll("td");
            // Ищем колонку с именем: та что не число и не статус
            let name = "";
            tds.forEach(td => {
                const t = td.innerText.trim();
                // Пропускаем числа (ID) и короткие слова типа "active"
                if (t && !/^\d+$/.test(t) && t.length > 4 && !["active","inactive","disabled"].includes(t.toLowerCase())) {
                    if (!name) name = t;
                }
            });
            if (!name && tds.length > 2) name = tds[2].innerText.trim();
            if (link && name) {
                results.push({
                    name: name,
                    href: link.getAttribute("href")
                });
            }
        });
        return results;
    }""", broker_id)

    log.info(f"Найдено брокеров по запросу '{broker_id}': {[r['name'] for r in rows]}")

    if not rows:
        return None

    # Проверяем что хоть один результат реально содержит запрос
    # Если нет — поиск вернул мусор (не успел отфильтровать)
    query_lower = broker_id.lower().strip()
    relevant = [r for r in rows if query_lower in r["name"].lower()]
    if not relevant:
        log.info(f"Ни один результат не содержит '{broker_id}' — брокер не найден")
        return None
    rows = relevant  # работаем только с релевантными

    # 1. Точное совпадение имени (без учёта регистра)
    for row in rows:
        if row["name"].lower().strip() == query_lower:
            log.info(f"Точное совпадение: {row['name']}")
            href = row["href"].replace("/settings", "")
            return href

    # 2. Имя начинается с запроса (например "MN" → "MN 216", но не "MN FR 216")
    for row in rows:
        name_lower = row["name"].lower().strip()
        # Убираем числовой префикс типа "272 - MN"
        clean = re.sub(r"^\d+\s*-\s*", "", name_lower).strip()
        if clean == query_lower or name_lower == query_lower:
            log.info(f"Совпадение после очистки префикса: {row['name']}")
            return row["href"].replace("/settings", "")

    # 3. Частичное совпадение — предпочитаем CPA, затем кратчайшее имя
    partial = [r for r in rows if query_lower in r["name"].lower()]
    if partial:
        # Если в запросе явно указан CRG — берём CRG
        if "crg" in query_lower:
            crg = [r for r in partial if "crg" in r["name"].lower()]
            if crg:
                best = min(crg, key=lambda r: len(r["name"]))
                log.info(f"Выбран CRG по запросу: {best['name']}")
                return best["href"].replace("/settings", "")
        # Иначе предпочитаем CPA
        cpa = [r for r in partial if "cpa" in r["name"].lower()]
        if cpa:
            best = min(cpa, key=lambda r: len(r["name"]))
            log.info(f"Предпочтён CPA: {best['name']}")
            return best["href"].replace("/settings", "")
        # Нет ни CPA ни CRG — берём кратчайшее
        best = min(partial, key=lambda r: len(r["name"]))
        log.info(f"Частичное совпадение (кратчайшее): {best['name']}")
        return best["href"].replace("/settings", "")

    # 4. Первый результат как запасной
    log.info(f"Берём первый результат: {rows[0]['name']}")
    return rows[0]["href"].replace("/settings", "")


# ══════════════════════════════════════════
#  ДЕЙСТВИЯ В CRM
# ══════════════════════════════════════════

async def _scrape_countries_from_page(page) -> list:
    """Собрать список стран с текущей открытой страницы Opening Hours."""
    try:
        await page.wait_for_selector("button.btn-primary.btn-sm, button.btn.btn-sm.btn-primary", timeout=8000)
    except Exception:
        pass
    await page.wait_for_timeout(400)
    countries = await page.evaluate("""() => {
        const days = new Set(['monday','tuesday','wednesday','thursday','friday','saturday','sunday']);
        const result = [];
        document.querySelectorAll('table tr, .table tr').forEach(row => {
            const hasBtn = row.querySelector('button.btn-primary.btn-sm, button.btn.btn-sm.btn-primary');
            if (!hasBtn) return;
            const td = row.querySelector('td');
            if (!td) return;
            const name = td.innerText.trim();
            if (name && !days.has(name.toLowerCase())) result.push(name);
        });
        return [...new Set(result)];
    }""")
    return countries


async def action_change_hours(broker_id: str, start: str, end: str,
                               countries_filter: list, no_traffic: bool,
                               days_filter: list = None) -> str:
    """Изменить часы работы брокера."""
    page = await get_page()

    base_path = await find_and_open_broker(page, broker_id)
    if not base_path:
        return f"❌ Брокер «{broker_id}» не найден. Ничего не изменено."

    # Переходим напрямую на страницу Opening Hours
    oh_url = f"{CRM_URL.rstrip('/')}{base_path}/opening_hours"
    await page.goto(oh_url, wait_until="domcontentloaded", timeout=60000)
    await page.wait_for_timeout(3000)

    # ── Парсим start и end один раз ──────────────────────
    sh, sm = (start.split(":") + ["00"])[:2]
    eh, em = (end.split(":") + ["00"])[:2]
    start_val = f"{sh.zfill(2)}:{sm.zfill(2)}"
    end_val   = f"{eh.zfill(2)}:{em.zfill(2)}"

    # Собираем карандаши и имена стран для обработки
    async def collect_pencils():
        btns = await page.query_selector_all(
            "button.btn-primary.btn-sm:not(.btn_big), button.btn.btn-sm.btn-primary:not(.float-right)"
        )
        result = []
        for btn in btns:
            text = (await btn.inner_text()).strip()
            if not text:  # кнопки без текста = иконки-карандаши
                c_name = await btn.evaluate("""el => {
                    const row = el.closest('tr');
                    if (!row) return '';
                    const td = row.querySelector('td');
                    return td ? td.innerText.trim() : '';
                }""")
                result.append((btn, c_name))
        return result

    pencils_with_names = await collect_pencils()
    log.info(f"Найдено кнопок-карандашей: {len(pencils_with_names)}")

    if not pencils_with_names:
        log.info(f"URL страницы: {page.url}")
        return "❌ Не найдены кнопки редактирования часов. Ничего не изменено."

    # Собираем имена стран для обработки (фильтруем заранее)
    countries_to_process = []
    for _, country_name in pencils_with_names:
        if "all" not in countries_filter and country_name:
            if not any(c.lower() in country_name.lower() for c in countries_filter):
                log.info(f"Пропускаю {country_name} — не в фильтре {countries_filter}")
                continue
        countries_to_process.append(country_name)

    results = []

    for country_name in countries_to_process:
        # После каждого сохранения DOM обновляется — заново ищем карандаш по имени страны
        fresh_pencils = await collect_pencils()
        target_pencil = None
        for btn, c_name in fresh_pencils:
            if c_name.strip().lower() == country_name.strip().lower():
                target_pencil = btn
                break

        if not target_pencil:
            results.append(f"⚠️ {country_name}: карандаш не найден после обновления DOM")
            continue

        await target_pencil.click()
        await page.wait_for_timeout(600)

        # Ждём модальное окно
        try:
            modal = await page.wait_for_selector(".modal-body, [role='dialog']", timeout=4000)
        except Exception:
            results.append(f"⚠️ {country_name}: модальное окно не открылось")
            continue

        await page.wait_for_timeout(500)

        # ── Меняем часы через timepicker text inputs ────────
        all_inputs = await modal.query_selector_all("input")
        time_inputs = []
        for inp in all_inputs:
            inp_type = await inp.get_attribute("type") or ""
            inp_cls = await inp.get_attribute("class") or ""
            if "timepicker" in inp_cls and inp_type == "text":
                time_inputs.append(inp)

        log.debug(f"Timepicker inputs: {len(time_inputs)}")

        # По 2 на каждый день: start, end
        for i in range(0, len(time_inputs), 2):
            start_inp = time_inputs[i]
            end_inp   = time_inputs[i+1] if i+1 < len(time_inputs) else None

            await start_inp.evaluate(
                f"""el => {{
                    el.value = '{start_val}';
                    el.dispatchEvent(new Event('input', {{bubbles:true}}));
                    el.dispatchEvent(new Event('change', {{bubbles:true}}));
                }}"""
            )
            await page.wait_for_timeout(80)

            if end_inp:
                await end_inp.evaluate(
                    f"""el => {{
                        el.value = '{end_val}';
                        el.dispatchEvent(new Event('input', {{bubbles:true}}));
                        el.dispatchEvent(new Event('change', {{bubbles:true}}));
                    }}"""
                )
                await page.wait_for_timeout(80)

        # ── Отключаем дни (убираем галочки) ───────
        if days_filter and "all" not in [d.lower() for d in days_filter]:
            all_days = ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"]
            days_to_keep = [d.lower() for d in days_filter]
            days_to_disable = [d for d in all_days if d not in days_to_keep]
            log.info(f"Отключаю дни: {days_to_disable}")

            day_checkboxes = await modal.query_selector_all("input[type='checkbox']")
            for cb in day_checkboxes:
                label_text = await cb.evaluate("el => el.closest('label, tr, div')?.textContent?.toLowerCase() || ''")
                if "no traffic" in label_text:
                    continue
                for day in days_to_disable:
                    if day in label_text:
                        checked = await cb.is_checked()
                        if checked:
                            await cb.evaluate("el => el.click()")
                            await page.wait_for_timeout(100)
                        break
        if no_traffic:
            checkboxes = await modal.query_selector_all("input[type='checkbox']")
            for cb in checkboxes:
                label_text = await cb.evaluate("el => el.closest('label, tr, div')?.textContent || ''")
                if "no traffic" in label_text.lower():
                    checked = await cb.is_checked()
                    if not checked:
                        await cb.evaluate("el => el.click()")
                        await page.wait_for_timeout(100)

        # ── Сохраняем ─────────────────────────────
        try:
            save_btn = await page.wait_for_selector("text=SAVE OPENING HOURS", timeout=3000)
            await save_btn.click()
            await page.wait_for_timeout(700)
            results.append(f"✅ {country_name}: {start}–{end} сохранено")
        except Exception:
            await _close_modal(page)
            results.append(f"⚠️ {country_name}: кнопка Save не найдена")

    return "\n".join(results) if results else "⚠️ Нет строк для изменения."


async def action_edit_country_add_days(broker_id: str, country: str, start: str, end: str,
                                        no_traffic: bool, days_to_add: list) -> str:
    """
    Редактировать существующую запись страны: добавить галочки на нужные дни
    и выставить часы — не трогая уже включённые дни.
    """
    page = await get_page()

    base_path = await find_and_open_broker(page, broker_id)
    if not base_path:
        return f"❌ Брокер «{broker_id}» не найден. Ничего не изменено."

    oh_url = f"{CRM_URL.rstrip('/')}{base_path}/opening_hours"
    await page.goto(oh_url, wait_until="domcontentloaded", timeout=60000)
    try:
        await page.wait_for_selector("button.btn-primary.btn-sm, button.btn.btn-sm.btn-primary", timeout=12000)
    except Exception:
        pass
    await page.wait_for_timeout(500)

    # Ищем карандаш нужной страны
    edit_buttons = await page.query_selector_all("button.btn-primary.btn-sm, button.btn.btn-sm.btn-primary")
    target_pencil = None
    for btn in edit_buttons:
        if (await btn.inner_text()).strip():
            continue
        c_name = await btn.evaluate("""el => {
            const row = el.closest('tr');
            return row ? row.querySelector('td')?.innerText?.trim() : '';
        }""")
        if country.lower() in c_name.lower():
            target_pencil = btn
            break

    if not target_pencil:
        return f"❌ Страна «{country}» не найдена у брокера. Ничего не изменено."

    await target_pencil.click()
    await page.wait_for_timeout(600)

    try:
        modal = await page.wait_for_selector(".modal-body, [role='dialog']", timeout=4000)
    except Exception:
        return f"❌ {country}: модальное окно не открылось."

    await page.wait_for_timeout(400)

    days_lower = [d.lower() for d in days_to_add]
    log.info(f"Добавляю дни {days_lower} к {country}")

    sh, sm = (start.split(":") + ["00"])[:2]
    eh, em = (end.split(":") + ["00"])[:2]
    start_val = f"{sh.zfill(2)}:{sm.zfill(2)}"
    end_val   = f"{eh.zfill(2)}:{em.zfill(2)}"

    # Проходим по строкам модалки — каждая строка = один день
    # Включаем только нужные дни и только им меняем время
    checkboxes = await modal.query_selector_all("input[type='checkbox']")
    enabled = []

    for cb in checkboxes:
        label_text = await cb.evaluate("el => el.closest('label,tr,div')?.textContent?.toLowerCase() || ''")
        if "no traffic" in label_text:
            continue

        matched_day = None
        for day in days_lower:
            if day in label_text:
                matched_day = day
                break

        if matched_day is None:
            continue  # Этот день нас не касается — не трогаем

        # Включаем день если не включён
        if not await cb.is_checked():
            await cb.evaluate("el => el.click()")
            await page.wait_for_timeout(100)
        enabled.append(matched_day.capitalize())

        # Находим timepicker'ы именно этой строки (row/div дня)
        row_time_inputs = await cb.evaluate(f"""el => {{
            const row = el.closest('tr, .row, li, [class*="day"]');
            if (!row) return [];
            const inputs = row.querySelectorAll('input.timepicker-input, input[class*="timepicker"]');
            return Array.from(inputs).map(i => i.className);
        }}""")
        log.info(f"  День {matched_day}: timepickers в строке = {row_time_inputs}")

        # Устанавливаем время только для этой строки
        await cb.evaluate(f"""el => {{
            const row = el.closest('tr, .row, li, [class*="day"]');
            if (!row) return;
            const inputs = row.querySelectorAll('input.timepicker-input, input[class*="timepicker"]');
            if (inputs[0]) {{
                inputs[0].value = '{start_val}';
                inputs[0].dispatchEvent(new Event('input', {{bubbles:true}}));
                inputs[0].dispatchEvent(new Event('change', {{bubbles:true}}));
            }}
            if (inputs[1]) {{
                inputs[1].value = '{end_val}';
                inputs[1].dispatchEvent(new Event('input', {{bubbles:true}}));
                inputs[1].dispatchEvent(new Event('change', {{bubbles:true}}));
            }}
        }}""")
        await page.wait_for_timeout(80)

    # No traffic — ставим глобальный чекбокс если есть
    if no_traffic:
        for cb in checkboxes:
            label_text = await cb.evaluate("el => el.closest('label,tr,div')?.textContent || ''")
            if "no traffic" in label_text.lower():
                if not await cb.is_checked():
                    await cb.evaluate("el => el.click()")
                    await page.wait_for_timeout(100)

    try:
        save_btn = await page.wait_for_selector("text=SAVE OPENING HOURS", timeout=3000)
        await save_btn.click()
        await page.wait_for_timeout(700)
        return f"✅ {country}: добавлены дни {', '.join(enabled)} с часами {start}–{end}"
    except Exception:
        await _close_modal(page)
        return f"⚠️ {country}: кнопка Save не найдена."


async def action_add_country_hours_multi(broker_id: str, country: str,
                                         schedule_groups: list, no_traffic: bool,
                                         country_exists: bool = False) -> str:
    """
    Добавить/обновить часы для страны с несколькими группами дней за один проход модалки.
    schedule_groups: [{"days": [...], "start": "10:00", "end": "19:00"}, ...]
    """
    page = await get_page()

    base_path = await find_and_open_broker(page, broker_id)
    if not base_path:
        return f"❌ Брокер «{broker_id}» не найден."

    oh_url = f"{CRM_URL.rstrip('/')}{base_path}/opening_hours"
    await page.goto(oh_url, wait_until="domcontentloaded", timeout=60000)

    if country_exists:
        # Редактируем существующую запись
        try:
            await page.wait_for_selector("button.btn-primary.btn-sm, button.btn.btn-sm.btn-primary", timeout=10000)
        except Exception:
            pass
        await page.wait_for_timeout(400)
        btns = await page.query_selector_all("button.btn-primary.btn-sm, button.btn.btn-sm.btn-primary")
        target = None
        for btn in btns:
            if (await btn.inner_text()).strip():
                continue
            c = await btn.evaluate("el => { const r = el.closest('tr'); return r ? r.querySelector('td')?.innerText?.trim() : ''; }")
            if country.lower() in c.lower():
                target = btn
                break
        if not target:
            return f"❌ Страна «{country}» не найдена."
        await target.click()
    else:
        # Добавляем новую запись
        try:
            add_btn = await page.wait_for_selector(
                "button:has-text('ADD OPENING HOURS'), a:has-text('ADD OPENING HOURS')", timeout=12000
            )
            await add_btn.click()
        except Exception:
            return "❌ Не найдена кнопка ADD OPENING HOURS."

    await page.wait_for_timeout(800)
    try:
        modal = await page.wait_for_selector(".modal-body, [role='dialog']", timeout=5000)
    except Exception:
        return "❌ Модальное окно не открылось."
    await page.wait_for_timeout(500)

    # Выбираем страну (только для новой записи)
    if not country_exists:
        try:
            dropdown_toggle = await modal.query_selector(".smart__dropdown, [class*='smart__dropdown']")
            if dropdown_toggle:
                await dropdown_toggle.click()
                await page.wait_for_timeout(400)
            search_input = await page.query_selector("input[id*='search-input'], input[id*='search']")
            if search_input:
                await search_input.click(click_count=3)
                await search_input.type(country, delay=50)
                for _ in range(20):
                    await page.wait_for_timeout(200)
                    cnt = await page.evaluate("() => document.querySelectorAll('li.dropdown-item').length")
                    if cnt < 10:
                        break
            items = await page.query_selector_all("li.dropdown-item")
            found = False
            for item in items:
                txt = (await item.inner_text()).strip()
                if country.lower() in txt.lower():
                    await item.click()
                    found = True
                    await page.wait_for_timeout(400)
                    break
            if not found:
                await _close_modal(page)
                return f"❌ Страна «{country}» не найдена в списке."
        except Exception as e:
            return f"❌ Ошибка при выборе страны: {e}"

        # Переполучаем modal после выбора страны
        await page.wait_for_timeout(600)
        modal = await page.query_selector(".modal-body, [role='dialog']")
        if not modal:
            return "❌ Модальное окно закрылось после выбора страны."

    # Собираем все дни со всех групп
    all_days_in_groups = set()
    for g in schedule_groups:
        for d in g.get("days", []):
            all_days_in_groups.add(d.lower())

    all_days = ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"]

    # Строим карту: день → (start, end)
    day_to_time = {}
    for g in schedule_groups:
        for d in g.get("days", []):
            day_to_time[d.lower()] = (g["start"], g["end"])

    log.info(f"schedule_groups для {country}: {day_to_time}")

    # Обрабатываем каждый чекбокс дня
    checkboxes = await modal.query_selector_all("input[type='checkbox']")
    for cb in checkboxes:
        label_text = await cb.evaluate("el => el.closest('label,tr,div')?.textContent?.toLowerCase() || ''")
        if "no traffic" in label_text:
            continue

        matched_day = None
        for d in all_days:
            if d in label_text:
                matched_day = d
                break
        if not matched_day:
            continue

        if matched_day in day_to_time:
            # Включаем день
            if not await cb.is_checked():
                await cb.evaluate("el => el.click()")
                await page.wait_for_timeout(100)
            # Ставим время для этого дня
            start_val, end_val = day_to_time[matched_day]
            sh, sm = (start_val.split(":") + ["00"])[:2]
            eh, em = (end_val.split(":") + ["00"])[:2]
            sv = f"{sh.zfill(2)}:{sm.zfill(2)}"
            ev = f"{eh.zfill(2)}:{em.zfill(2)}"
            await cb.evaluate(f"""el => {{
                const row = el.closest('tr, .row, li, [class*="day"]');
                if (!row) return;
                const inputs = row.querySelectorAll('input.timepicker-input, input[class*="timepicker"]');
                if (inputs[0]) {{
                    inputs[0].click(); inputs[0].value = '{sv}';
                    inputs[0].dispatchEvent(new Event('input',{{bubbles:true}}));
                    inputs[0].dispatchEvent(new Event('change',{{bubbles:true}}));
                    inputs[0].dispatchEvent(new Event('blur',{{bubbles:true}}));
                }}
                if (inputs[1]) {{
                    inputs[1].click(); inputs[1].value = '{ev}';
                    inputs[1].dispatchEvent(new Event('input',{{bubbles:true}}));
                    inputs[1].dispatchEvent(new Event('change',{{bubbles:true}}));
                    inputs[1].dispatchEvent(new Event('blur',{{bubbles:true}}));
                }}
            }}""")
            await page.wait_for_timeout(80)
        else:
            # Выключаем день — его нет ни в одной группе
            if await cb.is_checked():
                await cb.evaluate("el => el.click()")
                await page.wait_for_timeout(80)

    # No traffic
    if no_traffic:
        for cb in checkboxes:
            label_text = await cb.evaluate("el => el.closest('label,tr,div')?.textContent || ''")
            if "no traffic" in label_text.lower():
                if not await cb.is_checked():
                    await cb.evaluate("el => el.click()")
                    await page.wait_for_timeout(100)

    # Сохраняем
    try:
        save_btn = await page.wait_for_selector("text=SAVE OPENING HOURS", timeout=3000)
        await save_btn.click()
        await page.wait_for_timeout(700)
        groups_str = ", ".join(f"{g['start']}–{g['end']} ({'/'.join(g['days'])})" for g in schedule_groups)
        action_word = "Обновлены" if country_exists else "Добавлены"
        return f"✅ {action_word} часы для {country}: {groups_str}"
    except Exception:
        await _close_modal(page)
        return f"⚠️ {country}: кнопка Save не найдена."


async def action_add_country_hours(broker_id: str, country: str, start: str, end: str,
                                    no_traffic: bool, days_filter: list = None) -> str:
    """Добавить часы работы для новой страны."""
    page = await get_page()

    base_path = await find_and_open_broker(page, broker_id)
    if not base_path:
        return f"❌ Брокер «{broker_id}» не найден. Ничего не изменено."

    oh_url = f"{CRM_URL.rstrip('/')}{base_path}/opening_hours"
    log.info(f"Открываю: {oh_url}")
    await page.goto(oh_url, wait_until="domcontentloaded", timeout=60000)

    # Ждём пока Vue отрендерит кнопку ADD OPENING HOURS
    try:
        add_btn = await page.wait_for_selector(
            "button:has-text('ADD OPENING HOURS'), a:has-text('ADD OPENING HOURS'), .btn:has-text('ADD')",
            timeout=12000
        )
        log.info("Кнопка ADD OPENING HOURS найдена, кликаю...")
        await add_btn.click()
        await page.wait_for_timeout(800)
    except Exception as e:
        log.info(f"ADD OPENING HOURS не найдена: {e}")
        return "❌ Не найдена кнопка ADD OPENING HOURS. Ничего не изменено."

    log.info("Жду модальное окно...")

    # Ждём модальное окно
    try:
        modal = await page.wait_for_selector(".modal-body, [role='dialog']", timeout=5000)
    except Exception:
        return "❌ Модальное окно не открылось. Ничего не изменено."

    await page.wait_for_timeout(500)

    # Открываем дропдаун стран
    try:
        # Кликаем по smart__dropdown чтобы открыть поиск
        await page.wait_for_timeout(300)

        dropdown_toggle = await modal.query_selector(
            ".smart__dropdown, [class*='smart__dropdown'], .smart__dropdown__input__element"
        )
        if dropdown_toggle:
            await dropdown_toggle.click()
            await page.wait_for_timeout(600)
            log.info("Кликнул по smart__dropdown")

        # Ищем поле поиска — у него id заканчивается на __search-input
        search_input = await page.query_selector("input[id*='search-input']")
        if not search_input:
            search_input = await page.query_selector("input[id*='search']")
        if not search_input:
            # Ищем input внутри dropdown__menu
            search_input = await page.query_selector(
                ".bg-white input[type='text'], "
                "[class*='dropdown__menu'] input, "
                "[class*='dropdown-content-header'] input"
            )
        log.info(f"Search input найден: {search_input is not None}")

        if search_input:
            await search_input.click()
            await search_input.click(click_count=3)
            await search_input.type(country, delay=50)
            # Ждём пока список отфильтруется (не 100 элементов)
            for _ in range(20):
                await page.wait_for_timeout(200)
                items_count = await page.evaluate("() => document.querySelectorAll('li.dropdown-item').length")
                if items_count < 10:
                    break
            log.info(f"Введено: '{country}', элементов в списке: {items_count}")
        else:
            log.info("Поле поиска не найдено!")

        # Ждём dropdown-item
        try:
            await page.wait_for_selector("li.dropdown-item", timeout=5000)
        except Exception:
            log.info("dropdown-item не появился")

        found = False
        items = await page.query_selector_all("li.dropdown-item")
        log.info(f"Элементов после поиска: {len(items)}")
        for item in items:
            txt = (await item.inner_text()).strip()
            log.info(f"  '{txt}'")
            if country.lower() in txt.lower():
                await item.click()
                found = True
                log.info(f"Выбрано: {country}")
                await page.wait_for_timeout(400)
                break

        if not found:
            return f"❌ Страна «{country}» не найдена в списке. Ничего не изменено."

    except Exception as e:
        return f"❌ Ошибка при выборе страны: {e}"

    # Vue перерендеривает модалку после выбора страны — переполучаем её
    await page.wait_for_timeout(600)
    modal = await page.query_selector(".modal-body, [role='dialog']")
    if not modal:
        return "❌ Модальное окно закрылось после выбора страны."
    log.info("Модалка переполучена после выбора страны")

    # ── Парсим время ──────────────────────────
    sh, sm = (start.split(":") + ["00"])[:2]
    eh, em = (end.split(":") + ["00"])[:2]
    start_val = f"{sh.zfill(2)}:{sm.zfill(2)}"
    end_val   = f"{eh.zfill(2)}:{em.zfill(2)}"

    # Устанавливаем время через JS
    all_inputs = await modal.query_selector_all("input")
    time_inputs = []
    for inp in all_inputs:
        inp_type = await inp.get_attribute("type") or ""
        inp_cls  = await inp.get_attribute("class") or ""
        if "timepicker" in inp_cls and inp_type == "text":
            time_inputs.append(inp)
    log.info(f"Timepicker inputs найдено: {len(time_inputs)}")

    async def set_timepicker(inp, val):
        """Установить значение timepicker так чтобы Vue увидел изменение."""
        await inp.click(click_count=3)  # выделяем всё
        await inp.type(val)
        await inp.evaluate(
            f"el => {{ "
            f"el.value = '{val}'; "
            f"el.dispatchEvent(new Event('focus', {{bubbles:true}})); "
            f"el.dispatchEvent(new Event('input', {{bubbles:true}})); "
            f"el.dispatchEvent(new Event('change', {{bubbles:true}})); "
            f"el.dispatchEvent(new Event('blur',  {{bubbles:true}})); "
            f"}}"
        )
        await page.wait_for_timeout(150)

    for i in range(0, len(time_inputs), 2):
        await set_timepicker(time_inputs[i], start_val)
        if i + 1 < len(time_inputs):
            await set_timepicker(time_inputs[i+1], end_val)

    await page.wait_for_timeout(300)

    # ── Отключаем дни ─────────────────────────
    if days_filter and "all" not in [d.lower() for d in days_filter]:
        all_days = ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"]
        days_to_keep = [d.lower() for d in days_filter]
        days_to_disable = [d for d in all_days if d not in days_to_keep]
        checkboxes = await modal.query_selector_all("input[type='checkbox']")
        for cb in checkboxes:
            label_text = await cb.evaluate("el => el.closest('label,tr,div')?.textContent?.toLowerCase() || ''")
            if "no traffic" in label_text:
                continue
            for day in days_to_disable:
                if day in label_text:
                    if await cb.is_checked():
                        await cb.evaluate("el => el.click()")
                    break

    # ── No traffic ────────────────────────────
    if no_traffic:
        checkboxes = await modal.query_selector_all("input[type='checkbox']")
        for cb in checkboxes:
            label_text = await cb.evaluate("el => el.closest('label,tr,div')?.textContent || ''")
            if "no traffic" in label_text.lower():
                if not await cb.is_checked():
                    await cb.evaluate("el => el.click()")
                    await page.wait_for_timeout(100)

    # ── Сохраняем ─────────────────────────────
    try:
        save_btn = await page.wait_for_selector("text=SAVE OPENING HOURS", timeout=3000)
        await save_btn.click()
        await page.wait_for_timeout(700)
        return f"✅ Добавлены часы для {country}: {start}–{end}"
    except Exception:
        return "⚠️ Не найдена кнопка Save. Возможно данные не сохранились."


async def action_get_broker_revenue(broker_id: str, countries: list) -> str:
    """Узнать прайс брокера для указанных стран."""
    page = await get_page()

    base_path = await find_and_open_broker(page, broker_id)
    if not base_path:
        return f"❌ Брокер «{broker_id}» не найден."

    rev_url = f"{CRM_URL.rstrip('/')}{base_path}/revenues"
    await page.goto(rev_url, wait_until="domcontentloaded", timeout=60000)
    await page.wait_for_timeout(1000)

    # Ждём загрузки таблицы
    try:
        await page.wait_for_selector("table tr td, .table tr td", timeout=8000)
    except Exception:
        pass
    await page.wait_for_timeout(500)

    table_data = await page.evaluate("""() => {
        const result = [];
        const rows = document.querySelectorAll("table tbody tr, table tr");
        rows.forEach(row => {
            const tds = row.querySelectorAll("td");
            if (tds.length < 3) return;
            const country = tds[0]?.innerText?.trim() || "";
            const amount  = tds[2]?.innerText?.trim() || "";
            if (country && amount && !country.toLowerCase().includes("country")) {
                result.push({country, amount});
            }
        });
        return result;
    }""")
    log.info(f"Прайсы брокера {broker_id}: {table_data}")

    if not table_data:
        return f"❌ У брокера {broker_id} нет прайсов."

    results = []
    for country in countries:
        found = None
        for row in table_data:
            if country.lower() in row["country"].lower():
                found = row
                break
        if found:
            results.append(f"✅ {found['country']}: {found['amount']}")
        else:
            results.append(f"❌ {country}: прайс не найден")

    return "\n".join(results)


async def action_get_hours(broker_id: str, countries: list) -> str:
    """Узнать текущие часы работы брокера для указанных стран."""
    page = await get_page()

    base_path = await find_and_open_broker(page, broker_id)
    if not base_path:
        return f"❌ Брокер «{broker_id}» не найден."

    oh_url = f"{CRM_URL.rstrip('/')}{base_path}/opening_hours"
    await page.goto(oh_url, wait_until="domcontentloaded", timeout=60000)

    # Ждём загрузки таблицы
    try:
        await page.wait_for_selector("button.btn-primary.btn-sm, button.btn.btn-sm.btn-primary", timeout=10000)
    except Exception:
        pass
    await page.wait_for_timeout(500)

    # Скрапим все данные о часах со страницы
    hours_data = await page.evaluate("""() => {
        const allDays = ['monday','tuesday','wednesday','thursday','friday','saturday','sunday'];
        const result = [];
        const rows = document.querySelectorAll('table tr, .table tr');

        rows.forEach(row => {
            const tds = row.querySelectorAll('td');
            if (tds.length < 2) return;

            const firstTd = tds[0]?.innerText?.trim() || '';
            // Пропускаем строки где первая колонка = день недели (вложенные строки)
            if (allDays.includes(firstTd.toLowerCase())) return;
            // Пропускаем пустые
            if (!firstTd) return;

            // Это строка страны — собираем её расписание
            // Ищем вложенные строки с днями внутри этого блока
            const countryName = firstTd;
            const schedule = [];

            // Дни могут быть в этой же строке или в следующих
            // Сначала ищем текст содержащий дни и время
            const allText = row.innerText;
            allDays.forEach(day => {
                const dayCapital = day.charAt(0).toUpperCase() + day.slice(1);
                // Ищем паттерн: "Monday    11:00 - 18:00" или "Monday    closed"
                const regex = new RegExp(dayCapital + '\\\\s+([\\\\d:]+\\\\s*[-–]\\\\s*[\\\\d:]+|closed)', 'i');
                const match = allText.match(regex);
                if (match) {
                    schedule.push({day: dayCapital, time: match[1].trim()});
                }
            });

            if (schedule.length > 0) {
                result.push({country: countryName, schedule: schedule});
            }
        });

        return result;
    }""")
    log.info(f"Часы брокера {broker_id}: {len(hours_data)} стран найдено")

    if not hours_data:
        return f"❌ У брокера {broker_id} нет расписания."

    # Фильтруем по запрошенным странам
    results = []
    filter_all = "all" in [c.lower() for c in countries]

    for entry in hours_data:
        country_name = entry["country"]
        schedule = entry["schedule"]

        if not filter_all:
            if not any(c.lower() in country_name.lower() for c in countries):
                continue

        # Группируем одинаковые часы для компактного вывода
        # Например: Mon-Fri 11:00-18:00, Sat-Sun closed
        groups = []
        current_time = None
        current_days = []

        for item in schedule:
            if item["time"] == current_time:
                current_days.append(item["day"])
            else:
                if current_days:
                    groups.append((current_days, current_time))
                current_days = [item["day"]]
                current_time = item["time"]
        if current_days:
            groups.append((current_days, current_time))

        # Форматируем
        day_abbr = {"Monday": "Mon", "Tuesday": "Tue", "Wednesday": "Wed",
                    "Thursday": "Thu", "Friday": "Fri", "Saturday": "Sat", "Sunday": "Sun"}
        parts = []
        for days, time_str in groups:
            if len(days) == 1:
                day_str = day_abbr.get(days[0], days[0])
            elif len(days) == len(schedule):
                day_str = "Mon-Sun"
            else:
                day_str = f"{day_abbr.get(days[0], days[0])}-{day_abbr.get(days[-1], days[-1])}"
            parts.append(f"{day_str}: {time_str}")

        schedule_str = ", ".join(parts)
        results.append(f"🕐 {country_name}: {schedule_str}")

    if not results:
        missing = ", ".join(countries)
        return f"❌ Страны ({missing}) не найдены у брокера."

    return "\n".join(results)


async def action_get_affiliate_revenue(affiliate_id: str, countries: list) -> str:
    """Узнать прайс аффилиата для указанных стран."""
    page = await get_page()

    affiliate_id = str(affiliate_id).strip()
    if affiliate_id.isdigit():
        aff_base = f"/sources/{affiliate_id}"
    else:
        return f"❌ Укажи числовой ID аффилиата."

    payouts_url = f"{CRM_URL.rstrip('/')}{aff_base}/payouts"
    await page.goto(payouts_url, wait_until="domcontentloaded", timeout=60000)
    await page.wait_for_timeout(1500)

    # Читаем всю таблицу прайсов
    # Ждём пока Loading исчезнет и таблица заполнится реальными данными
    for _ in range(20):
        await page.wait_for_timeout(500)
        loading = await page.query_selector("td:has-text('Loading'), .loading, [class*='loading']")
        if not loading:
            # Проверяем что в таблице уже есть данные (не пусто)
            row_count = await page.evaluate("() => document.querySelectorAll('table tbody tr').length")
            if row_count > 0:
                break
    await page.wait_for_timeout(500)

    # Логируем сырые данные таблицы для диагностики
    raw_rows = await page.evaluate("""() => {
        const rows = document.querySelectorAll("table tr, [role='row']");
        return Array.from(rows).slice(0, 3).map(row => {
            const tds = row.querySelectorAll("td, [role='cell']");
            return Array.from(tds).map(td => td.innerText.trim());
        });
    }""")
    log.info(f"Сырые строки таблицы аффилиата: {raw_rows}")

    table_data = await page.evaluate("""() => {
        const result = [];
        const rows = document.querySelectorAll("table tbody tr, table tr, [role='row']");
        rows.forEach(row => {
            const tds = row.querySelectorAll("td, [role='cell']");
            if (tds.length < 5) return;
            const country = tds[2]?.innerText?.trim() || "";
            const amount  = tds[4]?.innerText?.trim() || "";
            if (country && amount && !country.toLowerCase().includes("country")) {
                result.push({country, amount});
            }
        });
        return result;
    }""")
    log.info(f"Прайсы аффилиата {affiliate_id}: {table_data}")

    if not table_data:
        return f"❌ У аффилиата {affiliate_id} нет прайсов."

    results = []
    for country in countries:
        found = None
        for row in table_data:
            if country.lower() in row["country"].lower():
                found = row
                break
        if found:
            results.append(f"✅ {found['country']}: {found['amount']}")
        else:
            results.append(f"❌ {country}: прайс не найден")

    return "\n".join(results)


async def action_add_affiliate_revenue(affiliate_id: str, country: str, amount: str) -> str:
    """Добавить прайс (payout) для аффилиата."""
    page = await get_page()

    # Идём напрямую по ID если это число
    affiliate_id = str(affiliate_id).strip()
    if affiliate_id.isdigit():
        aff_base = f"/sources/{affiliate_id}"
        test_url = f"{CRM_URL.rstrip('/')}{aff_base}/settings"
        await page.goto(test_url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(800)
        if "login" in page.url.lower() or "/sources?" in page.url:
            return f"❌ Аффилиат «{affiliate_id}» не найден."
        log.info(f"Аффилиат найден напрямую: {aff_base}")
    else:
        # Поиск по имени
        aff_url = f"{CRM_URL.rstrip('/')}/sources?search="
        await page.goto(aff_url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(1500)
        search = await page.query_selector("input[placeholder*='affiliate' i], input[placeholder*='Search' i]")
        if search:
            await search.click(click_count=3)
            await search.type(affiliate_id, delay=50)
            await page.wait_for_timeout(1500)
        settings_link = await page.query_selector("a[href*='/sources/'][href*='/settings']")
        if not settings_link:
            return f"❌ Аффилиат «{affiliate_id}» не найден."
        href = await settings_link.get_attribute("href")
        aff_base = href.replace("/settings", "")
        log.info(f"Аффилиат найден по имени: {aff_base}")

    # Переходим на FTDs Payout (всегда заново)
    payouts_url = f"{CRM_URL.rstrip('/')}{aff_base}/payouts"
    await page.goto(payouts_url, wait_until="domcontentloaded", timeout=60000)
    await page.wait_for_timeout(1500)

    # Проверяем — есть ли уже запись для этой страны
    existing_pencil = None
    if country.lower() != "all":
        rows = await page.query_selector_all("table tr")
        for row in rows:
            country_td = await row.query_selector("td:nth-child(3)")
            if not country_td:
                continue
            td_text = (await country_td.inner_text()).strip()
            if country.lower() in td_text.lower():
                existing_pencil = await row.query_selector("button.btn-outline-primary")
                if existing_pencil:
                    # Читаем текущую сумму из таблицы (колонка Amount)
                    amount_td = await row.query_selector("td:nth-child(6), td:nth-child(5)")
                    old_amount = (await amount_td.inner_text()).strip() if amount_td else "?"
                    old_amount = old_amount.replace("$", "").strip()
                    log.info(f"Запись для {country} уже существует (${old_amount}) — редактирую")
                    break

    if existing_pencil:
        # Редактируем существующую запись
        await existing_pencil.click()
        await page.wait_for_timeout(600)
        try:
            modal = await page.wait_for_selector(".modal-body, [role='dialog']", timeout=5000)
        except Exception:
            return "❌ Модальное окно не открылось."
        await page.wait_for_timeout(400)

        amount_input = await modal.query_selector("input[type='text'].form-control")
        if not amount_input:
            all_inputs = await modal.query_selector_all("input")
            for inp in all_inputs:
                inp_id = await inp.get_attribute("id") or ""
                inp_type = await inp.get_attribute("type") or ""
                if "search" not in inp_id and inp_type != "checkbox":
                    amount_input = inp
                    break

        if amount_input:
            await amount_input.click(click_count=3)
            await amount_input.type(str(amount))
            await page.wait_for_timeout(300)
            log.info(f"Обновлена сумма: {amount}")
        else:
            return "❌ Поле Amount не найдено."

        await page.wait_for_timeout(400)
        try:
            save_btn = await page.wait_for_selector(
                ".modal button[type='submit'], .modal-footer button[type='submit'], .modal .btn-ladda",
                timeout=5000
            )
            await save_btn.click()
            await page.wait_for_timeout(1000)
            country_label = country if country.lower() != "all" else "все страны"
            log.info(f"Прайс обновлён для {country_label}: ${old_amount} → ${amount}")
            return f"✅ {country_label}: ${old_amount} → ${amount}"
        except Exception as e:
            await _close_modal(page)
            return "⚠️ Не найдена кнопка Save."

    # Записи нет — добавляем новую
    try:
        add_btn = await page.wait_for_selector(
            "button:has-text('ADD PAYOUT'), a:has-text('ADD PAYOUT'), .btn:has-text('ADD PAYOUT')",
            timeout=12000
        )
        await add_btn.click()
        await page.wait_for_timeout(800)
        log.info("Кнопка ADD PAYOUT нажата")
    except Exception:
        return "❌ Не найдена кнопка ADD PAYOUT."

    try:
        modal = await page.wait_for_selector(".modal-body, [role='dialog']", timeout=5000)
    except Exception:
        return "❌ Модальное окно не открылось."
    await page.wait_for_timeout(500)

    # Выбираем страну
    if country.lower() != "all":
        dropdown_toggle = await modal.query_selector(
            ".smart__dropdown, [class*='smart__dropdown'], .smart__dropdown__input__element"
        )
        if dropdown_toggle:
            await dropdown_toggle.click()
            await page.wait_for_timeout(400)

        search_input = await page.query_selector("input[id*='skgjexg__search-input'], input[id*='search-input'], input[id*='search']")
        if not search_input:
            search_input = await page.query_selector(".bg-white input[type='text'], [class*='dropdown__menu'] input")

        if search_input:
            await search_input.click(click_count=3)
            await search_input.type(country, delay=50)
            for _ in range(20):
                await page.wait_for_timeout(200)
                cnt = await page.evaluate("() => document.querySelectorAll('li.dropdown-item, .dropdown-item').length")
                if cnt < 10:
                    break
            log.info(f"Введено в поиск: '{country}'")

        try:
            await page.wait_for_selector("li.dropdown-item, .dropdown-item", timeout=5000)
        except Exception:
            pass

        items = await page.query_selector_all("li.dropdown-item, .dropdown-item")
        log.info(f"Элементов в дропдауне: {len(items)}")
        found = False
        for item in items:
            txt = (await item.inner_text()).strip()
            if country.lower() in txt.lower():
                await item.click()
                found = True
                log.info(f"Страна {country} выбрана!")
                await page.wait_for_timeout(400)
                break

        if not found:
            await _close_modal(page)
            return f"❌ Страна «{country}» не найдена. Ничего не изменено."

    await page.wait_for_timeout(500)
    modal = await page.query_selector(".modal-body, [role='dialog']")
    if not modal:
        return "❌ Модальное окно закрылось после выбора страны."

    amount_input = await modal.query_selector("input[type='text'].form-control, input[type='number'], input[placeholder*='amount' i]")
    if not amount_input:
        all_inputs = await modal.query_selector_all("input")
        for inp in all_inputs:
            inp_id = await inp.get_attribute("id") or ""
            inp_type = await inp.get_attribute("type") or ""
            if "search" not in inp_id and inp_type != "checkbox":
                amount_input = inp
                break

    if amount_input:
        await amount_input.click(click_count=3)
        await amount_input.type(str(amount))
        await page.wait_for_timeout(300)
        log.info(f"Введена сумма: {amount}")
    else:
        return "❌ Поле Amount не найдено."

    await page.wait_for_timeout(400)
    try:
        save_btn = await page.wait_for_selector(
            ".modal button[type='submit'], .modal-footer button[type='submit'], .modal .btn-ladda",
            timeout=5000
        )
        log.info("Кнопка Save найдена, кликаю...")
        await save_btn.click()
        await page.wait_for_timeout(1000)
        country_label = country if country.lower() != "all" else "все страны"
        log.info(f"Прайс аффилиата сохранён для {country_label}: ${amount}")
        return f"✅ Прайс добавлен для {country_label}: ${amount}"
    except Exception as e:
        log.error(f"Кнопка Save не найдена: {e}")
        await _close_modal(page)
        return "⚠️ Не найдена кнопка Save."


async def action_add_revenue(broker_id: str, country: str, amount: str) -> str:
    """Добавить или обновить прайс (revenue) для страны брокера."""
    page = await get_page()

    base_path = await find_and_open_broker(page, broker_id)
    if not base_path:
        return f"❌ Брокер «{broker_id}» не найден. Ничего не изменено."

    # Переходим на страницу FTDs Revenue
    rev_url = f"{CRM_URL.rstrip('/')}{base_path}/revenues"
    await page.goto(rev_url, wait_until="domcontentloaded", timeout=60000)
    await page.wait_for_timeout(1000)

    # Проверяем — есть ли уже запись для этой страны
    existing_pencil = None
    old_amount = None
    if country.lower() != "all":
        try:
            await page.wait_for_selector("table tr td", timeout=5000)
        except Exception:
            pass
        rows = await page.query_selector_all("table tr")
        for row in rows:
            country_td = await row.query_selector("td:first-child")
            if not country_td:
                continue
            td_text = (await country_td.inner_text()).strip()
            if country.lower() in td_text.lower():
                # Читаем текущую сумму (3-я колонка)
                amount_td = await row.query_selector("td:nth-child(3)")
                if amount_td:
                    old_amount = (await amount_td.inner_text()).strip().replace("$", "").strip()
                existing_pencil = await row.query_selector("a.btn-primary, button.btn-primary:not(.btn-danger)")
                if existing_pencil:
                    log.info(f"Запись для {country} уже существует (${old_amount}) — редактирую")
                    break

    if existing_pencil:
        await existing_pencil.click()
        await page.wait_for_timeout(600)
        try:
            modal = await page.wait_for_selector(".modal-body, [role='dialog']", timeout=5000)
        except Exception:
            return "❌ Модальное окно не открылось."
        await page.wait_for_timeout(400)

        # Vue перерендеривает — переполучаем modal
        await page.wait_for_timeout(400)
        modal = await page.query_selector(".modal-body, [role='dialog']")

        amount_input = await modal.query_selector("input[type='text'].form-control, input[type='number']")
        if not amount_input:
            all_inputs = await modal.query_selector_all("input")
            for inp in all_inputs:
                inp_id = await inp.get_attribute("id") or ""
                inp_type = await inp.get_attribute("type") or ""
                if "search" not in inp_id and inp_type != "checkbox":
                    amount_input = inp
                    break

        if amount_input:
            await amount_input.click(click_count=3)
            await amount_input.type(str(amount))
            await page.wait_for_timeout(300)
        else:
            return "❌ Поле Amount не найдено."

        await page.wait_for_timeout(400)
        try:
            save_btn = await page.wait_for_selector(
                ".modal button[type='submit'], .modal-footer button[type='submit'], .modal .btn-ladda",
                timeout=5000
            )
            await save_btn.click()
            await page.wait_for_timeout(1000)
            country_label = country if country.lower() != "all" else "все страны"
            if old_amount:
                return f"✅ {country_label}: ${old_amount} → ${amount}"
            return f"✅ Прайс обновлён для {country_label}: ${amount}"
        except Exception:
            await _close_modal(page)
            return "⚠️ Не найдена кнопка Save."

    # Записи нет — добавляем новую
    # Нажимаем ADD REVENUE или ADD THE FIRST REVENUE
    try:
        add_btn = await page.wait_for_selector(
            "button:has-text('ADD REVENUE'), button:has-text('ADD THE FIRST REVENUE')",
            timeout=10000
        )
        await add_btn.click()
        await page.wait_for_timeout(800)
        log.info("Кнопка ADD REVENUE нажата")
    except Exception:
        return "❌ Не найдена кнопка ADD REVENUE. Ничего не изменено."

    # Ждём модалку
    try:
        modal = await page.wait_for_selector(".modal-body, [role='dialog']", timeout=5000)
    except Exception:
        return "❌ Модальное окно не открылось."

    await page.wait_for_timeout(500)

    # ── Выбираем страну ───────────────────────
    if country.lower() != "all":
        dropdown_toggle = await modal.query_selector(
            ".smart__dropdown, [class*='smart__dropdown']"
        )
        if dropdown_toggle:
            await dropdown_toggle.click()
            await page.wait_for_timeout(600)
            log.info("Кликнул по dropdown")

        search_input = await page.query_selector("input[id*='search-input']")
        if not search_input:
            search_input = await page.query_selector("input[id*='search']")

        if search_input:
            await search_input.click(click_count=3)
            await search_input.type(country, delay=50)
            # Ждём пока список отфильтруется
            for _ in range(20):
                await page.wait_for_timeout(200)
                items_count = await page.evaluate("() => document.querySelectorAll('li.dropdown-item').length")
                if items_count < 10:
                    break
            log.info(f"Введено в поиск: '{country}', элементов: {items_count}")
        else:
            log.info("Поле поиска страны не найдено!")

        try:
            await page.wait_for_selector("li.dropdown-item", timeout=5000)
        except Exception:
            pass

        items = await page.query_selector_all("li.dropdown-item")
        log.info(f"Элементов в дропдауне: {len(items)}")
        found = False
        for item in items:
            txt = (await item.inner_text()).strip()
            log.info(f"  '{txt}'")
            if country.lower() in txt.lower():
                await item.click()
                found = True
                log.info(f"Страна {country} выбрана!")
                await page.wait_for_timeout(400)
                break

        if not found:
            return f"❌ Страна «{country}» не найдена. Ничего не изменено."

    # ── Вводим сумму ──────────────────────────
    # Закрываем дропдаун если открыт — кликаем в безопасное место
    await page.keyboard.press("Escape")
    await page.wait_for_timeout(300)

    amount_input = await modal.query_selector(
        "input[type='number'], input[placeholder*='amount' i], "
        "input[placeholder*='Amount' i]"
    )
    if not amount_input:
        # Ищем все input в модалке кроме поиска стран
        all_inputs = await modal.query_selector_all("input")
        for inp in all_inputs:
            inp_id = await inp.get_attribute("id") or ""
            inp_type = await inp.get_attribute("type") or ""
            if "search" not in inp_id and inp_type != "checkbox":
                amount_input = inp
                break

    if amount_input:
        await amount_input.click()
        await amount_input.fill(str(amount))
        await page.wait_for_timeout(300)
        log.info(f"Введена сумма: {amount}")
    else:
        return "❌ Поле Amount не найдено."

    # ── Сохраняем ─────────────────────────────
    # Даём Vue время обработать ввод суммы
    await page.wait_for_timeout(500)
    try:
        # Ищем кнопку Save строго внутри модалки
        save_btn = await page.wait_for_selector(
            ".modal button[type='submit'], .modal-footer button[type='submit'], "
            "[role='dialog'] button[type='submit'], .modal .btn-ladda",
            timeout=5000
        )
        log.info("Кнопка Save найдена, кликаю...")
        await save_btn.click()
        await page.wait_for_timeout(1000)
        country_label = country if country.lower() != "all" else "все страны"
        log.info(f"Прайс сохранён для {country_label}: ${amount}")
        return f"✅ Прайс добавлен для {country_label}: ${amount}"
    except Exception as e:
        log.error(f"Кнопка Save не найдена: {e}")
        # Логируем все кнопки в модалке для диагностики
        btns = await page.evaluate("""() => {
            const modal = document.querySelector('.modal, [role=dialog]');
            if (!modal) return [];
            return Array.from(modal.querySelectorAll('button')).map(b => b.innerText.trim());
        }""")
        log.info(f"Кнопки в модалке: {btns}")
        await _close_modal(page)
        return "⚠️ Не найдена кнопка Save."


async def _close_modal(page) -> None:
    """Закрыть модальное окно если оно открыто."""
    try:
        close_btn = await page.query_selector(".modal .close, .modal button[aria-label='Close'], [role='dialog'] .close")
        if close_btn:
            await close_btn.click()
            await page.wait_for_timeout(400)
            return
    except Exception:
        pass
    # Запасной вариант — Escape
    await page.keyboard.press("Escape")
    await page.wait_for_timeout(400)


async def _close_days_for_pencil(page, pencil, country_name: str, days_to_close: list) -> str:
    """Вспомогательная: открыть модалку по карандашу и закрыть дни."""
    await pencil.click()
    await page.wait_for_timeout(600)

    try:
        modal = await page.wait_for_selector(".modal-body, [role='dialog']", timeout=4000)
    except Exception:
        return f"❌ {country_name}: модальное окно не открылось."

    await page.wait_for_timeout(300)

    days_lower = [d.lower() for d in days_to_close]
    checkboxes = await modal.query_selector_all("input[type='checkbox']")
    closed = []
    for cb in checkboxes:
        label_text = await cb.evaluate("el => el.closest('label,tr,div')?.textContent?.toLowerCase() || ''")
        if "no traffic" in label_text:
            continue
        for day in days_lower:
            if day in label_text:
                if await cb.is_checked():
                    await cb.evaluate("el => el.click()")
                    await page.wait_for_timeout(100)
                    closed.append(day.capitalize())
                break

    if not closed:
        # Дни уже закрыты — просто закрываем модалку без сохранения
        await _close_modal(page)
        return f"⚠️ {country_name}: дни {days_to_close} уже закрыты или не найдены."

    try:
        save_btn = await page.wait_for_selector("text=SAVE OPENING HOURS", timeout=3000)
        await save_btn.click()
        await page.wait_for_timeout(700)
        return f"✅ {country_name}: закрыты дни {', '.join(closed)}."
    except Exception:
        await _close_modal(page)
        return f"⚠️ {country_name}: кнопка Save не найдена."


async def action_close_days(broker_id: str, country: str, days_to_close: list) -> str:
    """Закрыть конкретные дни для страны (или всех стран) брокера."""
    page = await get_page()

    base_path = await find_and_open_broker(page, broker_id)
    if not base_path:
        return f"❌ Брокер «{broker_id}» не найден. Ничего не изменено."

    oh_url = f"{CRM_URL.rstrip('/')}{base_path}/opening_hours"
    await page.goto(oh_url, wait_until="domcontentloaded", timeout=60000)

    # Ждём пока Vue отрендерит карандаши
    try:
        await page.wait_for_selector(
            "button.btn-primary.btn-sm, button.btn.btn-sm.btn-primary",
            timeout=12000
        )
    except Exception:
        pass
    await page.wait_for_timeout(500)

    log.info(f"Закрываю дни: {days_to_close} для страны: {country}")

    # Собираем все карандаши с именами стран
    edit_buttons = await page.query_selector_all("button.btn-primary.btn-sm, button.btn.btn-sm.btn-primary")
    pencil_buttons = []
    for btn in edit_buttons:
        if not (await btn.inner_text()).strip():
            c_name = await btn.evaluate("""el => {
                const row = el.closest('tr');
                return row ? row.querySelector('td')?.innerText?.trim() : '';
            }""")
            pencil_buttons.append((btn, c_name))

    log.info(f"Найдено стран: {[n for _, n in pencil_buttons]}")

    # Режим "все страны"
    if country.lower() == "all":
        if not pencil_buttons:
            return "❌ Страны не найдены у брокера."
        results = []
        for pencil, c_name in pencil_buttons:
            # После сохранения модалки страница перерендеривается — нужно заново найти карандаш
            await page.wait_for_timeout(300)
            fresh_buttons = await page.query_selector_all("button.btn-primary.btn-sm, button.btn.btn-sm.btn-primary")
            fresh_pencils = []
            for btn in fresh_buttons:
                if not (await btn.inner_text()).strip():
                    fresh_pencils.append(btn)

            # Ищем карандаш по имени страны среди свежих кнопок
            target = None
            for btn in fresh_pencils:
                name = await btn.evaluate("""el => {
                    const row = el.closest('tr');
                    return row ? row.querySelector('td')?.innerText?.trim() : '';
                }""")
                if name.strip().lower() == c_name.strip().lower():
                    target = btn
                    break

            if not target:
                results.append(f"⚠️ {c_name}: карандаш не найден после обновления.")
                continue

            msg = await _close_days_for_pencil(page, target, c_name, days_to_close)
            results.append(msg)
            log.info(msg)

        return "\n".join(results)

    # Режим конкретной страны
    target_pencil = None
    target_name = country
    for pencil, c_name in pencil_buttons:
        if country.lower() in c_name.lower():
            target_pencil = pencil
            target_name = c_name
            break

    if not target_pencil:
        return f"❌ Страна «{country}» не найдена у брокера. Ничего не изменено."

    return await _close_days_for_pencil(page, target_pencil, target_name, days_to_close)


async def action_toggle_broker(broker_id: str, activate: bool) -> str:
    """Включить или выключить брокера."""
    page = await get_page()

    base_path = await find_and_open_broker(page, broker_id)
    if not base_path:
        return f"❌ Брокер «{broker_id}» не найден. Ничего не изменено."

    # Переходим напрямую на страницу Settings
    settings_url = f"{CRM_URL.rstrip('/')}{base_path}/settings"
    await page.goto(settings_url, wait_until="domcontentloaded", timeout=60000)
    await page.wait_for_timeout(500)
    try:
        toggle = await page.wait_for_selector(
            "input[type='checkbox'][id*='active'], label:has-text('Broker is active') input",
            timeout=5000
        )
        is_checked = await toggle.is_checked()

        if activate and not is_checked:
            await toggle.check()
        elif not activate and is_checked:
            await toggle.uncheck()
        else:
            state = "уже включён" if activate else "уже выключен"
            return f"ℹ️ Брокер «{broker_id}» {state}. Ничего не изменено."

        # Сохраняем
        save = await page.wait_for_selector("text=SAVE SETTINGS", timeout=4000)
        await save.click()
        await page.wait_for_timeout(500)

        action_word = "включён" if activate else "выключен"
        return f"✅ Брокер «{broker_id}» успешно {action_word}."

    except Exception as e:
        return f"❌ Не могу изменить статус брокера: {e}\nНичего не изменено."


# ══════════════════════════════════════════
#  TELEGRAM BOT
# ══════════════════════════════════════════

def escape_md(text: str) -> str:
    """Экранировать спецсимволы Markdown v1 в тексте для Telegram."""
    for ch in ('_', '*', '`', '['):
        text = text.replace(ch, f'\\{ch}')
    return text


def build_confirm_text(action: dict) -> str:
    """Сформировать текст запроса подтверждения."""
    a = action.get("action")

    if a == "change_hours":
        h = action.get("hours", {})
        brokers = ", ".join(str(b) for b in action.get("broker_ids", []))
        countries = ", ".join(action.get("countries", ["все"]))
        days = ", ".join(action.get("days", ["все дни"]))
        return (
            f"📋 *Запрос подтверждения*\n\n"
            f"Действие: изменить часы работы\n"
            f"Брокеры: `{brokers}`\n"
            f"Время: `{h.get('start','?')} — {h.get('end','?')}`\n"
            f"Страны: {countries}\n"
            f"Дни: {days}\n"
            f"No-traffic: {'✅ да' if action.get('no_traffic', True) else '❌ нет'}\n\n"
            f"Подтверждаешь?"
        )

    if a == "add_hours":
        brokers = ", ".join(str(b) for b in action.get("broker_ids", []))
        ch_list = action.get("country_hours", [])
        schedule_groups = action.get("schedule_groups", [])
        countries_str = ", ".join(ch["country"] for ch in ch_list)

        if schedule_groups:
            # Показываем расписание по группам
            sched_lines = []
            for g in schedule_groups:
                days_str = ", ".join(g.get("days", []))
                sched_lines.append(f"  • {g.get('start')}–{g.get('end')}: {days_str}")
            sched_str = "\n".join(sched_lines)
            return (
                f"📋 *Запрос подтверждения*\n\n"
                f"Действие: добавить часы для стран\n"
                f"Брокеры: `{brokers}`\n"
                f"Страны: {countries_str}\n"
                f"Расписание:\n{sched_str}\n"
                f"No-traffic: {'✅ да' if action.get('no_traffic', True) else '❌ нет'}\n\n"
                f"Подтверждаешь?"
            )
        else:
            days = ", ".join(action.get("days_to_keep", ["пн–пт"]))
            lines = "\n".join(f"  • {ch['country']}: {ch['start']}–{ch['end']}" for ch in ch_list)
            return (
                f"📋 *Запрос подтверждения*\n\n"
                f"Действие: добавить часы для стран\n"
                f"Брокеры: `{brokers}`\n"
                f"Страны и часы:\n{lines}\n"
                f"Дни: {days}\n"
                f"No-traffic: {'✅ да' if action.get('no_traffic', True) else '❌ нет'}\n\n"
                f"Подтверждаешь?"
            )

    if a == "toggle_broker":
        brokers = ", ".join(str(b) for b in action.get("broker_ids", []))
        word = "ВКЛЮЧИТЬ" if action.get("active") else "ВЫКЛЮЧИТЬ"
        return (
            f"📋 *Запрос подтверждения*\n\n"
            f"Действие: {word} брокера\n"
            f"Брокеры: `{brokers}`\n\n"
            f"Подтверждаешь?"
        )

    if a == "close_days":
        brokers = ", ".join(str(b) for b in action.get("broker_ids", []))
        cd_list = action.get("countries_days", [])
        lines = "\n".join(f"  • {cd['country']}: {', '.join(cd['days_to_close'])}" for cd in cd_list)
        return (
            f"📋 *Запрос подтверждения*\n\n"
            f"Действие: закрыть дни\n"
            f"Брокеры: `{brokers}`\n"
            f"Страны и дни:\n{lines}\n\n"
            f"Подтверждаешь?"
        )

    if a == "add_revenue":
        brokers = ", ".join(str(b) for b in action.get("broker_ids", []))
        cr_list = action.get("country_revenues", [])
        if cr_list:
            lines = "\n".join(f"  • {cr['country']}: ${cr['amount']}" for cr in cr_list)
        else:
            country = action.get("countries", ["all"])[0]
            amount = action.get("amount", "?")
            lines = f"  • {country}: ${amount}"
        return (
            f"📋 *Запрос подтверждения*\n\n"
            f"Действие: добавить прайс\n"
            f"Брокеры: `{brokers}`\n"
            f"Страны и суммы:\n{lines}\n\n"
            f"Подтверждаешь?"
        )

    if a == "add_affiliate_revenue":
        aff_id = action.get("affiliate_id", "?")
        cr_list = action.get("country_revenues", [])
        lines = "\n".join(f"  • {cr['country']}: ${cr['amount']}" for cr in cr_list) if cr_list else "  • все страны"
        return (
            f"📋 *Запрос подтверждения*\n\n"
            f"Действие: добавить прайс аффилиату\n"
            f"Аффилиат: `{aff_id}`\n"
            f"Страны и суммы:\n{lines}\n\n"
            f"Подтверждаешь?"
        )

    return f"📋 Действие: `{a}`\n\nПодтверждаешь?"


async def on_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id

    # Защита — только твой аккаунт
    if chat_id != ALLOWED_CHAT_ID:
        return

    text = update.message.text.strip()
    await update.message.reply_text("🤔 Анализирую команду…")

    action = await asyncio.get_event_loop().run_in_executor(None, parse_command, text)

    # Для add_affiliate_revenue broker_ids может быть пустым — используем affiliate_id
    if action.get("action") == "add_affiliate_revenue" and action.get("affiliate_id") and not action.get("broker_ids"):
        action["broker_ids"] = [str(action["affiliate_id"])]

    # Для get_affiliate_revenue тоже
    if action.get("action") == "get_affiliate_revenue" and action.get("affiliate_id") and not action.get("broker_ids"):
        action["broker_ids"] = [str(action["affiliate_id"])]

    # get_prices — выполняем без подтверждения
    if action.get("action") == "get_prices":
        if not action.get("broker_ids"):
            action["broker_ids"] = ["_"]  # placeholder чтобы пройти валидацию
        queries = action.get("queries", [])
        if queries:
            await update.message.reply_text("🔍 Ищу прайсы…")
            sub_results = []
            for q in queries:
                qtype = q.get("type", "broker")
                qid = q.get("id", "")
                qcountries = q.get("countries", [])
                lid = alog.log_action(f"get_{qtype}_revenue", str(qid),
                                      ", ".join(qcountries), "pending", user_command=text)
                if qtype == "affiliate":
                    sub_msg = await action_get_affiliate_revenue(str(qid), qcountries)
                    sub_results.append(f"*Афф {escape_md(str(qid))}:*\n{escape_md(sub_msg)}")
                else:
                    sub_msg = await action_get_broker_revenue(str(qid), qcountries)
                    sub_results.append(f"*Брокер {escape_md(str(qid))}:*\n{escape_md(sub_msg)}")
                alog.update_action(lid, "success" if "❌" not in sub_msg else "error", sub_msg[:200])
            alog.set_status("last_action", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            await update.message.reply_text("\n\n".join(sub_results), parse_mode="Markdown")
            return

    # get_broker_revenue и get_affiliate_revenue — тоже без подтверждения
    if action.get("action") in ("get_broker_revenue", "get_affiliate_revenue"):
        await update.message.reply_text("🔍 Ищу прайс…")
        if action.get("action") == "get_broker_revenue":
            for bid in action.get("broker_ids", []):
                lid = alog.log_action("get_broker_revenue", str(bid),
                                      ", ".join(action.get("countries", [])), "pending", user_command=text)
                result = await action_get_broker_revenue(str(bid), action.get("countries", []))
                alog.update_action(lid, "success" if "❌" not in result else "error", result[:200])
                await update.message.reply_text(f"*Брокер {escape_md(str(bid))}:*\n{escape_md(result)}", parse_mode="Markdown")
        else:
            aff_id = str(action.get("affiliate_id") or action.get("broker_ids", ["?"])[0])
            lid = alog.log_action("get_affiliate_revenue", aff_id,
                                  ", ".join(action.get("countries", [])), "pending", user_command=text)
            result = await action_get_affiliate_revenue(aff_id, action.get("countries", []))
            alog.update_action(lid, "success" if "❌" not in result else "error", result[:200])
            await update.message.reply_text(f"*Афф {escape_md(aff_id)}:*\n{escape_md(result)}", parse_mode="Markdown")
        alog.set_status("last_action", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        return

    # get_hours — узнать часы работы, тоже без подтверждения
    if action.get("action") == "get_hours":
        await update.message.reply_text("🔍 Ищу часы…")
        for bid in action.get("broker_ids", []):
            lid = alog.log_action("get_hours", str(bid),
                                  ", ".join(action.get("countries", ["all"])), "pending", user_command=text)
            result = await action_get_hours(str(bid), action.get("countries", ["all"]))
            alog.update_action(lid, "success" if "❌" not in result else "error", result[:200])
            await update.message.reply_text(f"*Брокер {escape_md(str(bid))}:*\n{escape_md(result)}", parse_mode="Markdown")
        alog.set_status("last_action", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        return

    if action.get("action") == "unknown" or not action.get("broker_ids"):
        log.warning(f"Команда не распознана. action={action}")
        await update.message.reply_text(
            "❓ Не понял команду. Попробуй например:\n\n"
            "• «поменяй часы Test Broker на 10-19»\n"
            "• «дай часы Nexus FR» или «wh Nexus FR»\n"
            "• «включи брокера 32»\n"
            "• «выключи брокеров 32 и 2111»"
        )
        return

    # Сохраняем и просим подтвердить
    kb = [[
        InlineKeyboardButton("✅ Выполнить", callback_data="confirm"),
        InlineKeyboardButton("❌ Отмена",    callback_data="cancel"),
    ]]
    sent = await update.message.reply_text(
        build_confirm_text(action),
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(kb)
    )
    # Ключ = (chat_id, message_id) — каждая команда получает свой слот
    action["_user_command"] = text  # сохраняем для логирования
    pending[(chat_id, sent.message_id)] = action


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat_id = update.effective_chat.id
    pending_key = (chat_id, query.message.message_id)

    if query.data == "cancel":
        pending.pop(pending_key, None)
        await query.edit_message_text("❌ Отменено. Ничего не изменено.")
        return

    action = pending.pop(pending_key, None)
    if not action:
        await query.edit_message_text("❌ Команда устарела. Отправь заново.")
        return

    await query.edit_message_text("⏳ Выполняю, это займёт несколько секунд…")

    # Логируем начало выполнения
    user_cmd = action.get("_user_command", "")
    log_ids = []

    try:
        results = []
        a = action["action"]

        for broker_id in action.get("broker_ids", []):
            lid = alog.log_action(a, str(broker_id), json.dumps(action, ensure_ascii=False)[:300],
                                  "pending", user_command=user_cmd)
            log_ids.append(lid)
            if a == "change_hours":
                h = action.get("hours", {})
                msg = await action_change_hours(
                    broker_id=str(broker_id),
                    start=h.get("start", "09:00"),
                    end=h.get("end", "17:00"),
                    countries_filter=action.get("countries", ["all"]),
                    no_traffic=action.get("no_traffic", True),
                    days_filter=action.get("days_to_keep", action.get("days", ["all"]))
                )
            elif a == "add_hours":
                country_hours_list = action.get("country_hours", [])
                # Обратная совместимость: если старый формат (countries + hours)
                if not country_hours_list:
                    h = action.get("hours", {})
                    countries = action.get("countries", [])
                    country = countries[0] if countries and "all" not in countries else ""
                    if country:
                        country_hours_list = [{"country": country, "start": h.get("start", "09:00"), "end": h.get("end", "17:00")}]

                if not country_hours_list:
                    msg = "❌ Укажи страну и часы для добавления."
                else:
                    schedule_groups = action.get("schedule_groups", [])
                    days_filter    = action.get("days_to_keep", action.get("days", ["all"]))
                    requested_day  = action.get("requested_day", "")
                    skip_missing   = action.get("skip_missing", False)

                    # Определяем является ли запрошенный день выходным
                    weekends = {"saturday", "sunday"}
                    weekdays = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
                    req_day_lower = requested_day.lower() if requested_day else ""
                    is_weekend_request = req_day_lower in weekends
                    # Нет дня = рабочие дни по умолчанию (не выходной)
                    if not requested_day:
                        is_weekend_request = False

                    # Получаем список существующих стран (открываем страницу один раз)
                    existing_countries = []
                    try:
                        page = await get_page()
                        broker_base = await find_and_open_broker(page, str(broker_id))
                        if broker_base:
                            oh_url = f"{CRM_URL.rstrip('/')}{broker_base}/opening_hours"
                            await page.goto(oh_url, wait_until="domcontentloaded", timeout=60000)
                            existing_countries = await _scrape_countries_from_page(page)
                            log.info(f"Существующие страны: {existing_countries}")
                    except Exception as e:
                        log.warning(f"Не удалось получить существующие страны: {e}")

                    sub_results = []
                    skipped = []

                    for ch in country_hours_list:
                        country_name  = ch.get("country", "")
                        country_start = ch.get("start", "09:00")
                        country_end   = ch.get("end", "17:00")

                        country_exists = any(country_name.lower() in ec.lower() for ec in existing_countries)

                        # Если есть schedule_groups — используем мульти-функцию (один проход)
                        if schedule_groups:
                            sub_msg = await action_add_country_hours_multi(
                                broker_id=str(broker_id),
                                country=country_name,
                                schedule_groups=schedule_groups,
                                no_traffic=action.get("no_traffic", True),
                                country_exists=country_exists
                            )
                            sub_results.append(sub_msg)
                            continue

                        if requested_day:
                            if country_exists:
                                # Страна есть + конкретный день → добавляем только этот день
                                log.info(f"{country_name} существует → добавляю день {requested_day}")
                                sub_msg = await action_edit_country_add_days(
                                    broker_id=str(broker_id),
                                    country=country_name,
                                    start=country_start,
                                    end=country_end,
                                    no_traffic=action.get("no_traffic", True),
                                    days_to_add=[requested_day]
                                )
                            else:
                                # Страны нет → создаём
                                if is_weekend_request:
                                    # Выходной → только этот день
                                    new_days_filter = [requested_day]
                                    log.info(f"{country_name} не существует, выходной → создаю только {new_days_filter}")
                                else:
                                    # Будний или нет дня → создаём с Пн–Пт
                                    new_days_filter = weekdays
                                    log.info(f"{country_name} не существует → создаю с Пн–Пт")
                                sub_msg = await action_add_country_hours(
                                    broker_id=str(broker_id),
                                    country=country_name,
                                    start=country_start,
                                    end=country_end,
                                    no_traffic=action.get("no_traffic", True),
                                    days_filter=new_days_filter
                                )
                        elif skip_missing and existing_countries:
                            if not country_exists:
                                skipped.append(country_name)
                                log.info(f"Пропускаю {country_name} — нет у брокера")
                                continue
                            # Страна есть + skip_missing → редактируем, только добавляем дни
                            days_to_add = days_filter if "all" not in [d.lower() for d in days_filter] else ["Saturday", "Sunday"]
                            sub_msg = await action_edit_country_add_days(
                                broker_id=str(broker_id),
                                country=country_name,
                                start=country_start,
                                end=country_end,
                                no_traffic=action.get("no_traffic", True),
                                days_to_add=days_to_add
                            )
                        else:
                            sub_msg = await action_add_country_hours(
                                broker_id=str(broker_id),
                                country=country_name,
                                start=country_start,
                                end=country_end,
                                no_traffic=action.get("no_traffic", True),
                                days_filter=days_filter
                            )
                        sub_results.append(sub_msg)

                    if skipped:
                        sub_results.append(f"⏭ Пропущены (нет у брокера): {', '.join(skipped)}")
                    msg = "\n".join(sub_results)
            elif a == "close_days":
                countries_days = action.get("countries_days", [])
                # Обратная совместимость: старый формат (одна страна)
                if not countries_days:
                    countries = action.get("countries", [])
                    country = countries[0] if countries and "all" not in countries else "all"
                    days_to_close = action.get("days_to_close", [])
                    if days_to_close:
                        countries_days = [{"country": country, "days_to_close": days_to_close}]

                if not countries_days:
                    msg = "❌ Укажи страны и дни для закрытия."
                else:
                    sub_results = []
                    for cd in countries_days:
                        sub_msg = await action_close_days(
                            broker_id=str(broker_id),
                            country=cd.get("country", "all"),
                            days_to_close=cd.get("days_to_close", [])
                        )
                        sub_results.append(sub_msg)
                    msg = "\n".join(sub_results)
            elif a == "add_revenue":
                country_revenues = action.get("country_revenues", [])
                # Обратная совместимость: старый формат (одна страна + amount)
                if not country_revenues:
                    countries = action.get("countries", [])
                    country = countries[0] if countries and "all" not in countries else "all"
                    amount = action.get("amount", "")
                    if amount:
                        country_revenues = [{"country": country, "amount": amount}]

                if not country_revenues:
                    msg = "❌ Укажи страну и сумму прайса."
                else:
                    sub_results = []
                    for cr in country_revenues:
                        sub_msg = await action_add_revenue(
                            broker_id=str(broker_id),
                            country=cr.get("country", "all"),
                            amount=str(cr.get("amount", ""))
                        )
                        sub_results.append(sub_msg)
                    msg = "\n".join(sub_results)
            elif a == "add_affiliate_revenue":
                aff_id = str(action.get("affiliate_id") or broker_id)
                cr_list = action.get("country_revenues", [])
                if not cr_list:
                    countries = action.get("countries", [])
                    country = countries[0] if countries and "all" not in countries else "all"
                    amount = action.get("amount", "")
                    if amount:
                        cr_list = [{"country": country, "amount": amount}]
                if not cr_list:
                    msg = "❌ Укажи страну и сумму прайса."
                else:
                    sub_results = []
                    for cr in cr_list:
                        sub_msg = await action_add_affiliate_revenue(
                            affiliate_id=aff_id,
                            country=cr.get("country", "all"),
                            amount=str(cr.get("amount", ""))
                        )
                        sub_results.append(sub_msg)
                    msg = "\n".join(sub_results)
            elif a == "toggle_broker":
                msg = await action_toggle_broker(str(broker_id), action.get("active", True))
            else:
                msg = f"⚠️ Действие «{a}» пока не поддерживается."

            # Выбираем правильный лейбл: Афф или Брокер
            if a in ("add_affiliate_revenue", "get_affiliate_revenue"):
                label = "Афф"
            else:
                label = "Брокер"
            results.append(f"*{label} {escape_md(str(broker_id))}:*\n{escape_md(msg)}")

        # Обновляем логи результатами
        for i, lid in enumerate(log_ids):
            bid = action.get("broker_ids", ["?"])[i] if i < len(action.get("broker_ids", [])) else "?"
            res_text = results[i] if i < len(results) else ""
            status = "error" if "❌" in res_text else "success"
            alog.update_action(lid, status, res_text[:200])
        alog.set_status("last_action", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        await context.bot.send_message(
            chat_id,
            "\n\n".join(results) or "✅ Готово.",
            parse_mode="Markdown"
        )

    except Exception as e:
        log.exception("Ошибка при выполнении действия")
        for lid in log_ids:
            alog.update_action(lid, "error", str(e)[:200])
        await context.bot.send_message(
            chat_id,
            f"❌ Произошла ошибка:\n`{escape_md(str(e))}`\n\nПроверь логи. Ничего не гарантируется.",
            parse_mode="Markdown"
        )


async def on_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != ALLOWED_CHAT_ID:
        return
    await update.message.reply_text(
        "👋 Привет! Я бот для управления LeadGreed CRM.\n\n"
        "Что умею:\n"
        "• Менять часы работы брокеров\n"
        "• Включать / выключать брокеров\n\n"
        "Пиши команду на русском — я пойму 😊"
    )


# ══════════════════════════════════════════
#  ОЧИСТКА РЕСУРСОВ
# ══════════════════════════════════════════

async def _cleanup_browser():
    """Закрыть браузер и Playwright при завершении."""
    global _browser, _playwright
    try:
        if _browser and _browser.is_connected():
            await _browser.close()
            log.info("Браузер закрыт.")
    except Exception as e:
        log.warning(f"Ошибка при закрытии браузера: {e}")
    try:
        if _playwright:
            await _playwright.stop()
            log.info("Playwright остановлен.")
    except Exception as e:
        log.warning(f"Ошибка при остановке Playwright: {e}")


def _sync_cleanup():
    """Синхронная обёртка для atexit."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.create_task(_cleanup_browser())
        else:
            loop.run_until_complete(_cleanup_browser())
    except Exception:
        pass


atexit.register(_sync_cleanup)


# ══════════════════════════════════════════
#  ЗАПУСК
# ══════════════════════════════════════════

def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", on_start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_message))
    app.add_handler(CallbackQueryHandler(on_callback))

    log.info("Бот запущен ✅")
    alog.set_status("bot_started", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()