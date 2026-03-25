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
    TELEGRAM_TOKEN, ANTHROPIC_API_KEY, ALLOWED_USERS
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

# Последнее найденное полное имя брокера (заполняется в find_and_open_broker)
_last_broker_full_name: str = ""

# LATAM страны — для автоматической маршрутизации к "Latam" вариантам брокеров
LATAM_COUNTRIES = {
    "brazil", "mexico", "colombia", "argentina", "chile", "peru", "ecuador",
    "venezuela", "bolivia", "paraguay", "uruguay", "costa rica", "panama",
    "dominican republic", "guatemala", "honduras", "el salvador", "nicaragua", "cuba"
}

# ══════════════════════════════════════════
#  ОЧЕРЕДЬ ЗАДАЧ
# ══════════════════════════════════════════

_task_queue: Optional[asyncio.Queue] = None
_queue_worker_task = None


async def _queue_worker():
    """Воркер — обрабатывает задачи из очереди по одной."""
    while True:
        task_func, args, kwargs = await _task_queue.get()
        try:
            await task_func(*args, **kwargs)
        except Exception as e:
            log.exception(f"Queue error: {e}")
        finally:
            _task_queue.task_done()


async def enqueue(task_func, *args, **kwargs):
    """Добавить задачу в очередь. Возвращает позицию в очереди."""
    await _task_queue.put((task_func, args, kwargs))
    return _task_queue.qsize()


# ══════════════════════════════════════════
#  AI — разбор команды пользователя
# ══════════════════════════════════════════

SYSTEM_PROMPT = """
Ты парсер команд для бота управления CRM LeadGreed.
Получаешь команду на русском языке и возвращаешь ТОЛЬКО JSON — без пояснений, без markdown.

Возможные action:
- change_hours  — изменить часы работы broker
- add_hours     — добавить hours for новой страны
- close_days    — закрыть конкретные дни (убрать галочки) для страны
- add_revenue   — добавить прайс/выплату для страны broker
- toggle_broker — включить / выключить broker
- add_affiliate_revenue — добавить прайс/выплату для аффилиата
- set_prices            — добавить прайсы для НЕСКОЛЬКИХ объектов (брокер + аффилиат) в одном сообщении
- get_affiliate_revenue — узнать прайс аффилиата для страны
- get_broker_revenue   — узнать прайс брокера для страны
- get_hours            — узнать текущие часы работы брокера для страны
- change_caps          — изменить дневной лимит (cap) брокера для страны
- lead_task            — комбинированная задача: поставить часы + капу одновременно (из лид-формы)
- bulk_schedule        — расписание на выходные/конкретные дни: поставить часы + закрыть другие дни (skip missing countries)
- multi_broker_task    — несколько брокеров из одного сообщения: часы, капы, закрытие для разных брокеров
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
- ВАЖНО: Никогда не склеивай имя брокера/аффилиата и страну в одно поле. ISO коды (DE, FR, ES, HR, ID, NL, CZ...) и названия стран (германия, испания...) — это ВСЕГДА countries, а не часть broker_ids. Даже если ISO код стоит СРАЗУ после имени брокера без разделителя.
  Примеры:
  • "MediaNow HR 1350" → broker_ids: ["MediaNow"], countries: ["Croatia"], amount: 1350. НЕ broker_ids: ["MediaNow HR"]!
  • "легион де" → broker_ids: ["Legion"], countries: ["Germany"]. НЕ broker_ids: ["Legion DE"]!
  • "Helios ID 1050" → broker_ids: ["Helios"], countries: ["Indonesia"], amount: 1050. НЕ broker_ids: ["Helios ID"]!
  • "Theta NL 1650" → broker_ids: ["Theta"], countries: ["Netherlands"], amount: 1650
  Правило: 2-буквенный ISO код (HR, DE, FR, ID, NL, CZ, ES...) — это ВСЕГДА страна, НИКОГДА не часть имени брокера. Единственное исключение — "MM affiliates" (это имя брокера, не Мьянма).
- ВАЖНО: Имена брокеров могут состоять из НЕСКОЛЬКИХ слов, но ТОЛЬКО в следующих случаях:
  • Суффикс CRG/CPA/CPL: "Fintrix CRG", "Nexus CPA", "Helios CRG", "Avelux CRG", "Clickbait CRG"
  • Специальные имена: "Swin FR CRG", "Swin EN CRG", "Swin FR CRG duplicate", "Swinftd CRG FR", "Swinftd CRG FR DUPLICATE", "Swinftd CRG ENG", "Swinftd FLAT FR", "Swinftd FLAT ENG", "Theta Holding", "MM affiliates", "PRX_AVE", "PRX_AVE CPA"
  ИСКЛЮЧЕНИЕ: "MM affiliates" — это имя брокера, НЕ Мьянма. "UY MM affiliates 800" → broker_ids: ["MM affiliates"], countries: ["Uruguay"], amount: 800.
  Любое другое 2-буквенное слово после имени (DE, FR, HR, ID, NL...) — это СТРАНА, а не часть имени.
  Примеры: "Fintrix CRG DE 15 cap" → broker_ids: ["Fintrix CRG"], country: "Germany". "MediaNow HR 1350" → broker_ids: ["MediaNow"], country: "Croatia".

ВАЖНО — правила маршрутизации Swin (Swinftd):
"Swin" в CRM называется "Swinftd". У Swinftd есть несколько интеграций:

ВАЖНО — маппинг AVE (PRX_AVE):
"AVE" в CRM называется "PRX_AVE". У PRX_AVE есть два варианта:
  • PRX_AVE CPA (ID 2822) — для CPA-прайсов (без процента)
  • PRX_AVE (ID 2812) — для CRG-прайсов (с процентом)
Примеры:
  "AVE CRG DE 1650 16%" → broker_ids: ["PRX_AVE"], countries: ["Germany"], amount: 1650 (CRG = без CPA суффикса)
  "AVE CPA DE 1200" → broker_ids: ["PRX_AVE CPA"], countries: ["Germany"], amount: 1200
  "AVE DE 1650 16%" → broker_ids: ["PRX_AVE"], countries: ["Germany"], amount: 1650 (есть % = CRG)
  • Swinftd CRG FR (для Франции CRG)
  • Swinftd CRG FR DUPLICATE (для Франции CRG, дубликат)
  • Swinftd CRG ENG (для всех остальных стран CRG)
  • Swinftd FLAT FR (для Франции CPA/flat)
  • Swinftd FLAT ENG (для всех остальных стран CPA/flat)
Правила выбора:
  • Если в прайсе есть % (процент) → это CRG-интеграция
  • Если страна = France → использовать FR-вариант
  • Если страна ≠ France → использовать ENG-вариант
Примеры:
  "Swin HK eng 1550 12% (5% deduct)" → broker_ids: ["Swinftd CRG ENG"], countries: ["Hong Kong"], amount: 1550
  "Swin FR 1400 10%" → broker_ids: ["Swinftd CRG FR"], countries: ["France"], amount: 1400
  "Swin DE 1200" (без %) → broker_ids: ["Swinftd FLAT ENG"], countries: ["Germany"], amount: 1200
  "Swin FR 1200" (без %) → broker_ids: ["Swinftd FLAT FR"], countries: ["France"], amount: 1200

ВАЖНО — общее правило "процент = CRG":
Если в строке с прайсом есть символ % — это CRG-прайс. Для брокеров у которых есть отдельные CRG/CPA интеграции, используй CRG-вариант.
- Названия брокеров и аффилиатов могут быть написаны кириллицей — транслитерируй в латиницу. Примеры: "мн"→"MN", "нексус"→"Nexus", "марси"→"Marsi", "фара"→"Farah", "капитан"→"Capitan", "ройбис"→"RoiBees", "финтрикс"→"Fintrix". Общее правило транслитерации: м→M, н→N, к→K, с→S, р→R и т.д. Сохраняй регистр как в оригинальном названии если известно, иначе используй Title Case. Примеры: "белигия"→"Belgium", "аргентина"→"Argentina", "KE"→"Kenya", "NG"→"Nigeria", "DE"→"Germany", "UK"→"United Kingdom", "IT"→"Italy", "FR"→"France", "ES"→"Spain", "PL"→"Poland", "RO"→"Romania", "HU"→"Hungary", "CZ"→"Czech Republic", "PT"→"Portugal", "GR"→"Greece", "SE"→"Sweden", "NO"→"Norway", "FI"→"Finland", "DK"→"Denmark", "NL"→"Netherlands", "BE"→"Belgium", "AT"→"Austria", "CH"→"Switzerland", "TR"→"Turkey", "IL"→"Israel", "AE"→"United Arab Emirates", "SA"→"Saudi Arabia", "ZA"→"South Africa", "EG"→"Egypt", "MA"→"Morocco", "GH"→"Ghana", "TZ"→"Tanzania", "UG"→"Uganda", "ET"→"Ethiopia", "IN"→"India", "PK"→"Pakistan", "BD"→"Bangladesh", "ID"→"Indonesia", "TH"→"Thailand", "VN"→"Vietnam", "PH"→"Philippines", "MY"→"Malaysia", "SG"→"Singapore", "JP"→"Japan", "KR"→"Korea, Republic of", "CN"→"China", "AU"→"Australia", "NZ"→"New Zealand", "CA"→"Canada", "MX"→"Mexico", "CO"→"Colombia", "PE"→"Peru", "CL"→"Chile", "VE"→"Venezuela", "EC"→"Ecuador", "BO"→"Bolivia", "PY"→"Paraguay", "UY"→"Uruguay", "CR"→"Costa Rica", "DO"→"Dominican Republic", "GT"→"Guatemala", "HN"→"Honduras", "SV"→"El Salvador", "NI"→"Nicaragua", "PA"→"Panama", "CU"→"Cuba", "US"→"United States", "BR"→"Brazil", "AR"→"Argentina", "UA"→"Ukraine", "RU"→"Russia", "BY"→"Belarus", "KZ"→"Kazakhstan", "UZ"→"Uzbekistan", "AZ"→"Azerbaijan", "GE"→"Georgia", "AM"→"Armenia", "MD"→"Moldova", "LT"→"Lithuania", "LV"→"Latvia", "EE"→"Estonia", "BG"→"Bulgaria", "HR"→"Croatia", "RS"→"Serbia", "SK"→"Slovakia", "SI"→"Slovenia", "BA"→"Bosnia and Herzegovina", "AL"→"Albania", "MK"→"North Macedonia", "ME"→"Montenegro"
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
  Если страна не указана — countries: ["all"] (показать all countries)
- Для change_caps: broker_ids = брокер, country_caps = список {country, cap, delta?}
  Ключевые слова: "cap", "капа", "кап", "лимит", "total cap"
  Если одна страна и одна капа — тоже country_caps (список из одного элемента).
  Если пользователь говорит "добавь N к капе" / "add N cap" / "increase cap by N" / "прибавь N" —
  используй поле "delta" вместо "cap" (delta — число, может быть отрицательным для уменьшения).
  Примеры:
  • "set Legion DE cap 20" → {"action": "change_caps", "broker_ids": ["Legion"], "country_caps": [{"country": "Germany", "cap": 20}]}
  • "legion de total cap 20" → {"action": "change_caps", "broker_ids": ["Legion"], "country_caps": [{"country": "Germany", "cap": 20}]}
  • "add 5 cap to Legion DE" → {"action": "change_caps", "broker_ids": ["Legion"], "country_caps": [{"country": "Germany", "delta": 5}]}
  • "добавь 5 к капе легион де" → {"action": "change_caps", "broker_ids": ["Legion"], "country_caps": [{"country": "Germany", "delta": 5}]}
  • "уменьши капу легион де на 3" → {"action": "change_caps", "broker_ids": ["Legion"], "country_caps": [{"country": "Germany", "delta": -3}]}
  • "Nexus DE 15 cap ES 20 cap" → {"action": "change_caps", "broker_ids": ["Nexus"], "country_caps": [{"country": "Germany", "cap": 15}, {"country": "Spain", "cap": 20}]}
  • "капитан FR ES cap 10" → {"action": "change_caps", "broker_ids": ["Capitan"], "country_caps": [{"country": "France", "cap": 10}, {"country": "Spain", "cap": 10}]}
  • "Fintrix CRG DE 15 cap" → {"action": "change_caps", "broker_ids": ["Fintrix CRG"], "country_caps": [{"country": "Germany", "cap": 15}]}
  • "Nexus CPA FR cap 20" → {"action": "change_caps", "broker_ids": ["Nexus CPA"], "country_caps": [{"country": "France", "cap": 20}]}
  • "legion de 20 cap aff 127" → {"action": "change_caps", "broker_ids": ["Legion"], "country_caps": [{"country": "Germany", "cap": 20, "affiliate_id": "127"}]}
  • "поставь капу 15 легион де для аффа 127" → {"action": "change_caps", "broker_ids": ["Legion"], "country_caps": [{"country": "Germany", "cap": 15, "affiliate_id": "127"}]}
  • "legion de 20 cap aff 107 144" → {"action": "change_caps", "broker_ids": ["Legion"], "country_caps": [{"country": "Germany", "cap": 20, "affiliate_id": ["107", "144"]}]}
  • "Nexus FR cap 15 aff 107 144 145" → {"action": "change_caps", "broker_ids": ["Nexus"], "country_caps": [{"country": "France", "cap": 15, "affiliate_id": ["107", "144", "145"]}]}
  Если в команде есть "aff N", "affiliate N", "аф N", "аффу N", "для аффа N" — добавляй поле "affiliate_id" в country_caps.
  affiliate_id может быть строкой (один аффилиат) или списком строк (несколько аффилиатов).
  Если после "aff" идёт НЕСКОЛЬКО чисел — это несколько аффилиатов, запиши их как список: "affiliate_id": ["107", "144"].
  ВАЖНО: cap или delta должен быть числом (int). Country обязательна.
  Отличай от прайсов! "cap", "кап", "лимит" → change_caps. "прайс", "price", "$" → add_revenue.
- Для get_caps: узнать текущие капы брокера для страны/стран.
  Ключевые слова: "какая капа", "what cap", "check cap", "cap?", "сколько капа", "узнай капу", "get cap", "how much", "сколько набрали", "сколько лидов", "how many", "filled", "filled?"
  Примеры:
  • "какая капа у легиона де" → {"action": "get_caps", "broker_ids": ["Legion"], "countries": ["Germany"]}
  • "legion de cap?" → {"action": "get_caps", "broker_ids": ["Legion"], "countries": ["Germany"]}
  • "check cap nexus fr es" → {"action": "get_caps", "broker_ids": ["Nexus"], "countries": ["France", "Spain"]}
  • "что за капа у capitan" → {"action": "get_caps", "broker_ids": ["Capitan"], "countries": ["all"]}
  • "Fintrix CRG DE cap?" → {"action": "get_caps", "broker_ids": ["Fintrix CRG"], "countries": ["Germany"]}
  • "cap legion australia aff 28" → {"action": "get_caps", "broker_ids": ["Legion"], "countries": ["Australia"], "affiliate_id": "28"}
  • "какая капа легион австралия аф 28" → {"action": "get_caps", "broker_ids": ["Legion"], "countries": ["Australia"], "affiliate_id": "28"}
  Если страна не указана — countries: ["all"]. Если аф не указан — affiliate_id: null (показать все капы).

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

Если пользователь добавляет прайсы для НЕСКОЛЬКИХ объектов (брокер И аффилиат) сразу — используй action "set_prices" с полем "price_tasks":
{
  "action": "set_prices",
  "broker_ids": [],
  "price_tasks": [
    {"type": "broker",    "id": "MM affiliates", "country": "Uruguay", "amount": 800},
    {"type": "affiliate", "id": "60",            "country": "Uruguay", "amount": 700}
  ]
}
Примеры:
  "UY MM affiliates 800
   60 aff UY 700"
→ {"action": "set_prices", "broker_ids": [], "price_tasks": [
    {"type": "broker", "id": "MM affiliates", "country": "Uruguay", "amount": 800},
    {"type": "affiliate", "id": "60", "country": "Uruguay", "amount": 700}
  ]}
  "Capex CL 950 CPA
   28 aff CL 600"
→ {"action": "set_prices", "broker_ids": [], "price_tasks": [
    {"type": "broker", "id": "Capex", "country": "Chile", "amount": 950},
    {"type": "affiliate", "id": "28", "country": "Chile", "amount": 600}
  ]}
Используй set_prices когда в сообщении есть И прайс для брокера, И прайс для аффилиата.
Если только прайс для брокера — используй add_revenue. Если только для аффилиата — используй add_affiliate_revenue.

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

- Для add_revenue и add_affiliate_revenue: если в команде есть "aff N", "affiliate N", "аф N", "аффу N", "для аффа N" — добавляй поле "affiliate_id": "N" в каждый элемент country_revenues.
  Примеры:
  • "добавь прайс Nexus MY 1250 aff 144" → {"action": "add_revenue", "broker_ids": ["Nexus"], "country_revenues": [{"country": "Malaysia", "amount": 1250, "affiliate_id": "144"}]}
  • "broker 32 / MY 1250 / ID 1000 / aff 127" → country_revenues с affiliate_id "127" в каждом объекте
  Если affiliate_id не указан — поле не добавляй.

Формат "прайс-листа" для add_revenue с несколькими странами:
Пользователь может прислать:
  FR 1300$ cpa
  ES 1500$ cpa
или:
  FR 1300
  ES 1500

Правила разбора:
- ISO код страны → переводи в полное название
- ВАЖНО: В формате прайс-листа (строки вида `КОД ЧИСЛО`) любой 2-буквенный токен — это ВСЕГДА ISO код страны, даже если он совпадает со словами "ID", "IN", "IS", "NO", "IT", "TO" и т.д. Никогда не интерпретируй его как идентификатор или служебное слово. Примеры: "ID 1250" → {country: "Indonesia", amount: 1250}, "IN 900" → {country: "India", amount: 900}
- Число (целое, без %) → сумма прайса (amount). Берём ПЕРВОЕ большое число (обычно 3-4 цифры) как amount.
- $ — игнорируй
- cpa / crg / тип сделки — игнорируй
- Проценты (10%, 15%, 5% и т.д.) — ПОЛНОСТЬЮ ИГНОРИРУЙ, это комиссии, не относятся к прайсу
- "deduct", "full deduct", "- N% deduct" — ПОЛНОСТЬЮ ИГНОРИРУЙ, это условия выплат
- "test", "test tomorrow", "test today" — ПОЛНОСТЬЮ ИГНОРИРУЙ, это пометки менеджера
- "tomorrow", "today" в контексте прайсов — ИГНОРИРУЙ (это когда прайс начнёт действовать, не наше дело)
- Используй поле "country_revenues" — список объектов {country, amount}
- "amount" должен быть числом (без $ и без %)

Примеры:
  "Clickbait CRG / CZ 1450 10% - 10% deduct" → broker_ids: ["Clickbait CRG"], country_revenues: [{country: "Czech Republic", amount: 1450}]
  "Fintrix CRG / 29 ES 1550 15% test tomorrow - 5% deduct" → broker_ids: ["Fintrix CRG"], affiliate_id: "29", country_revenues: [{country: "Spain", amount: 1550}]
  "Theta Holding / NL 1650$ CPA" → broker_ids: ["Theta Holding"], country_revenues: [{country: "Netherlands", amount: 1650}]
  "28 price for CRG tomorrow / CZ 1200 10% - full deduct" → affiliate_id: "28", action: "add_affiliate_revenue", country_revenues: [{country: "Czech Republic", amount: 1200}]

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
  • Числа в начале (17) — affiliate id
  • ISO коды стран (CA, DE, BR...) — переводи в полное название → country
  • Языки (EN, RU, PL...) — ИГНОРИРУЙ (это язык деска, не страна)
  • Тип сделки (CPA, CRG) — запомни тип, используй для определения нужен ли affiliate_id в капе
  • "today" / "сегодня" → days_to_keep: [название сегодняшнего дня]
  • "tomorrow" / "завтра" → days_to_keep: [название завтрашнего дня]
  • Название дня (Monday, Saturday...) → days_to_keep: [этот день]
- Вторая строка содержит брокера, капу и часы:
  • Первое слово/фраза до числа — имя брокера → broker_ids
  • "N cap" — лимит → country_caps: [{country, cap: N}]
  • HH:MM-HH:MM или HH:MM–HH:MM — часы работы → hours start/end
  • "00:00" в конце времени означает полночь — оставляй как "00:00"
  • gmt+N / UTC+N — часовой пояс, ИГНОРИРУЙ (CRM сам управляет таймзоной)
- Если в сообщении есть И часы, И капа — используй action: "lead_task"
- Если только часы (без "N cap") — используй action: "add_hours"
- Если только капа (без HH:MM-HH:MM) — используй action: "change_caps"
- skip_missing: НЕ используй для lead-формы, вместо этого используй "requested_day"
- "requested_day" — конкретный день который запросил пользователь (например "Thursday", "Saturday").
  Если день не указан — оставляй "requested_day": null.
  Бот сам решит что делать:
  • день указан + будний → создаст/обновит с Пн–Пт
  • день указан + weekend → только этот день
  • день не указан → рабочие дни (Пн–Пт) по умолчанию

ВАЖНО — правила для капы без affiliate_id:
Следующие CRG-брокеры получают капу БЕЗ affiliate_id (поле affiliate_id НЕ добавлять в country_caps):
Capitan, Legion, Fintrix CRG, Swin FR CRG, Swin FR CRG duplicate, Swin EN CRG, Swinftd CRG FR, Swinftd CRG FR DUPLICATE, Swinftd CRG ENG, Avelux CRG, Clickbait CRG, Imperius, EMP, CMT, GLB, Capex, Helios CRG.
Если тип сделки CRG и брокер из этого списка — НЕ добавляй affiliate_id в капу.
Если тип сделки CPA или брокер НЕ из этого списка — добавляй affiliate_id из первой строки в country_caps.

Пример 1 (CRG брокер — капа БЕЗ affiliate_id):
  "225 FR CRG tomorrow / Capitan 15 cap 10:00-19:00 gmt+2"  (сегодня понедельник)
→ {
    "action": "lead_task",
    "broker_ids": ["Capitan"],
    "country_hours": [{"country": "France", "start": "10:00", "end": "19:00"}],
    "country_caps": [{"country": "France", "cap": 15}],
    "days_to_keep": ["Tuesday"],
    "requested_day": "Tuesday",
    "no_traffic": true,
    "skip_missing": false
  }

Пример 2 (CPA брокер — капа С affiliate_id):
  "225 FR CPA tomorrow / Nexus CPA 15 cap 10:00-19:00 gmt+2"  (сегодня понедельник)
→ {
    "action": "lead_task",
    "broker_ids": ["Nexus CPA"],
    "country_hours": [{"country": "France", "start": "10:00", "end": "19:00"}],
    "country_caps": [{"country": "France", "cap": 15, "affiliate_id": "225"}],
    "days_to_keep": ["Tuesday"],
    "requested_day": "Tuesday",
    "no_traffic": true,
    "skip_missing": false
  }

Пример 3 (только часы, без капы):
  "17 CA EN CRG today / Capitan 17:00-00:00 gmt+2"  (сегодня четверг)
→ {
    "action": "add_hours",
    "broker_ids": ["Capitan"],
    "country_hours": [{"country": "Canada", "start": "17:00", "end": "00:00"}],
    "days_to_keep": ["Thursday"],
    "requested_day": "Thursday",
    "no_traffic": true,
    "skip_missing": false
  }

Формат "мульти-брокер задача" (multi_broker_task):
Когда в одном сообщении указана ОДНА страна/день в заголовке и НЕСКОЛЬКО брокеров с разными действиями.
Формат:
  DE CRG tomorrow
  123/122/28
  Legion 20 cap total 09:00-18:00 gmt+2
  ... (текст про фаннелы, маппинг — ИГНОРИРУЙ)
  122/123
  Fintrix CRG PAUSED
  123/122/28
  Capitan 15 cap 11:00-19:00 gmt+2
  ... (текст про фаннелы, маппинг — ИГНОРИРУЙ)

Правила разбора:
- Первая строка = заголовок: страна (ISO код) + тип (CRG/CPA) + день (today/tomorrow/название дня)
- Строки с числами через / (123/122/28) — ID аффилиатов, ИГНОРИРУЙ
- Строки с именем брокера + "N cap" + "HH:MM-HH:MM" — поставить капу и часы
- "cap total" или просто "cap" без "aff" — капа БЕЗ affiliate_id
- Строка "PAUSED" или "паузд" после имени брокера — ЗАКРЫТЬ часы этого брокера на указанный день
- Строки с "funnel", "map", "keep", "sharing", "dif" — ИГНОРИРУЙ, это инструкции для других людей
- gmt+N — конвертируй время в GMT+2

Результат — action "multi_broker_task" с полем "tasks" (список подзадач):
{
  "action": "multi_broker_task",
  "broker_ids": [],
  "tasks": [
    {
      "type": "lead_task",
      "broker_id": "Legion",
      "country": "Germany",
      "cap": 20,
      "start": "09:00",
      "end": "18:00",
      "day": "Wednesday",
      "no_traffic": true
    },
    {
      "type": "close_day",
      "broker_id": "Fintrix CRG",
      "country": "Germany",
      "day": "Wednesday"
    },
    {
      "type": "lead_task",
      "broker_id": "Capitan",
      "country": "Germany",
      "cap": 15,
      "start": "11:00",
      "end": "19:00",
      "day": "Wednesday",
      "no_traffic": true
    }
  ]
}
Каждая подзадача имеет type:
  - "lead_task" — поставить часы + капу (cap может быть null если только часы)
  - "close_day" — закрыть часы на этот день (PAUSED)

Формат "desk-расписания":
Пользователь может прислать расписание в таком формате:
  JP desk 07:30-12:30
  GEO: JP
  EN desk 11:00-17:00
  GEO: BE FI NL NZ AU
  SUNDAY OFF (или OFF, или выходной)

Или полное расписание брокера на выходные:
  Nexus Schedule GMT+3
  (28.03-29.03)
  SATURDAY 😶
  FR desk 14:00-20:00
  GEO: FR
  EN desk 11:00-18:00
  GEO: BE FI NL NZ AU DK NO SE IE
  PL desk 11:00-16:00
  GEO: PL
  RU desk 11:00-16:00
  GEO: MD AZ KZ LT LV EE
  BG desk 11:00-16:00
  GEO: BG
  CZ desk 11:00-16:00
  GEO: CZ SK
  HR desk 11:00-16:00
  GEO: HR RS
  DE desk 11:00-18:00
  GEO: DE AT CH
  SUNDAY 😶
  OFF

Правила разбора:
- Первая строка содержит имя брокера (например "Nexus Schedule GMT+3" → broker_ids: ["Nexus"]). Слова "Schedule", "GMT+N", даты в скобках — ИГНОРИРУЙ.
- Строка "X desk HH:MM-HH:MM" или "X desk HH:MM–HH:MM" — название деска и часы работы
- Строка "GEO: XX YY ZZ" — страны (ISO коды) к которым применяются часы предыдущего деска. Переводи каждый код в полное английское название
- Строка "SATURDAY" или "SATURDAY 😶" — следующие блоки desk+GEO относятся к субботе
- Строка "SUNDAY" или "SUNDAY 😶" — следующие блоки относятся к воскресенью
- Строка "OFF" после дня — этот день полностью закрыт, добавь его в days_to_close
- Слово "desk" игнорируй — оно не несёт смысла для CRM
- Эмодзи (😶 и др.) — ИГНОРИРУЙ

Если расписание содержит НЕСКОЛЬКО ДНЕЙ (например субботу с часами + воскресенье OFF) — используй action "bulk_schedule":
{
  "action": "bulk_schedule",
  "broker_ids": ["Nexus"],
  "country_hours": [
    {"country": "France", "start": "14:00", "end": "20:00"},
    {"country": "Belgium", "start": "11:00", "end": "18:00"},
    {"country": "Finland", "start": "11:00", "end": "18:00"},
    ...все страны из GEO-строк...
  ],
  "days_to_keep": ["Saturday"],
  "days_to_close": ["Sunday"],
  "skip_missing": true,
  "no_traffic": true
}

ВАЖНО для bulk_schedule:
- country_hours содержит ВСЕ страны из GEO-строк с часами соответствующего деска
- days_to_keep — дни, для которых нужно поставить часы (Saturday)
- days_to_close — дни, которые нужно закрыть (Sunday если OFF)
- skip_missing: true — если страны нет у брокера, пропускать без ошибки
- Генерируй country_hours: один элемент на КАЖДУЮ страну из КАЖДОЙ GEO-строки с часами соответствующего деска

Если расписание только на ОДИН день (без close) — используй обычный "add_hours" с skip_missing: true.

- Для desk-формата всегда ставь "skip_missing": true — не нужно добавлять страны которых not found for broker, только обновлять существующие

ВАЖНО — конвертация часовых поясов:
CRM работает в GMT+2. Если в сообщении указан другой часовой пояс (GMT+3, GMT+1, UTC+3...) — ОБЯЗАТЕЛЬНО конвертируй все часы в GMT+2 перед записью в JSON.
Формула: время_GMT2 = время_оригинал - (оригинал_offset - 2)
Примеры:
  • GMT+3 → GMT+2: вычитаем 1 час. "14:00-20:00 GMT+3" → "13:00-19:00"
  • GMT+1 → GMT+2: прибавляем 1 час. "10:00-18:00 GMT+1" → "11:00-19:00"
  • GMT+0 → GMT+2: прибавляем 2 часа. "08:00-16:00 GMT+0" → "10:00-18:00"
  • Если часовой пояс не указан — считай что время уже в GMT+2, ничего не конвертируй.
Всегда записывай в JSON уже сконвертированное время.

Пример команды: "поставь hours for этих стран на субботу: JP desk 07:30-12:30 / GEO: JP / EN desk 11:00-17:00 / GEO: BE FI NL"
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

Контекст ответа:
Иногда команда приходит как ответ на другое сообщение. Формат:
[Ответ на сообщение:]
<текст оригинального сообщения>

[Новая команда:]
<текст новой команды>

В этом случае:
- Используй оригинальное сообщение для КОНТЕКСТА (имя брокера, список стран, часы)
- Используй новую команду как ДЕЙСТВИЕ (что нужно сделать)
Примеры:
  [Ответ на:] "Nexus Schedule GMT+3 ... FR desk 14:00-20:00 ... GEO: FR ..."
  [Команда:] "FR with Nexus is off this weekend pls"
  → {"action": "close_days", "broker_ids": ["Nexus"], "countries_days": [{"country": "France", "days_to_close": ["Saturday", "Sunday"]}]}
  
  [Ответ на:] "Capitan 15 cap 10:00-19:00"
  [Команда:] "change to 20 cap"
  → {"action": "change_caps", "broker_ids": ["Capitan"], "country_caps": [{"country": "France", "cap": 20}]}
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
        log.info("Starting browser...")
        _playwright = await async_playwright().start()
        _browser = await _playwright.chromium.launch(headless=True)
        _context = await _browser.new_context(viewport={"width": 1440, "height": 900})
        _page = await _context.new_page()
        await do_login()
    else:
        # Проверяем что сессия не истекла
        if "login" in _page.url.lower():
            log.info("Session expired, re-logging...")
            await do_login()

    return _page


async def do_login():
    """Войти в CRM."""
    await _page.goto(CRM_URL, wait_until="commit", timeout=30000)
    await _page.wait_for_timeout(1000)

    # Если уже на дашборде — всё хорошо
    if "login" not in _page.url.lower() and "dashboard" in _page.url.lower():
        log.info("Already logged in.")
        return

    log.info("Logging in...")
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
    log.info(f"Post-login URL: {_page.url}")

    if "login" in _page.url.lower():
        raise Exception("Login failed — check credentials in config.py")

    log.info("Login successful.")
    alog.set_status("last_login", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


async def find_and_open_broker(page: Page, broker_id: str, country_hint: str = None) -> Optional[str]:
    """
    Найти брокера и вернуть его base path (/clients/ID).
    Возвращает None если брокер not found.
    country_hint — название страны, для LATAM-маршрутизации.
    """
    global _last_broker_full_name
    _last_broker_full_name = broker_id  # fallback
    broker_id = str(broker_id).strip()

    # Определяем, нужен ли LATAM-вариант
    is_latam = False
    if country_hint and country_hint.lower() in LATAM_COUNTRIES:
        is_latam = True

    # Если ID числовой — идём напрямую, без поиска
    if broker_id.isdigit():
        base = f"/clients/{broker_id}"
        test_url = f"{CRM_URL.rstrip('/')}{base}/settings"
        await page.goto(test_url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(800)
        current = page.url
        log.info(f"After navigation URL: {current}")
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

    # Очищаем поле и вводим имя — принудительно триггерим Vue фильтрацию
    await search.click(click_count=3)
    await page.keyboard.press("Backspace")
    await page.wait_for_timeout(300)
    await search.fill("")
    await page.wait_for_timeout(200)
    await search.type(broker_id, delay=60)
    await page.wait_for_timeout(400)
    await search.evaluate(
        "el => { el.dispatchEvent(new Event('input', {bubbles:true})); "
        "el.dispatchEvent(new Event('change', {bubbles:true})); }"
    )
    await page.wait_for_timeout(600)

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
    rows = await page.evaluate(r"""(query) => {
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

    log.info(f"Brokers found for query '{broker_id}': {[r['name'] for r in rows]}")

    if not rows:
        return None

    # Проверяем что хоть один результат реально содержит запрос
    # Если нет — поиск вернул мусор (не успел отфильтровать)
    query_lower = broker_id.lower().strip()
    relevant = [r for r in rows if query_lower in r["name"].lower()]
    if not relevant:
        log.info(f"No results contain '{broker_id}' — broker not found")
        return None
    rows = relevant  # работаем только с релевантными

    # 1. Точное совпадение имени (без учёта регистра, без эмодзи)
    for row in rows:
        # Убираем эмодзи и лишние пробелы
        clean_name = re.sub(r'[^\w\s_\-().]', '', row["name"]).strip().lower()
        clean_name = re.sub(r'\s+', ' ', clean_name)
        if clean_name == query_lower:
            log.info(f"Exact match: {row['name']}")
            _last_broker_full_name = row["name"]
            href = row["href"].replace("/settings", "")
            return href

    # 2. Имя начинается с запроса (например "MN" → "MN 216", но не "MN FR 216")
    for row in rows:
        name_lower = row["name"].lower().strip()
        # Убираем числовой префикс типа "272 - MN" и эмодзи
        clean = re.sub(r"^\d+\s*-\s*", "", name_lower).strip()
        clean = re.sub(r'[^\w\s_\-().]', '', clean).strip()
        clean = re.sub(r'\s+', ' ', clean)
        if clean == query_lower or name_lower == query_lower:
            log.info(f"Match after prefix cleanup: {row['name']}")
            _last_broker_full_name = row["name"]
            return row["href"].replace("/settings", "")

    # 3. Частичное совпадение — с учётом LATAM-маршрутизации
    partial = [r for r in rows if query_lower in r["name"].lower()]
    if partial:
        # LATAM: если страна латам — предпочитаем вариант с "latam" в имени
        if is_latam:
            latam_matches = [r for r in partial if "latam" in r["name"].lower()]
            if latam_matches:
                best = min(latam_matches, key=lambda r: len(r["name"]))
                log.info(f"LATAM preferred: {best['name']}")
                _last_broker_full_name = best["name"]
                return best["href"].replace("/settings", "")

        # Если в запросе явно указан CRG — берём CRG
        if "crg" in query_lower:
            crg = [r for r in partial if "crg" in r["name"].lower()]
            if crg:
                best = min(crg, key=lambda r: len(r["name"]))
                log.info(f"Selected CRG by query: {best['name']}")
                _last_broker_full_name = best["name"]
                return best["href"].replace("/settings", "")
        # Если в запросе явно указан CPA — берём CPA
        if "cpa" in query_lower:
            cpa = [r for r in partial if "cpa" in r["name"].lower()]
            if cpa:
                best = min(cpa, key=lambda r: len(r["name"]))
                log.info(f"Selected CPA by query: {best['name']}")
                _last_broker_full_name = best["name"]
                return best["href"].replace("/settings", "")
        # Иначе предпочитаем CPA
        cpa = [r for r in partial if "cpa" in r["name"].lower()]
        if cpa:
            best = min(cpa, key=lambda r: len(r["name"]))
            log.info(f"Preferred CPA: {best['name']}")
            _last_broker_full_name = best["name"]
            return best["href"].replace("/settings", "")
        # Нет ни CPA ни CRG — берём кратчайшее
        best = min(partial, key=lambda r: len(r["name"]))
        log.info(f"Partial match (shortest): {best['name']}")
        _last_broker_full_name = best["name"]
        return best["href"].replace("/settings", "")

    # 4. Первый результат как запасной
    log.info(f"Taking first result: {rows[0]['name']}")
    _last_broker_full_name = rows[0]["name"]
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
        return f"❌ Broker '{broker_id}' not found. Nothing changed."

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
    log.info(f"Pencil buttons found: {len(pencils_with_names)}")

    if not pencils_with_names:
        log.info(f"Page URL: {page.url}")
        return "❌ Edit buttons not found. Nothing changed."

    # Собираем имена стран для обработки (фильтруем заранее)
    countries_to_process = []
    for _, country_name in pencils_with_names:
        if "all" not in countries_filter and country_name:
            if not any(c.lower() in country_name.lower() for c in countries_filter):
                log.info(f"Skipping {country_name} — not in filter {countries_filter}")
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
            results.append(f"⚠️ {country_name}: карандаш not found после обновления DOM")
            continue

        await target_pencil.click()
        await page.wait_for_timeout(600)

        # Ждём модальное окно
        try:
            modal = await page.wait_for_selector(".modal-body, [role='dialog']", timeout=4000)
        except Exception:
            results.append(f"⚠️ {country_name}: modal did not open")
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
            log.info(f"Disabling days: {days_to_disable}")

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
            results.append(f"✅ {country_name}: {start}–{end} saved")
        except Exception:
            await _close_modal(page)
            results.append(f"⚠️ {country_name}: Save button not found")

    return "\n".join(results) if results else "⚠️ No rows to change."


async def action_edit_country_add_days(broker_id: str, country: str, start: str, end: str,
                                        no_traffic: bool, days_to_add: list) -> str:
    """
    Редактировать существующую запись страны: добавить галочки на нужные дни
    и выставить часы — не трогая already activeные дни.
    """
    page = await get_page()

    base_path = await find_and_open_broker(page, broker_id)
    if not base_path:
        return f"❌ Broker '{broker_id}' not found. Nothing changed."

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
        return f"❌ Country '{country}' not found for this broker. Nothing changed."

    await target_pencil.click()
    await page.wait_for_timeout(600)

    try:
        modal = await page.wait_for_selector(".modal-body, [role='dialog']", timeout=4000)
    except Exception:
        return f"❌ {country}: modal did not open."

    await page.wait_for_timeout(400)

    days_lower = [d.lower() for d in days_to_add]
    log.info(f"Adding days {days_lower} to {country}")

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

        # Включаем день если не enabled
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
        log.info(f"  Day {matched_day}: timepickers in row = {row_time_inputs}")

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
        return f"✅ {country}: days added: {', '.join(enabled)} with hours {start}–{end}"
    except Exception:
        await _close_modal(page)
        return f"⚠️ {country}: Save button not found."


async def action_add_country_hours_multi(broker_id: str, country: str,
                                         schedule_groups: list, no_traffic: bool,
                                         country_exists: bool = False) -> str:
    """
    Добавить/обновить hours for страны с несколькими группами дней за один проход модалки.
    schedule_groups: [{"days": [...], "start": "10:00", "end": "19:00"}, ...]
    """
    page = await get_page()

    base_path = await find_and_open_broker(page, broker_id)
    if not base_path:
        return f"❌ Broker '{broker_id}' not found."

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
            return f"❌ Country '{country}' not found."
        await target.click()
    else:
        # Добавляем новую запись
        try:
            add_btn = await page.wait_for_selector(
                "button:has-text('ADD OPENING HOURS'), a:has-text('ADD OPENING HOURS')", timeout=12000
            )
            await add_btn.click()
        except Exception:
            return "❌ ADD OPENING HOURS button not found."

    await page.wait_for_timeout(800)
    try:
        modal = await page.wait_for_selector(".modal-body, [role='dialog']", timeout=5000)
    except Exception:
        return "❌ Modal did not open."
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
                return f"❌ Country '{country}' not found in list."
        except Exception as e:
            return f"❌ Error selecting country: {e}"

        # Переполучаем modal после выбора страны
        await page.wait_for_timeout(600)
        modal = await page.query_selector(".modal-body, [role='dialog']")
        if not modal:
            return "❌ Modal closed after country selection."

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

    log.info(f"schedule_groups for {country}: {day_to_time}")

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
        action_word = "Updated" if country_exists else "Added"
        return f"✅ {action_word} hours for {country}: {groups_str}"
    except Exception:
        await _close_modal(page)
        return f"⚠️ {country}: Save button not found."


async def action_add_country_hours(broker_id: str, country: str, start: str, end: str,
                                    no_traffic: bool, days_filter: list = None) -> str:
    """Добавить часы работы для новой страны."""
    page = await get_page()

    base_path = await find_and_open_broker(page, broker_id)
    if not base_path:
        return f"❌ Broker '{broker_id}' not found. Nothing changed."

    oh_url = f"{CRM_URL.rstrip('/')}{base_path}/opening_hours"
    log.info(f"Opening: {oh_url}")
    await page.goto(oh_url, wait_until="domcontentloaded", timeout=60000)

    # Ждём пока Vue отрендерит кнопку ADD OPENING HOURS
    try:
        add_btn = await page.wait_for_selector(
            "button:has-text('ADD OPENING HOURS'), a:has-text('ADD OPENING HOURS'), .btn:has-text('ADD')",
            timeout=12000
        )
        log.info("ADD OPENING HOURS found, clicking...")
        await add_btn.click()
        await page.wait_for_timeout(800)
    except Exception as e:
        log.info(f"ADD OPENING HOURS not found: {e}")
        return "❌ ADD OPENING HOURS button not found. Nothing changed."

    log.info("Waiting for modal...")

    # Ждём модальное окно
    try:
        modal = await page.wait_for_selector(".modal-body, [role='dialog']", timeout=5000)
    except Exception:
        return "❌ Modal did not open. Nothing changed."

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
            log.info("Clicked smart__dropdown")

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
        log.info(f"Search input found: {search_input is not None}")

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
            log.info(f"Entered: '{country}', elements in list: {items_count}")
        else:
            log.info("Search field not found!")

        # Ждём dropdown-item
        try:
            await page.wait_for_selector("li.dropdown-item", timeout=5000)
        except Exception:
            log.info("dropdown-item did not appear")

        found = False
        items = await page.query_selector_all("li.dropdown-item")
        log.info(f"Elements after search: {len(items)}")
        for item in items:
            txt = (await item.inner_text()).strip()
            log.info(f"  '{txt}'")
            if country.lower() in txt.lower():
                await item.click()
                found = True
                log.info(f"Selected: {country}")
                await page.wait_for_timeout(400)
                break

        if not found:
            return f"❌ Country '{country}' not found in list. Nothing changed."

    except Exception as e:
        return f"❌ Error selecting country: {e}"

    # Vue перерендеривает модалку после выбора страны — переполучаем её
    await page.wait_for_timeout(600)
    modal = await page.query_selector(".modal-body, [role='dialog']")
    if not modal:
        return "❌ Modal closed after country selection."
    log.info("Modal re-fetched after country selection")

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
    log.info(f"Timepicker inputs found: {len(time_inputs)}")

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
        return f"✅ Hours added for {country}: {start}–{end}"
    except Exception:
        return "⚠️ Save button not found. Data may not have been saved."


async def action_get_broker_revenue(broker_id: str, countries: list) -> str:
    """Узнать прайс брокера для указанных стран."""
    page = await get_page()

    base_path = await find_and_open_broker(page, broker_id)
    if not base_path:
        return f"❌ Broker '{broker_id}' not found."

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
    log.info(f"Broker prices {broker_id}: {table_data}")

    if not table_data:
        return f"❌ Broker {broker_id} has no prices."

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
            results.append(f"❌ {country}: прайс not found")

    return "\n".join(results)


async def action_get_hours(broker_id: str, countries: list) -> str:
    """Узнать текущие часы работы брокера для указанных стран."""
    page = await get_page()

    base_path = await find_and_open_broker(page, broker_id)
    if not base_path:
        return f"❌ Broker '{broker_id}' not found."

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
    log.info(f"Broker hours {broker_id}: {len(hours_data)} countries found")

    if not hours_data:
        return f"❌ Broker {broker_id} has no schedule."

    # Фильтруем по запрошенным странам
    results = []
    filter_all = "all" in [c.lower() for c in countries]

    for entry in hours_data:
        country_name = entry["country"]
        schedule = entry["schedule"]

        if not filter_all:
            if not any(c.lower() in country_name.lower() for c in countries):
                continue

        # Группируем одинаковые hours for компактного вывода
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
        return f"❌ Countries ({missing}) not found for this broker."

    return "\n".join(results)


async def action_get_affiliate_revenue(affiliate_id: str, countries: list) -> str:
    """Узнать прайс аффилиата для указанных стран."""
    page = await get_page()

    affiliate_id = str(affiliate_id).strip()
    if affiliate_id.isdigit():
        aff_base = f"/sources/{affiliate_id}"
    else:
        return f"❌ Please provide numeric affiliate ID."

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
    log.info(f"Raw affiliate table rows: {raw_rows}")

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
    log.info(f"Affiliate prices {affiliate_id}: {table_data}")

    if not table_data:
        return f"❌ Affiliate {affiliate_id} has no prices."

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
            results.append(f"❌ {country}: прайс not found")

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
            return f"❌ Affiliate '{affiliate_id}' not found."
        log.info(f"Affiliate found directly: {aff_base}")
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
            return f"❌ Affiliate '{affiliate_id}' not found."
        href = await settings_link.get_attribute("href")
        aff_base = href.replace("/settings", "")
        log.info(f"Affiliate found by name: {aff_base}")

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
                    log.info(f"Entry for {country} already exists (${old_amount}) — editing")
                    break

    if existing_pencil:
        # Редактируем существующую запись
        await existing_pencil.click()
        await page.wait_for_timeout(600)
        try:
            modal = await page.wait_for_selector(".modal-body, [role='dialog']", timeout=5000)
        except Exception:
            return "❌ Modal did not open."
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
            log.info(f"Amount updated: {amount}")
        else:
            return "❌ Amount field not found."

        await page.wait_for_timeout(400)
        try:
            save_btn = await page.wait_for_selector(
                ".modal button[type='submit'], .modal-footer button[type='submit'], .modal .btn-ladda",
                timeout=5000
            )
            await save_btn.click()
            await page.wait_for_timeout(1000)
            country_label = country if country.lower() != "all" else "all countries"
            log.info(f"Price updated for {country_label}: ${old_amount} → ${amount}")
            return f"✅ {country_label}: ${old_amount} → ${amount}"
        except Exception as e:
            await _close_modal(page)
            return "⚠️ Save button not found."

    # Записи нет — добавляем новую
    try:
        add_btn = await page.wait_for_selector(
            "button:has-text('ADD PAYOUT'), a:has-text('ADD PAYOUT'), .btn:has-text('ADD PAYOUT')",
            timeout=12000
        )
        await add_btn.click()
        await page.wait_for_timeout(800)
        log.info("ADD PAYOUT clicked")
    except Exception:
        return "❌ ADD PAYOUT button not found."

    try:
        modal = await page.wait_for_selector(".modal-body, [role='dialog']", timeout=5000)
    except Exception:
        return "❌ Modal did not open."
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
            log.info(f"Searched for: '{country}'")

        try:
            await page.wait_for_selector("li.dropdown-item, .dropdown-item", timeout=5000)
        except Exception:
            pass

        items = await page.query_selector_all("li.dropdown-item, .dropdown-item")
        log.info(f"Dropdown elements: {len(items)}")
        found = False
        for item in items:
            txt = (await item.inner_text()).strip()
            if country.lower() in txt.lower():
                await item.click()
                found = True
                log.info(f"Country {country} выбрана!")
                await page.wait_for_timeout(400)
                break

        if not found:
            await _close_modal(page)
            return f"❌ Country '{country}' not found. Nothing changed."

    await page.wait_for_timeout(500)
    modal = await page.query_selector(".modal-body, [role='dialog']")
    if not modal:
        return "❌ Modal closed after country selection."

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
        log.info(f"Amount entered: {amount}")
    else:
        return "❌ Amount field not found."

    await page.wait_for_timeout(400)
    try:
        save_btn = await page.wait_for_selector(
            ".modal button[type='submit'], .modal-footer button[type='submit'], .modal .btn-ladda",
            timeout=5000
        )
        log.info("Save button found, clicking...")
        await save_btn.click()
        await page.wait_for_timeout(1000)
        country_label = country if country.lower() != "all" else "all countries"
        log.info(f"Affiliate price saved for {country_label}: ${amount}")
        return f"✅ Price added for {country_label}: ${amount}"
    except Exception as e:
        log.error(f"Кнопка Save not foundа: {e}")
        await _close_modal(page)
        return "⚠️ Save button not found."


async def action_add_revenue(broker_id: str, country: str, amount: str, affiliate_id: str = None) -> str:
    """Добавить или обновить прайс (revenue) для страны брокера."""
    page = await get_page()

    base_path = await find_and_open_broker(page, broker_id, country_hint=country)
    if not base_path:
        return f"❌ Broker '{broker_id}' not found. Nothing changed."
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
                existing_pencil = await row.query_selector("button.btn-outline-primary, a.btn-primary, button.btn-primary:not(.btn-danger)")
                if existing_pencil:
                    log.info(f"Entry for {country} already exists (${old_amount}) — editing")
                    break

    if existing_pencil:
        await existing_pencil.click()
        await page.wait_for_timeout(600)
        try:
            modal = await page.wait_for_selector(".modal-body, [role='dialog']", timeout=5000)
        except Exception:
            return "❌ Modal did not open."
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
            return "❌ Amount field not found."

        await page.wait_for_timeout(400)
        try:
            save_btn = await page.wait_for_selector(
                ".modal button[type='submit'], .modal-footer button[type='submit'], .modal .btn-ladda",
                timeout=5000
            )
            await save_btn.click()
            await page.wait_for_timeout(1000)
            country_label = country if country.lower() != "all" else "all countries"
            if old_amount:
                return f"✅ {country_label}: ${old_amount} → ${amount}"
            return f"✅ Price updated for {country_label}: ${amount}"
        except Exception:
            await _close_modal(page)
            return "⚠️ Save button not found."

    # Записи нет — добавляем новую
    # Нажимаем ADD REVENUE или ADD THE FIRST REVENUE
    try:
        add_btn = await page.wait_for_selector(
            "button:has-text('ADD REVENUE'), button:has-text('ADD THE FIRST REVENUE')",
            timeout=10000
        )
        await add_btn.click()
        await page.wait_for_timeout(800)
        log.info("ADD REVENUE clicked")
    except Exception:
        return "❌ ADD REVENUE button not found. Nothing changed."

    # Ждём модалку
    try:
        modal = await page.wait_for_selector(".modal-body, [role='dialog']", timeout=5000)
    except Exception:
        return "❌ Modal did not open."

    await page.wait_for_timeout(500)

    # ── Выбираем страну ───────────────────────
    if country.lower() != "all":
        dropdown_toggle = await modal.query_selector(
            ".smart__dropdown, [class*='smart__dropdown']"
        )
        if dropdown_toggle:
            await dropdown_toggle.click()
            await page.wait_for_timeout(600)
            log.info("Clicked dropdown")

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
            log.info(f"Searched for: '{country}', elements: {items_count}")
        else:
            log.info("Поле поиска страны not foundо!")

        try:
            await page.wait_for_selector("li.dropdown-item", timeout=5000)
        except Exception:
            pass

        items = await page.query_selector_all("li.dropdown-item")
        log.info(f"Dropdown elements: {len(items)}")
        found = False
        for item in items:
            txt = (await item.inner_text()).strip()
            log.info(f"  '{txt}'")
            if country.lower() in txt.lower():
                await item.click()
                found = True
                log.info(f"Country {country} выбрана!")
                await page.wait_for_timeout(400)
                break

        if not found:
            return f"❌ Country '{country}' not found. Nothing changed."

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
        log.info(f"Amount entered: {amount}")
    else:
        return "❌ Amount field not found."

    # ── Параметр Affiliate (если нужен) ──────
    if affiliate_id:
        modal = await page.query_selector(".modal-body, [role='dialog']")
        param_ok = await _add_affiliate_parameter(page, modal, str(affiliate_id), close_dropdown=False)
        if not param_ok:
            log.warning(f"Could not add affiliate parameter {affiliate_id} — saving without it")
            affiliate_id = None  # не показывать (aff X) в сообщении если не добавилось
        # Закрываем суб-модалку ADD PARAMETER если осталась открытой (блокирует SAVE)
        await page.evaluate("""() => {
            // Ищем кнопки закрытия ×  внутри вложенных модалок (не основной)
            const closeBtns = document.querySelectorAll('.modal .close, [role=dialog] .close, button[aria-label="Close"]');
            // Кликаем по последней (самой вложенной)
            if (closeBtns.length > 1) closeBtns[closeBtns.length - 1].click();
        }""")
        await page.wait_for_timeout(400)

    # ── Сохраняем ─────────────────────────────
    # Даём Vue время обработать ввод суммы
    await page.wait_for_timeout(500)
    try:
        # Кликаем SAVE через JS — обходит проблему pointer-events interception
        saved = await page.evaluate("""() => {
            const modals = document.querySelectorAll('.modal.show, [role=dialog]');
            // Берём первую (основную) модалку
            for (const modal of modals) {
                const footer = modal.querySelector('.modal-footer');
                if (footer) {
                    const btn = footer.querySelector('button[type=submit], .btn-ladda, .btn-success');
                    if (btn) { btn.click(); return true; }
                }
                const btns = modal.querySelectorAll('button');
                for (const btn of btns) {
                    const t = btn.innerText.trim().toUpperCase();
                    if (t === 'SAVE' || btn.type === 'submit') { btn.click(); return true; }
                }
            }
            return false;
        }""")
        await page.wait_for_timeout(1200)
        country_label = country if country.lower() != "all" else "all countries"
        aff_label = f" (aff {affiliate_id})" if affiliate_id else ""
        if saved:
            log.info(f"Price saved for {country_label}: ${amount}{aff_label}")
            return f"✅ Price added for {country_label}: ${amount}{aff_label}"
        else:
            raise Exception("SAVE button not found via JS")
    except Exception as e:
        log.error(f"Кнопка Save not foundа: {e}")
        btns = await page.evaluate("""() => {
            const modal = document.querySelector('.modal, [role=dialog]');
            if (!modal) return [];
            return Array.from(modal.querySelectorAll('button')).map(b => b.innerText.trim());
        }""")
        log.info(f"Buttons in modal: {btns}")
        await _close_modal(page)
        return "⚠️ Save button not found."


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
        return f"❌ {country_name}: modal did not open."

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
        return f"⚠️ {country_name}: days {days_to_close} already closed or not found."

    try:
        save_btn = await page.wait_for_selector("text=SAVE OPENING HOURS", timeout=3000)
        await save_btn.click()
        await page.wait_for_timeout(700)
        return f"✅ {country_name}: days closed: {', '.join(closed)}."
    except Exception:
        await _close_modal(page)
        return f"⚠️ {country_name}: Save button not found."


async def action_close_days(broker_id: str, country: str, days_to_close: list) -> str:
    """Закрыть конкретные дни для страны (или всех стран) брокера."""
    page = await get_page()

    base_path = await find_and_open_broker(page, broker_id)
    if not base_path:
        return f"❌ Broker '{broker_id}' not found. Nothing changed."

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

    # Режим "all countries"
    if country.lower() == "all":
        if not pencil_buttons:
            return "❌ Countries not found for this broker."
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
                results.append(f"⚠️ {c_name}: карандаш not found после обновления.")
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
        return f"❌ Country '{country}' not found for this broker. Nothing changed."

    return await _close_days_for_pencil(page, target_pencil, target_name, days_to_close)


async def action_toggle_broker(broker_id: str, activate: bool) -> str:
    """Включить или выключить брокера."""
    page = await get_page()

    base_path = await find_and_open_broker(page, broker_id)
    if not base_path:
        return f"❌ Broker '{broker_id}' not found. Nothing changed."

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
            state = "already active" if activate else "already inactive"
            return f"ℹ️ Broker '{broker_id}' {state}. Nothing changed."

        # Сохраняем
        save = await page.wait_for_selector("text=SAVE SETTINGS", timeout=4000)
        await save.click()
        await page.wait_for_timeout(500)

        action_word = "enabled" if activate else "disabled"
        return f"✅ Broker '{broker_id}' successfully {action_word}."

    except Exception as e:
        return f"❌ Cannot change broker status: {e}\nNothing changed."


async def action_change_caps(broker_id: str, country: str, cap_value: int = 0, delta: int = None, affiliate_id: str = None, delete_first: bool = False) -> str:
    """Изменить или создать cap для страны брокера. delta — прибавить к текущему значению. affiliate_id — добавить параметр Affiliates."""
    page = await get_page()

    base_path = await find_and_open_broker(page, broker_id)
    if not base_path:
        return f"❌ Broker '{broker_id}' not found."

    caps_url = f"{CRM_URL.rstrip('/')}{base_path}/caps"
    await page.goto(caps_url, wait_until="domcontentloaded", timeout=30000)
    await page.wait_for_timeout(3000)  # даём Vue время отрендерить данные

    # Диагностика
    all_rows_text = await page.evaluate("""() =>
        Array.from(document.querySelectorAll('table tr'))
             .filter(r => r.querySelector('td') !== null)
             .map(r => r.innerText.trim()).filter(t => t)
    """)
    log.info(f"Caps data rows ({len(all_rows_text)}): {all_rows_text[:5]}")

    # Ищем строку с нужной страной через JS
    cap_row_data = await page.evaluate("""(countryQuery) => {
        const rows = Array.from(document.querySelectorAll('table tr'))
            .filter(r => r.querySelector('td') !== null);
        for (const row of rows) {
            const tds = row.querySelectorAll('td');
            if (tds.length < 4) continue;
            // Countries колонка — ищем страну (последняя td перед кнопками)
            // Пробуем все td начиная с 4-й
            let countryText = '';
            for (let i = 3; i < tds.length; i++) {
                const t = tds[i].innerText.trim();
                if (t && !t.includes('%') && !t.match(/^[0-9/]+$/) && t.length > 2) {
                    countryText = t;
                    break;
                }
            }
            if (!countryText.toLowerCase().includes(countryQuery.toLowerCase())) continue;

            // Нашли строку — читаем Filled (формат "0/10")
            let oldCap = null;
            for (const td of tds) {
                const t = td.innerText.trim();
                if (t.includes('/')) {
                    const parts = t.split('/');
                    if (parts.length === 2 && !isNaN(parts[1].trim())) {
                        oldCap = parts[1].trim();
                        break;
                    }
                }
            }
            // Есть ли кнопка редактирования
            const hasEditBtn = !!row.querySelector(
                'a.btn-primary, button.btn-primary, a[class*="primary"], button[class*="primary"]'
            );
            return {found: true, oldCap, hasEditBtn, rowText: row.innerText.trim().substring(0, 100)};
        }
        return {found: false};
    }""", country)

    log.info(f"Cap row search result: {cap_row_data}")

    existing_pencil = None
    old_cap = None

    if cap_row_data.get("found"):
        old_cap = cap_row_data.get("oldCap")
        log.info(f"Found cap row for {country}: old_cap={old_cap}")
        # Ищем кнопку редактирования через Playwright
        rows = await page.query_selector_all("table tr")
        for row in rows:
            row_text = (await row.inner_text()).strip()
            if country.lower() in row_text.lower():
                pencil = await row.query_selector(
                    "a.btn-primary, button.btn-primary, a[class*='primary'], button[class*='primary']"
                )
                if pencil:
                    existing_pencil = pencil
                break

    # Если задан delta — вычисляем новое значение
    if delta is not None:
        if old_cap is not None:
            try:
                cap_value = int(old_cap) + delta
                log.info(f"Delta mode: {old_cap} + {delta} = {cap_value}")
            except ValueError:
                return f"❌ {country}: could not parse current cap '{old_cap}'."
        else:
            # Капы нет — сигнализируем вызывающему коду чтобы спросил пользователя
            log.info(f"No existing cap for {country} — asking user to confirm creation")
            return f"__NO_CAP__|{country}|{delta}"


    if existing_pencil:
        # Редактируем существующий кап
        await existing_pencil.click()
        await page.wait_for_timeout(800)

        try:
            modal = await page.wait_for_selector(".modal-body, [role='dialog']", timeout=5000)
        except Exception:
            return f"❌ {country}: modal did not open."

        await page.wait_for_timeout(500)

        # Находим поле Cap — input[type="number"] с min="0"
        cap_input = await modal.query_selector("input[type='number'].form-control")
        if not cap_input:
            # Ищем по label "Cap"
            cap_input = await page.evaluate("""() => {
                const labels = document.querySelectorAll('.modal label, [role=dialog] label');
                for (const label of labels) {
                    if (label.textContent.trim().toLowerCase().startsWith('cap')) {
                        const input = label.closest('.form-group, .row')?.querySelector('input[type=number]');
                        if (input) return true;
                    }
                }
                return false;
            }""")
            if cap_input:
                cap_input = await page.query_selector(".modal input[type='number'], [role='dialog'] input[type='number']")

        if not cap_input:
            # Fallback — последний number input в модалке
            all_number_inputs = await modal.query_selector_all("input[type='number']")
            if all_number_inputs:
                cap_input = all_number_inputs[-1]

        if not cap_input:
            await _close_modal(page)
            return f"❌ {country}: Cap field not found in modal."

        await cap_input.click(click_count=3)
        await cap_input.type(str(cap_value))
        await page.wait_for_timeout(300)

        # Сохраняем — кнопка SAVE CAP
        try:
            save_btn = await page.wait_for_selector(
                ".modal button:has-text('SAVE CAP'), .modal-footer button[type='submit'], "
                ".modal .btn-ladda.btn-success",
                timeout=5000
            )
            await save_btn.click()
            await page.wait_for_timeout(1000)
            if old_cap:
                return f"✅ {country}: cap {old_cap} → {cap_value}"
            return f"✅ {country}: cap set to {cap_value}"
        except Exception:
            await _close_modal(page)
            return f"⚠️ {country}: SAVE CAP button not found."

    else:
        # Кап не найден
        # Если delete_first — сразу удаляем капу без параметров (пользователь уже подтвердил)
        if delete_first and affiliate_id:
            deleted = await _delete_cap_without_params(page, country)
            if deleted:
                log.info(f"Deleted cap without params for {country} (user confirmed)")
                await page.wait_for_timeout(1000)

        # Если задан affiliate_id и не delete_first — проверяем есть ли капа без параметров
        elif affiliate_id:
            has_no_param_cap = await page.evaluate("""(countryQuery) => {
                const rows = Array.from(document.querySelectorAll('table tr'))
                    .filter(r => r.querySelector('td') !== null);
                for (const row of rows) {
                    if (!row.innerText.toLowerCase().includes(countryQuery.toLowerCase())) continue;
                    const hasBadge = !!row.querySelector('span.badge-primary, span.badge.cursor-pointer');
                    if (!hasBadge) return true;
                }
                return false;
            }""", country)

            if has_no_param_cap:
                log.info(f"Found cap without params for {country} — asking user what to do")
                return f"__HAS_NO_PARAM_CAP__|{country}|{cap_value}|{affiliate_id}"

        # Создаём новый кап через ADD CAP
        try:
            add_btn = await page.wait_for_selector(
                "button:has-text('ADD CAP'), a:has-text('ADD CAP'), "
                "button.btn_big.btn-primary",
                timeout=8000
            )
            await add_btn.click()
            await page.wait_for_timeout(800)
        except Exception:
            return f"❌ ADD CAP button not found."

        try:
            modal = await page.wait_for_selector(".modal-body, [role='dialog']", timeout=5000)
        except Exception:
            return f"❌ Modal did not open."

        await page.wait_for_timeout(500)

        # === Выбор страны в Caps ===
        # DevTools: Countries dropdown имеет id="country-8or" на div.smart__dropdown.
        # Поле поиска: input#country-8or__search-input (появляется после клика).
        # В модалке несколько smart__dropdown (Type, Interval, Timezone, Countries)
        # поэтому таргетируем именно #country-8or, а не первый попавшийся.
        try:
            # Ищем Countries dropdown по id
            dropdown_trigger = await page.query_selector("#country-8or")
            if not dropdown_trigger:
                # Fallback: последний smart__dropdown в модалке (Countries идёт последним)
                all_dropdowns = await modal.query_selector_all(".smart__dropdown")
                if all_dropdowns:
                    dropdown_trigger = all_dropdowns[-1]
                    log.info(f"Fallback: using last of {len(all_dropdowns)} smart__dropdowns")
            if not dropdown_trigger:
                raise Exception("Countries dropdown (#country-8or) not found")

            await dropdown_trigger.click()
            await page.wait_for_timeout(500)
            log.info("Clicked countries dropdown")

            # Ждём поле поиска: input#country-8or__search-input
            search_input = None
            try:
                await page.wait_for_function(
                    "document.getElementById('country-8or__search-input') !== null",
                    timeout=5000
                )
                search_input = await page.query_selector("#country-8or__search-input")
                log.info("Search input found: #country-8or__search-input")
            except Exception:
                try:
                    await page.wait_for_function(
                        "!!document.querySelector(\"input[id*='search-input']\")",
                        timeout=3000
                    )
                    search_input = await page.query_selector("input[id*='search-input']")
                    log.info("Search input found via id*=search-input fallback")
                except Exception:
                    log.info("Search input not found")

            if search_input:
                await search_input.click(click_count=3)
                await search_input.fill(country)
                await search_input.evaluate(
                    "el => { el.dispatchEvent(new Event('input',{bubbles:true})); "
                    "el.dispatchEvent(new Event('change',{bubbles:true})); }"
                )
                # Ждём пока Vue отфильтрует список (< 10 элементов)
                for _ in range(20):
                    await page.wait_for_timeout(200)
                    cnt = await page.evaluate(
                        "() => document.querySelectorAll('#country-8or-list li, li.dropdown-item, li.flex-fill').length"
                    )
                    if cnt < 10:
                        break
                log.info(f"Typed: {country}")
            else:
                log.warning("Search input not found — keyboard fallback")
                await page.keyboard.type(country, delay=60)
                await page.wait_for_timeout(600)

            # Кликаем по стране в списке (ul#country-8or-list)
            country_selected = False
            # Кликаем через JS evaluate — элемент не устаревает
            country_selected = await page.evaluate("""(countryName) => {
                const lists = [
                    ...document.querySelectorAll('#country-8or-list li'),
                    ...document.querySelectorAll('li.dropdown-item'),
                    ...document.querySelectorAll('li.flex-fill'),
                ];
                for (const item of lists) {
                    if (item.innerText.trim().toLowerCase().includes(countryName.toLowerCase())) {
                        item.click();
                        return item.innerText.trim();
                    }
                }
                return null;
            }""", country)
            if country_selected:
                log.info(f"Country selected via JS: {country_selected}")
                country_selected = True
                await page.wait_for_timeout(400)
            else:
                # Fallback — Playwright query прямо перед кликом
                items = await page.query_selector_all("li.dropdown-item, li.flex-fill")
                log.info(f"Dropdown items: {len(items)}")
                for item in items:
                    try:
                        txt = (await item.inner_text()).strip()
                        if country.lower() in txt.lower():
                            await item.click()
                            country_selected = True
                            log.info(f"Country selected: {txt}")
                            await page.wait_for_timeout(400)
                            break
                    except Exception:
                        continue

            if not country_selected:
                await _close_modal(page)
                return f"❌ Country '{country}' not found in list."

        except Exception as e:
            await _close_modal(page)
            return f"❌ Error selecting country: {e}"

        # Закрываем дропдаун — повторный клик по #country-8or
        try:
            toggle_close = await page.query_selector("#country-8or")
            if not toggle_close:
                all_dropdowns = await modal.query_selector_all(".smart__dropdown")
                if all_dropdowns:
                    toggle_close = all_dropdowns[-1]
            if toggle_close:
                await toggle_close.click()
                await page.wait_for_timeout(400)
                log.info("Closed countries dropdown")
        except Exception:
            pass

        modal = await page.query_selector(".modal-body, [role='dialog']")
        if not modal:
            return "❌ Modal closed after country selection."

        # Вводим значение капа
        cap_input = await modal.query_selector("input[type='number'].form-control")
        if not cap_input:
            all_number_inputs = await modal.query_selector_all("input[type='number']")
            if all_number_inputs:
                cap_input = all_number_inputs[-1]

        if not cap_input:
            await _close_modal(page)
            return f"❌ Cap field not found."

        await cap_input.click(click_count=3)
        await cap_input.type(str(cap_value))
        await page.wait_for_timeout(300)

        # Добавляем параметр Affiliates если нужно
        if affiliate_id:
            modal = await page.query_selector(".modal-body, [role='dialog']")
            if modal:
                param_ok = await _add_affiliate_parameter(page, modal, affiliate_id)
                log.info(f"Affiliate parameter added: {param_ok}")

        # Сохраняем
        try:
            save_btn = await page.wait_for_selector(
                ".modal button:has-text('SAVE CAP'), .modal-footer button[type='submit'], "
                ".modal .btn-ladda.btn-success",
                timeout=5000
            )
            await save_btn.click()
            await page.wait_for_timeout(1000)
            if affiliate_id:
                if isinstance(affiliate_id, list):
                    aff_info = f" (aff {', '.join(str(a) for a in affiliate_id)})"
                else:
                    aff_info = f" (aff {affiliate_id})"
            else:
                aff_info = ""
            return f"✅ {country}: cap created: {cap_value}{aff_info}"
        except Exception:
            await _close_modal(page)
            return f"⚠️ {country}: SAVE CAP button not found."



async def action_get_caps(broker_id: str, countries: list, affiliate_id: str = None) -> str:
    """Получить текущие капы брокера для указанных стран."""
    page = await get_page()

    base_path = await find_and_open_broker(page, broker_id)
    if not base_path:
        return f"❌ Broker '{broker_id}' not found."

    caps_url = f"{CRM_URL.rstrip('/')}{base_path}/caps"
    await page.goto(caps_url, wait_until="domcontentloaded", timeout=30000)
    # Ждём появления таблицы или кнопки ADD CAP
    try:
        await page.wait_for_selector("table, button:has-text('ADD CAP')", timeout=10000)
    except Exception:
        pass
    await page.wait_for_timeout(1500)


    # Читаем всю таблицу капов включая Attributes
    # Структура: Interval | Type | Filled(0/20+%) | Countries | Attributes | Actions
    # Кликаем по всем бейджам "N parameters" чтобы Vue отрендерил детали
    await page.evaluate(
        "() => {"
        " var badges = document.querySelectorAll('span.badge.cursor-pointer.badge-primary');"
        " for (var i = 0; i < badges.length; i++) { badges[i].click(); }"
        "}"
    )
    await page.wait_for_timeout(800)

    rows_data = await page.evaluate(
        "() => {"
        " var result = [];"
        " var rows = document.querySelectorAll('table tbody tr, table tr');"
        " for (var i = 0; i < rows.length; i++) {"
        "  var tds = rows[i].querySelectorAll('td');"
        "  if (tds.length < 3) continue;"
        "  var interval = tds[0].innerText.trim();"
        "  var tp = tds[1].innerText.trim();"
        "  var raw = tds[2].innerText.trim();"
        "  var filled = raw;"
        "  var nl = String.fromCharCode(10); var pts = raw.split(nl);"
        "  for (var p = 0; p < pts.length; p++) { var t = pts[p].trim(); if (t.indexOf('/') > 0) { filled = t; break; } }"
        "  var ctry = tds[3] ? tds[3].innerText.trim() : '';"
        "  var attrTd = tds[4] || null; var attr = ''; if (attrTd) {"
        "   var detailRows = attrTd.querySelectorAll('.row.detail_distro, .detail_distro');"
        "   if (detailRows.length > 0) {"
        "    for (var d = 0; d < detailRows.length; d++) {"
        "     var paramDivs = detailRows[d].querySelectorAll('.parameter');"
        "     var dparts = []; for (var p2 = 0; p2 < paramDivs.length; p2++) { dparts.push(paramDivs[p2].innerText.trim()); }"
        "     if (dparts.length > 0) attr += dparts.join('  ');"
        "    }"
        "   } else { attr = attrTd.innerText.trim(); }"
        "  }"
        "  if (interval && tp && filled) result.push({interval:interval, type:tp, filled:filled, countries:ctry, attributes:attr});"
        " }"
        " return result;"
        "}"
    )
    # Диагностика: показываем сырой HTML таблицы если данные пустые
    if not rows_data:
        debug_info = await page.evaluate(
            "() => {"
            " var t = document.querySelector('table');"
            " if (!t) return 'NO TABLE';"
            " var rows = t.querySelectorAll('tr');"
            " var out = 'rows=' + rows.length + ' | ';"
            " for (var i = 0; i < Math.min(3, rows.length); i++) {"
            "  out += '[' + rows[i].innerText.trim().substring(0, 80) + '] ';"
            " }"
            " return out;"
            "}"
        )
        log.info(f"Caps table debug: {debug_info}")

    if not rows_data:
        return f"❌ Broker {broker_id} has no caps."

    filter_all = not countries or "all" in [c.lower() for c in countries]

    lines = []
    for row in rows_data:
        country_cell = row.get("countries", "")
        attrs = row.get("attributes", "")

        # Фильтр по стране
        if not filter_all:
            if not any(c.lower() in country_cell.lower() for c in countries):
                continue

        # Фильтр по аффилиату если указан
        if affiliate_id:
            if str(affiliate_id) not in attrs:
                continue

        filled   = row.get("filled", "?")
        interval = row.get("interval", "").lower()
        cap_type = row.get("type", "").lower()
        country_label = country_cell if country_cell else "all countries"
        # attrs может содержать "1 parameters\nAffiliates *127" — убираем строку с "parameters"
        attr_lines = [l.strip() for l in attrs.split("\n") if l.strip() and "parameters" not in l.lower()]
        aff_info = " | " + ", ".join(attr_lines) if attr_lines else ""
        lines.append(f"• {country_label}: {filled}{aff_info}")

    if not lines:
        no_aff = f" for aff {affiliate_id}" if affiliate_id else ""
        return f"❌ No caps found for {', '.join(countries)}{no_aff}."

    return "\n".join(lines)


async def _delete_cap_without_params(page, country: str) -> bool:
    """Удалить капу без параметров для указанной страны. Возвращает True если удалено."""
    # Ищем строку с нужной страной И без параметров (нет badge.badge-primary)
    deleted = await page.evaluate("""(countryQuery) => {
        const rows = Array.from(document.querySelectorAll('table tr'))
            .filter(r => r.querySelector('td') !== null);
        for (const row of rows) {
            // Проверяем страну
            const countryText = row.innerText.toLowerCase();
            if (!countryText.includes(countryQuery.toLowerCase())) continue;
            // Проверяем что нет параметров (badge)
            const hasBadge = !!row.querySelector('span.badge-primary, span.badge.cursor-pointer');
            if (hasBadge) continue;
            // Нашли — кликаем красную кнопку
            const deleteBtn = row.querySelector('button.btn-danger, button.btn.btn-danger');
            if (deleteBtn) {
                deleteBtn.click();
                return true;
            }
        }
        return false;
    }""", country)

    if not deleted:
        log.info(f"No cap without params found for {country} to delete")
        return False

    log.info(f"Clicked delete for cap without params: {country}")
    await page.wait_for_timeout(600)

    # Подтверждаем удаление — кнопка DELETE в модальном окне
    try:
        confirm_btn = await page.wait_for_selector(
            "button.btn-ladda.btn-danger, .modal button.btn-danger, [role='dialog'] button.btn-danger",
            timeout=4000
        )
        await confirm_btn.click()
        await page.wait_for_timeout(1000)
        log.info(f"Cap deleted for {country}")
        return True
    except Exception as e:
        log.warning(f"Delete confirm button not found: {e}")
        await page.keyboard.press("Escape")
        return False


async def _add_affiliate_parameter(page, modal, affiliate_id, close_dropdown: bool = True) -> bool:
    """Добавить параметр Affiliates к капе. affiliate_id может быть строкой или списком строк."""
    # Нормализуем — всегда работаем со списком
    if isinstance(affiliate_id, str):
        aff_ids = [affiliate_id]
    elif isinstance(affiliate_id, list):
        aff_ids = [str(a) for a in affiliate_id]
    else:
        aff_ids = [str(affiliate_id)]
    # Нажимаем + ADD PARAMETER (синяя кнопка) через JS
    try:
        clicked = await page.evaluate("""() => {
            // Ищем синюю кнопку ADD PARAMETER (не зелёную submit)
            const btns = document.querySelectorAll('button');
            for (const btn of btns) {
                const txt = btn.innerText.trim();
                if (txt.includes('ADD PARAMETER') && !btn.classList.contains('btn-success')) {
                    btn.click();
                    return true;
                }
            }
            return false;
        }""")
        if not clicked:
            log.warning("ADD PARAMETER button not found via JS")
            return False
        log.info("Clicked ADD PARAMETER via JS")
        # Ждём пока Vue отрендерит строку с дропдауном Parameter
        await page.wait_for_timeout(1500)
    except Exception as e:
        log.warning(f"ADD PARAMETER error: {e}")
        return False

    # Выбираем тип параметра — "Affiliates"
    # Кликаем по внутреннему курсорному div smart__dropdown__input__element
    try:
        # Пробуем открыть Parameter dropdown несколько раз с паузами
        items_count = 0
        for attempt in range(5):
            clicked = await page.evaluate("""() => {
                // Вариант 1: по id
                const byId = document.querySelector('#fiqwoar .smart__dropdown__input__element');
                if (byId) { byId.click(); return 'by-id-inner'; }
                const byId2 = document.querySelector('#fiqwoar');
                if (byId2) { byId2.click(); return 'by-id'; }

                // Вариант 2: по лейблу "Parameter" — ищем cursor-pointer div
                const labels = document.querySelectorAll('label');
                for (const lbl of labels) {
                    if (lbl.innerText.trim().toLowerCase() === 'parameter') {
                        const row = lbl.closest('.form-row, .form-group') || lbl.parentElement;
                        // Ищем cursor-pointer (кликабельный inner div)
                        const inner = row?.querySelector('[class*="cursor-pointer"]') ||
                                      row?.querySelector('.smart__dropdown__input__element') ||
                                      row?.querySelector('[class*="smart__dropdown"]');
                        if (inner) { inner.click(); return 'by-label'; }
                    }
                }
                // Вариант 3: любой only-dropdown smart__dropdown в модалке
                const onlies = document.querySelectorAll('[class*="only-dropdown"] [class*="cursor-pointer"]');
                if (onlies.length > 0) { onlies[0].click(); return 'by-only-dropdown'; }
                return false;
            }""")
            log.info(f"Parameter dropdown click attempt {attempt+1}: {clicked}")
            await page.wait_for_timeout(600)

            items_count = await page.evaluate(
                "() => document.querySelectorAll('li.flex-fill, li.dropdown-item').length"
            )
            if items_count > 0:
                break
            await page.wait_for_timeout(400)

        log.info(f"Parameter dropdown items: {items_count}")

        if items_count == 0:
            log.warning("Parameter dropdown list did not appear after 5 attempts")
            return False

        selected = await page.evaluate("""() => {
            const items = document.querySelectorAll('li.flex-fill, li.dropdown-item');
            for (const item of items) {
                const t = item.innerText.trim().toLowerCase();
                if (t === 'affiliate' || t === 'affiliates') {
                    item.click();
                    return true;
                }
            }
            return false;
        }""")
        if not selected:
            log.warning("'Affiliate' not found in parameter list")
            return False
        # Ждём пока Vue отрендерит дропдаун аффов
        try:
            await page.wait_for_selector(
                "label[for*='parameterValue'] ~ *, .smart__dropdown__min__width, [id*='search-input']",
                timeout=3000
            )
        except Exception:
            pass
        await page.wait_for_timeout(600)
        log.info("Selected parameter type: Affiliates")
    except Exception as e:
        log.warning(f"Error selecting parameter type: {e}")
        return False

    # Теперь появился дропдаун для выбора аффилиата
    try:
        # Открываем дропдаун аффов — ищем по лейблу "Affiliate"
        await page.wait_for_timeout(400)
        await page.evaluate("""() => {
            const labels = document.querySelectorAll('label');
            for (const lbl of labels) {
                const t = lbl.innerText.trim().toLowerCase();
                if (t === 'affiliate' || t === 'affiliates') {
                    const row = lbl.closest('.form-row, .form-group');
                    const inner = row?.querySelector('.smart__dropdown__input__element') ||
                                  row?.querySelector('[class*="cursor-pointer"]') ||
                                  row?.querySelector('[class*="smart__dropdown"]');
                    if (inner) { inner.click(); return; }
                }
            }
        }""")
        await page.wait_for_timeout(600)

        # Для каждого аффилиата — ищем и выбираем
        # Это мультиселект: после клика на элемент дропдаун остаётся открытым
        for aff_idx, aff_id in enumerate(aff_ids):
            if aff_idx > 0:
                # Дропдаун уже открыт — просто ждём и очищаем поиск
                await page.wait_for_timeout(500)

            # Search input аффов — берём последний видимый
            aff_inp = None
            all_visible = await page.query_selector_all('input[id*="search-input"], input[id*="search"]')
            for inp in reversed(all_visible):
                if await inp.is_visible():
                    aff_inp = inp
                    break

            if not aff_inp:
                # Дропдаун мог закрыться — переоткрываем
                for reopen_attempt in range(3):
                    await page.evaluate("""() => {
                        const labels = document.querySelectorAll('label');
                        for (const lbl of labels) {
                            const t = lbl.innerText.trim().toLowerCase();
                            if (t === 'affiliate' || t === 'affiliates') {
                                const row = lbl.closest('.form-row, .form-group');
                                const inner = row?.querySelector('.smart__dropdown__input__element') ||
                                              row?.querySelector('[class*="cursor-pointer"]') ||
                                              row?.querySelector('[class*="smart__dropdown"]');
                                if (inner) { inner.click(); return; }
                            }
                        }
                    }""")
                    await page.wait_for_timeout(800)
                    all_visible = await page.query_selector_all('input[id*="search-input"], input[id*="search"]')
                    for inp in reversed(all_visible):
                        if await inp.is_visible():
                            aff_inp = inp
                            break
                    if aff_inp:
                        break
                    log.info(f"Reopen attempt {reopen_attempt+1}: search input not visible")

            if aff_inp:
                inp_id = await aff_inp.get_attribute("id")
                log.info(f"Aff search input: {inp_id}, searching for aff {aff_id}")
                # Фокус на поле
                await aff_inp.focus()
                await page.wait_for_timeout(200)
                # Тройной клик чтобы выделить всё
                await aff_inp.click(click_count=3)
                await page.wait_for_timeout(100)
                # Удаляем выделенное
                await page.keyboard.press("Delete")
                await page.wait_for_timeout(300)
                # Проверяем что поле пустое
                val = await aff_inp.input_value()
                if val:
                    # Если не очистилось — пробуем ещё раз
                    await aff_inp.click(click_count=3)
                    await page.keyboard.press("Backspace")
                    await page.wait_for_timeout(300)
                # Вводим новый ID посимвольно через клавиатуру
                for char in str(aff_id):
                    await page.keyboard.press(char)
                    await page.wait_for_timeout(100)
                log.info(f"Typed '{aff_id}' char by char via keyboard")
                # Ждём Vue фильтрацию
                await page.wait_for_timeout(800)
                for _ in range(10):
                    await page.wait_for_timeout(300)
                    cnt_check = await page.evaluate(
                        "() => document.querySelectorAll('li.flex-fill, li.dropdown-item').length"
                    )
                    if 0 < cnt_check < 20:
                        break
                await page.wait_for_timeout(200)
            else:
                log.warning(f"Aff search input not found for aff {aff_id}")
                continue

            cnt = await page.evaluate(
                "() => document.querySelectorAll('li.flex-fill, li.dropdown-item').length"
            )
            # Логируем первые 3 элемента для дебага
            first_items = await page.evaluate("""() => {
                const items = document.querySelectorAll('li.flex-fill, li.dropdown-item');
                return Array.from(items).slice(0, 3).map(i => i.innerText.trim());
            }""")
            log.info(f"Affiliate items after filter for {aff_id}: {cnt}, first: {first_items}")

            selected_aff = await page.evaluate("""(affId) => {
                const items = document.querySelectorAll('li.flex-fill, li.dropdown-item');
                for (const item of items) {
                    const txt = item.innerText.trim();
                    if (txt.includes('(' + affId + ')') ||
                        txt.startsWith(affId + ' ') ||
                        txt.startsWith('*' + affId) ||
                        txt === affId) {
                        item.click();
                        return txt;
                    }
                }
                if (items.length > 0) {
                    const first = items[0].innerText.trim();
                    items[0].click();
                    return 'fallback:' + first;
                }
                return null;
            }""", str(aff_id))

            if not selected_aff:
                log.warning(f"Affiliate {aff_id} not found in list (items={cnt})")
            else:
                log.info(f"Selected affiliate: {selected_aff}")
            await page.wait_for_timeout(300)

        # Закрываем дропдаун аффов — только для caps
        if close_dropdown:
            await page.evaluate("""() => {
                const labels = document.querySelectorAll('label');
                for (const lbl of labels) {
                    if (lbl.innerText.trim().toLowerCase() === 'affiliates') {
                        const row = lbl.closest('.form-row, .form-group');
                        const inner = row?.querySelector('.smart__dropdown__input__element') ||
                                      row?.querySelector('[class*="cursor-pointer"]') ||
                                      row?.querySelector('[class*="smart__dropdown"]');
                        if (inner) { inner.click(); return; }
                    }
                }
            }""")
            await page.wait_for_timeout(200)

    except Exception as e:
        log.warning(f"Error selecting affiliate: {e}")
        return False

    # Нажимаем зелёную кнопку ADD PARAMETER (подтверждение) через JS
    clicked_green = await page.evaluate("""() => {
        const btns = document.querySelectorAll('button');
        for (const btn of btns) {
            if (btn.innerText.trim().includes('ADD PARAMETER') &&
                (btn.classList.contains('btn-success') || btn.classList.contains('btn-ladda'))) {
                btn.click();
                return true;
            }
        }
        return false;
    }""")
    if clicked_green:
        await page.wait_for_timeout(800)
        log.info("Clicked green ADD PARAMETER via JS")
        return True
    else:
        log.warning("Green ADD PARAMETER button not found")
        return False


# ══════════════════════════════════════════
#  TELEGRAM BOT
# ══════════════════════════════════════════

def escape_md(text: str) -> str:
    """Экранировать спецсимволы Markdown v1 в тексте для Telegram."""
    for ch in ('*', '`', '['):
        text = text.replace(ch, f'\\{ch}')
    return text


def build_confirm_text(action: dict) -> str:
    """Сформировать текст запроса подтверждения."""
    a = action.get("action")

    if a == "change_hours":
        h = action.get("hours", {})
        brokers = ", ".join(str(b) for b in action.get("broker_ids", []))
        countries = ", ".join(action.get("countries", ["все"]))
        days = ", ".join(action.get("days_to_keep", ["Mon–Fri"]))
        return (
            f"📋 *Confirmation required*\n\n"
            f"Action: change working hours\n"
            f"Brokers: `{brokers}`\n"
            f"Time: `{h.get('start','?')} — {h.get('end','?')}`\n"
            f"Countries: {countries}\n"
            f"Days: {days}\n"
            f"No-traffic: {'✅ yes' if action.get('no_traffic', True) else '❌ no'}\n\n"
            f"Confirm?"
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
                f"📋 *Confirmation required*\n\n"
                f"Action: добавить hours for стран\n"
                f"Brokers: `{brokers}`\n"
                f"Countries: {countries_str}\n"
                f"Schedule:\n{sched_str}\n"
                f"No-traffic: {'✅ yes' if action.get('no_traffic', True) else '❌ no'}\n\n"
                f"Confirm?"
            )
        else:
            days = ", ".join(action.get("days_to_keep", ["Mon–Fri"]))
            lines = "\n".join(f"  • {ch['country']}: {ch['start']}–{ch['end']}" for ch in ch_list)
            return (
                f"📋 *Confirmation required*\n\n"
                f"Action: добавить hours for стран\n"
                f"Brokers: `{brokers}`\n"
                f"Countries & hours:\n{lines}\n"
                f"Days: {days}\n"
                f"No-traffic: {'✅ yes' if action.get('no_traffic', True) else '❌ no'}\n\n"
                f"Confirm?"
            )

    if a == "toggle_broker":
        brokers = ", ".join(str(b) for b in action.get("broker_ids", []))
        word = "ENABLE" if action.get("active") else "DISABLE"
        return (
            f"📋 *Confirmation required*\n\n"
            f"Action: {word} брокера\n"
            f"Brokers: `{brokers}`\n\n"
            f"Confirm?"
        )

    if a == "close_days":
        brokers = ", ".join(str(b) for b in action.get("broker_ids", []))
        cd_list = action.get("countries_days", [])
        lines = "\n".join(f"  • {cd['country']}: {', '.join(cd['days_to_close'])}" for cd in cd_list)
        return (
            f"📋 *Confirmation required*\n\n"
            f"Action: close days\n"
            f"Brokers: `{brokers}`\n"
            f"Countries & days:\n{lines}\n\n"
            f"Confirm?"
        )

    if a == "add_revenue":
        brokers = ", ".join(str(b) for b in action.get("broker_ids", []))
        cr_list = action.get("country_revenues", [])
        if cr_list:
            lines = "\n".join(
                f"  • {cr['country']}: ${cr['amount']}" +
                (f" (aff {cr['affiliate_id']})" if cr.get('affiliate_id') else "")
                for cr in cr_list
            )
        else:
            country = action.get("countries", ["all"])[0]
            amount = action.get("amount", "?")
            lines = f"  • {country}: ${amount}"
        return (
            f"📋 *Confirmation required*\n\n"
            f"Action: добавить прайс\n"
            f"Brokers: `{brokers}`\n"
            f"Countries & amounts:\n{lines}\n\n"
            f"Confirm?"
        )

    if a == "add_affiliate_revenue":
        aff_id = action.get("affiliate_id", "?")
        cr_list = action.get("country_revenues", [])
        lines = "\n".join(f"  • {cr['country']}: ${cr['amount']}" for cr in cr_list) if cr_list else "  • all countries"
        return (
            f"📋 *Confirmation required*\n\n"
            f"Action: add affiliate price\n"
            f"Affiliate: `{aff_id}`\n"
            f"Countries & amounts:\n{lines}\n\n"
            f"Confirm?"
        )

    if a == "change_caps":
        brokers = ", ".join(str(b) for b in action.get("broker_ids", []))
        cc_list = action.get("country_caps", [])
        # Обратная совместимость: старый формат
        if not cc_list:
            countries = action.get("countries", [])
            cap_val = action.get("caps", "?")
            cc_list = [{"country": c, "cap": cap_val} for c in countries]
        def _cap_line(cc):
            aff = f" (aff {cc['affiliate_id']})" if cc.get('affiliate_id') else ""
            if cc.get('delta') is not None:
                sign = "+" if int(cc['delta']) >= 0 else ""
                return f"  • {cc['country']}: {sign}{cc['delta']}{aff}"
            return f"  • {cc['country']}: {cc.get('cap', '?')}{aff}"
        lines = "\n".join(_cap_line(cc) for cc in cc_list)
        return (
            f"📋 *Confirmation required*\n\n"
            f"Action: change cap\n"
            f"Brokers: `{brokers}`\n"
            f"Countries & caps:\n{lines}\n\n"
            f"Confirm?"
        )

    if a == "lead_task":
        brokers = ", ".join(str(b) for b in action.get("broker_ids", []))
        # Hours info
        ch_list = action.get("country_hours", [])
        hours_lines = "\n".join(f"  • {ch['country']}: {ch['start']}–{ch['end']}" for ch in ch_list)
        days = ", ".join(action.get("days_to_keep", ["Mon–Fri"]))
        # Caps info
        cc_list = action.get("country_caps", [])
        def _cap_line_lt(cc):
            aff = f" (aff {cc['affiliate_id']})" if cc.get('affiliate_id') else ""
            return f"  • {cc['country']}: {cc.get('cap', '?')}{aff}"
        caps_lines = "\n".join(_cap_line_lt(cc) for cc in cc_list)
        return (
            f"📋 *Confirmation required*\n\n"
            f"Action: set hours + cap\n"
            f"Brokers: `{brokers}`\n"
            f"Hours:\n{hours_lines}\n"
            f"Days: {days}\n"
            f"Caps:\n{caps_lines}\n"
            f"No-traffic: {'✅ yes' if action.get('no_traffic', True) else '❌ no'}\n\n"
            f"Confirm?"
        )

    if a == "bulk_schedule":
        brokers = ", ".join(str(b) for b in action.get("broker_ids", []))
        ch_list = action.get("country_hours", [])
        days_keep = ", ".join(action.get("days_to_keep", []))
        days_close = ", ".join(action.get("days_to_close", []))
        # Группируем по часам для компактности
        from collections import defaultdict
        hours_groups = defaultdict(list)
        for ch in ch_list:
            hours_groups[f"{ch['start']}–{ch['end']}"].append(ch['country'])
        hours_lines = "\n".join(f"  • {h}: {', '.join(countries)}" for h, countries in hours_groups.items())
        close_str = f"\nClose: {days_close}" if days_close else ""
        return (
            f"📋 *Confirmation required*\n\n"
            f"Action: bulk schedule ({len(ch_list)} countries)\n"
            f"Brokers: `{brokers}`\n"
            f"Hours ({days_keep}):\n{hours_lines}{close_str}\n"
            f"Skip missing: ✅ yes\n\n"
            f"Confirm?"
        )

    if a == "multi_broker_task":
        tasks = action.get("tasks", [])
        lines = []
        for t in tasks:
            bid = t.get("broker_id", "?")
            country = t.get("country", "?")
            if t.get("type") == "close_day":
                lines.append(f"  🚫 {bid}: close {country} on {t.get('day', '?')}")
            else:
                cap_str = f", cap {t.get('cap')}" if t.get("cap") else ""
                hours_str = f" {t.get('start', '?')}–{t.get('end', '?')}" if t.get("start") else ""
                lines.append(f"  ✏️ {bid}: {country}{hours_str}{cap_str} ({t.get('day', '?')})")
        tasks_str = "\n".join(lines)
        return (
            f"📋 *Confirmation required*\n\n"
            f"Action: multi-broker task ({len(tasks)} brokers)\n"
            f"Tasks:\n{tasks_str}\n\n"
            f"Confirm?"
        )

    return f"📋 Action: `{a}`\n\nConfirm?"


async def _execute_get_task(bot, chat_id: int, action: dict, text: str):
    """Выполнить get-операцию (вызывается из очереди)."""
    a = action.get("action")
    try:
        if a == "get_prices":
            if not action.get("broker_ids"):
                action["broker_ids"] = ["_"]
            queries = action.get("queries", [])
            sub_results = []
            for q in queries:
                qtype = q.get("type", "broker")
                qid = q.get("id", "")
                qcountries = q.get("countries", [])
                lid = alog.log_action(f"get_{qtype}_revenue", str(qid),
                                      ", ".join(qcountries), "pending", user_command=text)
                if qtype == "affiliate":
                    sub_msg = await action_get_affiliate_revenue(str(qid), qcountries)
                    sub_results.append(f"*Aff {escape_md(str(qid))}:*\n{escape_md(sub_msg)}")
                else:
                    sub_msg = await action_get_broker_revenue(str(qid), qcountries)
                    sub_results.append(f"*Broker {escape_md(str(qid))}:*\n{escape_md(sub_msg)}")
                alog.update_action(lid, "success" if "❌" not in sub_msg else "error", sub_msg[:200])
            await bot.send_message(chat_id, "\n\n".join(sub_results), parse_mode="Markdown", disable_notification=True)

        elif a in ("get_broker_revenue", "get_affiliate_revenue"):
            if a == "get_broker_revenue":
                for bid in action.get("broker_ids", []):
                    lid = alog.log_action("get_broker_revenue", str(bid),
                                          ", ".join(action.get("countries", [])), "pending", user_command=text)
                    result = await action_get_broker_revenue(str(bid), action.get("countries", []))
                    alog.update_action(lid, "success" if "❌" not in result else "error", result[:200])
                    display_name = _last_broker_full_name if _last_broker_full_name != str(bid) else str(bid)
                    await bot.send_message(chat_id, f"*Broker {escape_md(display_name)}:*\n{escape_md(result)}", parse_mode="Markdown", disable_notification=True)
            else:
                aff_id = str(action.get("affiliate_id") or action.get("broker_ids", ["?"])[0])
                lid = alog.log_action("get_affiliate_revenue", aff_id,
                                      ", ".join(action.get("countries", [])), "pending", user_command=text)
                result = await action_get_affiliate_revenue(aff_id, action.get("countries", []))
                alog.update_action(lid, "success" if "❌" not in result else "error", result[:200])
                await bot.send_message(chat_id, f"*Aff {escape_md(aff_id)}:*\n{escape_md(result)}", parse_mode="Markdown", disable_notification=True)

        elif a == "get_hours":
            for bid in action.get("broker_ids", []):
                lid = alog.log_action("get_hours", str(bid),
                                      ", ".join(action.get("countries", ["all"])), "pending", user_command=text)
                result = await action_get_hours(str(bid), action.get("countries", ["all"]))
                alog.update_action(lid, "success" if "❌" not in result else "error", result[:200])
                display_name = _last_broker_full_name if _last_broker_full_name != str(bid) else str(bid)
                await bot.send_message(chat_id, f"*Broker {escape_md(display_name)}:*\n{escape_md(result)}", parse_mode="Markdown", disable_notification=True)

        elif a == "get_caps":
            for bid in action.get("broker_ids", []):
                lid = alog.log_action("get_caps", str(bid),
                                      ", ".join(action.get("countries", ["all"])), "pending", user_command=text)
                result = await action_get_caps(str(bid), action.get("countries", ["all"]),
                                                   affiliate_id=action.get("affiliate_id"))
                alog.update_action(lid, "success" if "❌" not in result else "error", result[:200])
                await bot.send_message(chat_id, f"*Caps {escape_md(str(bid))}:*\n{escape_md(result)}", parse_mode="Markdown", disable_notification=True)

        alog.set_status("last_action", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    except Exception as e:
        log.exception(f"Error in get task: {e}")
        await bot.send_message(chat_id, f"❌ Error: `{escape_md(str(e))}`", parse_mode="Markdown", disable_notification=True)


async def _execute_confirmed_task(bot, chat_id: int, action: dict):
    """Выполнить подтверждённое действие (вызывается из очереди)."""
    user_cmd = action.get("_user_command", "")
    log_ids = []

    try:
        results = []
        a = action["action"]

        # set_prices — мульти-прайс (брокер + аффилиат в одном сообщении)
        if a == "set_prices":
            price_tasks = action.get("price_tasks", [])
            for pt in price_tasks:
                pt_type = pt.get("type", "broker")
                pt_id = str(pt.get("id", ""))
                pt_country = pt.get("country", "")
                pt_amount = str(pt.get("amount", ""))
                lid = alog.log_action(f"set_prices_{pt_type}", pt_id, f"{pt_country} ${pt_amount}",
                                      "pending", user_command=user_cmd)
                log_ids.append(lid)
                try:
                    if pt_type == "affiliate":
                        sub_msg = await action_add_affiliate_revenue(pt_id, pt_country, pt_amount)
                        results.append(f"*Aff {escape_md(pt_id)}:*\n{escape_md(sub_msg)}")
                    else:
                        sub_msg = await action_add_revenue(pt_id, pt_country, pt_amount)
                        display_name = _last_broker_full_name if _last_broker_full_name != pt_id else pt_id
                        results.append(f"*Broker {escape_md(display_name)}:*\n{escape_md(sub_msg)}")
                    alog.update_action(lid, "success" if "❌" not in sub_msg else "error", sub_msg[:200])
                except Exception as e:
                    results.append(f"*{pt_type} {escape_md(pt_id)}:*\n❌ {escape_md(str(e))}")
                    alog.update_action(lid, "error", str(e)[:200])

            alog.set_status("last_action", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            msg_text = "\n\n".join(results) or "✅ Done."
            for attempt in range(3):
                try:
                    await bot.send_message(chat_id, msg_text, parse_mode="Markdown", disable_notification=True)
                    break
                except Exception:
                    if attempt < 2:
                        await asyncio.sleep(3)
            return

        # multi_broker_task — несколько брокеров из одного сообщения
        if a == "multi_broker_task":
            tasks = action.get("tasks", [])
            for task in tasks:
                t_type = task.get("type", "lead_task")
                t_broker = task.get("broker_id", "")
                t_country = task.get("country", "")
                t_day = task.get("day", "")

                lid = alog.log_action(f"multi_{t_type}", t_broker, f"{t_country} {t_day}",
                                      "pending", user_command=user_cmd)
                log_ids.append(lid)

                try:
                    if t_type == "close_day":
                        # Закрываем день
                        close_msg = await action_close_days(
                            broker_id=t_broker,
                            country=t_country,
                            days_to_close=[t_day]
                        )
                        display_name = _last_broker_full_name if _last_broker_full_name != t_broker else t_broker
                        results.append(f"*Broker {escape_md(display_name)}:*\n🚫 {escape_md(close_msg)}")
                        alog.update_action(lid, "success" if "❌" not in close_msg else "error", close_msg[:200])

                    else:
                        # lead_task — капа + часы
                        sub_parts = []

                        # Капа
                        if task.get("cap") is not None:
                            cap_msg = await action_change_caps(
                                broker_id=t_broker,
                                country=t_country,
                                cap_value=int(task["cap"]),
                                delta=None,
                                affiliate_id=None,
                                delete_first=False,
                            )
                            if cap_msg.startswith("__"):
                                cap_msg = await action_change_caps(
                                    broker_id=t_broker,
                                    country=t_country,
                                    cap_value=int(task["cap"]),
                                    delta=None,
                                    affiliate_id=None,
                                    delete_first=True,
                                )
                            sub_parts.append(f"🎯 Cap: {cap_msg}")

                        # Часы
                        if task.get("start") and task.get("end"):
                            # Проверяем есть ли страна у брокера
                            page = await get_page()
                            broker_base = await find_and_open_broker(page, t_broker)
                            if broker_base:
                                oh_url = f"{CRM_URL.rstrip('/')}{broker_base}/opening_hours"
                                await page.goto(oh_url, wait_until="domcontentloaded", timeout=60000)
                                existing = await _scrape_countries_from_page(page)
                                country_exists = any(t_country.lower() in ec.lower() for ec in existing)

                                weekends = {"saturday", "sunday"}
                                is_weekend = t_day.lower() in weekends if t_day else False

                                if t_day:
                                    if country_exists:
                                        hours_msg = await action_edit_country_add_days(
                                            broker_id=t_broker,
                                            country=t_country,
                                            start=task["start"],
                                            end=task["end"],
                                            no_traffic=task.get("no_traffic", True),
                                            days_to_add=[t_day]
                                        )
                                    else:
                                        new_days = [t_day] if is_weekend else ["Monday","Tuesday","Wednesday","Thursday","Friday"]
                                        hours_msg = await action_add_country_hours(
                                            broker_id=t_broker,
                                            country=t_country,
                                            start=task["start"],
                                            end=task["end"],
                                            no_traffic=task.get("no_traffic", True),
                                            days_filter=new_days
                                        )
                                else:
                                    hours_msg = await action_change_hours(
                                        broker_id=t_broker,
                                        start=task["start"],
                                        end=task["end"],
                                        countries_filter=[t_country],
                                        no_traffic=task.get("no_traffic", True),
                                        days_filter=["Monday","Tuesday","Wednesday","Thursday","Friday"]
                                    )
                                sub_parts.append(f"🕐 Hours: {hours_msg}")
                            else:
                                sub_parts.append(f"❌ Broker '{t_broker}' not found")

                        display_name = _last_broker_full_name if _last_broker_full_name != t_broker else t_broker
                        results.append(f"*Broker {escape_md(display_name)}:*\n{escape_md(chr(10).join(sub_parts))}")
                        alog.update_action(lid, "success" if not any("❌" in p for p in sub_parts) else "error",
                                          "; ".join(sub_parts)[:200])

                except Exception as e:
                    results.append(f"*Broker {escape_md(t_broker)}:*\n❌ {escape_md(str(e))}")
                    alog.update_action(lid, "error", str(e)[:200])

            alog.set_status("last_action", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            msg_text = "\n\n".join(results) or "✅ Done."
            for attempt in range(3):
                try:
                    await bot.send_message(chat_id, msg_text, parse_mode="Markdown", disable_notification=True)
                    break
                except Exception:
                    if attempt < 2:
                        await asyncio.sleep(3)
            return

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
                    days_filter=action.get("days_to_keep", action.get("days", ["Monday","Tuesday","Wednesday","Thursday","Friday"]))
                )
            elif a == "add_hours":
                country_hours_list = action.get("country_hours", [])
                if not country_hours_list:
                    h = action.get("hours", {})
                    countries = action.get("countries", [])
                    country = countries[0] if countries and "all" not in countries else ""
                    if country:
                        country_hours_list = [{"country": country, "start": h.get("start", "09:00"), "end": h.get("end", "17:00")}]

                if not country_hours_list:
                    msg = "❌ Укажи страну и hours for добавления."
                else:
                    schedule_groups = action.get("schedule_groups", [])
                    days_filter    = action.get("days_to_keep", action.get("days", ["Monday","Tuesday","Wednesday","Thursday","Friday"]))
                    requested_day  = action.get("requested_day", "")
                    skip_missing   = action.get("skip_missing", False)

                    weekends = {"saturday", "sunday"}
                    weekdays = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
                    req_day_lower = requested_day.lower() if requested_day else ""
                    is_weekend_request = req_day_lower in weekends
                    if not requested_day:
                        is_weekend_request = False

                    existing_countries = []
                    try:
                        page = await get_page()
                        broker_base = await find_and_open_broker(page, str(broker_id))
                        if broker_base:
                            oh_url = f"{CRM_URL.rstrip('/')}{broker_base}/opening_hours"
                            await page.goto(oh_url, wait_until="domcontentloaded", timeout=60000)
                            existing_countries = await _scrape_countries_from_page(page)
                            log.info(f"Existing countries: {existing_countries}")
                    except Exception as e:
                        log.warning(f"Failed to get existing countries: {e}")

                    sub_results = []
                    skipped = []

                    for ch in country_hours_list:
                        country_name  = ch.get("country", "")
                        country_start = ch.get("start", "09:00")
                        country_end   = ch.get("end", "17:00")

                        country_exists = any(country_name.lower() in ec.lower() for ec in existing_countries)

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
                                log.info(f"{country_name} exists → adding day {requested_day}")
                                sub_msg = await action_edit_country_add_days(
                                    broker_id=str(broker_id),
                                    country=country_name,
                                    start=country_start,
                                    end=country_end,
                                    no_traffic=action.get("no_traffic", True),
                                    days_to_add=[requested_day]
                                )
                            else:
                                if is_weekend_request:
                                    new_days_filter = [requested_day]
                                    log.info(f"{country_name} does not exist, weekend → creating only {new_days_filter}")
                                else:
                                    new_days_filter = weekdays
                                    log.info(f"{country_name} не exists → creating Mon-Fri")
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
                                log.info(f"Skipping {country_name} — not found for broker")
                                continue
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
                        sub_results.append(f"⏭ Skipped (not found): {', '.join(skipped)}")
                    msg = "\n".join(sub_results)
            elif a == "close_days":
                countries_days = action.get("countries_days", [])
                if not countries_days:
                    countries = action.get("countries", [])
                    country = countries[0] if countries and "all" not in countries else "all"
                    days_to_close = action.get("days_to_close", [])
                    if days_to_close:
                        countries_days = [{"country": country, "days_to_close": days_to_close}]

                if not countries_days:
                    msg = "❌ Please specify countries and days."
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
                if not country_revenues:
                    countries = action.get("countries", [])
                    country = countries[0] if countries and "all" not in countries else "all"
                    amount = action.get("amount", "")
                    if amount:
                        country_revenues = [{"country": country, "amount": amount}]

                if not country_revenues:
                    msg = "❌ Please specify country and amount."
                else:
                    sub_results = []
                    for cr in country_revenues:
                        sub_msg = await action_add_revenue(
                            broker_id=str(broker_id),
                            country=cr.get("country", "all"),
                            amount=str(cr.get("amount", "")),
                            affiliate_id=str(cr["affiliate_id"]) if cr.get("affiliate_id") else None
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
                    msg = "❌ Please specify country and amount."
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
            elif a == "change_caps":
                cc_list = action.get("country_caps", [])
                # Обратная совместимость: старый формат (countries + caps)
                if not cc_list:
                    countries = action.get("countries", [])
                    cap_val = action.get("caps")
                    if countries and cap_val is not None:
                        cc_list = [{"country": c, "cap": cap_val} for c in countries]
                if not cc_list:
                    msg = "❌ Please specify country and cap value."
                else:
                    sub_results = []
                    for cc in cc_list:
                        delta_val = cc.get("delta")
                        cap_val   = cc.get("cap")
                        delta_val  = cc.get("delta")
                        cap_val    = cc.get("cap")
                        aff_id_val   = cc.get("affiliate_id")
                        delete_first = cc.get("_delete_first", False)
                        # affiliate_id может быть строкой, списком или None
                        if isinstance(aff_id_val, list):
                            aff_param = aff_id_val  # передаём список как есть
                        elif aff_id_val is not None:
                            aff_param = str(aff_id_val)
                        else:
                            aff_param = None
                        sub_msg = await action_change_caps(
                            broker_id=str(broker_id),
                            country=cc.get("country", ""),
                            cap_value=int(cap_val) if cap_val is not None else 0,
                            delta=int(delta_val) if delta_val is not None else None,
                            affiliate_id=aff_param,
                            delete_first=delete_first,
                        )
                        # Есть капа без параметров — спрашиваем удалить или оставить
                        if sub_msg.startswith("__HAS_NO_PARAM_CAP__"):
                            _, hnp_country, hnp_cap, hnp_aff = sub_msg.split("|")
                            kb = [[
                                InlineKeyboardButton("🗑 Delete & recreate", callback_data="confirm_delete_cap"),
                                InlineKeyboardButton("➕ Keep & add new", callback_data="confirm"),
                            ]]
                            # Сохраняем оба варианта действий в pending
                            # confirm → просто создать новую (без удаления)
                            # confirm_delete_cap → сначала удалить, потом создать
                            create_action = {
                                "action": "change_caps",
                                "broker_ids": [str(broker_id)],
                                "country_caps": [{"country": hnp_country, "cap": int(hnp_cap), "affiliate_id": hnp_aff}],
                            }
                            delete_and_create_action = {
                                "action": "change_caps",
                                "broker_ids": [str(broker_id)],
                                "country_caps": [{"country": hnp_country, "cap": int(hnp_cap), "affiliate_id": hnp_aff, "_delete_first": True}],
                            }
                            sent = await bot.send_message(
                                chat_id,
                                f"⚠️ *{hnp_country}* already has a cap without parameters.\nWhat should I do?",
                                parse_mode="Markdown",
                                reply_markup=InlineKeyboardMarkup(kb),
                                disable_notification=True
                            )
                            # Сохраняем оба варианта — callback_data определит какой использовать
                            pending[(chat_id, sent.message_id)] = {
                                "confirm": create_action,
                                "confirm_delete_cap": delete_and_create_action,
                            }
                            sub_results.append(f"⚠️ {hnp_country}: cap without params exists — asked user")

                        # Если cap не найден — спрашиваем пользователя
                        elif sub_msg.startswith("__NO_CAP__"):
                            no_country, no_delta = sub_msg.split("|")[1], sub_msg.split("|")[2]
                            create_action = {
                                "action": "change_caps",
                                "broker_ids": [str(broker_id)],
                                "country_caps": [{"country": no_country, "cap": int(no_delta)}],
                            }
                            kb = [[
                                InlineKeyboardButton(f"✅ Create cap {no_delta}", callback_data="confirm"),
                                InlineKeyboardButton("❌ Cancel", callback_data="cancel"),
                            ]]
                            sent = await bot.send_message(
                                chat_id,
                                f"⚠️ *{no_country}* has no cap yet.\n"
                                f"Create new cap: *{no_delta}*?",
                                parse_mode="Markdown",
                                reply_markup=InlineKeyboardMarkup(kb),
                                disable_notification=True
                            )
                            pending[(chat_id, sent.message_id)] = create_action
                            sub_results.append(f"⚠️ {no_country}: no existing cap — asked for confirmation")
                        else:
                            sub_results.append(sub_msg)
                    msg = "\n".join(sub_results)

            elif a == "lead_task":
                # Комбинированная задача: сначала капа, потом часы
                sub_results = []

                # 1. Ставим капу
                cc_list = action.get("country_caps", [])
                for cc in cc_list:
                    cap_val = cc.get("cap")
                    aff_id_val = cc.get("affiliate_id")
                    if isinstance(aff_id_val, list):
                        aff_param = aff_id_val
                    elif aff_id_val is not None:
                        aff_param = str(aff_id_val)
                    else:
                        aff_param = None
                    try:
                        sub_msg = await action_change_caps(
                            broker_id=str(broker_id),
                            country=cc.get("country", ""),
                            cap_value=int(cap_val) if cap_val is not None else 0,
                            delta=None,
                            affiliate_id=aff_param,
                            delete_first=False,
                        )
                        if sub_msg.startswith("__"):
                            sub_msg = await action_change_caps(
                                broker_id=str(broker_id),
                                country=cc.get("country", ""),
                                cap_value=int(cap_val) if cap_val is not None else 0,
                                delta=None,
                                affiliate_id=str(aff_id_val) if aff_id_val is not None else None,
                                delete_first=True,
                            )
                        sub_results.append(f"🎯 Cap: {sub_msg}")
                    except Exception as cap_err:
                        sub_results.append(f"❌ Cap error: {cap_err}")

                # 2. Ставим часы
                country_hours_list = action.get("country_hours", [])
                if country_hours_list:
                    days_filter = action.get("days_to_keep", ["Monday","Tuesday","Wednesday","Thursday","Friday"])
                    requested_day = action.get("requested_day", "")
                    weekends = {"saturday", "sunday"}
                    weekdays = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
                    req_day_lower = requested_day.lower() if requested_day else ""
                    is_weekend_request = req_day_lower in weekends
                    if not requested_day:
                        is_weekend_request = False

                    existing_countries = []
                    try:
                        page = await get_page()
                        broker_base = await find_and_open_broker(page, str(broker_id))
                        if broker_base:
                            oh_url = f"{CRM_URL.rstrip('/')}{broker_base}/opening_hours"
                            await page.goto(oh_url, wait_until="domcontentloaded", timeout=60000)
                            existing_countries = await _scrape_countries_from_page(page)
                    except Exception as e:
                        log.warning(f"Failed to get existing countries: {e}")

                    for ch in country_hours_list:
                        country_name = ch.get("country", "")
                        country_start = ch.get("start", "09:00")
                        country_end = ch.get("end", "17:00")
                        country_exists = any(country_name.lower() in ec.lower() for ec in existing_countries)

                        try:
                            if requested_day:
                                if country_exists:
                                    sub_msg = await action_edit_country_add_days(
                                        broker_id=str(broker_id),
                                        country=country_name,
                                        start=country_start,
                                        end=country_end,
                                        no_traffic=action.get("no_traffic", True),
                                        days_to_add=[requested_day]
                                    )
                                else:
                                    new_days_filter = [requested_day] if is_weekend_request else weekdays
                                    sub_msg = await action_add_country_hours(
                                        broker_id=str(broker_id),
                                        country=country_name,
                                        start=country_start,
                                        end=country_end,
                                        no_traffic=action.get("no_traffic", True),
                                        days_filter=new_days_filter
                                    )
                            else:
                                if country_exists:
                                    sub_msg = await action_change_hours(
                                        broker_id=str(broker_id),
                                        start=country_start,
                                        end=country_end,
                                        countries_filter=[country_name],
                                        no_traffic=action.get("no_traffic", True),
                                        days_filter=days_filter
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
                            sub_results.append(f"🕐 Hours: {sub_msg}")
                        except Exception as hours_err:
                            sub_results.append(f"❌ Hours error: {hours_err}")

                msg = "\n".join(sub_results)

            elif a == "bulk_schedule":
                # Оптимизированное расписание: открываем страницу ОДИН раз,
                # для каждой страны — карандаш → модалка → часы + close → save
                sub_results = []
                country_hours_list = action.get("country_hours", [])
                days_keep = action.get("days_to_keep", [])
                days_close = action.get("days_to_close", [])
                days_keep_lower = [d.lower() for d in days_keep]
                days_close_lower = [d.lower() for d in days_close]

                # 1. Открываем страницу часов брокера ОДИН раз
                page = await get_page()
                broker_base = await find_and_open_broker(page, str(broker_id))
                if not broker_base:
                    msg = f"❌ Broker '{broker_id}' not found."
                else:
                    oh_url = f"{CRM_URL.rstrip('/')}{broker_base}/opening_hours"
                    await page.goto(oh_url, wait_until="domcontentloaded", timeout=60000)
                    try:
                        await page.wait_for_selector("button.btn-primary.btn-sm", timeout=12000)
                    except Exception:
                        pass
                    await page.wait_for_timeout(500)

                    existing_countries = await _scrape_countries_from_page(page)
                    log.info(f"Existing countries for {broker_id}: {len(existing_countries)}")

                    skipped = []
                    updated = []
                    errors = []

                    # 2. Для каждой страны — находим карандаш, открываем модалку, ставим всё за раз
                    for ch in country_hours_list:
                        country_name = ch.get("country", "")
                        country_start = ch.get("start", "09:00")
                        country_end = ch.get("end", "17:00")
                        country_exists = any(country_name.lower() in ec.lower() for ec in existing_countries)

                        if not country_exists:
                            skipped.append(country_name)
                            continue

                        try:
                            # Находим карандаш этой страны на уже открытой странице
                            edit_buttons = await page.query_selector_all("button.btn-primary.btn-sm, button.btn.btn-sm.btn-primary")
                            target_pencil = None
                            for btn in edit_buttons:
                                if (await btn.inner_text()).strip():
                                    continue
                                c_name = await btn.evaluate("""el => {
                                    const row = el.closest('tr');
                                    return row ? row.querySelector('td')?.innerText?.trim() : '';
                                }""")
                                if country_name.lower() in c_name.lower():
                                    target_pencil = btn
                                    break

                            if not target_pencil:
                                skipped.append(country_name)
                                continue

                            await target_pencil.click()
                            await page.wait_for_timeout(600)

                            try:
                                modal = await page.wait_for_selector(".modal-body, [role='dialog']", timeout=4000)
                            except Exception:
                                errors.append(f"{country_name}: modal didn't open")
                                continue

                            await page.wait_for_timeout(400)

                            sh, sm = (country_start.split(":") + ["00"])[:2]
                            eh, em = (country_end.split(":") + ["00"])[:2]
                            start_val = f"{sh.zfill(2)}:{sm.zfill(2)}"
                            end_val = f"{eh.zfill(2)}:{em.zfill(2)}"

                            # Проходим по чекбоксам дней в модалке
                            checkboxes = await modal.query_selector_all("input[type='checkbox']")
                            for cb in checkboxes:
                                label_text = await cb.evaluate("el => el.closest('label,tr,div')?.textContent?.toLowerCase() || ''")
                                if "no traffic" in label_text:
                                    # Включаем no-traffic
                                    if action.get("no_traffic", True) and not await cb.is_checked():
                                        await cb.evaluate("el => el.click()")
                                        await page.wait_for_timeout(80)
                                    continue

                                # Проверяем — этот день нужно включить (days_keep) или выключить (days_close)?
                                is_keep_day = any(d in label_text for d in days_keep_lower)
                                is_close_day = any(d in label_text for d in days_close_lower)

                                if is_keep_day:
                                    # Включаем + ставим часы
                                    if not await cb.is_checked():
                                        await cb.evaluate("el => el.click()")
                                        await page.wait_for_timeout(80)
                                    # Ставим время
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
                                elif is_close_day:
                                    # Выключаем (снимаем галочку)
                                    if await cb.is_checked():
                                        await cb.evaluate("el => el.click()")
                                        await page.wait_for_timeout(80)

                            # Сохраняем
                            try:
                                save_btn = await page.wait_for_selector("text=SAVE OPENING HOURS", timeout=3000)
                                await save_btn.click()
                                await page.wait_for_timeout(700)
                                updated.append(country_name)
                                log.info(f"✅ {country_name}: {start_val}-{end_val} + close {days_close}")
                            except Exception:
                                await _close_modal(page)
                                errors.append(f"{country_name}: save failed")

                        except Exception as e:
                            errors.append(f"{country_name}: {str(e)[:80]}")
                            log.exception(f"bulk_schedule error for {country_name}")

                    # 3. Формируем итог
                    if updated:
                        days_str = ", ".join(days_keep)
                        sub_results.append(f"✅ Updated {len(updated)} countries for {days_str}")
                    if days_close and updated:
                        close_str = ", ".join(days_close)
                        sub_results.append(f"✅ Closed {close_str} for {len(updated)} countries")
                    if skipped:
                        sub_results.append(f"⏭ Skipped ({len(skipped)}): {', '.join(skipped)}")
                    if errors:
                        sub_results.append(f"❌ Errors ({len(errors)}): {', '.join(errors)}")

                    msg = "\n".join(sub_results) if sub_results else "⚠️ Nothing to update."

            else:
                msg = f"⚠️ Действие '{a}' is not supported yet."

            if a in ("add_affiliate_revenue",):
                label = "Aff"
                display_name = str(broker_id)
            else:
                label = "Broker"
                display_name = _last_broker_full_name if _last_broker_full_name != str(broker_id) else str(broker_id)
            results.append(f"*{label} {escape_md(display_name)}:*\n{escape_md(msg)}")

        # Update logs
        for i, lid in enumerate(log_ids):
            res_text = results[i] if i < len(results) else ""
            status = "error" if "❌" in res_text else "success"
            alog.update_action(lid, status, res_text[:200])
        alog.set_status("last_action", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        msg_text = "\n\n".join(results) or "✅ Done."
        for attempt in range(3):
            try:
                await bot.send_message(chat_id, msg_text, parse_mode="Markdown", disable_notification=True)
                break
            except Exception as send_err:
                log.warning(f"Send attempt {attempt+1} failed: {send_err}")
                if attempt < 2:
                    await asyncio.sleep(3)

    except Exception as e:
        log.exception("Error executing action")
        for lid in log_ids:
            alog.update_action(lid, "error", str(e)[:200])
        for attempt in range(3):
            try:
                await bot.send_message(chat_id, f"❌ Error:\n`{escape_md(str(e))}`", parse_mode="Markdown", disable_notification=True)
                break
            except Exception:
                if attempt < 2:
                    await asyncio.sleep(3)


async def on_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id

    # Whitelist — чужие полностью игнорируются (без ответа)
    if user_id not in ALLOWED_USERS:
        return

    text = update.message.text.strip()

    # Если сообщение — ответ на другое сообщение, добавляем контекст
    reply_context = ""
    if update.message.reply_to_message and update.message.reply_to_message.text:
        reply_context = update.message.reply_to_message.text.strip()

    # В групповых чатах — реагируем только на сообщения, похожие на CRM-команды
    # В личке — обрабатываем всё
    is_group = update.effective_chat.type in ("group", "supergroup")
    if is_group:
        # Объединяем текст + контекст для проверки
        combined_text = f"{text}\n{reply_context}" if reply_context else text
        text_upper = combined_text.upper()
        # Паттерн 1: ISO код страны (2 заглавные буквы) + число 3-4 цифры (прайс)
        has_price_pattern = bool(re.search(r'\b[A-Z]{2}\b', text_upper) and re.search(r'\b\d{3,4}\b', combined_text))
        # Паттерн 2: CRM-команды (cap, wh, price, hours)
        text_lower = combined_text.lower()
        crm_commands = ("cap", "price", "wh ", "hours", "прайс", "часы", "кап", "лимит",
                        "schedule", "geo:", "desk", "off", "close", "закрыть", "выходн", "paused")
        has_command = any(kw in text_lower for kw in crm_commands)
        # Паттерн 3: время (HH:MM-HH:MM) — расписание
        has_time = bool(re.search(r'\d{1,2}:\d{2}\s*[-–]\s*\d{1,2}:\d{2}', combined_text))
        if not (has_price_pattern or has_command or has_time):
            return
        # CPL — игнорируем полностью
        if "cpl" in text.lower():
            return

    # Если есть контекст из reply — передаём AI оба текста
    if reply_context:
        text = f"[Ответ на сообщение:]\n{reply_context}\n\n[Новая команда:]\n{text}"

    # В личке показываем статус, в группе — молчим до результата
    if not is_group:
        await update.message.reply_text("🤔 Analyzing command...", disable_notification=True)

    action = await asyncio.get_event_loop().run_in_executor(None, parse_command, text)

    # Для add_affiliate_revenue broker_ids может быть пустым — используем affiliate_id
    if action.get("action") == "add_affiliate_revenue" and action.get("affiliate_id") and not action.get("broker_ids"):
        action["broker_ids"] = [str(action["affiliate_id"])]

    # Для get_affiliate_revenue тоже
    if action.get("action") == "get_affiliate_revenue" and action.get("affiliate_id") and not action.get("broker_ids"):
        action["broker_ids"] = [str(action["affiliate_id"])]

    # Get-операции — выполняем без подтверждения, через очередь
    if action.get("action") in ("get_prices", "get_broker_revenue", "get_affiliate_revenue", "get_hours", "get_caps"):
        queue_size = _task_queue.qsize()
        if queue_size > 0:
            await update.message.reply_text(f"⏳ Queued, position #{queue_size + 1}…", disable_notification=True)
        elif not is_group:
            emoji = "🔍" if action.get("action") != "get_hours" else "🕐"
            await update.message.reply_text(f"{emoji} Looking up...", disable_notification=True)
        await enqueue(_execute_get_task, context.bot, chat_id, action, text)
        return

    # Прайсы — выполняем без подтверждения, через очередь, без промежуточных сообщений
    if action.get("action") in ("add_revenue", "add_affiliate_revenue", "set_prices", "bulk_schedule"):
        queue_size = _task_queue.qsize()
        if queue_size > 0 and not is_group:
            await update.message.reply_text(f"⏳ Queued, position #{queue_size + 1}…", disable_notification=True)
        action["_user_command"] = text
        await enqueue(_execute_confirmed_task, context.bot, chat_id, action)
        return

    if action.get("action") == "unknown" or (not action.get("broker_ids") and action.get("action") not in ("set_prices", "bulk_schedule", "multi_broker_task")):
        log.warning(f"Command not recognized. action={action}")
        # В группе молчим, в личке — показываем подсказку
        if not is_group:
            await update.message.reply_text(
                "❓ Command not recognized. Try for example:\n\n"
                "• 'Nexus FR 10:00-18:00' — set hours\n"
                "• 'wh Nexus FR' — check hours\n"
                "• 'Nexus FR price' — check price\n"
                "• 'Legion DE cap 20' — set cap",
                disable_notification=True
            )
        return

    # Сохраняем и просим подтвердить
    kb = [[
        InlineKeyboardButton("✅ Execute", callback_data="confirm"),
        InlineKeyboardButton("❌ Cancel",    callback_data="cancel"),
    ]]
    sent = await update.message.reply_text(
        build_confirm_text(action),
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(kb),
        disable_notification=True
    )
    # Ключ = (chat_id, message_id) — каждая команда получает свой слот
    action["_user_command"] = text  # сохраняем для логирования
    pending[(chat_id, sent.message_id)] = action


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    pending_key = (chat_id, query.message.message_id)

    # Whitelist
    if user_id not in ALLOWED_USERS:
        return

    if query.data == "cancel":
        pending.pop(pending_key, None)
        await query.edit_message_text("❌ Cancelled. Nothing changed.")
        return

    stored = pending.pop(pending_key, None)
    if not stored:
        await query.edit_message_text("❌ Command expired. Please resend.")
        return

    # stored может быть dict с двумя вариантами (для confirm/confirm_delete_cap)
    # или просто action
    if isinstance(stored, dict) and "confirm" in stored and "confirm_delete_cap" in stored:
        action = stored.get(query.data, stored.get("confirm"))
    else:
        action = stored

    # Если выбрано удаление перед созданием — ставим флаг
    if query.data == "confirm_delete_cap":
        for cc in action.get("country_caps", []):
            cc["_delete_first"] = True

    queue_size = _task_queue.qsize()
    if queue_size > 0:
        await query.edit_message_text(f"⏳ Queued, position #{queue_size + 1}…")
    else:
        await query.edit_message_text("⏳ Working on it...")

    await enqueue(_execute_confirmed_task, context.bot, chat_id, action)


async def on_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ALLOWED_USERS:
        return
    await update.message.reply_text(
        "👋 Hi! I'm the LeadGreed CRM bot.\n\n"
        "I can:\n"
        "• Set broker working hours\n"
        "• Look up hours and prices\n"
        "• Set caps and prices\n\n"
        "Send a command — I understand 😊",
        disable_notification=True
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

async def _post_init(application):
    """Инициализация после создания event loop."""
    global _task_queue, _queue_worker_task
    _task_queue = asyncio.Queue()
    _queue_worker_task = asyncio.create_task(_queue_worker())
    log.info("Task queue started.")


def main():
    app = Application.builder().token(TELEGRAM_TOKEN).post_init(_post_init).build()
    app.add_handler(CommandHandler("start", on_start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_message))
    app.add_handler(CallbackQueryHandler(on_callback))

    log.info("Bot started ✅")
    alog.set_status("bot_started", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()