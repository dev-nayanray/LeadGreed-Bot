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

# Кэш base_path брокеров — ключ: broker_id, значение: (base_path, timestamp)
_broker_path_cache: dict = {}
_BROKER_CACHE_TTL = 300  # 5 минут

# Ротации для отчётов
# Формат: {"broker_name": {"affs": ["122","123"], "country": "Germany"}, ...}
today_rotations: dict = {}
tomorrow_rotations: dict = {}
# Ротации для которых уже отправили "started" уведомление
fired_started: set = set()

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
_worker_busy: bool = False  # True пока воркер выполняет задачу


async def _queue_worker():
    """Воркер — обрабатывает задачи из очереди по одной."""
    global _worker_busy
    while True:
        task_func, args, kwargs = await _task_queue.get()
        _worker_busy = True
        try:
            await task_func(*args, **kwargs)
        except Exception as e:
            log.exception(f"Queue error: {e}")
        finally:
            _worker_busy = False
            _task_queue.task_done()


async def enqueue(task_func, *args, **kwargs):
    """Добавить задачу в очередь. Возвращает True если задача встала в очередь (воркер занят)."""
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
  "set Legion inactive" / "deactivate Legion" / "disable Capitan" → {"action": "toggle_broker", "broker_ids": ["Legion"], "active": false}
  "activate Legion" / "enable Capitan" / "set Legion active" → {"action": "toggle_broker", "broker_ids": ["Legion"], "active": true}
  "swin all crg integrations close" / "close all swin crg" → {"action": "toggle_broker", "broker_ids": ["Swinftd CRG"], "active": false}
  "close/disable/inactive/set inactive/put inactive/make inactive" = deactivate (active: false)
  "open/enable/active/activate/set active/put active/bring back/back to active/is back/is active now/active now/they're back/use X for" = activate (active: true)
  Дополнительные примеры (все → toggle_broker):
  "nexus system bring back to active" → broker_ids: ["Nexus"], active: true
  "open back ventury" → broker_ids: ["Ventury"], active: true
  "active now" (в контексте брокера) → active: true
  "Pls use Fusion for PT injection" / "Put them back active" → broker_ids: ["Fusion"], active: true
  "please put Swin back to active" → broker_ids: ["Swinftd"], active: true  (все Swin интеграции)
  "MN is inactive till the next year" → broker_ids: ["MN"], active: false
  "Kaya is back to active" → broker_ids: ["Kaya"], active: true
  "Axia put active" → broker_ids: ["Axia"], active: true
  ВАЖНО: "DE PL RO are working today" после имени брокера — это НЕ toggle, это контекст/информация, игнорируй
  ВАЖНО: "they solved the issue" / "will update once" / "till the next year" — информационный текст, игнорируй
  Бот сам найдёт все совпадения и пропустит уже неактивных/активных.
- add_affiliate_revenue — добавить прайс/выплату для аффилиата
- set_prices            — добавить прайсы для НЕСКОЛЬКИХ объектов (брокер + аффилиат) в одном сообщении
- get_affiliate_revenue — узнать прайс аффилиата для страны
- get_broker_revenue   — узнать прайс брокера для страны
- get_hours            — узнать текущие часы работы брокера для страны
- change_caps          — изменить дневной лимит (cap) брокера для страны
- map_affiliate        — добавить маппинг аффилиата для брокера (Override Affiliate ID's)
- funnel_slug_override — добавить API Offer Slug Override для брокера (фаннел маппинг)
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
  "amount": null,
  "override_code": null,
  "override_codes": [],
  "funnel_countries": [],
  "affiliate_ids": []
  "country": null
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
- Региональные алиасы — заменяй на список стран:
  • "nordics" / "nordic" / "нордикс" → Ireland, Norway, Belgium, Denmark, Sweden, Finland
  • Пример: "167 nordics price" → countries: ["Ireland", "Norway", "Belgium", "Denmark", "Sweden", "Finland"]
- ВАЖНО: Никогда не склеивай имя брокера/аффилиата и страну в одно поле. ISO коды (DE, FR, ES, HR, ID, NL, CZ...) и названия стран (германия, испания...) — это ВСЕГДА countries, а не часть broker_ids. Даже если ISO код стоит СРАЗУ после имени брокера без разделителя.
- ВАЖНО: Если в сообщении указан числовой ID брокера (например "2251 - Fugazi CH - CRG"), ВСЕГДА используй числовой ID в broker_ids: ["2251"]. Числовой ID надёжнее имени. Форматы: "2251 - Fugazi", "2251-Fugazi", "#2251", "ID 2251". В multi_broker_task тоже используй числовой ID в broker_id если он указан.
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

ВАЖНО — маппинг MN:
У MN есть несколько интеграций в CRM, но работаем только с двумя:
  • "1961 - MN FR" (ID 1961) — ТОЛЬКО для Франции
  • "272 - MN" (ID 272) — для всех остальных стран (не Франции)
  MN 216 и MN FR 216 (ID 3192, 3202) — НЕ трогаем, игнорируем.
Правила выбора:
  • Страна = France → broker_ids: ["1961"]
  • Страна ≠ France → broker_ids: ["272"]
  • Если страны разные (одна FR + другие) → две отдельных задачи: "1961" для FR, "272" для остальных
Примеры:
  "mn fr wh" → {"action": "get_hours", "broker_ids": ["1961"], "countries": ["France"]}
  "1961 mn fr wh" → {"action": "get_hours", "broker_ids": ["1961"], "countries": ["France"]}
  "mn de wh" → {"action": "get_hours", "broker_ids": ["272"], "countries": ["Germany"]}
  "mn fr de wh" → broker_ids: ["1961"] для France, broker_ids: ["272"] для Germany (два запроса)
  "MN FR 1400" → {"action": "add_revenue", "broker_ids": ["1961"], "country_revenues": [{"country": "France", "amount": 1400}]}
  "MN DE 1200" → {"action": "add_revenue", "broker_ids": ["272"], "country_revenues": [{"country": "Germany", "amount": 1200}]}
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
- ВАЖНО: close_days требует конкретную страну. Если страна не указана — ставь "country": "all" (закрыть все страны).
  "Legion DE pause" → country: "Germany". "Universo Friday off" → country: "all" (все страны).
- ВАЖНО: Если в сообщении перечислены конкретные страны (ISO коды или названия) — закрывай ТОЛЬКО их, не все страны брокера!
  "Nexus need to close Sat BE FI NL" → закрыть только Belgium, Finland, Netherlands на Saturday
  "Nexus close BE FI" → закрыть только Belgium, Finland (не все страны Nexus!)
- Для close_days: days_to_close = список дней которые нужно закрыть
  • "pause" / "paused" без дня → закрыть ТОЛЬКО сегодня
  • "close" / "off" без конкретного дня И без перечня стран → закрыть ВСЕ рабочие дни (Mon-Fri): days_to_close: ["Monday","Tuesday","Wednesday","Thursday","Friday"]
  • "close" с перечнем стран но без дня → закрыть только сегодня для этих стран
  • "Sat" / "Saturday" / "суббота" → days_to_close: ["Saturday"]
  • "Sun" / "Sunday" / "воскресенье" → days_to_close: ["Sunday"]
  • "Friday off" → только пятницу: days_to_close: ["Friday"]
  • "close weekend" → days_to_close: ["Saturday","Sunday"]
  Примеры:
  "Axia close AR BR CL" → days_to_close: ["Monday","Tuesday","Wednesday","Thursday","Friday"] (нет дня = все рабочие)
  "Nexus need to close Sat BE FI NL NZ AU DK NO SE IE" → broker: Nexus, days_to_close: ["Saturday"], countries: [Belgium, Finland, Netherlands, New Zealand, Australia, Denmark, Norway, Sweden, Ireland]
- ВАЖНО: Правило различения брокера и аффилиата при запросе прайса:
  • Просто число + страны + "прайс/price/payout/выплата" → ЭТО АФФИЛИАТ (get_affiliate_revenue)
    Примеры: "28 прайс испания", "159 DE price", "28 франция прайс", "what payout we have in nordics 167"
  • Число может стоять В ЛЮБОМ МЕСТЕ фразы — до или после стран/ключевого слова
    Примеры: "167 nordics price", "nordics 167 price", "what payout in nordics 167" — всё это get_affiliate_revenue, affiliate_id: "167"
  • Имя (текст) + страны + "прайс/price/payout" → ЭТО БРОКЕР (get_broker_revenue)
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
  В любом случае: если первая строка начинается с числа и не содержит часов (HH:MM) — нужно определить, брокер это или аффилиат:

  Признаки что это БРОКЕР (используй add_revenue):
  • Формат "ЧИСЛО - ИмяБрокера" (число + дефис + текст с именем бренда): "3422 - Naga Joshua", "2251 - Fugazi CH"
  • Число из 4+ цифр (у нас нет аффилиатов с 4-значным ID — самый большой афф ~233)
  • После числа есть имя/название бренда (слова которые не являются ISO кодами стран)
  В этом случае: broker_id = это число, а число на следующей строке (если есть) = affiliate_id параметр

  Признаки что это АФФИЛИАТ (используй add_affiliate_revenue):
  • Просто число без имени и без дефиса: "159", "71", "28"
  • Число из 1-3 цифр стоит одно на строке
  • ОБЯЗАТЕЛЬНО: должна быть явная сумма прайса (обычно 400–2000). Без суммы — это НЕ прайс, action: "unknown"

  ВАЖНО: если в сообщении есть число (ID аффа) + страны, но НЕТ явной суммы — это НЕ add_affiliate_revenue.
  Примеры сообщений БЕЗ суммы — action: "unknown":
  • "112 pls set in\nES MN/Avelux\nIT MN/Nexus" — нет суммы, непонятная команда → unknown
  • "159 DE ES" — нет суммы → unknown
  Примеры С суммой — add_affiliate_revenue:
  • "159 DE 1300" — есть сумма 1300 → ok
  • "112\nES 800\nIT 900" — есть суммы → ok

  Примеры:
  • "3422 - Naga Joshua\n71 BR 800" → broker_id: "3422", country: "Brazil", amount: 800, affiliate_id: "71" (add_revenue)
  • "2251 - Fugazi CH\n122 DE 1600 16%" → broker_id: "2251", country: "Germany", amount: 1600, affiliate_id: "122" (add_revenue)
  • "71\nBR 800" → affiliate_id: "71", country: "Brazil", amount: 800 (add_affiliate_revenue)
  • "159\nFR 1000\nES 1100" → affiliate_id: "159" (add_affiliate_revenue)
  • "228 ID 650\n228 MY 1150" → affiliate_id: "228", country_revenues: [{"country": "Indonesia", "amount": 650}, {"country": "Malaysia", "amount": 1150}] (add_affiliate_revenue)
    ВАЖНО: здесь "ID" = Indonesia (ISO код), НЕ идентификатор! "228" = аффилиат, "ID" = страна, "650" = сумма.
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
- ВАЖНО: "100%", "50%", "N%" БЕЗ числа-прайса перед ним — это НЕ прайс, это процент распределения. Если единственное число в сообщении — с процентом, НЕ создавай add_revenue.
  "Ave cpa AT pls add 21/117/28/13 for them 100%" → action: "unknown" (это запрос на добавление аффилиатов, не CRM-команда)
  "pls add N/N/N for them" — это инструкция для других людей, ИГНОРИРУЙ
- Числа через / (21/117/28/13) — это ID аффилиатов, НЕ прайсы
- "test", "test tomorrow", "test today" — ПОЛНОСТЬЮ ИГНОРИРУЙ, это пометки менеджера
- "tomorrow", "today" в контексте прайсов — ИГНОРИРУЙ (это когда прайс начнёт действовать, не наше дело)
- "in fb", "from fb", "in google", "from google", "fb traffic", "google traffic" и подобные — это источник трафика, ПОЛНОСТЬЮ ИГНОРИРУЙ, не влияет на действие
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
  • Числа в начале (17) — affiliate id → сохраняй в поле "affiliate_ids": ["17"] в lead_task
  • Пример: "12 AU CRG for tomorrow to Swin..." → lead_task должен содержать "affiliate_ids": ["12"]
  • Пример: "122/191 DE CRG for tomorrow to Legion..." → lead_task должен содержать "affiliate_ids": ["122", "191"]
  • ISO коды стран (CA, DE, BR...) — переводи в полное название → country
  • Языки (EN, RU, PL...) — ИГНОРИРУЙ (это язык деска, не страна)
  • Тип сделки (CPA, CRG) — запомни тип, используй для определения нужен ли affiliate_id в капе
  • "today" / "сегодня" → days_to_keep: [название сегодняшнего дня]
  • "tomorrow" / "завтра" → days_to_keep: [название завтрашнего дня]
  • Название дня (Monday, Saturday...) → days_to_keep: [этот день]
  • "from Monday" / "from [день]" / "starting Monday" / "с понедельника" — НЕ означает "только понедельник"!
    Это означает "начиная с этого момента, все рабочие дни". → days_to_keep: ["Monday","Tuesday","Wednesday","Thursday","Friday"], requested_day: null
- Вторая строка содержит брокера, капу и часы:
  • Первое слово/фраза до числа — имя брокера → broker_ids
  • "N cap" — лимит → country_caps: [{country, cap: N}]
  • HH:MM-HH:MM или HH:MM–HH:MM — часы работы → hours start/end
  • HH.MM-HH.MM (с точками) — тоже часы работы. Конвертируй в HH:MM формат: 14.00 → 14:00, 22.30 → 22:30, 17.00 → 17:00
  • "00:00" в конце времени означает полночь — оставляй как "00:00"
  • gmt+N / UTC+N — часовой пояс, конвертируй в GMT+3 (CRM работает в GMT+3)
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
- Если перед именем брокера указан числовой ID (например "2251 - Fugazi CH - CRG"), используй ID: broker_id: "2251"
- "cap total" или просто "cap" без "aff" — капа БЕЗ affiliate_id
- Строка "PAUSED" или "паузд" после имени брокера — ЗАКРЫТЬ часы этого брокера на указанный день
- Строки с "funnel", "map", "keep", "sharing", "dif", "rotation", "what's the rotation", "whats the rotation" — ИГНОРИРУЙ, это не CRM-команды. "rotation" = распределение лидов, НЕ часы.
- gmt+N — конвертируй время в GMT+3

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
      "no_traffic": true,
      "affiliate_ids": ["122", "191"]
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
      "no_traffic": true,
      "affiliate_ids": ["12"]
    }
  ]
}
Каждая подзадача имеет type:
  - "lead_task" — поставить часы + капу (cap может быть null если только часы)
  - "close_day" — закрыть день для страны
  - "funnel_override" — добавить фаннел маппинг (override_codes, funnel_countries, affiliate_ids или affiliate_id)
    Триггеры для funnel_override: "Funnel - X", "Funnels - X", "Funnels to map: X", "map funnel X", "funnel X", "mapping X", "Funnel mapping X" — ВСЕГДА создавай задачу funnel_override, даже если рядом есть фразы типа "same as prev week", "as usual", "like before" и т.д.
    Фраза "same as prev week" / "as usual" — НЕ означает пропустить маппинг, а просто уточнение от пользователя. Маппинг всё равно нужно выполнить.
    Affiliate IDs для фаннела берутся из числового префикса сообщения (например "225 UK CRG" → affiliate_ids: ["225"]).
  - "affiliate_override" — добавить маппинг аффилиата (affiliate_id, override_code, country)
    Если нужно замаппить НЕСКОЛЬКО аффов с одним override_code ("map as 123 all", "map all as 123") — создай отдельную задачу affiliate_override для каждого аффа из списка.
    "map as 123 all" / "map all as 123" / "map as 123 in case we are sharing" → для каждого аффа из списка: {"type": "affiliate_override", "broker_id": ..., "affiliate_id": "XXX", "override_code": "123"}
    Пример: "123/122/28 Legion ... map as 123 all" →
    [
      {"type": "affiliate_override", "broker_id": "Legion", "affiliate_id": "123", "override_code": "123"},
      {"type": "affiliate_override", "broker_id": "Legion", "affiliate_id": "122", "override_code": "123"},
      {"type": "affiliate_override", "broker_id": "Legion", "affiliate_id": "28",  "override_code": "123"}
    ]
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
CRM работает в GMT+3. Если в сообщении указан другой часовой пояс (GMT+2, GMT+1, UTC+0...) — ОБЯЗАТЕЛЬНО конвертируй все часы в GMT+3 перед записью в JSON.
Формула: время_GMT3 = время_оригинал - (оригинал_offset - 3)
Примеры:
  • GMT+3 → GMT+3: ничего не меняем. "14:00-20:00 GMT+3" → "14:00-20:00"
  • GMT+2 → GMT+3: прибавляем 1 час. "10:00-18:00 GMT+2" → "11:00-19:00"
  • GMT+1 → GMT+3: прибавляем 2 часа. "10:00-18:00 GMT+1" → "12:00-20:00"
  • GMT+0 → GMT+3: прибавляем 3 часа. "08:00-16:00 GMT+0" → "11:00-19:00"
  • Если часовой пояс не указан — считай что время уже в GMT+3, ничего не конвертируй.
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

- Для map_affiliate: broker_ids = брокер, affiliate_id = ID аффилиата, override_code = код маппинга, country = страна (опционально)
  Формат: "map aff 123 to Nexus override 456" или "Nexus / aff 123 / DE / override 456"
  Примеры:
  • "map aff 123 to Nexus with override 456 for DE" → {"action": "map_affiliate", "broker_ids": ["Nexus"], "affiliate_id": "123", "override_code": "456", "country": "Germany"}
  • "Nexus aff 123 override 456" → {"action": "map_affiliate", "broker_ids": ["Nexus"], "affiliate_id": "123", "override_code": "456", "country": null}
  • "добавь маппинг аффа 123 для Nexus DE override 789" → {"action": "map_affiliate", "broker_ids": ["Nexus"], "affiliate_id": "123", "override_code": "789", "country": "Germany"}
  Если страна не указана — country: null (маппинг для всех стран)

- Для funnel_slug_override: broker_ids = брокер, override_code = название фаннела (текст), countries = список стран (опционально), affiliate_id = ID аффилиата (опционально)
  Ключевые слова: "funnel", "slug", "фаннел", "slug override", "funnel mapping", "funnel override", "api offer slug"
  ВАЖНО: "for DE", "for Germany", "for DE ES" и т.д. — это страны → funnel_countries. ISO коды после "for" — это всегда страны.
  ВАЖНО: "aff 123", "aff123", "affiliate 123" — это affiliate_id = "123". Число после "aff" — всегда аффилиат.
  Используй поле "funnel_countries" (не "countries"!) чтобы не путаться с другими правилами.
  Если фаннел ОДИН — используй override_codes: ["Название"] (список из одного элемента).
  Если фаннелов НЕСКОЛЬКО — все в одном списке override_codes, это одна запись в CRM.
  Если аффилиатов НЕСКОЛЬКО (формат "122/123" или "122, 123") — используй поле affiliate_ids (список), affiliate_id оставь null.
  Если аффилиат ОДИН — используй affiliate_id (строка), affiliate_ids оставь [].
  Примеры:
  • "Nexus funnel override Pemex for DE" → {"action": "funnel_slug_override", "broker_ids": ["Nexus"], "override_codes": ["Pemex"], "funnel_countries": ["Germany"], "affiliate_id": null, "affiliate_ids": []}
  • "Naga Joshua funnel override Pemex for DE aff 123" → {"action": "funnel_slug_override", "broker_ids": ["Naga Joshua"], "override_codes": ["Pemex"], "funnel_countries": ["Germany"], "affiliate_id": "123", "affiliate_ids": []}
  • "122/123 AVE CRG Funnels - FinanzBot KI, KI Trading" → {"action": "funnel_slug_override", "broker_ids": ["AVE CRG"], "override_codes": ["FinanzBot KI", "KI Trading"], "funnel_countries": [], "affiliate_id": null, "affiliate_ids": ["122", "123"]}
  • "Funnels - FinanzBot KI, KI Trading (2 dif funnels pls to map)" → override_codes: ["FinanzBot KI", "KI Trading"]
  • "funnel to map - AI Trading App" → override_codes: ["AI Trading App"]
  • "map funnel as Immediate Profit" → override_codes: ["Immediate Profit"]
  override_codes — список точных названий фаннелов (текст, не числа). Если страна не указана — funnel_countries: []
  ВАЖНО: если команда содержит "last funnel", "same funnel as", "map funnel as last one in X country", "funnel as last one in X id" — это запрос на автоматическое определение фаннела.
  В этом случае используй поля: use_last_funnel: true, reference_affiliate: "<id аффа>", reference_country: "<страна>", override_codes: []
  Пример: "map funnel as last one in 33 AU" → {
    "type": "funnel_override",
    "broker_id": "...",
    "use_last_funnel": true,
    "reference_affiliate": "33",
    "reference_country": "Australia",
    "override_codes": [],
    "funnel_countries": ["Australia"],
    "affiliate_id": null,
    "affiliate_ids": []
  }

- Возвращай ТОЛЬКО JSON

Правила для коротких команд в группах:
- "Broker in fb" / "Broker from fb" / "Broker in google" — это просто информация об источнике трафика, НЕ команда → action: "unknown"
- "Legion DE pause for now" / "Legion DE pause" / "пауза Legion DE" → закрыть часы Legion для Germany на сегодня
  → {"action": "close_days", "broker_ids": ["Legion"], "countries_days": [{"country": "Germany", "days_to_close": ["<сегодняшний день>"]}]}
- "legion is back in de today at 11:00" / "Legion DE back at 11:00" → поставить часы с 11:00 до конца рабочего дня (19:00 по умолчанию)
  → {"action": "add_hours", "broker_ids": ["Legion"], "country_hours": [{"country": "Germany", "start": "11:00", "end": "19:00"}], "days_to_keep": ["<сегодня>"], "requested_day": "<сегодня>"}
- "pause" / "paused" / "пауза" = закрыть часы
- "back" / "is back" / "started" / "resume" = открыть/поставить часы
- "Legion DE closed?" / "Nexus FR open?" → проверить часы (get_hours)
  → {"action": "get_hours", "broker_ids": ["Legion"], "countries": ["Germany"]}
- Если указано "at HH:MM" без конечного времени — start = указанное время, end = null (бот сам прочитает текущий end из CRM)

Контекст ответа:
Иногда команда приходит как ответ на другое сообщение. Формат:
[Ответ на сообщение:]
<текст оригинального сообщения>

[Новая команда:]
<текст новой команды>

В этом случае:
- Используй оригинальное сообщение для КОНТЕКСТА (имя брокера, список стран, часы)
- Используй новую команду как ДЕЙСТВИЕ (что нужно сделать)
- ВАЖНО: "today"/"tomorrow"/"yesterday" в оригинальном сообщении могут быть УСТАРЕВШИМИ (сообщение могло быть написано вчера). Если новая команда НЕ содержит "tomorrow"/"today" — считай что действие нужно выполнить СЕГОДНЯ (requested_day = сегодняшний день). "today"/"tomorrow" из reply-контекста ИГНОРИРУЙ.
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
        system=[{
            "type": "text",
            "text": SYSTEM_PROMPT,
            "cache_control": {"type": "ephemeral"}
        }],
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
    """Wrapper с кэшированием результата."""
    import time
    cache_key = str(broker_id).strip().lower()
    if cache_key in _broker_path_cache:
        cached_path, cached_time, cached_name = _broker_path_cache[cache_key]
        if time.time() - cached_time < _BROKER_CACHE_TTL:
            log.info(f"find_and_open_broker: cache hit for '{broker_id}' → {cached_path}")
            global _last_broker_full_name
            if cached_name:
                _last_broker_full_name = cached_name
            return cached_path
    result = await _find_and_open_broker_impl(page, broker_id, country_hint)
    if result:
        _cache_broker_path(broker_id, result, _last_broker_full_name)
    return result


async def _find_and_open_broker_impl(page: Page, broker_id: str, country_hint: str = None) -> Optional[str]:
    """
    Найти брокера и вернуть его base path (/clients/ID).
    Возвращает None если брокер not found.
    country_hint — название страны, для LATAM-маршрутизации.
    """
    import time
    global _last_broker_full_name
    _last_broker_full_name = broker_id  # fallback
    broker_id = str(broker_id).strip()

    # Определяем, нужен ли LATAM-вариант
    is_latam = False
    if country_hint and country_hint.lower() in LATAM_COUNTRIES:
        is_latam = True
    log.info(f"find_and_open_broker: broker_id='{broker_id}', country_hint='{country_hint}', is_latam={is_latam}")

    # Если ID числовой — ищем через поиск (чтобы получить полное имя)
    # Если не найдём — fallback на прямой переход
    if broker_id.isdigit():
        # Сначала пробуем через поиск
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
        if search:
            await search.click(click_count=3)
            await page.keyboard.press("Backspace")
            await page.wait_for_timeout(300)
            await search.fill("")
            await page.wait_for_timeout(200)
            await search.type(broker_id, delay=60)
            await page.wait_for_timeout(1500)
            # Ищем строку с этим ID
            rows = await page.evaluate(r"""(bid) => {
                const results = [];
                document.querySelectorAll("table tr").forEach(row => {
                    const link = row.querySelector("a[href*='/clients/'][href*='/settings']") ||
                                 row.querySelector("a.btn-primary");
                    if (!link) return;
                    const href = link.getAttribute("href");
                    if (!href.includes('/clients/' + bid + '/')) return;
                    const tds = row.querySelectorAll("td");
                    let name = "";
                    tds.forEach(td => {
                        const t = td.innerText.trim();
                        if (t && !/^\d+$/.test(t) && t.length > 4 && !["active","inactive","disabled"].includes(t.toLowerCase())) {
                            if (!name) name = t;
                        }
                    });
                    if (link && name) {
                        results.push({ name: name, href: href });
                    }
                });
                return results;
            }""", broker_id)
            if rows:
                _last_broker_full_name = rows[0]["name"]
                log.info(f"Found broker by ID search: {rows[0]['name']}")
                return rows[0]["href"].replace("/settings", "")

        # Fallback — прямой переход
        base = f"/clients/{broker_id}"
        test_url = f"{CRM_URL.rstrip('/')}{base}/settings"
        await page.goto(test_url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(800)
        current = page.url
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
            let status = "";
            tds.forEach(td => {
                const t = td.innerText.trim();
                if (["active","inactive","disabled"].includes(t.toLowerCase())) {
                    status = t.toLowerCase();
                    return;
                }
                // Пропускаем числа (ID)
                if (t && !/^\d+$/.test(t) && t.length > 4) {
                    if (!name) name = t;
                }
            });
            if (!name && tds.length > 2) name = tds[2].innerText.trim();
            if (link && name) {
                results.push({
                    name: name,
                    href: link.getAttribute("href"),
                    status: status || "unknown"
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
    # Сначала ищем точное вхождение подстроки
    relevant = [r for r in rows if query_lower in r["name"].lower()]
    if not relevant:
        # Fallback: все слова запроса должны присутствовать в имени брокера
        query_words = query_lower.split()
        relevant = [r for r in rows if all(w in r["name"].lower() for w in query_words)]
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
        # Иначе предпочитаем CPA (но только если есть active CPA)
        cpa = [r for r in partial if "cpa" in r["name"].lower()]
        active_cpa = [r for r in cpa if r.get("status") == "active"]
        if active_cpa:
            best = min(active_cpa, key=lambda r: len(r["name"]))
            log.info(f"Preferred CPA (active): {best['name']}")
            _last_broker_full_name = best["name"]
            return best["href"].replace("/settings", "")
        # Нет active CPA — берём кратчайшее active
        active_partial = [r for r in partial if r.get("status") == "active"]
        if active_partial:
            best = min(active_partial, key=lambda r: len(r["name"]))
        else:
            best = min(partial, key=lambda r: len(r["name"]))
        log.info(f"Partial match (shortest): {best['name']}")
        _last_broker_full_name = best["name"]
        return best["href"].replace("/settings", "")

    # 4. Первый результат как запасной
    log.info(f"Taking first result: {rows[0]['name']}")
    _last_broker_full_name = rows[0]["name"]
    return rows[0]["href"].replace("/settings", "")


def _cache_broker_path(broker_id: str, path, name: str = None):
    """Сохранить base_path в кэш."""
    import time
    if path:
        _broker_path_cache[broker_id.strip().lower()] = (path, time.time(), name or broker_id)


# ══════════════════════════════════════════
#  ДЕЙСТВИЯ В CRM
# ══════════════════════════════════════════

async def _scrape_countries_from_page(page) -> list:
    """Собрать список стран с текущей открытой страницы Opening Hours."""
    try:
        await page.wait_for_selector("button.btn-primary.btn-sm, button.btn-outline-primary.btn-sm, button.btn.btn-sm.btn-primary, button.btn.btn-sm.btn-outline-primary", timeout=8000)
    except Exception:
        pass
    await page.wait_for_timeout(400)
    countries = await page.evaluate("""() => {
        const days = new Set(['monday','tuesday','wednesday','thursday','friday','saturday','sunday']);
        const result = [];
        document.querySelectorAll('table tr, .table tr').forEach(row => {
            const hasBtn = row.querySelector('button.btn-primary.btn-sm, button.btn-outline-primary.btn-sm, button.btn.btn-sm.btn-primary, button.btn.btn-sm.btn-outline-primary');
            if (!hasBtn) return;
            const td = row.querySelector('td');
            if (!td) return;
            const name = td.innerText.trim();
            if (name && !days.has(name.toLowerCase())) result.push(name);
        });
        return [...new Set(result)];
    }""")
    return countries


async def _read_current_hours_for_country(page, country: str) -> dict:
    """Прочитать текущие часы работы для страны с открытой страницы Opening Hours.
    Возвращает {'start': '09:00', 'end': '20:00'} или {} если не найдено."""
    try:
        result = await page.evaluate("""(countryName) => {
            const rows = document.querySelectorAll('table tr, .table tr');
            for (const row of rows) {
                const td = row.querySelector('td');
                if (!td) continue;
                if (!td.innerText.trim().toLowerCase().includes(countryName.toLowerCase())) continue;
                // Ищем первую строку с временем (не closed)
                const text = row.innerText;
                const timeMatch = text.match(/(\\d{1,2}:\\d{2})\\s*[-–]\\s*(\\d{1,2}:\\d{2})/);
                if (timeMatch) {
                    return {start: timeMatch[1], end: timeMatch[2]};
                }
            }
            return {};
        }""", country)
        if result:
            log.info(f"Current hours for {country}: {result.get('start', '?')}-{result.get('end', '?')}")
        return result or {}
    except Exception:
        return {}


async def action_change_hours(broker_id: str, start: str, end: str,
                               countries_filter: list, no_traffic: bool,
                               days_filter: list = None, base_path: str = None) -> str:
    """Изменить часы работы брокера."""
    page = await get_page()

    if not base_path:
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
            "button.btn-primary.btn-sm, button.btn-outline-primary.btn-sm:not(.btn_big), button.btn.btn-sm.btn-primary, button.btn.btn-sm.btn-outline-primary:not(.float-right)"
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

    # Если конкретные страны запрошены — проверяем есть ли они
    if "all" not in countries_filter:
        existing_names = [c.lower() for _, c in pencils_with_names]
        missing_countries = []
        for cf in countries_filter:
            found = any(cf.lower() in en for en in existing_names)
            if not found:
                missing_countries.append(cf)

        # Если запрошенная страна не найдена — добавляем через add_country_hours
        if missing_countries and not pencils_with_names:
            # Страница пустая — добавляем все запрошенные страны
            log.info(f"No countries found, adding: {missing_countries}")
            results = []
            for mc in missing_countries:
                sub_msg = await action_add_country_hours(
                    broker_id=broker_id, country=mc, start=start, end=end,
                    no_traffic=no_traffic, days_filter=days_filter,
                    base_path=base_path
                )
                results.append(sub_msg)
            return "\n".join(results)
        elif missing_countries:
            # Часть стран не найдена — добавляем их
            log.info(f"Missing countries, will add: {missing_countries}")
            add_results = []
            for mc in missing_countries:
                sub_msg = await action_add_country_hours(
                    broker_id=broker_id, country=mc, start=start, end=end,
                    no_traffic=no_traffic, days_filter=days_filter,
                    base_path=base_path
                )
                add_results.append(sub_msg)

    if not pencils_with_names:
        log.info(f"Page URL: {page.url}")
        return "❌ No countries found for this broker."

    # Собираем имена стран для обработки (фильтруем заранее)
    add_results = []  # результаты добавления новых стран
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
            results.append(f"⚠️ {country_name}: pencil not found after DOM update")
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

    all_results = add_results + results
    return "\n".join(all_results) if all_results else "⚠️ No rows to change."


async def action_edit_country_add_days(broker_id: str, country: str, start: str, end: str,
                                        no_traffic: bool, days_to_add: list, base_path: str = None) -> str:
    """
    Редактировать существующую запись страны: добавить галочки на нужные дни
    и выставить часы — не трогая already activeные дни.
    """
    page = await get_page()

    if not base_path:
        base_path = await find_and_open_broker(page, broker_id)
    if not base_path:
        return f"❌ Broker '{broker_id}' not found. Nothing changed."

    oh_url = f"{CRM_URL.rstrip('/')}{base_path}/opening_hours"
    await page.goto(oh_url, wait_until="domcontentloaded", timeout=60000)
    try:
        await page.wait_for_selector("button.btn-primary.btn-sm, button.btn-outline-primary.btn-sm, button.btn.btn-sm.btn-primary, button.btn.btn-sm.btn-outline-primary", timeout=12000)
    except Exception:
        pass
    await page.wait_for_timeout(500)

    # Ищем карандаш нужной страны
    edit_buttons = await page.query_selector_all("button.btn-primary.btn-sm, button.btn-outline-primary.btn-sm, button.btn.btn-sm.btn-primary, button.btn.btn-sm.btn-outline-primary")
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
    start_val = f"{sh.zfill(2)}:{sm.zfill(2)}"
    end_val = ""
    if end:
        eh, em = (end.split(":") + ["00"])[:2]
        end_val = f"{eh.zfill(2)}:{em.zfill(2)}"

    # Проверяем: ночные часы?
    is_overnight = False
    if end_val:
        try:
            s_h = int(start_val.split(":")[0])
            e_h = int(end_val.split(":")[0])
            if e_h < s_h:
                is_overnight = True
                log.info(f"Overnight hours: {start_val}-{end_val} → 00:00-{end_val} + {start_val}-24:00")
        except Exception:
            pass

    # Проходим по строкам модалки — каждая строка = один день
    # Включаем только нужные дни и только им меняем время
    checkboxes = await modal.query_selector_all("input[type='checkbox']")
    enabled = []
    actual_end = end_val  # будет обновлён если end пустой

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

        if is_overnight:
            # Ночные часы: ставим 00:00-end в первый слот, добавляем второй слот start-24:00
            # Первый слот: 00:00 - end
            await cb.evaluate(f"""el => {{
                const row = el.closest('tr, .row, li, [class*="day"]');
                if (!row) return;
                const inputs = row.querySelectorAll('input.timepicker-input, input[class*="timepicker"]');
                if (inputs[0]) {{
                    inputs[0].value = '00:00';
                    inputs[0].dispatchEvent(new Event('input', {{bubbles:true}}));
                    inputs[0].dispatchEvent(new Event('change', {{bubbles:true}}));
                }}
                if (inputs[1]) {{
                    inputs[1].value = '{end_val}';
                    inputs[1].dispatchEvent(new Event('input', {{bubbles:true}}));
                    inputs[1].dispatchEvent(new Event('change', {{bubbles:true}}));
                }}
            }}""")
            await page.wait_for_timeout(100)

            # Нажимаем "+" для этого дня
            plus_clicked = await cb.evaluate("""el => {
                const row = el.closest('tr, .row, li, [class*="day"]');
                if (!row) return false;
                const plus = row.querySelector('.add-step svg, .add-step, svg.fa-plus-circle, svg[data-icon="plus-circle"]');
                if (plus) { plus.click(); return true; }
                // Fallback: span с title "Add a new time step"
                const span = row.querySelector('[title*="Add a new time step"]');
                if (span) { span.click(); return true; }
                return false;
            }""")
            log.info(f"  Day {matched_day}: clicked '+' for second slot: {plus_clicked}")
            await page.wait_for_timeout(600)

            # Второй слот: start - 24:00
            await cb.evaluate(f"""el => {{
                const row = el.closest('tr, .row, li, [class*="day"]');
                if (!row) return;
                const inputs = row.querySelectorAll('input.timepicker-input, input[class*="timepicker"]');
                // Второй слот = inputs[2] и inputs[3]
                if (inputs[2]) {{
                    inputs[2].value = '{start_val}';
                    inputs[2].dispatchEvent(new Event('input', {{bubbles:true}}));
                    inputs[2].dispatchEvent(new Event('change', {{bubbles:true}}));
                }}
                if (inputs[3]) {{
                    inputs[3].value = '24:00';
                    inputs[3].dispatchEvent(new Event('input', {{bubbles:true}}));
                    inputs[3].dispatchEvent(new Event('change', {{bubbles:true}}));
                }}
            }}""")
            await page.wait_for_timeout(80)
            actual_end = end_val

        elif end_val:
            # Обычные часы — start и end
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
        else:
            # Только start — end оставляем как есть
            actual_end = await cb.evaluate("""el => {
                const row = el.closest('tr, .row, li, [class*="day"]');
                if (!row) return '';
                const inputs = row.querySelectorAll('input.timepicker-input, input[class*="timepicker"]');
                return inputs[1] ? inputs[1].value : '';
            }""")
            log.info(f"  Read existing end from modal: {actual_end}")
            await cb.evaluate(f"""el => {{
                const row = el.closest('tr, .row, li, [class*="day"]');
                if (!row) return;
                const inputs = row.querySelectorAll('input.timepicker-input, input[class*="timepicker"]');
                if (inputs[0]) {{
                    inputs[0].value = '{start_val}';
                    inputs[0].dispatchEvent(new Event('input', {{bubbles:true}}));
                    inputs[0].dispatchEvent(new Event('change', {{bubbles:true}}));
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

    display_end = actual_end or end_val or "?"
    try:
        save_btn = await page.wait_for_selector("text=SAVE OPENING HOURS", timeout=3000)
        await save_btn.click()
        await page.wait_for_timeout(700)
        if is_overnight:
            return f"✅ {country}: {', '.join(enabled)} with hours {start_val}–{display_end}"
        return f"✅ {country}: days added: {', '.join(enabled)} with hours {start_val}–{display_end}"
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
            await page.wait_for_selector("button.btn-primary.btn-sm, button.btn-outline-primary.btn-sm, button.btn.btn-sm.btn-primary, button.btn.btn-sm.btn-outline-primary", timeout=10000)
        except Exception:
            pass
        await page.wait_for_timeout(400)
        btns = await page.query_selector_all("button.btn-primary.btn-sm, button.btn-outline-primary.btn-sm, button.btn.btn-sm.btn-primary, button.btn.btn-sm.btn-outline-primary")
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
                                    no_traffic: bool, days_filter: list = None, base_path: str = None) -> str:
    """Добавить часы работы для новой страны."""
    page = await get_page()

    if not base_path:
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

    # Проверяем: ночные часы (end < start, напр. 16:00-01:00)?
    is_overnight = False
    try:
        s_h = int(start_val.split(":")[0])
        e_h = int(end_val.split(":")[0])
        if e_h < s_h:
            is_overnight = True
    except Exception:
        pass

    if is_overnight:
        log.info(f"Overnight hours detected: {start_val}-{end_val} → split into 00:00-{end_val} + {start_val}-24:00")

        # Находим Monday строку
        monday_row = await page.evaluate("""() => {
            const days = document.querySelectorAll('.modal .day, .modal [class*="day_inner"], .modal [class*="day_wrapper"]');
            for (const d of days) {
                if (d.textContent.toLowerCase().includes('monday')) return true;
            }
            return false;
        }""")
        log.info(f"Monday row found: {monday_row}")

        # Ставим первый слот Monday: 00:00 - end
        if len(time_inputs) >= 2:
            await set_timepicker(time_inputs[0], "00:00")
            await set_timepicker(time_inputs[1], end_val)
        await page.wait_for_timeout(300)

        # Нажимаем "+" для Monday — через Playwright click (не JS)
        plus_btn = await page.query_selector('.modal [title*="Add a new time step for Monday"], .modal .add-step span[title*="Monday"]')
        if not plus_btn:
            # Fallback: первая кнопка add-step
            plus_btn = await page.query_selector('.modal .add-step span[title*="Add a new time step"]')
        if not plus_btn:
            plus_btn = await page.query_selector('.modal svg[data-icon="plus-circle"]')

        if plus_btn:
            await plus_btn.click()
            log.info("Clicked '+' via Playwright click")
        else:
            log.info("'+' button not found")
        await page.wait_for_timeout(1500)  # ждём анимацию Vue

        # Заново собираем ВСЕ timepicker'ы в модалке
        modal = await page.query_selector(".modal-body, [role='dialog']")
        time_inputs2 = []
        if modal:
            all_inputs = await modal.query_selector_all("input")
            for inp in all_inputs:
                inp_cls = await inp.get_attribute("class") or ""
                inp_type = await inp.get_attribute("type") or ""
                if "timepicker" in inp_cls and inp_type == "text":
                    time_inputs2.append(inp)
        log.info(f"Timepicker inputs after add-step: {len(time_inputs2)}")

        # Если новые инпуты появились (16 вместо 14) — второй слот на позициях 2,3
        if len(time_inputs2) > len(time_inputs):
            await set_timepicker(time_inputs2[2], start_val)
            await set_timepicker(time_inputs2[3], "24:00")
            log.info(f"Second slot set: {start_val}-24:00")
        else:
            # "+" не создал новых инпутов — пробуем через JS напрямую
            log.info("No new inputs appeared, trying JS approach")
            # Ищем инпуты внутри Monday day_wrapper
            set_ok = await page.evaluate(f"""() => {{
                const days = document.querySelectorAll('.modal .day, .modal [class*="day_wrapper"]');
                for (const d of days) {{
                    if (!d.textContent.toLowerCase().includes('monday')) continue;
                    const inputs = d.querySelectorAll('input.timepicker-input, input[class*="timepicker"]');
                    // Если 4+ инпутов — значит второй слот есть
                    if (inputs.length >= 4) {{
                        inputs[2].value = '{start_val}';
                        inputs[2].dispatchEvent(new Event('input', {{bubbles:true}}));
                        inputs[2].dispatchEvent(new Event('change', {{bubbles:true}}));
                        inputs[3].value = '24:00';
                        inputs[3].dispatchEvent(new Event('input', {{bubbles:true}}));
                        inputs[3].dispatchEvent(new Event('change', {{bubbles:true}}));
                        return 'set-' + inputs.length;
                    }}
                    return 'only-' + inputs.length;
                }}
                return 'no-monday';
            }}""")
            log.info(f"JS approach result: {set_ok}")

        # Нажимаем "copy to all"
        await page.wait_for_timeout(300)
        copied = await page.evaluate("""() => {
            const spans = document.querySelectorAll('.modal span.font-italic, .modal strong');
            for (const span of spans) {
                if (span.textContent.toLowerCase().includes('copy to all')) {
                    span.click();
                    return true;
                }
            }
            return false;
        }""")
        log.info(f"Clicked 'copy to all': {copied}")
        await page.wait_for_timeout(500)

    else:
        # Обычные часы — ставим для всех дней
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
            results.append(f"❌ {country}: price not found")

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
        await page.wait_for_selector("button.btn-primary.btn-sm, button.btn-outline-primary.btn-sm, button.btn.btn-sm.btn-primary, button.btn.btn-sm.btn-outline-primary", timeout=10000)
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
            results.append(f"❌ {country}: price not found")

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
    old_amount = None
    if country.lower() != "all":
        try:
            await page.wait_for_selector("table tr td", timeout=8000)
        except Exception:
            pass
        await page.wait_for_timeout(1500)
        rows = await page.query_selector_all("table tr")
        log.info(f"Affiliate payout table rows: {len(rows)}")
        for row in rows:
            country_td = await row.query_selector("td:nth-child(3)")
            if not country_td:
                continue
            td_text = (await country_td.inner_text()).strip()
            if country.lower() in td_text.lower():
                existing_pencil = await row.query_selector("button.btn-outline-primary, button.btn-primary.btn-sm")
                if existing_pencil:
                    # Читаем текущую сумму — ищем td с числовым значением (не "Fixed amount")
                    old_amount = "?"
                    all_tds = await row.query_selector_all("td")
                    for td in all_tds:
                        td_text = (await td.inner_text()).strip().replace("$", "").strip()
                        # Ищем td где содержимое — просто число
                        if td_text.replace(".", "").replace(",", "").isdigit():
                            old_amount = td_text
                            break
                    log.info(f"Entry for {country} already exists (${old_amount}) — editing")
                    break
                else:
                    log.info(f"Found {country} row but no pencil button")

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


async def action_add_affiliate_revenue_grouped(affiliate_id: str, countries: list, amount: str) -> str:
    """Добавить прайс для НЕСКОЛЬКИХ стран аффилиата за один проход модалки (мультиселект)."""
    page = await get_page()

    affiliate_id = str(affiliate_id).strip()
    if affiliate_id.isdigit():
        aff_base = f"/sources/{affiliate_id}"
        test_url = f"{CRM_URL.rstrip('/')}{aff_base}/settings"
        await page.goto(test_url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(800)
        if "login" in page.url.lower() or "/sources?" in page.url:
            return f"❌ Affiliate '{affiliate_id}' not found."
    else:
        return f"❌ Please provide numeric affiliate ID."

    payouts_url = f"{CRM_URL.rstrip('/')}{aff_base}/payouts"
    await page.goto(payouts_url, wait_until="domcontentloaded", timeout=60000)
    await page.wait_for_timeout(1500)

    # Разделяем страны на новые (нужно ADD) и существующие (нужно EDIT отдельно)
    try:
        await page.wait_for_selector("table tr td", timeout=8000)
    except Exception:
        pass
    await page.wait_for_timeout(1000)

    existing_rows = await page.evaluate("""() => {
        const result = [];
        document.querySelectorAll("table tr").forEach(row => {
            const td = row.querySelector("td:nth-child(3)");
            if (td) result.push(td.innerText.trim().toLowerCase());
        });
        return result;
    }""")

    new_countries = []
    existing_countries_to_edit = []
    for c in countries:
        if any(c.lower() in row for row in existing_rows):
            existing_countries_to_edit.append(c)
        else:
            new_countries.append(c)

    results = []

    # Существующие — редактируем по одной (старый способ)
    for c in existing_countries_to_edit:
        sub_msg = await action_add_affiliate_revenue(affiliate_id, c, amount)
        results.append(sub_msg)

    # Новые — открываем модалку ОДИН раз и выбираем все страны
    if new_countries:
        try:
            add_btn = await page.wait_for_selector(
                "button:has-text('ADD PAYOUT'), a:has-text('ADD PAYOUT')", timeout=12000
            )
            await add_btn.click()
            await page.wait_for_timeout(800)
        except Exception:
            for c in new_countries:
                results.append(f"❌ {c}: ADD PAYOUT button not found.")
            return "\n".join(results)

        try:
            modal = await page.wait_for_selector(".modal-body, [role='dialog']", timeout=5000)
        except Exception:
            for c in new_countries:
                results.append(f"❌ {c}: Modal did not open.")
            return "\n".join(results)
        await page.wait_for_timeout(500)

        # Открываем дропдаун стран
        dropdown_toggle = await modal.query_selector(
            ".smart__dropdown, [class*='smart__dropdown'], .smart__dropdown__input__element"
        )
        if dropdown_toggle:
            await dropdown_toggle.click()
            await page.wait_for_timeout(800)

        selected = []
        for country in new_countries:
            items_count = await page.evaluate("() => document.querySelectorAll('li.dropdown-item, .dropdown-item').length")
            if items_count == 0:
                log.info(f"Reopening dropdown before {country}")
                dropdown_toggle = await modal.query_selector(
                    ".smart__dropdown, [class*='smart__dropdown'], .smart__dropdown__input__element"
                )
                if dropdown_toggle:
                    await dropdown_toggle.click()
                    await page.wait_for_timeout(800)

            search_input = None
            try:
                search_input = await page.wait_for_selector(
                    "input[id*='search-input'], input[id*='search']",
                    timeout=3000
                )
            except Exception:
                search_input = await page.query_selector(".bg-white input[type='text']")

            if not search_input:
                log.warning(f"Search input not found for {country}")
                continue

            val_before = await search_input.input_value()

            await search_input.evaluate("""el => {
                el.value = '';
                el.dispatchEvent(new Event('input', {bubbles:true}));
                el.dispatchEvent(new Event('change', {bubbles:true}));
            }""")
            await page.wait_for_timeout(400)

            # Переполучаем — Vue мог перерисовать
            search_input = await page.query_selector("input[id*='search-input']")
            if not search_input:
                search_input = await page.query_selector("input[id*='search'], .bg-white input[type='text']")
            if not search_input:
                log.warning(f"Search input disappeared after clear for {country}")
                continue

            await search_input.click()
            await page.wait_for_timeout(100)
            await search_input.type(country, delay=60)
            await page.wait_for_timeout(400)

            for _ in range(15):
                await page.wait_for_timeout(200)
                cnt = await page.evaluate("() => document.querySelectorAll('li.dropdown-item').length")
                if cnt < 15:
                    break
            log.info(f"After fill '{country}': items = {cnt}")

            items = await page.query_selector_all("li.dropdown-item, .dropdown-item")
            clicked = False
            for item in items:
                txt = (await item.inner_text()).strip()
                if country.lower() in txt.lower():
                    await item.click()
                    clicked = True
                    log.info(f"Clicked: '{txt}'")
                    break

            if clicked:
                selected.append(country)
                log.info(f"Selected country: {country}")
                await page.wait_for_timeout(500)
            else:
                log.warning(f"Country not found: {country}. Items: {[await i.inner_text() for i in items]}")

        if not selected:
            await _close_modal(page)
            for c in new_countries:
                results.append(f"❌ {c}: not found in dropdown.")
            return "\n".join(results)

        # Закрываем дропдаун — кликаем на modal вне дропдауна
        await page.keyboard.press("Escape")
        await page.wait_for_timeout(400)

        modal = await page.query_selector(".modal-body, [role='dialog']")
        if not modal:
            for c in new_countries:
                results.append(f"❌ {c}: modal closed unexpectedly.")
            return "\n".join(results)

        # Вводим сумму
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
        else:
            await _close_modal(page)
            for c in new_countries:
                results.append(f"❌ {c}: Amount field not found.")
            return "\n".join(results)

        # Сохраняем
        await page.wait_for_timeout(400)
        try:
            save_btn = await page.wait_for_selector(
                ".modal button[type='submit'], .modal-footer button[type='submit'], .modal .btn-ladda",
                timeout=5000
            )
            await save_btn.click()
            await page.wait_for_timeout(1200)
            for c in selected:
                results.append(f"✅ Price added for {c}: ${amount}")
            not_selected = [c for c in new_countries if c not in selected]
            for c in not_selected:
                results.append(f"❌ {c}: not found in dropdown.")
        except Exception:
            await _close_modal(page)
            for c in new_countries:
                results.append(f"⚠️ {c}: Save button not found.")

    return "\n".join(results)


async def action_add_revenue_grouped(broker_id: str, countries: list, amount: str, affiliate_id: str = None, base_path: str = None) -> str:
    """Добавить/обновить прайс для НЕСКОЛЬКИХ стран брокера за один проход модалки (мультиселект)."""
    page = await get_page()

    if not base_path:
        base_path = await find_and_open_broker(page, broker_id, country_hint=countries[0] if countries else None)
    if not base_path:
        return f"❌ Broker '{broker_id}' not found."

    rev_url = f"{CRM_URL.rstrip('/')}{base_path}/revenues"
    await page.goto(rev_url, wait_until="domcontentloaded", timeout=60000)
    await page.wait_for_timeout(1000)

    # Разделяем на новые и существующие
    try:
        await page.wait_for_selector("table tr td", timeout=5000)
    except Exception:
        pass
    await page.wait_for_timeout(1500)

    existing_rows = await page.evaluate("""() => {
        const result = [];
        document.querySelectorAll("table tr td:first-child").forEach(td => {
            result.push(td.innerText.trim().toLowerCase());
        });
        return result;
    }""")

    new_countries = []
    existing_to_edit = []
    for c in countries:
        if any(c.lower() in row for row in existing_rows):
            existing_to_edit.append(c)
        else:
            new_countries.append(c)

    results = []

    # Существующие — редактируем по одной
    for c in existing_to_edit:
        sub_msg = await action_add_revenue(broker_id, c, amount, affiliate_id=affiliate_id, base_path=base_path)
        results.append(sub_msg)

    # Новые — один проход ADD REVENUE с мультиселектом
    if new_countries:
        try:
            add_btn = await page.wait_for_selector(
                "button:has-text('ADD REVENUE'), button:has-text('ADD THE FIRST REVENUE')", timeout=10000
            )
            await add_btn.click()
            await page.wait_for_timeout(800)
        except Exception:
            for c in new_countries:
                results.append(f"❌ {c}: ADD REVENUE button not found.")
            return "\n".join(results)

        try:
            modal = await page.wait_for_selector(".modal-body, [role='dialog']", timeout=5000)
        except Exception:
            for c in new_countries:
                results.append(f"❌ {c}: Modal did not open.")
            return "\n".join(results)
        await page.wait_for_timeout(500)

        # Открываем дропдаун стран
        dropdown_toggle = await modal.query_selector(".smart__dropdown, [class*='smart__dropdown']")
        if dropdown_toggle:
            await dropdown_toggle.click()
            await page.wait_for_timeout(800)

        # Выбираем страны по очереди
        selected = []
        for country in new_countries:
            # Проверяем открыт ли дропдаун
            items_count = await page.evaluate("() => document.querySelectorAll('li.dropdown-item').length")
            if items_count == 0:
                log.info(f"Reopening dropdown before {country}")
                dropdown_toggle = await modal.query_selector(".smart__dropdown, [class*='smart__dropdown']")
                if dropdown_toggle:
                    await dropdown_toggle.click()
                    await page.wait_for_timeout(800)

            # Ждём появления search input
            search_input = None
            try:
                search_input = await page.wait_for_selector(
                    "input[id*='search-input'], input[id*='search']",
                    timeout=3000
                )
            except Exception:
                search_input = await page.query_selector(".bg-white input[type='text']")

            if not search_input:
                log.warning(f"Search input not found for {country}")
                continue

            val_before = await search_input.input_value()

            # Очищаем через JS
            await search_input.evaluate("""el => {
                el.value = '';
                el.dispatchEvent(new Event('input', {bubbles:true}));
                el.dispatchEvent(new Event('change', {bubbles:true}));
            }""")
            await page.wait_for_timeout(400)

            # Переполучаем search_input — Vue мог перерисовать элемент
            search_input = await page.query_selector("input[id*='search-input']")
            if not search_input:
                search_input = await page.query_selector("input[id*='search'], .bg-white input[type='text']")
            if not search_input:
                log.warning(f"Search input disappeared after clear for {country}")
                continue

            # Вводим страну через type() — надёжно триггерит Vue
            await search_input.click()
            await page.wait_for_timeout(100)
            await search_input.type(country, delay=60)
            await page.wait_for_timeout(400)

            # Ждём фильтрации
            for _ in range(15):
                await page.wait_for_timeout(200)
                cnt = await page.evaluate("() => document.querySelectorAll('li.dropdown-item').length")
                if cnt < 15:
                    break

            items = await page.query_selector_all("li.dropdown-item")
            clicked = False
            for item in items:
                txt = (await item.inner_text()).strip()
                if country.lower() in txt.lower():
                    await item.click()
                    clicked = True
                    log.info(f"Clicked: '{txt}'")
                    break
            if not clicked:
                log.warning(f"Country not found. Items: {[await i.inner_text() for i in items]}")
            else:
                selected.append(country)
                log.info(f"Selected country for grouped revenue: {country}")
                await page.wait_for_timeout(500)

        if not selected:
            await _close_modal(page)
            for c in new_countries:
                results.append(f"❌ {c}: not found in dropdown.")
            return "\n".join(results)

        # Закрываем дропдаун
        await page.keyboard.press("Escape")
        await page.wait_for_timeout(400)

        modal = await page.query_selector(".modal-body, [role='dialog']")
        if not modal:
            for c in new_countries:
                results.append(f"❌ {c}: modal closed unexpectedly.")
            return "\n".join(results)

        # Вводим сумму
        await page.keyboard.press("Escape")
        await page.wait_for_timeout(300)
        amount_input = await modal.query_selector("input[type='number'], input[placeholder*='amount' i], input[placeholder*='Amount' i]")
        if not amount_input:
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
        else:
            await _close_modal(page)
            for c in new_countries:
                results.append(f"❌ {c}: Amount field not found.")
            return "\n".join(results)

        # Добавляем affiliate_id если нужно
        if affiliate_id:
            modal = await page.query_selector(".modal-body, [role='dialog']")
            if modal:
                await _add_affiliate_parameter(page, modal, str(affiliate_id), close_dropdown=False)
                await page.evaluate("""() => {
                    const closeBtns = document.querySelectorAll('.modal .close, [role=dialog] .close, button[aria-label="Close"]');
                    if (closeBtns.length > 1) closeBtns[closeBtns.length - 1].click();
                }""")
                await page.wait_for_timeout(400)

        # Сохраняем
        await page.wait_for_timeout(500)
        try:
            saved = await page.evaluate("""() => {
                const modals = document.querySelectorAll('.modal.show, [role=dialog]');
                for (const modal of modals) {
                    const footer = modal.querySelector('.modal-footer');
                    if (footer) {
                        const btn = footer.querySelector('button[type=submit], .btn-ladda, .btn-success');
                        if (btn) { btn.click(); return true; }
                    }
                }
                return false;
            }""")
            await page.wait_for_timeout(1200)
            aff_label = f" (aff {affiliate_id})" if affiliate_id else ""
            if saved:
                for c in selected:
                    results.append(f"✅ Price added for {c}: ${amount}{aff_label}")
            else:
                raise Exception("Save button not found")
            not_selected = [c for c in new_countries if c not in selected]
            for c in not_selected:
                results.append(f"❌ {c}: not found in dropdown.")
        except Exception:
            await _close_modal(page)
            for c in new_countries:
                results.append(f"⚠️ {c}: Save button not found.")

    return "\n".join(results)


async def action_add_revenue(broker_id: str, country: str, amount: str, affiliate_id: str = None, base_path: str = None) -> str:
    """Добавить или обновить прайс (revenue) для страны брокера."""
    page = await get_page()

    if not base_path:
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
            await page.wait_for_selector("table tr td", timeout=8000)
        except Exception:
            pass
        await page.wait_for_timeout(1500)
        rows = await page.query_selector_all("table tr")
        log.info(f"Revenue table rows found: {len(rows)}")
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
                else:
                    log.info(f"Found {country} row (${old_amount}) but no pencil button")

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
            log.info("Country search field not found!")

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
    affiliate_param_failed = False
    if affiliate_id:
        modal = await page.query_selector(".modal-body, [role='dialog']")
        param_ok = await _add_affiliate_parameter(page, modal, str(affiliate_id), close_dropdown=False)
        if not param_ok:
            log.warning(f"Could not add affiliate parameter {affiliate_id} — saving without it")
            affiliate_param_failed = True
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
            result = f"✅ Price added for {country_label}: ${amount}{aff_label}"
            if affiliate_param_failed:
                result += f"\n⚠️ Affiliate parameter not added (try again or add manually)"
            return result
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
    closed = []        # дни которые удалось закрыть (сняли галочку)
    already_closed = []  # дни которые уже были закрыты
    not_found = []     # дни которых не нашли в модалке вообще

    found_days = set()
    for cb in checkboxes:
        label_text = await cb.evaluate("el => el.closest('label,tr,div')?.textContent?.toLowerCase() || ''")
        if "no traffic" in label_text:
            continue
        for day in days_lower:
            if day in label_text:
                found_days.add(day)
                if await cb.is_checked():
                    await cb.evaluate("el => el.click()")
                    await page.wait_for_timeout(100)
                    closed.append(day.capitalize())
                else:
                    already_closed.append(day.capitalize())
                break

    for day in days_lower:
        if day not in found_days:
            not_found.append(day.capitalize())

    if not closed:
        # Ничего не меняли — закрываем модалку без сохранения
        await _close_modal(page)
        parts = []
        if already_closed:
            # Если все запрошенные дни закрыты — компактно
            if len(already_closed) == len(days_to_close):
                parts.append("already closed")
            else:
                parts.append(f"already closed: {', '.join(already_closed)}")
        if not_found:
            parts.append(f"not found: {', '.join(not_found)}")
        return f"⚠️ {country_name}: {' | '.join(parts)}."

    # Есть что сохранять
    try:
        save_btn = await page.wait_for_selector("text=SAVE OPENING HOURS", timeout=3000)
        await save_btn.click()
        await page.wait_for_timeout(700)
        # Формируем итоговое сообщение
        parts = []
        if len(closed) == len(days_to_close):
            parts.append("all days closed")
        else:
            parts.append(f"closed: {', '.join(closed)}")
        if already_closed:
            parts.append(f"already closed: {', '.join(already_closed)}")
        if not_found:
            parts.append(f"not found: {', '.join(not_found)}")
        return f"✅ {country_name}: {' | '.join(parts)}."
    except Exception:
        await _close_modal(page)
        return f"⚠️ {country_name}: Save button not found."


async def action_close_days(broker_id: str, country: str, days_to_close: list, country_hint: str = None, base_path: str = None) -> str:
    """Закрыть конкретные дни для страны (или всех стран) брокера."""
    page = await get_page()

    if not base_path:
        base_path = await find_and_open_broker(page, broker_id, country_hint=country_hint or country)
    if not base_path:
        return f"❌ Broker '{broker_id}' not found. Nothing changed."

    oh_url = f"{CRM_URL.rstrip('/')}{base_path}/opening_hours"
    await page.goto(oh_url, wait_until="domcontentloaded", timeout=60000)

    # Ждём пока Vue отрендерит карандаши
    try:
        await page.wait_for_selector(
            "button.btn-primary.btn-sm, button.btn-outline-primary.btn-sm, button.btn.btn-sm.btn-primary, button.btn.btn-sm.btn-outline-primary",
            timeout=12000
        )
    except Exception:
        pass
    await page.wait_for_timeout(500)

    log.info(f"Закрываю дни: {days_to_close} для страны: {country}")

    # Собираем все карандаши с именами стран
    edit_buttons = await page.query_selector_all("button.btn-primary.btn-sm, button.btn-outline-primary.btn-sm, button.btn.btn-sm.btn-primary, button.btn.btn-sm.btn-outline-primary")
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
            fresh_buttons = await page.query_selector_all("button.btn-primary.btn-sm, button.btn-outline-primary.btn-sm, button.btn.btn-sm.btn-primary, button.btn.btn-sm.btn-outline-primary")
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
                results.append(f"⚠️ {c_name}: pencil not found after DOM update.")
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


async def action_add_affiliate_mapping(broker_id: str, affiliate_id: str,
                                        override_code: str, country: str = None,
                                        base_path: str = None) -> str:
    """Добавить маппинг аффилиата для брокера (Override Affiliate ID's)."""
    page = await get_page()

    if not base_path:
        base_path = await find_and_open_broker(page, broker_id)
    if not base_path:
        return f"❌ Broker '{broker_id}' not found."

    # Переходим на страницу Override Affiliate ID's
    url = f"{CRM_URL.rstrip('/')}{base_path}/mapped-sources"
    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
    await page.wait_for_timeout(1500)
    log.info(f"Opened mapped-sources: {url}")

    # Ждём загрузки таблицы перед чтением маппингов
    try:
        await page.wait_for_selector("table tr td", timeout=8000)
    except Exception:
        pass
    await page.wait_for_timeout(500)

    # Читаем существующие маппинги из таблицы
    existing_mappings = await page.evaluate("""() => {
        const rows = document.querySelectorAll('table tr');
        const result = [];
        rows.forEach(row => {
            const tds = row.querySelectorAll('td');
            if (tds.length < 4) return;
            const affCell = tds[0]?.innerText?.trim() || '';
            const countryCell = tds[1]?.innerText?.trim() || '';
            const overrideCell = tds[3]?.innerText?.trim() || '';
            if (affCell && overrideCell) {
                result.push({aff: affCell, country: countryCell, override: overrideCell});
            }
        });
        return result;
    }""")

    # Проверяем нет ли нотификации об ошибке сразу на странице
    page_error = await page.evaluate("""() => {
        const els = document.querySelectorAll('.noty_body, [class*="noty_body"]');
        for (const el of els) {
            const txt = el.innerText?.trim() || '';
            if (txt.toLowerCase().includes('not being sent') || txt.toLowerCase().includes('mapped source')) {
                return txt;
            }
        }
        return null;
    }""")
    if page_error:
        return f"⚠️ Aff ID is not being sent to {_last_broker_full_name or broker_id}"

    # Ждём загрузки страницы — у брокеров с большим количеством записей это дольше
    try:
        await page.wait_for_selector(
            "button, .empty-state, table",
            timeout=10000
        )
    except Exception:
        pass
    await page.wait_for_timeout(500)

    # Нажимаем кнопку ADD через JS — надёжнее чем wait_for_selector
    try:
        # Ждём появления кнопки (страницы с большими таблицами грузятся дольше)
        clicked_add = None
        for attempt in range(15):  # до 15 секунд
            await page.wait_for_timeout(1000)
            clicked_add = await page.evaluate("""() => {
                const btns = document.querySelectorAll('button');
                for (const btn of btns) {
                    const txt = btn.innerText?.toUpperCase() || '';
                    if (txt.includes('ADD') && txt.includes('AFFILIATE') && txt.includes('OVERRIDE')) {
                        btn.click();
                        return btn.innerText.trim();
                    }
                }
                // Fallback по классу
                const byClass = document.querySelector('button.btn_big.btn-primary, button.btn-primary.btn_big');
                if (byClass) { byClass.click(); return byClass.innerText.trim(); }
                return null;
            }""")
            if clicked_add:
                break
        if not clicked_add:
            return f"⚠️ Aff ID is not being sent to {_last_broker_full_name or broker_id}"
        log.info(f"Clicked ADD button: {clicked_add[:50]}")
        await page.wait_for_timeout(800)
    except Exception:
        return f"⚠️ Aff ID is not being sent to {_last_broker_full_name or broker_id}"

    # Ждём модалку
    try:
        modal = await page.wait_for_selector(".modal-body, [role='dialog']", timeout=5000)
    except Exception:
        return "❌ Modal did not open."
    await page.wait_for_timeout(500)

    # ── 1. Выбираем аффилиата ──────────────────
    try:
        # Кликаем по smart__dropdown__input__element первого поля (Affiliate)
        aff_dropdown = await modal.query_selector(
            ".smart__dropdown__input__element, .smart__dropdown, [class*='smart__dropdown']"
        )
        if aff_dropdown:
            await aff_dropdown.click()
            await page.wait_for_timeout(600)

        # Ищем поле поиска аффилиата
        search_input = await page.wait_for_selector(
            "input[id*='search-input'], input[id*='search']",
            timeout=4000
        )
        await search_input.type(str(affiliate_id), delay=80)
        await page.wait_for_timeout(800)

        # Ждём фильтрации
        for _ in range(10):
            await page.wait_for_timeout(200)
            cnt = await page.evaluate("() => document.querySelectorAll('li.dropdown-item, li.flex-fill').length")
            if 0 < cnt < 20:
                break

        # Кликаем на нужного аффилиата — ищем по ID в скобках или в тексте
        items = await page.query_selector_all("li.dropdown-item, li.flex-fill")
        clicked_aff = False
        for item in items:
            txt = (await item.inner_text()).strip()
            if f"({affiliate_id})" in txt or txt.startswith(f"*{affiliate_id}") or txt.startswith(affiliate_id):
                await item.click()
                clicked_aff = True
                log.info(f"Selected affiliate: {txt}")
                await page.wait_for_timeout(400)
                break

        if not clicked_aff:
            # Fallback — первый результат если только один
            if len(items) == 1:
                txt = (await items[0].inner_text()).strip()
                await items[0].click()
                clicked_aff = True
                log.info(f"Selected affiliate (only result): {txt}")
                await page.wait_for_timeout(400)

        if not clicked_aff:
            await _close_modal(page)
            return f"❌ Affiliate '{affiliate_id}' not found in list."
    except Exception as e:
        await _close_modal(page)
        return f"❌ Error selecting affiliate: {e}"

    await page.wait_for_timeout(400)

    # ── 2. Выбираем страну (если указана) ─────
    if country and country.lower() != "all":
        try:
            await page.wait_for_timeout(500)

            # Открываем Country дропдаун по лейблу через JS
            await page.evaluate("""() => {
                const labels = document.querySelectorAll('.modal label, [role=dialog] label');
                for (const lbl of labels) {
                    if (lbl.innerText.trim().toLowerCase() === 'country') {
                        const row = lbl.closest('.form-group, .row, fieldset') || lbl.parentElement;
                        const inner = row?.querySelector('.smart__dropdown__input__element') ||
                                      row?.querySelector('[class*="cursor-pointer"]') ||
                                      row?.querySelector('[class*="smart__dropdown"]');
                        if (inner) { inner.click(); return true; }
                    }
                }
                return false;
            }""")
            await page.wait_for_timeout(800)

            # Вводим страну через JS напрямую в поле поиска
            typed = await page.evaluate(f"""(countryName) => {{
                const inputs = document.querySelectorAll('input[id*="search-input"], input[id*="search"]');
                for (const inp of inputs) {{
                    if (inp.offsetParent !== null) {{
                        inp.value = countryName;
                        inp.dispatchEvent(new Event('input', {{bubbles: true}}));
                        inp.dispatchEvent(new Event('change', {{bubbles: true}}));
                        return true;
                    }}
                }}
                return false;
            }}""", country)
            log.info(f"Country typed via JS: {typed}")
            await page.wait_for_timeout(800)

            # Кликаем через JS — не держим ссылки на элементы
            clicked = await page.evaluate(f"""(countryName) => {{
                const items = document.querySelectorAll('li.dropdown-item, li.flex-fill');
                // Сначала точное совпадение
                for (const item of items) {{
                    if (item.innerText.trim().toLowerCase() === countryName.toLowerCase()) {{
                        item.click();
                        return item.innerText.trim();
                    }}
                }}
                // Потом частичное
                for (const item of items) {{
                    if (item.innerText.trim().toLowerCase().includes(countryName.toLowerCase())) {{
                        item.click();
                        return item.innerText.trim();
                    }}
                }}
                return null;
            }}""", country)
            if clicked:
                log.info(f"Selected country: {clicked}")
            else:
                log.warning(f"Country '{country}' not found in dropdown")
            await page.wait_for_timeout(400)
        except Exception as e:
            log.warning(f"Could not select country '{country}': {e}")
            country = None  # помечаем что страна не выбрана

    await page.wait_for_timeout(300)

    # ── 3. Вводим Affiliate ID override code ──
    try:
        # Поле "Add code, press enter or click add"
        code_input = await modal.query_selector(
            "input[placeholder*='Add code'], input[placeholder*='add']"
        )
        if not code_input:
            # По классу b-form-tags-input
            code_input = await modal.query_selector("input.b-form-tags-input, input[class*='form-tags']")

        if not code_input:
            await _close_modal(page)
            return "❌ Affiliate ID overrides input not found."

        await code_input.click()
        await page.wait_for_timeout(200)
        await code_input.type(str(override_code), delay=60)
        await page.wait_for_timeout(400)

        # Нажимаем кнопку ADD рядом с полем
        add_code_btn = await modal.query_selector(
            "button.btn-outline-secondary, button:has-text('Add')"
        )
        if add_code_btn:
            await add_code_btn.click()
            log.info(f"Clicked ADD for override code: {override_code}")
        else:
            # Fallback — Enter
            await code_input.press("Enter")
            log.info(f"Pressed Enter for override code: {override_code}")
        await page.wait_for_timeout(400)
    except Exception as e:
        await _close_modal(page)
        return f"❌ Error entering override code: {e}"

    # ── 4. Сохраняем ──────────────────────────
    try:
        save_btn = await page.wait_for_selector(
            ".modal button[type='submit'], .modal-footer button[type='submit'], "
            ".modal .btn-ladda.btn-success",
            timeout=5000
        )
        await save_btn.click()
        await page.wait_for_timeout(1000)

        # Проверяем появился ли диалог "same override ID already exists" — нужно нажать CONFIRM
        confirm_btn = await page.query_selector("button:has-text('CONFIRM'), .btn-primary:has-text('CONFIRM')")
        if confirm_btn:
            await confirm_btn.click()
            log.info("Clicked CONFIRM for duplicate override ID dialog")
            await page.wait_for_timeout(1000)

        await page.wait_for_timeout(400)

        # Проверяем нет ли ошибки "record already exists"
        error_msg = await page.evaluate("""() => {
            const alerts = document.querySelectorAll('.noty_body, .alert, .toast, [class*="alert"], [class*="toast"], [class*="noty"]');
            for (const el of alerts) {
                const txt = el.innerText?.trim() || '';
                if (txt.toLowerCase().includes('already exist') || txt.toLowerCase().includes('already exists')) {
                    return 'already_exists:' + txt;
                }
                if (txt.toLowerCase().includes('not being sent') || txt.toLowerCase().includes('mapped source')) {
                    return 'not_sent:' + txt;
                }
            }
            return null;
        }""")
        if error_msg:
            log.info(f"CRM noty detected after SAVE: {error_msg[:80]}")
            await _close_modal(page)
            if error_msg.startswith('already_exists:'):
                # Ищем существующий маппинг для этого аффа/страны
                existing_override = None
                for m in existing_mappings:
                    aff_match = str(affiliate_id) in m["aff"]
                    country_match = not country or country.lower() in m["country"].lower()
                    if aff_match and country_match:
                        existing_override = m["override"]
                        break
                if existing_override:
                    return f"⚠️ aff {affiliate_id} / {country} already mapped as {existing_override}"
                return f"⚠️ aff {affiliate_id}{f' / {country}' if country else ''} already mapped"
            elif error_msg.startswith('not_sent:'):
                return f"⚠️ Aff ID is not being sent to {_last_broker_full_name or broker_id}"

        country_str = f" / {country}" if country and country.lower() != "all" else ""
        log.info(f"Mapping saved: aff {affiliate_id} → {override_code} for broker {broker_id}{country_str}")
        result = f"✅ Mapped aff {affiliate_id}{country_str}: override ID = {override_code}"
        return result
    except Exception:
        await _close_modal(page)
        return "⚠️ Save button not found."


async def action_add_funnel_slug_override(broker_id: str, override_codes: list,
                                           countries: list = None, affiliate_id: str = None,
                                           slug: str = None, base_path: str = None) -> str:
    """Добавить API Offer Slug Override для брокера."""
    page = await get_page()

    if not base_path:
        base_path = await find_and_open_broker(page, broker_id)
    if not base_path:
        return f"❌ Broker '{broker_id}' not found."

    url = f"{CRM_URL.rstrip('/')}{base_path}/funnel_slug_overrides"
    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
    await page.wait_for_timeout(1500)
    log.info(f"Opened funnel_slug_overrides: {url}")

    # Нажимаем кнопку ADD
    try:
        add_btn = await page.wait_for_selector(
            "button:has-text('ADD THE FIRST API OFFER SLUG OVERRIDE'), "
            "button:has-text('ADD A NEW API OFFER SLUG OVERRIDE'), "
            "a:has-text('ADD A NEW API OFFER SLUG OVERRIDE')",
            timeout=10000
        )
        await add_btn.click()
        await page.wait_for_timeout(800)
        log.info("Clicked ADD API OFFER SLUG OVERRIDE")
    except Exception:
        return "❌ ADD API OFFER SLUG OVERRIDE button not found."

    try:
        modal = await page.wait_for_selector(".modal-body, [role='dialog']", timeout=5000)
    except Exception:
        return "❌ Modal did not open."
    await page.wait_for_timeout(500)

    # ── 1. Выбираем страны (мультиселект с чекбоксами) ──
    countries_selected = []
    countries_failed = []
    if countries:
        try:
            # Открываем Country дропдаун по лейблу
            await page.evaluate("""() => {
                const labels = document.querySelectorAll('.modal label, [role=dialog] label');
                for (const lbl of labels) {
                    if (lbl.innerText.trim().toLowerCase() === 'country') {
                        const row = lbl.closest('.form-group, .row, fieldset') || lbl.parentElement;
                        const inner = row?.querySelector('.smart__dropdown__input__element') ||
                                      row?.querySelector('[class*="cursor-pointer"]') ||
                                      row?.querySelector('[class*="smart__dropdown"]');
                        if (inner) { inner.click(); return true; }
                    }
                }
                return false;
            }""")
            await page.wait_for_timeout(800)

            for country in countries:
                # Очищаем поле и вводим страну через Playwright type()
                search_inp = await page.evaluate("""() => {
                    const inputs = document.querySelectorAll('input[id*="search-input"], input[id*="search"]');
                    for (const inp of inputs) {
                        if (inp.offsetParent !== null) {
                            inp.value = '';
                            inp.dispatchEvent(new Event('input', {bubbles: true}));
                            return inp.id;
                        }
                    }
                    return null;
                }""")
                await page.wait_for_timeout(200)
                if search_inp:
                    inp_el = await page.query_selector(f"#{search_inp}")
                    if inp_el:
                        await inp_el.click()
                        await inp_el.type(country, delay=60)
                        log.info(f"Typed country via Playwright: {country}")
                else:
                    await page.keyboard.type(country, delay=60)
                await page.wait_for_timeout(700)

                # Кликаем через JS
                clicked = await page.evaluate(f"""(countryName) => {{
                    const items = document.querySelectorAll('li.dropdown-item, li.flex-fill');
                    for (const item of items) {{
                        if (item.innerText.trim().toLowerCase() === countryName.toLowerCase()) {{
                            item.click();
                            return item.innerText.trim();
                        }}
                    }}
                    for (const item of items) {{
                        if (item.innerText.trim().toLowerCase().includes(countryName.toLowerCase())) {{
                            item.click();
                            return item.innerText.trim();
                        }}
                    }}
                    return null;
                }}""", country)
                if clicked:
                    log.info(f"Selected country: {clicked}")
                    countries_selected.append(country)
                else:
                    # Пробуем короткий вариант (первое слово)
                    short = country.split()[0] if ' ' in country else country[:4]
                    if inp_el:
                        await inp_el.click(click_count=3)
                        await inp_el.type(short, delay=60)
                        await page.wait_for_timeout(700)
                    clicked = await page.evaluate(f"""(countryName) => {{
                        const items = document.querySelectorAll('li.dropdown-item, li.flex-fill');
                        for (const item of items) {{
                            if (item.innerText.trim().toLowerCase().includes(countryName.toLowerCase())) {{
                                item.click();
                                return item.innerText.trim();
                            }}
                        }}
                        return null;
                    }}""", short)
                    if clicked:
                        log.info(f"Selected country (short search '{short}'): {clicked}")
                        countries_selected.append(country)
                    else:
                        log.warning(f"Country '{country}' not found (tried full + '{short}')")
                        countries_failed.append(country)
                await page.wait_for_timeout(300)

            # Закрываем Country дропдаун — повторный клик
            await page.evaluate("""() => {
                const labels = document.querySelectorAll('.modal label, [role=dialog] label');
                for (const lbl of labels) {
                    if (lbl.innerText.trim().toLowerCase() === 'country') {
                        const row = lbl.closest('.form-group, .row, fieldset') || lbl.parentElement;
                        const inner = row?.querySelector('.smart__dropdown__input__element') ||
                                      row?.querySelector('[class*="cursor-pointer"]') ||
                                      row?.querySelector('[class*="smart__dropdown"]');
                        if (inner) { inner.click(); return true; }
                    }
                }
                return false;
            }""")
            await page.wait_for_timeout(500)
            log.info("Closed country dropdown")
        except Exception as e:
            log.warning(f"Could not select countries: {e}")

    # ── 2. Выбираем аффилиата (опционально) ──────────
    aff_selected = False
    if affiliate_id:
        try:
            # Ждём дольше после закрытия country dropdown
            await page.wait_for_timeout(1000)

            # Открываем Affiliate дропдаун по лейблу через JS
            await page.evaluate("""() => {
                const labels = document.querySelectorAll('.modal label, [role=dialog] label');
                for (const lbl of labels) {
                    if (lbl.innerText.trim().toLowerCase() === 'affiliate') {
                        const row = lbl.closest('.form-group, .row, fieldset') || lbl.parentElement;
                        const inner = row?.querySelector('.smart__dropdown__input__element') ||
                                      row?.querySelector('[class*="cursor-pointer"]') ||
                                      row?.querySelector('[class*="smart__dropdown"]');
                        if (inner) { inner.click(); return true; }
                    }
                }
                return false;
            }""")
            await page.wait_for_timeout(900)

            # Ждём чтобы список аффилиатов прогрузился
            try:
                await page.wait_for_selector("li.dropdown-item, li.flex-fill", timeout=3000)
            except Exception:
                pass
            await page.wait_for_timeout(500)
            aff_search = await page.evaluate("""() => {
                const labels = document.querySelectorAll('.modal label, [role=dialog] label');
                for (const lbl of labels) {
                    if (lbl.innerText.trim().toLowerCase() === 'affiliate') {
                        const row = lbl.closest('.form-group, .row, fieldset') || lbl.parentElement;
                        const inp = row?.querySelector('input[id*="search-input"], input[id*="search"]');
                        if (inp && inp.offsetParent !== null) return inp.id;
                    }
                }
                // Fallback — любой видимый search input
                const inputs = document.querySelectorAll('input[id*="search-input"], input[id*="search"]');
                for (const inp of inputs) {
                    if (inp.offsetParent !== null) return inp.id;
                }
                return null;
            }""")

            if aff_search:
                search_inp = await page.query_selector(f"#{aff_search}")
            else:
                search_inp = None

            if search_inp:
                await search_inp.click()
                await page.wait_for_timeout(100)
                await search_inp.type(str(affiliate_id), delay=80)
                log.info(f"Affiliate search typed via Playwright: {affiliate_id}")
            else:
                # Последний fallback
                await page.keyboard.type(str(affiliate_id), delay=80)
                log.info(f"Affiliate search typed via keyboard: {affiliate_id}")
            await page.wait_for_timeout(900)

            # Ждём пока список отфильтруется (должно стать < 10 элементов)
            for _ in range(15):
                await page.wait_for_timeout(200)
                cnt = await page.evaluate("() => document.querySelectorAll('li.dropdown-item, li.flex-fill').length")
                if 0 < cnt < 10:
                    break

            # Кликаем через JS
            clicked_aff = await page.evaluate(f"""(affId) => {{
                const items = document.querySelectorAll('li.dropdown-item, li.flex-fill');
                for (const item of items) {{
                    const txt = item.innerText.trim();
                    // Точное совпадение: (225) или *225 или начинается с "225 "
                    if (txt.includes('(' + affId + ')') || txt.startsWith('*' + affId) || txt.startsWith(affId + ' ') || txt.startsWith(affId + '-')) {{
                        item.click();
                        return txt;
                    }}
                }}
                // Частичное: число встречается в тексте как отдельное слово
                for (const item of items) {{
                    const txt = item.innerText.trim();
                    const re = new RegExp('\\\\b' + affId + '\\\\b');
                    if (re.test(txt)) {{
                        item.click();
                        return txt;
                    }}
                }}
                if (items.length === 1) {{ items[0].click(); return items[0].innerText.trim(); }}
                return null;
            }}""", str(affiliate_id))
            if clicked_aff:
                aff_selected = True
                log.info(f"Selected affiliate: {clicked_aff}")
            else:
                log.warning(f"Affiliate '{affiliate_id}' not found in list")
                await _close_modal(page)
                return f"⚠️ Affiliate {affiliate_id} not found — skipped"
            await page.wait_for_timeout(400)
        except Exception as e:
            log.warning(f"Could not select affiliate '{affiliate_id}': {e}")

    # ── 3. Вводим Override codes (один или несколько) ─
    try:
        for override_code in override_codes:
            code_input = await modal.query_selector(
                "input[placeholder*='Add override'], input[placeholder*='override' i], input[id*='override']"
            )
            if not code_input:
                code_input = await modal.query_selector("input.b-form-tags-input, input[class*='form-tags']")

            if not code_input:
                await _close_modal(page)
                return "❌ Overrides input field not found."

            await code_input.click()
            await page.wait_for_timeout(200)
            await code_input.type(str(override_code), delay=60)
            await page.wait_for_timeout(400)

            # Нажимаем ADD
            add_code_btn = await modal.query_selector(
                "button.btn-outline-secondary, button:has-text('Add')"
            )
            if add_code_btn:
                await add_code_btn.click()
                log.info(f"Clicked ADD for override: {override_code}")
            else:
                await code_input.press("Enter")
                log.info(f"Pressed Enter for override: {override_code}")
            await page.wait_for_timeout(400)
    except Exception as e:
        await _close_modal(page)
        return f"❌ Error entering override: {e}"

    # ── 4. Сохраняем ──────────────────────────────────
    try:
        save_btn = await page.wait_for_selector(
            ".modal button[type='submit'], .modal-footer button[type='submit'], "
            ".modal .btn-ladda.btn-success",
            timeout=5000
        )
        await save_btn.click()
        await page.wait_for_timeout(1000)

        # Проверяем появился ли диалог "same override ID already exists" — нужно нажать CONFIRM
        confirm_btn = await page.query_selector("button:has-text('CONFIRM'), .btn-primary:has-text('CONFIRM')")
        if confirm_btn:
            await confirm_btn.click()
            log.info("Clicked CONFIRM for duplicate override ID dialog")
            await page.wait_for_timeout(1000)

        await page.wait_for_timeout(400)

        # Проверяем нет ли ошибки "record already exists"
        error_msg = await page.evaluate("""() => {
            const alerts = document.querySelectorAll('.noty_body, .alert, .toast, [class*="alert"], [class*="toast"], [class*="noty"]');
            for (const el of alerts) {
                const txt = el.innerText?.trim() || '';
                if (txt.toLowerCase().includes('already exist') || txt.toLowerCase().includes('already exists')) {
                    return 'already_exists:' + txt;
                }
                if (txt.toLowerCase().includes('not being sent') || txt.toLowerCase().includes('mapped source')) {
                    return 'not_sent:' + txt;
                }
            }
            return null;
        }""")
        if error_msg:
            log.info(f"CRM noty detected after SAVE: {error_msg[:80]}")
            await _close_modal(page)
            countries_str_err = ", ".join(countries) if countries else "all countries"
            if error_msg.startswith('already_exists:'):
                return f"⚠️ Record already exists ('{', '.join(override_codes)}' for {countries_str_err} already set)"
            elif error_msg.startswith('not_sent:'):
                return f"⚠️ Aff ID is not being sent to {_last_broker_full_name or broker_id}"

        countries_str = ", ".join(countries) if countries else "all countries"
        aff_str = f" / aff {affiliate_id}" if affiliate_id else ""
        codes_str = " and ".join(f"'{c}'" for c in override_codes)
        log.info(f"Funnel override saved: {', '.join(override_codes)} for {broker_id} / {countries_str}{aff_str}")

        warnings = []
        if countries_failed:
            warnings.append(f"⚠️ Countries not found: {', '.join(countries_failed)}")
        if affiliate_id and not aff_selected:
            warnings.append(f"⚠️ Affiliate {affiliate_id} not selected (not found in list)")

        result = f"✅ funnels {codes_str} mapped for {countries_str}"
        if warnings:
            result += "\n" + "\n".join(warnings)
        return result
    except Exception:
        await _close_modal(page)
        return "⚠️ Save button not found."


async def action_toggle_broker(broker_id: str, activate: bool) -> str:
    """Включить или выключить брокера (или нескольких по паттерну)."""
    page = await get_page()

    # Ищем через поиск чтобы найти ВСЕ совпадения
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
        return "❌ Search field not found."

    await search.click(click_count=3)
    await page.keyboard.press("Backspace")
    await page.wait_for_timeout(300)
    await search.fill("")
    await page.wait_for_timeout(200)
    # Для поиска берём первое слово (напр. "Swinftd CRG" → "Swinftd")
    search_term = broker_id.split()[0] if broker_id else broker_id
    await search.type(search_term, delay=60)
    await page.wait_for_timeout(2000)

    # Собираем ВСЕ совпадения с именем и статусом
    rows = await page.evaluate(r"""(query) => {
        const results = [];
        document.querySelectorAll("table tr").forEach(row => {
            const link = row.querySelector("a[href*='/clients/'][href*='/settings']") ||
                         row.querySelector("a.btn-primary");
            if (!link) return;
            const tds = row.querySelectorAll("td");
            let name = "";
            let status = "";
            tds.forEach(td => {
                const t = td.innerText.trim();
                if (["active","inactive","disabled"].includes(t.toLowerCase())) {
                    status = t.toLowerCase();
                    return;
                }
                if (t && !/^\d+$/.test(t) && t.length > 4) {
                    if (!name) name = t;
                }
            });
            if (!name && tds.length > 2) name = tds[2].innerText.trim();
            if (link && name) {
                results.push({
                    name: name,
                    href: link.getAttribute("href"),
                    status: status || "unknown"
                });
            }
        });
        return results;
    }""", search_term)

    log.info(f"Toggle search for '{broker_id}': found {len(rows)} brokers")

    # Фильтруем по паттерну (broker_id может быть "Swinftd CRG" — нужен partial match)
    query_lower = broker_id.lower().strip()
    # Убираем эмодзи при сравнении
    import unicodedata
    def clean_name(n):
        return ''.join(c for c in n if not unicodedata.category(c).startswith('So')).strip().lower()

    matching = []
    if broker_id.isdigit():
        # Числовой ID — точное совпадение: href должен содержать /clients/272/ (не /clients/2722/)
        for r in rows:
            href = r.get("href", "")
            # href вида /clients/272/settings — извлекаем ID
            import re as _re
            m = _re.search(r'/clients/(\d+)/', href)
            if m and m.group(1) == broker_id:
                matching.append(r)
        if not matching:
            # Fallback: имя начинается с "272 -"
            matching = [r for r in rows if clean_name(r["name"]).startswith(query_lower + " ") or clean_name(r["name"]) == query_lower]
    else:
        matching = [r for r in rows if query_lower in clean_name(r["name"])]
        if not matching:
            matching = [r for r in rows if all(w in clean_name(r["name"]) for w in query_lower.split())]

    if not matching:
        return f"❌ No brokers matching '{broker_id}' found."

    log.info(f"Matching brokers: {[(r['name'], r['status']) for r in matching]}")

    # Фильтруем: если деактивируем — только active; если активируем — только inactive
    if activate:
        to_toggle = [r for r in matching if r["status"] != "active"]
    else:
        to_toggle = [r for r in matching if r["status"] == "active"]

    if not to_toggle:
        state = "already inactive" if not activate else "already active"
        names = ", ".join(r["name"] for r in matching)
        return f"ℹ️ All matching brokers ({names}) are {state}. Nothing changed."

    skipped = [r for r in matching if r not in to_toggle]
    results = []

    for broker in to_toggle:
        href = broker["href"]
        settings_url = f"{CRM_URL.rstrip('/')}{href}" if not href.startswith("http") else href
        if "/settings" not in settings_url:
            settings_url = settings_url.replace("/settings", "") + "/settings"
        await page.goto(settings_url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(1500)

        try:
            toggle = await page.wait_for_selector("input#active, input[id*='active'][type='checkbox']", timeout=5000)
            label = await page.query_selector("label[for='active'].custom-control-label, label:has-text('Broker is active')")
            if label:
                await label.click()
                log.info(f"Clicked toggle for {broker['name']}")
            else:
                await toggle.evaluate("el => el.click()")
            await page.wait_for_timeout(500)

            save = await page.wait_for_selector("text=SAVE SETTINGS", timeout=4000)
            await save.click()
            await page.wait_for_timeout(1000)

            action_word = "active" if activate else "inactive"
            results.append(f"✅ {broker['name']}: {action_word}")
        except Exception as e:
            results.append(f"❌ {broker['name']}: error — {e}")

    if skipped:
        state = "already inactive" if not activate else "already active"
        for s in skipped:
            results.append(f"⏭ {s['name']}: {state}")

    return "\n".join(results)


async def action_change_caps(broker_id: str, country: str, cap_value: int = 0, delta: int = None, affiliate_id: str = None, delete_first: bool = False, base_path: str = None) -> str:
    """Изменить или создать cap для страны брокера. delta — прибавить к текущему значению. affiliate_id — добавить параметр Affiliates."""
    page = await get_page()

    if not base_path:
        base_path = await find_and_open_broker(page, broker_id)
    if not base_path:
        return f"❌ Broker '{broker_id}' not found."

    caps_url = f"{CRM_URL.rstrip('/')}{base_path}/caps"
    await page.goto(caps_url, wait_until="domcontentloaded", timeout=30000)
    # Ждём загрузки таблицы или кнопки ADD CAP
    try:
        await page.wait_for_selector("table tr td, button:has-text('ADD CAP'), a:has-text('ADD CAP')", timeout=10000)
    except Exception:
        pass
    await page.wait_for_timeout(2000)  # даём Vue время отрендерить данные
    log.info(f"Caps page URL: {page.url}")

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
        # Если country = "all" или пустая — пропускаем выбор (оставляем "All countries")
        if not country or country.lower() == "all":
            country_selected = True  # пропускаем, поле остаётся пустым = все страны
        else:
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
                    // Сначала точное совпадение
                    for (const item of lists) {
                        if (item.innerText.trim().toLowerCase() === countryName.toLowerCase()) {
                            item.click();
                            return item.innerText.trim();
                        }
                    }
                    // Потом частичное
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
                    actual_country = country_selected  # реальное имя выбранной страны
                    country_selected = True
                    await page.wait_for_timeout(400)
                else:
                    actual_country = country
                    # Fallback — Playwright query прямо перед кликом
                    items = await page.query_selector_all("li.dropdown-item, li.flex-fill")
                    log.info(f"Dropdown items: {len(items)}")
                    for item in items:
                        try:
                            txt = (await item.inner_text()).strip()
                            if country.lower() == txt.lower():
                                await item.click()
                                country_selected = True
                                actual_country = txt
                                log.info(f"Country selected: {txt}")
                                await page.wait_for_timeout(400)
                                break
                        except Exception:
                            continue
                    if not country_selected:
                        for item in items:
                            try:
                                txt = (await item.inner_text()).strip()
                                if country.lower() in txt.lower():
                                    await item.click()
                                    country_selected = True
                                    actual_country = txt
                                    log.info(f"Country selected (partial): {txt}")
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
                f"Action: set hours\n"
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
                f"Action: set hours\n"
                f"Brokers: `{brokers}`\n"
                f"Countries & hours:\n{lines}\n"
                f"Days: {days}\n"
                f"No-traffic: {'✅ yes' if action.get('no_traffic', True) else '❌ no'}\n\n"
                f"Confirm?"
            )

    if a == "toggle_broker":
        brokers = ", ".join(str(b) for b in action.get("broker_ids", []))
        word = "ACTIVATE" if action.get("active") else "DEACTIVATE"
        return (
            f"📋 *Confirmation required*\n\n"
            f"Action: {word} broker\n"
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
            f"Action: set price\n"
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

        # Группируем задачи по брокеру
        brokers_data = {}
        for t in tasks:
            bid = t.get("broker_id", "?")
            if bid not in brokers_data:
                brokers_data[bid] = {"lead": None, "funnels": [], "aff_overrides": [], "close_days": []}
            tt = t.get("type")
            if tt == "lead_task":
                brokers_data[bid]["lead"] = t
            elif tt == "funnel_override":
                brokers_data[bid]["funnels"].append(t)
            elif tt == "affiliate_override":
                brokers_data[bid]["aff_overrides"].append(t)
            elif tt == "close_day":
                brokers_data[bid]["close_days"].append(t)

        sections = []
        for bid, d in brokers_data.items():
            lead = d["lead"]
            geo = lead.get("country", "—") if lead else "—"
            cap = str(lead.get("cap", "—")) if lead else "—"
            start = lead.get("start", "") if lead else ""
            end = lead.get("end", "") if lead else ""
            day = lead.get("day", "") if lead else ""
            wh = f"{start}–{end}" if start else "—"
            if day:
                wh += f" ({day})"

            funnel_codes = []
            for ft in d["funnels"]:
                funnel_codes.extend(ft.get("override_codes", []))
            funnel_str = ", ".join(funnel_codes) if funnel_codes else "—"

            aff_codes = []
            for at in d["aff_overrides"]:
                aff_codes.append(f"{at.get('affiliate_id')}→{at.get('override_code')}")
            aff_str = ", ".join(aff_codes) if aff_codes else "—"

            sections.append(
                f"*Broker:* {bid}\n"
                f"Geo: {geo}\n"
                f"Cap: {cap}\n"
                f"WH: {wh}\n"
                f"Aff ID override: {aff_str}\n"
                f"Funnel override: {funnel_str}"
            )

        body = "\n\n".join(sections)
        return f"📋 *Confirmation required*\n\n{body}\n\nConfirm?"

    if a == "funnel_slug_override":
        brokers = ", ".join(str(b) for b in action.get("broker_ids", []))
        override_code = action.get("override_code", "")
        override_codes = action.get("override_codes") or ([override_code] if override_code else [])
        codes_str = ", ".join(override_codes) if override_codes else "?"
        countries_list = action.get("funnel_countries") or action.get("countries", [])
        countries_str = ", ".join(countries_list) if countries_list else "all countries"
        aff_id = action.get("affiliate_id")
        aff_ids = action.get("affiliate_ids", [])
        if aff_ids:
            aff_str = f"\nAffiliates: `{', '.join(aff_ids)}`"
        elif aff_id:
            aff_str = f"\nAffiliate: `{aff_id}`"
        else:
            aff_str = ""
        return (
            f"📋 *Confirmation required*\n\n"
            f"Action: add funnel slug override\n"
            f"Broker: `{brokers}`\n"
            f"Countries: {countries_str}{aff_str}\n"
            f"Override(s): `{codes_str}`\n\n"
            f"Confirm?"
        )

    if a == "map_affiliate":
        brokers = ", ".join(str(b) for b in action.get("broker_ids", []))
        aff_id = action.get("affiliate_id", "?")
        override_code = action.get("override_code", "?")
        country = action.get("country") or "all countries"
        return (
            f"📋 *Confirmation required*\n\n"
            f"Action: add affiliate ID mapping\n"
            f"Broker: `{brokers}`\n"
            f"Affiliate: `{aff_id}`\n"
            f"Country: {country}\n"
            f"Override ID: `{override_code}`\n\n"
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
            # Обработка queries (несколько брокеров/стран в одном сообщении)
            queries = action.get("queries", [])
            if queries:
                for q in queries:
                    bid = q.get("id", "")
                    countries = q.get("countries", ["all"])
                    if not bid:
                        continue
                    lid = alog.log_action("get_caps", str(bid),
                                          ", ".join(countries), "pending", user_command=text)
                    result = await action_get_caps(str(bid), countries,
                                                       affiliate_id=action.get("affiliate_id"))
                    alog.update_action(lid, "success" if "❌" not in result else "error", result[:200])
                    display_name = _last_broker_full_name if _last_broker_full_name != str(bid) else str(bid)
                    await bot.send_message(chat_id, f"*Caps {escape_md(display_name)}:*\n{escape_md(result)}", parse_mode="Markdown", disable_notification=True)
            else:
                for bid in action.get("broker_ids", []):
                    lid = alog.log_action("get_caps", str(bid),
                                          ", ".join(action.get("countries", ["all"])), "pending", user_command=text)
                    result = await action_get_caps(str(bid), action.get("countries", ["all"]),
                                                       affiliate_id=action.get("affiliate_id"))
                    alog.update_action(lid, "success" if "❌" not in result else "error", result[:200])
                    display_name = _last_broker_full_name if _last_broker_full_name != str(bid) else str(bid)
                    await bot.send_message(chat_id, f"*Caps {escape_md(display_name)}:*\n{escape_md(result)}", parse_mode="Markdown", disable_notification=True)

        alog.set_status("last_action", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    except Exception as e:
        log.exception(f"Error in get task: {e}")
        await bot.send_message(chat_id, f"❌ Error: `{escape_md(str(e))}`", parse_mode="Markdown", disable_notification=True)


async def _execute_confirmed_task(bot, chat_id: int, action: dict):
    """Выполнить подтверждённое действие (вызывается из очереди)."""
    global _last_broker_full_name
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
            # Кэш base_path — ищем каждого брокера только один раз
            broker_base_cache: dict = {}
            # Кэш стран — берём из lead_task для использования в других задачах
            broker_country_cache: dict = {}
            # Группировка строк результата по брокеру (OrderedDict сохраняет порядок)
            broker_lines: dict = {}

            # Собираем данные ротации для отчётов
            # rotation_info: {broker_id: {"affs": [...], "country": "...", "is_tomorrow": bool}}
            rotation_info: dict = {}
            for task in tasks:
                if task.get("type") == "lead_task" and task.get("country") and task.get("broker_id"):
                    broker_country_cache[task["broker_id"]] = task["country"]
                    bid = task["broker_id"]
                    if bid not in rotation_info:
                        rotation_info[bid] = {"affs": [], "country": task["country"], "is_tomorrow": False}
                    # Определяем is_tomorrow по дню недели
                    import datetime as _dt
                    day_names = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
                    tomorrow_name = day_names[(_dt.datetime.now().weekday() + 1) % 7]
                    t_day_val = task.get("day", "")
                    if t_day_val and t_day_val.lower() == tomorrow_name.lower():
                        rotation_info[bid]["is_tomorrow"] = True
                # Собираем аффов из funnel_override
                if task.get("type") == "funnel_override":
                    bid = task.get("broker_id", "")
                    affs = task.get("affiliate_ids", [])
                    if bid not in rotation_info:
                        rotation_info[bid] = {"affs": [], "country": broker_country_cache.get(bid, ""), "is_tomorrow": False}
                    rotation_info[bid]["affs"].extend(affs)
                # Собираем оригинального аффа из lead_task
                if task.get("type") == "lead_task":
                    bid = task.get("broker_id", "")
                    affs = task.get("affiliate_ids", [])
                    if bid and affs:
                        if bid not in rotation_info:
                            rotation_info[bid] = {"affs": [], "country": task.get("country", ""), "is_tomorrow": False}
                        for a in affs:
                            if str(a) not in rotation_info[bid]["affs"]:
                                rotation_info[bid]["affs"].append(str(a))
                # Собираем оригинального аффа из affiliate_override (affiliate_id = кто реально шлёт трафик)
                if task.get("type") == "affiliate_override":
                    bid = task.get("broker_id", "")
                    aff_id = str(task.get("affiliate_id", "") or "")
                    if bid and aff_id:
                        if bid not in rotation_info:
                            rotation_info[bid] = {"affs": [], "country": broker_country_cache.get(bid, ""), "is_tomorrow": False}
                        if aff_id not in rotation_info[bid]["affs"]:
                            rotation_info[bid]["affs"].append(aff_id)

            for task in tasks:
                if task.get("type") == "lead_task" and task.get("country") and task.get("broker_id"):
                    broker_country_cache[task["broker_id"]] = task["country"]
            for task in tasks:
                t_type = task.get("type", "lead_task")
                t_broker = task.get("broker_id", "")
                t_country = task.get("country", "")
                t_day = task.get("day", "")

                # Получаем base_path из кэша или ищем один раз
                if t_broker not in broker_base_cache:
                    _page = await get_page()
                    _bp = await find_and_open_broker(_page, t_broker, country_hint=t_country)
                    broker_base_cache[t_broker] = _bp
                    if _bp:
                        log.info(f"Cached base_path for '{t_broker}': {_bp}")
                    else:
                        log.warning(f"Broker '{t_broker}' not found — skipping tasks for it")
                t_base_path = broker_base_cache.get(t_broker)

                lid = alog.log_action(f"multi_{t_type}", t_broker, f"{t_country} {t_day}",
                                      "pending", user_command=user_cmd)
                log_ids.append(lid)

                try:
                    if t_type == "close_day":
                        # Закрываем день
                        close_msg = await action_close_days(
                            broker_id=t_broker,
                            country=t_country,
                            days_to_close=[t_day],
                            country_hint=t_country
                        )
                        display_name = _last_broker_full_name if _last_broker_full_name != t_broker else t_broker
                        if display_name not in broker_lines: broker_lines[display_name] = []
                        broker_lines[display_name].append(f"🚫 {close_msg}")
                        alog.update_action(lid, "success" if "❌" not in close_msg else "error", close_msg[:200])

                    elif t_type == "funnel_override":
                        t_override_codes = task.get("override_codes", [])
                        t_funnel_countries = task.get("funnel_countries") or []
                        # Если страна не указана — берём из lead_task того же брокера
                        if not t_funnel_countries:
                            inherited = broker_country_cache.get(t_broker) or t_country
                            if inherited:
                                t_funnel_countries = [inherited]
                        t_funnel_countries = t_funnel_countries or None
                        t_aff_ids = task.get("affiliate_ids", [])
                        t_aff_id = task.get("affiliate_id") or None

                        # Автоматически определяем фанел если use_last_funnel
                        if task.get("use_last_funnel"):
                            ref_aff = task.get("reference_affiliate", "")
                            ref_country = task.get("reference_country", "") or (t_funnel_countries[0] if t_funnel_countries else "")
                            if ref_aff and ref_country:
                                fetched_funnel = await _fetch_last_funnel(ref_aff, ref_country)
                                if fetched_funnel:
                                    t_override_codes = [fetched_funnel]
                                    log.info(f"Auto-detected funnel for aff {ref_aff} / {ref_country}: {fetched_funnel}")
                                else:
                                    display_name = _last_broker_full_name if _last_broker_full_name != t_broker else t_broker
                                    if display_name not in broker_lines: broker_lines[display_name] = []
                                    broker_lines[display_name].append(f"📝 Mapping: ❌ Could not find last funnel for aff {ref_aff} / {ref_country}")
                                    continue

                        if not t_override_codes:
                            funnel_msg = "❌ No override codes specified"
                            display_name = _last_broker_full_name if _last_broker_full_name != t_broker else t_broker
                            if display_name not in broker_lines: broker_lines[display_name] = []
                            broker_lines[display_name].append(f"📝 Mapping: {funnel_msg}")
                        elif t_aff_ids:
                            sub_parts = []
                            for one_aff in t_aff_ids:
                                sub_msg = await action_add_funnel_slug_override(
                                    broker_id=t_broker,
                                    override_codes=t_override_codes,
                                    countries=t_funnel_countries,
                                    affiliate_id=str(one_aff),
                                    base_path=t_base_path
                                )
                                sub_parts.append(f"aff {one_aff}: {sub_msg}")
                            display_name = _last_broker_full_name if _last_broker_full_name != t_broker else t_broker
                            if display_name not in broker_lines: broker_lines[display_name] = []
                            broker_lines[display_name].append("📝 Mapping: " + "\n".join(sub_parts))
                            alog.update_action(lid, "success", "; ".join(sub_parts)[:200])
                        else:
                            funnel_msg = await action_add_funnel_slug_override(
                                broker_id=t_broker,
                                override_codes=t_override_codes,
                                countries=t_funnel_countries,
                                affiliate_id=t_aff_id,
                                base_path=t_base_path
                            )
                            display_name = _last_broker_full_name if _last_broker_full_name != t_broker else t_broker
                            if display_name not in broker_lines: broker_lines[display_name] = []
                            broker_lines[display_name].append(f"📝 Mapping: {funnel_msg}")
                            alog.update_action(lid, "success" if "✅" in funnel_msg else "error", funnel_msg[:200])

                    elif t_type == "affiliate_override":
                        t_aff_id = str(task.get("affiliate_id", ""))
                        t_override_code = str(task.get("override_code", ""))
                        t_map_country = task.get("country") or None

                        if not t_aff_id or not t_override_code:
                            aff_msg = "❌ affiliate_id or override_code missing"
                        else:
                            aff_msg = await action_add_affiliate_mapping(
                                broker_id=t_broker,
                                affiliate_id=t_aff_id,
                                override_code=t_override_code,
                                country=t_map_country,
                                base_path=t_base_path
                            )
                        display_name = _last_broker_full_name if _last_broker_full_name != t_broker else t_broker
                        if display_name not in broker_lines: broker_lines[display_name] = []
                        broker_lines[display_name].append(f"📝 Aff mapping: {aff_msg}")
                        alog.update_action(lid, "success" if "✅" in aff_msg else "error", aff_msg[:200])

                    else:
                        # lead_task — капа + часы
                        sub_parts = []

                        # Используем кэшированный base_path
                        page = await get_page()
                        mb_broker_base = t_base_path

                        # Капа
                        if task.get("cap") is not None:
                            cap_msg = await action_change_caps(
                                broker_id=t_broker,
                                country=t_country,
                                cap_value=int(task["cap"]),
                                delta=None,
                                affiliate_id=None,
                                delete_first=False,
                                base_path=mb_broker_base,
                            )
                            if cap_msg.startswith("__"):
                                cap_msg = await action_change_caps(
                                    broker_id=t_broker,
                                    country=t_country,
                                    cap_value=int(task["cap"]),
                                    delta=None,
                                    affiliate_id=None,
                                    delete_first=True,
                                    base_path=mb_broker_base,
                                )
                            sub_parts.append(f"🎯 Cap: {cap_msg}")

                        # Часы
                        if task.get("start"):
                            t_start = task["start"]
                            t_end = task.get("end") or ""
                            if mb_broker_base:
                                oh_url = f"{CRM_URL.rstrip('/')}{mb_broker_base}/opening_hours"
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
                                            start=t_start,
                                            end=t_end,
                                            no_traffic=task.get("no_traffic", True),
                                            days_to_add=[t_day],
                                            base_path=mb_broker_base
                                        )
                                    else:
                                        new_days = [t_day] if is_weekend else ["Monday","Tuesday","Wednesday","Thursday","Friday"]
                                        hours_msg = await action_add_country_hours(
                                            broker_id=t_broker,
                                            country=t_country,
                                            start=t_start,
                                            end=t_end,
                                            no_traffic=task.get("no_traffic", True),
                                            days_filter=new_days,
                                            base_path=mb_broker_base
                                        )
                                else:
                                    hours_msg = await action_change_hours(
                                        broker_id=t_broker,
                                        start=t_start,
                                        end=t_end,
                                        countries_filter=[t_country],
                                        no_traffic=task.get("no_traffic", True),
                                        days_filter=["Monday","Tuesday","Wednesday","Thursday","Friday"],
                                        base_path=mb_broker_base
                                    )
                                sub_parts.append(f"🕐 Hours: {hours_msg}")
                            else:
                                sub_parts.append(f"❌ Broker '{t_broker}' not found")

                        display_name = _last_broker_full_name if _last_broker_full_name != t_broker else t_broker
                        if display_name not in broker_lines:
                            broker_lines[display_name] = []
                        for part in sub_parts:
                            broker_lines[display_name].append(part)
                        alog.update_action(lid, "success" if not any("❌" in p for p in sub_parts) else "error",
                                          "; ".join(sub_parts)[:200])

                except Exception as e:
                    display_name = _last_broker_full_name if _last_broker_full_name != t_broker else t_broker
                    if display_name not in broker_lines: broker_lines[display_name] = []
                    broker_lines[display_name].append(f"❌ {str(e)}")
                    alog.update_action(lid, "error", str(e)[:200])

            # Сохраняем ротации для отчётов
            for bid, info in rotation_info.items():
                broker_display = _last_broker_full_name if _last_broker_full_name != bid else bid
                entry = {"affs": list(set(info["affs"])), "country": info["country"]}
                if info["is_tomorrow"]:
                    tomorrow_rotations[broker_display] = entry
                    log.info(f"Rotation saved to tomorrow: {broker_display} / {info['country']} / affs {entry['affs']}")
                else:
                    today_rotations[broker_display] = entry
                    log.info(f"Rotation saved to today: {broker_display} / {info['country']} / affs {entry['affs']}")

            # Собираем финальное сообщение — один блок на брокера
            alog.set_status("last_action", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            final_parts = []
            for broker_name, lines in broker_lines.items():
                block = f"*Broker {escape_md(broker_name)}:*\n" + "\n\n".join(lines)
                final_parts.append(block)
            msg_text = "\n\n".join(final_parts) or "✅ Done."
            for attempt in range(3):
                try:
                    await bot.send_message(chat_id, msg_text, parse_mode="Markdown", disable_notification=True)
                    break
                except Exception:
                    if attempt < 2:
                        await asyncio.sleep(3)
            return

        for broker_id in action.get("broker_ids", []):
            _last_broker_full_name = str(broker_id)  # сбрасываем на текущий ID

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
                    msg = "❌ Please specify country and hours."
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
                        first_country = next((ch.get("country") for ch in country_hours_list if ch.get("country")), None)
                        broker_base = await find_and_open_broker(page, str(broker_id), country_hint=first_country)
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
                        country_end   = ch.get("end") or ""

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
                                    days_to_add=[requested_day],
                                    base_path=broker_base
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
                                    days_filter=new_days_filter,
                                    base_path=broker_base
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
                                days_to_add=days_to_add,
                                base_path=broker_base
                            )
                        else:
                            sub_msg = await action_add_country_hours(
                                broker_id=str(broker_id),
                                country=country_name,
                                start=country_start,
                                end=country_end,
                                no_traffic=action.get("no_traffic", True),
                                days_filter=days_filter,
                                base_path=broker_base
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
                    if days_to_close and country != "all":
                        countries_days = [{"country": country, "days_to_close": days_to_close}]

                # Если country=all — требуем подтверждение (не блокируем)
                # Подтверждение обеспечивается тем, что close_days НЕ в списке no-confirmation

                if not countries_days:
                    msg = "❌ Please specify countries and days."
                else:
                    sub_results = []
                    # Определяем hint-страну для LATAM маршрутизации
                    first_country = next((cd.get("country") for cd in countries_days if cd.get("country", "all").lower() != "all"), None)
                    # Ищем брокера ОДИН раз
                    page = await get_page()
                    broker_base = await find_and_open_broker(page, str(broker_id), country_hint=first_country)
                    if not broker_base:
                        msg = f"❌ Broker '{broker_id}' not found."
                    else:
                        # Если все страны с одинаковыми днями — можно обработать все разом
                        # через "all" mode если countries=all, или по одной
                        all_same_days = len(set(tuple(cd.get("days_to_close", [])) for cd in countries_days)) == 1
                        countries_list = [cd.get("country", "all") for cd in countries_days]

                        if len(countries_days) == 1 and countries_list[0].lower() == "all":
                            # Одна запись "all" — используем all mode
                            sub_msg = await action_close_days(
                                broker_id=str(broker_id),
                                country="all",
                                days_to_close=countries_days[0].get("days_to_close", []),
                                base_path=broker_base
                            )
                            sub_results.append(sub_msg)
                        else:
                            for cd in countries_days:
                                sub_msg = await action_close_days(
                                    broker_id=str(broker_id),
                                    country=cd.get("country", "all"),
                                    days_to_close=cd.get("days_to_close", []),
                                    base_path=broker_base
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
                    # Фильтруем записи без суммы
                    country_revenues = [cr for cr in country_revenues if cr.get("amount") is not None and str(cr.get("amount", "")).strip()]
                    if not country_revenues:
                        msg = "❌ Amount not specified."
                    else:
                        sub_results = []
                        # Ищем брокера один раз
                        page = await get_page()
                        first_country = next((cr.get("country") for cr in country_revenues if cr.get("country", "all").lower() != "all"), None)
                        rev_broker_base = await find_and_open_broker(page, str(broker_id), country_hint=first_country)
                        # Группируем страны по одинаковой сумме + affiliate_id
                        from collections import defaultdict
                        groups = defaultdict(list)
                        for cr in country_revenues:
                            key = (str(cr.get("amount", "")), str(cr.get("affiliate_id") or ""))
                            groups[key].append(cr.get("country", "all"))
                        for (grp_amount, grp_aff_id), grp_countries in groups.items():
                            aff_param = grp_aff_id if grp_aff_id else None
                            if len(grp_countries) == 1:
                                sub_msg = await action_add_revenue(
                                    broker_id=str(broker_id),
                                    country=grp_countries[0],
                                    amount=grp_amount,
                                    affiliate_id=aff_param,
                                    base_path=rev_broker_base
                                )
                                sub_results.append(sub_msg)
                            else:
                                sub_msg = await action_add_revenue_grouped(
                                    broker_id=str(broker_id),
                                    countries=grp_countries,
                                    amount=grp_amount,
                                    affiliate_id=aff_param,
                                    base_path=rev_broker_base
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
                    # Группируем страны по одинаковой сумме → меньше открытий модалки
                    from collections import defaultdict
                    groups = defaultdict(list)
                    for cr in cr_list:
                        groups[str(cr.get("amount", ""))].append(cr.get("country", "all"))
                    for grp_amount, grp_countries in groups.items():
                        if len(grp_countries) == 1:
                            # Одна страна — старый способ
                            sub_msg = await action_add_affiliate_revenue(
                                affiliate_id=aff_id,
                                country=grp_countries[0],
                                amount=grp_amount
                            )
                            sub_results.append(sub_msg)
                        else:
                            # Несколько стран с одной суммой — групповой способ
                            sub_msg = await action_add_affiliate_revenue_grouped(
                                affiliate_id=aff_id,
                                countries=grp_countries,
                                amount=grp_amount
                            )
                            sub_results.append(sub_msg)
                    msg = "\n".join(sub_results)
            elif a == "toggle_broker":
                msg = await action_toggle_broker(str(broker_id), action.get("active", True))
            elif a == "map_affiliate":
                aff_id = str(action.get("affiliate_id", ""))
                override_code = str(action.get("override_code", ""))
                country = action.get("country") or None
                if not aff_id or not override_code:
                    msg = "❌ Please specify affiliate ID and override code."
                else:
                    msg = await action_add_affiliate_mapping(
                        broker_id=str(broker_id),
                        affiliate_id=aff_id,
                        override_code=override_code,
                        country=country
                    )
            elif a == "funnel_slug_override":
                override_code = action.get("override_code", "")
                override_codes = action.get("override_codes") or ([override_code] if override_code else [])
                countries_list = action.get("funnel_countries") or action.get("countries", [])
                aff_id = str(action.get("affiliate_id", "")) or None
                aff_ids = action.get("affiliate_ids", [])
                if not override_codes:
                    msg = "❌ Please specify funnel override code (name)."
                elif aff_ids:
                    # Несколько аффилиатов — делаем по одному
                    sub_results = []
                    for one_aff in aff_ids:
                        sub_msg = await action_add_funnel_slug_override(
                            broker_id=str(broker_id),
                            override_codes=override_codes,
                            countries=countries_list if countries_list else None,
                            affiliate_id=str(one_aff)
                        )
                        sub_results.append(f"aff {one_aff}: {sub_msg}")
                    msg = "\n".join(sub_results)
                else:
                    msg = await action_add_funnel_slug_override(
                        broker_id=str(broker_id),
                        override_codes=override_codes,
                        countries=countries_list if countries_list else None,
                        affiliate_id=aff_id
                    )
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
                    # Ищем брокера один раз
                    page = await get_page()
                    caps_broker_base = await find_and_open_broker(page, str(broker_id))
                    for cc in cc_list:
                        delta_val  = cc.get("delta")
                        cap_val    = cc.get("cap")
                        aff_id_val = cc.get("affiliate_id")
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
                            base_path=caps_broker_base,
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

                # Ищем брокера ОДИН раз
                page = await get_page()
                lt_broker_base = await find_and_open_broker(page, str(broker_id))
                if not lt_broker_base:
                    msg = f"❌ Broker '{broker_id}' not found."
                else:

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
                                base_path=lt_broker_base,
                            )
                            if sub_msg.startswith("__"):
                                sub_msg = await action_change_caps(
                                    broker_id=str(broker_id),
                                    country=cc.get("country", ""),
                                    cap_value=int(cap_val) if cap_val is not None else 0,
                                    delta=None,
                                    affiliate_id=str(aff_id_val) if aff_id_val is not None else None,
                                    delete_first=True,
                                    base_path=lt_broker_base,
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
                            oh_url = f"{CRM_URL.rstrip('/')}{lt_broker_base}/opening_hours"
                            await page.goto(oh_url, wait_until="domcontentloaded", timeout=60000)
                            existing_countries = await _scrape_countries_from_page(page)
                        except Exception as e:
                            log.warning(f"Failed to get existing countries: {e}")

                        for ch in country_hours_list:
                            country_name = ch.get("country", "")
                            country_start = ch.get("start", "09:00")
                            country_end = ch.get("end") or ""
                            
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
                                            days_to_add=[requested_day],
                                            base_path=lt_broker_base
                                        )
                                    else:
                                        new_days_filter = [requested_day] if is_weekend_request else weekdays
                                        sub_msg = await action_add_country_hours(
                                            broker_id=str(broker_id),
                                            country=country_name,
                                            start=country_start,
                                            end=country_end,
                                            no_traffic=action.get("no_traffic", True),
                                            days_filter=new_days_filter,
                                            base_path=lt_broker_base
                                        )
                                else:
                                    if country_exists:
                                        sub_msg = await action_change_hours(
                                            broker_id=str(broker_id),
                                            start=country_start,
                                            end=country_end,
                                            countries_filter=[country_name],
                                            no_traffic=action.get("no_traffic", True),
                                            days_filter=days_filter,
                                            base_path=lt_broker_base
                                        )
                                    else:
                                        sub_msg = await action_add_country_hours(
                                            broker_id=str(broker_id),
                                            country=country_name,
                                            start=country_start,
                                            end=country_end,
                                            no_traffic=action.get("no_traffic", True),
                                            days_filter=days_filter,
                                            base_path=lt_broker_base
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
                        await page.wait_for_selector("button.btn-primary.btn-sm, button.btn-outline-primary.btn-sm", timeout=12000)
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
                            edit_buttons = await page.query_selector_all("button.btn-primary.btn-sm, button.btn-outline-primary.btn-sm, button.btn.btn-sm.btn-primary, button.btn.btn-sm.btn-outline-primary")
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
                msg = f"⚠️ Action '{a}' is not supported yet."

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

    text = (update.message.text or "").strip()
    if not text:
        return

    # Если сообщение — ответ на другое сообщение, добавляем контекст
    reply_context = ""
    if update.message.reply_to_message and update.message.reply_to_message.text:
        reply_context = update.message.reply_to_message.text.strip()

    # В групповых чатах — реагируем только на сообщения, похожие на CRM-команды
    # В личке — обрабатываем всё
    is_group = update.effective_chat.type in ("group", "supergroup")
    if is_group:
        text_lower_orig = text.lower()
        text_upper_orig = text.upper()

        # Сначала проверяем САМО сообщение (без reply-контекста)
        # Если новое сообщение не содержит CRM-паттернов — игнорируем
        # (даже если reply содержит "capitan" или другие ключевые слова)
        crm_commands = ("cap", "price", "wh ", "hours", "прайс", "часы", "кап", "лимит",
                        "schedule", "geo:", "desk", "off", "close", "закрыть", "выходн",
                        "pause", "back in", "is back", "inactive", "deactivate", "activate", "disable", "enable",
                        "put active", "put inactive", "bring back", "back to active", "active now", "is active",
                        "set active", "set inactive", "make active", "make inactive", "total")
        # Числа с % (100%) или через / (21/117/28) — не прайсы
        text_no_slashed = re.sub(r'\d+(/\d+)+', '', text)  # убираем "21/117/28/13"
        msg_has_price = bool(re.search(r'\b[A-Z]{2}\b', text_upper_orig) and re.search(r'\b\d{3,4}\b(?!%)', text_no_slashed))
        msg_has_command = any(kw in text_lower_orig for kw in crm_commands)
        msg_has_time = bool(re.search(r'\d{1,2}:\d{2}\s*[-–]\s*\d{1,2}:\d{2}', text) or
                            re.search(r'\d{1,2}\.\d{2}\s*[-–]\s*\d{1,2}\.\d{2}', text) or
                            re.search(r'\bat\s+\d{1,2}:\d{2}', text_lower_orig))
        msg_has_broker_name = bool(re.search(r'\b(legion|nexus|capitan|fintrix|capex|swin|helios|axia|fugazi|ave|theta|imperius|emp|cmt|glb|mn|marsi|farah|roibees|clickbait|avelux|mediaNow|universo|fusion|ventury)\b', text_lower_orig, re.IGNORECASE))

        # Имя брокера одно по себе — не команда. Нужно ещё что-то (ISO код, число, время, ключевое слово)
        msg_has_action_context = bool(
            re.search(r'\b[A-Z]{2}\b', text_upper_orig) or  # ISO код
            re.search(r'\b\d{3,4}\b(?!%)', text_no_slashed) or  # число (прайс/капа)
            re.search(r'\d{1,2}[:.]\d{2}', text) or            # время (: или .)
            any(kw in text_lower_orig for kw in crm_commands)  # ключевое слово
        )

        if not (msg_has_price or msg_has_command or msg_has_time or (msg_has_broker_name and msg_has_action_context)):
            return

        # Блокируем обсуждения/жалобы — если есть разговорные слова и НЕТ имени брокера + команды
        conversational = ("rejection", "rejections", "many reject", "why ", "how come",
                          "imagine", "not sure", "what happened", "what's going on",
                          "paying", "invoice", "i think", "probably",
                          "не умеет", "почему", "сломал", "не работает", "ебан",
                          "we call", "do we", "can we", "should we", "will we",
                          "is it", "are they", "do they")
        if any(kw in text_lower_orig for kw in conversational):
            if not (msg_has_broker_name and (msg_has_command or msg_has_time)):
                return

        # Вопросы (заканчиваются на ?) без явного имени брокера и команды — игнорируем
        if text_lower_orig.rstrip().endswith("?"):
            if not (msg_has_broker_name and (msg_has_command or msg_has_time)):
                return

        # Теперь объединяем для полной проверки (reply-контекст даёт дополнительную информацию)
        combined_text = f"{text}\n{reply_context}" if reply_context else text
        text_lower = combined_text.lower()
        text_upper = combined_text.upper()
        # CPL — игнорируем полностью
        if "cpl" in text.lower():
            return
        # Сообщения с email — это уведомления о лидах, не CRM-команды
        if re.search(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', text):
            return
        # "started" без времени — уведомление о лиде ("Ave DE started"), не команда
        if "started" in text_lower and not re.search(r'\d{1,2}:\d{2}', text):
            return
        # "rotation" как единственная тема — не CRM-команда. Но если есть cap/hours/price — пропускаем
        if "rotation" in text_lower_orig and not (msg_has_price or msg_has_command or msg_has_time):
            return
        # "balance" — обсуждение оплаты, не команда
        if "balance" in text_lower_orig:
            return
        # Вопросы типа "28 DE closed?" с числовым ID — игнорируем (это про аффа)
        # Но "Legion DE closed?" — обрабатываем (это запрос часов брокера)
        stripped = text.strip()
        first_word = stripped.split()[0] if stripped.split() else ""
        # Числовой ID + close/pause/off = аффилиат, игнорируем
        # НО "3372 - GLB CRG" (число-дефис-имя) = брокер с ID, обрабатываем
        if first_word.isdigit():
            has_broker_format = bool(re.search(r'^\d+\s*-\s*\w', stripped))
            if not has_broker_format and any(kw in text_lower for kw in ("close", "pause", "off", "open")):
                return

    # Если есть контекст из reply — передаём AI оба текста
    if reply_context:
        text = f"[Ответ на сообщение:]\n{reply_context}\n\n[Новая команда:]\n{text}"

    # В личке показываем статус, в группе — молчим до результата
    if not is_group:
        await update.message.reply_text("🤔 Analyzing command...", disable_notification=True)

    # Логируем входящий текст для дебага
    log.info(f"Incoming text (chat={chat_id}, user={user_id}): {text[:200]}")

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
        is_busy = _worker_busy or queue_size > 0
        if is_busy:
            position = queue_size + (1 if _worker_busy else 0) + 1
            await update.message.reply_text(f"⏳ Queued, position #{position}…", disable_notification=True)
        else:
            emoji = "🔍" if action.get("action") != "get_hours" else "🕐"
            await update.message.reply_text(f"{emoji} Checking…", disable_notification=True)
        await enqueue(_execute_get_task, context.bot, chat_id, action, text)
        return

    # Прайсы — выполняем без подтверждения, через очередь, без промежуточных сообщений
    if action.get("action") in ("add_revenue", "add_affiliate_revenue", "set_prices"):
        action["_user_command"] = text
        await enqueue(_execute_confirmed_task, context.bot, chat_id, action)
        return

    # bulk_schedule и multi_broker_task — запрашиваем подтверждение
    if action.get("action") in ("bulk_schedule", "multi_broker_task"):
        kb = [[
            InlineKeyboardButton("✅ Execute", callback_data="confirm"),
            InlineKeyboardButton("❌ Cancel",  callback_data="cancel"),
        ]]
        sent = await update.message.reply_text(
            build_confirm_text(action),
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(kb),
            disable_notification=True
        )
        action["_user_command"] = text
        pending[(chat_id, sent.message_id)] = action
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
    is_busy = _worker_busy or queue_size > 0
    if is_busy:
        position = queue_size + (1 if _worker_busy else 0) + 1
        await query.edit_message_text(f"⏳ Queued, position #{position}…")
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
    # Загружаем ротации из файла если есть
    _load_rotations()
    # Запускаем фоновый отчёт каждые 15 минут
    asyncio.create_task(_report_loop(application.bot))


def _load_rotations():
    """Загрузить ротации из JSON файла при старте."""
    import os
    path = "/root/auto-b2026/rotations_today.json"
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                data = json.load(f)
            # Поддерживаем два формата: просто ротации или {rotations, fired_started}
            if "rotations" in data:
                today_rotations.clear()
                today_rotations.update(data["rotations"])
                fired_started.clear()
                fired_started.update(data.get("fired_started", []))
            else:
                today_rotations.clear()
                today_rotations.update(data)
            log.info(f"Loaded {len(today_rotations)} rotations, {len(fired_started)} fired from {path}")
        except Exception as e:
            log.warning(f"Failed to load rotations: {e}")


def _save_rotations():
    """Сохранить ротации и fired_started в JSON файл."""
    path = "/root/auto-b2026/rotations_today.json"
    try:
        with open(path, "w") as f:
            json.dump({"rotations": today_rotations, "fired_started": list(fired_started)}, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.warning(f"Failed to save rotations: {e}")


REPORT_CHAT_ID = -5132784554  # Notifications чат

_COUNTRY_ISO = {
    "germany": "DE", "united kingdom": "GB", "uk": "GB", "australia": "AU",
    "france": "FR", "spain": "ES", "italy": "IT", "netherlands": "NL",
    "belgium": "BE", "switzerland": "CH", "austria": "AT", "sweden": "SE",
    "norway": "NO", "denmark": "DK", "finland": "FI", "portugal": "PT",
    "poland": "PL", "czech republic": "CZ", "hungary": "HU", "romania": "RO",
    "greece": "GR", "turkey": "TR", "israel": "IL", "canada": "CA",
    "united states": "US", "brazil": "BR", "mexico": "MX", "argentina": "AR",
    "colombia": "CO", "chile": "CL", "peru": "PE", "south africa": "ZA",
    "nigeria": "NG", "kenya": "KE", "ghana": "GH", "india": "IN",
    "indonesia": "ID", "malaysia": "MY", "singapore": "SG", "thailand": "TH",
    "vietnam": "VN", "philippines": "PH", "japan": "JP", "south korea": "KR",
    "new zealand": "NZ", "ukraine": "UA", "russia": "RU", "kazakhstan": "KZ",
    "united arab emirates": "AE", "saudi arabia": "SA", "egypt": "EG",
    "morocco": "MA", "croatia": "HR", "serbia": "RS", "slovakia": "SK",
    "slovenia": "SI", "bulgaria": "BG", "latvia": "LV", "lithuania": "LT",
    "estonia": "EE", "moldova": "MD", "georgia": "GE", "armenia": "AM",
}

# Флаги стран по ISO коду или полному названию
_COUNTRY_FLAGS = {
    "germany": "🇩🇪", "united kingdom": "🇬🇧", "uk": "🇬🇧", "australia": "🇦🇺",
    "france": "🇫🇷", "spain": "🇪🇸", "italy": "🇮🇹", "netherlands": "🇳🇱",
    "belgium": "🇧🇪", "switzerland": "🇨🇭", "austria": "🇦🇹", "sweden": "🇸🇪",
    "norway": "🇳🇴", "denmark": "🇩🇰", "finland": "🇫🇮", "portugal": "🇵🇹",
    "poland": "🇵🇱", "czech republic": "🇨🇿", "hungary": "🇭🇺", "romania": "🇷🇴",
    "greece": "🇬🇷", "turkey": "🇹🇷", "israel": "🇮🇱", "canada": "🇨🇦",
    "united states": "🇺🇸", "brazil": "🇧🇷", "mexico": "🇲🇽", "argentina": "🇦🇷",
    "colombia": "🇨🇴", "chile": "🇨🇱", "peru": "🇵🇪", "south africa": "🇿🇦",
    "nigeria": "🇳🇬", "kenya": "🇰🇪", "ghana": "🇬🇭", "india": "🇮🇳",
    "indonesia": "🇮🇩", "malaysia": "🇲🇾", "singapore": "🇸🇬", "thailand": "🇹🇭",
    "vietnam": "🇻🇳", "philippines": "🇵🇭", "japan": "🇯🇵", "south korea": "🇰🇷",
    "new zealand": "🇳🇿", "ukraine": "🇺🇦", "russia": "🇷🇺", "kazakhstan": "🇰🇿",
    "united arab emirates": "🇦🇪", "saudi arabia": "🇸🇦", "egypt": "🇪🇬",
    "morocco": "🇲🇦", "croatia": "🇭🇷", "serbia": "🇷🇸", "slovakia": "🇸🇰",
    "slovenia": "🇸🇮", "bulgaria": "🇧🇬", "latvia": "🇱🇻", "lithuania": "🇱🇹",
    "estonia": "🇪🇪", "moldova": "🇲🇩", "georgia": "🇬🇪", "armenia": "🇦🇲",
}

def _country_flag(country: str) -> str:
    """Вернуть флаг для страны."""
    if not country:
        return ""
    flag = _COUNTRY_FLAGS.get(country.lower(), "")
    if not flag:
        iso = _COUNTRY_ISO.get(country.lower(), country[:2].upper())
        try:
            flag = chr(0x1F1E6 + ord(iso[0]) - ord('A')) + chr(0x1F1E6 + ord(iso[1]) - ord('A'))
        except Exception:
            flag = ""
    return flag


def _country_iso(country: str) -> str:
    """Вернуть ISO код страны."""
    return _COUNTRY_ISO.get(country.lower(), country[:2].upper())


async def _fetch_last_funnel(affiliate_id: str, country: str) -> str:
    """Найти последний фанел аффилиата для страны. Пока не реализовано."""
    return ""


async def _fetch_first_lead(broker_name: str, aff_ids: list, country: str) -> str:
    """Получить email первого лида для ротации."""
    import aiohttp
    import urllib.parse

    if not _context:
        return ""

    cookies_list = await _context.cookies()
    cookies = {c["name"]: c["value"] for c in cookies_list}
    xsrf = cookies.get("XSRF-TOKEN", "")
    try:
        xsrf = urllib.parse.unquote(xsrf)
    except Exception:
        pass

    now = datetime.datetime.now()
    from_dt = now.strftime("%Y-%m-%d 00:00:00")
    to_dt = now.strftime("%Y-%m-%d 23:59:59")

    payload = {
        "from_datetime": from_dt,
        "to_datetime": to_dt,
        "timezone": "Europe/Istanbul",
        "test_leads": "exclude",
        "page": 1,
        "per_page": 100,
        "breakdowns": [],
        "trafficType": "all",
        "ord": [{"field": "id", "direction": "asc"}],  # первый лид = наименьший ID
        "filter": {"isGlobalSearch": False, "globalSearchValues": [], "search": "", "searchType": "single", "searchBy": "email", "converted": "all", "successful": "all", "queued": "all"},
        "fields": ["broker_name", "affiliate_name", "country", "email", "created_at", "affid", "broker_id"],
        "narrowDownAffiliate": None,
        "narrowDownCountry": None,
        "narrowDownBroker": None,
        "from_page": "stats",
    }

    headers = {
        "Content-Type": "application/json;charset=utf-8",
        "Accept": "application/json, text/plain, */*",
        "X-XSRF-TOKEN": xsrf,
        "Referer": f"{CRM_URL}/stats/details",
        "Origin": CRM_URL,
    }

    try:
        async with aiohttp.ClientSession(cookies=cookies) as session:
            async with session.post(
                f"{CRM_URL}/api/stats",
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=30),
                ssl=False
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    leads = data.get("data", []) if isinstance(data, dict) else data
                    # Фильтруем по affid из нашего списка аффов
                    aff_ids_int = [int(a) for a in aff_ids if str(a).isdigit()]
                    for lead in leads:
                        lead_affid = lead.get("affid")
                        lead_broker = lead.get("broker_name", "") or ""
                        lead_email = lead.get("email", "")
                        # Проверяем совпадение аффа
                        if lead_affid in aff_ids_int and lead_email:
                            # Проверяем совпадение брокера (частичное)
                            if broker_name.lower() in lead_broker.lower() or lead_broker.lower() in broker_name.lower():
                                log.info(f"First lead found: {lead_email} (aff {lead_affid}, broker {lead_broker})")
                                return lead_email
                    # Если точного совпадения по брокеру нет — берём первый лид с нужным аффом
                    for lead in leads:
                        if lead.get("affid") in aff_ids_int and lead.get("email"):
                            return lead.get("email", "")
    except Exception as e:
        log.warning(f"First lead fetch error: {e}")
    return ""
    """Запросить статистику из CRM API используя cookies браузера."""
    import aiohttp
    import urllib.parse

    if not _context:
        return []

    cookies_list = await _context.cookies()
    cookies = {c["name"]: c["value"] for c in cookies_list}

    # XSRF-TOKEN может быть URL-encoded
    xsrf = cookies.get("XSRF-TOKEN", "")
    try:
        xsrf = urllib.parse.unquote(xsrf)
    except Exception:
        pass

    now = datetime.datetime.now()
    from_dt = now.strftime("%Y-%m-%d 00:00:00")
    to_dt = now.strftime("%Y-%m-%d 23:59:59")

    payload = {
        "successfullLeadsOnly": False,
        "hideDuplicateFailedLeads": False,
        "from_datetime": from_dt,
        "to_datetime": to_dt,
        "timezone": "Europe/Istanbul",
        "group_by": group_by,
        "breakdowns": [],
        "from_page": "stats",
        "breakdown_request": True,
        "test_leads": "exclude",
        "trafficType": "all",
        "insideHoursOnly": False,
        "createdInsideDateRangeOnly": False,
        "aggregateFields": [
            {"key": "id", "show": True},
            {"key": "name", "show": True},
            {"key": "total_leads", "show": True},
            {"key": "successful_leads", "show": True},
        ],
    }

    headers = {
        "Content-Type": "application/json;charset=utf-8",
        "Accept": "application/json, text/plain, */*",
        "X-XSRF-TOKEN": xsrf,
        "Referer": f"{CRM_URL}/stats/analytics",
        "Origin": CRM_URL,
    }

    try:
        async with aiohttp.ClientSession(cookies=cookies) as session:
            async with session.post(
                f"{CRM_URL}/api/stats",
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=30),
                ssl=False
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    log.info(f"Stats API ({group_by}): {len(data)} records")
                    return data
                else:
                    text = await resp.text()
                    log.warning(f"Stats API returned {resp.status}: {text[:200]}")
                    return []
    except Exception as e:
        log.warning(f"Stats API error: {e}")
        return []


async def _fetch_all_leads_today() -> list:
    """Fetch all detailed leads for today via aiohttp (independent of page state)."""
    import aiohttp
    import urllib.parse

    if not _context:
        log.warning("_fetch_all_leads_today: no browser context")
        return []

    cookies_list = await _context.cookies()
    cookies = {c["name"]: c["value"] for c in cookies_list}
    xsrf = cookies.get("XSRF-TOKEN", "")
    try:
        xsrf = urllib.parse.unquote(xsrf)
    except Exception:
        pass

    now = datetime.datetime.now()
    from_dt = now.strftime("%Y-%m-%d 00:00:00")
    to_dt = now.strftime("%Y-%m-%d 23:59:59")

    payload = {
        "successfullLeadsOnly": False,
        "hideDuplicateFailedLeads": False,
        "from_datetime": from_dt,
        "to_datetime": to_dt,
        "timezone": "Europe/Istanbul",
        "test_leads": "exclude",
        "trafficType": "all",
        "page": 1,
        "per_page": 1000,
        "breakdowns": [],
        "ord": [{"field": "id", "direction": "asc"}],
        "filter": {"isGlobalSearch": False, "globalSearchValues": [], "search": "", "searchType": "single", "searchBy": "email", "converted": "all", "successful": "all", "queued": "all"},
        "fields": ["affid", "country", "broker_name", "email", "broker_id"],
        "narrowDownAffiliate": None,
        "narrowDownCountry": None,
        "narrowDownBroker": None,
        "from_page": "stats",
    }

    headers = {
        "Content-Type": "application/json;charset=utf-8",
        "Accept": "application/json, text/plain, */*",
        "X-XSRF-TOKEN": xsrf,
        "Referer": f"{CRM_URL}/stats/details",
        "Origin": CRM_URL,
    }

    try:
        async with aiohttp.ClientSession(cookies=cookies) as session:
            async with session.post(
                f"{CRM_URL}/api/stats",
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=30),
                ssl=False
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    leads = data.get("data", []) if isinstance(data, dict) else data
                    total = data.get("total", len(leads)) if isinstance(data, dict) else len(leads)
                    log.info(f"_fetch_all_leads_today: got {len(leads)} leads (total={total})")
                    return leads
                else:
                    text = await resp.text()
                    log.warning(f"_fetch_all_leads_today: HTTP {resp.status}: {text[:200]}")
    except Exception as e:
        log.warning(f"_fetch_all_leads_today error: {e}")
    return []


async def _fetch_crm_stats(group_by: str) -> list:
    """Запросить статистику из CRM API используя cookies браузера."""
    if not _page:
        return []

    now = datetime.datetime.now()
    from_dt = now.strftime("%Y-%m-%d 00:00:00")
    to_dt = now.strftime("%Y-%m-%d 23:59:59")

    payload = {
        "successfullLeadsOnly": False,
        "hideDuplicateFailedLeads": False,
        "from_datetime": from_dt,
        "to_datetime": to_dt,
        "timezone": "Europe/Istanbul",
        "group_by": group_by,
        "breakdowns": [],
        "from_page": "stats",
        "breakdown_request": True,
        "test_leads": "exclude",
        "trafficType": "all",
        "insideHoursOnly": False,
        "createdInsideDateRangeOnly": False,
        "aggregateFields": [
            {"key": "id", "show": True},
            {"key": "name", "show": True},
            {"key": "total_leads", "show": True},
            {"key": "successful_leads", "show": True},
        ],
    }

    try:
        payload_json = json.dumps(payload)
        result = await _page.evaluate(f"""async () => {{
            try {{
                const xsrf = document.cookie.split('; ').find(r => r.startsWith('XSRF-TOKEN='))?.split('=')[1];
                const decodedXsrf = xsrf ? decodeURIComponent(xsrf) : '';
                const resp = await fetch('/api/stats', {{
                    method: 'POST',
                    headers: {{
                        'Content-Type': 'application/json',
                        'Accept': 'application/json',
                        'X-XSRF-TOKEN': decodedXsrf
                    }},
                    body: {json.dumps(payload_json)}
                }});
                if (!resp.ok) return null;
                return await resp.json();
            }} catch(e) {{ return null; }}
        }}""")

        if result and isinstance(result, (list, dict)):
            data = result if isinstance(result, list) else result.get("data", [])
            log.info(f"Stats API ({group_by}): {len(data)} records")
            return data
    except Exception as e:
        log.warning(f"Stats API error ({group_by}): {e}")
    return []


async def _build_report() -> str:
    """Сформировать текст отчёта по лидам — только по активным ротациям.
    Один запрос за все лиды дня → клиент-сайд подсчёт по country + broker.
    Показывает ВСЕ аффы которые шлют лиды брокеру в эту страну.
    """
    if not today_rotations:
        return ""

    all_leads = await _fetch_all_leads_today()
    log.info(f"_build_report: {len(all_leads)} total leads fetched for client-side filtering")

    # DEBUG: показываем формат данных для отладки
    if all_leads:
        sample = all_leads[0]
        log.info(f"_build_report SAMPLE lead: country={sample.get('country')!r} broker={sample.get('broker_name')!r} affid={sample.get('affid')!r}")
    for bname, binfo in today_rotations.items():
        log.info(f"_build_report ROTATION: broker={bname!r} country={binfo.get('country')!r} affs={binfo.get('affs')!r}")

    now = datetime.datetime.utcnow() + datetime.timedelta(hours=3)
    time_str = now.strftime("%H:%M")
    lines = [f"📊 *Stats {time_str}*\n"]

    for broker_name, info in today_rotations.items():
        country = info.get("country", "")
        rotation_affs = info.get("affs", [])

        country_iso = _country_iso(country)
        flag = _country_flag(country)

        # Считаем ВСЕ лиды для broker+country, группируем по affid
        aff_counts = {}
        for lead in all_leads:
            # Проверяем страну
            lead_country = (lead.get("country") or "").lower()
            if country and country.lower() not in lead_country and lead_country not in country.lower():
                continue
            # Проверяем брокера (частичное совпадение)
            lead_broker = (lead.get("broker_name") or "").lower()
            if not (broker_name.lower() in lead_broker or lead_broker in broker_name.lower()):
                continue
            # Подходящий лид — считаем по affid
            aff_key = str(lead.get("affid", "?"))
            aff_counts[aff_key] = aff_counts.get(aff_key, 0) + 1

        total = sum(aff_counts.values())
        log.info(f"_build_report MATCH: broker={broker_name!r} country={country!r} → matched {total} leads, aff_counts={aff_counts}")
        lines.append(f"{flag} *{country_iso}* — {broker_name}")
        lines.append(f"  Leads: {total}")

        # Сначала аффы из ротации
        shown = set()
        for aff_id in rotation_affs:
            count = aff_counts.get(aff_id, 0)
            lines.append(f"    {aff_id} — {count}")
            shown.add(aff_id)

        # Потом остальные аффы (вне ротации) — если есть лиды
        for aff_id, count in sorted(aff_counts.items(), key=lambda x: -x[1]):
            if aff_id not in shown and count > 0:
                lines.append(f"    {aff_id} — {count} ⚠️")
                shown.add(aff_id)

        lines.append("")

    return "\n".join(lines).strip()


async def _report_loop(bot):
    """Фоновый цикл отправки отчётов каждые 5 минут с 08:00 до 20:00 GMT+3."""
    log.info("Report loop started.")
    last_midnight_swap = None
    while True:
        try:
            await asyncio.sleep(60)

            # Ensure browser is initialized (needed for aiohttp cookies)
            if not _context:
                try:
                    await get_page()
                    log.info("Report loop: browser initialized")
                except Exception as e:
                    log.warning(f"Report loop: browser init failed: {e}")
                    continue

            now = datetime.datetime.utcnow()
            local_hour = (now.hour + 3) % 24
            local_minute = now.minute
            today_date = (now + datetime.timedelta(hours=3)).date()

            # Midnight swap — при первой проверке нового дня
            if last_midnight_swap != today_date and local_hour == 0:
                last_midnight_swap = today_date
                today_rotations.clear()
                today_rotations.update(tomorrow_rotations)
                tomorrow_rotations.clear()
                fired_started.clear()
                log.info(f"Midnight swap: today_rotations now has {len(today_rotations)} entries")

            # Проверка "started" — каждую минуту без ограничения по времени
            if today_rotations:
                unfired = [k for k in today_rotations if k not in fired_started]
                if unfired:
                    all_leads = await _fetch_all_leads_today()

                    for broker_name in unfired:
                        info = today_rotations[broker_name]
                        country = info.get("country", "")

                        # Ищем хотя бы один лид для broker + country (любой афф)
                        found_lead = None
                        for lead in all_leads:
                            lead_country = (lead.get("country") or "").lower()
                            if country and country.lower() not in lead_country and lead_country not in country.lower():
                                continue
                            lead_broker = (lead.get("broker_name") or "").lower()
                            if not (broker_name.lower() in lead_broker or lead_broker in broker_name.lower()):
                                continue
                            found_lead = lead
                            break

                        if found_lead:
                            fired_started.add(broker_name)
                            _save_rotations()
                            flag = _country_flag(country)
                            country_iso = _country_iso(country)
                            aff_str = "/".join(info.get("affs", []))
                            lead_aff = str(found_lead.get("affid", "?"))
                            time_str = (datetime.datetime.utcnow() + datetime.timedelta(hours=3)).strftime("%H:%M")
                            msg = f"▶️ *STARTED*\n{broker_name} {flag}{country_iso}"
                            if aff_str:
                                msg += f" (aff {aff_str})"
                            # Если первый лид от аффа вне ротации — показываем
                            if lead_aff not in info.get("affs", []):
                                msg += f"\n⚠️ first lead from aff {lead_aff}"
                            msg += f" • {time_str}"
                            # Email первого лида — уже нашли его
                            first_email = found_lead.get("email", "")
                            if first_email:
                                msg += f"\n📧 {first_email}"
                            await bot.send_message(
                                REPORT_CHAT_ID,
                                msg,
                                parse_mode="Markdown",
                                disable_notification=False  # started — с уведомлением
                            )
                            log.info(f"Started notification: {broker_name} {country_iso}")

            # Только с 08:00 до 20:00 и каждые 15 минут
            if 8 <= local_hour < 20 and local_minute % 15 == 0:
                report = await _build_report()
                if report:
                    await bot.send_message(
                        REPORT_CHAT_ID,
                        report,
                        parse_mode="Markdown",
                        disable_notification=True
                    )
                    log.info(f"Report sent to {REPORT_CHAT_ID}")
                await asyncio.sleep(60)
        except Exception as e:
            log.warning(f"Report loop error: {e}")
            await asyncio.sleep(60)


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