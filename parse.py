import os
import re
import sys
import logging
from collections import deque
from datetime import timedelta
import requests
import sqlite3
import time
import json
import random
from bs4 import BeautifulSoup


# ================== ПУТИ ==================

def _base_dir() -> str:
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))

def _app_path(f: str) -> str:
    return os.path.join(_base_dir(), f)

def _internal_path(f: str) -> str:
    if getattr(sys, "frozen", False):
        return os.path.join(sys._MEIPASS, f)
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), f)

_internal = _internal_path("")
if _internal not in sys.path:
    sys.path.insert(0, _internal)

import hltb_client


# ================== СТОП-ФЛАГ ==================
# Объявляем в самом начале — используется во всех функциях ниже

_GUI_STOP_EVENT = None

def _should_stop() -> bool:
    return _GUI_STOP_EVENT is not None and _GUI_STOP_EVENT.is_set()

class StopRequested(Exception):
    pass


# ================== ЛОГИРОВАНИЕ ==================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(_app_path("parser.log"), encoding="utf-8"),
    ]
)
log = logging.getLogger(__name__)


# ================== НАСТРОЙКИ ==================

MIN_APP_TIME = 3.0
MAX_RETRIES  = 3
SKIPPED_FILE = _app_path("skipped_appids.json")

if os.path.exists(SKIPPED_FILE):
    with open(SKIPPED_FILE, "r", encoding="utf-8") as f:
        skipped_appids = set(json.load(f))
else:
    skipped_appids = set()

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7"
}

AGE_COOKIES = {
    "birthtime": "568022401",
    "lastagecheckage": "1-0-1990"
}

RU_MONTHS = {
    "янв": "01", "фев": "02", "мар": "03", "апр": "04",
    "мая": "05", "май": "05", "июн": "06", "июл": "07",
    "авг": "08", "сен": "09", "окт": "10", "ноя": "11", "дек": "12"
}


# ================== БАЗА ДАННЫХ ==================

def init_databases():
    games_db     = sqlite3.connect(_app_path("games.db"))
    nongames_db  = sqlite3.connect(_app_path("nongames.db"))
    games_cur    = games_db.cursor()
    nongames_cur = nongames_db.cursor()

    games_cur.executescript("""
        CREATE TABLE IF NOT EXISTS games (
            appid INTEGER PRIMARY KEY,
            name TEXT,
            price_usd REAL,
            short_description TEXT,
            header_image TEXT,
            release_year INTEGER,
            release_month INTEGER,
            release_day INTEGER,
            total_reviews INTEGER,
            positive_reviews INTEGER,
            negative_reviews INTEGER,
            review_percent INTEGER,
            review_score INTEGER,
            hltb_main REAL,
            hltb_extra REAL,
            hltb_completion REAL,
            hltb_id INTEGER
        );
        CREATE TABLE IF NOT EXISTS tags_dict (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE
        );
        CREATE TABLE IF NOT EXISTS tags_games (
            appid INTEGER, tag_id INTEGER,
            PRIMARY KEY (appid, tag_id)
        );
        CREATE TABLE IF NOT EXISTS genres_dict (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE
        );
        CREATE TABLE IF NOT EXISTS genres_games (
            appid INTEGER, genre_id INTEGER,
            PRIMARY KEY (appid, genre_id)
        );
        CREATE TABLE IF NOT EXISTS categories_dict (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE
        );
        CREATE TABLE IF NOT EXISTS categories_games (
            appid INTEGER, category_id INTEGER,
            PRIMARY KEY (appid, category_id)
        );
        CREATE TABLE IF NOT EXISTS developers_dict (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE
        );
        CREATE TABLE IF NOT EXISTS developers_games (
            appid INTEGER, developer_id INTEGER,
            PRIMARY KEY (appid, developer_id)
        );
        CREATE TABLE IF NOT EXISTS publishers_dict (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE
        );
        CREATE TABLE IF NOT EXISTS publishers_games (
            appid INTEGER, publisher_id INTEGER,
            PRIMARY KEY (appid, publisher_id)
        );
        CREATE TABLE IF NOT EXISTS languages_dict (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE
        );
        CREATE TABLE IF NOT EXISTS languages_games (
            appid INTEGER, language_id INTEGER, full_audio INTEGER,
            PRIMARY KEY (appid, language_id)
        );
        CREATE TABLE IF NOT EXISTS parser_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            last_appid INTEGER,
            current_appid INTEGER
        );
        INSERT OR IGNORE INTO parser_state (id, last_appid, current_appid)
        VALUES (1, 0, NULL);
    """)

    nongames_cur.execute("""
        CREATE TABLE IF NOT EXISTS items (
            appid INTEGER PRIMARY KEY,
            name TEXT, type TEXT, appdetails_json TEXT
        )
    """)

    # Миграция старой БД
    try:
        games_cur.execute(
            "ALTER TABLE parser_state ADD COLUMN current_appid INTEGER")
        games_db.commit()
        log.info("Миграция БД: добавлена колонка current_appid")
    except Exception:
        pass  # колонка уже есть

    games_db.commit()
    nongames_db.commit()
    return games_db, games_cur, nongames_db, nongames_cur


# ================== STEAM API ==================

def get_appdetails(appid, lang="en"):
    log.info(f"[{appid}] Steam API (lang={lang})...")
    r = requests.get(
        "https://store.steampowered.com/api/appdetails",
        params={"appids": appid, "cc": "US", "l": lang},
        headers=HEADERS, timeout=10
    )
    r.raise_for_status()
    return r.json().get(str(appid), {})


def get_price_usd(data):
    price = data.get("price_overview")
    if not price:
        return None
    return price.get("final", 0) / 100


def get_tags(appid):
    log.info(f"[{appid}] Теги...")
    r = requests.get(
        f"https://store.steampowered.com/app/{appid}?l=russian",
        headers=HEADERS, cookies=AGE_COOKIES, timeout=10
    )
    if r.status_code != 200:
        return []
    return [t.get_text(strip=True)
            for t in BeautifulSoup(r.text, "html.parser").select("a.app_tag")]


def get_reviews_summary(appid):
    log.info(f"[{appid}] Отзывы...")
    r = requests.get(
        f"https://store.steampowered.com/appreviews/{appid}",
        params={"json": 1, "language": "all",
                "purchase_type": "all", "filter": "all"},
        headers=HEADERS, timeout=10
    )
    r.raise_for_status()
    s = r.json().get("query_summary", {})
    return (
        s.get("total_reviews", 0),
        s.get("total_positive", 0),
        s.get("total_negative", 0),
        s.get("review_score"),
    )


# ================== HLTB ==================

def get_hltb(game_name: str):
    if _should_stop():
        raise StopRequested()
    log.info(f"HLTB: поиск «{game_name}»...")
    t = time.time()
    try:
        clean = re.sub(r'[^A-Za-zА-Яа-я0-9 ]+', ' ', game_name).lower().strip()
        results = hltb_client.search(clean)
        elapsed = time.time() - t
        if not results:
            log.info(f"HLTB: не найдено ({elapsed:.2f}s)")
            return None, None, None, None
        r = results[0]
        log.info(
            f"HLTB: {r['game_name']} | "
            f"main={r['main_story']}h extra={r['main_extra']}h "
            f"100%={r['completionist']}h ({elapsed:.2f}s)"
        )
        return r["main_story"], r["main_extra"], r["completionist"], r["game_id"]
    except StopRequested:
        raise
    except KeyboardInterrupt:
        raise
    except Exception as e:
        log.warning(f"HLTB ошибка: {e} ({time.time()-t:.2f}s)")
        return None, None, None, None


# ================== ВСПОМОГАТЕЛЬНЫЕ ==================

def convert_release_date(date_str) -> tuple:
    if not date_str:
        return None, None, None
    try:
        date_str = date_str.lower().strip()
        date_str = re.sub(r'\s*г\.\s*$', '', date_str)
        parts = date_str.split()
        if len(parts) != 3:
            log.warning(f"Неожиданный формат даты: '{date_str}'")
            return None, None, None
        day, month_rus, year = parts
        month = RU_MONTHS.get(month_rus[:3])
        if not month:
            log.warning(f"Неизвестный месяц: '{month_rus}'")
            return None, None, None
        return int(year), int(month), int(day)
    except Exception as e:
        log.warning(f"Ошибка конвертации даты '{date_str}': {e}")
        return None, None, None


def parse_supported_languages(raw: str) -> dict:
    if not raw:
        return {}
    raw = re.sub(r"<.*?>", "", raw)
    raw = re.sub(r"\*?\s*languages with full audio support.*$", "", raw,
                 flags=re.IGNORECASE)
    languages = {}
    for part in [p.strip() for p in raw.split(",") if p.strip()]:
        if part.endswith("*"):
            languages[part[:-1].strip()] = True
        else:
            languages[part] = False
    return languages


def format_eta(seconds):
    if seconds < 0:
        return "0s"
    return str(timedelta(seconds=int(seconds)))


def random_test_appids(appids, n=5):
    test_ids = appids.copy()
    random.shuffle(test_ids)
    return test_ids[:n]


def retry_call(func, *args, retries=MAX_RETRIES, delay=2, appid=None, label=""):
    for attempt in range(1, retries + 1):
        if _should_stop():
            raise StopRequested()
        try:
            return func(*args)
        except StopRequested:
            raise
        except KeyboardInterrupt:
            raise
        except Exception as e:
            log.warning(f"[!] {label} ошибка (попытка {attempt}/{retries}): {e}")
            if attempt < retries:
                for _ in range(delay):
                    if _should_stop():
                        raise StopRequested()
                    time.sleep(1)
            else:
                if appid is not None:
                    skipped_appids.add(appid)
                    with open(SKIPPED_FILE, "w", encoding="utf-8") as f:
                        json.dump(sorted(skipped_appids), f,
                                  ensure_ascii=False, indent=2)
                raise


# ================== СОСТОЯНИЕ ПАРСЕРА ==================

def get_parser_state(games_cur):
    """Возвращает (last_appid, current_appid)."""
    games_cur.execute(
        "SELECT last_appid, current_appid FROM parser_state WHERE id=1")
    row = games_cur.fetchone()
    return (row[0] or 0), row[1]


def set_current_appid(games_db, games_cur, appid):
    """Записывает appid ДО начала обработки — checkpoint."""
    games_cur.execute(
        "UPDATE parser_state SET current_appid=? WHERE id=1", (appid,))
    games_db.commit()


def set_last_processed_appid(games_db, games_cur, appid):
    """Записывает appid ПОСЛЕ успешной обработки, сбрасывает current."""
    games_cur.execute(
        "UPDATE parser_state SET last_appid=?, current_appid=NULL WHERE id=1",
        (appid,))
    games_db.commit()


# ================== ОБРАБОТКА ==================

def process_app(appid, games_db, games_cur, nongames_db, nongames_cur):
    start = time.time()

    app  = retry_call(get_appdetails, appid, appid=appid, label="Steam EN")
    data = app.get("data", {})
    item_type = data.get("type")
    name      = data.get("name")
    log.info(f"[{appid}] {name!r} type={item_type!r}")

    if not app.get("success"):
        nongames_cur.execute("INSERT OR REPLACE INTO items VALUES (?,?,?,?)",
                             (appid, name, item_type, None))
        nongames_db.commit()
        return

    if item_type != "game":
        nongames_cur.execute("INSERT OR REPLACE INTO items VALUES (?,?,?,?)",
                             (appid, name, item_type,
                              json.dumps(data, ensure_ascii=False)))
        nongames_db.commit()
        return

    app_ru  = retry_call(get_appdetails, appid, "ru",
                         appid=appid, label="Steam RU")
    data_ru = app_ru.get("data", {})

    price = get_price_usd(data)
    tags  = retry_call(get_tags, appid, appid=appid, label="Steam tags")

    total_reviews, positive_reviews, negative_reviews, review_score = retry_call(
        get_reviews_summary, appid, appid=appid, label="Steam reviews"
    )

    if data.get("release_date", {}).get("coming_soon"):
        release_year = release_month = release_day = None
        hltb_main = hltb_extra = hltb_completion = hltb_id = None
    else:
        release_year, release_month, release_day = convert_release_date(
            data_ru.get("release_date", {}).get("date")
        )
        hltb_main, hltb_extra, hltb_completion, hltb_id = get_hltb(name)

    review_percent = (
        int(positive_reviews / total_reviews * 100) if total_reviews else None
    )

    languages = parse_supported_languages(data.get("supported_languages"))

    games_cur.execute("""
        INSERT OR REPLACE INTO games
        (appid, name, price_usd, short_description, header_image,
         release_year, release_month, release_day,
         total_reviews, positive_reviews, negative_reviews,
         review_percent, review_score,
         hltb_main, hltb_extra, hltb_completion, hltb_id)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        appid, name, price,
        data_ru.get("short_description"),
        data.get("header_image"),
        release_year, release_month, release_day,
        total_reviews, positive_reviews, negative_reviews,
        review_percent, review_score,
        hltb_main, hltb_extra, hltb_completion, hltb_id,
    ))

    for lang, has_audio in languages.items():
        games_cur.execute(
            "INSERT OR IGNORE INTO languages_dict (name) VALUES (?)", (lang,))
        games_cur.execute("""
            INSERT OR REPLACE INTO languages_games (appid, language_id, full_audio)
            VALUES (?, (SELECT id FROM languages_dict WHERE name=?), ?)
        """, (appid, lang, 1 if has_audio else 0))

    for cat in {c["description"].strip()
                for c in data.get("categories", []) if c.get("description")}:
        games_cur.execute(
            "INSERT OR IGNORE INTO categories_dict (name) VALUES (?)", (cat,))
        games_cur.execute("""
            INSERT OR IGNORE INTO categories_games (appid, category_id)
            VALUES (?, (SELECT id FROM categories_dict WHERE name=?))
        """, (appid, cat))

    for genre in {g["description"].strip()
                  for g in data.get("genres", []) if g.get("description")}:
        games_cur.execute(
            "INSERT OR IGNORE INTO genres_dict (name) VALUES (?)", (genre,))
        games_cur.execute("""
            INSERT OR IGNORE INTO genres_games (appid, genre_id)
            VALUES (?, (SELECT id FROM genres_dict WHERE name=?))
        """, (appid, genre))

    for tag in tags:
        games_cur.execute(
            "INSERT OR IGNORE INTO tags_dict (name) VALUES (?)", (tag,))
        games_cur.execute("""
            INSERT OR IGNORE INTO tags_games (appid, tag_id)
            VALUES (?, (SELECT id FROM tags_dict WHERE name=?))
        """, (appid, tag))

    for dev in data.get("developers", []):
        games_cur.execute(
            "INSERT OR IGNORE INTO developers_dict (name) VALUES (?)", (dev,))
        games_cur.execute("""
            INSERT OR IGNORE INTO developers_games (appid, developer_id)
            VALUES (?, (SELECT id FROM developers_dict WHERE name=?))
        """, (appid, dev))

    for pub in data.get("publishers", []):
        games_cur.execute(
            "INSERT OR IGNORE INTO publishers_dict (name) VALUES (?)", (pub,))
        games_cur.execute("""
            INSERT OR IGNORE INTO publishers_games (appid, publisher_id)
            VALUES (?, (SELECT id FROM publishers_dict WHERE name=?))
        """, (appid, pub))

    games_db.commit()
    set_last_processed_appid(games_db, games_cur, appid)

    elapsed = time.time() - start
    if elapsed < MIN_APP_TIME:
        time.sleep(MIN_APP_TIME - elapsed)


# ================== ЗАПУСК ==================

def run():
    """Вызывается из GUI в потоке или напрямую через __main__."""
    games_db, games_cur, nongames_db, nongames_cur = init_databases()

    appids_path = _app_path("steam_appids.json")
    if not os.path.exists(appids_path):
        log.error(f"Файл не найден: {appids_path}")
        return

    with open(appids_path, "r", encoding="utf-8") as f:
        appids = sorted(set(json.load(f)))

    last_appid, current_appid = get_parser_state(games_cur)
    if current_appid:
        log.info(
            f"Прошлый сеанс прерван на AppID={current_appid}, перезапускаем его")
        appids = [a for a in appids if a >= current_appid]
    elif last_appid:
        log.info(f"Продолжаем с appid > {last_appid}")
        appids = [a for a in appids if a > last_appid]

    total = len(appids)
    processed_times = deque(maxlen=200)
    log.info(f"Всего к обработке: {total}")

    try:
        for idx, appid in enumerate(appids, 1):
            if _should_stop():
                log.info("Остановлено пользователем")
                break

            app_start = time.time()
            log.info(
                f"\n=== {idx}/{total} AppID={appid} ({idx/total*100:.1f}%) ===")
            set_current_appid(games_db, games_cur, appid)
            try:
                process_app(appid, games_db, games_cur, nongames_db, nongames_cur)
                status = "Готово"
            except StopRequested:
                log.info("Остановлено пользователем")
                break
            except Exception as e:
                status = f"Ошибка: {e}"

            elapsed = time.time() - app_start
            processed_times.append(elapsed)
            avg = sum(processed_times) / len(processed_times)
            eta = avg * (total - idx)
            log.info(
                f"[{appid}] {status} | "
                f"{elapsed:.2f}s | avg {avg:.2f}s | "
                f"осталось {total-idx} | ETA {format_eta(eta)}"
            )
    except KeyboardInterrupt:
        log.info("Прервано пользователем")
    finally:
        games_db.close()
        nongames_db.close()
        log.info("БД закрыты")


if __name__ == "__main__":
    run()