import os
import re
from datetime import timedelta
import requests
import sqlite3
import time
import json
import random
from bs4 import BeautifulSoup
from howlongtobeatpy import HowLongToBeat

# ================== НАСТРОЙКИ ==================
with open('steam_api.json', 'r', encoding='utf-8') as f:
    STEAM_API_KEY = json.load(f)
MIN_APP_TIME = 3.0
MAX_RETRIES = 3
SKIPPED_FILE = "skipped_appids.json"
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

hltb = HowLongToBeat()
RU_MONTHS = {
    "янв": "01",
    "фев": "02",
    "мар": "03",
    "апр": "04",
    "мая": "05",
    "май": "05",
    "июн": "06",
    "июл": "07",
    "авг": "08",
    "сен": "09",
    "окт": "10",
    "ноя": "11",
    "дек": "12"
}

# ================== БАЗЫ ==================

games_db = sqlite3.connect("games.db")
nongames_db = sqlite3.connect("nongames.db")

games_cur = games_db.cursor()
nongames_cur = nongames_db.cursor()

games_cur.execute("""
CREATE TABLE IF NOT EXISTS games (
    appid INTEGER PRIMARY KEY,
    name TEXT,
    price_usd REAL,
    short_description TEXT,
    header_image TEXT,
    developers TEXT,
    publishers TEXT,
    release_date TEXT,
    supported_languages TEXT,
    total_reviews INTEGER,
    positive_reviews INTEGER,
    negative_reviews INTEGER,
    review_score TEXT,
    hltb_main REAL,
    hltb_extra REAL,
    hltb_completion REAL,
    hltb_id INTEGER
)
""")

games_cur.execute("""
CREATE TABLE IF NOT EXISTS game_categories (
    appid INTEGER,
    category TEXT,
    PRIMARY KEY (appid, category),
    FOREIGN KEY (appid) REFERENCES games(appid)
)
""")

games_cur.execute("""
CREATE TABLE IF NOT EXISTS game_genres (
    appid INTEGER,
    genre TEXT,
    PRIMARY KEY (appid, genre),
    FOREIGN KEY (appid) REFERENCES games(appid)
)
""")

games_cur.execute("""
CREATE TABLE IF NOT EXISTS game_tags (
    appid INTEGER,
    tag TEXT,
    PRIMARY KEY (appid, tag),
    FOREIGN KEY (appid) REFERENCES games(appid)
)
""")

games_cur.execute("""
CREATE TABLE IF NOT EXISTS parser_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    last_appid INTEGER
)
""")

games_cur.execute("""
INSERT OR IGNORE INTO parser_state (id, last_appid)
VALUES (1, 0)
""")

nongames_cur.execute("""
CREATE TABLE IF NOT EXISTS items (
    appid INTEGER PRIMARY KEY,
    name TEXT,
    type TEXT,
    appdetails_json TEXT
)
""")

games_db.commit()
nongames_db.commit()

# ================== STEAM ==================

def get_appdetails(appid):
    print(f"[{appid}]")
    print("Парсим Steam API...")
    url = "https://store.steampowered.com/api/appdetails"
    params = {
        "appids": appid,
        "cc": "US",
        "l": "en"
    }
    r = requests.get(url, params=params, headers=HEADERS, timeout=10)
    r.raise_for_status()
    data = r.json()
    return data.get(str(appid), {})

def get_appdetails_ru(appid):
    print(f"[{appid}]")
    url = "https://store.steampowered.com/api/appdetails"
    params = {
        "appids": appid,
        "cc": "US",
        "l": "ru"
    }
    r = requests.get(url, params=params, headers=HEADERS, timeout=10)
    r.raise_for_status()
    data = r.json()
    return data.get(str(appid), {})

def get_price_usd(data):
    price = data.get("price_overview")
    if not price:
        return None
    return price.get("final", 0) / 100

def get_tags(appid):
    print("Парсим теги...")
    url = f"https://store.steampowered.com/app/{appid}?l=russian"
    r = requests.get(url, headers=HEADERS, cookies=AGE_COOKIES, timeout=10)
    if r.status_code != 200:
        return []
    soup = BeautifulSoup(r.text, "html.parser")
    return [t.get_text(strip=True) for t in soup.select("a.app_tag")]

def get_reviews_summary(appid):
    print("Парсим отзывы Steam...")
    url = f"https://store.steampowered.com/appreviews/{appid}"
    params = {
        "json": 1,
        "language": "all",
        "purchase_type": "all",
        "filter": "all"
    }
    r = requests.get(url, params=params, headers=HEADERS, timeout=10)
    r.raise_for_status()
    data = r.json()
    summary = data.get("query_summary", {})
    return (
        summary.get("total_reviews", 0),
        summary.get("total_positive", 0),
        summary.get("total_negative", 0),
        summary.get("review_score_desc")
    )

# ================== HLTB ==================



def get_hltb(game_name):
    start_hltb = time.time()
    print(f"Парсим HLTB...")
    try:
        clean_name = re.sub(r'[^A-Za-zА-Яа-я0-9 ]+', ' ', game_name).lower().strip()
        results = hltb.search(clean_name)
        if not results:
            return None, None, None, None
        r = results[0]
        end_hltb = time.time()
        print(end_hltb - start_hltb)
        return r.main_story, r.main_extra, r.completionist, r.game_id

    except KeyboardInterrupt:
        raise

    except Exception:
        end_hltb = time.time()
        print(end_hltb - start_hltb)
        return None, None, None, None


# ================== ОБРАБОТКА ==================

def process_app(appid):
    start = time.time()

    app = retry_call(
        get_appdetails,
        appid,
        appid=appid,
        label="Steam appdetails"
    )
    app_ru = retry_call(
        get_appdetails_ru,
        appid,
        appid=appid,
        label="Steam appdetails"
    )
    data = app.get("data", {})
    data_ru = app_ru.get("data", {})
    item_type = data.get("type")
    name = data.get("name")
    print(f"[{name}]")

    if not app.get("success"):
        nongames_cur.execute(
            "INSERT OR REPLACE INTO items VALUES (?,?,?,?)",
            (appid, name, item_type, None)
        )
        nongames_db.commit()
        return

    if item_type != "game":
        name = data.get("name")
        nongames_cur.execute(
            "INSERT OR REPLACE INTO items VALUES (?,?,?,?)",
            (appid, name, item_type, json.dumps(data, ensure_ascii=False))
        )
        nongames_db.commit()
        return

    price = get_price_usd(data)
    tags = retry_call(
        get_tags,
        appid,
        appid=appid,
        label="Steam tags"
    )

    for tag in tags:
        games_cur.execute('INSERT INTO game_tags (appid, tag) VALUES (?,?)',
                          (appid, tag))

    total_reviews, positive_reviews, negative_reviews, review_score = retry_call(
        get_reviews_summary,
        appid,
        appid=appid,
        label="Steam reviews"
    )

    if data.get("release_date", {}).get('coming_soon'):
        release_date = 'Coming soon'
        hltb_main, hltb_extra, hltb_completion, hltb_id = None, None, None, None
    else:
        release_date = convert_release_date(data_ru.get("release_date", {}).get("date"))
        hltb_main, hltb_extra, hltb_completion, hltb_id = get_hltb(name)

    supported_languages = data.get("supported_languages")

    games_cur.execute("""
    INSERT OR REPLACE INTO games
    (appid, name, price_usd, short_description, header_image, developers, publishers, release_date,
     supported_languages,
     total_reviews, positive_reviews, negative_reviews, review_score,
     hltb_main, hltb_extra, hltb_completion, hltb_id)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        appid,
        name,
        price,
        data_ru.get("short_description"),
        data.get("header_image"),
        ", ".join(data.get("developers", [])),
        ", ".join(data.get("publishers", [])),
        release_date,
        supported_languages,
        total_reviews,
        positive_reviews,
        negative_reviews,
        review_score,
        hltb_main,
        hltb_extra,
        hltb_completion,
        hltb_id,
        # json.dumps(data, ensure_ascii=False)
    ))

    # Категории
    categories = {
        c["description"].strip()
        for c in data.get("categories", [])
        if c.get("description")
    }

    games_cur.executemany(
        'INSERT OR IGNORE INTO game_categories (appid, category) VALUES (?, ?)',
        [(appid, cat) for cat in categories]
    )

    # Жанры
    genres = {
        g["description"].strip()
        for g in data.get("genres", [])
        if g.get("description")
    }

    games_cur.executemany(
        'INSERT OR IGNORE INTO game_genres (appid, genre) VALUES (?, ?)',
        [(appid, genre) for genre in genres]
    )

    games_db.commit()
    set_last_processed_appid(appid)

    elapsed = time.time() - start
    if elapsed < MIN_APP_TIME:
        time.sleep(MIN_APP_TIME - elapsed)

def get_last_processed_appid():
    games_cur.execute("SELECT last_appid FROM parser_state WHERE id=1")
    return games_cur.fetchone()[0]

def set_last_processed_appid(appid):
    games_cur.execute(
        "UPDATE parser_state SET last_appid=? WHERE id=1",
        (appid,)
    )
    games_db.commit()

def retry_call(func, *args, retries=MAX_RETRIES, delay=2, appid=None, label=""):
    for attempt in range(1, retries + 1):
        try:
            return func(*args)

        except KeyboardInterrupt:
            raise

        except Exception as e:
            print(f"[!] {label} ошибка (попытка {attempt}/{retries}): {e}")
            if attempt < retries:
                time.sleep(delay)
            else:
                if appid is not None:
                    skipped_appids.add(appid)
                    with open(SKIPPED_FILE, "w", encoding="utf-8") as f:
                        json.dump(sorted(skipped_appids), f, ensure_ascii=False, indent=2)
                raise


# ================== ЗАПУСК ==================

def random_test_appids(n=5):
    test_ids = appids
    random.shuffle(test_ids)
    return test_ids[:n]


def format_eta(seconds):
    if seconds < 0:
        return "0s"
    return str(timedelta(seconds=int(seconds)))


def convert_release_date(date_str: str) -> str:
    date_str = date_str.lower().strip()
    date_str = re.sub(r'\s*г\.\s*$', '', date_str)
    parts = date_str.split()
    day, month_rus, year = parts
    month = RU_MONTHS.get(month_rus[:3])
    return f"{year}-{month}-{int(day):02d}"


processed_times = []
start_all = None

if __name__ == "__main__":
    FILE = "steam_appids.json"
    with open(FILE, 'r', encoding='utf-8') as f:
        appids = json.load(f)
    appids = sorted(set(appids))

    last_appid = get_last_processed_appid()

    if last_appid:
        print(f"Продолжаем с appid > {last_appid}")
        appids = [a for a in appids if a > last_appid]

    # appids = [1599660]
    appids = random_test_appids(150)
    total = len(appids)

    start_all = time.time()

    try:
        for idx, appid in enumerate(appids, 1):
            app_start = time.time()
            try:
                print(f"\n=== Обработка {idx}/{total} AppID ({idx/total*100:.1f}%) ===")
                process_app(appid)
                status = "Готово"
            except Exception as e:
                status = f"Ошибка: {e}"
            elapsed = time.time() - app_start
            processed_times.append(elapsed)
            avg_time = sum(processed_times) / len(processed_times)
            remaining = total - idx
            eta_seconds = avg_time * remaining
            print(f"[{appid}] {status}")
            print(
                f"Время: {elapsed:.2f}s | "
                f"Среднее: {avg_time:.2f}s | "
                f"Осталось: {remaining} | "
                f"ETA: {format_eta(eta_seconds)}"
            )
    except KeyboardInterrupt:
        print("Прервано пользователем")