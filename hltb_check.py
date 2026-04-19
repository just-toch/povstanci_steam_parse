"""
hltb_check.py — проверка доступности HowLongToBeat.
Используется как модуль из gui.py: from hltb_check import check_hltb
Возвращает dict с результатами каждого шага.
"""

import re
import json
import time
import requests
from fake_useragent import UserAgent

BASE_URL  = "https://howlongtobeat.com/"
TEST_GAME = "Portal 2"


def _ua():
    try:
        return UserAgent().random.strip()
    except Exception:
        return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"


def check_hltb(game: str = TEST_GAME) -> dict:
    """
    Проверяет доступность HLTB и делает тестовый поиск.
    Возвращает:
    {
        "ok": bool,
        "steps": [{"name": str, "ok": bool, "detail": str}, ...],
        "result": {"game_name": str, "main": float, "extra": float, "comp": float} | None,
        "error": str | None,
    }
    """
    steps  = []
    ua     = _ua()
    hdrs   = {"User-Agent": ua, "referer": BASE_URL}
    result = None
    error  = None

    def step(name, ok, detail):
        steps.append({"name": name, "ok": ok, "detail": detail})

    # ── ШАГ 1: доступность сайта ──────────────
    try:
        t = time.time()
        r = requests.get(BASE_URL, headers=hdrs, timeout=10)
        elapsed = time.time() - t

        if r.status_code == 403:
            step("Доступность сайта", False,
                 f"403 Forbidden за {elapsed:.2f}s — "
                 f"провайдер или IP заблокирован")
            return {"ok": False, "steps": steps, "result": None,
                    "error": "HLTB заблокирован (403). Попробуйте VPN или смените провайдера."}

        if r.status_code != 200:
            step("Доступность сайта", False,
                 f"HTTP {r.status_code} за {elapsed:.2f}s")
            return {"ok": False, "steps": steps, "result": None,
                    "error": f"Неожиданный статус {r.status_code}"}

        step("Доступность сайта", True, f"HTTP 200 за {elapsed:.2f}s")
        page_html = r.text

    except requests.exceptions.ConnectionError:
        step("Доступность сайта", False, "Нет соединения с howlongtobeat.com")
        return {"ok": False, "steps": steps, "result": None,
                "error": "Нет соединения. Проверьте интернет или DNS."}
    except requests.exceptions.Timeout:
        step("Доступность сайта", False, "Таймаут (10s)")
        return {"ok": False, "steps": steps, "result": None,
                "error": "Сайт не отвечает (таймаут)."}

    # ── ШАГ 2: поиск endpoint в скриптах ──────
    import re
    from bs4 import BeautifulSoup

    soup    = BeautifulSoup(page_html, "html.parser")
    scripts = [s["src"] for s in soup.find_all("script", src=True) if s.get("src")]
    pattern = re.compile(
        r'fetch\s*\(\s*["\']\/api\/([a-zA-Z0-9_/]+)[^"\']*["\']\s*,\s*{[^}]*method:\s*["\']POST["\'][^}]*}',
        re.DOTALL | re.IGNORECASE,
    )

    endpoint = None
    for src in scripts:
        url = BASE_URL + src if src.startswith("/") else src
        try:
            sr = requests.get(url, headers=hdrs, timeout=10)
            if sr.status_code != 200:
                continue
            m = pattern.search(sr.text)
            if m:
                endpoint = "/api/" + m.group(1).split("/")[0]
                break
        except Exception:
            continue

    if not endpoint:
        step("Поиск API endpoint", False, "Не найден ни в одном скрипте")
        return {"ok": False, "steps": steps, "result": None,
                "error": "Не удалось найти API endpoint — структура сайта изменилась."}

    step("Поиск API endpoint", True, endpoint)

    # ── ШАГ 3: auth-токен ─────────────────────
    try:
        t  = time.time()
        ar = requests.get(
            BASE_URL + endpoint + "/init",
            headers=hdrs,
            params={"t": int(time.time() * 1000)},
            timeout=10,
        )
        elapsed = time.time() - t

        if ar.status_code != 200:
            step("Auth-токен", False, f"HTTP {ar.status_code} за {elapsed:.2f}s")
            return {"ok": False, "steps": steps, "result": None,
                    "error": f"Auth-запрос вернул {ar.status_code}"}

        auth_data  = ar.json()
        auth_token = auth_data.get("token")
        auth_key   = auth_val = None
        for k, v in auth_data.items():
            if re.search(r"key", k, re.I):
                auth_key = v
            elif re.search(r"val", k, re.I):
                auth_val = v

        if not auth_token or not auth_key:
            step("Auth-токен", False, "Токен или ключ не найдены в ответе")
            return {"ok": False, "steps": steps, "result": None,
                    "error": "Не удалось получить auth-токен."}

        step("Auth-токен", True, f"получен за {elapsed:.2f}s")

    except Exception as e:
        step("Auth-токен", False, str(e))
        return {"ok": False, "steps": steps, "result": None, "error": str(e)}

    # ── ШАГ 4: тестовый поиск ─────────────────
    search_headers = {
        "content-type": "application/json",
        "accept":        "*/*",
        "User-Agent":    ua,
        "Referer":       BASE_URL,
        "Origin":        BASE_URL,
        "x-auth-token":  str(auth_token),
        "x-hp-key":      str(auth_key),
        "x-hp-val":      str(auth_val),
    }
    payload = json.dumps({
        "searchType": "games",
        "searchTerms": game.split(),
        "searchPage": 1,
        "size": 3,
        "searchOptions": {
            "games": {
                "userId": 0, "platform": "", "sortCategory": "popular",
                "rangeCategory": "main", "rangeTime": {"min": 0, "max": 0},
                "gameplay": {"perspective": "", "flow": "", "genre": "", "difficulty": ""},
                "rangeYear": {"max": "", "min": ""}, "modifier": "",
            },
            "users": {"sortCategory": "postcount"},
            "lists": {"sortCategory": "follows"},
            "filter": "", "sort": 0, "randomizer": 0,
        },
        "useCache": True,
        auth_key: auth_val,
    })

    try:
        t   = time.time()
        sr  = requests.post(BASE_URL + endpoint,
                            headers=search_headers, data=payload, timeout=10)
        elapsed = time.time() - t

        if sr.status_code != 200:
            step(f"Поиск «{game}»", False, f"HTTP {sr.status_code} за {elapsed:.2f}s")
            return {"ok": False, "steps": steps, "result": None,
                    "error": f"Поиск вернул {sr.status_code}"}

        games = sr.json().get("data", [])
        if not games:
            step(f"Поиск «{game}»", False, f"0 результатов за {elapsed:.2f}s")
            return {"ok": False, "steps": steps, "result": None,
                    "error": "Поиск вернул пустой список."}

        g = games[0]
        result = {
            "game_name": g.get("game_name", "?"),
            "main":  round(g.get("comp_main", 0) / 3600, 1),
            "extra": round(g.get("comp_plus", 0) / 3600, 1),
            "comp":  round(g.get("comp_100",  0) / 3600, 1),
        }
        step(f"Поиск «{game}»", True,
             f"{result['game_name']} — main {result['main']}h за {elapsed:.2f}s")

    except Exception as e:
        step(f"Поиск «{game}»", False, str(e))
        return {"ok": False, "steps": steps, "result": None, "error": str(e)}

    return {"ok": True, "steps": steps, "result": result, "error": None}


# ── CLI-режим ─────────────────────────────────
if __name__ == "__main__":
    print(f"Проверка HLTB...\n")
    res = check_hltb()
    for s in res["steps"]:
        icon = "✅" if s["ok"] else "❌"
        print(f"  {icon}  {s['name']}: {s['detail']}")
    print()
    if res["ok"]:
        r = res["result"]
        print(f"✅ HLTB работает! Результат для «{r['game_name']}»:")
        print(f"   Main {r['main']}h  |  Extra {r['extra']}h  |  100% {r['comp']}h")
    else:
        print(f"❌ {res['error']}")