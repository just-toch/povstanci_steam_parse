"""
Замена howlongtobeatpy — работает напрямую с HLTB API.
Подключается к parse.py вместо библиотеки.
"""

import re
import time
import json
import logging
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

log = logging.getLogger(__name__)

BASE_URL = "https://howlongtobeat.com/"
_cache: dict = {}  # endpoint + токен кешируются на сессию


def _get_user_agent() -> str:
    try:
        return UserAgent().random.strip()
    except Exception:
        return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"


def _fetch_search_endpoint(user_agent: str) -> str | None:
    """Находит актуальный /api/... endpoint в JS-скриптах сайта."""
    headers = {"User-Agent": user_agent, "referer": BASE_URL}
    try:
        r = requests.get(BASE_URL, headers=headers, timeout=15)
        if r.status_code != 200:
            log.warning(f"HLTB главная вернула {r.status_code}")
            return None
    except Exception as e:
        log.warning(f"HLTB недоступен: {e}")
        return None

    soup = BeautifulSoup(r.text, "html.parser")
    scripts = [s["src"] for s in soup.find_all("script", src=True)]

    # Сначала пробуем _app-* скрипты, потом все остальные
    app_scripts = [s for s in scripts if "_app-" in s]
    ordered = app_scripts + [s for s in scripts if s not in app_scripts]

    pattern = re.compile(
        r'fetch\s*\(\s*["\']\/api\/([a-zA-Z0-9_/]+)[^"\']*["\']\s*,\s*{[^}]*method:\s*["\']POST["\'][^}]*}',
        re.DOTALL | re.IGNORECASE,
    )

    for src in ordered:
        url = BASE_URL + src if src.startswith("/") else src
        try:
            sr = requests.get(url, headers=headers, timeout=15)
            if sr.status_code != 200:
                continue
            m = pattern.search(sr.text)
            if m:
                path = m.group(1).split("/")[0]
                endpoint = f"/api/{path}"
                log.debug(f"HLTB endpoint найден: {endpoint}")
                return endpoint
        except Exception:
            continue

    return None


def _fetch_auth_token(endpoint: str, user_agent: str) -> dict | None:
    """Получает x-auth-token, x-hp-key, x-hp-val."""
    headers = {"User-Agent": user_agent, "referer": BASE_URL}
    params = {"t": int(time.time() * 1000)}
    auth_url = BASE_URL + endpoint + "/init"
    try:
        r = requests.get(auth_url, headers=headers, params=params, timeout=15)
        if r.status_code != 200:
            log.warning(f"HLTB auth вернул {r.status_code}")
            return None
        data = r.json()
        auth_key = auth_value = None
        for k, v in data.items():
            if re.search(r"key", k, re.I):
                auth_key = v
            elif re.search(r"val", k, re.I):
                auth_value = v
        return {"token": data.get("token"), "key": auth_key, "value": auth_value}
    except Exception as e:
        log.warning(f"HLTB auth ошибка: {e}")
        return None


def _get_session_data() -> tuple[str, str, dict] | None:
    """
    Возвращает (user_agent, endpoint, auth) из кеша или загружает заново.
    Токены живут ~5 минут, поэтому кешируем с временем жизни.
    """
    now = time.time()
    if _cache.get("expires", 0) > now:
        return _cache["ua"], _cache["endpoint"], _cache["auth"]

    ua = _get_user_agent()
    endpoint = _fetch_search_endpoint(ua)
    if not endpoint:
        return None
    auth = _fetch_auth_token(endpoint, ua)
    if not auth:
        return None

    _cache.update({"ua": ua, "endpoint": endpoint, "auth": auth, "expires": now + 270})
    return ua, endpoint, auth


def search(game_name: str, size: int = 5) -> list[dict]:
    """
    Ищет игру на HLTB. Возвращает список словарей с полями:
      game_id, game_name, main_story, main_extra, completionist
    Возвращает [] если ничего не найдено или произошла ошибка.
    """
    session = _get_session_data()
    if session is None:
        log.warning("HLTB: не удалось получить сессию")
        return []

    ua, endpoint, auth = session

    headers = {
        "content-type": "application/json",
        "accept": "*/*",
        "User-Agent": ua,
        "Referer": BASE_URL,
        "Origin": BASE_URL,
        "x-auth-token": str(auth["token"]),
        "x-hp-key": str(auth["key"]),
        "x-hp-val": str(auth["value"]),
    }

    payload = {
        "searchType": "games",
        "searchTerms": game_name.split(),
        "searchPage": 1,
        "size": size,
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
        auth["key"]: auth["value"],
    }

    try:
        r = requests.post(
            BASE_URL + endpoint,
            headers=headers,
            data=json.dumps(payload),
            timeout=15,
        )
        if r.status_code != 200:
            log.warning(f"HLTB поиск вернул {r.status_code}")
            # Сбрасываем кеш — возможно, токен протух
            _cache.clear()
            return []

        games = r.json().get("data", [])
        return [
            {
                "game_id":      g.get("game_id"),
                "game_name":    g.get("game_name"),
                "main_story":   round(g.get("comp_main", 0) / 3600, 1) or None,
                "main_extra":   round(g.get("comp_plus", 0) / 3600, 1) or None,
                "completionist": round(g.get("comp_100", 0) / 3600, 1) or None,
            }
            for g in games
        ]
    except Exception as e:
        log.warning(f"HLTB поиск ошибка: {e}")
        _cache.clear()
        return []