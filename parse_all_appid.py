import logging
import time
import json
import os

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException

# ================== ЛОГИРОВАНИЕ ==================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("appid_collector.log", encoding="utf-8"),
    ]
)
log = logging.getLogger(__name__)

# ================== НАСТРОЙКИ ==================
BASE_URL = "https://store.steampowered.com/search/?ignore_preferences=1&ndl=1&start={start}"
RESULTS_PER_PAGE = 50
WAIT_TIMEOUT = 10
HEADLESS = True

OUTPUT_FILE = "steam_appids.json"
STATE_FILE = "collector_state.json"
COOKIES_FILE = "steam_cookies.json"

SAVE_EVERY = 1
MAX_RETRIES = 10
BROWSER_RESTART_DELAY = 5


# ================== СОСТОЯНИЕ ==================

def load_state():
    all_appids = set()
    last_page = 0

    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            all_appids = set(json.load(f))
        log.info(f"Загружено {len(all_appids)} AppID")

    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
        last_page = state.get("last_page", 0)
        log.info(f"Продолжаем со страницы {last_page + 1}")

    return all_appids, last_page


def save_state(all_appids, last_page):
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(sorted(all_appids), f, ensure_ascii=False, indent=2)

    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump({"last_page": last_page}, f)

    log.info(f"Сохранено {len(all_appids)} AppID (страница {last_page})")


# ================== COOKIES ==================

def load_cookies(driver):
    if not os.path.exists(COOKIES_FILE):
        log.warning("Файл cookies не найден — продолжаем без авторизации")
        return False

    with open(COOKIES_FILE, "r", encoding="utf-8") as f:
        cookies = json.load(f)

    driver.get("https://store.steampowered.com/")

    for cookie in cookies:
        cookie.pop("sameSite", None)
        try:
            driver.add_cookie(cookie)
        except Exception:
            pass

    driver.refresh()
    time.sleep(2)

    if "login" in driver.current_url:
        log.warning("Cookies не сработали (возможно, устарели)")
        return False

    log.info("Авторизация через cookies применена")
    return True


# ================== БРАУЗЕР ==================

def create_driver():
    options = Options()

    if HEADLESS:
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")

    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-crash-reporter")
    options.add_argument("--disable-extensions")
    options.add_argument("--log-level=3")
    options.add_argument("--disable-blink-features=AutomationControlled")

    driver = webdriver.Chrome(options=options)

    # 🔑 ВАЖНО: применяем cookies сразу после запуска
    load_cookies(driver)

    return driver


def is_driver_alive(driver):
    try:
        _ = driver.current_url
        return True
    except WebDriverException:
        return False


def quit_driver_safe(driver):
    try:
        driver.quit()
    except Exception:
        pass


# ================== ПАРСИНГ ==================

def parse_page_appids(page_source):
    soup = BeautifulSoup(page_source, "html.parser")
    divs = soup.select("a.search_result_row[data-ds-appid]")

    page_appids = set()

    for div in divs:
        raw = div.get("data-ds-appid", "")
        for part in raw.split(","):
            part = part.strip()
            if part.isdigit():
                page_appids.add(int(part))

    return page_appids


def load_page(driver, url):
    driver.get(url)

    WebDriverWait(driver, WAIT_TIMEOUT).until(
        EC.presence_of_element_located(
            (By.CSS_SELECTOR, "a.search_result_row[data-ds-appid]")
        )
    )

    return driver.page_source


# ================== СБОР ==================

def collect_appids():
    all_appids, start_page = load_state()
    page = start_page
    driver = create_driver()

    try:
        while True:
            start_index = page * RESULTS_PER_PAGE
            url = BASE_URL.format(start=start_index)

            page_source = None

            for attempt in range(1, MAX_RETRIES + 1):

                if not is_driver_alive(driver):
                    log.warning(f"Chrome упал, перезапуск (попытка {attempt})")
                    quit_driver_safe(driver)
                    time.sleep(BROWSER_RESTART_DELAY)
                    driver = create_driver()

                try:
                    page_source = load_page(driver, url)
                    break

                except WebDriverException as e:
                    log.warning(
                        f"Страница {page+1}, попытка {attempt} — WebDriverException"
                    )

                    quit_driver_safe(driver)
                    driver = create_driver()

                    if attempt < MAX_RETRIES:
                        time.sleep(BROWSER_RESTART_DELAY)

                except Exception as e:
                    log.warning(
                        f"Страница {page+1}, попытка {attempt}: {e}"
                    )

                    if attempt < MAX_RETRIES:
                        time.sleep(2)

            if page_source is None:
                log.error(f"Пропуск страницы {page+1}")
                page += 1
                continue

            page_appids = parse_page_appids(page_source)

            if not page_appids:
                log.info(f"Страница {page+1} пуста — конец выдачи")
                break

            before_count = len(all_appids)

            all_appids |= page_appids

            new_count = len(all_appids) - before_count

            log.info(
                f"[Страница {page + 1}] Найдено: {len(page_appids)}, новых: {new_count}, всего: {len(all_appids)}"
            )

            page += 1

            if page % SAVE_EVERY == 0:
                save_state(all_appids, page)

    except KeyboardInterrupt:
        log.info("Остановлено пользователем")

    finally:
        quit_driver_safe(driver)
        log.info("Браузер закрыт")

    save_state(all_appids, page)

    log.info(f"Всего собрано: {len(all_appids)}")

    if os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)


if __name__ == "__main__":
    collect_appids()