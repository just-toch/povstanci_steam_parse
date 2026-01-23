from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import math
import json
import os

# ================== НАСТРОЙКИ ==================
BASE_URL = "https://store.steampowered.com/search/?query=&start={start}"
RESULTS_PER_PAGE = 50
WAIT_TIMEOUT = 10
HEADLESS = True
OUTPUT_FILE = "steam_appids.json"
SAVE_EVERY = 1
MAX_RETRIES = 3

# ================== БРАУЗЕР ==================
options = Options()
if HEADLESS:
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")

driver = webdriver.Chrome(options=options)

# ================== ЗАГРУЗКА СУЩЕСТВУЮЩЕГО JSON ==================
all_appids = []
if os.path.exists(OUTPUT_FILE):
    with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
        all_appids = json.load(f)
    print(f"[INFO] Загружено {len(all_appids)} AppID из {OUTPUT_FILE}")

# ================== СБОР ==================
start_time = time.time()
page = len(all_appids) // RESULTS_PER_PAGE

while True:
    start_index = page * RESULTS_PER_PAGE
    url = BASE_URL.format(start=start_index)
    success = False

    for attempt in range(MAX_RETRIES):
        try:
            driver.get(url)
            WebDriverWait(driver, WAIT_TIMEOUT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a.search_result_row[data-ds-appid]"))
            )
            success = True
            break
        except Exception as e:
            print(f"[!] Ошибка на странице {page+1}, попытка {attempt+1}: {e}")
            time.sleep(2)

    if not success:
        print(f"[!] Страница {page+1} не загрузилась после {MAX_RETRIES} попыток, пропускаем")
        page += 1
        continue

    soup = BeautifulSoup(driver.page_source, "html.parser")
    divs = soup.select("a.search_result_row[data-ds-appid]")
    page_appids = []

    for div in divs:
        raw = div.get("data-ds-appid", "")
        for part in raw.split(","):
            part = part.strip()
            if part.isdigit():
                page_appids.append(int(part))

    if len(page_appids) == 0:
        print(f"Страница {page+1} пуста, достигнут конец")
        break

    all_appids.extend(page_appids)

    print(f"[{page+1}] Страница обработана, AppID собрано: {len(page_appids)}")

    if (page + 1) % SAVE_EVERY == 0:
        all_appids = sorted(set(all_appids))
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(all_appids, f, ensure_ascii=False, indent=2)
        print(f"Сохранено ({len(all_appids)} AppID)")

    page += 1

# ================== СОХРАНЕНИЕ ФИНАЛЬНО ==================
driver.quit()

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(all_appids, f, ensure_ascii=False, indent=2)

print(f"\nВсего AppID собрано: {len(all_appids)}")

