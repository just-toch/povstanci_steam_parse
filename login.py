import json
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# ================== НАСТРОЙКИ ==================
LOGIN_URL = "https://store.steampowered.com/login/"
COOKIES_FILE = "steam_cookies.json"

# ================== БРАУЗЕР ==================
options = Options()

# ВАЖНО: headless = False
options.add_argument("--disable-gpu")
options.add_argument("--window-size=1280,900")
options.add_argument("--disable-blink-features=AutomationControlled")

driver = webdriver.Chrome(options=options)

# ================== ЛОГИН ==================
driver.get(LOGIN_URL)

print("\n[INFO] Открыл страницу логина Steam")
print("[INFO] Войди ВРУЧНУЮ:")
print(" - логин")
print(" - пароль")
print(" - Steam Guard")
print("\nПосле успешного входа нажми ENTER в консоли\n")

input(">>> ")

# небольшая пауза на догрузку
time.sleep(3)

# ================== ПРОВЕРКА ==================
current_url = driver.current_url
if "login" in current_url:
    print("[ERROR] Похоже, логин не завершён")
    driver.quit()
    exit(1)

print("[INFO] Логин подтверждён")

# ================== СОХРАНЕНИЕ COOKIES ==================
cookies = driver.get_cookies()

with open(COOKIES_FILE, "w", encoding="utf-8") as f:
    json.dump(cookies, f, ensure_ascii=False, indent=2)

print(f"[SUCCESS] Cookies сохранены в {COOKIES_FILE}")
print(f"[INFO] Всего cookies: {len(cookies)}")

driver.quit()
