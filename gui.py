"""
Steam Parser GUI — запуск: python gui.py
Лежит в той же папке, что parse.py и games.db
"""

import json
import os
import queue
import re
import signal
import sqlite3
import sys
import threading
import tkinter as tk
from tkinter import ttk, messagebox
import io
import threading
import urllib.request
import webbrowser

# ═══════════════════════════════════════════════
#  ЦВЕТА И ШРИФТЫ
# ═══════════════════════════════════════════════
C_BG      = "#0f0f13"
C_PANEL   = "#16161d"
C_CARD    = "#1c1c26"
C_BORDER  = "#2a2a3a"
C_ACCENT  = "#7c6af7"
C_ACCENT2 = "#4ecdc4"
C_TEXT    = "#e8e8f0"
C_MUTED   = "#7a7a9a"
C_SUCCESS = "#4ade80"
C_WARN    = "#fbbf24"
C_DANGER  = "#f87171"
C_LIVE    = "#22c55e"
C_HEAD_BG = "#22222f"   # фон заголовков таблицы
C_HEAD_FG = "#b0b0cc"   # текст заголовков таблицы

FONT_MONO  = ("Consolas", 9)
FONT_BODY  = ("Segoe UI", 10)
FONT_BOLD  = ("Segoe UI Semibold", 10)
FONT_TITLE = ("Segoe UI Semibold", 13)
FONT_SMALL = ("Segoe UI", 8)
FONT_CAP   = ("Consolas", 8)

def _base_dir() -> str:
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))

def _app_path(f: str) -> str:
    """Файлы данных — рядом с exe/скриптом (БД, json)."""
    return os.path.join(_base_dir(), f)

def _internal_path(f: str) -> str:
    """Bundled-модули — внутри sys._MEIPASS или рядом со скриптом."""
    if getattr(sys, "frozen", False):
        return os.path.join(sys._MEIPASS, f)
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), f)

DB_PATH = _app_path("games.db")
PARSER_SCRIPT = "parse.py"
MAX_LOG       = 600


# ═══════════════════════════════════════════════
#  БД
# ═══════════════════════════════════════════════
def db_connect():
    if not os.path.exists(DB_PATH):
        return None
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def db_stats():
    conn = db_connect()
    if not conn:
        return {}
    cur = conn.cursor()
    d = {}
    try:
        cur.execute("SELECT COUNT(*) FROM games");             d["games"]     = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM games WHERE hltb_main IS NOT NULL")
        d["with_hltb"] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM games WHERE price_usd = 0")
        d["free"]      = cur.fetchone()[0]
        cur.execute("SELECT last_appid FROM parser_state WHERE id=1")
        row = cur.fetchone();  d["last_appid"] = row[0] if row else 0
        cur.execute("SELECT COUNT(*) FROM tags_dict"); d["tags"] = cur.fetchone()[0]
    except Exception:
        pass
    conn.close()
    return d


def db_search(q="", sort="total_reviews", asc=False, limit=200, offset=0):
    conn = db_connect()
    if not conn:
        return []
    cur  = conn.cursor()
    allowed = {"appid", "total_reviews", "hltb_main", "price_usd",
               "review_percent", "release_year", "name"}
    if sort not in allowed:
        sort = "total_reviews"
    order = "ASC" if asc else "DESC"
    where  = "WHERE name LIKE ?" if q else ""
    params = [f"%{q}%"] if q else []
    try:
        cur.execute(f"""
            SELECT appid, name, price_usd, release_year,
                   total_reviews, review_percent, review_score,
                   hltb_main, hltb_extra, hltb_completion
            FROM games {where}
            ORDER BY {sort} {order}
            LIMIT ? OFFSET ?
        """, params + [limit, offset])
        rows = [dict(r) for r in cur.fetchall()]
    except Exception:
        rows = []
    conn.close()
    return rows


def db_game_detail(appid):
    conn = db_connect()
    if not conn:
        return {}
    cur = conn.cursor()
    cur.execute("SELECT * FROM games WHERE appid=?", (appid,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return {}
    g = dict(row)
    for tbl, col, jtbl, jcol in [
        ("tags_dict",       "tags",       "tags_games",       "tag_id"),
        ("genres_dict",     "genres",     "genres_games",     "genre_id"),
        ("developers_dict", "developers", "developers_games", "developer_id"),
        ("publishers_dict", "publishers", "publishers_games", "publisher_id"),
    ]:
        cur.execute(f"""
            SELECT d.name FROM {tbl} d
            JOIN {jtbl} j ON d.id=j.{jcol}
            WHERE j.appid=?
        """, (appid,))
        g[col] = [r[0] for r in cur.fetchall()]
    conn.close()
    return g


# ═══════════════════════════════════════════════
#  ГЛАВНОЕ ОКНО
# ═══════════════════════════════════════════════
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Steam Parser")
        self.geometry("1240x820")
        self.minsize(960, 620)
        self.configure(bg=C_BG)
        # Кириллица: явно задаём кодировку для stdout при subprocess
        self._proc       = None
        self._logq       = queue.Queue()
        self._running    = False
        self._stop_event = threading.Event()

        self._styles()
        self._build()
        self._refresh_stats()
        self._poll()

    # ── стили ───────────────────────────────────
    def _styles(self):
        s = ttk.Style(self)
        s.theme_use("clam")

        s.configure(".", background=C_BG, foreground=C_TEXT,
                    fieldbackground=C_CARD, troughcolor=C_PANEL,
                    selectbackground=C_ACCENT, selectforeground="#fff",
                    bordercolor=C_BORDER, darkcolor=C_BG, lightcolor=C_BG,
                    font=FONT_BODY)

        s.configure("TNotebook", background=C_BG, borderwidth=0, tabmargins=[0,0,0,0])
        s.configure("TNotebook.Tab", background=C_PANEL, foreground=C_MUTED,
                    padding=[20,9], font=FONT_BOLD, borderwidth=0)
        s.map("TNotebook.Tab",
              background=[("selected", C_CARD)],
              foreground=[("selected", C_TEXT)])

        # Treeview — строки
        s.configure("Treeview",
                    background=C_CARD, foreground=C_TEXT,
                    fieldbackground=C_CARD, rowheight=30,
                    borderwidth=0, font=FONT_BODY)
        # Заголовки таблицы: явный тёмный фон + светлый текст
        s.configure("Treeview.Heading",
                    background=C_HEAD_BG, foreground=C_HEAD_FG,
                    relief="flat", font=FONT_BOLD,
                    borderwidth=0)
        s.map("Treeview.Heading",
              background=[("active", C_BORDER), ("pressed", C_BORDER)],
              foreground=[("active", C_TEXT)])
        s.map("Treeview",
              background=[("selected", C_ACCENT)],
              foreground=[("selected", "#fff")])

        s.configure("TScrollbar", background=C_BORDER,
                    troughcolor=C_PANEL, borderwidth=0, arrowsize=0)
        s.configure("TEntry", fieldbackground=C_CARD, foreground=C_TEXT,
                    bordercolor=C_BORDER, insertcolor=C_TEXT, padding=6)
        s.configure("TCombobox", fieldbackground=C_CARD, foreground=C_TEXT,
                    bordercolor=C_BORDER, selectbackground=C_CARD,
                    selectforeground=C_TEXT)
        s.map("TCombobox",
              fieldbackground=[("readonly", C_CARD)],
              foreground=[("readonly", C_TEXT)],
              selectbackground=[("readonly", C_CARD)])

    # ── вёрстка ─────────────────────────────────
    def _build(self):
        # Шапка
        hdr = tk.Frame(self, bg=C_PANEL, height=52)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)
        tk.Label(hdr, text="STEAM PARSER", bg=C_PANEL, fg=C_ACCENT,
                 font=("Consolas", 13, "bold")).pack(side="left", padx=20, pady=12)
        self._dot = tk.Label(hdr, text="●", bg=C_PANEL, fg=C_MUTED, font=FONT_BODY)
        self._dot.pack(side="right", padx=(0,8), pady=12)
        self._hdr_lbl = tk.Label(hdr, text="Парсер не запущен",
                                  bg=C_PANEL, fg=C_MUTED, font=FONT_SMALL)
        self._hdr_lbl.pack(side="right", padx=(0,12), pady=12)

        nb = ttk.Notebook(self)
        nb.pack(fill="both", expand=True)

        t1 = tk.Frame(nb, bg=C_BG)
        t2 = tk.Frame(nb, bg=C_BG)
        nb.add(t1, text="  ▶  Парсер  ")
        nb.add(t2, text="  ⊞  Библиотека  ")

        self._build_parser(t1)
        self._build_browse(t2)

    # ══════════════════════════════════════════
    #  ВКЛАДКА: ПАРСЕР
    # ══════════════════════════════════════════
    def _build_parser(self, parent):
        left = tk.Frame(parent, bg=C_PANEL, width=270)
        left.pack(side="left", fill="y")
        left.pack_propagate(False)

        def cap(text):
            tk.Label(left, text=text, bg=C_PANEL, fg=C_MUTED,
                     font=FONT_CAP).pack(anchor="w", padx=20, pady=(18,6))

        def sep():
            tk.Frame(left, bg=C_BORDER, height=1).pack(fill="x", padx=20, pady=10)

        cap("УПРАВЛЕНИЕ")

        self._btn_start = self._btn(left, "▶  Запустить парсер",
                                     C_ACCENT, "#fff", self._start)
        self._btn_start.pack(fill="x", padx=20, pady=(0,6))

        self._btn_stop = self._btn(left, "■  Остановить",
                                    C_CARD, C_MUTED, self._stop, state="disabled")
        self._btn_stop.pack(fill="x", padx=20)

        sep()
        cap("СТАТИСТИКА БД")

        self._stat_vars = {}
        for key, label in [("games","Игр в базе"), ("with_hltb","С HLTB"),
                            ("last_appid","Последний AppID")]:
            row = tk.Frame(left, bg=C_PANEL)
            row.pack(fill="x", padx=20, pady=2)
            tk.Label(row, text=label, bg=C_PANEL, fg=C_MUTED,
                     font=FONT_SMALL, anchor="w").pack(side="left")
            v = tk.StringVar(value="—")
            self._stat_vars[key] = v
            tk.Label(row, textvariable=v, bg=C_PANEL, fg=C_TEXT,
                     font=FONT_BOLD, anchor="e").pack(side="right")

        sep()
        cap("ПРОГРЕСС")

        # Текущая игра
        self._prog_game = tk.Label(left, text="Ожидание...", bg=C_PANEL, fg=C_TEXT,
                                    font=FONT_SMALL, wraplength=225, justify="left",
                                    anchor="w")
        self._prog_game.pack(anchor="w", padx=20, pady=(0,6))

        # Прогресс-бар (canvas — чтобы точно покрасить)
        pb_frame = tk.Frame(left, bg=C_BORDER, height=6, bd=0)
        pb_frame.pack(fill="x", padx=20, pady=(0,6))
        pb_frame.pack_propagate(False)
        self._pb_bg = tk.Frame(pb_frame, bg=C_BORDER)
        self._pb_bg.place(relwidth=1, relheight=1)
        self._pb_fill = tk.Frame(pb_frame, bg=C_ACCENT)
        self._pb_fill.place(relwidth=0, relheight=1)

        # Строка: idx/total  pct%
        row_pct = tk.Frame(left, bg=C_PANEL)
        row_pct.pack(fill="x", padx=20, pady=(0,4))
        self._prog_idx = tk.Label(row_pct, text="—", bg=C_PANEL, fg=C_MUTED,
                                   font=FONT_SMALL, anchor="w")
        self._prog_idx.pack(side="left")
        self._prog_pct = tk.Label(row_pct, text="", bg=C_PANEL, fg=C_ACCENT,
                                   font=FONT_BOLD, anchor="e")
        self._prog_pct.pack(side="right")

        # ETA
        row_eta = tk.Frame(left, bg=C_PANEL)
        row_eta.pack(fill="x", padx=20, pady=(0,2))
        tk.Label(row_eta, text="ETA", bg=C_PANEL, fg=C_MUTED,
                 font=FONT_SMALL, anchor="w").pack(side="left")
        self._prog_eta = tk.Label(row_eta, text="—", bg=C_PANEL, fg=C_WARN,
                                   font=FONT_BOLD, anchor="e")
        self._prog_eta.pack(side="right")

        # Среднее время / игру
        row_avg = tk.Frame(left, bg=C_PANEL)
        row_avg.pack(fill="x", padx=20, pady=(0,2))
        tk.Label(row_avg, text="Ср. время/игру", bg=C_PANEL, fg=C_MUTED,
                 font=FONT_SMALL, anchor="w").pack(side="left")
        self._prog_avg = tk.Label(row_avg, text="—", bg=C_PANEL, fg=C_MUTED,
                                   font=FONT_SMALL, anchor="e")
        self._prog_avg.pack(side="right")

        # Кнопка обновления БД внизу
        sep()
        self._btn(left, "↻  Обновить статистику",
                  C_CARD, C_MUTED, self._refresh_stats,
                  hover_bg=C_BORDER).pack(fill="x", padx=20, pady=(0,6))

        sep()
        cap("HLTB")

        # Строка статуса HLTB
        hltb_row = tk.Frame(left, bg=C_PANEL)
        hltb_row.pack(fill="x", padx=20, pady=(0,6))
        tk.Label(hltb_row, text="Статус:", bg=C_PANEL, fg=C_MUTED,
                 font=FONT_SMALL, anchor="w").pack(side="left")
        self._hltb_status = tk.Label(hltb_row, text="не проверено",
                                      bg=C_PANEL, fg=C_MUTED, font=FONT_BOLD, anchor="e")
        self._hltb_status.pack(side="right")

        self._hltb_detail = tk.Label(left, text="", bg=C_PANEL, fg=C_MUTED,
                                      font=FONT_SMALL, wraplength=225, justify="left", anchor="w")
        self._hltb_detail.pack(anchor="w", padx=20, pady=(0,6))

        self._btn_hltb = self._btn(left, "⟳  Проверить HLTB",
                                    C_CARD, C_MUTED, self._check_hltb,
                                    hover_bg=C_BORDER)
        self._btn_hltb.pack(fill="x", padx=20, pady=(0,20))

        # Правая часть — лог
        right = tk.Frame(parent, bg=C_BG)
        right.pack(fill="both", expand=True)

        tk.Label(right, text="ЛОГ", bg=C_BG, fg=C_MUTED,
                 font=FONT_CAP).pack(anchor="w", padx=16, pady=(16,4))

        box = tk.Frame(right, bg=C_CARD)
        box.pack(fill="both", expand=True, padx=16, pady=(0,16))

        self._log = tk.Text(box, bg=C_CARD, fg=C_TEXT, font=FONT_MONO,
                             wrap="word", relief="flat", bd=0,
                             insertbackground=C_TEXT,
                             selectbackground=C_ACCENT,
                             selectforeground="#fff",
                             state="disabled", padx=12, pady=10,
                             cursor="arrow")
        self._log.pack(side="left", fill="both", expand=True)
        sb = ttk.Scrollbar(box, command=self._log.yview)
        sb.pack(side="right", fill="y")
        self._log["yscrollcommand"] = sb.set

        # Оставляем стандартные биндинги выделения мышью,
        # добавляем только Ctrl+C/A и блокировку ввода текста
        self._log.bind("<Control-c>",  self._log_copy)
        self._log.bind("<Control-C>",  self._log_copy)
        self._log.bind("<Control-a>",  self._log_select_all)
        self._log.bind("<Control-A>",  self._log_select_all)
        # Блокируем любой ввод с клавиатуры (кроме обработанных выше)
        self._log.bind("<Key>", lambda e: "break")

        # Контекстное меню
        self._log_menu = tk.Menu(self._log, tearoff=0,
                                  bg=C_CARD, fg=C_TEXT,
                                  activebackground=C_ACCENT,
                                  activeforeground="#fff",
                                  relief="flat", bd=1,
                                  font=FONT_BODY)
        self._log_menu.add_command(label="Копировать",    command=self._log_copy)
        self._log_menu.add_command(label="Выделить всё",  command=self._log_select_all)
        self._log_menu.add_separator()
        self._log_menu.add_command(label="Очистить лог",  command=self._log_clear)
        self._log.bind("<Button-3>", self._log_context)

        self._log.tag_configure("warn",    foreground=C_WARN)
        self._log.tag_configure("err",     foreground=C_DANGER)
        self._log.tag_configure("ok",      foreground=C_SUCCESS)
        self._log.tag_configure("section", foreground=C_ACCENT2)
        self._log.tag_configure("dim",     foreground=C_MUTED)
        self._log.tag_configure("sel",     background=C_ACCENT, foreground="#fff")

    # ══════════════════════════════════════════
    #  ВКЛАДКА: БИБЛИОТЕКА
    # ══════════════════════════════════════════
    def _build_browse(self, parent):
        # Панель фильтров
        top = tk.Frame(parent, bg=C_PANEL, height=56)
        top.pack(fill="x")
        top.pack_propagate(False)

        def lbl(text):
            tk.Label(top, text=text, bg=C_PANEL, fg=C_MUTED,
                     font=FONT_BODY).pack(side="left", padx=(14,4), pady=14)

        lbl("Поиск:")
        self._q = tk.StringVar()
        self._q.trace_add("write", lambda *_: self._search())
        ttk.Entry(top, textvariable=self._q, width=26).pack(side="left", pady=14)

        lbl("Сортировка:")
        # Человекочитаемые метки → ключи БД
        self._sort_options = {
            "Отзывы":      "total_reviews",
            "AppID":       "appid",
            "Название":    "name",
            "Цена":        "price_usd",
            "Год":         "release_year",
            "Рейтинг %":  "review_percent",
            "HLTB Main":  "hltb_main",
        }
        self._sort_label = tk.StringVar(value="Отзывы")
        cb = ttk.Combobox(top, textvariable=self._sort_label,
                           values=list(self._sort_options.keys()),
                           state="readonly", width=13)
        cb.pack(side="left", pady=14)
        cb.bind("<<ComboboxSelected>>", lambda _: self._search())

        self._asc = tk.BooleanVar()
        tk.Checkbutton(top, text="↑ По возрастанию",
                       variable=self._asc,
                       bg=C_PANEL, fg=C_MUTED,
                       selectcolor=C_CARD,
                       activebackground=C_PANEL, activeforeground=C_TEXT,
                       font=FONT_BODY, command=self._search
                       ).pack(side="left", padx=10)

        self._cnt = tk.Label(top, text="", bg=C_PANEL, fg=C_MUTED, font=FONT_SMALL)
        self._cnt.pack(side="right", padx=16)

        # Кнопка обновления таблицы
        self._btn(top, "↻ Обновить", C_CARD, C_MUTED, self._search,
                  hover_bg=C_BORDER, pady=5
                  ).pack(side="right", padx=(0,8), pady=10)

        # Таблица
        cols   = ("appid","name","price","year","reviews","pct","rating","hm","he","h100")
        heads  = ("AppID","Название","Цена $","Год","Отзывы","% +","Оценка","Main ч","Extra ч","100% ч")
        widths = (75, 280, 72, 55, 90, 60, 120, 75, 75, 75)

        self._tree = ttk.Treeview(parent, columns=cols, show="headings",
                                   selectmode="browse")
        for col, h, w in zip(cols, heads, widths):
            self._tree.heading(col, text=h,
                               command=lambda c=col: self._hdr_click(c))
            self._tree.column(col, width=w, anchor="center", minwidth=40)
        self._tree.column("name", anchor="w")

        self._tree.tag_configure("odd",  background=C_CARD)
        self._tree.tag_configure("even", background="#191921")
        self._tree.tag_configure("sel",  background=C_ACCENT, foreground="#fff")

        vsb = ttk.Scrollbar(parent, orient="vertical",   command=self._tree.yview)
        hsb = ttk.Scrollbar(parent, orient="horizontal", command=self._tree.xview)
        self._tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        hsb.pack(side="bottom", fill="x")
        vsb.pack(side="right",  fill="y")
        self._tree.pack(fill="both", expand=True)
        self._tree.bind("<Double-1>", self._detail)

        # Текущий ключ сортировки (для стрелок в заголовке)
        self._active_sort_col = "reviews"  # col id
        self._search()

    # ── хелпер: кнопка ──────────────────────────
    def _btn(self, parent, text, bg, fg, cmd,
             state="normal", hover_bg=None, pady=10):
        b = tk.Button(parent, text=text, bg=bg, fg=fg,
                      relief="flat", font=FONT_BOLD, cursor="hand2",
                      activebackground=hover_bg or bg,
                      activeforeground=fg if hover_bg else "#fff",
                      pady=pady, state=state, command=cmd)
        if hover_bg:
            b.bind("<Enter>", lambda _: b.configure(bg=hover_bg, fg=C_TEXT))
            b.bind("<Leave>", lambda _: b.configure(bg=bg, fg=fg))
        return b

    # ── парсер: запуск/стоп ─────────────────────
    def _start(self):
        if self._running:
            return
        # Добавляем папку с модулями в sys.path (для exe: sys._MEIPASS)
        internal = _internal_path("")
        if internal not in sys.path:
            sys.path.insert(0, internal)
        try:
            import parse as parser_module
        except ImportError as e:
            messagebox.showerror("Ошибка", f"parse.py не найден: {e}")
            return
        self._stop_event.clear()
        self._running = True
        self._set_live(True)
        threading.Thread(
            target=self._run_parser,
            args=(parser_module,),
            daemon=True
        ).start()

    def _stop(self):
        self._stop_event.set()

    def _run_parser(self, parser_module):
        """Запускает парсер в потоке, перехватывает логи через logging."""
        import logging

        class QueueHandler(logging.Handler):
            def __init__(self, q):
                super().__init__()
                self.q = q
            def emit(self, record):
                self.q.put(self.format(record))

        handler = QueueHandler(self._logq)
        handler.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(message)s",
            datefmt="%H:%M:%S"
        ))

        root_logger = logging.getLogger()
        root_logger.addHandler(handler)

        try:
            # Передаём stop_event в парсер — он проверяет его в retry_call и get_hltb
            parser_module._GUI_STOP_EVENT = self._stop_event
            parser_module.run()
        except Exception as e:
            self._logq.put(f"[ERROR] Парсер завершился с ошибкой: {e}")
        finally:
            parser_module._GUI_STOP_EVENT = None
            root_logger.removeHandler(handler)
            self._logq.put(None)

    # Regex для парсинга строки вида:
    # === Обработка 42/1000 AppID=730 (4.2%) ===
    _RE_IDX = re.compile(r'=== .* (\d+)/(\d+) .*\((\d+\.\d+)%\)')
    # строка вида: [730] Готово | 3.21s | avg 4.10s | осталось 958 | ETA 1:05:22
    _RE_ETA = re.compile(r'avg (\S+)s.*ETA (.+)$')

    def _poll(self):
        try:
            while True:
                item = self._logq.get_nowait()
                if item is None:
                    self._running = False
                    self._proc    = None
                    self._set_live(False)
                    self._log_add("── завершено ──", "dim")
                    self._refresh_stats()
                    self._prog_game.configure(text="Завершено", fg=C_SUCCESS)
                    self._prog_eta.configure(text="—")
                else:
                    self._log_add(item)
                    self._parse_progress(item)
        except queue.Empty:
            pass
        self.after(120, self._poll)

    def _parse_progress(self, line):
        # Строка заголовка: текущий индекс и процент
        m = self._RE_IDX.search(line)
        if m:
            cur, total, pct = int(m.group(1)), int(m.group(2)), float(m.group(3))
            self._prog_idx.configure(text=f"{cur:,} / {total:,}")
            self._prog_pct.configure(text=f"{pct:.1f}%")
            self._pb_fill.place(relwidth=pct/100)
            # Название игры — слово после AppID= в той же строке
            gm = re.search(r'AppID=(\d+)', line)
            if gm:
                self._prog_game.configure(
                    text=f"AppID {gm.group(1)}", fg=C_TEXT)
            return

        # Строка с ETA и средним временем
        m = self._parse_eta_line(line)
        if m:
            avg, eta = m
            self._prog_avg.configure(text=f"{avg}s")
            self._prog_eta.configure(text=eta)
            return

        # Название игры из строки "[appid] 'Название'"  или "Название: ..."
        if line.startswith("[") and "]" in line:
            name = line.split("]", 1)[-1].strip()
            if name and len(name) < 80:
                self._prog_game.configure(text=name, fg=C_TEXT)

    def _parse_eta_line(self, line):
        """Парсит строку: [730] Готово | 3.21s | avg 4.10s | осталось 958 | ETA 1:05:22"""
        m = self._RE_ETA.search(line)
        if m:
            return m.group(1), m.group(2).strip()
        # Формат из logging: ... avg 4.10s | осталось 958 | ETA 1:05:22
        m2 = re.search(r'avg\s+(\S+)s.*ETA\s+(.+)$', line)
        if m2:
            return m2.group(1), m2.group(2).strip()
        return None

    def _log_add(self, text, force=None):
        self._log.configure(state="normal")
        if force:
            tag = force
        elif "[WARNING]" in text or "[!]" in text:
            tag = "warn"
        elif "[ERROR]" in text or "Ошибка" in text:
            tag = "err"
        elif "Готово" in text or "OK" in text:
            tag = "ok"
        elif "===" in text:
            tag = "section"
        else:
            tag = ""
        self._log.insert("end", text + "\n", tag)
        self._log.see("end")
        self._log.configure(state="disabled")

    # ── HLTB проверка ──────────────────────────
    def _check_hltb(self):
        self._btn_hltb.configure(state="disabled", text="Проверяю...", fg=C_MUTED)
        self._hltb_status.configure(text="...", fg=C_MUTED)
        self._hltb_detail.configure(text="")
        threading.Thread(target=self._run_hltb_check, daemon=True).start()

    def _run_hltb_check(self):
        try:
            from hltb_check import check_hltb
            res = check_hltb()
        except ImportError as e:
            self.after(0, lambda: self._hltb_done(None, f"hltb_check.py не найден: {e}"))
            return
        except Exception as e:
            self.after(0, lambda: self._hltb_done(None, str(e)))
            return
        self.after(0, lambda: self._hltb_done(res))
    def _hltb_done(self, res, import_err=None):
        self._btn_hltb.configure(state="normal", text="⟳  Проверить HLTB", fg=C_MUTED)
        if import_err:
            self._hltb_status.configure(text="ошибка", fg=C_DANGER)
            self._hltb_detail.configure(text=import_err, fg=C_DANGER)
            return

        if res["ok"]:
            r = res["result"]
            self._hltb_status.configure(text="доступен ✓", fg=C_SUCCESS)
            self._hltb_detail.configure(
                text=f"{r['game_name']}: {r['main']}h / {r['extra']}h / {r['comp']}h",
                fg=C_MUTED)
        else:
            self._hltb_status.configure(text="недоступен ✗", fg=C_DANGER)
            self._hltb_detail.configure(text=res["error"], fg=C_WARN)

        # Показываем шаги в лог
        self._log_add("── Проверка HLTB ──", "section")
        for s in res["steps"]:
            tag  = "ok" if s["ok"] else "err"
            icon = "✓" if s["ok"] else "✗"
            self._log_add(f"  {icon}  {s['name']}: {s['detail']}", tag)
        if res["error"]:
            self._log_add(f"  → {res['error']}", "warn")

    # ── лог: выделение и копирование ──────────
    def _log_copy(self, e=None):
        try:
            text = self._log.get("sel.first", "sel.last")
        except tk.TclError:
            text = self._log.get("1.0", "end").rstrip()
        if text:
            self.clipboard_clear()
            self.clipboard_append(text)

    def _log_select_all(self, e=None):
        self._log.configure(state="normal")
        self._log.tag_add("sel", "1.0", "end")
        self._log.configure(state="disabled")

    def _log_clear(self):
        self._log.configure(state="normal")
        self._log.delete("1.0", "end")
        self._log.configure(state="disabled")

    def _log_context(self, e):
        try:
            self._log_menu.tk_popup(e.x_root, e.y_root)
        finally:
            self._log_menu.grab_release()

    def _set_live(self, on):
        if on:
            self._dot.configure(fg=C_LIVE)
            self._hdr_lbl.configure(text="Парсер работает…", fg=C_LIVE)
            self._btn_start.configure(state="disabled", bg=C_BORDER, fg=C_MUTED)
            self._btn_stop.configure(state="normal",   bg=C_DANGER, fg="#fff")
            # Сбрасываем прогресс при старте
            self._prog_game.configure(text="Запуск...", fg=C_MUTED)
            self._prog_idx.configure(text="—")
            self._prog_pct.configure(text="")
            self._prog_eta.configure(text="—")
            self._prog_avg.configure(text="—")
            self._pb_fill.place(relwidth=0)
        else:
            self._dot.configure(fg=C_MUTED)
            self._hdr_lbl.configure(text="Парсер не запущен", fg=C_MUTED)
            self._btn_start.configure(state="normal",   bg=C_ACCENT, fg="#fff")
            self._btn_stop.configure(state="disabled",  bg=C_CARD,   fg=C_MUTED)

    # ── статистика ──────────────────────────────
    def _refresh_stats(self):
        d = db_stats()
        for k, v in self._stat_vars.items():
            val = d.get(k, "—")
            v.set(f"{val:,}" if isinstance(val, int) else str(val))

    # ── таблица ────────────────────────────────
    # Маппинг col_id → ключ БД
    _COL_DB = {
        "appid":   "appid",
        "name":    "name",
        "price":   "price_usd",
        "year":    "release_year",
        "reviews": "total_reviews",
        "pct":     "review_percent",
        "hm":      "hltb_main",
        "he":      "hltb_extra",
        "h100":    "hltb_completion",
    }

    def _search(self, *_):
        sort_label = self._sort_label.get()
        sort_key   = self._sort_options.get(sort_label, "total_reviews")
        rows = db_search(q=self._q.get(), sort=sort_key, asc=self._asc.get())
        self._tree.delete(*self._tree.get_children())
        for i, r in enumerate(rows):
            price = f"${r['price_usd']:.2f}" if r["price_usd"] else "Free"
            pct   = f"{r['review_percent']}%" if r["review_percent"] is not None else ""
            hm    = f"{r['hltb_main']:.1f}"   if r["hltb_main"]    else "—"
            he    = f"{r['hltb_extra']:.1f}"  if r["hltb_extra"]   else "—"
            h100  = f"{r['hltb_completion']:.1f}" if r["hltb_completion"] else "—"
            tag   = "odd" if i % 2 else "even"
            self._tree.insert("", "end", iid=str(r["appid"]), tags=(tag,), values=(
                r["appid"], r["name"] or "", price,
                r["release_year"] or "", r["total_reviews"] or 0,
                pct, r["review_score"] or "", hm, he, h100
            ))
        self._cnt.configure(text=f"{len(rows)} игр")

    def _hdr_click(self, col):
        """Клик по заголовку — сортировка, стрелка направления."""
        db_key = self._COL_DB.get(col)
        if not db_key:
            return
        # Найти метку по db_key
        label = next((k for k, v in self._sort_options.items() if v == db_key), None)
        if label:
            if self._sort_label.get() == label:
                self._asc.set(not self._asc.get())
            else:
                self._sort_label.set(label)
                self._asc.set(False)
            self._search()

    def _detail(self, _event):
        sel = self._tree.focus()
        if sel:
            g = db_game_detail(int(sel))
            if g:
                DetailWindow(self, g)


# ═══════════════════════════════════════════════
#  КАРТОЧКА ИГРЫ
# ═══════════════════════════════════════════════
class DetailWindow(tk.Toplevel):
    def __init__(self, parent, g):
        super().__init__(parent)
        self.title(g.get("name", "Игра"))
        self.geometry("660x640")
        self.configure(bg=C_BG)
        self.resizable(True, True)
        self._img_ref = None
        self._build(g)

    def _load_image(self, url):
        try:
            from PIL import Image, ImageTk
            req = urllib.request.Request(
                url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                raw = resp.read()
            img = Image.open(io.BytesIO(raw))
            new_w = 660
            new_h = int(img.height * new_w / img.width)
            img = img.resize((new_w, new_h), Image.LANCZOS)
            photo = ImageTk.PhotoImage(img)
            def _show():
                if not self.winfo_exists():
                    return
                self._img_ref = photo
                self._img_frame.configure(height=new_h)
                self._img_lbl.configure(image=photo, text="", bg=C_BG)
            self.after(0, _show)
        except ImportError:
            self.after(0, lambda: self._img_lbl.configure(
                text="pip install pillow", fg=C_WARN))
        except Exception:
            self.after(0, lambda: self._img_lbl.configure(
                text="Не удалось загрузить", fg=C_MUTED))

    def _build(self, g):
        # Шапка
        hdr = tk.Frame(self, bg=C_PANEL)
        hdr.pack(fill="x")
        tk.Label(hdr, text=g.get("name",""), bg=C_PANEL, fg=C_TEXT,
                 font=FONT_TITLE, anchor="w", wraplength=480
                 ).pack(side="left", padx=20, pady=14)
        tk.Button(hdr, text="Steam ↗", bg=C_ACCENT, fg="#fff",
                  relief="flat", font=FONT_SMALL, cursor="hand2",
                  activebackground="#6455d6", activeforeground="#fff",
                  padx=10, pady=4,
                  command=lambda: webbrowser.open(
                      f"https://store.steampowered.com/app/{g['appid']}")
                  ).pack(side="right", padx=16, pady=14)

        # Картинка — загружается в фоне
        img_url = g.get("header_image")
        if img_url:
            self._img_frame = tk.Frame(self, bg=C_CARD, height=80)
            self._img_frame.pack(fill="x")
            self._img_frame.pack_propagate(False)
            self._img_lbl = tk.Label(
                self._img_frame, bg=C_CARD,
                text="...", fg=C_MUTED, font=FONT_SMALL)
            self._img_lbl.pack(expand=True)
            threading.Thread(
                target=self._load_image, args=(img_url,), daemon=True
            ).start()

        # Прокручиваемый контент
        canvas = tk.Canvas(self, bg=C_BG, highlightthickness=0)
        sb = ttk.Scrollbar(self, orient="vertical", command=canvas.yview)
        canvas.configure(yscrollcommand=sb.set)
        sb.pack(side="right", fill="y")
        canvas.pack(fill="both", expand=True)

        inner = tk.Frame(canvas, bg=C_BG)
        wid = canvas.create_window((0,0), window=inner, anchor="nw")
        canvas.bind("<Configure>", lambda e: canvas.itemconfig(wid, width=e.width))
        inner.bind("<Configure>", lambda e: canvas.configure(
            scrollregion=canvas.bbox("all")))
        # Скролл колесом мыши
        canvas.bind_all("<MouseWheel>",
                        lambda e: canvas.yview_scroll(-1*(e.delta//120), "units"))

        def sec(title):
            tk.Label(inner, text=title, bg=C_BG, fg=C_MUTED,
                     font=FONT_CAP).pack(anchor="w", padx=20, pady=(16,2))
            tk.Frame(inner, bg=C_BORDER, height=1).pack(fill="x", padx=20, pady=(0,8))

        def kv(label, value, color=None):
            row = tk.Frame(inner, bg=C_BG)
            row.pack(fill="x", padx=20, pady=3)
            tk.Label(row, text=label, bg=C_BG, fg=C_MUTED,
                     font=FONT_BODY, width=20, anchor="w").pack(side="left")
            tk.Label(row, text=str(value) if value is not None else "—",
                     bg=C_BG, fg=color or C_TEXT, font=FONT_BOLD,
                     anchor="w", wraplength=400, justify="left"
                     ).pack(side="left")

        def chips(items):
            wrap = tk.Frame(inner, bg=C_BG)
            wrap.pack(fill="x", padx=20, pady=(0,4))
            for item in items:
                tk.Label(wrap, text=item, bg=C_CARD, fg=C_TEXT,
                         font=FONT_SMALL, padx=7, pady=3
                         ).pack(side="left", padx=2, pady=2)

        price   = f"${g['price_usd']:.2f}" if g.get("price_usd") else "Бесплатно"
        release = (f"{g.get('release_day','?')}.{g.get('release_month','?')}.{g.get('release_year','?')}"
                   if g.get("release_year") else "—")

        sec("ОСНОВНОЕ")
        kv("AppID",       g["appid"])
        kv("Цена",        price, C_ACCENT2)
        kv("Дата выхода", release)
        kv("Разработчик", ", ".join(g.get("developers",[])) or "—")
        kv("Издатель",    ", ".join(g.get("publishers",[])) or "—")

        sec("ОТЗЫВЫ")
        total = g.get("total_reviews") or 0
        pos   = g.get("positive_reviews") or 0
        neg   = g.get("negative_reviews") or 0
        pct   = g.get("review_percent")
        col   = C_SUCCESS if pct and pct>=70 else C_WARN if pct and pct>=40 else C_DANGER
        kv("Всего",       f"{total:,}")
        kv("Позитивных",  f"{pos:,}  ({pct}%)" if pct else str(pos), col)
        kv("Негативных",  f"{neg:,}")
        kv("Оценка",      g.get("review_score") or "—")

        sec("HOW LONG TO BEAT")
        kv("Основной сюжет",   f"{g['hltb_main']:.1f} ч"       if g.get("hltb_main")       else "—", C_ACCENT2)
        kv("Сюжет + доп.",     f"{g['hltb_extra']:.1f} ч"      if g.get("hltb_extra")      else "—", C_ACCENT2)
        kv("100%",             f"{g['hltb_completion']:.1f} ч"  if g.get("hltb_completion") else "—", C_ACCENT2)

        if g.get("tags"):
            sec("ТЕГИ")
            chips(g["tags"][:24])

        for label, key in [("Жанры", "genres"), ("Категории", "categories")]:
            if g.get(key):
                tk.Label(inner, text=label, bg=C_BG, fg=C_MUTED,
                         font=FONT_CAP).pack(anchor="w", padx=20, pady=(10,2))
                chips(g[key])

        if g.get("short_description"):
            sec("ОПИСАНИЕ")
            tk.Label(inner, text=g["short_description"],
                     bg=C_BG, fg=C_MUTED, font=FONT_BODY,
                     wraplength=580, justify="left", anchor="w"
                     ).pack(anchor="w", padx=20, pady=(0,20))


if __name__ == "__main__":
    App().mainloop()