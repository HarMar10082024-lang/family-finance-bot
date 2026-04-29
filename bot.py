"""
Семейный финансовый Telegram-бот.

Стек:
- python-telegram-bot v20+ (async API). Документация: https://docs.python-telegram-bot.org/
- SQLite (стандартная библиотека Python, sqlite3). Документация: https://docs.python.org/3/library/sqlite3.html

Запуск:
1) Получите токен у @BotFather (официальная инструкция Telegram:
   https://core.telegram.org/bots/features#botfather).
2) Установите зависимости:  pip install -r requirements.txt
3) Экспортируйте токен в переменную окружения TELEGRAM_BOT_TOKEN
   (Linux/macOS:  export TELEGRAM_BOT_TOKEN="123:ABC..."
    Windows PS:   $env:TELEGRAM_BOT_TOKEN="123:ABC...")
4) Запустите:  python bot.py

ВАЖНО: бот хранит данные в локальном файле family_finance.db в той же папке.
Резервируйте этот файл — в нём вся ваша финансовая история.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ---------------------------------------------------------------------------
# Настройки и логирование
# ---------------------------------------------------------------------------

# Путь к БД: по умолчанию рядом со скриптом (для локального запуска).
# На Railway задайте переменную DB_PATH, например DB_PATH=/data/family_finance.db,
# и подключите Volume на /data — тогда база переживёт редеплои.
DB_PATH = Path(os.environ.get("DB_PATH", Path(__file__).with_name("family_finance.db")))

# ID владельца в Telegram. Только этому пользователю разрешено вызывать /backup,
# и только ему отправляются автоматические бэкапы. Узнать свой ID — команда /myid.
OWNER_TELEGRAM_ID = int(os.environ.get("OWNER_TELEGRAM_ID", "0")) or None

# Период автобэкапа (в днях). По умолчанию — 7. Можно переопределить переменной.
BACKUP_INTERVAL_DAYS = int(os.environ.get("BACKUP_INTERVAL_DAYS", "7"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("family-finance-bot")


# ---------------------------------------------------------------------------
# Слой работы с БД
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS debts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id         INTEGER NOT NULL,
    creditor        TEXT    NOT NULL,
    principal       REAL    NOT NULL,           -- остаток долга
    annual_rate_pct REAL    NOT NULL DEFAULT 0, -- годовая ставка, %
    monthly_payment REAL    NOT NULL DEFAULT 0,
    term_months     INTEGER,
    notes           TEXT,
    created_at      TEXT    NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS incomes (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id     INTEGER NOT NULL,
    source      TEXT    NOT NULL,
    amount      REAL    NOT NULL,
    is_monthly  INTEGER NOT NULL DEFAULT 1,     -- 1 = регулярный, 0 = разовый
    notes       TEXT,
    created_at  TEXT    NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS expenses (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id     INTEGER NOT NULL,
    category    TEXT    NOT NULL,
    amount      REAL    NOT NULL,
    is_monthly  INTEGER NOT NULL DEFAULT 1,
    notes       TEXT,
    created_at  TEXT    NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS goals (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id     INTEGER NOT NULL,
    name        TEXT    NOT NULL,
    target      REAL    NOT NULL,
    saved       REAL    NOT NULL DEFAULT 0,
    deadline    TEXT,                            -- ISO-дата или NULL
    notes       TEXT,
    created_at  TEXT    NOT NULL DEFAULT (datetime('now'))
);
"""


@contextmanager
def db_conn():
    """Контекстный менеджер для соединения с SQLite."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    with db_conn() as c:
        c.executescript(SCHEMA)
    log.info("БД инициализирована: %s", DB_PATH)


# ---------------------------------------------------------------------------
# Доменные модели и расчёты
# ---------------------------------------------------------------------------


@dataclass
class Debt:
    id: int
    creditor: str
    principal: float
    annual_rate_pct: float
    monthly_payment: float
    term_months: int | None
    notes: str | None


def fetch_debts(chat_id: int) -> list[Debt]:
    with db_conn() as c:
        rows = c.execute(
            "SELECT * FROM debts WHERE chat_id=? ORDER BY annual_rate_pct DESC",
            (chat_id,),
        ).fetchall()
    return [
        Debt(
            id=r["id"],
            creditor=r["creditor"],
            principal=r["principal"],
            annual_rate_pct=r["annual_rate_pct"],
            monthly_payment=r["monthly_payment"],
            term_months=r["term_months"],
            notes=r["notes"],
        )
        for r in rows
    ]


def total_monthly_income(chat_id: int) -> float:
    with db_conn() as c:
        row = c.execute(
            "SELECT COALESCE(SUM(amount),0) AS s FROM incomes WHERE chat_id=? AND is_monthly=1",
            (chat_id,),
        ).fetchone()
    return float(row["s"])


def total_monthly_expense(chat_id: int) -> float:
    with db_conn() as c:
        row = c.execute(
            "SELECT COALESCE(SUM(amount),0) AS s FROM expenses WHERE chat_id=? AND is_monthly=1",
            (chat_id,),
        ).fetchone()
    return float(row["s"])


def total_monthly_debt_payment(chat_id: int) -> float:
    with db_conn() as c:
        row = c.execute(
            "SELECT COALESCE(SUM(monthly_payment),0) AS s FROM debts WHERE chat_id=?",
            (chat_id,),
        ).fetchone()
    return float(row["s"])


def fetch_goals(chat_id: int) -> list[sqlite3.Row]:
    with db_conn() as c:
        return c.execute(
            "SELECT * FROM goals WHERE chat_id=? ORDER BY created_at",
            (chat_id,),
        ).fetchall()


# ---------------------------------------------------------------------------
# Парсинг аргументов команд
# ---------------------------------------------------------------------------


def parse_kv_args(text: str) -> dict[str, str]:
    """
    Простой парсер вида:
        /add_debt creditor=Sberbank principal=500000 rate=18 payment=15000 term=48 notes="ипотека на квартиру"
    Поддержка значений в кавычках.
    """
    out: dict[str, str] = {}
    tokens: list[str] = []
    buf = ""
    in_q = False
    for ch in text:
        if ch == '"':
            in_q = not in_q
            continue
        if ch == " " and not in_q:
            if buf:
                tokens.append(buf)
                buf = ""
            continue
        buf += ch
    if buf:
        tokens.append(buf)

    for tok in tokens[1:]:  # пропускаем саму команду
        if "=" in tok:
            k, _, v = tok.partition("=")
            out[k.strip().lower()] = v.strip()
    return out


def fnum(x: float) -> str:
    """Аккуратное форматирование числа без хвоста .00 для целых."""
    if abs(x - round(x)) < 1e-9:
        return f"{int(round(x)):,}".replace(",", " ")
    return f"{x:,.2f}".replace(",", " ")


# ---------------------------------------------------------------------------
# Хэндлеры команд
# ---------------------------------------------------------------------------


HELP_TEXT = (
    "<b>Команды:</b>\n"
    "/add_debt creditor=&lt;кому&gt; principal=&lt;остаток&gt; rate=&lt;%годовых&gt; "
    "payment=&lt;ежемес.&gt; term=&lt;мес.&gt; notes=&quot;...&quot;\n"
    "/list_debts — список долгов\n"
    "/del_debt &lt;id&gt; — удалить долг\n\n"
    "/add_income source=&lt;откуда&gt; amount=&lt;сумма&gt; monthly=1|0 notes=&quot;...&quot;\n"
    "/add_expense category=&lt;категория&gt; amount=&lt;сумма&gt; monthly=1|0 notes=&quot;...&quot;\n"
    "/add_goal name=&lt;цель&gt; target=&lt;сумма&gt; saved=&lt;уже накоплено&gt; deadline=YYYY-MM-DD\n\n"
    "/balance — сводка месяца\n"
    "/analyze — анализ ситуации\n"
    "/distribute &lt;сумма&gt; — куда направить свободные деньги\n\n"
    "<b>Резервные копии:</b>\n"
    "/myid — узнать свой Telegram ID (для настройки)\n"
    "/backup — прислать файл базы владельцу (только OWNER_TELEGRAM_ID)\n\n"
    "/help — эта справка"
)


async def cmd_start(update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
    init_db()
    await update.message.reply_text(
        "Привет! Я веду семейный учёт долгов, доходов, расходов и целей.\n\n"
        "Начните с /add_income (зарплаты), /add_expense (регулярные расходы), "
        "/add_debt (долги). Затем /analyze покажет картину, "
        "а /distribute &lt;сумма&gt; подскажет, куда направить лишнее.\n\n"
        + HELP_TEXT,
        parse_mode=ParseMode.HTML,
    )


async def cmd_help(update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(HELP_TEXT, parse_mode=ParseMode.HTML)


async def cmd_add_debt(update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
    args = parse_kv_args(update.message.text)
    try:
        creditor = args["creditor"]
        principal = float(args["principal"])
    except KeyError:
        await update.message.reply_text(
            "Нужно как минимум: creditor=... principal=...\n"
            "Пример: /add_debt creditor=Sberbank principal=500000 rate=18 payment=15000 term=48"
        )
        return
    rate = float(args.get("rate", 0))
    payment = float(args.get("payment", 0))
    term = int(args["term"]) if args.get("term") else None
    notes = args.get("notes")

    with db_conn() as c:
        cur = c.execute(
            "INSERT INTO debts(chat_id,creditor,principal,annual_rate_pct,"
            "monthly_payment,term_months,notes) VALUES (?,?,?,?,?,?,?)",
            (update.effective_chat.id, creditor, principal, rate, payment, term, notes),
        )
        new_id = cur.lastrowid
    await update.message.reply_text(f"Долг #{new_id} «{creditor}» добавлен.")


async def cmd_list_debts(update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
    debts = fetch_debts(update.effective_chat.id)
    if not debts:
        await update.message.reply_text("Долгов нет.")
        return
    lines = ["<b>Ваши долги (по убыванию ставки):</b>"]
    total = 0.0
    for d in debts:
        total += d.principal
        term = f", срок {d.term_months} мес." if d.term_months else ""
        notes = f"\n   <i>{d.notes}</i>" if d.notes else ""
        lines.append(
            f"#{d.id} {d.creditor}: остаток {fnum(d.principal)}, "
            f"ставка {d.annual_rate_pct:g}%, платёж {fnum(d.monthly_payment)}/мес{term}{notes}"
        )
    lines.append(f"\n<b>Итого долгов: {fnum(total)}</b>")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


async def cmd_del_debt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("Использование: /del_debt <id>")
        return
    try:
        did = int(context.args[0])
    except ValueError:
        await update.message.reply_text("id должен быть числом.")
        return
    with db_conn() as c:
        cur = c.execute(
            "DELETE FROM debts WHERE id=? AND chat_id=?",
            (did, update.effective_chat.id),
        )
    if cur.rowcount:
        await update.message.reply_text(f"Долг #{did} удалён.")
    else:
        await update.message.reply_text("Не нашёл такой долг.")


async def cmd_add_income(update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
    args = parse_kv_args(update.message.text)
    try:
        source = args["source"]
        amount = float(args["amount"])
    except KeyError:
        await update.message.reply_text(
            "Пример: /add_income source=Зарплата amount=120000 monthly=1"
        )
        return
    is_monthly = int(args.get("monthly", "1"))
    with db_conn() as c:
        c.execute(
            "INSERT INTO incomes(chat_id,source,amount,is_monthly,notes) VALUES (?,?,?,?,?)",
            (update.effective_chat.id, source, amount, is_monthly, args.get("notes")),
        )
    await update.message.reply_text(f"Доход «{source}» {fnum(amount)} записан.")


async def cmd_add_expense(update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
    args = parse_kv_args(update.message.text)
    try:
        category = args["category"]
        amount = float(args["amount"])
    except KeyError:
        await update.message.reply_text(
            "Пример: /add_expense category=Аренда amount=40000 monthly=1"
        )
        return
    is_monthly = int(args.get("monthly", "1"))
    with db_conn() as c:
        c.execute(
            "INSERT INTO expenses(chat_id,category,amount,is_monthly,notes) VALUES (?,?,?,?,?)",
            (update.effective_chat.id, category, amount, is_monthly, args.get("notes")),
        )
    await update.message.reply_text(f"Расход «{category}» {fnum(amount)} записан.")


async def cmd_add_goal(update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
    args = parse_kv_args(update.message.text)
    try:
        name = args["name"]
        target = float(args["target"])
    except KeyError:
        await update.message.reply_text(
            "Пример: /add_goal name=\"Подушка безопасности\" target=300000 saved=50000 deadline=2026-12-31"
        )
        return
    saved = float(args.get("saved", 0))
    deadline = args.get("deadline")
    with db_conn() as c:
        c.execute(
            "INSERT INTO goals(chat_id,name,target,saved,deadline,notes) VALUES (?,?,?,?,?,?)",
            (update.effective_chat.id, name, target, saved, deadline, args.get("notes")),
        )
    await update.message.reply_text(f"Цель «{name}» добавлена.")


async def cmd_balance(update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_chat.id
    inc = total_monthly_income(uid)
    exp = total_monthly_expense(uid)
    debt_pay = total_monthly_debt_payment(uid)
    free = inc - exp - debt_pay
    text = (
        "<b>Месячный баланс</b>\n"
        f"Доходы:           {fnum(inc)}\n"
        f"Расходы:          {fnum(exp)}\n"
        f"Платежи по долгам: {fnum(debt_pay)}\n"
        f"<b>Свободно:        {fnum(free)}</b>"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


# ---------------------------------------------------------------------------
# /analyze — общая картина и рекомендации
# ---------------------------------------------------------------------------


def emergency_fund_target(monthly_expense: float, monthly_debt_payment: float) -> float:
    """
    Целевой размер «подушки безопасности» = 3 месяца обязательных трат.
    Источник методологии (3–6 месяцев): Consumer Financial Protection Bureau,
    «An essential guide to building an emergency fund»,
    https://www.consumerfinance.gov/an-essential-guide-to-building-an-emergency-fund/
    Берём нижнюю границу — 3 месяца — как стартовую цель.
    """
    return 3 * (monthly_expense + monthly_debt_payment)


async def cmd_analyze(update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_chat.id
    inc = total_monthly_income(uid)
    exp = total_monthly_expense(uid)
    debt_pay = total_monthly_debt_payment(uid)
    free = inc - exp - debt_pay
    debts = fetch_debts(uid)
    goals = fetch_goals(uid)

    lines = ["<b>Анализ ситуации</b>"]
    lines.append(
        f"Доход {fnum(inc)} − расходы {fnum(exp)} − платежи по долгам "
        f"{fnum(debt_pay)} = свободно {fnum(free)}/мес."
    )

    if inc <= 0:
        lines.append("⚠ Не указаны доходы. Добавьте через /add_income.")
    elif free < 0:
        lines.append(
            "🔴 Дефицит бюджета: расходы и платежи превышают доход. "
            "Без сокращения расходов или роста дохода долги будут расти."
        )
    else:
        share = free / inc * 100 if inc else 0
        lines.append(f"Свободные деньги — {share:.1f}% дохода.")

    if debts:
        total_debt = sum(d.principal for d in debts)
        worst = max(debts, key=lambda d: d.annual_rate_pct)
        lines.append(f"Всего долгов: {fnum(total_debt)} ({len(debts)} шт.).")
        lines.append(
            f"Самая дорогая ставка — у «{worst.creditor}»: {worst.annual_rate_pct:g}%/год. "
            "По методу лавины (avalanche) гасить досрочно надо именно его — это даёт минимум переплаты."
        )
    else:
        lines.append("Долгов нет — отличное положение.")

    ef_target = emergency_fund_target(exp, debt_pay)
    lines.append(f"Резервный фонд: цель ≈ {fnum(ef_target)} (3 мес. обязательных трат).")
    if goals:
        lines.append("\n<b>Цели:</b>")
        for g in goals:
            pct = (g["saved"] / g["target"] * 100) if g["target"] else 0
            dl = f", до {g['deadline']}" if g["deadline"] else ""
            lines.append(
                f"• {g['name']}: {fnum(g['saved'])}/{fnum(g['target'])} ({pct:.0f}%){dl}"
            )

    lines.append(
        "\nИсточник стратегии:\n"
        "• Метод лавины vs. снежного кома — Consumer Financial Protection Bureau, "
        "https://www.consumerfinance.gov/about-us/blog/which-debt-pay-first/\n"
        "• Резерв 3–6 мес. — CFPB, "
        "https://www.consumerfinance.gov/an-essential-guide-to-building-an-emergency-fund/"
    )
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


# ---------------------------------------------------------------------------
# /distribute <сумма>
# ---------------------------------------------------------------------------


def avalanche_savings(debt: Debt, extra: float) -> float:
    """
    Оценка «грубой выгоды» досрочного погашения за 12 месяцев:
    extra * rate. Это упрощённая верхняя оценка — реальная экономия зависит
    от схемы погашения (аннуитет/дифференцированный) и оставшегося срока.
    Помечено как ориентировочный показатель.
    """
    return extra * debt.annual_rate_pct / 100.0


async def cmd_distribute(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_chat.id
    if not context.args:
        await update.message.reply_text("Использование: /distribute <сумма>")
        return
    try:
        amount = float(context.args[0].replace(",", "."))
    except ValueError:
        await update.message.reply_text("Сумма должна быть числом.")
        return
    if amount <= 0:
        await update.message.reply_text("Сумма должна быть положительной.")
        return

    inc = total_monthly_income(uid)
    exp = total_monthly_expense(uid)
    debt_pay = total_monthly_debt_payment(uid)
    monthly_oblig = exp + debt_pay
    ef_target = emergency_fund_target(exp, debt_pay)

    # Сколько «накоплено» в резерв оцениваем по сумме saved у цели с именем,
    # содержащим «подуш» / «резерв» / «emergency». Если такой цели нет — считаем 0.
    goals = fetch_goals(uid)
    ef_saved = 0.0
    ef_goal_name = None
    for g in goals:
        nm = (g["name"] or "").lower()
        if any(k in nm for k in ("подуш", "резерв", "emergency", "safety")):
            ef_saved = float(g["saved"])
            ef_goal_name = g["name"]
            break

    debts = fetch_debts(uid)
    lines = [f"<b>Куда направить {fnum(amount)}?</b>"]

    # Шаг 1: дефицит бюджета
    if inc and inc - monthly_oblig < 0:
        deficit = monthly_oblig - inc
        lines.append(
            f"⚠ В бюджете дефицит {fnum(deficit)}/мес. Сначала закройте дефицит этого месяца."
        )

    # Шаг 2: резервный фонд
    plan: list[tuple[str, float, str]] = []  # (название, сумма, обоснование)
    remaining = amount

    if ef_saved < ef_target and remaining > 0:
        need_ef = min(ef_target - ef_saved, remaining)
        plan.append(
            (
                f"Резервный фонд{f' ({ef_goal_name})' if ef_goal_name else ''}",
                need_ef,
                f"до цели {fnum(ef_target)} осталось {fnum(ef_target - ef_saved)}",
            )
        )
        remaining -= need_ef

    # Шаг 3: дорогие долги (avalanche)
    if remaining > 0 and debts:
        # Гасим по убыванию ставки, но не больше остатка долга
        for d in debts:
            if remaining <= 0:
                break
            if d.annual_rate_pct <= 0:
                continue  # беспроцентные — позже
            pay = min(d.principal, remaining)
            est = avalanche_savings(d, pay)
            plan.append(
                (
                    f"Досрочно: «{d.creditor}» (#{d.id}, {d.annual_rate_pct:g}%)",
                    pay,
                    f"ориент. экономия за год ≈ {fnum(est)}",
                )
            )
            remaining -= pay

    # Шаг 4: беспроцентные долги
    if remaining > 0 and debts:
        for d in debts:
            if remaining <= 0:
                break
            if d.annual_rate_pct > 0:
                continue
            pay = min(d.principal, remaining)
            plan.append(
                (
                    f"Беспроцентный долг: «{d.creditor}» (#{d.id})",
                    pay,
                    "освобождает ежемесячный поток",
                )
            )
            remaining -= pay

    # Шаг 5: цели (кроме подушки)
    if remaining > 0:
        other_goals = [
            g for g in goals
            if not any(k in (g["name"] or "").lower() for k in ("подуш", "резерв", "emergency", "safety"))
        ]
        for g in other_goals:
            if remaining <= 0:
                break
            need = max(0.0, float(g["target"]) - float(g["saved"]))
            if need <= 0:
                continue
            pay = min(need, remaining)
            plan.append(
                (
                    f"Цель: «{g['name']}»",
                    pay,
                    f"до цели остаётся {fnum(need - pay)}",
                )
            )
            remaining -= pay

    if remaining > 0:
        plan.append(
            (
                "Свободный остаток / инвестиции",
                remaining,
                "все приоритеты закрыты — можно рассмотреть инвестиционный счёт",
            )
        )

    if not plan:
        lines.append("Не нашёл, куда направить — добавьте долги/цели через /add_debt и /add_goal.")
    else:
        for i, (title, sum_, note) in enumerate(plan, 1):
            lines.append(f"{i}. <b>{fnum(sum_)}</b> → {title}\n   <i>{note}</i>")

    lines.append(
        "\n<b>Логика приоритетов:</b>\n"
        "1) Резерв 3 мес. трат — CFPB.\n"
        "2) Долги по убыванию ставки (метод лавины) — CFPB.\n"
        "3) Беспроцентные долги — освобождают денежный поток.\n"
        "4) Финансовые цели.\n"
        "5) Остаток — инвестиции/сбережения.\n"
        "\n<i>Оценка экономии — ориентировочная (extra × ставка). "
        "Точная выгода зависит от типа платежа (аннуитет/дифф.) и оставшегося срока кредита.</i>"
    )
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


# ---------------------------------------------------------------------------
# Резервные копии (Уровень 2)
# ---------------------------------------------------------------------------


def make_backup_copy() -> Path:
    """
    Делает безопасную копию SQLite-базы через официальный online-backup API.
    Метод Connection.backup() корректно копирует БД, даже если в неё параллельно
    идут записи. Документация: https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.backup
    Возвращает путь к временному файлу-копии (вызывающая сторона сама удаляет).
    """
    tmp_dir = Path(tempfile.gettempdir())
    stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    dst_path = tmp_dir / f"family_finance_backup_{stamp}.db"
    src = sqlite3.connect(DB_PATH)
    dst = sqlite3.connect(dst_path)
    try:
        with dst:
            src.backup(dst)
    finally:
        src.close()
        dst.close()
    return dst_path


async def cmd_myid(update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
    """Показывает Telegram user_id и chat_id текущего чата.

    user_id  — нужен для OWNER_TELEGRAM_ID (право на /backup и автобэкапы).
    chat_id  — идентификатор бюджета в данном чате (личного или группового).
    """
    uid = update.effective_user.id
    cid = update.effective_chat.id
    name = update.effective_user.full_name
    chat_type = update.effective_chat.type  # 'private' | 'group' | 'supergroup' | 'channel'
    await update.message.reply_text(
        f"Ваш Telegram <b>user_id</b>: <code>{uid}</code>\n"
        f"Имя: {name}\n"
        f"<b>chat_id</b> текущего чата: <code>{cid}</code> ({chat_type})\n\n"
        "<i>user_id</i> используется для <code>OWNER_TELEGRAM_ID</code> (бэкапы).\n"
        "<i>chat_id</i> — это «семейный» бюджет, который видят все участники чата.\n"
        "В личке с ботом chat_id совпадает с user_id.",
        parse_mode=ParseMode.HTML,
    )


async def cmd_backup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправляет файл базы владельцу. Доступно только OWNER_TELEGRAM_ID."""
    if OWNER_TELEGRAM_ID is None:
        await update.message.reply_text(
            "Бэкап не настроен: не задан OWNER_TELEGRAM_ID. "
            "Узнайте свой ID через /myid и пропишите его в переменные окружения."
        )
        return
    if update.effective_user.id != OWNER_TELEGRAM_ID:
        await update.message.reply_text("Эта команда доступна только владельцу бота.")
        return
    if not DB_PATH.exists():
        await update.message.reply_text("База ещё пуста — резервировать нечего.")
        return

    tmp_path = make_backup_copy()
    try:
        with open(tmp_path, "rb") as f:
            await context.bot.send_document(
                chat_id=OWNER_TELEGRAM_ID,
                document=f,
                filename=tmp_path.name,
                caption="Резервная копия семейной финансовой БД.",
            )
    finally:
        try:
            tmp_path.unlink()
        except OSError:
            pass


async def auto_backup_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Периодический автобэкап, запускается JobQueue раз в BACKUP_INTERVAL_DAYS дней."""
    if OWNER_TELEGRAM_ID is None or not DB_PATH.exists():
        return
    try:
        tmp_path = make_backup_copy()
    except Exception as e:  # noqa: BLE001
        log.exception("Автобэкап: не удалось скопировать БД: %s", e)
        return
    try:
        with open(tmp_path, "rb") as f:
            await context.bot.send_document(
                chat_id=OWNER_TELEGRAM_ID,
                document=f,
                filename=tmp_path.name,
                caption=f"Автобэкап (каждые {BACKUP_INTERVAL_DAYS} дн.).",
            )
        log.info("Автобэкап отправлен владельцу %s", OWNER_TELEGRAM_ID)
    except Exception as e:  # noqa: BLE001
        log.exception("Автобэкап: не удалось отправить файл: %s", e)
    finally:
        try:
            tmp_path.unlink()
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Запуск
# ---------------------------------------------------------------------------


async def on_unknown(update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Не знаю такой команды. /help — список.")


def main() -> None:
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not token:
        raise SystemExit(
            "Не задан TELEGRAM_BOT_TOKEN. Получите токен у @BotFather "
            "(https://core.telegram.org/bots/features#botfather) и экспортируйте его."
        )
    init_db()
    app = Application.builder().token(token).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("add_debt", cmd_add_debt))
    app.add_handler(CommandHandler("list_debts", cmd_list_debts))
    app.add_handler(CommandHandler("del_debt", cmd_del_debt))
    app.add_handler(CommandHandler("add_income", cmd_add_income))
    app.add_handler(CommandHandler("add_expense", cmd_add_expense))
    app.add_handler(CommandHandler("add_goal", cmd_add_goal))
    app.add_handler(CommandHandler("balance", cmd_balance))
    app.add_handler(CommandHandler("analyze", cmd_analyze))
    app.add_handler(CommandHandler("distribute", cmd_distribute))
    app.add_handler(CommandHandler("myid", cmd_myid))
    app.add_handler(CommandHandler("backup", cmd_backup))
    app.add_handler(MessageHandler(filters.COMMAND, on_unknown))

    # Автобэкап раз в BACKUP_INTERVAL_DAYS дней.
    # Документация JobQueue: https://docs.python-telegram-bot.org/en/v21.6/telegram.ext.jobqueue.html
    if app.job_queue is not None and OWNER_TELEGRAM_ID is not None:
        app.job_queue.run_repeating(
            auto_backup_job,
            interval=timedelta(days=BACKUP_INTERVAL_DAYS),
            first=timedelta(minutes=2),  # первый прогон через 2 мин. после старта
            name="auto_backup",
        )
        log.info(
            "Автобэкап включён: раз в %d дн., получатель=%s",
            BACKUP_INTERVAL_DAYS, OWNER_TELEGRAM_ID,
        )
    else:
        log.warning(
            "Автобэкап ВЫКЛЮЧЕН (нет OWNER_TELEGRAM_ID или JobQueue). "
            "Установите python-telegram-bot[job-queue] и задайте OWNER_TELEGRAM_ID."
        )

    log.info("Бот запущен. Нажмите Ctrl+C для остановки.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
