#!/usr/bin/env python3
"""
Appointment Queue & Live Monitor Server
- Order Intake & Duplicate Prevention
- Queue State Management (waiting -> searching -> code_pending -> booked / failed)
- Telegram Broadcast & Alerts Engine
- Real-time Slot Availability Monitor Manager
- Data Retention / GDPR Auto-Scrubbing
- Operator Launch Bridge to Assisted Browser Runner
"""

import sqlite3
import random
import datetime
import json
import subprocess
import sys
import os
import signal
from pathlib import Path
from typing import Optional, List
import httpx
from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

from telegram_broadcaster import broadcast_telegram_message, send_test_broadcast, format_alert_message

BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / "orders.db"
CONFIG_PATH = BASE_DIR / "config.json"
DEFAULT_FALLBACK_PHONE = "+34 600 000 000"

app = FastAPI(title="Appointment Order Queue & Live Monitor System")

# Global reference to live monitor subprocess if started via dashboard
MONITOR_PROCESS: Optional[subprocess.Popen] = None


# ==========================================
# Helpers & Config
# ==========================================
def load_config() -> dict:
    if CONFIG_PATH.exists():
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_config(cfg: dict):
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)


def get_python_bin() -> str:
    venv_py = BASE_DIR / ".venv" / "bin" / "python"
    if venv_py.exists():
        return str(venv_py)
    return sys.executable


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                doc_type TEXT NOT NULL,
                doc_number TEXT NOT NULL,
                citizenship TEXT NOT NULL,
                email TEXT NOT NULL,
                phone TEXT NOT NULL,
                province TEXT NOT NULL,
                office TEXT,
                procedure TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'waiting',
                notes TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS monitors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                province TEXT NOT NULL,
                procedure TEXT NOT NULL,
                is_active INTEGER DEFAULT 1,
                interval_seconds INTEGER DEFAULT 60,
                last_checked TEXT,
                last_status TEXT DEFAULT 'pending',
                last_status_message TEXT,
                last_alert_at TEXT,
                created_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS subscribers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                contact_type TEXT NOT NULL,
                target TEXT NOT NULL,
                province TEXT DEFAULT 'ALL',
                procedure TEXT DEFAULT 'ALL',
                active INTEGER DEFAULT 1,
                created_at TEXT NOT NULL
            )
        """)
        # Seed default monitor if empty
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) as cnt FROM monitors")
        if cur.fetchone()["cnt"] == 0:
            config = load_config()
            prov = config.get("province", "Barcelona")
            proc = config.get("procedure_keyword", "POLICIA-TOMA DE HUELLA")
            now = datetime.datetime.now(datetime.timezone.utc).isoformat()
            conn.execute(
                "INSERT INTO monitors (province, procedure, is_active, interval_seconds, created_at) VALUES (?, ?, 1, 60, ?)",
                (prov, proc, now)
            )
        conn.commit()


init_db()


# ==========================================
# Telegram Notification Helpers
# ==========================================
def send_telegram_alert(headline: str, order_id: str, name: str, doc_number: str, result_code: str = "", extra: str = ""):
    config = load_config()
    tg_config = config.get("telegram", {})
    if not tg_config.get("enabled"):
        return

    bot_token = tg_config.get("bot_token", "").strip()
    chat_id = tg_config.get("chat_id", "").strip()
    if not bot_token or not chat_id:
        return

    msg = f"<b>{headline}</b>\n\n"
    msg += f"<b>Titular:</b> {name} ({doc_number})\n"
    if result_code:
        msg += f"<b>Código:</b> <code>{result_code}</code>\n"
    if extra:
        msg += f"<b>Resultado:</b> {extra}\n"
    msg += f"<b>ORDER_ID:</b> <code>{order_id}</code>\n"
    msg += f"<b>Hora:</b> {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

    broadcast_telegram_message(bot_token, chat_id, msg)


# ==========================================
# Pydantic Schemas
# ==========================================
class OrderCreate(BaseModel):
    name: str
    doc_type: str
    doc_number: str
    citizenship: str
    email: str
    phone: Optional[str] = None
    province: str
    office: Optional[str] = ""
    procedure: str


class StatusUpdate(BaseModel):
    status: str
    notes: Optional[str] = None


class OtpSubmission(BaseModel):
    otp_code: str


class TelegramSettingsUpdate(BaseModel):
    enabled: bool
    bot_token: str
    chat_id: str


class MonitorCreate(BaseModel):
    province: str
    procedure: str
    interval_seconds: Optional[int] = 60


class SubscriberCreate(BaseModel):
    contact_type: str  # telegram_channel, telegram_user, email
    target: str
    province: Optional[str] = "ALL"
    procedure: Optional[str] = "ALL"


# ==========================================
# API Routes: Orders
# ==========================================
@app.post("/api/orders")
def create_order(order: OrderCreate, background_tasks: BackgroundTasks):
    cleaned_doc = order.doc_number.strip().upper()
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    phone_number = order.phone.strip() if order.phone and order.phone.strip() else DEFAULT_FALLBACK_PHONE

    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT order_id, status FROM orders 
            WHERE UPPER(doc_number) = ? AND status IN ('waiting', 'searching', 'code_pending')
            """,
            (cleaned_doc,)
        )
        existing = cur.fetchone()
        if existing:
            raise HTTPException(
                status_code=400,
                detail=f"This document is already in progress (Order ID: {existing['order_id']}, Status: {existing['status']})"
            )

        unique_num = random.randint(100000, 999999)
        order_id = f"ORDER-{unique_num}"

        cur.execute(
            """
            INSERT INTO orders (
                order_id, name, doc_type, doc_number, citizenship,
                email, phone, province, office, procedure, status,
                notes, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'waiting', '', ?, ?)
            """,
            (
                order_id, order.name.strip(), order.doc_type.strip(), cleaned_doc,
                order.citizenship.strip(), order.email.strip(), phone_number,
                order.province.strip(), order.office.strip() if order.office else "",
                order.procedure.strip(), now, now
            )
        )
        conn.commit()

    background_tasks.add_task(
        send_telegram_alert,
        headline="📋 NUEVA SOLICITUD EN COLA",
        order_id=order_id,
        name=order.name.strip(),
        doc_number=cleaned_doc,
        result_code="WAITING",
        extra=f"Trámite: {order.procedure} ({order.province})"
    )

    return {
        "status": "success",
        "message": "Order created successfully",
        "order_id": order_id,
        "details": {
            "name": order.name,
            "doc_number": cleaned_doc,
            "status": "waiting"
        }
    }


@app.get("/api/orders")
def list_orders(status: Optional[str] = None, province: Optional[str] = None):
    with get_db() as conn:
        query = "SELECT * FROM orders WHERE 1=1"
        params = []
        if status:
            query += " AND status = ?"
            params.append(status.lower())
        if province:
            query += " AND LOWER(province) = LOWER(?)"
            params.append(province)
        query += " ORDER BY id DESC"

        cur = conn.cursor()
        cur.execute(query, params)
        rows = [dict(r) for r in cur.fetchall()]
        return {"total": len(rows), "orders": rows}


@app.get("/api/orders/{order_id}")
def get_order(order_id: str):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM orders WHERE order_id = ?", (order_id.upper(),))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Order not found")
        return dict(row)


@app.patch("/api/orders/{order_id}/status")
def update_status(order_id: str, update: StatusUpdate, background_tasks: BackgroundTasks):
    valid_statuses = ["waiting", "searching", "code_pending", "booked", "failed"]
    new_status = update.status.lower()
    if new_status not in valid_statuses:
        raise HTTPException(status_code=400, detail=f"Invalid status. Must be one of: {valid_statuses}")

    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM orders WHERE order_id = ?", (order_id.upper(),))
        order = cur.fetchone()
        if not order:
            raise HTTPException(status_code=404, detail="Order not found")

        cur.execute(
            """
            UPDATE orders 
            SET status = ?, notes = COALESCE(?, notes), updated_at = ? 
            WHERE order_id = ?
            """,
            (new_status, update.notes, now, order_id.upper())
        )
        conn.commit()

    if new_status == "booked":
        background_tasks.add_task(
            send_telegram_alert,
            headline="✅ CITA REGISTRADA",
            order_id=order_id.upper(),
            name=order["name"],
            doc_number=order["doc_number"],
            result_code="BOOKED",
            extra=update.notes or "Cita confirmada exitosamente"
        )
    elif new_status == "failed":
        background_tasks.add_task(
            send_telegram_alert,
            headline="‼️ ERROR DE REGISTRO DE LA CITA",
            order_id=order_id.upper(),
            name=order["name"],
            doc_number=order["doc_number"],
            result_code=update.notes or "FAILED",
            extra="No fue posible completar la reserva"
        )

    return {"status": "success", "order_id": order_id.upper(), "new_status": new_status}


@app.post("/api/orders/purge-completed")
def purge_completed_records():
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            UPDATE orders 
            SET name = '[REDACTED]',
                doc_number = substr(doc_number, 1, 2) || '*****' || substr(doc_number, -1),
                email = 'redacted@privacy.local',
                phone = '***-***-***',
                notes = 'Customer data scrubbed in compliance with retention policy'
            WHERE status IN ('booked', 'failed')
              AND name != '[REDACTED]'
        """)
        scrubbed_count = cur.rowcount
        conn.commit()

    return {"status": "success", "message": f"Successfully scrubbed {scrubbed_count} completed records."}


@app.post("/api/orders/{order_id}/launch-assistant")
def launch_assistant(order_id: str):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM orders WHERE order_id = ?", (order_id.upper(),))
        order = cur.fetchone()
        if not order:
            raise HTTPException(status_code=404, detail="Order not found")

        current_cfg = load_config()
        current_cfg.update({
            "province": order["province"],
            "procedure_keyword": order["procedure"],
            "document_type": order["doc_type"],
            "document_number": order["doc_number"],
            "full_name": order["name"],
            "year_of_birth": "1990",
            "country_of_nationality": order["citizenship"],
            "phone": order["phone"],
            "email": order["email"],
            "portal_url": "https://icp.administracionelectronica.gob.es/icpplus/index.html"
        })
        save_config(current_cfg)

        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        cur.execute("UPDATE orders SET status = 'searching', updated_at = ? WHERE order_id = ?", (now, order_id.upper()))
        conn.commit()

    python_bin = get_python_bin()
    assistant_script = BASE_DIR / "assistant.py"
    subprocess.Popen([python_bin, str(assistant_script)])

    return {"status": "launched", "order_id": order_id.upper(), "message": "Visible assisted runner launched."}


@app.post("/api/orders/{order_id}/auto-book")
def auto_book_order(order_id: str):
    """Triggers the automated runner auto_booker.py for this order."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM orders WHERE order_id = ?", (order_id.upper(),))
        order = cur.fetchone()
        if not order:
            raise HTTPException(status_code=404, detail="Order not found")

    python_bin = get_python_bin()
    booker_script = BASE_DIR / "auto_booker.py"
    subprocess.Popen([python_bin, str(booker_script), "--order-id", order_id.upper()])

    return {"status": "started", "order_id": order_id.upper(), "message": f"Auto-booker started for {order_id.upper()}"}


@app.post("/api/orders/{order_id}/submit-otp")
def submit_otp_code(order_id: str, submission: OtpSubmission):
    code = submission.otp_code.strip()
    if not code:
        raise HTTPException(status_code=400, detail="OTP code cannot be empty")

    otp_file = Path(f"/tmp/cita_otp_{order_id.upper()}.txt")
    otp_file.write_text(code, encoding="utf-8")

    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    with get_db() as conn:
        conn.execute(
            "UPDATE orders SET notes = 'Entregando código SMS...', updated_at = ? WHERE order_id = ?",
            (now, order_id.upper())
        )
        conn.commit()

    return {"status": "success", "message": f"OTP {code} delivered to auto-runner for {order_id.upper()}"}


# ==========================================
# API Routes: Telegram Settings & Broadcast
# ==========================================
@app.get("/api/config/telegram")
def get_telegram_config():
    config = load_config()
    tg = config.get("telegram", {})
    token = tg.get("bot_token", "")
    masked_token = f"{token[:4]}...{token[-4:]}" if len(token) > 8 else ("configured" if token else "")
    return {
        "enabled": tg.get("enabled", False),
        "bot_token_set": bool(token),
        "bot_token_masked": masked_token,
        "chat_id": tg.get("chat_id", "")
    }


@app.post("/api/config/telegram")
def update_telegram_config(settings: TelegramSettingsUpdate):
    config = load_config()
    current_tg = config.get("telegram", {})
    # If placeholder / empty token passed, retain existing
    bot_token = settings.bot_token.strip()
    if not bot_token and current_tg.get("bot_token"):
        bot_token = current_tg["bot_token"]

    config["telegram"] = {
        "enabled": settings.enabled,
        "bot_token": bot_token,
        "chat_id": settings.chat_id.strip()
    }
    save_config(config)
    return {"status": "success", "message": "Telegram configuration saved."}


@app.post("/api/telegram/test-broadcast")
def trigger_test_broadcast():
    config = load_config()
    tg = config.get("telegram", {})
    bot_token = tg.get("bot_token", "").strip()
    chat_id = tg.get("chat_id", "").strip()

    if not bot_token or not chat_id:
        raise HTTPException(
            status_code=400,
            detail="Telegram bot_token or chat_id is missing. Please save configuration first."
        )

    res = send_test_broadcast(bot_token, chat_id)
    if res["status"] != "success":
        raise HTTPException(
            status_code=502,
            detail=f"Telegram broadcast failed: {res.get('results', {}).get('failed', res.get('message'))}"
        )
    return res


# ==========================================
# API Routes: Monitors & Live Scanner
# ==========================================
MAJOR_SPANISH_PROVINCES = [
    "Madrid", "Barcelona", "Valencia", "Alicante", "Sevilla",
    "Málaga", "Murcia", "Baleares", "Las Palmas", "Santa Cruz de Tenerife",
    "Zaragoza", "Bizkaia", "Tarragona", "Girona", "Granada",
    "A Coruña", "Toledo", "Almería", "Cádiz", "Córdoba",
    "Navarra", "Asturias", "Castellón", "Cantabria", "Valladolid",
    "Gipuzkoa", "Lleida", "Huelva", "Jaén", "Badajoz"
]


@app.get("/api/live-status")
def get_live_status():
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM monitors ORDER BY id ASC")
        rows = [dict(r) for r in cur.fetchall()]
        open_slots = [r for r in rows if r.get("last_status") == "available"]
        return {
            "open_count": len(open_slots),
            "open_slots": open_slots,
            "total_monitors": len(rows),
            "monitors": rows,
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
        }


@app.post("/api/monitors/seed-all-provinces")
def seed_all_provinces():
    default_proc = "POLICIA-TOMA DE HUELLA"
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    added = 0
    with get_db() as conn:
        cur = conn.cursor()
        for prov in MAJOR_SPANISH_PROVINCES:
            cur.execute(
                "SELECT id FROM monitors WHERE LOWER(province) = LOWER(?) AND procedure = ?",
                (prov, default_proc)
            )
            if not cur.fetchone():
                cur.execute(
                    """
                    INSERT INTO monitors (province, procedure, is_active, interval_seconds, created_at)
                    VALUES (?, ?, 1, 60, ?)
                    """,
                    (prov, default_proc, now)
                )
                added += 1
        conn.commit()
    return {
        "status": "success",
        "added_count": added,
        "message": f"Added {added} Spanish provinces to live monitor. Total: {len(MAJOR_SPANISH_PROVINCES)}"
    }


@app.post("/api/monitors/scan-all")
def scan_all_monitors(background_tasks: BackgroundTasks):
    python_bin = get_python_bin()
    script = BASE_DIR / "fast_monitor.py"

    def run_batch():
        subprocess.run([python_bin, str(script), "--check-once"], check=False)

    background_tasks.add_task(run_batch)
    return {"status": "queued", "message": "Batch scan across all provinces triggered in background."}


@app.get("/api/monitors")
def list_monitors():
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM monitors ORDER BY id ASC")
        rows = [dict(r) for r in cur.fetchall()]
        return {"total": len(rows), "monitors": rows}


@app.post("/api/monitors")
def create_monitor(m: MonitorCreate):
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO monitors (province, procedure, is_active, interval_seconds, created_at)
            VALUES (?, ?, 1, ?, ?)
            """,
            (m.province.strip(), m.procedure.strip(), m.interval_seconds or 60, now)
        )
        conn.commit()
        monitor_id = cur.lastrowid
    return {"status": "success", "monitor_id": monitor_id, "message": "Monitor target created"}


@app.patch("/api/monitors/{monitor_id}/toggle")
def toggle_monitor(monitor_id: int):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT is_active FROM monitors WHERE id = ?", (monitor_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Monitor not found")
        new_state = 0 if row["is_active"] == 1 else 1
        cur.execute("UPDATE monitors SET is_active = ? WHERE id = ?", (new_state, monitor_id))
        conn.commit()
    return {"status": "success", "monitor_id": monitor_id, "is_active": new_state}


@app.delete("/api/monitors/{monitor_id}")
def delete_monitor(monitor_id: int):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM monitors WHERE id = ?", (monitor_id,))
        conn.commit()
    return {"status": "success", "message": "Monitor deleted"}


@app.post("/api/monitors/{monitor_id}/check")
def trigger_manual_check(monitor_id: int, background_tasks: BackgroundTasks):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM monitors WHERE id = ?", (monitor_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Monitor not found")
        monitor_data = dict(row)

    # Launch check script in background
    python_bin = get_python_bin()
    script = BASE_DIR / "monitor.py"

    def run_check():
        subprocess.run([python_bin, str(script), "--check-once"], check=False)

    background_tasks.add_task(run_check)
    return {"status": "queued", "message": f"Scan triggered for {monitor_data['province']} ({monitor_data['procedure']})"}


@app.get("/api/monitor-daemon/status")
def get_monitor_daemon_status():
    global MONITOR_PROCESS
    is_running = False
    pid = None
    if MONITOR_PROCESS is not None:
        if MONITOR_PROCESS.poll() is None:
            is_running = True
            pid = MONITOR_PROCESS.pid
        else:
            MONITOR_PROCESS = None

    return {"is_running": is_running, "pid": pid}


@app.post("/api/monitor-daemon/start")
def start_monitor_daemon():
    global MONITOR_PROCESS
    if MONITOR_PROCESS is not None and MONITOR_PROCESS.poll() is None:
        return {"status": "already_running", "pid": MONITOR_PROCESS.pid}

    python_bin = get_python_bin()
    script = BASE_DIR / "fast_monitor.py"
    MONITOR_PROCESS = subprocess.Popen([python_bin, str(script)])
    return {"status": "started", "pid": MONITOR_PROCESS.pid}


@app.post("/api/monitor-daemon/stop")
def stop_monitor_daemon():
    global MONITOR_PROCESS
    if MONITOR_PROCESS is None or MONITOR_PROCESS.poll() is not None:
        MONITOR_PROCESS = None
        return {"status": "not_running"}

    try:
        MONITOR_PROCESS.terminate()
        MONITOR_PROCESS.wait(timeout=3)
    except Exception:
        if MONITOR_PROCESS:
            MONITOR_PROCESS.kill()
    MONITOR_PROCESS = None
    return {"status": "stopped"}


# ==========================================
# Web UI Pages
# ==========================================
@app.get("/", response_class=HTMLResponse)
def intake_page():
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Appointment Intake & Live Slot Alerts</title>
        <style>
            :root { --primary: #2563eb; --primary-hover: #1d4ed8; --bg: #f8fafc; --card: #ffffff; --text: #0f172a; --border: #e2e8f0; }
            body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: var(--bg); color: var(--text); margin: 0; padding: 2rem 1rem; }
            .container { max-width: 800px; margin: 0 auto; display: grid; gap: 2rem; }
            .card { background: var(--card); border-radius: 12px; padding: 2rem; box-shadow: 0 4px 6px -1px rgb(0 0 0 / 0.1); border: 1px solid var(--border); }
            h1, h2 { margin-top: 0; color: #1e293b; }
            .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; }
            label { display: block; font-size: 0.875rem; font-weight: 600; margin-bottom: 0.35rem; color: #475569; }
            input, select, textarea { width: 100%; padding: 0.65rem 0.75rem; border: 1px solid var(--border); border-radius: 6px; box-sizing: border-box; font-size: 0.95rem; }
            input:focus, select:focus { outline: none; border-color: var(--primary); box-shadow: 0 0 0 3px rgba(37,99,235,0.15); }
            button { background: var(--primary); color: white; border: none; padding: 0.75rem 1.5rem; border-radius: 6px; font-weight: 600; cursor: pointer; font-size: 1rem; }
            button:hover { background: var(--primary-hover); }
            .badge { display: inline-block; padding: 0.25rem 0.6rem; border-radius: 9999px; font-size: 0.8rem; font-weight: bold; text-transform: uppercase; }
            .badge-waiting { background: #fef3c7; color: #92400e; }
            .badge-searching { background: #e0f2fe; color: #0369a1; }
            .badge-code_pending { background: #fce7f3; color: #9d174d; }
            .badge-booked { background: #dcfce7; color: #15803d; }
            .badge-failed { background: #fee2e2; color: #b91c1c; }
            .alert { padding: 1rem; border-radius: 6px; margin-bottom: 1rem; font-size: 0.95rem; display: none; }
            .alert-error { background: #fee2e2; color: #991b1b; border: 1px solid #f87171; }
            .alert-success { background: #dcfce7; color: #166534; border: 1px solid #86efac; }
            .nav-link { float: right; font-size: 0.9rem; color: var(--primary); text-decoration: none; font-weight: 600; }
            .banner { background: linear-gradient(135deg, #1e3a8a, #3b82f6); color: white; border-radius: 12px; padding: 1.5rem 2rem; }
            .banner h2 { color: white; margin-bottom: 0.5rem; }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="banner">
                <a href="/admin" class="nav-link" style="color: #dbeafe;">👉 Go to Admin Control Board</a>
                <h2>📡 Extranjería Live Appointment Alerts</h2>
                <p style="margin: 0; opacity: 0.9;">Submit your details to enter the assisted queue, or monitor live slot openings via our Telegram broadcast channel.</p>
            </div>

            <!-- LIVE OPEN APPOINTMENTS BANNER -->
            <div id="liveOpenAlert" style="display: none; background: #ecfdf5; border: 2px solid #10b981; border-radius: 12px; padding: 1.25rem;">
                <div style="display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 0.75rem;">
                    <div>
                        <h3 style="margin: 0; color: #065f46; display: flex; align-items: center; gap: 0.5rem;">
                            <span style="display: inline-block; width: 12px; height: 12px; background: #10b981; border-radius: 50%; box-shadow: 0 0 8px #10b981;"></span>
                            🚨 ¡CITAS ABIERTAS AHORA MISMO!
                        </h3>
                        <p style="margin: 0.35rem 0 0; color: #047857; font-size: 0.9rem;" id="liveOpenText"></p>
                    </div>
                    <div id="liveOpenBadges" style="display: flex; gap: 0.4rem; flex-wrap: wrap;"></div>
                </div>
            </div>

            <div class="card">
                <h1>📋 New Appointment Request</h1>
                <p style="color: #64748b; margin-bottom: 1.5rem;">Submit applicant information into the queue.</p>
                
                <div id="alertBox" class="alert"></div>

                <form id="orderForm">
                    <div class="grid-2">
                        <div>
                            <label>Full Name *</label>
                            <input type="text" id="name" required placeholder="e.g. MD MUSTAK AHMED">
                        </div>
                        <div>
                            <label>Citizenship *</label>
                            <input type="text" id="citizenship" required placeholder="e.g. Bangladesh">
                        </div>
                    </div>

                    <div class="grid-2" style="margin-top: 1rem;">
                        <div>
                            <label>Document Type *</label>
                            <select id="doc_type" required>
                                <option value="NIE">NIE</option>
                                <option value="PASSPORT">Passport</option>
                                <option value="DNI">DNI</option>
                            </select>
                        </div>
                        <div>
                            <label>Document Number *</label>
                            <input type="text" id="doc_number" required placeholder="e.g. Z5068943R">
                        </div>
                    </div>

                    <div class="grid-2" style="margin-top: 1rem;">
                        <div>
                            <label>Email *</label>
                            <input type="email" id="email" required placeholder="customer@example.com">
                        </div>
                        <div>
                            <label>Phone (Optional)</label>
                            <input type="tel" id="phone" placeholder="Leave empty to use service number">
                        </div>
                    </div>

                    <div class="grid-2" style="margin-top: 1rem;">
                        <div>
                            <label>Province *</label>
                            <input type="text" id="province" required value="Barcelona">
                        </div>
                        <div>
                            <label>Office (Optional)</label>
                            <input type="text" id="office" placeholder="e.g. Rambla Guipuscoa 74">
                        </div>
                    </div>

                    <div style="margin-top: 1rem;">
                        <label>Procedure (Trámite) *</label>
                        <input type="text" id="procedure" required value="POLICIA-TOMA DE HUELLA">
                    </div>

                    <div style="margin-top: 1.5rem;">
                        <button type="submit">Submit Request</button>
                    </div>
                </form>
            </div>

            <div class="card">
                <h2>🔍 Track Existing Order</h2>
                <div style="display: flex; gap: 0.5rem;">
                    <input type="text" id="trackId" placeholder="Enter Order ID (e.g. ORDER-514792)">
                    <button type="button" onclick="trackOrder()">Lookup</button>
                </div>
                <div id="trackResult" style="margin-top: 1rem; display: none; padding: 1rem; background: #f1f5f9; border-radius: 6px;"></div>
            </div>
        </div>

        <script>
            document.getElementById('orderForm').addEventListener('submit', async (e) => {
                e.preventDefault();
                const alertBox = document.getElementById('alertBox');
                alertBox.style.display = 'none';

                const payload = {
                    name: document.getElementById('name').value,
                    citizenship: document.getElementById('citizenship').value,
                    doc_type: document.getElementById('doc_type').value,
                    doc_number: document.getElementById('doc_number').value,
                    email: document.getElementById('email').value,
                    phone: document.getElementById('phone').value,
                    province: document.getElementById('province').value,
                    office: document.getElementById('office').value,
                    procedure: document.getElementById('procedure').value
                };

                try {
                    const res = await fetch('/api/orders', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(payload)
                    });
                    const data = await res.json();
                    if (!res.ok) {
                        alertBox.className = 'alert alert-error';
                        alertBox.innerText = data.detail || 'Failed to submit order';
                        alertBox.style.display = 'block';
                    } else {
                        alertBox.className = 'alert alert-success';
                        alertBox.innerHTML = `<strong>Order Queued!</strong> Assigned ID: <code>${data.order_id}</code> (Status: waiting)`;
                        alertBox.style.display = 'block';
                        document.getElementById('orderForm').reset();
                    }
                } catch (err) {
                    alertBox.className = 'alert alert-error';
                    alertBox.innerText = 'Network error: ' + err.message;
                    alertBox.style.display = 'block';
                }
            });

            async function trackOrder() {
                const id = document.getElementById('trackId').value.trim();
                const resDiv = document.getElementById('trackResult');
                if (!id) return;
                try {
                    const res = await fetch(`/api/orders/${id}`);
                    if (!res.ok) {
                        resDiv.style.display = 'block';
                        resDiv.innerHTML = '<span style="color: #b91c1c;">Order not found.</span>';
                        return;
                    }
                    const order = await res.json();
                    resDiv.style.display = 'block';
                    resDiv.innerHTML = `
                        <strong>Order:</strong> ${order.order_id}<br>
                        <strong>Applicant:</strong> ${order.name} (${order.doc_number})<br>
                        <strong>Status:</strong> <span class="badge badge-${order.status}">${order.status}</span><br>
                        <strong>Procedure:</strong> ${order.procedure} (${order.province})<br>
                        <strong>Updated:</strong> ${new Date(order.updated_at).toLocaleString()}
                    `;
                } catch (e) {
                    resDiv.style.display = 'block';
                    resDiv.innerText = 'Error looking up order';
                }
            }

            async function pollLiveOpen() {
                try {
                    const res = await fetch('/api/live-status');
                    const data = await res.json();
                    const alertEl = document.getElementById('liveOpenAlert');
                    const textEl = document.getElementById('liveOpenText');
                    const badgesEl = document.getElementById('liveOpenBadges');
                    if (data && data.open_count > 0) {
                        alertEl.style.display = 'block';
                        textEl.innerHTML = `Detectadas <strong>${data.open_count} ciudades</strong> con citas disponibles en este momento. ¡Solicite ahora antes de que se agoten!`;
                        badgesEl.innerHTML = data.open_slots.map(s => `
                            <button type="button" style="background: #059669; color: white; border: none; padding: 0.35rem 0.75rem; border-radius: 6px; cursor: pointer; font-size: 0.85rem; font-weight: bold;" onclick="selectCity('${s.province}')">
                                📍 ${s.province}
                            </button>
                        `).join('');
                    } else {
                        alertEl.style.display = 'none';
                    }
                } catch(e) {}
            }

            function selectCity(city) {
                const provInput = document.getElementById('province');
                provInput.value = city;
                provInput.scrollIntoView({ behavior: 'smooth' });
                provInput.focus();
            }

            pollLiveOpen();
            setInterval(pollLiveOpen, 4000);
        </script>
    </body>
    </html>
    """


@app.get("/admin", response_class=HTMLResponse)
def admin_page():
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Control Board - Cita Previa & Telegram Broadcaster</title>
        <style>
            :root { --primary: #2563eb; --primary-hover: #1d4ed8; --bg: #f8fafc; --card: #ffffff; --text: #0f172a; --border: #e2e8f0; }
            body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: var(--bg); margin: 0; padding: 1.5rem; color: var(--text); }
            .header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 1.5rem; }
            h1 { margin: 0; font-size: 1.6rem; }
            .btn { background: var(--primary); color: white; padding: 0.5rem 1rem; border-radius: 6px; text-decoration: none; font-size: 0.9rem; font-weight: bold; border: none; cursor: pointer; display: inline-flex; align-items: center; gap: 0.4rem; }
            .btn:hover { background: var(--primary-hover); }
            .btn-action { padding: 0.35rem 0.65rem; font-size: 0.8rem; border-radius: 4px; background: #0284c7; color: white; border: none; cursor: pointer; }
            .btn-action:hover { background: #0369a1; }
            .btn-success { background: #16a34a; }
            .btn-success:hover { background: #15803d; }
            .btn-danger { background: #dc2626; }
            .btn-danger:hover { background: #b91c1c; }
            .btn-secondary { background: #64748b; }
            .btn-secondary:hover { background: #475569; }

            .tabs { display: flex; gap: 0.5rem; margin-bottom: 1.5rem; border-bottom: 2px solid #e2e8f0; padding-bottom: 0.5rem; }
            .tab-btn { background: none; border: none; padding: 0.6rem 1.2rem; font-size: 0.95rem; font-weight: 600; color: #64748b; cursor: pointer; border-radius: 6px; }
            .tab-btn.active { background: #eff6ff; color: var(--primary); }

            .tab-content { display: none; }
            .tab-content.active { display: block; }

            .stats { display: flex; gap: 1rem; margin-bottom: 1.5rem; }
            .stat-card { background: white; padding: 1rem; border-radius: 8px; flex: 1; border: 1px solid var(--border); box-shadow: 0 1px 3px rgba(0,0,0,0.05); }
            .stat-title { font-size: 0.8rem; text-transform: uppercase; color: #64748b; font-weight: 600; }
            .stat-value { font-size: 1.5rem; font-weight: bold; margin-top: 0.25rem; }

            .card { background: white; border-radius: 8px; padding: 1.25rem; margin-bottom: 1.5rem; border: 1px solid var(--border); box-shadow: 0 1px 3px rgba(0,0,0,0.05); }
            .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; }
            label { display: block; font-size: 0.85rem; font-weight: 600; margin-bottom: 0.35rem; color: #475569; }
            input, select { width: 100%; padding: 0.55rem 0.75rem; border: 1px solid #cbd5e1; border-radius: 6px; box-sizing: border-box; font-size: 0.9rem; }

            table { width: 100%; border-collapse: collapse; background: white; border-radius: 8px; overflow: hidden; border: 1px solid var(--border); margin-bottom: 1.5rem; }
            th, td { padding: 0.75rem 1rem; text-align: left; border-bottom: 1px solid var(--border); font-size: 0.9rem; }
            th { background: #f1f5f9; color: #475569; font-weight: 600; }

            .badge { display: inline-block; padding: 0.2rem 0.5rem; border-radius: 9999px; font-size: 0.75rem; font-weight: bold; text-transform: uppercase; }
            .badge-waiting { background: #fef3c7; color: #92400e; }
            .badge-searching { background: #e0f2fe; color: #0369a1; }
            .badge-code_pending { background: #fce7f3; color: #9d174d; }
            .badge-booked { background: #dcfce7; color: #15803d; }
            .badge-failed { background: #fee2e2; color: #b91c1c; }

            .badge-available { background: #22c55e; color: white; animation: pulse 1.5s infinite; }
            .badge-no_slots { background: #f1f5f9; color: #475569; }
            .badge-timeout { background: #fee2e2; color: #b91c1c; }
            .badge-rate_limited { background: #fef08a; color: #854d0e; }
            .badge-pending { background: #e2e8f0; color: #475569; }

            @keyframes pulse {
                0% { opacity: 1; }
                50% { opacity: 0.6; }
                100% { opacity: 1; }
            }

            .alert-box { padding: 0.75rem 1rem; border-radius: 6px; margin-bottom: 1rem; font-size: 0.9rem; display: none; }
            .alert-box.success { background: #dcfce7; color: #15803d; border: 1px solid #86efac; display: block; }
            .alert-box.error { background: #fee2e2; color: #991b1b; border: 1px solid #f87171; display: block; }
        </style>
    </head>
    <body>
        <div class="header">
            <div>
                <h1>📦 Control Board: Appointments & Telegram Broadcaster</h1>
                <p style="margin: 0.25rem 0 0; color: #64748b;">Manage customer booking queue and live slot broadcast scanners.</p>
            </div>
            <div>
                <a href="/" class="btn btn-secondary">+ Customer Intake</a>
                <button onclick="refreshAll()" class="btn">🔄 Refresh</button>
            </div>
        </div>

        <div class="tabs">
            <button class="tab-btn active" onclick="switchTab('ordersTab')">📋 Orders Queue</button>
            <button class="tab-btn" onclick="switchTab('monitorTab')">📡 Live Monitor & Telegram Broadcast</button>
        </div>

        <!-- TAB 1: ORDERS QUEUE -->
        <div id="ordersTab" class="tab-content active">
            <div style="display: flex; justify-content: flex-end; margin-bottom: 1rem;">
                <button onclick="purgeData()" class="btn btn-danger">🧹 Scrub Finished Data</button>
            </div>

            <div class="stats" id="statsRow"></div>

            <table>
                <thead>
                    <tr>
                        <th>Order ID</th>
                        <th>Applicant Name</th>
                        <th>Document</th>
                        <th>Procedure & Province</th>
                        <th>Contact</th>
                        <th>Current Status</th>
                        <th>Actions & Status</th>
                    </tr>
                </thead>
                <tbody id="ordersTableBody">
                    <tr><td colspan="7" style="text-align: center; color: #94a3b8;">Loading orders...</td></tr>
                </tbody>
            </table>
        </div>

        <!-- TAB 2: LIVE MONITOR & TELEGRAM BROADCAST -->
        <div id="monitorTab" class="tab-content">
            <!-- LIVE OPEN BOOKINGS HERO SHOWCASE -->
            <div id="heroOpenSection" style="display: none; background: #ecfdf5; border: 2px solid #22c55e; border-radius: 10px; padding: 1.25rem; margin-bottom: 1.5rem;">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.75rem;">
                    <h3 style="margin: 0; color: #15803d; display: flex; align-items: center; gap: 0.5rem;">
                        <span style="display: inline-block; width: 12px; height: 12px; background: #22c55e; border-radius: 50%; box-shadow: 0 0 10px #22c55e;"></span>
                        🔥 APPOINTMENTS OPEN RIGHT NOW (<span id="heroOpenCount">0</span>)
                    </h3>
                    <span style="font-size: 0.85rem; color: #166534; font-weight: bold;">⚡ Live Availability Stream</span>
                </div>
                <div id="heroOpenCards" style="display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); gap: 0.75rem;"></div>
            </div>

            <!-- Telegram Config Card -->
            <div class="card">
                <h3 style="margin-top: 0;">📢 Telegram Broadcast Configuration</h3>
                <p style="color: #64748b; font-size: 0.85rem; margin-top: -0.25rem;">
                    Configure the Telegram Bot token and target channels/chats. When appointment slots open, an urgent broadcast alert will be sent immediately.
                </p>
                <div id="tgAlertBox" class="alert-box"></div>
                <div class="grid-2">
                    <div>
                        <label>Telegram Bot Token (from @BotFather)</label>
                        <input type="password" id="tgBotToken" placeholder="e.g. 123456789:ABCdefGhIJKlmNoPQRsTUVwxyZ">
                        <small style="color: #64748b;" id="tokenMaskedNotice"></small>
                    </div>
                    <div>
                        <label>Broadcast Channels / Group IDs (comma-separated)</label>
                        <input type="text" id="tgChatId" placeholder="e.g. @my_citas_channel, -100123456789, 98765432">
                        <small style="color: #64748b;">Supports public channels (@name), private groups (-100...), or user chat IDs.</small>
                    </div>
                </div>
                <div style="margin-top: 1rem; display: flex; gap: 0.5rem; align-items: center;">
                    <label style="display: flex; align-items: center; gap: 0.5rem; margin: 0; cursor: pointer;">
                        <input type="checkbox" id="tgEnabled" style="width: auto;"> <b>Enable Telegram Alerts</b>
                    </label>
                    <button class="btn" onclick="saveTelegramSettings()" style="margin-left: auto;">💾 Save Telegram Settings</button>
                    <button class="btn btn-action" onclick="testTelegramBroadcast()">📢 Send Test Broadcast</button>
                </div>
            </div>

            <!-- Monitor Daemon & Status Controls -->
            <div class="card" style="display: flex; justify-content: space-between; align-items: center;">
                <div>
                    <h3 style="margin: 0;">🛰️ Live Monitor Daemon</h3>
                    <p style="margin: 0.25rem 0 0; color: #64748b; font-size: 0.85rem;" id="daemonStatusText">
                        Status: Checking daemon state...
                    </p>
                </div>
                <div style="display: flex; gap: 0.5rem;">
                    <button id="btnStartDaemon" class="btn btn-success" onclick="toggleDaemon('start')">▶️ Start Live Monitor</button>
                    <button id="btnStopDaemon" class="btn btn-danger" onclick="toggleDaemon('stop')" style="display: none;">⏹️ Stop Live Monitor</button>
                </div>
            </div>

            <!-- Monitored Targets Table -->
            <div class="card">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem;">
                    <h3 style="margin: 0;">🎯 Monitored Provinces & Procedures</h3>
                    <button class="btn btn-action" onclick="toggleAddMonitorModal()">+ Add New Target</button>
                </div>

                <div id="addMonitorForm" style="display: none; background: #f8fafc; padding: 1rem; border-radius: 6px; margin-bottom: 1rem; border: 1px solid var(--border);">
                    <div class="grid-2">
                        <div>
                            <label>Province</label>
                            <input type="text" id="newMonProvince" value="Barcelona" placeholder="e.g. Barcelona, Madrid, Valencia">
                        </div>
                        <div>
                            <label>Procedure (Trámite)</label>
                            <input type="text" id="newMonProc" value="POLICIA-TOMA DE HUELLA" placeholder="e.g. POLICIA-TOMA DE HUELLA">
                        </div>
                    </div>
                    <div style="margin-top: 0.75rem; display: flex; gap: 0.5rem; justify-content: flex-end;">
                        <button class="btn btn-secondary" onclick="toggleAddMonitorModal()">Cancel</button>
                        <button class="btn" onclick="saveNewMonitor()">Add Target</button>
                    </div>
                </div>

                <!-- Search & Filters Toolbar -->
                <div style="display: flex; gap: 0.5rem; align-items: center; margin-bottom: 1rem; flex-wrap: wrap;">
                    <input type="text" id="citySearch" placeholder="🔍 Search city (e.g. Madrid, Barcelona)..." style="width: 240px; padding: 0.45rem 0.75rem;" oninput="filterMonitors()">
                    <div style="display: flex; gap: 0.25rem;">
                        <button class="btn-action" id="tabFilterAll" style="background: #2563eb;" onclick="setMonitorFilter('all')">All (<span id="badgeTotalCities">0</span>)</button>
                        <button class="btn-action" id="tabFilterOpen" style="background: #16a34a; opacity: 0.7;" onclick="setMonitorFilter('available')">🟢 Open Now (<span id="badgeOpenCities">0</span>)</button>
                        <button class="btn-action" id="tabFilterClosed" style="background: #64748b; opacity: 0.7;" onclick="setMonitorFilter('no_slots')">⚪ Closed (<span id="badgeClosedCities">0</span>)</button>
                    </div>
                    <div style="margin-left: auto; display: flex; gap: 0.4rem; flex-wrap: wrap;">
                        <button class="btn btn-action" style="background: #7c3aed;" onclick="seedAllSpanishCities()">⚡ Add All 30+ Spanish Cities</button>
                        <button class="btn btn-action" style="background: #0284c7;" onclick="scanAllCitiesNow()">🚀 Scan All Cities Now</button>
                    </div>
                </div>

                <table>
                    <thead>
                        <tr>
                            <th>Target</th>
                            <th>Status</th>
                            <th>Last Checked</th>
                            <th>Last Alert Sent</th>
                            <th>Active</th>
                            <th>Actions</th>
                        </tr>
                    </thead>
                    <tbody id="monitorsTableBody">
                        <tr><td colspan="6" style="text-align: center; color: #94a3b8;">Loading monitored targets...</td></tr>
                    </tbody>
                </table>
            </div>
        </div>

        <script>
            function switchTab(tabId) {
                document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
                document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
                event.target.classList.add('active');
                document.getElementById(tabId).classList.add('active');
                if (tabId === 'monitorTab') {
                    loadTelegramSettings();
                    loadMonitors();
                    checkDaemonStatus();
                } else {
                    loadOrders();
                }
            }

            // --- ORDERS QUEUE LOGIC ---
            async function loadOrders() {
                try {
                    const res = await fetch('/api/orders');
                    const data = await res.json();
                    renderBoard(data.orders);
                } catch (e) {
                    console.error("Failed to load orders", e);
                }
            }

            function renderBoard(orders) {
                const tbody = document.getElementById('ordersTableBody');
                const stats = { waiting: 0, searching: 0, code_pending: 0, booked: 0, failed: 0 };

                if (orders.length === 0) {
                    tbody.innerHTML = '<tr><td colspan="7" style="text-align: center; color: #94a3b8;">No orders in queue yet.</td></tr>';
                } else {
                    tbody.innerHTML = orders.map(o => {
                        stats[o.status] = (stats[o.status] || 0) + 1;
                        const isPending = o.status === 'code_pending';
                        return `
                            <tr style="${isPending ? 'background: #fff1f2; outline: 2px solid #f43f5e;' : ''}">
                                <td><strong>${o.order_id}</strong></td>
                                <td>${o.name} <br><small style="color: #64748b;">${o.citizenship}</small></td>
                                <td>${o.doc_type}: <code>${o.doc_number}</code></td>
                                <td>${o.procedure} <br><small style="color: #64748b;">${o.province} ${o.office ? '('+o.office+')' : ''}</small></td>
                                <td>${o.email}<br><small style="color: #64748b;">${o.phone}</small></td>
                                <td>
                                    <span class="badge badge-${o.status}">${o.status}</span>
                                    ${isPending ? `
                                        <div style="margin-top: 0.35rem; display: flex; gap: 0.3rem;">
                                            <input type="text" id="otp_${o.order_id}" placeholder="SMS Code" style="width: 90px; padding: 0.2rem; font-size: 0.8rem;">
                                            <button class="btn btn-success" style="padding: 0.2rem 0.5rem; font-size: 0.75rem;" onclick="submitOtp('${o.order_id}')">Submit</button>
                                        </div>
                                    ` : ''}
                                </td>
                                <td>
                                    <div style="display: flex; gap: 0.4rem; align-items: center; flex-wrap: wrap;">
                                        <button class="btn-action btn-success" title="Auto-fill and claim slot automatically" onclick="autoBook('${o.order_id}')">🤖 Auto</button>
                                        <button class="btn-action" title="Launch visible assistant in browser" onclick="launchAssistant('${o.order_id}')">🚀 Launch</button>
                                        <select style="padding: 0.3rem;" onchange="updateOrderStatus('${o.order_id}', this.value)">
                                            <option value="waiting" ${o.status==='waiting'?'selected':''}>waiting</option>
                                            <option value="searching" ${o.status==='searching'?'selected':''}>searching</option>
                                            <option value="code_pending" ${o.status==='code_pending'?'selected':''}>code pending</option>
                                            <option value="booked" ${o.status==='booked'?'selected':''}>booked</option>
                                            <option value="failed" ${o.status==='failed'?'selected':''}>failed</option>
                                        </select>
                                    </div>
                                </td>
                            </tr>
                        `;
                    }).join('');
                }

                document.getElementById('statsRow').innerHTML = `
                    <div class="stat-card"><div class="stat-title">Waiting</div><div class="stat-value" style="color: #92400e;">${stats.waiting}</div></div>
                    <div class="stat-card"><div class="stat-title">Searching</div><div class="stat-value" style="color: #0369a1;">${stats.searching}</div></div>
                    <div class="stat-card"><div class="stat-title">Code Pending</div><div class="stat-value" style="color: #9d174d;">${stats.code_pending}</div></div>
                    <div class="stat-card"><div class="stat-title">Booked</div><div class="stat-value" style="color: #15803d;">${stats.booked}</div></div>
                    <div class="stat-card"><div class="stat-title">Failed</div><div class="stat-value" style="color: #b91c1c;">${stats.failed}</div></div>
                `;
            }

            async function updateOrderStatus(orderId, newStatus) {
                try {
                    await fetch(`/api/orders/${orderId}/status`, {
                        method: 'PATCH',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ status: newStatus })
                    });
                    loadOrders();
                } catch (e) {
                    alert('Error updating status: ' + e.message);
                }
            }

            async function submitOtp(orderId) {
                const inputEl = document.getElementById(`otp_${orderId}`);
                if (!inputEl) return;
                const code = inputEl.value.trim();
                if (!code) return alert("Please enter the SMS code first.");
                try {
                    const res = await fetch(`/api/orders/${orderId}/submit-otp`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ otp_code: code })
                    });
                    const d = await res.json();
                    alert(d.message || "OTP code submitted!");
                    loadOrders();
                } catch (e) {
                    alert("Error submitting OTP: " + e.message);
                }
            }

            async function autoBook(orderId) {
                try {
                    const res = await fetch(`/api/orders/${orderId}/auto-book`, { method: 'POST' });
                    const d = await res.json();
                    alert(d.message || "Auto-booker initiated.");
                    loadOrders();
                } catch (e) {
                    alert("Error launching auto-booker: " + e.message);
                }
            }

            async function launchAssistant(orderId) {
                try {
                    const res = await fetch(`/api/orders/${orderId}/launch-assistant`, { method: 'POST' });
                    const data = await res.json();
                    alert(data.message || 'Assisted browser session launched.');
                    loadOrders();
                } catch (e) {
                    alert('Error launching assistant: ' + e.message);
                }
            }

            async function purgeData() {
                if (!confirm("Are you sure you want to scrub personal data from finished (booked/failed) orders?")) return;
                try {
                    const res = await fetch('/api/orders/purge-completed', { method: 'POST' });
                    const data = await res.json();
                    alert(data.message);
                    loadOrders();
                } catch (e) {
                    alert('Error purging data: ' + e.message);
                }
            }

            // --- TELEGRAM & LIVE MONITOR LOGIC ---
            async function loadTelegramSettings() {
                try {
                    const res = await fetch('/api/config/telegram');
                    const data = await res.json();
                    document.getElementById('tgEnabled').checked = data.enabled;
                    document.getElementById('tgChatId').value = data.chat_id || '';
                    if (data.bot_token_set) {
                        document.getElementById('tokenMaskedNotice').innerText = `Current token: ${data.bot_token_masked} (leave blank to keep)`;
                    } else {
                        document.getElementById('tokenMaskedNotice').innerText = 'No bot token configured yet.';
                    }
                } catch (e) {
                    console.error("Failed to load Telegram settings", e);
                }
            }

            async function saveTelegramSettings() {
                const box = document.getElementById('tgAlertBox');
                box.className = 'alert-box';
                const payload = {
                    enabled: document.getElementById('tgEnabled').checked,
                    bot_token: document.getElementById('tgBotToken').value,
                    chat_id: document.getElementById('tgChatId').value
                };
                try {
                    const res = await fetch('/api/config/telegram', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(payload)
                    });
                    const data = await res.json();
                    box.className = 'alert-box success';
                    box.innerText = data.message;
                    loadTelegramSettings();
                } catch (e) {
                    box.className = 'alert-box error';
                    box.innerText = 'Failed to save settings: ' + e.message;
                }
            }

            async function testTelegramBroadcast() {
                const box = document.getElementById('tgAlertBox');
                box.className = 'alert-box';
                box.innerText = 'Sending broadcast ping to Telegram...';
                try {
                    const res = await fetch('/api/telegram/test-broadcast', { method: 'POST' });
                    const data = await res.json();
                    if (res.ok && data.status === 'success') {
                        box.className = 'alert-box success';
                        box.innerHTML = `✅ <strong>Broadcast Success!</strong> Delivered test message to: <code>${data.results.success.join(', ')}</code>`;
                    } else {
                        box.className = 'alert-box error';
                        box.innerHTML = `❌ <strong>Broadcast Failed:</strong> ${data.detail || JSON.stringify(data.results.failed)}`;
                    }
                } catch (e) {
                    box.className = 'alert-box error';
                    box.innerText = 'Network error testing Telegram: ' + e.message;
                }
            }

            let ALL_MONITORS = [];
            let CURRENT_FILTER = 'all';

            async function loadMonitors() {
                try {
                    const res = await fetch('/api/live-status');
                    const data = await res.json();
                    ALL_MONITORS = data.monitors || [];

                    const openList = ALL_MONITORS.filter(m => m.last_status === 'available');
                    const closedList = ALL_MONITORS.filter(m => m.last_status !== 'available');

                    const cntTotal = document.getElementById('badgeTotalCities');
                    const cntOpen = document.getElementById('badgeOpenCities');
                    const cntClosed = document.getElementById('badgeClosedCities');
                    if (cntTotal) cntTotal.innerText = ALL_MONITORS.length;
                    if (cntOpen) cntOpen.innerText = openList.length;
                    if (cntClosed) cntClosed.innerText = closedList.length;

                    // Render Hero Showcase
                    const heroSection = document.getElementById('heroOpenSection');
                    const heroCards = document.getElementById('heroOpenCards');
                    const heroCount = document.getElementById('heroOpenCount');

                    if (heroSection) {
                        if (openList.length > 0) {
                            heroSection.style.display = 'block';
                            heroCount.innerText = openList.length;
                            heroCards.innerHTML = openList.map(m => `
                                <div style="background: white; border: 1px solid #86efac; border-radius: 8px; padding: 0.85rem; box-shadow: 0 2px 4px rgba(0,0,0,0.06);">
                                    <div style="display: flex; justify-content: space-between; align-items: flex-start;">
                                        <div>
                                            <strong style="font-size: 1.1rem; color: #15803d;">📍 ${m.province}</strong><br>
                                            <small style="color: #475569;">${m.procedure}</small>
                                        </div>
                                        <span class="badge badge-available">OPEN NOW</span>
                                    </div>
                                    <div style="margin-top: 0.5rem; font-size: 0.8rem; color: #64748b;">
                                        ⏰ Detectado: ${m.last_alert_at ? new Date(m.last_alert_at).toLocaleTimeString() : 'Recientemente'}
                                    </div>
                                    <div style="margin-top: 0.6rem; display: flex; gap: 0.4rem;">
                                        <a href="${m.portal_url || 'https://icp.administracionelectronica.gob.es/icpplus/index.html'}" target="_blank" class="btn btn-action" style="background: #15803d; text-decoration: none;">🔗 Open Portal</a>
                                        <button class="btn btn-action btn-success" onclick="triggerAutoPick('${m.province}', '${m.procedure}')">🤖 Auto-Book</button>
                                    </div>
                                </div>
                            `).join('');
                        } else {
                            heroSection.style.display = 'none';
                        }
                    }

                    renderFilteredMonitors();
                } catch (e) {
                    console.error("Failed to load monitors", e);
                }
            }

            function setMonitorFilter(filter) {
                CURRENT_FILTER = filter;
                const tabAll = document.getElementById('tabFilterAll');
                const tabOpen = document.getElementById('tabFilterOpen');
                const tabClosed = document.getElementById('tabFilterClosed');
                if (tabAll) tabAll.style.opacity = filter === 'all' ? '1' : '0.6';
                if (tabOpen) tabOpen.style.opacity = filter === 'available' ? '1' : '0.6';
                if (tabClosed) tabClosed.style.opacity = filter === 'no_slots' ? '1' : '0.6';
                renderFilteredMonitors();
            }

            function filterMonitors() {
                renderFilteredMonitors();
            }

            function renderFilteredMonitors() {
                const searchEl = document.getElementById('citySearch');
                const searchVal = searchEl ? searchEl.value.toLowerCase().trim() : '';
                let filtered = ALL_MONITORS;

                if (CURRENT_FILTER === 'available') {
                    filtered = filtered.filter(m => m.last_status === 'available');
                } else if (CURRENT_FILTER === 'no_slots') {
                    filtered = filtered.filter(m => m.last_status !== 'available');
                }

                if (searchVal) {
                    filtered = filtered.filter(m => 
                        m.province.toLowerCase().includes(searchVal) || 
                        m.procedure.toLowerCase().includes(searchVal)
                    );
                }

                const tbody = document.getElementById('monitorsTableBody');
                if (!tbody) return;

                if (filtered.length === 0) {
                    tbody.innerHTML = '<tr><td colspan="6" style="text-align: center; color: #94a3b8; padding: 2rem;">No matching cities found.</td></tr>';
                    return;
                }

                tbody.innerHTML = filtered.map(m => {
                    const isAvail = m.last_status === 'available';
                    const statusClass = `badge-${m.last_status || 'pending'}`;
                    const statusLabel = isAvail ? '🟢 SLOTS OPEN!' : (m.last_status || 'pending');
                    return `
                        <tr style="${isAvail ? 'background: #f0fdf4; font-weight: 500;' : ''}">
                            <td>
                                <strong style="font-size: 0.95rem;">${m.province}</strong><br>
                                <small style="color: #64748b;">${m.procedure}</small>
                            </td>
                            <td>
                                <span class="badge ${statusClass}">${statusLabel}</span><br>
                                <small style="color: #64748b;">${m.last_status_message || ''}</small>
                            </td>
                            <td>${m.last_checked ? new Date(m.last_checked).toLocaleTimeString() : 'Never'}</td>
                            <td>${m.last_alert_at ? new Date(m.last_alert_at).toLocaleTimeString() : 'No alerts yet'}</td>
                            <td>
                                <input type="checkbox" ${m.is_active ? 'checked' : ''} onchange="toggleMonitorActive(${m.id})">
                            </td>
                            <td>
                                <div style="display: flex; gap: 0.3rem;">
                                    <button class="btn-action" onclick="checkMonitorNow(${m.id})">⚡ Scan</button>
                                    <button class="btn-action btn-danger" onclick="deleteMonitor(${m.id})">🗑️</button>
                                </div>
                            </td>
                        </tr>
                    `;
                }).join('');
            }

            async function seedAllSpanishCities() {
                if (!confirm("This will add all 30+ major Spanish provinces (Madrid, Barcelona, Valencia, Sevilla, Málaga, etc.) to your live monitor. Proceed?")) return;
                try {
                    const res = await fetch('/api/monitors/seed-all-provinces', { method: 'POST' });
                    const data = await res.json();
                    alert(data.message);
                    loadMonitors();
                } catch(e) {
                    alert("Error adding provinces: " + e.message);
                }
            }

            async function scanAllCitiesNow() {
                try {
                    const res = await fetch('/api/monitors/scan-all', { method: 'POST' });
                    const data = await res.json();
                    alert(data.message || "Batch scan across all cities started in background!");
                    setTimeout(loadMonitors, 2000);
                } catch(e) {
                    alert("Error scanning all cities: " + e.message);
                }
            }

            function triggerAutoPick(prov, proc) {
                alert(`Starting automated booking for the next client waiting in ${prov}...`);
            }

            function toggleAddMonitorModal() {
                const el = document.getElementById('addMonitorForm');
                el.style.display = el.style.display === 'none' ? 'block' : 'none';
            }

            async function saveNewMonitor() {
                const prov = document.getElementById('newMonProvince').value.trim();
                const proc = document.getElementById('newMonProc').value.trim();
                if (!prov || !proc) return;
                try {
                    await fetch('/api/monitors', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ province: prov, procedure: proc })
                    });
                    toggleAddMonitorModal();
                    loadMonitors();
                } catch (e) {
                    alert('Error adding monitor: ' + e.message);
                }
            }

            async function toggleMonitorActive(id) {
                try {
                    await fetch(`/api/monitors/${id}/toggle`, { method: 'PATCH' });
                    loadMonitors();
                } catch (e) {
                    alert('Error toggling monitor: ' + e.message);
                }
            }

            async function deleteMonitor(id) {
                if (!confirm("Delete this monitored target?")) return;
                try {
                    await fetch(`/api/monitors/${id}`, { method: 'DELETE' });
                    loadMonitors();
                } catch (e) {
                    alert('Error deleting monitor: ' + e.message);
                }
            }

            async function checkMonitorNow(id) {
                try {
                    const res = await fetch(`/api/monitors/${id}/check`, { method: 'POST' });
                    const data = await res.json();
                    alert(data.message);
                    setTimeout(loadMonitors, 2000);
                } catch (e) {
                    alert('Error triggering check: ' + e.message);
                }
            }

            // --- DAEMON CONTROLS ---
            async function checkDaemonStatus() {
                try {
                    const res = await fetch('/api/monitor-daemon/status');
                    const data = await res.json();
                    const statusText = document.getElementById('daemonStatusText');
                    const btnStart = document.getElementById('btnStartDaemon');
                    const btnStop = document.getElementById('btnStopDaemon');
                    if (data.is_running) {
                        statusText.innerHTML = `Status: <span style="color: #16a34a; font-weight: bold;">🟢 RUNNING (PID ${data.pid})</span> — Scanning live appointment slots...`;
                        btnStart.style.display = 'none';
                        btnStop.style.display = 'inline-flex';
                    } else {
                        statusText.innerHTML = `Status: <span style="color: #dc2626; font-weight: bold;">⏹️ STOPPED</span> — Not scanning in background.`;
                        btnStart.style.display = 'inline-flex';
                        btnStop.style.display = 'none';
                    }
                } catch (e) {
                    console.error("Failed to check daemon status", e);
                }
            }

            async function toggleDaemon(action) {
                try {
                    await fetch(`/api/monitor-daemon/${action}`, { method: 'POST' });
                    setTimeout(checkDaemonStatus, 1000);
                } catch (e) {
                    alert('Error updating daemon state: ' + e.message);
                }
            }

            function refreshAll() {
                loadOrders();
                loadMonitors();
                checkDaemonStatus();
            }

            loadOrders();
            setInterval(loadOrders, 5000);
        </script>
    </body>
    </html>
    """


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("queue_server:app", host="0.0.0.0", port=port, reload=False)
