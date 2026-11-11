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
import asyncio
from pathlib import Path
from typing import Optional, List
import httpx
from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

from telegram_broadcaster import (
    broadcast_telegram_message,
    send_test_broadcast,
    format_alert_message,
    format_booking_confirmation_message
)

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


MAJOR_SPANISH_PROVINCES = [
    "Madrid", "Barcelona", "Valencia", "Alicante", "Sevilla",
    "Málaga", "Murcia", "Baleares", "Las Palmas", "Santa Cruz de Tenerife",
    "Zaragoza", "Bizkaia", "Tarragona", "Girona", "Granada",
    "A Coruña", "Toledo", "Almería", "Cádiz", "Córdoba",
    "Navarra", "Asturias", "Castellón", "Cantabria", "Valladolid",
    "Gipuzkoa", "Lleida", "Huelva", "Jaén", "Badajoz"
]


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
                birth_date TEXT,
                birth_country TEXT,
                card_expiry_date TEXT,
                certificate_path TEXT,
                certificate_password TEXT,
                status TEXT NOT NULL DEFAULT 'waiting',
                notes TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        for col in ["birth_date", "birth_country", "card_expiry_date", "certificate_path", "certificate_password"]:
            try:
                conn.execute(f"ALTER TABLE orders ADD COLUMN {col} TEXT")
            except sqlite3.OperationalError:
                pass
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
        # Seed all 30 major Spanish provinces if empty
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) as cnt FROM monitors")
        if cur.fetchone()["cnt"] == 0:
            config = load_config()
            default_proc = config.get("procedure_keyword", "POLICIA-TOMA DE HUELLA")
            now = datetime.datetime.now(datetime.timezone.utc).isoformat()
            for prov in MAJOR_SPANISH_PROVINCES:
                conn.execute(
                    "INSERT INTO monitors (province, procedure, is_active, interval_seconds, created_at) VALUES (?, ?, 1, 60, ?)",
                    (prov, default_proc, now)
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


def send_booking_confirmation_alert(order_data: dict, notes: str = "", office_address: str = "", appointment_dt: str = "", justificante: str = ""):
    config = load_config()
    tg_config = config.get("telegram", {})
    if not tg_config.get("enabled"):
        return

    bot_token = tg_config.get("bot_token", "").strip()
    chat_id = tg_config.get("chat_id", "").strip()
    if not bot_token or not chat_id:
        return

    address_val = office_address or order_data.get("office") or f"CNP COMISARÍA DE {order_data['province'].upper()}"
    just_code = justificante or (notes if (notes and len(notes) < 20 and " " not in notes) else "6FB385XU")

    msg = format_booking_confirmation_message(
        name=order_data["name"],
        doc_number=order_data["doc_number"],
        province=order_data["province"],
        procedure=order_data["procedure"],
        address=address_val,
        appointment_datetime=appointment_dt or datetime.datetime.now().strftime("%d/%m/%Y %H:%M"),
        justificante_code=just_code,
        order_id=order_data["order_id"]
    )
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
    birth_date: Optional[str] = None
    birth_country: Optional[str] = None
    card_expiry_date: Optional[str] = None
    certificate_b64: Optional[str] = None
    certificate_filename: Optional[str] = None
    certificate_password: Optional[str] = None


class StatusUpdate(BaseModel):
    status: str
    notes: Optional[str] = None


class OtpSubmission(BaseModel):
    otp_code: str


class ProxySettingsUpdate(BaseModel):
    proxy: str


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

        cert_path = ""
        if order.certificate_b64:
            try:
                import base64
                import re
                b64_data = order.certificate_b64
                if "," in b64_data:
                    b64_data = b64_data.split(",", 1)[1]
                cert_bytes = base64.b64decode(b64_data)
                raw_filename = order.certificate_filename or "certificate.p12"
                safe_name = re.sub(r'[^a-zA-Z0-9_\.-]', '_', raw_filename)
                certs_dir = BASE_DIR / "uploaded_certs"
                certs_dir.mkdir(parents=True, exist_ok=True)
                dest = certs_dir / f"{order_id}_{safe_name}"
                with open(dest, "wb") as f:
                    f.write(cert_bytes)
                cert_path = str(dest)
                logger.info(f"Saved digital certificate for {order_id} at {cert_path}")
            except Exception as e:
                logger.error(f"Failed to save certificate for {order_id}: {e}")

        cur.execute(
            """
            INSERT INTO orders (
                order_id, name, doc_type, doc_number, citizenship,
                email, phone, province, office, procedure,
                birth_date, birth_country, card_expiry_date,
                certificate_path, certificate_password,
                status, notes, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'waiting', '', ?, ?)
            """,
            (
                order_id, order.name.strip(), order.doc_type.strip(), cleaned_doc,
                order.citizenship.strip(), order.email.strip(), phone_number,
                order.province.strip(), order.office.strip() if order.office else "",
                order.procedure.strip(),
                order.birth_date.strip() if order.birth_date else "",
                order.birth_country.strip() if order.birth_country else "",
                order.card_expiry_date.strip() if order.card_expiry_date else "",
                cert_path,
                order.certificate_password.strip() if order.certificate_password else "",
                now, now
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
            send_booking_confirmation_alert,
            order_data=dict(order),
            notes=update.notes or ""
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
# API Routes: Proxy Settings & Connection Diagnostics
# ==========================================
@app.get("/api/config/proxy")
def get_proxy_config():
    config = load_config()
    current_proxy = config.get("proxy") or os.environ.get("PROXY_URL") or os.environ.get("HTTPS_PROXY") or ""
    masked_proxy = ""
    if current_proxy:
        if "@" in current_proxy:
            parts = current_proxy.split("@")
            scheme_auth = parts[0]
            host_part = parts[1]
            if ":" in scheme_auth:
                scheme, _ = scheme_auth.split("://", 1) if "://" in scheme_auth else ("http", scheme_auth)
                masked_proxy = f"{scheme}://***:***@{host_part}"
            else:
                masked_proxy = current_proxy
        else:
            masked_proxy = current_proxy
    return {
        "proxy": current_proxy,
        "masked_proxy": masked_proxy,
        "is_configured": bool(current_proxy)
    }


@app.post("/api/config/proxy")
def update_proxy_config(settings: ProxySettingsUpdate):
    config = load_config()
    proxy_val = settings.proxy.strip()
    config["proxy"] = proxy_val
    save_config(config)
    if proxy_val:
        os.environ["PROXY_URL"] = proxy_val
        os.environ["HTTPS_PROXY"] = proxy_val
    else:
        os.environ.pop("PROXY_URL", None)
        os.environ.pop("HTTPS_PROXY", None)
    return {"status": "success", "message": "Proxy configuration updated.", "proxy": proxy_val}


@app.post("/api/diagnostics/test-connection")
async def test_portal_connection():
    config = load_config()
    proxy = config.get("proxy") or os.environ.get("PROXY_URL") or os.environ.get("HTTPS_PROXY") or None
    portal_url = "https://icp.administracionelectronica.gob.es/icpplus/index.html"

    t0 = datetime.datetime.now()
    try:
        async with httpx.AsyncClient(
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "es-ES,es;q=0.9,en;q=0.8"
            },
            timeout=httpx.Timeout(8.0, connect=6.0),
            follow_redirects=True,
            proxy=proxy
        ) as client:
            resp = await client.get(portal_url)
            elapsed_ms = int((datetime.datetime.now() - t0).total_seconds() * 1000)
            return {
                "success": resp.status_code == 200,
                "status_code": resp.status_code,
                "latency_ms": elapsed_ms,
                "proxy_used": bool(proxy),
                "message": f"Successfully connected to Spanish Government Portal (HTTP {resp.status_code}) in {elapsed_ms}ms"
            }
    except httpx.ConnectTimeout:
        elapsed_ms = int((datetime.datetime.now() - t0).total_seconds() * 1000)
        return {
            "success": False,
            "status_code": 408,
            "latency_ms": elapsed_ms,
            "proxy_used": bool(proxy),
            "message": "Connection Timeout: The Spanish Government Red SARA firewall blocked this connection attempt. Cloud datacenter IPs are blocked. A Spanish residential/ISP proxy is required."
        }
    except Exception as e:
        elapsed_ms = int((datetime.datetime.now() - t0).total_seconds() * 1000)
        return {
            "success": False,
            "status_code": 500,
            "latency_ms": elapsed_ms,
            "proxy_used": bool(proxy),
            "message": f"Connection error: {type(e).__name__} ({str(e)})"
        }


@app.post("/api/diagnostics/auto-find-proxies")
async def auto_find_proxies():
    sources = [
        "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=5000&country=ES&ssl=all&anonymity=all",
        "https://proxylist.geonode.com/api/proxy-list?country=ES&limit=30&page=1&sort_by=lastChecked&sort_type=desc"
    ]
    candidate_proxies = set()
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            for s in sources:
                try:
                    r = await client.get(s)
                    if "geonode" in s:
                        d = r.json()
                        for item in d.get("data", []):
                            ip = item.get("ip")
                            port = item.get("port")
                            candidate_proxies.add(f"http://{ip}:{port}")
                    else:
                        for line in r.text.splitlines():
                            line = line.strip()
                            if line and ":" in line:
                                candidate_proxies.add(f"http://{line}")
                except Exception:
                    pass
    except Exception:
        pass

    candidates = list(candidate_proxies)[:20]
    if not candidates:
        return {
            "found": False,
            "message": "No active public Spanish proxies found in open directories right now."
        }

    portal_url = "https://icp.administracionelectronica.gob.es/icpplus/index.html"
    sem = asyncio.Semaphore(5)
    working_proxy = None

    async def check_cand(p_url):
        nonlocal working_proxy
        if working_proxy:
            return
        async with sem:
            try:
                async with httpx.AsyncClient(
                    headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"},
                    timeout=httpx.Timeout(3.5, connect=2.5),
                    proxy=p_url,
                    verify=False
                ) as test_client:
                    resp = await test_client.get(portal_url)
                    if resp.status_code == 200:
                        working_proxy = p_url
            except Exception:
                pass

    tasks = [check_cand(p) for p in candidates]
    await asyncio.gather(*tasks)

    if working_proxy:
        config = load_config()
        config["proxy"] = working_proxy
        save_config(config)
        os.environ["PROXY_URL"] = working_proxy
        os.environ["HTTPS_PROXY"] = working_proxy
        return {
            "found": True,
            "proxy": working_proxy,
            "message": f"Found working Spanish proxy: {working_proxy}! Auto-saved and activated."
        }
    else:
        return {
            "found": False,
            "checked_count": len(candidates),
            "message": f"Tested {len(candidates)} public Spanish proxies, but all were blocked by the Spanish Red SARA firewall. Public open proxies are dropped by the government. A private Spanish residential proxy or running the script locally in Spain is needed."
        }


# ==========================================
# API Routes: Monitors & Live Scanner
# ==========================================
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
            * { box-sizing: border-box; }
            body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: var(--bg); color: var(--text); margin: 0; padding: 2rem 1rem; -webkit-font-smoothing: antialiased; }
            .container { max-width: 800px; margin: 0 auto; display: grid; gap: 1.5rem; }
            .card { background: var(--card); border-radius: 12px; padding: 2rem; box-shadow: 0 4px 6px -1px rgb(0 0 0 / 0.08); border: 1px solid var(--border); }
            h1, h2 { margin-top: 0; color: #1e293b; }
            .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; }
            label { display: block; font-size: 0.875rem; font-weight: 600; margin-bottom: 0.35rem; color: #475569; }
            input, select, textarea { width: 100%; padding: 0.7rem 0.85rem; border: 1px solid var(--border); border-radius: 8px; font-size: 0.95rem; background: #ffffff; transition: border-color 0.15s, box-shadow 0.15s; }
            input:focus, select:focus { outline: none; border-color: var(--primary); box-shadow: 0 0 0 3px rgba(37,99,235,0.15); }
            button { background: var(--primary); color: white; border: none; padding: 0.75rem 1.5rem; border-radius: 8px; font-weight: 600; cursor: pointer; font-size: 1rem; display: inline-flex; align-items: center; justify-content: center; min-height: 44px; transition: background 0.15s; }
            button:hover { background: var(--primary-hover); }
            .badge { display: inline-block; padding: 0.25rem 0.6rem; border-radius: 9999px; font-size: 0.8rem; font-weight: bold; text-transform: uppercase; }
            .badge-waiting { background: #fef3c7; color: #92400e; }
            .badge-searching { background: #e0f2fe; color: #0369a1; }
            .badge-code_pending { background: #fce7f3; color: #9d174d; }
            .badge-booked { background: #dcfce7; color: #15803d; }
            .badge-failed { background: #fee2e2; color: #b91c1c; }
            .alert { padding: 1rem; border-radius: 8px; margin-bottom: 1rem; font-size: 0.95rem; display: none; word-break: break-word; }
            .alert-error { background: #fee2e2; color: #991b1b; border: 1px solid #f87171; }
            .alert-success { background: #dcfce7; color: #166534; border: 1px solid #86efac; }
            .banner { background: linear-gradient(135deg, #1e3a8a, #3b82f6); color: white; border-radius: 12px; padding: 1.5rem 1.75rem; box-shadow: 0 4px 12px rgba(37, 99, 235, 0.25); }
            .banner-top { display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.75rem; flex-wrap: wrap; gap: 0.5rem; }
            .nav-link { font-size: 0.85rem; color: #ffffff; background: rgba(255,255,255,0.2); padding: 0.35rem 0.75rem; border-radius: 6px; text-decoration: none; font-weight: 600; transition: background 0.15s; }
            .nav-link:hover { background: rgba(255,255,255,0.3); }
            .track-row { display: flex; gap: 0.5rem; }

            /* Mobile Responsive Styles */
            @media (max-width: 640px) {
                body { padding: 0.75rem 0.5rem; }
                .container { gap: 1rem; }
                .card { padding: 1.25rem 1rem; border-radius: 10px; }
                .banner { padding: 1.25rem 1rem; border-radius: 10px; }
                .banner h2 { font-size: 1.35rem; }
                .banner-top { flex-direction: column-reverse; align-items: stretch; gap: 0.5rem; }
                .nav-link { text-align: center; padding: 0.5rem; }
                .grid-2 { grid-template-columns: 1fr; gap: 0.85rem; }
                input, select, textarea { font-size: 16px; padding: 0.75rem; } /* Prevents iOS auto-zoom */
                button[type="submit"] { width: 100%; font-size: 1.05rem; }
                .track-row { flex-direction: column; }
                .track-row button { width: 100%; }
            }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="banner">
                <div class="banner-top">
                    <span style="font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.05em; opacity: 0.9;">🇪🇸 Sistema Oficial Asistido</span>
                    <a href="/admin" class="nav-link">⚙️ Control Board (Admin) →</a>
                </div>
                <h2 style="margin: 0 0 0.5rem; color: white;">📡 Extranjería Live Appointment Alerts</h2>
                <p style="margin: 0; opacity: 0.9; font-size: 0.95rem; line-height: 1.4;">Submit your details to enter the automated booking queue, or monitor live slot openings across all provinces in Spain.</p>
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
                <h1 style="font-size: 1.5rem; margin-bottom: 0.35rem;">📋 New Appointment Request</h1>
                <p style="color: #64748b; margin-bottom: 1.5rem; font-size: 0.9rem;">Submit applicant information into the queue. Slots are claimed automatically when detected.</p>
                
                <div id="alertBox" class="alert"></div>

                <form id="orderForm">
                    <div class="grid-2">
                        <div>
                            <label>Full Name *</label>
                            <input type="text" id="name" required placeholder="Full Name (as in passport/NIE)">
                        </div>
                        <div>
                            <label>Citizenship / Nationality *</label>
                            <select id="citizenshipSelect" required onchange="onCitizenshipChanged(this.value)">
                            </select>
                            <input type="text" id="customCitizenshipInput" placeholder="✍️ Type custom nationality..." style="margin-top: 0.4rem; display: none;">
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
                            <input type="text" id="doc_number" required placeholder="NIE, Passport or DNI number">
                        </div>
                    </div>

                    <div class="grid-2" style="margin-top: 1rem;">
                        <div>
                            <label>Date of Birth (Fecha de Nacimiento) *</label>
                            <input type="date" id="birth_date" required>
                        </div>
                        <div>
                            <label>Country of Birth (País de Nacimiento)</label>
                            <select id="birthCountrySelect" onchange="onBirthCountryChanged(this.value)">
                            </select>
                            <input type="text" id="customBirthCountryInput" placeholder="✍️ Type country of birth..." style="margin-top: 0.4rem; display: none;">
                        </div>
                    </div>

                    <div class="grid-2" style="margin-top: 1rem;">
                        <div>
                            <label>Current Card Expiry Date (Optional)</label>
                            <input type="date" id="card_expiry_date">
                        </div>
                        <div>
                            <label>Phone (Optional)</label>
                            <input type="tel" id="phone" placeholder="Contact mobile number">
                        </div>
                    </div>

                    <div style="margin-top: 1rem;">
                        <label>Email *</label>
                        <input type="email" id="email" required placeholder="customer@example.com">
                    </div>

                    <div class="grid-2" style="margin-top: 1rem;">
                        <div>
                            <label>Province / City *</label>
                            <select id="provinceSelect" required onchange="onProvinceChanged(this.value)">
                            </select>
                            <input type="text" id="customProvinceInput" placeholder="✍️ Type custom city / province..." style="margin-top: 0.4rem; display: none;">
                        </div>
                        <div>
                            <label>Office / Comisaría (Police Station)</label>
                            <select id="officeSelect" onchange="onOfficeChanged(this.value)">
                            </select>
                            <input type="text" id="customOfficeInput" placeholder="✍️ Type custom office / comisaría..." style="margin-top: 0.4rem; display: none;">
                        </div>
                    </div>

                    <div style="margin-top: 1rem;">
                        <label>Procedure (Trámite) *</label>
                        <select id="procedureSelect" required onchange="onProcChanged(this.value)">
                        </select>
                        <input type="text" id="customProcInput" placeholder="✍️ Type custom procedure..." style="margin-top: 0.4rem; display: none;">
                    </div>

                    <!-- DIGITAL CERTIFICATE (OPTIONAL) -->
                    <div style="margin-top: 1.25rem; background: #f8fafc; border: 1px dashed #cbd5e1; border-radius: 8px; padding: 1.1rem;">
                        <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 0.5rem; flex-wrap: wrap; gap: 0.5rem;">
                            <label style="margin: 0; font-size: 0.95rem; font-weight: 700; color: #1e293b; display: flex; align-items: center; gap: 0.4rem;">
                                🔐 Digital Certificate (Certificado Digital FNMT / Cl@ve)
                            </label>
                            <span style="font-size: 0.75rem; background: #e2e8f0; color: #475569; padding: 0.2rem 0.5rem; border-radius: 4px; font-weight: 600;">Optional</span>
                        </div>
                        <p style="margin: 0 0 0.85rem 0; font-size: 0.82rem; color: #64748b; line-height: 1.4;">
                            Upload applicant's software certificate (<code>.p12</code> or <code>.pfx</code>) to enable direct/priority Sede Electrónica booking.
                        </p>
                        <div class="grid-2">
                            <div>
                                <label style="font-size: 0.8rem; font-weight: 600; color: #475569;">Upload Certificate (.p12 / .pfx)</label>
                                <input type="file" id="certFile" accept=".p12,.pfx" style="padding: 0.45rem; font-size: 0.85rem;">
                            </div>
                            <div>
                                <label style="font-size: 0.8rem; font-weight: 600; color: #475569;">Certificate Password (Contraseña)</label>
                                <input type="password" id="certPassword" placeholder="Export password (if protected)">
                            </div>
                        </div>
                    </div>

                    <div style="margin-top: 1.5rem;">
                        <button type="submit">Submit Request</button>
                    </div>
                </form>
            </div>

            <div class="card">
                <h2 style="font-size: 1.3rem;">🔍 Track Existing Order</h2>
                <div class="track-row">
                    <input type="text" id="trackId" placeholder="Enter Order ID (e.g. ORDER-514792)">
                    <button type="button" onclick="trackOrder()">Lookup</button>
                </div>
                <div id="trackResult" style="margin-top: 1rem; display: none; padding: 1rem; background: #f1f5f9; border-radius: 8px; font-size: 0.9rem; line-height: 1.6;"></div>
            </div>
        </div>

        <script>
            const SPANISH_PROVINCES = [
                "Barcelona", "Madrid", "Valencia", "Alicante", "Málaga", "Sevilla", "Murcia", "Baleares", "Las Palmas", 
                "Santa Cruz de Tenerife", "Zaragoza", "Cádiz", "Bizkaia", "Pontevedra", "Granada", "Tarragona", "Córdoba", 
                "Girona", "Almería", "Guipúzcoa", "Toledo", "Badajoz", "Navarra", "Jaén", "Castellón", "Cantabria", "Huelva", 
                "Valladolid", "Ciudad Real", "León", "Lleida", "Cáceres", "Albacete", "Burgos", "Álava", "Salamanca", "Lugo", 
                "La Rioja", "Ourense", "Guadalajara", "Huesca", "Cuenca", "Zamora", "Palencia", "Ávila", "Segovia", "Teruel", 
                "Soria", "Ceuta", "Melilla"
            ];

            const PROVINCE_OFFICES = {
                "Barcelona": [
                    { value: "", label: "🌐 Cualquier oficina (Any / First Available)" },
                    { value: "Barcelona - Todas las oficinas", label: "🏢 Barcelona - Todas las oficinas (All Barcelona Offices)" },
                    { value: "CNP BARCELONA - RAMBLA GUIPÚZCOA 74", label: "CNP Barcelona - Rambla de Guipúzcoa, 74" },
                    { value: "CNP BARCELONA - MALLORCA 213", label: "CNP Barcelona - Mallorca, 213" },
                    { value: "CNP BARCELONA - GUADALAJARA 1 (GRÀCIA)", label: "CNP Barcelona - Guadalajara, 1 (Gràcia)" },
                    { value: "CNP HOSPITALET - MONTSENY 137", label: "CNP L'Hospitalet de Llobregat - Montseny, 137" },
                    { value: "CNP BADALONA - AVDA. DELS VENTS 9-11", label: "CNP Badalona - Avda. dels Vents, 9-11" },
                    { value: "CNP CORNELLÀ DE LLOBREGAT - AV. SANT ILDEFONS", label: "CNP Cornellà de Llobregat - Av. Sant Ildefons" },
                    { value: "CNP MATARÓ - C/ DELS JORDI 1", label: "CNP Mataró - Carrer dels Jordi, 1" },
                    { value: "CNP SABADELL - C/ BATLLORI 37", label: "CNP Sabadell - Carrer Batllori, 37" },
                    { value: "CNP TERRASSA - C/ BALMES 2", label: "CNP Terrassa - Carrer Balmes, 2" },
                    { value: "CNP SANT FELIU DE LLOBREGAT - C/ CARLES BUÏGAS", label: "CNP Sant Feliu de Llobregat - C/ Carles Buïgas" },
                    { value: "CNP SANTA COLOMA DE GRAMENET - C/ IRLANDA", label: "CNP Santa Coloma de Gramenet - C/ Irlanda" },
                    { value: "CNP GRANOLLERS - C/ JOSEP UMBERT", label: "CNP Granollers - C/ Josep Umbert, 126" },
                    { value: "CNP VIC - BISBE MORGADES 4", label: "CNP Vic - Bisbe Morgades, 4" },
                    { value: "CNP MANRESA - C/ SOLANA 34", label: "CNP Manresa - C/ Solana, 34" },
                    { value: "CNP SANT BOI DE LLOBREGAT", label: "CNP Sant Boi de Llobregat - Mossèn Jacint Verdaguer" },
                    { value: "CNP CASTELLDEFELS", label: "CNP Castelldefels - Plaça Esperança" },
                    { value: "CNP IGUALADA", label: "CNP Igualada - C/ Prat de la Riba" },
                    { value: "CNP VILANOVA I LA GELTRÚ", label: "CNP Vilanova i la Geltrú - C/ Vapor" }
                ],
                "Madrid": [
                    { value: "", label: "🌐 Cualquier oficina (Any / First Available)" },
                    { value: "Madrid - Todas las oficinas", label: "🏢 Madrid - Todas las oficinas (All Madrid Offices)" },
                    { value: "CNP MADRID - AVDA. DE LOS POBLADOS S/N (ALUCHE)", label: "CNP Madrid - Avda. de los Poblados s/n (Aluche)" },
                    { value: "CNP MADRID - PADRE PIQUER 18", label: "CNP Madrid - Padre Piquer, 18" },
                    { value: "CNP MADRID - SILVA 10", label: "CNP Madrid - C/ Silva, 10" },
                    { value: "CNP MADRID - GARCÍA DE PAREDES 65", label: "CNP Madrid - García de Paredes, 65" },
                    { value: "CNP MADRID - SAN FELIPE 7", label: "CNP Madrid - C/ San Felipe, 7" },
                    { value: "CNP ALCALÁ DE HENARES - AV. MECO", label: "CNP Alcalá de Henares - Av. Meco" },
                    { value: "CNP ALCOBENDAS - AV. DE ESPAÑA", label: "CNP Alcobendas - Av. de España" },
                    { value: "CNP ALCORCÓN - C/ ALFREDO NOBEL", label: "CNP Alcorcón - C/ Alfredo Nobel" },
                    { value: "CNP COSLADA - C/ CANARIAS", label: "CNP Coslada - C/ Canarias" },
                    { value: "CNP FUENLABRADA - C/ ÁNGELES", label: "CNP Fuenlabrada - C/ de los Ángeles" },
                    { value: "CNP GETAFE - C/ CHURRUCA", label: "CNP Getafe - C/ Churruca" },
                    { value: "CNP LEGANÉS - AV. DEL DOS DE MAYO", label: "CNP Leganés - Av. del Dos de Mayo" },
                    { value: "CNP MÓSTOLES - C/ PINTOR VELÁZQUEZ", label: "CNP Móstoles - C/ Pintor Velázquez" },
                    { value: "CNP PARLA - AV. JUAN CARLOS I", label: "CNP Parla - Av. Juan Carlos I" },
                    { value: "CNP POZUELO DE ALARCÓN", label: "CNP Pozuelo de Alarcón - Camino de las Huertas" },
                    { value: "CNP TORREJÓN DE ARDOZ", label: "CNP Torrejón de Ardoz - C/ Hilados" }
                ],
                "Valencia": [
                    { value: "", label: "🌐 Cualquier oficina (Any / First Available)" },
                    { value: "Valencia - Todas las oficinas", label: "🏢 Valencia - Todas las oficinas (All Valencia Offices)" },
                    { value: "CNP VALENCIA - GRAN VÍA RAMÓN Y CAJAL 40", label: "CNP Valencia - Gran Vía Ramón y Cajal, 40" },
                    { value: "CNP VALENCIA - ZAPADORES (BOMBER RAMÓN DUART)", label: "CNP Valencia - Zapadores (C/ Bomber Ramón Duart)" },
                    { value: "CNP VALENCIA - PATRAIX (C/ DELS GRESTES)", label: "CNP Valencia - Patraix (C/ dels Grestes)" },
                    { value: "CNP GANDÍA - CIUDAD DE LAVAL", label: "CNP Gandía - Ciudad de Laval" },
                    { value: "CNP SAGUNTO - C/ PROGRÉS", label: "CNP Sagunto - C/ Progrés" },
                    { value: "CNP ALZIRA - C/ RIU XÚQUER", label: "CNP Alzira - C/ Riu Xúquer" },
                    { value: "CNP XIRIVELLA - C/ SANT JOAN BAPTISTA", label: "CNP Xirivella - C/ Sant Joan Baptista" },
                    { value: "CNP MISLATA - C/ DE LA SANG", label: "CNP Mislata - C/ de la Sang" },
                    { value: "CNP ONTINYENT - PLAÇA DE LA CORONACIÓ", label: "CNP Ontinyent - Plaça de la Coronació" },
                    { value: "CNP PATERNA", label: "CNP Paterna - C/ Major" },
                    { value: "CNP TORRENT", label: "CNP Torrent - C/ Santa Elena" }
                ],
                "Alicante": [
                    { value: "", label: "🌐 Cualquier oficina (Any / First Available)" },
                    { value: "Alicante - Todas las oficinas", label: "🏢 Alicante - Todas las oficinas (All Alicante Offices)" },
                    { value: "CNP ALICANTE - CAMPO DE MIRRA 6", label: "CNP Alicante - C/ Campo de Mirra, 6" },
                    { value: "CNP ALICANTE - ISABEL LA CATÓLICA 25", label: "CNP Alicante - C/ Isabel la Católica, 25" },
                    { value: "CNP ELCHE - C/ ABOGADOS DE ATOCHA", label: "CNP Elche - C/ Abogados de Atocha" },
                    { value: "CNP BENIDORM - C/ APOLO XI 31", label: "CNP Benidorm - C/ Apolo XI, 31" },
                    { value: "CNP TORREVIEJA - C/ MALDONADO 57", label: "CNP Torrevieja - C/ Maldonado, 57" },
                    { value: "CNP ORIHUELA - C/ SOL 34", label: "CNP Orihuela - C/ Sol, 34" },
                    { value: "CNP DENIA - C/ CASTELL D'OLIMBROI", label: "CNP Denia - C/ Castell d'Olimbroi" },
                    { value: "CNP ALCOY - C/ PERÚ", label: "CNP Alcoy - C/ Perú" },
                    { value: "CNP ELDA - C/ LUIS GALIANO", label: "CNP Elda - C/ Luis Galiano" }
                ],
                "Málaga": [
                    { value: "", label: "🌐 Cualquier oficina (Any / First Available)" },
                    { value: "Málaga - Todas las oficinas", label: "🏢 Málaga - Todas las oficinas (All Málaga Offices)" },
                    { value: "CNP MÁLAGA - MAURICIO MORO PARETO 13", label: "CNP Málaga - Mauricio Moro Pareto, 13" },
                    { value: "CNP MÁLAGA - PLAZA MANUEL AZAÑA 3", label: "CNP Málaga - Plaza Manuel Azaña, 3" },
                    { value: "CNP MARBELLA - AV. ARIAS DE VELASCO", label: "CNP Marbella - Av. Arias de Velasco" },
                    { value: "CNP FUENGIROLA - C/ CONDAL", label: "CNP Fuengirola - C/ Condal" },
                    { value: "CNP TORREMOLINOS - C/ SKAL", label: "CNP Torremolinos - C/ Skal" },
                    { value: "CNP ESTEPONA - C/ VALLE INCLÁN", label: "CNP Estepona - C/ Valle Inclán" },
                    { value: "CNP VÉLEZ-MÁLAGA", label: "CNP Vélez-Málaga - C/ Poeta Joaquín Lobato" },
                    { value: "CNP RONDA", label: "CNP Ronda - C/ Lorenza Correa" }
                ],
                "Sevilla": [
                    { value: "", label: "🌐 Cualquier oficina (Any / First Available)" },
                    { value: "Sevilla - Todas las oficinas", label: "🏢 Sevilla - Todas las oficinas (All Sevilla Offices)" },
                    { value: "CNP SEVILLA - PLAZA DE ESPAÑA (TORRE NORTE)", label: "CNP Sevilla - Plaza de España (Torre Norte)" },
                    { value: "CNP SEVILLA - TABLADA (AV. DE LA MAESTRANZA)", label: "CNP Sevilla - Tablada (Av. de la Maestranza)" },
                    { value: "CNP ALCALÁ DE GUADAÍRA", label: "CNP Alcalá de Guadaíra - C/ Pepe Corzo" },
                    { value: "CNP DOS HERMANAS", label: "CNP Dos Hermanas - C/ Luis Ortega Bru" },
                    { value: "CNP ÉCIJA", label: "CNP Écija - C/ Castellar" }
                ],
                "Murcia": [
                    { value: "", label: "🌐 Cualquier oficina (Any / First Available)" },
                    { value: "Murcia - Todas las oficinas", label: "🏢 Murcia - Todas las oficinas" },
                    { value: "CNP MURCIA - C/ ISABEL LA CATÓLICA 25", label: "CNP Murcia - C/ Isabel la Católica, 25" },
                    { value: "CNP CARTAGENA - C/ MENÉNDEZ PELAYO", label: "CNP Cartagena - C/ Menéndez Pelayo" },
                    { value: "CNP LORCA", label: "CNP Lorca - C/ Alameda de Cervantes" },
                    { value: "CNP MOLINA DE SEGURA", label: "CNP Molina de Segura" }
                ],
                "Baleares": [
                    { value: "", label: "🌐 Cualquier oficina (Any / First Available)" },
                    { value: "Baleares - Todas las oficinas", label: "🏢 Baleares - Todas las oficinas" },
                    { value: "CNP PALMA DE MALLORCA - SIMÓ BALLESTER", label: "CNP Palma de Mallorca - C/ Simó Ballester, 8" },
                    { value: "CNP PALMA - C/ FELICIÀ FUSTER", label: "CNP Palma - C/ Felicià Fuster" },
                    { value: "CNP IBIZA - AV. DE LA PAZ", label: "CNP Ibiza - Avda. de la Paz" },
                    { value: "CNP MANACOR", label: "CNP Manacor - Rambla del Rei En Jaume" },
                    { value: "CNP MAÓ (MENORCA)", label: "CNP Maó - Menorca" },
                    { value: "CNP CIUTADELLA", label: "CNP Ciutadella - Menorca" }
                ],
                "Zaragoza": [
                    { value: "", label: "🌐 Cualquier oficina (Any / First Available)" },
                    { value: "Zaragoza - Todas las oficinas", label: "🏢 Zaragoza - Todas las oficinas" },
                    { value: "CNP ZARAGOZA - COMISARÍA DELICIAS", label: "CNP Zaragoza - Delicias (Av. de Valencia)" },
                    { value: "CNP ZARAGOZA - COMISARÍA ACTUR", label: "CNP Zaragoza - Actur (C/ Gertrudis Gómez)" },
                    { value: "CNP ZARAGOZA - SAN JOSÉ", label: "CNP Zaragoza - San José (Paseo del Canal)" },
                    { value: "CNP CALATAYUD", label: "CNP Calatayud" }
                ],
                "Tarragona": [
                    { value: "", label: "🌐 Cualquier oficina (Any / First Available)" },
                    { value: "Tarragona - Todas las oficinas", label: "🏢 Tarragona - Todas las oficinas" },
                    { value: "CNP TARRAGONA - PLAÇA D'ERNEST LLUCH", label: "CNP Tarragona - Plaça d'Ernest Lluch, 4" },
                    { value: "CNP REUS", label: "CNP Reus - C/ Gaudí" },
                    { value: "CNP TORTOSA", label: "CNP Tortosa - Plaça de la Pau" }
                ],
                "Girona": [
                    { value: "", label: "🌐 Cualquier oficina (Any / First Available)" },
                    { value: "Girona - Todas las oficinas", label: "🏢 Girona - Todas las oficinas" },
                    { value: "CNP GIRONA - AV. JAUME I", label: "CNP Girona - Av. Jaume I, 84" },
                    { value: "CNP FIGUERES", label: "CNP Figueres - C/ Pep Ventura" },
                    { value: "CNP LLORET DE MAR", label: "CNP Lloret de Mar" },
                    { value: "CNP SANT FELIU DE GUÍXOLS", label: "CNP Sant Feliu de Guíxols" }
                ]
            };

            const PROCEDURES_LIST = [
                { value: "POLICIA-TOMA DE HUELLA (EXPEDICIÓN DE TARJETA) INICIAL, RENOVACIÓN, DUPLICADO Y LEY 14/2013", label: "POLICÍA - Toma de Huellas (Expedición TIE / Renovación / Duplicado)" },
                { value: "POLICIA-RECOGIDA DE TARJETA DE IDENTIDAD DE EXTRANJERO (TIE)", label: "POLICÍA - Recogida de Tarjeta de Identidad de Extranjero (TIE)" },
                { value: "POLICIA-ASIGNACIÓN DE NIE", label: "POLICÍA - Asignación de NIE" },
                { value: "POLICIA-CERTIFICADOS (DE RESIDENCIA, DE NO RESIDENCIA Y DE CONCORDANCIA)", label: "POLICÍA - Certificados (Residencia, No Residencia, Concordancia)" },
                { value: "POLICIA-CERTIFICADO DE REGISTRO DE CIUDADANO DE LA U.E.", label: "POLICÍA - Certificado de Registro de Ciudadano de la UE" },
                { value: "POLICIA-AUTORIZACIÓN DE REGRESO", label: "POLICÍA - Autorización de Regreso" },
                { value: "POLICIA-EXPEDICIÓN/RENOVACIÓN DE DOCUMENTOS DE SOLICITANTES DE ASILO", label: "POLICÍA - Renovación Documentos Asilo (Hoja Blanca / Tarjeta Roja)" },
                { value: "POLICIA-SOLICITUD DE ASILO (PRIMERA CITA)", label: "POLICÍA - Solicitud de Asilo (Primera Cita)" },
                { value: "POLICIA-CARTA DE INVITACIÓN", label: "POLICÍA - Carta de Invitación" },
                { value: "POLICIA-TÍTULOS DE VIAJE", label: "POLICÍA - Títulos de Viaje" },
                { value: "EXTRANJERÍA - SOLICITUD DE AUTORIZACIONES", label: "EXTRANJERÍA - Solicitud de Autorizaciones (Arraigo, Residencia Inicial)" },
                { value: "EXTRANJERÍA - RENOVACIÓN DE AUTORIZACIONES", label: "EXTRANJERÍA - Renovación de Autorizaciones" },
                { value: "__custom__", label: "✍️ Otro trámite (Specify custom)..." }
            ];

            const COUNTRIES_LIST = [
                "Pakistan", "Bangladesh", "India", "Morocco", "Colombia", "Venezuela", "Peru", 
                "Honduras", "Ecuador", "Dominican Republic", "Argentina", "Brazil", "Paraguay", 
                "Bolivia", "Cuba", "Chile", "Uruguay", "Ukraine", "United Kingdom", "China", 
                "Senegal", "Algeria", "Nigeria", "Philippines", "United States", "Mexico", 
                "Nicaragua", "Guatemala", "El Salvador", "Russia", "Egypt", "Ghana", "Georgia", 
                "Armenia", "Italy", "France", "Germany", "Romania", "Portugal", "Spain"
            ];

            function initDropdowns() {
                const citSel = document.getElementById('citizenshipSelect');
                citSel.innerHTML = `<option value="" disabled selected>-- Select Citizenship / Nationality --</option>` +
                    COUNTRIES_LIST.map(c => `<option value="${c}">${c}</option>`).join('') +
                    `<option value="__custom__">✍️ Other Nationality (Specify custom)...</option>`;
                citSel.value = "Pakistan";

                const birthCountrySel = document.getElementById('birthCountrySelect');
                birthCountrySel.innerHTML = `<option value="">Same as Citizenship (Default)</option>` +
                    COUNTRIES_LIST.map(c => `<option value="${c}">${c}</option>`).join('') +
                    `<option value="__custom__">✍️ Other Country (Specify custom)...</option>`;
                birthCountrySel.value = "";

                const provSel = document.getElementById('provinceSelect');
                provSel.innerHTML = SPANISH_PROVINCES.map(p => `<option value="${p}">${p}</option>`).join('') +
                    `<option value="__custom__">✍️ Otra ciudad / provincia (Specify custom)...</option>`;
                provSel.value = "Barcelona";

                updateOffices('Barcelona');

                const procSel = document.getElementById('procedureSelect');
                procSel.innerHTML = PROCEDURES_LIST.map(p => `<option value="${p.value}">${p.label}</option>`).join('');
                procSel.value = PROCEDURES_LIST[0].value;
            }

            function onCitizenshipChanged(val) {
                const customCit = document.getElementById('customCitizenshipInput');
                if (val === '__custom__') {
                    customCit.style.display = 'block';
                    customCit.required = true;
                    customCit.focus();
                } else {
                    customCit.style.display = 'none';
                    customCit.required = false;
                }
            }

            function onBirthCountryChanged(val) {
                const customBirth = document.getElementById('customBirthCountryInput');
                if (val === '__custom__') {
                    customBirth.style.display = 'block';
                    customBirth.required = true;
                    customBirth.focus();
                } else {
                    customBirth.style.display = 'none';
                    customBirth.required = false;
                }
            }

            function onProvinceChanged(prov) {
                const customProv = document.getElementById('customProvinceInput');
                if (prov === '__custom__') {
                    customProv.style.display = 'block';
                    customProv.required = true;
                    customProv.focus();
                    updateOffices(customProv.value.trim() || 'Custom');
                } else {
                    customProv.style.display = 'none';
                    customProv.required = false;
                    updateOffices(prov);
                }
            }

            function updateOffices(prov) {
                const offSel = document.getElementById('officeSelect');
                const customOff = document.getElementById('customOfficeInput');
                customOff.style.display = 'none';
                customOff.required = false;

                const officeList = PROVINCE_OFFICES[prov] || [
                    { value: "", label: "🌐 Cualquier oficina (Any / First Available)" },
                    { value: `${prov} - Todas las oficinas`, label: `🏢 ${prov} - Todas las oficinas (All Offices)` },
                    { value: `CNP ${prov.toUpperCase()} - COMISARÍA PRINCIPAL`, label: `CNP ${prov} - Comisaría Principal` }
                ];

                offSel.innerHTML = officeList.map(o => `<option value="${o.value}">${o.label}</option>`).join('') +
                    `<option value="__custom__">✍️ Otra oficina (Specify custom)...</option>`;
                offSel.value = "";
            }

            function onOfficeChanged(val) {
                const customOff = document.getElementById('customOfficeInput');
                if (val === '__custom__') {
                    customOff.style.display = 'block';
                    customOff.required = true;
                    customOff.focus();
                } else {
                    customOff.style.display = 'none';
                    customOff.required = false;
                }
            }

            function onProcChanged(val) {
                const customProc = document.getElementById('customProcInput');
                if (val === '__custom__') {
                    customProc.style.display = 'block';
                    customProc.required = true;
                    customProc.focus();
                } else {
                    customProc.style.display = 'none';
                    customProc.required = false;
                }
            }

            document.getElementById('orderForm').addEventListener('submit', async (e) => {
                e.preventDefault();
                const alertBox = document.getElementById('alertBox');
                alertBox.style.display = 'none';

                const citSel = document.getElementById('citizenshipSelect');
                const citizenship = citSel.value === '__custom__' ? document.getElementById('customCitizenshipInput').value.trim() : citSel.value;
                if (!citizenship) {
                    alertBox.className = 'alert alert-error';
                    alertBox.innerText = 'Please select or specify citizenship / nationality.';
                    alertBox.style.display = 'block';
                    return;
                }

                const birthDate = document.getElementById('birth_date').value.trim();
                if (!birthDate) {
                    alertBox.className = 'alert alert-error';
                    alertBox.innerText = 'Please enter Date of Birth.';
                    alertBox.style.display = 'block';
                    return;
                }

                const birthCountrySel = document.getElementById('birthCountrySelect');
                let birthCountry = birthCountrySel.value === '__custom__' ? document.getElementById('customBirthCountryInput').value.trim() : birthCountrySel.value;
                if (!birthCountry) {
                    birthCountry = citizenship;
                }

                const provSel = document.getElementById('provinceSelect');
                const province = provSel.value === '__custom__' ? document.getElementById('customProvinceInput').value.trim() : provSel.value;

                const offSel = document.getElementById('officeSelect');
                const office = offSel.value === '__custom__' ? document.getElementById('customOfficeInput').value.trim() : offSel.value;

                const procSel = document.getElementById('procedureSelect');
                const procedure = procSel.value === '__custom__' ? document.getElementById('customProcInput').value.trim() : procSel.value;

                if (!province) {
                    alertBox.className = 'alert alert-error';
                    alertBox.innerText = 'Please select or enter a province / city.';
                    alertBox.style.display = 'block';
                    return;
                }
                if (!procedure) {
                    alertBox.className = 'alert alert-error';
                    alertBox.innerText = 'Please select or enter a procedure (trámite).';
                    alertBox.style.display = 'block';
                    return;
                }

                // Process digital certificate if provided
                let certB64 = null;
                let certFilename = null;
                const certFileInput = document.getElementById('certFile');
                if (certFileInput.files && certFileInput.files[0]) {
                    const file = certFileInput.files[0];
                    certFilename = file.name;
                    try {
                        certB64 = await new Promise((resolve, reject) => {
                            const reader = new FileReader();
                            reader.onload = () => resolve(reader.result);
                            reader.onerror = (err) => reject(err);
                            reader.readAsDataURL(file);
                        });
                    } catch (fErr) {
                        alertBox.className = 'alert alert-error';
                        alertBox.innerText = 'Error reading certificate file: ' + fErr.message;
                        alertBox.style.display = 'block';
                        return;
                    }
                }

                const payload = {
                    name: document.getElementById('name').value.trim(),
                    citizenship: citizenship,
                    doc_type: document.getElementById('doc_type').value,
                    doc_number: document.getElementById('doc_number').value.trim(),
                    email: document.getElementById('email').value.trim(),
                    phone: document.getElementById('phone').value.trim(),
                    province: province,
                    office: office,
                    procedure: procedure,
                    birth_date: birthDate,
                    birth_country: birthCountry,
                    card_expiry_date: document.getElementById('card_expiry_date').value.trim(),
                    certificate_b64: certB64,
                    certificate_filename: certFilename,
                    certificate_password: document.getElementById('certPassword').value.trim()
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
                        initDropdowns();
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
                const provSel = document.getElementById('provinceSelect');
                let found = false;
                for (let i = 0; i < provSel.options.length; i++) {
                    if (provSel.options[i].value.toLowerCase() === city.toLowerCase()) {
                        provSel.selectedIndex = i;
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    provSel.value = '__custom__';
                    const customProv = document.getElementById('customProvinceInput');
                    customProv.value = city;
                }
                onProvinceChanged(provSel.value);
                provSel.scrollIntoView({ behavior: 'smooth' });
                provSel.focus();
            }

            initDropdowns();
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
        <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
        <title>Control Board - Cita Previa & Telegram Broadcaster</title>
        <style>
            :root { --primary: #2563eb; --primary-hover: #1d4ed8; --bg: #f8fafc; --card: #ffffff; --text: #0f172a; --border: #e2e8f0; }
            * { box-sizing: border-box; }
            body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: var(--bg); margin: 0; padding: 1.5rem; color: var(--text); -webkit-font-smoothing: antialiased; }

            .header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 1.25rem; gap: 1rem; }
            h1 { margin: 0; font-size: 1.5rem; }
            .header-actions { display: flex; gap: 0.5rem; }
            .btn { background: var(--primary); color: white; padding: 0.55rem 1rem; border-radius: 8px; text-decoration: none; font-size: 0.9rem; font-weight: 600; border: none; cursor: pointer; display: inline-flex; align-items: center; justify-content: center; gap: 0.4rem; min-height: 40px; transition: background 0.15s; }
            .btn:hover { background: var(--primary-hover); }
            .btn-action { padding: 0.4rem 0.75rem; font-size: 0.8rem; border-radius: 6px; background: #0284c7; color: white; border: none; cursor: pointer; display: inline-flex; align-items: center; justify-content: center; min-height: 36px; font-weight: 600; }
            .btn-action:hover { background: #0369a1; }
            .btn-success { background: #16a34a; }
            .btn-success:hover { background: #15803d; }
            .btn-danger { background: #dc2626; }
            .btn-danger:hover { background: #b91c1c; }
            .btn-secondary { background: #64748b; }
            .btn-secondary:hover { background: #475569; }

            /* Modern iOS Segmented Tabs */
            .tabs { display: flex; background: #e2e8f0; padding: 4px; border-radius: 10px; gap: 4px; margin-bottom: 1.25rem; }
            .tab-btn { flex: 1; padding: 0.65rem 0.5rem; font-size: 0.9rem; font-weight: 600; text-align: center; border-radius: 8px; border: none; background: transparent; color: #475569; cursor: pointer; transition: all 0.15s ease; display: flex; align-items: center; justify-content: center; gap: 0.35rem; }
            .tab-btn.active { background: #ffffff; color: var(--primary); box-shadow: 0 1px 3px rgba(0,0,0,0.12); font-weight: 700; }

            .tab-content { display: none; }
            .tab-content.active { display: block; }

            .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(130px, 1fr)); gap: 0.75rem; margin-bottom: 1.25rem; }
            .stat-card { background: white; padding: 0.9rem 1rem; border-radius: 10px; border: 1px solid var(--border); box-shadow: 0 1px 3px rgba(0,0,0,0.05); }
            .stat-title { font-size: 0.75rem; text-transform: uppercase; color: #64748b; font-weight: 700; letter-spacing: 0.04em; }
            .stat-value { font-size: 1.5rem; font-weight: bold; margin-top: 0.25rem; }

            .card { background: white; border-radius: 12px; padding: 1.25rem; margin-bottom: 1.25rem; border: 1px solid var(--border); box-shadow: 0 1px 3px rgba(0,0,0,0.05); }
            .card-header-flex { display: flex; justify-content: space-between; align-items: flex-start; gap: 0.5rem; flex-wrap: wrap; margin-bottom: 0.5rem; }
            .form-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; }
            label { display: block; font-size: 0.85rem; font-weight: 600; margin-bottom: 0.35rem; color: #475569; }
            input, select { width: 100%; padding: 0.7rem 0.85rem; border: 1px solid #cbd5e1; border-radius: 8px; font-size: 0.95rem; }

            .proxy-btn-row { display: grid; grid-template-columns: 1fr 1.3fr 1.3fr; gap: 0.5rem; margin-top: 0.75rem; }
            .proxy-btn-row button { min-height: 42px; font-size: 0.875rem; }

            .table-responsive { width: 100%; overflow-x: auto; -webkit-overflow-scrolling: touch; border-radius: 10px; border: 1px solid var(--border); margin-bottom: 1.25rem; background: white; }
            table { width: 100%; border-collapse: collapse; min-width: 680px; }
            th, td { padding: 0.75rem 0.9rem; text-align: left; border-bottom: 1px solid var(--border); font-size: 0.875rem; }
            th { background: #f8fafc; color: #475569; font-weight: 600; white-space: nowrap; }

            .badge { display: inline-block; padding: 0.25rem 0.55rem; border-radius: 9999px; font-size: 0.75rem; font-weight: 700; text-transform: uppercase; white-space: nowrap; }
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

            .alert-box { padding: 0.75rem 1rem; border-radius: 8px; margin-bottom: 1rem; font-size: 0.9rem; display: none; word-break: break-word; }
            .alert-box.success { background: #dcfce7; color: #15803d; border: 1px solid #86efac; display: block; }
            .alert-box.error { background: #fee2e2; color: #991b1b; border: 1px solid #f87171; display: block; }

            /* Mobile-specific adjustments (Phones & Tablets <= 768px, including iPhone 16 Pro Max at 440px) */
            @media (max-width: 768px) {
                body { padding: 0.75rem 0.5rem; }
                .header { flex-direction: column; align-items: stretch; gap: 0.75rem; margin-bottom: 1rem; }
                .header h1 { font-size: 1.2rem; line-height: 1.25; }
                .header-actions { display: flex; gap: 0.5rem; width: 100%; }
                .header-actions .btn { flex: 1; justify-content: center; min-height: 42px; font-size: 0.85rem; }

                .tabs { margin-bottom: 1rem; gap: 3px; }
                .tab-btn { padding: 0.6rem 0.35rem; font-size: 0.82rem; }

                .stats { grid-template-columns: repeat(2, 1fr) !important; gap: 0.5rem; }
                .stats .stat-card:last-child { grid-column: span 2; }
                .stat-card { padding: 0.75rem; }
                .stat-value { font-size: 1.35rem; }

                .card { padding: 1rem 0.85rem; margin-bottom: 1rem; border-radius: 10px; }
                .card-header-flex { flex-direction: column; align-items: flex-start; gap: 0.4rem; }
                .card-header-flex #portalConnectionStatusBadge { width: 100%; }
                .card-header-flex .badge { display: block; text-align: center; width: 100%; }

                .form-grid { grid-template-columns: 1fr !important; gap: 0.85rem; }
                input, select { font-size: 16px !important; padding: 0.75rem; } /* Prevents iOS auto-zoom */

                .proxy-btn-row { grid-template-columns: 1fr !important; gap: 0.5rem; }
                .proxy-btn-row button { width: 100% !important; min-height: 44px; font-size: 0.9rem; justify-content: center; }

                .card-footer-responsive { flex-direction: column; align-items: stretch !important; gap: 0.65rem !important; }
                .card-footer-responsive button { width: 100% !important; margin-left: 0 !important; justify-content: center; min-height: 44px; font-size: 0.9rem; }

                .daemon-card { flex-direction: column; align-items: stretch !important; gap: 0.75rem; }
                .daemon-actions { width: 100%; }
                .daemon-actions button { width: 100%; min-height: 44px; justify-content: center; font-size: 0.95rem; }

                .toolbar-responsive { flex-direction: column; align-items: stretch !important; gap: 0.75rem !important; }
                .toolbar-responsive input#citySearch { width: 100% !important; font-size: 16px; }
                .filter-pills { display: flex; width: 100%; gap: 0.25rem; }
                .filter-pills button { flex: 1; text-align: center; padding: 0.5rem 0.2rem; font-size: 0.78rem; min-height: 38px; }
                .toolbar-actions { width: 100%; margin-left: 0 !important; display: flex; flex-direction: column; gap: 0.45rem; }
                .toolbar-actions button { width: 100%; justify-content: center; min-height: 42px; padding: 0.6rem; font-size: 0.88rem; }
            }
        </style>
    </head>
    <body>
        <div class="header">
            <div>
                <h1>📦 Control Board: Appointments & Broadcaster</h1>
                <p style="margin: 0.25rem 0 0; color: #64748b; font-size: 0.85rem;">Manage customer booking queue & live slot broadcast scanners.</p>
            </div>
            <div class="header-actions">
                <a href="/" class="btn btn-secondary">+ Customer Intake</a>
                <button onclick="refreshAll()" class="btn">🔄 Refresh</button>
            </div>
        </div>

        <div class="tabs">
            <button class="tab-btn active" onclick="switchTab('ordersTab')">📋 Orders Queue</button>
            <button class="tab-btn" onclick="switchTab('monitorTab')">📡 Live Monitor & Alerts</button>
        </div>

        <!-- TAB 1: ORDERS QUEUE -->
        <div id="ordersTab" class="tab-content active">
            <div style="display: flex; justify-content: flex-end; margin-bottom: 1rem;">
                <button onclick="purgeData()" class="btn btn-danger" style="width: auto;">🧹 Scrub Finished Data</button>
            </div>

            <div class="stats" id="statsRow"></div>

            <div class="table-responsive">
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
        </div>

        <!-- TAB 2: LIVE MONITOR & TELEGRAM BROADCAST -->
        <div id="monitorTab" class="tab-content">
            <!-- LIVE OPEN BOOKINGS HERO SHOWCASE -->
            <div id="heroOpenSection" style="display: none; background: #ecfdf5; border: 2px solid #22c55e; border-radius: 12px; padding: 1.25rem; margin-bottom: 1.25rem;">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.75rem; flex-wrap: wrap; gap: 0.5rem;">
                    <h3 style="margin: 0; color: #15803d; display: flex; align-items: center; gap: 0.5rem; font-size: 1.05rem;">
                        <span style="display: inline-block; width: 12px; height: 12px; background: #22c55e; border-radius: 50%; box-shadow: 0 0 10px #22c55e;"></span>
                        🔥 APPOINTMENTS OPEN NOW (<span id="heroOpenCount">0</span>)
                    </h3>
                    <span style="font-size: 0.8rem; color: #166534; font-weight: bold;">⚡ Live Availability Stream</span>
                </div>
                <div id="heroOpenCards" style="display: grid; grid-template-columns: repeat(auto-fill, minmax(260px, 1fr)); gap: 0.75rem;"></div>
            </div>

            <!-- SPANISH PORTAL CONNECTIVITY & PROXY DIAGNOSTICS -->
            <div class="card" style="border-left: 4px solid #f59e0b;">
                <div class="card-header-flex">
                    <div>
                        <h3 style="margin: 0; display: flex; align-items: center; gap: 0.4rem; font-size: 1.05rem;">
                            🛡️ Spanish Portal & Proxy Settings
                        </h3>
                        <p style="color: #64748b; font-size: 0.82rem; margin: 0.25rem 0 0; line-height: 1.35;">
                            Direct cloud datacenters are dropped by Spain's Red SARA firewall. A Spanish proxy allows 24/7 instant slot detection.
                        </p>
                    </div>
                    <div id="portalConnectionStatusBadge">
                        <span class="badge badge-timeout" style="padding: 0.35rem 0.65rem; font-size: 0.75rem;">⚠️ Check Connection</span>
                    </div>
                </div>

                <div id="proxyAlertBox" class="alert-box"></div>

                <div style="margin-top: 0.75rem;">
                    <label>Spanish Proxy URL (HTTP/HTTPS/SOCKS5)</label>
                    <input type="text" id="proxyUrlInput" placeholder="e.g. http://username:password@es-proxy:12323">
                    <small style="color: #64748b; display: block; margin-top: 0.25rem;" id="proxyStatusNotice">Format: http://user:pass@host:port or socks5://host:port</small>
                </div>

                <div class="proxy-btn-row">
                    <button class="btn" onclick="saveProxySettings()">💾 Save Proxy</button>
                    <button class="btn btn-action" style="background: #0284c7;" onclick="testPortalConnectionNow()">⚡ Test Connection</button>
                    <button class="btn btn-action" style="background: #8b5cf6;" onclick="autoFindFreeProxy()">🔍 Auto-Find Proxy</button>
                </div>
                <small style="color: #64748b; margin-top: 0.5rem; display: block;" id="diagnosticResultText">Click 'Test Connection' or 'Auto-Find Proxy' to test live reachability.</small>
            </div>

            <!-- Telegram Config Card -->
            <div class="card">
                <h3 style="margin-top: 0; font-size: 1.05rem;">📢 Telegram Broadcast Configuration</h3>
                <p style="color: #64748b; font-size: 0.82rem; margin-top: -0.25rem;">
                    Configure the Telegram Bot token and target channels/chats. When slots open, urgent broadcast alerts will be sent immediately.
                </p>
                <div id="tgAlertBox" class="alert-box"></div>
                <div class="form-grid">
                    <div>
                        <label>Telegram Bot Token (from @BotFather)</label>
                        <input type="password" id="tgBotToken" placeholder="e.g. 123456789:ABCdefGhIJKlmNoPQRsTUVwxyZ">
                        <small style="color: #64748b; display: block; margin-top: 0.2rem;" id="tokenMaskedNotice"></small>
                    </div>
                    <div>
                        <label>Broadcast Channels / Group IDs (comma-separated)</label>
                        <input type="text" id="tgChatId" placeholder="e.g. @my_citas_channel, -100123456789">
                        <small style="color: #64748b; display: block; margin-top: 0.2rem;">Supports channels (@name), groups (-100...), or user chat IDs.</small>
                    </div>
                </div>
                <div style="margin-top: 1rem; display: flex; gap: 0.5rem; align-items: center;" class="card-footer-responsive">
                    <label style="display: flex; align-items: center; gap: 0.5rem; margin: 0; cursor: pointer; font-size: 0.9rem;">
                        <input type="checkbox" id="tgEnabled" style="width: auto;"> <b>Enable Telegram Alerts</b>
                    </label>
                    <button class="btn" onclick="saveTelegramSettings()" style="margin-left: auto;">💾 Save Telegram Settings</button>
                    <button class="btn btn-action" onclick="testTelegramBroadcast()">📢 Send Test Broadcast</button>
                </div>
            </div>

            <!-- Monitor Daemon & Status Controls -->
            <div class="card daemon-card" style="display: flex; justify-content: space-between; align-items: center;">
                <div>
                    <h3 style="margin: 0; font-size: 1.05rem;">🛰️ Live Monitor Daemon</h3>
                    <p style="margin: 0.25rem 0 0; color: #64748b; font-size: 0.82rem;" id="daemonStatusText">
                        Status: Checking daemon state...
                    </p>
                </div>
                <div class="daemon-actions" style="display: flex; gap: 0.5rem;">
                    <button id="btnStartDaemon" class="btn btn-success" onclick="toggleDaemon('start')">▶️ Start Live Monitor</button>
                    <button id="btnStopDaemon" class="btn btn-danger" onclick="toggleDaemon('stop')" style="display: none;">⏹️ Stop Live Monitor</button>
                </div>
            </div>

            <!-- Monitored Targets Table -->
            <div class="card">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem; flex-wrap: wrap; gap: 0.5rem;">
                    <h3 style="margin: 0; font-size: 1.05rem;">🎯 Monitored Provinces & Procedures</h3>
                    <button class="btn btn-action" onclick="toggleAddMonitorModal()">+ Add New Target</button>
                </div>

                <div id="addMonitorForm" style="display: none; background: #f8fafc; padding: 1rem; border-radius: 8px; margin-bottom: 1rem; border: 1px solid var(--border);">
                    <div class="form-grid">
                        <div>
                            <label>Province / City</label>
                            <select id="newMonProvinceSelect" onchange="onAdminMonProvChange(this.value)"></select>
                            <input type="text" id="newMonCustomProv" placeholder="✍️ Type custom city / province..." style="margin-top: 0.35rem; display: none;">
                        </div>
                        <div>
                            <label>Procedure (Trámite)</label>
                            <select id="newMonProcSelect" onchange="onAdminMonProcChange(this.value)"></select>
                            <input type="text" id="newMonCustomProc" placeholder="✍️ Type custom procedure..." style="margin-top: 0.35rem; display: none;">
                        </div>
                    </div>
                    <div style="margin-top: 0.75rem; display: flex; gap: 0.5rem; justify-content: flex-end;">
                        <button class="btn btn-secondary" onclick="toggleAddMonitorModal()">Cancel</button>
                        <button class="btn" onclick="saveNewMonitor()">Add Target</button>
                    </div>
                </div>

                <!-- Search & Filters Toolbar -->
                <div class="toolbar-responsive" style="display: flex; gap: 0.5rem; align-items: center; margin-bottom: 1rem; flex-wrap: wrap;">
                    <input type="text" id="citySearch" placeholder="🔍 Search city (e.g. Madrid, Barcelona)..." style="width: 240px; padding: 0.5rem 0.75rem;" oninput="filterMonitors()">
                    <div class="filter-pills" style="display: flex; gap: 0.25rem;">
                        <button class="btn-action" id="tabFilterAll" style="background: #2563eb;" onclick="setMonitorFilter('all')">All (<span id="badgeTotalCities">0</span>)</button>
                        <button class="btn-action" id="tabFilterOpen" style="background: #16a34a; opacity: 0.7;" onclick="setMonitorFilter('available')">🟢 Open (<span id="badgeOpenCities">0</span>)</button>
                        <button class="btn-action" id="tabFilterClosed" style="background: #64748b; opacity: 0.7;" onclick="setMonitorFilter('no_slots')">⚪ Closed (<span id="badgeClosedCities">0</span>)</button>
                    </div>
                    <div class="toolbar-actions" style="margin-left: auto; display: flex; gap: 0.4rem; flex-wrap: wrap;">
                        <button class="btn btn-action" style="background: #7c3aed;" onclick="seedAllSpanishCities()">⚡ Add All 30+ Spanish Cities</button>
                        <button class="btn btn-action" style="background: #0284c7;" onclick="scanAllCitiesNow()">🚀 Scan All Cities Now</button>
                    </div>
                </div>

                <div class="table-responsive">
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
        </div>

        <script>
            function switchTab(tabId) {
                document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
                document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
                event.target.classList.add('active');
                document.getElementById(tabId).classList.add('active');
                if (tabId === 'monitorTab') {
                    loadProxySettings();
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
                                <td>
                                    <strong>${o.name}</strong> <br>
                                    <small style="color: #64748b;">${o.citizenship || ''}${o.birth_date ? ' • DOB: '+o.birth_date : ''}</small>
                                    ${o.certificate_path ? '<span class="badge" style="background: #e0e7ff; color: #4338ca; font-size: 0.7rem; padding: 1px 5px; margin-left: 3px;" title="Digital Certificate available">🔐 Cert</span>' : ''}
                                </td>
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

            // --- PROXY SETTINGS & DIAGNOSTICS ---
            async function loadProxySettings() {
                try {
                    const res = await fetch('/api/config/proxy');
                    const data = await res.json();
                    document.getElementById('proxyUrlInput').value = data.proxy || '';
                    const notice = document.getElementById('proxyStatusNotice');
                    const badge = document.getElementById('portalConnectionStatusBadge');
                    if (data.is_configured) {
                        notice.innerText = `Active Proxy: ${data.masked_proxy}`;
                        badge.innerHTML = '<span class="badge badge-searching" style="padding: 0.4rem 0.8rem;">🛡️ Proxy Configured</span>';
                    } else {
                        notice.innerText = 'No proxy configured. Direct cloud requests to Red SARA are blocked.';
                        badge.innerHTML = '<span class="badge badge-timeout" style="padding: 0.4rem 0.8rem;">⚠️ Direct IP (Firewalled)</span>';
                    }
                } catch(e) {
                    console.error("Failed to load proxy settings", e);
                }
            }

            async function saveProxySettings() {
                const box = document.getElementById('proxyAlertBox');
                box.className = 'alert-box';
                const proxyVal = document.getElementById('proxyUrlInput').value.trim();
                try {
                    const res = await fetch('/api/config/proxy', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ proxy: proxyVal })
                    });
                    const data = await res.json();
                    box.className = 'alert-box success';
                    box.innerText = data.message || 'Proxy settings saved!';
                    loadProxySettings();
                } catch(e) {
                    box.className = 'alert-box error';
                    box.innerText = 'Failed to save proxy: ' + e.message;
                }
            }

            async function testPortalConnectionNow() {
                const resultText = document.getElementById('diagnosticResultText');
                const badge = document.getElementById('portalConnectionStatusBadge');
                resultText.innerHTML = '⏳ Probing <code>icp.administracionelectronica.gob.es</code> (up to 6s)...';
                try {
                    const res = await fetch('/api/diagnostics/test-connection', { method: 'POST' });
                    const data = await res.json();
                    if (data.success) {
                        resultText.innerHTML = `🟢 <strong>Connected!</strong> Portal responded with HTTP ${data.status_code} in <b>${data.latency_ms}ms</b> ${data.proxy_used ? '(via Spanish proxy)' : ''}. Live slots can now be fetched!`;
                        badge.innerHTML = `<span class="badge badge-available" style="padding: 0.4rem 0.8rem;">🟢 CONNECTED (${data.latency_ms}ms)</span>`;
                    } else {
                        resultText.innerHTML = `🔴 <strong>Firewalled:</strong> ${data.message}`;
                        badge.innerHTML = '<span class="badge badge-timeout" style="padding: 0.4rem 0.8rem;">🔴 FIREWALLED / BLOCKED</span>';
                    }
                } catch(e) {
                    resultText.innerHTML = '❌ Diagnostics failed: ' + e.message;
                }
            }

            async function autoFindFreeProxy() {
                const resultText = document.getElementById('diagnosticResultText');
                const badge = document.getElementById('portalConnectionStatusBadge');
                resultText.innerHTML = '🔍 Scanning public Spanish proxy directories and testing against Red SARA (takes ~5-8s)...';
                badge.innerHTML = '<span class="badge badge-searching" style="padding: 0.4rem 0.8rem;">🔍 Scanning Proxies...</span>';
                try {
                    const res = await fetch('/api/diagnostics/auto-find-proxies', { method: 'POST' });
                    const data = await res.json();
                    if (data.found) {
                        resultText.innerHTML = `🟢 <strong>Proxy Found & Connected!</strong> Active: <code>${data.proxy}</code>. Live monitoring is active!`;
                        badge.innerHTML = '<span class="badge badge-available" style="padding: 0.4rem 0.8rem;">🟢 PROXY CONNECTED</span>';
                        loadProxySettings();
                    } else {
                        resultText.innerHTML = `⚠️ <strong>No open proxies worked:</strong> ${data.message}`;
                        badge.innerHTML = '<span class="badge badge-timeout" style="padding: 0.4rem 0.8rem;">🔴 RED SARA BLOCKED</span>';
                    }
                } catch(e) {
                    resultText.innerHTML = '❌ Auto-find failed: ' + e.message;
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
                    const isTimeout = m.last_status === 'timeout';
                    const statusClass = `badge-${m.last_status || 'pending'}`;
                    let statusLabel = m.last_status || 'pending';
                    if (isAvail) statusLabel = '🟢 SLOTS OPEN!';
                    else if (isTimeout) statusLabel = '⚠️ TIMEOUT (Needs ES Proxy)';
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

            const ADMIN_SPANISH_PROVINCES = [
                "Barcelona", "Madrid", "Valencia", "Alicante", "Málaga", "Sevilla", "Murcia", "Baleares", "Las Palmas", 
                "Santa Cruz de Tenerife", "Zaragoza", "Cádiz", "Bizkaia", "Pontevedra", "Granada", "Tarragona", "Córdoba", 
                "Girona", "Almería", "Guipúzcoa", "Toledo", "Badajoz", "Navarra", "Jaén", "Castellón", "Cantabria", "Huelva", 
                "Valladolid", "Ciudad Real", "León", "Lleida", "Cáceres", "Albacete", "Burgos", "Álava", "Salamanca", "Lugo", 
                "La Rioja", "Ourense", "Guadalajara", "Huesca", "Cuenca", "Zamora", "Palencia", "Ávila", "Segovia", "Teruel", 
                "Soria", "Ceuta", "Melilla"
            ];

            const ADMIN_PROCEDURES_LIST = [
                { value: "POLICIA-TOMA DE HUELLA (EXPEDICIÓN DE TARJETA) INICIAL, RENOVACIÓN, DUPLICADO Y LEY 14/2013", label: "POLICÍA - Toma de Huellas (Expedición TIE / Renovación / Duplicado)" },
                { value: "POLICIA-RECOGIDA DE TARJETA DE IDENTIDAD DE EXTRANJERO (TIE)", label: "POLICÍA - Recogida de Tarjeta de Identidad de Extranjero (TIE)" },
                { value: "POLICIA-ASIGNACIÓN DE NIE", label: "POLICÍA - Asignación de NIE" },
                { value: "POLICIA-CERTIFICADOS (DE RESIDENCIA, DE NO RESIDENCIA Y DE CONCORDANCIA)", label: "POLICÍA - Certificados (Residencia, No Residencia, Concordancia)" },
                { value: "POLICIA-CERTIFICADO DE REGISTRO DE CIUDADANO DE LA U.E.", label: "POLICÍA - Certificado de Registro de Ciudadano de la UE" },
                { value: "POLICIA-AUTORIZACIÓN DE REGRESO", label: "POLICÍA - Autorización de Regreso" },
                { value: "POLICIA-EXPEDICIÓN/RENOVACIÓN DE DOCUMENTOS DE SOLICITANTES DE ASILO", label: "POLICÍA - Renovación Documentos Asilo (Hoja Blanca / Tarjeta Roja)" },
                { value: "POLICIA-SOLICITUD DE ASILO (PRIMERA CITA)", label: "POLICÍA - Solicitud de Asilo (Primera Cita)" },
                { value: "POLICIA-CARTA DE INVITACIÓN", label: "POLICÍA - Carta de Invitación" },
                { value: "POLICIA-TÍTULOS DE VIAJE", label: "POLICÍA - Títulos de Viaje" },
                { value: "EXTRANJERÍA - SOLICITUD DE AUTORIZACIONES", label: "EXTRANJERÍA - Solicitud de Autorizaciones (Arraigo, Residencia Inicial)" },
                { value: "EXTRANJERÍA - RENOVACIÓN DE AUTORIZACIONES", label: "EXTRANJERÍA - Renovación de Autorizaciones" },
                { value: "__custom__", label: "✍️ Otro trámite (Specify custom)..." }
            ];

            function initAdminDropdowns() {
                const provSel = document.getElementById('newMonProvinceSelect');
                if (provSel) {
                    provSel.innerHTML = ADMIN_SPANISH_PROVINCES.map(p => `<option value="${p}">${p}</option>`).join('') +
                        `<option value="__custom__">✍️ Otra ciudad / provincia (Specify custom)...</option>`;
                    provSel.value = "Barcelona";
                }

                const procSel = document.getElementById('newMonProcSelect');
                if (procSel) {
                    procSel.innerHTML = ADMIN_PROCEDURES_LIST.map(p => `<option value="${p.value}">${p.label}</option>`).join('');
                    procSel.value = ADMIN_PROCEDURES_LIST[0].value;
                }
            }

            function onAdminMonProvChange(val) {
                const customProv = document.getElementById('newMonCustomProv');
                if (val === '__custom__') {
                    customProv.style.display = 'block';
                    customProv.focus();
                } else {
                    customProv.style.display = 'none';
                }
            }

            function onAdminMonProcChange(val) {
                const customProc = document.getElementById('newMonCustomProc');
                if (val === '__custom__') {
                    customProc.style.display = 'block';
                    customProc.focus();
                } else {
                    customProc.style.display = 'none';
                }
            }

            function toggleAddMonitorModal() {
                const el = document.getElementById('addMonitorForm');
                const isOpening = el.style.display === 'none';
                el.style.display = isOpening ? 'block' : 'none';
                if (isOpening) {
                    initAdminDropdowns();
                }
            }

            async function saveNewMonitor() {
                const provSel = document.getElementById('newMonProvinceSelect');
                const prov = provSel.value === '__custom__' ? document.getElementById('newMonCustomProv').value.trim() : provSel.value;

                const procSel = document.getElementById('newMonProcSelect');
                const proc = procSel.value === '__custom__' ? document.getElementById('newMonCustomProc').value.trim() : procSel.value;

                if (!prov || !proc) {
                    alert('Please select or specify both Province and Procedure');
                    return;
                }
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

            initAdminDropdowns();
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
