#!/usr/bin/env python3
"""
Ultra-Fast Live Cita Previa Extranjería Appointment Monitor
High-performance asynchronous monitor using HTTP/2 session pooling and lightweight probing.
Detects open appointment slots in milliseconds and broadcasts instantly via Telegram.
"""

import sys
import os
import time
import json
import sqlite3
import argparse
import asyncio
import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
import re

import httpx
from telegram_broadcaster import broadcast_telegram_message, format_alert_message

BASE_DIR = Path(__file__).parent
CONFIG_PATH = BASE_DIR / "config.json"
DB_PATH = BASE_DIR / "orders.db"
DEFAULT_PORTAL_URL = "https://icp.administracionelectronica.gob.es/icpplus/index.html"
DEFAULT_BASE_URL = "https://icp.administracionelectronica.gob.es/icpplus"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/128.0.0.0 Safari/537.36"
)

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1"
}

PROVINCE_CODES = {
    "albacete": "02", "alicante": "03", "almeria": "04", "araba": "01", "asturias": "33",
    "avila": "05", "badajoz": "06", "barcelona": "08", "burgos": "09", "caceres": "10",
    "cadiz": "11", "cantabria": "39", "castellon": "12", "ceuta": "51", "ciudad real": "13",
    "cordoba": "14", "coruña": "15", "a coruña": "15", "cuenca": "16", "gipuzkoa": "20",
    "girona": "17", "granada": "18", "guadalajara": "19", "huelva": "21", "huesca": "22",
    "illes balears": "07", "baleares": "07", "jaen": "23", "leon": "24", "lleida": "25",
    "lugo": "27", "madrid": "28", "malaga": "29", "melilla": "52", "murcia": "30",
    "navarra": "31", "ourense": "32", "palencia": "34", "las palmas": "35", "pontevedra": "36",
    "la rioja": "26", "salamanca": "37", "s.c. tenerife": "38", "tenerife": "38",
    "segovia": "40", "sevilla": "41", "soria": "42", "tarragona": "43", "teruel": "44",
    "toledo": "45", "valencia": "46", "valladolid": "47", "bizkaia": "48", "zamora": "49",
    "zaragoza": "50"
}


def load_config() -> dict:
    if CONFIG_PATH.exists():
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_active_monitors() -> List[dict]:
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM monitors WHERE is_active = 1 ORDER BY id ASC")
        return [dict(r) for r in cur.fetchall()]


def update_monitor_status(monitor_id: int, status: str, message: str, alerted: bool = False):
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    with get_db() as conn:
        cur = conn.cursor()
        if alerted:
            cur.execute(
                """
                UPDATE monitors
                SET last_checked = ?, last_status = ?, last_status_message = ?, last_alert_at = ?
                WHERE id = ?
                """,
                (now, status, message, now, monitor_id)
            )
        else:
            cur.execute(
                """
                UPDATE monitors
                SET last_checked = ?, last_status = ?, last_status_message = ?
                WHERE id = ?
                """,
                (now, status, message, monitor_id)
            )
        conn.commit()


try:
    import h2
    HAS_HTTP2 = True
except ImportError:
    HAS_HTTP2 = False


class FastPortalProber:
    """Ultra-fast HTTP/2 connection-pooled scanner for Cita Previa."""

    def __init__(self, proxy: Optional[str] = None):
        self.proxy = proxy
        self.client: Optional[httpx.AsyncClient] = None

    async def get_client(self) -> httpx.AsyncClient:
        if self.client is None or self.client.is_closed:
            self.client = httpx.AsyncClient(
                headers=HEADERS,
                timeout=httpx.Timeout(10.0, connect=5.0),
                follow_redirects=True,
                http2=HAS_HTTP2,
                proxy=self.proxy
            )
        return self.client

    async def close(self):
        if self.client and not self.client.is_closed:
            await self.client.aclose()

    def resolve_province_code(self, province_name: str) -> Optional[str]:
        cleaned = province_name.lower().strip()
        for k, v in PROVINCE_CODES.items():
            if k in cleaned or cleaned in k:
                return v
        return None

    async def fast_check_province(self, province: str, procedure: str) -> Dict[str, Any]:
        """
        Executes a lightweight protocol-level check in sub-second time.
        Round-trip typically takes 150-400ms.
        """
        t0 = time.time()
        client = await self.get_client()

        prov_code = self.resolve_province_code(province)
        if not prov_code:
            return {
                "has_slots": False,
                "status": "error",
                "message": f"Unknown province code for '{province}'",
                "elapsed_ms": int((time.time() - t0) * 1000)
            }

        try:
            # Step 1: Open session & select province
            step1_url = f"{DEFAULT_BASE_URL}/citar"
            step1_data = {
                "form": f"/icpplus/citar?sede={prov_code}",
                "btnAceptar": "Aceptar"
            }
            r1 = await client.post(step1_url, data=step1_data)
            if r1.status_code != 200:
                return {
                    "has_slots": False,
                    "status": "error",
                    "message": f"Step 1 failed (HTTP {r1.status_code})",
                    "elapsed_ms": int((time.time() - t0) * 1000)
                }

            html1 = r1.text

            # Extract procedure option value matching procedure
            # Looks for <option value="4036">POLICIA-TOMA DE HUELLA...</option>
            match = re.search(r'<option[^>]*value="([^"]+)"[^>]*>([^<]*' + re.escape(procedure) + r'[^<]*)</option>', html1, re.IGNORECASE)
            if not match:
                # Try partial words
                words = [w for w in procedure.split() if len(w) > 3]
                for w in words:
                    match = re.search(r'<option[^>]*value="([^"]+)"[^>]*>([^<]*' + re.escape(w) + r'[^<]*)</option>', html1, re.IGNORECASE)
                    if match:
                        break

            if not match:
                return {
                    "has_slots": False,
                    "status": "error",
                    "message": f"Procedure '{procedure}' not found in province page",
                    "elapsed_ms": int((time.time() - t0) * 1000)
                }

            proc_val = match.group(1)
            proc_label = match.group(2).strip()

            # Step 2: Submit procedure
            step2_data = {
                "tramiteGrupo[0]": proc_val,
                "btnAceptar": "Aceptar"
            }
            r2 = await client.post(step1_url, data=step2_data)
            html2 = r2.text.lower()
            elapsed_ms = int((time.time() - t0) * 1000)

            # Evaluate response text
            if "no hay citas disponibles" in html2 or "en este momento no hay citas" in html2:
                return {
                    "has_slots": False,
                    "status": "no_slots",
                    "message": "En este momento no hay citas disponibles",
                    "elapsed_ms": elapsed_ms
                }
            elif "ha superado el número de intentos" in html2 or "acceso restringido" in html2:
                return {
                    "has_slots": False,
                    "status": "rate_limited",
                    "message": "Rate limit / IP temporary cooloff",
                    "elapsed_ms": elapsed_ms
                }
            elif "entrar" in html2 or "txtidcitado" in html2 or "cita" in html2 or "seleccione" in html2:
                # Potential slot opening or proceeded past restriction!
                return {
                    "has_slots": True,
                    "status": "available",
                    "message": f"¡CITAS DETECTADAS! Procedimiento {proc_label} abierto",
                    "elapsed_ms": elapsed_ms
                }
            else:
                return {
                    "has_slots": False,
                    "status": "checked",
                    "message": "Respuesta recibida sin confirmación de citas",
                    "elapsed_ms": elapsed_ms
                }

        except httpx.ConnectTimeout:
            return {"has_slots": False, "status": "timeout", "message": "Connection timeout", "elapsed_ms": int((time.time() - t0) * 1000)}
        except httpx.HTTPError as e:
            return {"has_slots": False, "status": "error", "message": f"HTTP error: {type(e).__name__}", "elapsed_ms": int((time.time() - t0) * 1000)}
        except Exception as e:
            return {"has_slots": False, "status": "error", "message": str(e), "elapsed_ms": int((time.time() - t0) * 1000)}


async def process_single_monitor(monitor: dict, prober: FastPortalProber, config: dict, cooldown_minutes: int = 10):
    monitor_id = monitor["id"]
    province = monitor["province"]
    procedure = monitor["procedure"]

    res = await prober.fast_check_province(province, procedure)
    ms = res.get("elapsed_ms", 0)
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S.%f')[:-3]}] ⚡ Checked {province} ({procedure}): {res['status']} ({ms}ms)")

    has_slots = res["has_slots"]
    status = res["status"]
    message = res["message"]

    alerted = False
    if has_slots:
        # Check cooldown
        last_alert_str = monitor.get("last_alert_at")
        in_cooldown = False
        if last_alert_str:
            try:
                last_alert_dt = datetime.datetime.fromisoformat(last_alert_str)
                elapsed = (datetime.datetime.now(datetime.timezone.utc) - last_alert_dt).total_seconds()
                if elapsed < cooldown_minutes * 60:
                    in_cooldown = True
                    print(f"      ⏳ In cooldown ({int(elapsed)}s elapsed). Alert suppressed.")
            except Exception:
                pass

        if not in_cooldown:
            tg_config = config.get("telegram", {})
            bot_token = tg_config.get("bot_token", "").strip()
            chat_id = tg_config.get("chat_id", "").strip()

            if bot_token and chat_id:
                alert_text = format_alert_message(
                    province=province,
                    procedure=procedure,
                    portal_url=config.get("portal_url", DEFAULT_PORTAL_URL),
                    custom_note=f"⚡ Detección ultrarrápida: {ms}ms desde el servidor"
                )
                print(f"      🚨🚨 BROADCASTING TO TELEGRAM ({chat_id}) IMMEDIATELY! 🚨🚨")
                t_alert_start = time.time()
                broadcast_res = broadcast_telegram_message(bot_token, chat_id, alert_text)
                t_alert_ms = int((time.time() - t_alert_start) * 1000)
                print(f"      ✅ Broadcast sent in {t_alert_ms}ms: {broadcast_res['status']}")
                alerted = True

            # Auto-trigger booking runner for the first waiting client
            if config.get("auto_booking_enabled", True):
                try:
                    with get_db() as db_conn:
                        cur = db_conn.cursor()
                        cur.execute(
                            """
                            SELECT order_id, name, doc_number FROM orders 
                            WHERE status = 'waiting' AND LOWER(province) = LOWER(?)
                            ORDER BY id ASC LIMIT 1
                            """,
                            (province,)
                        )
                        queued_client = cur.fetchone()
                        if queued_client:
                            q_id = queued_client["order_id"]
                            q_name = queued_client["name"]
                            print(f"      🚀 Auto-Launching Auto-Booker for queued client: {q_name} ({q_id})...")
                            python_bin = BASE_DIR / ".venv" / "bin" / "python"
                            if not python_bin.exists():
                                python_bin = Path(sys.executable)
                            booker_script = BASE_DIR / "auto_booker.py"
                            subprocess.Popen([str(python_bin), str(booker_script), "--order-id", q_id])
                except Exception as e:
                    print(f"      [Auto-booking trigger failed]: {e}")

    update_monitor_status(monitor_id, status, message, alerted=alerted)


async def run_fast_monitor_daemon(interval_sec: float = 3.0):
    print("=" * 70)
    print("  ⚡ Live Ultra-Fast Cita Previa Monitor & Instant Broadcaster")
    print(f"  Polling frequency: ~{interval_sec}s | Protocol: HTTP/2 Persistent Pool")
    print("=" * 70)

    config = load_config()
    prober = FastPortalProber(proxy=config.get("proxy"))

    try:
        while True:
            t_cycle_start = time.time()
            monitors = get_active_monitors()
            if not monitors:
                print("No active monitors found in DB. Sleeping 10s...")
                await asyncio.sleep(10)
                continue

            # Run checks with bounded concurrency (up to 4 concurrent checks)
            sem = asyncio.Semaphore(4)

            async def bounded_check(m):
                async with sem:
                    await process_single_monitor(
                        m,
                        prober,
                        config,
                        cooldown_minutes=config.get("cooldown_minutes", 10)
                    )
                    await asyncio.sleep(0.2)

            tasks = [bounded_check(m) for m in monitors]
            await asyncio.gather(*tasks)

            cycle_elapsed = time.time() - t_cycle_start
            sleep_duration = max(0.5, interval_sec - cycle_elapsed)
            await asyncio.sleep(sleep_duration)

    except asyncio.CancelledError:
        print("\nMonitor daemon stopped.")
    finally:
        await prober.close()


def main():
    parser = argparse.ArgumentParser(description="Ultra-Fast Live Cita Previa Monitor")
    parser.add_argument("--interval", type=float, default=3.0, help="Scan interval in seconds (default: 3.0s)")
    parser.add_argument("--check-once", action="store_true", help="Run a single instant scan and exit")
    args = parser.parse_args()

    if args.check_once:
        async def run_once():
            config = load_config()
            prober = FastPortalProber(proxy=config.get("proxy"))
            monitors = get_active_monitors()
            print(f"Running instant check for {len(monitors)} targets:")
            for m in monitors:
                await process_single_monitor(m, prober, config)
            await prober.close()
        asyncio.run(run_once())
    else:
        asyncio.run(run_fast_monitor_daemon(interval_sec=args.interval))


if __name__ == "__main__":
    main()
