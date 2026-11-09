#!/usr/bin/env python3
"""
Live Cita Previa Extranjería Appointment Monitor & Telegram Broadcaster
Monitors appointment availability across configured provinces and procedures
on the official Spanish portal (icp.administracionelectronica.gob.es).
When appointments open, it immediately triggers an urgent Telegram broadcast.
"""

import sys
import os
import time
import json
import sqlite3
import argparse
import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from telegram_broadcaster import broadcast_telegram_message, format_alert_message, send_test_broadcast

BASE_DIR = Path(__file__).parent
CONFIG_PATH = BASE_DIR / "config.json"
DB_PATH = BASE_DIR / "orders.db"
DEFAULT_PORTAL_URL = "https://icp.administracionelectronica.gob.es/icpplus/index.html"
DEFAULT_COOLDOWN_MINUTES = 10


def load_config() -> dict:
    if CONFIG_PATH.exists():
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"[Warning] Error loading config.json: {e}")
    return {}


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_monitor_tables():
    """Ensure monitors and subscribers tables exist in orders.db."""
    with get_db() as conn:
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
        conn.commit()


def get_active_monitors() -> List[dict]:
    ensure_monitor_tables()
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM monitors WHERE is_active = 1 ORDER BY id ASC")
        rows = [dict(r) for r in cur.fetchall()]
        if not rows:
            # If no monitors configured in DB yet, seed with default from config.json
            config = load_config()
            prov = config.get("province", "Barcelona")
            proc = config.get("procedure_keyword", "POLICIA-TOMA DE HUELLA")
            now = datetime.datetime.now(datetime.timezone.utc).isoformat()
            cur.execute(
                """
                INSERT INTO monitors (province, procedure, is_active, interval_seconds, created_at)
                VALUES (?, ?, 1, 60, ?)
                """,
                (prov, proc, now)
            )
            conn.commit()
            cur.execute("SELECT * FROM monitors WHERE is_active = 1")
            rows = [dict(r) for r in cur.fetchall()]
        return rows


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


def check_slot_availability(province: str, procedure: str, config: dict) -> Dict[str, Any]:
    """
    Checks appointment availability on the official portal for a given province and trámite.
    Returns: {"has_slots": bool, "status": str, "message": str, "office": Optional[str]}
    """
    portal_url = config.get("portal_url", DEFAULT_PORTAL_URL)
    proxy_config = None
    if config.get("proxy"):
        proxy_config = {"server": config["proxy"]}

    browser_args = ["--disable-blink-features=AutomationControlled"]

    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(
                headless=True,
                args=browser_args,
                proxy=proxy_config
            )
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
            )
            page = context.new_page()

            # Step 1: Open Portal
            page.goto(portal_url, wait_until="domcontentloaded", timeout=25000)
            page.wait_for_timeout(1000)

            # Step 2: Select Province
            province_selector = page.locator('select#form, select[name="sede"]').first
            if not province_selector.is_visible():
                browser.close()
                return {"has_slots": False, "status": "error", "message": "Province dropdown not found"}

            matched_prov = False
            for opt in province_selector.locator("option").all():
                if province.lower() in opt.inner_text().lower():
                    province_selector.select_option(value=opt.get_attribute("value"))
                    matched_prov = True
                    break

            if not matched_prov:
                browser.close()
                return {"has_slots": False, "status": "error", "message": f"Province '{province}' not in portal list"}

            accept_btn = page.locator('input#btnAceptar, input[type="submit"][value="Aceptar"]').first
            if accept_btn.is_visible():
                accept_btn.click()
                page.wait_for_load_state("domcontentloaded")
                page.wait_for_timeout(1500)

            # Step 3: Select Procedure
            procedure_selectors = ['select#tramiteGrupo\\[0\\]', 'select[name="tramite"]', 'select#tramite']
            tramite_dropdown = None
            for sel in procedure_selectors:
                loc = page.locator(sel).first
                if loc.is_visible():
                    tramite_dropdown = loc
                    break

            if not tramite_dropdown:
                browser.close()
                return {"has_slots": False, "status": "error", "message": "Procedure dropdown not found"}

            matched_proc = False
            for opt in tramite_dropdown.locator("option").all():
                if procedure.upper() in opt.inner_text().upper():
                    tramite_dropdown.select_option(value=opt.get_attribute("value"))
                    matched_proc = True
                    break

            if not matched_proc:
                browser.close()
                return {"has_slots": False, "status": "error", "message": f"Procedure '{procedure}' not found for {province}"}

            accept_btn = page.locator('input#btnAceptar, input[type="submit"][value="Aceptar"]').first
            if accept_btn.is_visible():
                accept_btn.click()
                page.wait_for_load_state("domcontentloaded")
                page.wait_for_timeout(1500)

            # Check if immediately returned no citas
            body_text = page.locator("body").inner_text().lower()
            if "no hay citas disponibles" in body_text:
                browser.close()
                return {"has_slots": False, "status": "no_slots", "message": "En este momento no hay citas disponibles"}

            # Step 4: Click Entrar if instructions page shown
            entrar_btn = page.locator('input#btnEntrar, input[value="Entrar"]').first
            if entrar_btn.is_visible():
                entrar_btn.click()
                page.wait_for_load_state("domcontentloaded")
                page.wait_for_timeout(1500)

            body_text = page.locator("body").inner_text().lower()
            if "no hay citas disponibles" in body_text:
                browser.close()
                return {"has_slots": False, "status": "no_slots", "message": "En este momento no hay citas disponibles"}

            # Step 5: Fill Document Info to reach the availability check
            doc_type = config.get("document_type", "NIE").upper()
            if doc_type == "NIE":
                nie_radio = page.locator('input#rdbTipoDocNie').first
                if nie_radio.is_visible():
                    nie_radio.check()
            elif doc_type in ["PASAPORTE", "PAS"]:
                pas_radio = page.locator('input#rdbTipoDocPas').first
                if pas_radio.is_visible():
                    pas_radio.check()

            id_input = page.locator('input#txtIdCitado').first
            name_input = page.locator('input#txtDesCitado').first
            if id_input.is_visible() and config.get("document_number"):
                id_input.fill(config["document_number"])
            if name_input.is_visible() and config.get("full_name"):
                name_input.fill(config["full_name"])

            year_input = page.locator('input#txtAnnoCitado').first
            if year_input.is_visible() and config.get("year_of_birth"):
                year_input.fill(str(config["year_of_birth"]))

            country_select = page.locator('select#txtPaisNac').first
            if country_select.is_visible() and config.get("country_of_nationality"):
                for opt in country_select.locator("option").all():
                    if config["country_of_nationality"].upper() in opt.inner_text().upper():
                        country_select.select_option(value=opt.get_attribute("value"))
                        break

            submit_btn = page.locator('input#btnEnviar, input[type="submit"][value="Aceptar"]').first
            if submit_btn.is_visible():
                submit_btn.click()
                page.wait_for_load_state("domcontentloaded")
                page.wait_for_timeout(2000)

            # Step 6: Action Menu (Solicitar Cita)
            solicitar_btn = page.locator('input#btnEnviar[value="Solicitar Cita"], input[value="Solicitar Cita"]').first
            if solicitar_btn.is_visible():
                solicitar_btn.click()
                page.wait_for_load_state("domcontentloaded")
                page.wait_for_timeout(2000)

            # Step 7: Contact Information
            phone_input = page.locator('input#txtTelefonoCitado').first
            email_input = page.locator('input#emailCMP').first
            email2_input = page.locator('input#emailCMP2').first

            if phone_input.is_visible() or email_input.is_visible():
                if phone_input.is_visible() and config.get("phone"):
                    phone_input.fill(config["phone"])
                if email_input.is_visible() and config.get("email"):
                    email_input.fill(config["email"])
                if email2_input.is_visible() and config.get("email"):
                    email2_input.fill(config["email"])

                next_btn = page.locator('input#btnSiguiente, input[value="Siguiente"]').first
                if next_btn.is_visible():
                    next_btn.click()
                    page.wait_for_load_state("domcontentloaded")
                    page.wait_for_timeout(2000)

            # Final evaluation
            current_body = page.locator("body").inner_text().lower()
            browser.close()

            if "no hay citas disponibles" in current_body:
                return {"has_slots": False, "status": "no_slots", "message": "En este momento no hay citas disponibles"}
            elif "ha superado el número de intentos" in current_body or "bloqueada" in current_body:
                return {"has_slots": False, "status": "rate_limited", "message": "Portal rate limit / CAPTCHA required"}
            else:
                # Calendar, offices, or slot times detected!
                return {
                    "has_slots": True,
                    "status": "available",
                    "message": "¡Citas detectadas! Calendario u oficina disponible para seleccionar",
                    "office": province
                }

        except PlaywrightTimeoutError:
            return {
                "has_slots": False,
                "status": "timeout",
                "message": "Conexión lenta o tiempo de espera agotado al conectar al portal"
            }
        except Exception as e:
            return {"has_slots": False, "status": "error", "message": str(e)}


def execute_monitor_check(monitor: dict, config: dict, force_alert: bool = False) -> dict:
    """Executes a single check for a target monitor and triggers Telegram broadcast if open."""
    monitor_id = monitor["id"]
    province = monitor["province"]
    procedure = monitor["procedure"]

    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] 🔍 Checking {province} -> {procedure}...")
    result = check_slot_availability(province, procedure, config)
    print(f"      Result: {result['status']} | {result['message']}")

    has_slots = result["has_slots"]
    status = result["status"]
    message = result["message"]

    alerted = False

    if has_slots or force_alert:
        # Check cooldown
        last_alert_str = monitor.get("last_alert_at")
        in_cooldown = False
        if last_alert_str and not force_alert:
            try:
                last_alert_dt = datetime.datetime.fromisoformat(last_alert_str)
                elapsed = (datetime.datetime.now(datetime.timezone.utc) - last_alert_dt).total_seconds()
                cooldown_sec = config.get("cooldown_minutes", DEFAULT_COOLDOWN_MINUTES) * 60
                if elapsed < cooldown_sec:
                    in_cooldown = True
                    print(f"      ⏳ In cooldown (Alerted {int(elapsed)}s ago. Cooldown: {cooldown_sec}s). Suppressing duplicate broadcast.")
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
                    office=result.get("office"),
                    portal_url=config.get("portal_url", DEFAULT_PORTAL_URL)
                )
                print(f"      🚀 Broadcasting alert to Telegram ({chat_id})...")
                broadcast_res = broadcast_telegram_message(bot_token, chat_id, alert_text)
                print(f"      Broadcast result: {broadcast_res}")
                alerted = True
            else:
                print("      ⚠️ Slots detected but Telegram credentials are not configured in config.json / admin dashboard.")

    update_monitor_status(monitor_id, status, message, alerted=alerted)
    return {"monitor_id": monitor_id, "status": status, "message": message, "alerted": alerted}


def run_daemon(interval_sec: Optional[int] = None):
    print("=" * 65)
    print("  📡 Cita Previa Extranjería - Live Slot Monitor & Broadcaster")
    print("=" * 65)
    ensure_monitor_tables()

    while True:
        try:
            config = load_config()
            monitors = get_active_monitors()

            if not monitors:
                print("No active monitors found. Waiting 30s...")
                time.sleep(30)
                continue

            for monitor in monitors:
                execute_monitor_check(monitor, config)
                time.sleep(2)  # Short pause between different monitors

            sleep_time = interval_sec or config.get("monitor_interval_seconds", 60)
            print(f"Sleeping for {sleep_time}s until next check cycle...\n")
            time.sleep(sleep_time)

        except KeyboardInterrupt:
            print("\nMonitor daemon stopped by user.")
            break
        except Exception as e:
            print(f"[Error in monitor loop]: {e}")
            time.sleep(15)


def main():
    parser = argparse.ArgumentParser(description="Live Appointment Monitor & Telegram Broadcaster")
    parser.add_argument("--daemon", action="store_true", help="Run the continuous monitoring loop")
    parser.add_argument("--check-once", action="store_true", help="Run a single check across all active monitors")
    parser.add_argument("--test-broadcast", action="store_true", help="Send a test notification to Telegram")
    parser.add_argument("--interval", type=int, help="Polling interval in seconds")
    parser.add_argument("--add", nargs=2, metavar=("PROVINCE", "PROCEDURE"), help="Add a new target to monitor")
    args = parser.parse_args()

    ensure_monitor_tables()
    config = load_config()

    if args.add:
        prov, proc = args.add
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with get_db() as conn:
            conn.execute(
                "INSERT INTO monitors (province, procedure, is_active, created_at) VALUES (?, ?, 1, ?)",
                (prov, proc, now)
            )
            conn.commit()
        print(f"✅ Added monitor for: {prov} -> {proc}")
        return

    if args.test_broadcast:
        tg_config = config.get("telegram", {})
        bot_token = tg_config.get("bot_token", "").strip()
        chat_id = tg_config.get("chat_id", "").strip()
        if not bot_token or not chat_id:
            print("❌ Telegram is not configured. Please set 'bot_token' and 'chat_id' in config.json or Admin UI.")
            sys.exit(1)
        print(f"Sending test broadcast to {chat_id}...")
        res = send_test_broadcast(bot_token, chat_id)
        print("Result:", json.dumps(res, indent=2))
        return

    if args.check_once:
        monitors = get_active_monitors()
        print(f"Checking {len(monitors)} monitor targets once:")
        for m in monitors:
            res = execute_monitor_check(m, config)
            print("Result:", res)
        return

    if args.daemon:
        run_daemon(args.interval)
        return

    # Default if no arguments: print status
    monitors = get_active_monitors()
    print("=" * 65)
    print("  Active Monitored Targets:")
    for m in monitors:
        print(f"  - [{m['id']}] {m['province']} | {m['procedure']} | Status: {m.get('last_status', 'pending')}")
    print("=" * 65)
    print("Usage: python monitor.py [--daemon | --check-once | --test-broadcast | --add <prov> <proc>]")


if __name__ == "__main__":
    main()

