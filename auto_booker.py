#!/usr/bin/env python3
"""
Auto-Claimer & Assisted Booking Runner for Cita Previa Extranjería
- Automatically claims detected open slots for queued applicants
- Pre-fills all forms (Province, Trámite, Identity, Contact)
- Automatically selects the first available appointment slot
- Alerts operator and sends Telegram notification when CAPTCHA or SMS OTP is required
- Accepts OTP from Admin Dashboard or Terminal, submits code, and saves confirmation receipt
"""

import sys
import os
import time
import json
import sqlite3
import argparse
import datetime
import subprocess
from pathlib import Path
from typing import Optional, Dict, Any

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from telegram_broadcaster import broadcast_telegram_message

BASE_DIR = Path(__file__).parent
CONFIG_PATH = BASE_DIR / "config.json"
DB_PATH = BASE_DIR / "orders.db"
CONFIRMATIONS_DIR = BASE_DIR / "confirmations"
CONFIRMATIONS_DIR.mkdir(exist_ok=True)

DEFAULT_PORTAL_URL = "https://icp.administracionelectronica.gob.es/icpplus/index.html"


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


def play_urgent_alarm():
    """Play macOS sound or terminal bell to alert human operator."""
    try:
        if sys.platform == "darwin":
            sound_file = "/System/Library/Sounds/Glass.aiff"
            if os.path.exists(sound_file):
                for _ in range(4):
                    subprocess.run(["afplay", sound_file], check=False)
                    time.sleep(0.15)
                return
    except Exception:
        pass
    print("\a\a\a\a")


def update_order_status(order_id: str, status: str, notes: Optional[str] = None):
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    with get_db() as conn:
        conn.execute(
            """
            UPDATE orders 
            SET status = ?, notes = COALESCE(?, notes), updated_at = ? 
            WHERE order_id = ?
            """,
            (status, notes, now, order_id.upper())
        )
        conn.commit()


def send_tg_notification(headline: str, body: str):
    config = load_config()
    tg = config.get("telegram", {})
    if not tg.get("enabled"):
        return
    bot_token = tg.get("bot_token", "").strip()
    chat_id = tg.get("chat_id", "").strip()
    if bot_token and chat_id:
        text = f"<b>{headline}</b>\n\n{body}\n\n⏰ <code>{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</code>"
        broadcast_telegram_message(bot_token, chat_id, text)


def get_otp_from_queue_or_file(order_id: str, timeout_seconds: int = 240) -> Optional[str]:
    """
    Waits for operator to enter OTP code via web dashboard or temporary file.
    The web dashboard writes to: /tmp/cita_otp_{order_id}.txt
    """
    otp_file = Path(f"/tmp/cita_otp_{order_id}.txt")
    if otp_file.exists():
        otp_file.unlink(missing_ok=True)

    print(f"\n⏳ Waiting up to {timeout_seconds}s for SMS OTP for {order_id}...")
    print(f"👉 Enter OTP in Admin Dashboard, or create {otp_file} with code, or type below:")

    t_start = time.time()
    while time.time() - t_start < timeout_seconds:
        if otp_file.exists():
            code = otp_file.read_text(encoding="utf-8").strip()
            if code:
                otp_file.unlink(missing_ok=True)
                return code
        time.sleep(1.0)
    return None


def run_auto_booking(order_id: str, headless: bool = False) -> Dict[str, Any]:
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM orders WHERE order_id = ?", (order_id.upper(),))
        order = cur.fetchone()
        if not order:
            return {"status": "error", "message": f"Order {order_id} not found in database"}
        order = dict(order)

    config = load_config()
    portal_url = config.get("portal_url", DEFAULT_PORTAL_URL)
    update_order_status(order_id, "searching", "Iniciando proceso de reserva automática...")

    print("=" * 65)
    print(f"  🤖 Auto-Booking Runner Initiated for {order['name']}")
    print(f"  Order ID: {order['order_id']} | Doc: {order['doc_number']}")
    print(f"  Province: {order['province']} | Trámite: {order['procedure']}")
    print("=" * 65)

    with sync_playwright() as p:
        # Launch browser (visible by default so operator can solve CAPTCHA if prompted)
        browser = p.chromium.launch(
            headless=headless,
            args=["--start-maximized", "--disable-blink-features=AutomationControlled"]
        )
        context = browser.new_context(
            no_viewport=True,
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
        )
        page = context.new_page()

        try:
            # Step 1: Open Portal
            print("[1/7] Opening Portal...")
            page.goto(portal_url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(1000)

            # Step 2: Select Province
            print(f"[2/7] Selecting Province: {order['province']}...")
            prov_sel = page.locator('select#form, select[name="sede"]').first
            prov_matched = False
            for opt in prov_sel.locator("option").all():
                if order["province"].lower() in opt.inner_text().lower():
                    prov_sel.select_option(value=opt.get_attribute("value"))
                    prov_matched = True
                    break

            if not prov_matched:
                update_order_status(order_id, "failed", f"Provincia '{order['province']}' no encontrada en el portal")
                browser.close()
                return {"status": "failed", "message": "Province not found"}

            page.locator('input#btnAceptar, input[type="submit"][value="Aceptar"]').first.click()
            page.wait_for_load_state("domcontentloaded")
            page.wait_for_timeout(1500)

            # Step 3: Select Procedure
            print(f"[3/7] Selecting Procedure: {order['procedure']}...")
            proc_selectors = ['select#tramiteGrupo\\[0\\]', 'select[name="tramite"]', 'select#tramite']
            tramite_sel = None
            for sel in proc_selectors:
                loc = page.locator(sel).first
                if loc.is_visible():
                    tramite_sel = loc
                    break

            if tramite_sel:
                matched_proc = False
                for opt in tramite_sel.locator("option").all():
                    if order["procedure"].upper() in opt.inner_text().upper():
                        tramite_sel.select_option(value=opt.get_attribute("value"))
                        matched_proc = True
                        break

                if matched_proc:
                    page.locator('input#btnAceptar, input[type="submit"][value="Aceptar"]').first.click()
                    page.wait_for_load_state("domcontentloaded")
                    page.wait_for_timeout(1500)

            # Check if instructions page shown
            entrar_btn = page.locator('input#btnEntrar, input[value="Entrar"]').first
            if entrar_btn.is_visible():
                entrar_btn.click()
                page.wait_for_load_state("domcontentloaded")
                page.wait_for_timeout(1500)

            # Step 4: Fill Applicant Personal Details
            print("[4/7] Auto-filling Personal Details...")
            doc_type = order.get("doc_type", "NIE").upper()
            if doc_type == "NIE":
                page.locator('input#rdbTipoDocNie').first.check()
            elif doc_type in ["PASAPORTE", "PAS"]:
                page.locator('input#rdbTipoDocPas').first.check()

            id_input = page.locator('input#txtIdCitado').first
            name_input = page.locator('input#txtDesCitado').first
            if id_input.is_visible():
                id_input.fill(order["doc_number"])
            if name_input.is_visible():
                name_input.fill(order["name"])

            year_input = page.locator('input#txtAnnoCitado').first
            if year_input.is_visible():
                year_input.fill("1990")

            country_sel = page.locator('select#txtPaisNac').first
            if country_sel.is_visible() and order.get("citizenship"):
                for opt in country_sel.locator("option").all():
                    if order["citizenship"].upper() in opt.inner_text().upper():
                        country_sel.select_option(value=opt.get_attribute("value"))
                        break

            # Check for CAPTCHA
            captcha_img = page.locator('img[src*="captcha"], img#captcha').first
            if captcha_img.is_visible():
                print("\n🚨🚨 CAPTCHA DETECTED ON FORM! 🚨🚨")
                play_urgent_alarm()
                send_tg_notification(
                    headline="🚨 <b>ATENCIÓN: RESUELVA EL CAPTCHA</b> 🚨",
                    body=(
                        f"👤 <b>Cliente:</b> {order['name']} ({order['doc_number']})\n"
                        f"📍 <b>Provincia:</b> {order['province']}\n"
                        f"👉 <i>Resuelva el CAPTCHA visual en la pantalla para continuar.</i>"
                    )
                )
                print("👉 Please solve the visual CAPTCHA in the browser window and press Submit.")
                # Wait until navigation occurs past the identity page
                page.wait_for_selector('input#btnEnviar[value="Solicitar Cita"], input[value="Solicitar Cita"]', timeout=90000)
            else:
                submit_btn = page.locator('input#btnEnviar, input[type="submit"][value="Aceptar"]').first
                if submit_btn.is_visible():
                    submit_btn.click()
                    page.wait_for_load_state("domcontentloaded")
                    page.wait_for_timeout(2000)

            # Step 5: Click 'Solicitar Cita'
            print("[5/7] Clicking 'Solicitar Cita'...")
            solicitar_btn = page.locator('input#btnEnviar[value="Solicitar Cita"], input[value="Solicitar Cita"]').first
            if solicitar_btn.is_visible():
                solicitar_btn.click()
                page.wait_for_load_state("domcontentloaded")
                page.wait_for_timeout(2000)

            # Step 6: Contact Info
            print("[6/7] Entering Phone & Email...")
            phone_input = page.locator('input#txtTelefonoCitado').first
            email_input = page.locator('input#emailCMP').first
            email2_input = page.locator('input#emailCMP2').first

            if phone_input.is_visible() or email_input.is_visible():
                if phone_input.is_visible() and order.get("phone"):
                    phone_input.fill(order["phone"])
                if email_input.is_visible() and order.get("email"):
                    email_input.fill(order["email"])
                if email2_input.is_visible() and order.get("email"):
                    email2_input.fill(order["email"])

                page.locator('input#btnSiguiente, input[value="Siguiente"]').first.click()
                page.wait_for_load_state("domcontentloaded")
                page.wait_for_timeout(2000)

            # Step 7: Check availability & slot selection
            body_text = page.locator("body").inner_text()
            if "no hay citas disponibles" in body_text.lower():
                print("❌ No appointment slots currently open at final step.")
                update_order_status(order_id, "waiting", "No hubo citas disponibles en el paso final. De vuelta a la cola.")
                browser.close()
                return {"status": "no_slots", "message": "No slots available at final step"}

            # Slots found! Select first radio/option if present
            radio_slot = page.locator('input[type="radio"][name*="cita"]').first
            if radio_slot.is_visible():
                print("👉 Auto-selecting first available appointment slot...")
                radio_slot.check()
                page.locator('input#btnSiguiente, input[value="Siguiente"]').first.click()
                page.wait_for_load_state("domcontentloaded")
                page.wait_for_timeout(2000)

            # Step 8: SMS OTP VERIFICATION CHECKPOINT
            print("\n" + "=" * 65)
            print("🚨🚨 REACHED SMS OTP VERIFICATION SCREEN! 🚨🚨")
            print("=" * 65)

            update_order_status(order_id, "code_pending", "Código SMS enviado al teléfono del cliente. Esperando código...")
            play_urgent_alarm()

            send_tg_notification(
                headline="📲 <b>¡URGENTE: CÓDIGO SMS ENVIADO!</b> 📲",
                body=(
                    f"👤 <b>Cliente:</b> {order['name']} (<code>{order['doc_number']}</code>)\n"
                    f"📱 <b>Teléfono:</b> <code>{order['phone']}</code>\n"
                    f"📍 <b>Provincia:</b> {order['province']}\n\n"
                    f"⚡ <i>El portal del Gobierno ha enviado el código SMS al teléfono del cliente.</i>\n"
                    f"👉 <b>Introduce el código SMS recibido en el panel de control o terminal.</b>"
                )
            )

            # Wait for SMS OTP input
            otp_code = get_otp_from_queue_or_file(order_id, timeout_seconds=300)
            if not otp_code:
                # If operator didn't enter via web dashboard, prompt in terminal
                try:
                    otp_code = input("\n👉 Enter SMS OTP code from client >> ").strip()
                except Exception:
                    pass

            if otp_code:
                print(f"👉 Submitting OTP code: {otp_code}...")
                otp_input = page.locator('input#txtCodigoVerificacion, input[name*="codigo"], input[name*="sms"]').first
                if otp_input.is_visible():
                    otp_input.fill(otp_code)

                # Check terms checkbox if present
                terms_chk = page.locator('input[type="checkbox"][name*="chk"]').first
                if terms_chk.is_visible():
                    terms_chk.check()

                confirm_btn = page.locator('input#btnConfirmar, input[value="Confirmar"], input[value="Aceptar"]').first
                if confirm_btn.is_visible():
                    confirm_btn.click()
                    page.wait_for_load_state("domcontentloaded")
                    page.wait_for_timeout(3000)

                # Check confirmation
                confirm_body = page.locator("body").inner_text()
                receipt_path = CONFIRMATIONS_DIR / f"receipt_{order_id}.png"
                page.screenshot(path=str(receipt_path), full_page=True)

                if "justificante" in confirm_body.lower() or "confirmada" in confirm_body.lower() or "número de justificante" in confirm_body.lower():
                    print("🎉🎉 APPOINTMENT SUCCESSFULLY BOOKED! 🎉🎉")
                    update_order_status(order_id, "booked", f"Cita confirmada exitosamente. Justificante guardado en {receipt_path.name}")
                    send_tg_notification(
                        headline="✅ <b>¡CITA CONFIRMADA EXITOSAMENTE!</b> ✅",
                        body=(
                            f"👤 <b>Titular:</b> {order['name']} ({order['doc_number']})\n"
                            f"📍 <b>Provincia:</b> {order['province']}\n"
                            f"📋 <b>Trámite:</b> {order['procedure']}\n"
                            f"📄 <b>Justificante:</b> Guardado en el servidor."
                        )
                    )
                    browser.close()
                    return {"status": "booked", "order_id": order_id, "receipt": str(receipt_path)}
                else:
                    print("⚠️ Code entered but confirmation page was not clearly detected. Check screenshot.")
                    update_order_status(order_id, "code_pending", "Código enviado, revise el navegador.")

            print("Closing browser. Process complete.")
            browser.close()
            return {"status": "finished", "order_id": order_id}

        except Exception as e:
            print(f"Error during auto booking: {e}")
            update_order_status(order_id, "failed", f"Error en ejecución: {str(e)[:100]}")
            browser.close()
            return {"status": "error", "message": str(e)}


def trigger_next_waiting_order(province: str, procedure: str) -> Optional[dict]:
    """Finds the next waiting client for this province/procedure and triggers auto-booking."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT * FROM orders 
            WHERE status = 'waiting' 
              AND LOWER(province) = LOWER(?)
            ORDER BY id ASC 
            LIMIT 1
            """,
            (province,)
        )
        row = cur.fetchone()
        if row:
            order_data = dict(row)
            order_id = order_data["order_id"]
            print(f"🎯 Found queued client for {province}: {order_data['name']} ({order_id})")
            return order_data
    return None


def main():
    parser = argparse.ArgumentParser(description="Auto-Claimer & Booking Runner for Cita Previa")
    parser.add_argument("--order-id", type=str, help="Order ID to process (e.g. ORDER-514792)")
    parser.add_argument("--auto-pick", nargs=2, metavar=("PROVINCE", "PROCEDURE"), help="Automatically pick next waiting order for province")
    parser.add_argument("--headless", action="store_true", help="Run browser in headless mode")
    args = parser.parse_args()

    if args.order_id:
        res = run_auto_booking(args.order_id, headless=args.headless)
        print("Result:", json.dumps(res, indent=2))
        return

    if args.auto_pick:
        prov, proc = args.auto_pick
        order = trigger_next_waiting_order(prov, proc)
        if order:
            res = run_auto_booking(order["order_id"], headless=args.headless)
            print("Result:", json.dumps(res, indent=2))
        else:
            print(f"No clients currently waiting for {prov} in orders.db.")
        return

    print("Usage: python auto_booker.py --order-id ORDER-123456 [--headless]")
    print("       python auto_booker.py --auto-pick 'Barcelona' 'POLICIA-TOMA DE HUELLA'")


if __name__ == "__main__":
    main()

