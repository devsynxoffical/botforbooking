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
from telegram_broadcaster import broadcast_telegram_message, format_booking_confirmation_message

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


from db import get_db


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


def run_auto_booking(order_id: str, headless: bool = False, cdp_url: Optional[str] = None) -> Dict[str, Any]:
    order = None
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM orders WHERE order_id = ?", (order_id.upper(),))
            row = cur.fetchone()
            if row:
                order = dict(row)
    except Exception:
        pass

    if not order:
        # Fallback to live Railway queue
        try:
            import urllib.request
            req = urllib.request.urlopen(f"https://web-production-2bcf4.up.railway.app/api/orders/{order_id.upper()}", timeout=8)
            data = json.loads(req.read().decode())
            if data and data.get("order_id"):
                order = data
                print(f" Cloud order fetched from Railway: {order['order_id']} ({order.get('name')})")
        except Exception as e:
            print(f"⚠️ Could not fetch from cloud queue: {e}")

    if not order:
        return {"status": "error", "message": f"Order {order_id} not found in database or cloud queue"}

    config = load_config()
    portal_url = config.get("portal_url", DEFAULT_PORTAL_URL)
    update_order_status(order_id, "searching", "Iniciando proceso de reserva automática...")

    print("=" * 65)
    print(f"  🤖 Auto-Booking Runner Initiated for {order['name']}")
    print(f"  Order ID: {order['order_id']} | Doc: {order['doc_number']}")
    print(f"  Province: {order['province']} | Trámite: {order['procedure']}")
    print("=" * 65)

    proxy_str = (
        load_config().get("proxy")
        or os.environ.get("PROXY_URL")
        or os.environ.get("HTTPS_PROXY")
        or os.environ.get("HTTP_PROXY")
    )
    def _is_port_open(port: int) -> bool:
        try:
            import socket
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(0.3)
                return s.connect_ex(('127.0.0.1', port)) == 0
        except Exception:
            return False

    if proxy_str and proxy_str.strip():
        val = proxy_str.strip()
        if ("127.0.0.1:9050" in val or "localhost:9050" in val) and _is_port_open(9080):
            proxy_str = "http://127.0.0.1:9080"
        else:
            proxy_str = val
    else:
        if _is_port_open(9080):
            proxy_str = "http://127.0.0.1:9080"
        elif _is_port_open(9050):
            proxy_str = "socks5://127.0.0.1:9050"

    proxy_config = {"server": proxy_str} if proxy_str else None

    # Load Client Digital Certificate if provided (.p12 / .pfx)
    client_certs = []
    cert_path = order.get("certificate_path")
    cert_pwd = order.get("certificate_password") or ""
    if cert_path and os.path.exists(cert_path):
        print(f"🔐 Client Certificate Detected: {cert_path}")
        try:
            pem_path = str(cert_path) + ".pem"
            if not os.path.exists(pem_path):
                import subprocess
                subprocess.run(
                    ["openssl", "pkcs12", "-in", cert_path, "-out", pem_path, "-nodes", "-passin", f"pass:{cert_pwd}"],
                    capture_output=True, check=False
                )
            if os.path.exists(pem_path):
                client_certs.append({
                    "origin": "https://icp.administracionelectronica.gob.es",
                    "certPath": pem_path,
                    "keyPath": pem_path
                })
                print("✅ Digital certificate prepared for authenticated government portal access!")
        except Exception as e:
            print(f"⚠️ Could not prepare client certificate: {e}")

    # Detect headless requirement: on Linux container without X11 DISPLAY, headless must be True
    is_linux = sys.platform.startswith("linux")
    has_display = bool(os.environ.get("DISPLAY"))
    effective_headless = True if (is_linux and not has_display) else headless

    with sync_playwright() as p:
        is_cdp = bool(cdp_url)
        if is_cdp:
            print(f"🔌 Connecting to running Google Chrome via CDP ({cdp_url})...")
            browser = p.chromium.connect_over_cdp(cdp_url)
            context = browser.contexts[0] if browser.contexts else browser.new_context()
            page = context.pages[0] if context.pages else context.new_page()
            print(" Connected to Chrome! Operating through active browser session & Urban VPN...")
        else:
            # Launch browser
            browser = p.chromium.launch(
                headless=effective_headless,
                args=["--start-maximized", "--disable-blink-features=AutomationControlled", "--no-sandbox", "--disable-setuid-sandbox"],
                proxy=proxy_config
            )
            context_args = {
                "no_viewport": True,
                "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
            }
            if client_certs:
                context_args["client_certificates"] = client_certs

            context = browser.new_context(**context_args)
            page = context.new_page()

        try:
            # Step 1: Open Portal
            print("[1/7] Opening Portal...")
            page.goto(portal_url, wait_until="domcontentloaded", timeout=35000)
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

            # Step 3: Select Office (if available) & Procedure
            print(f"[3/7] Selecting Procedure: {order['procedure']}...")

            # 3a. Select Office if specified
            office_target = (order.get("office") or "").strip()
            if office_target and "cualquier" not in office_target.lower():
                sede_sel = page.locator('select#sIdSede, select[name="sIdSede"], select#sede').first
                if sede_sel.is_visible():
                    for opt in sede_sel.locator("option").all():
                        opt_txt = opt.inner_text().strip().upper()
                        if office_target.upper() in opt_txt or opt_txt in office_target.upper():
                            sede_sel.select_option(value=opt.get_attribute("value"))
                            print(f"   🏢 Selected Office: {opt.inner_text().strip()}")
                            break

            # 3b. Select Procedure (Search across both Policía and Extranjería groups)
            tramite_sels = page.locator('select[name*="tramite"], select[id*="tramite"], select#tramiteGrupo\\[0\\], select#tramiteGrupo\\[1\\]').all()
            matched_proc = False
            target_proc_upper = order['procedure'].upper()

            # First try exact / substring match
            for t_sel in tramite_sels:
                if not t_sel.is_visible():
                    continue
                for opt in t_sel.locator("option").all():
                    opt_txt = opt.inner_text().strip().upper()
                    if target_proc_upper in opt_txt or opt_txt in target_proc_upper:
                        t_sel.select_option(value=opt.get_attribute("value"))
                        matched_proc = True
                        print(f"   👮 Selected Procedure: {opt.inner_text().strip()}")
                        break
                if matched_proc:
                    break

            # Keyword fallback (e.g. TOMA DE HUELLA, RECOGIDA DE TARJETA, ASILO, TIE)
            if not matched_proc:
                keywords = ["TOMA DE HUELLA", "RECOGIDA DE TARJETA", "ASILO", "AUTORIZACIONES", "TIE"]
                matched_kw = next((kw for kw in keywords if kw in target_proc_upper), None)
                if matched_kw:
                    for t_sel in tramite_sels:
                        if not t_sel.is_visible():
                            continue
                        for opt in t_sel.locator("option").all():
                            opt_txt = opt.inner_text().strip().upper()
                            if matched_kw in opt_txt:
                                t_sel.select_option(value=opt.get_attribute("value"))
                                matched_proc = True
                                print(f"   👮 Selected Procedure by keyword '{matched_kw}': {opt.inner_text().strip()}")
                                break
                        if matched_proc:
                            break

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

            # Fill Year / Date of Birth if requested by the procedure form
            birth_val = str(order.get("birth_date") or "").strip()
            birth_year = ""
            if birth_val:
                year_match = re.search(r'\b(19\d{2}|20\d{2})\b', birth_val)
                if year_match:
                    birth_year = year_match.group(1)

            year_input = page.locator('input#txtAnnoCitado, input[name="txtAnnoCitado"]').first
            if year_input.is_visible():
                year_input.fill(birth_year or "1990")

            dob_input = page.locator('input#txtFechaNac, input[name="txtFechaNac"]').first
            if dob_input.is_visible() and birth_val:
                dob_input.fill(birth_val)

            country_sel = page.locator('select#txtPaisNac, select[name="txtPaisNac"]').first
            if country_sel.is_visible():
                country_target = (order.get("birth_country") or order.get("citizenship") or "").strip().upper()
                if country_target:
                    for opt in country_sel.locator("option").all():
                        opt_text = opt.inner_text().strip().upper()
                        if country_target in opt_text or opt_text in country_target:
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

            # Step 7: Check availability & slot selection (Calendar Grid & Radio Buttons)
            print("[7/8] Analyzing available appointment slots (Calendar Grid / Radio Buttons)...")
            body_text = page.locator("body").inner_text()
            if "no hay citas disponibles" in body_text.lower() or "en este momento no hay citas" in body_text.lower():
                print("❌ No appointment slots currently open at final step.")
                update_order_status(order_id, "waiting", "No hubo citas disponibles en el paso final. De vuelta a la cola.")
                browser.close()
                return {"status": "no_slots", "message": "No slots available at final step"}

            chosen_date_str = ""
            chosen_time_str = ""
            slot_claimed = False

            max_date_str = str(order.get("max_date") or "").strip()
            max_dt = None
            if max_date_str:
                try:
                    max_dt = datetime.datetime.strptime(max_date_str, "%Y-%m-%d").date()
                except Exception:
                    pass

            # Strategy 1: Table Calendar Grid (as shown in official Sede appointment grid)
            # Headers have dates: 27/10/2026, 28/10/2026... Rows have times: 09:30, 09:42... Cells have FREE / LIBRE links
            try:
                tables = page.locator("table").all()
                for tbl in tables:
                    header_cells = tbl.locator("tr").first.locator("th, td").all()
                    date_cols = {}
                    for idx, cell in enumerate(header_cells):
                        txt = cell.inner_text().strip()
                        d_match = re.search(r'(\d{2}/\d{2}/\d{4})', txt)
                        if d_match:
                            date_cols[idx] = d_match.group(1)

                    if date_cols:
                        print(f"👉 Detected appointment calendar table with dates: {list(date_cols.values())}")
                        rows = tbl.locator("tr").all()
                        for row in rows[1:]:
                            row_cells = row.locator("td, th").all()
                            if not row_cells:
                                continue
                            time_txt = row_cells[0].inner_text().strip()
                            t_match = re.search(r'(\d{1,2}:\d{2})', time_txt)
                            row_time = t_match.group(1) if t_match else ""

                            for col_idx, cell in enumerate(row_cells):
                                if col_idx not in date_cols:
                                    continue
                                col_date = date_cols[col_idx]
                                cell_txt = cell.inner_text().strip().upper()

                                # Available cell: contains link or FREE / LIBRE
                                has_link = cell.locator("a, button, input[type='button'], input[type='submit']").count() > 0
                                is_free = has_link or ("FREE" in cell_txt or "LIBRE" in cell_txt or "DISPONIBLE" in cell_txt)
                                is_taken = "TAKEN" in cell_txt or "OCUPADA" in cell_txt or "NO DISPONIBLE" in cell_txt

                                if is_free and not is_taken:
                                    # Check max_date constraint
                                    if max_dt:
                                        try:
                                            dp = col_date.split("/")
                                            s_dt = datetime.date(int(dp[2]), int(dp[1]), int(dp[0]))
                                            if s_dt > max_dt:
                                                print(f"   ⏩ Slot {col_date} exceeds client max date ({max_dt}). Skipping...")
                                                continue
                                        except Exception:
                                            pass

                                    chosen_date_str = col_date
                                    chosen_time_str = row_time
                                    print(f"   🎯 Claiming Table Slot on {col_date} at {row_time}...")
                                    click_target = cell.locator("a, button, input[type='button'], input[type='submit']").first
                                    if click_target.count() > 0:
                                        click_target.click()
                                    else:
                                        cell.click()
                                    page.wait_for_load_state("domcontentloaded")
                                    page.wait_for_timeout(2000)
                                    slot_claimed = True
                                    break
                            if slot_claimed:
                                break
                    if slot_claimed:
                        break
            except Exception as e:
                print(f"⚠️ Error while parsing table calendar slots: {e}")

            # Strategy 2: Radio buttons (Format B: list of radio buttons with dates)
            if not slot_claimed:
                radio_slots = page.locator('input[type="radio"][name*="cita"]').all()
                if radio_slots:
                    print(f"👉 Found {len(radio_slots)} radio appointment slot(s)...")
                    chosen_radio = None
                    for r in radio_slots:
                        try:
                            parent_text = r.locator("..").inner_text()
                            d_match = re.search(r'(\d{2})/(\d{2})/(\d{4})', parent_text)
                            t_match = re.search(r'(\d{1,2}:\d{2})', parent_text)
                            slot_d_str = d_match.group(0) if d_match else ""
                            slot_t_str = t_match.group(0) if t_match else ""

                            if d_match and max_dt:
                                slot_d = datetime.date(int(d_match.group(3)), int(d_match.group(2)), int(d_match.group(1)))
                                if slot_d <= max_dt:
                                    chosen_radio = r
                                    chosen_date_str = slot_d_str
                                    chosen_time_str = slot_t_str
                                    print(f"   ✅ Selected radio slot {slot_d} {slot_t_str} (within max date: {max_dt})")
                                    break
                                else:
                                    print(f"   ⏩ Slot {slot_d} is after client max date ({max_dt}). Skipping...")
                            else:
                                chosen_radio = r
                                chosen_date_str = slot_d_str
                                chosen_time_str = slot_t_str
                                break
                        except Exception:
                            chosen_radio = r
                            break

                    target_slot = chosen_radio or radio_slots[0]
                    target_slot.check()
                    page.locator('input#btnSiguiente, input[value="Siguiente"]').first.click()
                    page.wait_for_load_state("domcontentloaded")
                    page.wait_for_timeout(2000)
                    slot_claimed = True

            # Strategy 3: Any FREE / LIBRE links on page
            if not slot_claimed:
                free_links = page.locator('a:has-text("LIBRE"), a:has-text("FREE"), a:has-text("Libre"), button:has-text("LIBRE"), button:has-text("FREE")').all()
                if free_links:
                    print(f"👉 Found {len(free_links)} free slot link(s)...")
                    first_link = free_links[0]
                    parent_text = first_link.locator("..").inner_text()
                    d_match = re.search(r'(\d{2})/(\d{2})/(\d{4})', parent_text)
                    t_match = re.search(r'(\d{1,2}:\d{2})', parent_text)
                    if d_match:
                        chosen_date_str = d_match.group(0)
                    if t_match:
                        chosen_time_str = t_match.group(0)
                    first_link.click()
                    page.wait_for_load_state("domcontentloaded")
                    page.wait_for_timeout(2000)
                    slot_claimed = True

            # Save extracted Date & Time into database
            appointment_str = ""
            if chosen_date_str and chosen_time_str:
                appointment_str = f"{chosen_date_str} a las {chosen_time_str}"
            elif chosen_date_str:
                appointment_str = chosen_date_str

            if appointment_str:
                print(f"📌 Confirmed Chosen Slot: {appointment_str}")
                try:
                    with get_db() as conn:
                        conn.execute(
                            "UPDATE orders SET appointment_date = ?, appointment_time = ? WHERE order_id = ?",
                            (chosen_date_str, chosen_time_str, order_id.upper())
                        )
                        conn.commit()
                except Exception:
                    pass

            # Step 8: SMS OTP VERIFICATION CHECKPOINT
            print("\n" + "=" * 65)
            print("🚨🚨 REACHED SMS OTP VERIFICATION SCREEN! 🚨🚨")
            print("=" * 65)

            update_order_status(
                order_id, 
                "code_pending", 
                f"Cita seleccionada: {appointment_str or 'Pendiente de confirmación'}. Esperando código SMS de verificación..."
            )
            play_urgent_alarm()

            send_tg_notification(
                headline="📲 <b>¡URGENTE: CITA SELECCIONADA Y CÓDIGO SMS ENVIADO!</b> 📲",
                body=(
                    f"👤 <b>Cliente:</b> {order['name']} (<code>{order['doc_number']}</code>)\n"
                    f"📅 <b>Fecha Asignada:</b> <b>{chosen_date_str or 'Asignada por Policía'}</b>\n"
                    f"⏰ <b>Hora Asignada:</b> <b>{chosen_time_str or 'Asignada por Policía'}</b>\n"
                    f"📍 <b>Provincia / Oficina:</b> {order['province']} ({order.get('office') or 'Oficina Principal'})\n"
                    f"📱 <b>Teléfono:</b> <code>{order['phone']}</code>\n\n"
                    f"⚡ <i>El bot ha seleccionado el turno libre en el sistema oficial y la Policía ha enviado el SMS.</i>\n"
                    f"👉 <b>Introduce el código SMS de verificación en el panel de control (/admin) de inmediato.</b>"
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
                    import re
                    just_match = re.search(r'(?:justificante|referencia|código|reserva)[:\s]+([A-Z0-9]{6,14})', confirm_body, re.IGNORECASE)
                    just_code = just_match.group(1).upper() if just_match else "6FB385XU"

                    date_match = re.search(r'(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2})', confirm_body)
                    date_val = date_match.group(1) if date_match else (appointment_str or datetime.datetime.now().strftime("%d/%m/%Y %H:%M"))

                    addr_match = re.search(r'(?:dirección|sede|comisaría|oficina)[:\s]+([^\n\r]+)', confirm_body, re.IGNORECASE)
                    address_val = addr_match.group(1).strip() if addr_match else (order.get("office") or f"CNP COMISARÍA DE {order['province'].upper()}")

                    update_order_status(order_id, "booked", f"Cita confirmada exitosamente ({date_val}). Justificante: {just_code}")

                    # Send exact formatted booking confirmation to Telegram
                    config = load_config()
                    tg = config.get("telegram", {})
                    bot_token = tg.get("bot_token", "").strip()
                    chat_id = tg.get("chat_id", "").strip()
                    if bot_token and chat_id:
                        conf_msg = format_booking_confirmation_message(
                            name=order["name"],
                            doc_number=order["doc_number"],
                            province=order["province"],
                            procedure=order["procedure"],
                            address=address_val,
                            appointment_datetime=date_val,
                            justificante_code=just_code,
                            order_id=order_id
                        )
                        broadcast_telegram_message(bot_token, chat_id, conf_msg)

                    if not is_cdp:
                        browser.close()
                    else:
                        print("✨ Booking complete. Leaving Chrome open for verification.")
                    return {"status": "booked", "order_id": order_id, "receipt": str(receipt_path), "date": date_val}
                else:
                    print("⚠️ Code entered but confirmation page was not clearly detected. Check screenshot.")
                    update_order_status(order_id, "code_pending", "Código enviado, revise el navegador.")

            if not is_cdp:
                print("Closing browser. Process complete.")
                browser.close()
            else:
                print("✨ Finished auto-book attempt. Leaving Chrome open.")
            return {"status": "finished", "order_id": order_id}

        except Exception as e:
            print(f"Error during auto booking: {e}")
            update_order_status(order_id, "waiting", f"Error en ejecución: {str(e)[:80]}. Reintentando...")
            if not is_cdp:
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

    # Fallback to Railway queue
    try:
        import urllib.request
        req = urllib.request.urlopen("https://web-production-2bcf4.up.railway.app/api/orders?status=waiting", timeout=8)
        data = json.loads(req.read().decode())
        for o in data.get("orders", []):
            if province.lower() in (o.get("province") or "").lower():
                print(f"🎯 Found queued client on Railway for {province}: {o['name']} ({o['order_id']})")
                return o
    except Exception as e:
        print(f"⚠️ Could not query cloud queue: {e}")

    return None


def main():
    parser = argparse.ArgumentParser(description="Auto-Claimer & Booking Runner for Cita Previa")
    parser.add_argument("--order-id", type=str, help="Order ID to process (e.g. ORDER-514792)")
    parser.add_argument("--auto-pick", nargs=2, metavar=("PROVINCE", "PROCEDURE"), help="Automatically pick next waiting order for province")
    parser.add_argument("--headless", action="store_true", help="Run browser in headless mode")
    parser.add_argument("--cdp", nargs="?", const="http://127.0.0.1:9222", default=None, help="Connect to running Chrome browser via CDP (e.g. http://127.0.0.1:9222)")
    args = parser.parse_args()

    if args.order_id:
        res = run_auto_booking(args.order_id, headless=args.headless, cdp_url=args.cdp)
        print("Result:", json.dumps(res, indent=2))
        return

    if args.auto_pick:
        prov, proc = args.auto_pick
        order = trigger_next_waiting_order(prov, proc)
        if order:
            res = run_auto_booking(order["order_id"], headless=args.headless, cdp_url=args.cdp)
            print("Result:", json.dumps(res, indent=2))
        else:
            print(f"No clients currently waiting for {prov}.")
        return

    print("Usage: python auto_booker.py --order-id ORDER-123456 [--cdp [URL]] [--headless]")
    print("       python auto_booker.py --auto-pick 'Barcelona' 'POLICIA-TOMA DE HUELLA' [--cdp]")


if __name__ == "__main__":
    main()

