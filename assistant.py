#!/usr/bin/env python3
"""
Cita Previa Extranjería Assistant
Assists with booking appointments on the Spanish immigration portal
by automating navigation and repetitive form-filling while letting the user
solve CAPTCHA and SMS verification.
"""

import json
import os
import sys
import time
import subprocess
from pathlib import Path
from playwright.sync_api import sync_playwright

CONFIG_PATH = Path(__file__).parent / "config.json"

def load_config():
    if not CONFIG_PATH.exists():
        print(f"Error: Configuration file not found at {CONFIG_PATH}")
        sys.exit(1)
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def play_alert():
    """Play alert sound on macOS or bell character on other OS."""
    try:
        if sys.platform == "darwin":
            sound_file = "/System/Library/Sounds/Glass.aiff"
            if os.path.exists(sound_file):
                for _ in range(3):
                    subprocess.run(["afplay", sound_file], check=False)
                    time.sleep(0.2)
                return
    except Exception:
        pass
    print("\a\a\a")  # Terminal bell fallback

def main():
    config = load_config()
    print("=" * 60)
    print("  Cita Previa Extranjería - Assisted Booking System")
    print(f"  Applicant: {config.get('full_name')} ({config.get('document_number')})")
    print(f"  Province: {config.get('province')}")
    print(f"  Trámite: {config.get('procedure_keyword')}")
    print("=" * 60)

    with sync_playwright() as p:
        # Launch headed browser so user can see and interact with CAPTCHA/SMS
        browser = p.chromium.launch(
            headless=False,
            args=["--start-maximized", "--disable-blink-features=AutomationControlled"]
        )
        context = browser.new_context(
            no_viewport=True,
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
        )
        page = context.new_page()

        print("\n[1/6] Navigating to appointment portal...")
        page.goto(config["portal_url"], wait_until="domcontentloaded")
        page.wait_for_timeout(1500)

        # Step 1: Select Province
        print(f"[2/6] Selecting Province: {config['province']}...")
        province_selector = page.locator('select#form, select[name="sede"]').first
        if province_selector.is_visible():
            options = province_selector.locator("option").all()
            selected = False
            for opt in options:
                text = opt.inner_text()
                if config["province"].lower() in text.lower():
                    province_selector.select_option(value=opt.get_attribute("value"))
                    selected = True
                    print(f"      Matched province option: {text.strip()}")
                    break
            
            if selected:
                accept_btn = page.locator('input#btnAceptar, input[type="submit"][value="Aceptar"]').first
                if accept_btn.is_visible():
                    accept_btn.click()
                    page.wait_for_load_state("domcontentloaded")
                    page.wait_for_timeout(1500)

        # Step 2: Select Procedure (Trámite)
        print(f"[3/6] Looking for procedure matching '{config['procedure_keyword']}'...")
        procedure_selectors = ['select#tramiteGrupo\\[0\\]', 'select[name="tramite"]', 'select#tramite']
        tramite_dropdown = None
        for sel in procedure_selectors:
            loc = page.locator(sel).first
            if loc.is_visible():
                tramite_dropdown = loc
                break

        if tramite_dropdown:
            options = tramite_dropdown.locator("option").all()
            selected_tramite = False
            for opt in options:
                text = opt.inner_text().upper()
                if config["procedure_keyword"].upper() in text:
                    tramite_dropdown.select_option(value=opt.get_attribute("value"))
                    selected_tramite = True
                    print(f"      Matched procedure option: {text.strip()}")
                    break

            if selected_tramite:
                accept_btn = page.locator('input#btnAceptar, input[type="submit"][value="Aceptar"]').first
                if accept_btn.is_visible():
                    accept_btn.click()
                    page.wait_for_load_state("domcontentloaded")
                    page.wait_for_timeout(1500)

        # Step 3: Information notice page (if shown)
        entrar_btn = page.locator('input#btnEntrar, input[value="Entrar"]').first
        if entrar_btn.is_visible():
            print("      Acknowledging information instructions...")
            entrar_btn.click()
            page.wait_for_load_state("domcontentloaded")
            page.wait_for_timeout(1500)

        # Step 4: Fill Personal Details
        print("[4/6] Filling applicant personal details...")
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

        if id_input.is_visible():
            id_input.fill(config["document_number"])
        if name_input.is_visible():
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

        print("\n" + "#" * 60)
        print("👉 ATTENTION: Form filled. Check the browser window.")
        print("   If a CAPTCHA appears, please solve it in the browser.")
        print("   Press [ENTER] in this terminal once ready to continue...")
        print("#" * 60)
        input("Press Enter to continue >> ")

        submit_btn = page.locator('input#btnEnviar, input[type="submit"][value="Aceptar"]').first
        if submit_btn.is_visible():
            submit_btn.click()
            page.wait_for_load_state("domcontentloaded")
            page.wait_for_timeout(2000)

        # Step 5: Action Menu (Solicitar Cita)
        solicitar_btn = page.locator('input#btnEnviar[value="Solicitar Cita"], input[value="Solicitar Cita"]').first
        if solicitar_btn.is_visible():
            print("[5/6] Clicking 'Solicitar Cita'...")
            solicitar_btn.click()
            page.wait_for_load_state("domcontentloaded")
            page.wait_for_timeout(2000)

        # Step 6: Contact Information (Phone / Email)
        phone_input = page.locator('input#txtTelefonoCitado').first
        email_input = page.locator('input#emailCMP').first
        email2_input = page.locator('input#emailCMP2').first

        if phone_input.is_visible() or email_input.is_visible():
            print("[6/6] Entering contact phone and email...")
            if phone_input.is_visible():
                phone_input.fill(config["phone"])
            if email_input.is_visible():
                email_input.fill(config["email"])
            if email2_input.is_visible():
                email2_input.fill(config["email"])

            next_btn = page.locator('input#btnSiguiente, input[value="Siguiente"]').first
            if next_btn.is_visible():
                next_btn.click()
                page.wait_for_load_state("domcontentloaded")
                page.wait_for_timeout(2000)

        # Final Check: Availability
        body_text = page.locator("body").inner_text()
        if "no hay citas disponibles" in body_text.lower():
            print("\n❌ Result: 'En este momento no hay citas disponibles'.")
            print("   The system currently has no open appointment slots for this procedure.")
        else:
            print("\n" + "🎉" * 20)
            print("🚨 APPOINTMENT SLOTS OR NEXT STEP DETECTED! 🚨")
            print("   Please check the open browser window immediately to select your date/time")
            print("   and confirm with the SMS OTP code sent to your phone.")
            print("🎉" * 20)
            play_alert()

        print("\nKeeping the browser open. Press Enter when you are done to close it...")
        input("Press Enter to exit >> ")
        browser.close()

if __name__ == "__main__":
    main()

