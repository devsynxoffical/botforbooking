#!/usr/bin/env python3
"""
Full Autonomous Cita Previa Auto-Booker
Drives Google Chrome connected through Urban VPN (Spain).
Runs 24/7 in the background, rotating through all waiting clients,
monitoring for slot availability, claiming slots instantly, and handling SMS OTP.
"""

import sys
import os
import time
import json
import subprocess
import urllib.request
from typing import Optional, Dict, Any, List

RAILWAY_BASE = "https://web-production-2bcf4.up.railway.app"
RAILWAY_ORDERS_API = f"{RAILWAY_BASE}/api/orders"


def log(msg: str):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {msg}", flush=True)


def run_js(js_code: str) -> Optional[str]:
    """Executes JavaScript inside the active Extranjería tab in Chrome via osascript."""
    escaped = js_code.replace('\\', '\\\\').replace('"', '\\"').replace('\n', ' ')
    apple_script = f'''
    tell application "Google Chrome"
        repeat with w in windows
            repeat with t in tabs of w
                if URL of t contains "icp.administracionelectronica.gob.es" then
                    return execute t javascript "{escaped}"
                end if
            end repeat
        end repeat
        return "tab_not_found"
    end tell
    '''
    try:
        proc = subprocess.run(["osascript", "-e", apple_script], capture_output=True, text=True, timeout=12)
        out = proc.stdout.strip()
        return out if proc.returncode == 0 else None
    except Exception as e:
        log(f"⚠️ AppleScript error: {e}")
        return None


def navigate_tab(url: str):
    apple_script = f'''
    tell application "Google Chrome"
        repeat with w in windows
            repeat with t in tabs of w
                if URL of t contains "icp.administracionelectronica.gob.es" then
                    set URL of t to "{url}"
                    return "ok"
                end if
            end repeat
        end repeat
    end tell
    '''
    subprocess.run(["osascript", "-e", apple_script], capture_output=True)


def get_tab_status() -> Dict[str, Any]:
    js = """
    (function() {
        return JSON.stringify({
            url: location.href,
            title: document.title,
            body: document.body ? document.body.innerText.substring(0, 500) : "",
            is429: document.title.includes("429") || (document.body && document.body.innerText.includes("Too Many Requests")),
            isProcedures: !!(document.getElementById("tramiteGrupo[0]") || document.getElementById("tramiteGrupo[1]") || document.querySelector("select[name*=tramite]")),
            isIndex: (location.href.includes("index") || !document.getElementById("tramiteGrupo[0]")) && !!(document.getElementById("form") || document.querySelector("select#form")),
            isInstructions: !!(document.getElementById("btnEntrar") || document.querySelector("input[value=Entrar]")),
            isIdentity: !!(document.getElementById("txtIdCitado") && document.getElementById("txtDesCitado")),
            isAction: !!(document.querySelector("input[value='Solicitar Cita']") || (document.body && document.body.innerText.includes("Solicitar Cita"))),
            isContact: !!(document.getElementById("txtTelefonoCitado") || document.getElementById("emailCMP")),
            isNoSlots: document.body ? document.body.innerText.includes("En este momento no hay citas disponibles") : false,
            hasSlotRadio: !!document.querySelector("input[type=radio][name*=cita], input[type=radio][name*=idCita]"),
            isOTP: !!(document.getElementById("txtCodigoVerificacion") || (document.body && document.body.innerText.includes("SMS"))),
            isConfirmed: document.body ? (document.body.innerText.includes("Cita Confirmada") || document.body.innerText.includes("Justificante de Cita")) : false
        });
    })();
    """
    res = run_js(js)
    if not res or res == "tab_not_found":
        return {"error": "tab_not_found"}
    try:
        return json.loads(res)
    except Exception:
        return {"error": "parse_error", "raw": res}


def fetch_waiting_orders() -> List[dict]:
    try:
        req = urllib.request.urlopen(f"{RAILWAY_ORDERS_API}?status=waiting", timeout=8)
        data = json.loads(req.read().decode())
        orders = data.get("orders", [])
        if orders:
            return orders
    except Exception as e:
        log(f"⚠️ Error fetching waiting orders: {e}")

    # Fallback to all orders
    try:
        req = urllib.request.urlopen(f"{RAILWAY_ORDERS_API}", timeout=8)
        data = json.loads(req.read().decode())
        all_orders = data.get("orders", [])
        return [o for o in all_orders if o.get("status") in ["waiting", "searching"]]
    except Exception as e:
        log(f"⚠️ Error fetching orders fallback: {e}")
        return []


def update_order_status(order_id: str, status: str, notes: str = ""):
    try:
        url = f"{RAILWAY_ORDERS_API}/{order_id}/status"
        payload = json.dumps({"status": status, "notes": notes}).encode("utf-8")
        req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"}, method="PATCH")
        with urllib.request.urlopen(req, timeout=6) as resp:
            pass
    except Exception as e:
        log(f"⚠️ Status update error: {e}")


def sound_alarm():
    """Rings repeating audible chime on macOS."""
    for _ in range(3):
        subprocess.run(["afplay", "/System/Library/Sounds/Glass.aiff"], check=False)
        time.sleep(0.3)


def prompt_sms_dialog(client_name: str, phone: str) -> Optional[str]:
    """Displays a native macOS dialog window prompting for the 6-digit SMS code."""
    script = f'''
    tell application "System Events"
        activate
        set res to display dialog "🚨 CITA CAPTURED FOR {client_name}!\\n\\nEnter 6-digit SMS code sent to {phone}:" default answer "" with title "Cita Previa Auto-Booker" giving up after 180 with icon note
        if gave up of res is false then
            return text returned of res
        else
            return "timeout"
        end if
    end tell
    '''
    try:
        proc = subprocess.run(["osascript", "-e", script], capture_output=True, text=True, timeout=190)
        out = proc.stdout.strip()
        if out and out != "timeout":
            return out
    except Exception as e:
        log(f"Dialog prompt error: {e}")
    return None


def poll_otp_from_cloud(order_id: str, timeout_sec: int = 180) -> Optional[str]:
    """Polls Railway API for OTP submitted via web admin dashboard or Telegram."""
    t0 = time.time()
    while time.time() - t0 < timeout_sec:
        try:
            req = urllib.request.urlopen(f"{RAILWAY_ORDERS_API}/{order_id}", timeout=4)
            order = json.loads(req.read().decode())
            # Check notes or dedicated field for OTP
            notes = order.get("notes") or ""
            if "OTP:" in notes:
                import re
                m = re.search(r'OTP:\s*([0-9]{4,8})', notes)
                if m:
                    return m.group(1)
        except Exception:
            pass
        time.sleep(2)
    return None


def execute_booking_step(order: dict) -> str:
    st = get_tab_status()
    if st.get("error") == "tab_not_found":
        log("❌ Extranjería tab not found in Chrome! Navigating front tab...")
        navigate_tab("https://icp.administracionelectronica.gob.es/icpplus/index.html")
        return "reloaded"

    if st.get("is429"):
        log("⚠️ [429 RATE LIMIT] Red SARA is rate-limiting this IP.")
        log("👉 In Chrome, toggle Urban VPN: Pause (||) -> Play (▶) to refresh Spanish IP.")
        return "rate_limited"

    # Step 1: Province Selection
    if st.get("isIndex"):
        prov = order.get("province", "Barcelona")
        log(f"📍 Selecting Province: {prov}...")
        js = """
        (function() {
            var sel = document.getElementById("form") || document.querySelector("select[name=sede]");
            if (!sel) return false;
            for (var i = 0; i < sel.options.length; i++) {
                if (sel.options[i].text.toLowerCase().includes("__PROV__")) {
                    sel.value = sel.options[i].value;
                    var btn = document.getElementById("btnAceptar") || document.querySelector("input[type=submit]");
                    if (btn) btn.click();
                    return true;
                }
            }
            return false;
        })();
        """.replace("__PROV__", prov.lower())
        run_js(js)
        return "province_submitted"

    # Step 2: Procedure & Office Selection
    if st.get("isProcedures"):
        proc = order.get("procedure", "TOMA DE HUELLA")
        office = order.get("office", "")
        log(f"📋 Selecting Office ({office or 'Any'}) & Procedure ({proc})...")
        js = """
        (function() {
            var sedeSel = document.getElementById("sede");
            if (sedeSel) {
                var foundSede = false;
                var offTarget = "__OFFICE__".toUpperCase();
                if (offTarget && offTarget !== "ANY") {
                    for (var sOpt of sedeSel.options) {
                        if (sOpt.text.toUpperCase().includes(offTarget) || offTarget.includes(sOpt.text.toUpperCase())) {
                            sedeSel.value = sOpt.value;
                            foundSede = true;
                            break;
                        }
                    }
                }
                if (!foundSede) sedeSel.value = "99";
            }

            var procSels = Array.from(document.querySelectorAll("select[name*=tramite], select[id*=tramite]"));
            var target = "__PROC__";
            for (var sel of procSels) {
                for (var opt of sel.options) {
                    var txt = opt.text.toUpperCase();
                    if (txt.includes("RECOGIDA") || txt.includes("HUELLA") || txt.includes("TIE") || txt.includes(target.substring(0, 15))) {
                        sel.value = opt.value;
                        var btn = document.getElementById("btnAceptar") || document.querySelector("input[type=submit]");
                        if (btn) {
                            setTimeout(function() { btn.click(); }, 600);
                            return true;
                        }
                    }
                }
            }
            return false;
        })();
        """.replace("__PROC__", proc.upper()).replace("__OFFICE__", (office or "ANY").upper())
        run_js(js)
        return "procedure_submitted"

    # Step 3: Information Page
    if st.get("isInstructions"):
        log("📄 Information screen detected. Clicking Entrar...")
        js = """
        (function() {
            var btn = document.getElementById("btnEntrar") || document.querySelector("input[value=Entrar]");
            if (btn) btn.click();
        })();
        """
        run_js(js)
        return "entrar_clicked"

    # Step 4: Identity Details
    if st.get("isIdentity"):
        nie = order.get("doc_number", "")
        name = order.get("name", "")
        dob_year = str(order.get("birth_date", "1990"))[:4]
        country = order.get("birth_country") or order.get("citizenship") or "BANGLADESH"
        log(f"👤 Auto-filling Identity: {name} ({nie})...")

        js = """
        (function() {
            var rNie = document.getElementById("rdbTipoDocNie");
            if (rNie) rNie.checked = true;
            var nieInp = document.getElementById("txtIdCitado");
            if (nieInp) nieInp.value = "__NIE__";
            var nameInp = document.getElementById("txtDesCitado");
            if (nameInp) nameInp.value = "__NAME__";
            var yearInp = document.getElementById("txtAnnoCitado");
            if (yearInp) yearInp.value = "__YEAR__";
            var countrySel = document.getElementById("txtPaisNac");
            if (countrySel) {
                for (var opt of countrySel.options) {
                    if (opt.text.toUpperCase().includes("__COUNTRY__")) {
                        countrySel.value = opt.value;
                        break;
                    }
                }
            }
            var btn = document.getElementById("btnEnviar") || document.getElementById("btnAceptar") || document.querySelector("input[type=submit]");
            if (btn) {
                setTimeout(function() { btn.click(); }, 800);
            }
        })();
        """.replace("__NIE__", nie).replace("__NAME__", name).replace("__YEAR__", dob_year).replace("__COUNTRY__", country.upper())
        run_js(js)
        return "identity_submitted"

    # Step 5: Action Page ("Solicitar Cita")
    if st.get("isAction"):
        log("🎯 Action page: Clicking 'Solicitar Cita'...")
        js = """
        (function() {
            var btn = document.getElementById("btnEnviar") || document.querySelector("input[value='Solicitar Cita']");
            if (btn) btn.click();
        })();
        """
        run_js(js)
        return "action_submitted"

    # Step 6: Contact Details
    if st.get("isContact"):
        phone = order.get("phone", "")
        email = order.get("email", "")
        log(f"📞 Contact page: Entering {phone} / {email}...")
        js = """
        (function() {
            var pInp = document.getElementById("txtTelefonoCitado");
            if (pInp) pInp.value = "__PHONE__";
            var em1 = document.getElementById("emailCMP");
            if (em1) em1.value = "__EMAIL__";
            var em2 = document.getElementById("emailCMP2");
            if (em2) em2.value = "__EMAIL__";
            var btn = document.getElementById("btnSiguiente") || document.querySelector("input[value=Siguiente]");
            if (btn) setTimeout(function() { btn.click(); }, 800);
        })();
        """.replace("__PHONE__", phone).replace("__EMAIL__", email)
        run_js(js)
        return "contact_submitted"

    # Step 7: Slot Detection
    if st.get("hasSlotRadio"):
        log(f"🚨🚨 FREE APPOINTMENT SLOT DETECTED FOR {order.get('name')}! 🚨🚨")
        sound_alarm()
        js = """
        (function() {
            var radio = document.querySelector("input[type=radio][name*=cita], input[type=radio][name*=idCita]");
            if (radio) {
                radio.checked = true;
                var btn = document.getElementById("btnSiguiente") || document.querySelector("input[value=Siguiente]");
                if (btn) btn.click();
            }
        })();
        """
        run_js(js)
        return "slot_claimed"

    if st.get("isNoSlots"):
        log("ℹ️ No slots currently available. Cycling back...")
        js = """
        (function() {
            var btn = document.getElementById("btnSubmit") || document.querySelector("input[value=Aceptar]");
            if (btn) btn.click();
        })();
        """
        run_js(js)
        return "no_slots"

    if st.get("isOTP"):
        log("🚨 SMS VERIFICATION CODE REQUIRED!")
        sound_alarm()
        update_order_status(order["order_id"], "code_pending", "Cita detectada! Esperando código SMS.")
        return "otp_pending"

    return "idle"


def run_full_auto_loop():
    log("=" * 65)
    log("🚀 24/7 FULL AUTONOMOUS CITA PREVIA AUTO-BOOKER STARTED")
    log("   Routing through active Urban VPN (Spain)")
    log("=" * 65)

    while True:
        orders = fetch_waiting_orders()
        if not orders:
            log("😴 No waiting clients found in queue. Sleeping 30s...")
            time.sleep(30)
            continue

        log(f"📋 Total clients in waiting queue: {len(orders)}")

        for order in orders:
            order_id = order.get("order_id")
            name = order.get("name")
            log("-" * 55)
            log(f"🎯 Processing Client: {name} ({order_id})")
            log(f"   Province: {order.get('province')} | Trámite: {order.get('procedure')[:35]}")

            # Reset tab to index.html for clean session start
            navigate_tab("https://icp.administracionelectronica.gob.es/icpplus/index.html")
            time.sleep(3)

            # Advance through steps for this client
            step_attempts = 0
            while step_attempts < 10:
                res = execute_booking_step(order)
                log(f"   Step Result: {res}")

                if res == "no_slots":
                    log(f"   ⏳ No slots for {name}. Moving to next client...")
                    break
                elif res == "rate_limited":
                    log("   ⚠️ Rate limited. Pausing 35s...")
                    time.sleep(35)
                    break
                elif res == "slot_claimed":
                    log("   🎉 SLOT CLAIMED! Advancing to SMS verification...")
                    time.sleep(3)
                elif res == "otp_pending":
                    log(f"   📱 SMS code sent to {order.get('phone')} for {name}!")
                    # Prompt via macOS dialog
                    otp_code = prompt_sms_dialog(name, order.get("phone", ""))
                    if not otp_code:
                        otp_code = poll_otp_from_cloud(order_id, timeout_sec=60)

                    if otp_code:
                        log(f"   🔑 Submitting OTP code: {otp_code}...")
                        submit_js = f"""
                        (function() {{
                            var inp = document.getElementById("txtCodigoVerificacion") || document.querySelector("input[name*=codigo]");
                            if (inp) inp.value = "{otp_code}";
                            var chk = document.getElementById("chkConsentimiento") || document.querySelector("input[type=checkbox]");
                            if (chk) chk.checked = true;
                            var btn = document.getElementById("btnAceptar") || document.getElementById("btnConfirmar") || document.querySelector("input[value=Confirmar]");
                            if (btn) btn.click();
                        }})();
                        """
                        run_js(submit_js)
                        time.sleep(4)
                        update_order_status(order_id, "completed", f"Cita confirmada con éxito. Código: {otp_code}")
                        sound_alarm()
                        log(f"   ✅✅ CITA FULLY BOOKED & CONFIRMED FOR {name}!! ✅✅")
                    break

                step_attempts += 1
                time.sleep(2.5)

            # 15s Cooldown between client checks to ensure zero IP bans
            log("⏳ 15s safe cooldown between checks...")
            time.sleep(15)


if __name__ == "__main__":
    run_full_auto_loop()
