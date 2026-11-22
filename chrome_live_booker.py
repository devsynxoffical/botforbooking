#!/usr/bin/env python3
"""
Chrome Live Auto-Booker via AppleScript
Directly drives the active Google Chrome tab routing through Urban VPN (Spain).
Zero configuration, zero port issues, bypasses local proxy blocks.
"""

import sys
import time
import json
import subprocess
import urllib.request
from typing import Optional, Dict, Any

RAILWAY_API = "https://web-production-2bcf4.up.railway.app/api/orders"


def run_js(js_code: str) -> Optional[str]:
    """Executes JavaScript inside the active Extranjería tab in Chrome via osascript."""
    # Escape quotes and backslashes for AppleScript
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
        print(f"⚠️ AppleScript error: {e}")
        return None


def navigate_tab(url: str):
    apple_script = f'''
    tell application "Google Chrome"
        set found to false
        repeat with w in windows
            repeat with t in tabs of w
                if URL of t contains "icp.administracionelectronica.gob.es" then
                    set URL of t to "{url}"
                    set found to true
                    return "ok"
                end if
            end repeat
        end repeat
        if not found then
            if (count of windows) > 0 then
                tell front window
                    make new tab with properties {{URL: "{url}"}}
                end tell
            else
                make new window
                set URL of active tab of front window to "{url}"
            end if
            return "opened_new"
        end if
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
            isNoSlots: (document.body && (
                document.body.innerText.toLowerCase().includes("no hay citas") ||
                document.body.innerText.toLowerCase().includes("no appointments") ||
                (location.href.includes("acCitar") && !document.querySelector("input[type=radio]"))
            )) || false,
            hasSlotRadio: !!document.querySelector("input[type=radio][name*=cita], input[type=radio][name*=idCita]"),
            isOTP: !!(document.getElementById("txtCodigoVerificacion") || (document.body && document.body.innerText.includes("SMS")))
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


def load_order(order_id: str = "ORDER-105817") -> Optional[dict]:
    try:
        req = urllib.request.urlopen(f"{RAILWAY_API}/{order_id}", timeout=6)
        data = json.loads(req.read().decode())
        if data and data.get("order_id"):
            return data
    except Exception as e:
        print(f"⚠️ Could not fetch from cloud queue: {e}")
    return None


def execute_step(order: dict):
    st = get_tab_status()
    if st.get("error") == "tab_not_found":
        print("❌ Extranjería tab not found in Chrome! Navigating front tab...")
        navigate_tab("https://icp.administracionelectronica.gob.es/icpplus/index.html")
        return "reloaded"

    if st.get("is429"):
        print("⚠️ [429 RATE LIMIT] Red SARA is rate-limiting this IP.")
        print("👉 In Chrome, click Urban VPN -> Pause (||) -> Play (▶) to get a fresh Spanish IP!")
        return "rate_limited"

    # Step 1: Province Selection
    if st.get("isIndex"):
        prov = order.get("province", "Barcelona")
        print(f"📍 Selecting Province: {prov}...")
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

    # Step 2: Procedure Selection
    if st.get("isProcedures"):
        proc = order.get("procedure", "TOMA DE HUELLA")
        office = order.get("office", "")
        print(f"📋 Selecting Office ({office or 'Any'}) & Procedure ({proc})...")
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
        print("📄 Instructions screen detected. Clicking Entrar...")
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
        print(f"👤 Auto-filling Identity for {order.get('name')} ({order.get('doc_number')})...")
        nie = order.get("doc_number", "")
        name = order.get("name", "")
        dob_year = order.get("birth_date", "1990")[:4]
        country = order.get("birth_country") or order.get("citizenship") or "BANGLADESH"

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
        print("🎯 Action page: Clicking 'Solicitar Cita'...")
        js = """
        (function() {
            var btn = document.getElementById("btnEnviar") || document.querySelector("input[value='Solicitar Cita']");
            if (btn) btn.click();
        })();
        """
        run_js(js)
        return "action_submitted"

    # Step 6: Contact Details (Phone & Email)
    if st.get("isContact"):
        phone = order.get("phone", "")
        email = order.get("email", "")
        print(f"📞 Contact details page: Entering {phone} / {email}...")
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
        print("🚨🚨 FREE APPOINTMENT SLOT DETECTED IN BROWSER! 🚨🚨")
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
        # Ring alert sound
        subprocess.run(["afplay", "/System/Library/Sounds/Glass.aiff"], check=False)
        return "slot_claimed"

    if st.get("isNoSlots"):
        print("ℹ️ No appointment slots currently open. Clicking Aceptar to cycle...")
        js = """
        (function() {
            var btn = document.getElementById("btnSubmit") || document.querySelector("input[value=Aceptar]");
            if (btn) btn.click();
        })();
        """
        run_js(js)
        return "no_slots"

    if st.get("isOTP"):
        print("🚨 SMS VERIFICATION CODE PROMPTED ON SCREEN! Check client's phone!")
        subprocess.run(["afplay", "/System/Library/Sounds/Ping.aiff"], check=False)
        return "otp_pending"

    return "idle"


def main():
    order_id = "ORDER-105817"
    is_loop = False
    for arg in sys.argv[1:]:
        if arg == "--loop":
            is_loop = True
        elif arg.startswith("ORDER-"):
            order_id = arg

    print(f"🚀 Loading order {order_id} from Railway...")
    order = load_order(order_id)
    if not order:
        print("❌ Could not load order details.")
        return

    print("=" * 60)
    print(f"  🇪🇸 Chrome Auto-Booker Active for {order['name']}")
    print(f"  Doc: {order['doc_number']} | Province: {order['province']}")
    print(f"  Trámite: {order['procedure']}")
    print("=" * 60)

    if not is_loop:
        res = execute_step(order)
        print(f"Result: {res}")
        return

    print("🔄 Running continuous step execution loop...")
    attempt_count = 0
    while True:
        res = execute_step(order)
        print(f"[{time.strftime('%H:%M:%S')}] Step Result: {res}")
        if res in ["slot_claimed", "otp_pending"]:
            print("🎉 Success! Reached final booking / OTP stage.")
            break
        elif res == "no_slots":
            attempt_count += 1
            print(f"⏳ Attempt #{attempt_count} complete (no slots). Cooldown 15s to keep Spanish IP safe...")
            for s in range(15, 0, -1):
                sys.stdout.write(f"\rNext check in {s}s... ")
                sys.stdout.flush()
                time.sleep(1)
            print("")
        elif res == "rate_limited":
            print("⚠️ Rate limit detected. Waiting 30s. If this persists, toggle Urban VPN.")
            time.sleep(30)
        else:
            time.sleep(2.5)


if __name__ == "__main__":
    main()
