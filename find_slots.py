#!/usr/bin/env python3
"""
Find Available Bookings (Cita Previa Slot Finder)
Dedicated tool to check if appointment slots are currently OPEN on the
Spanish Government portal (icp.administracionelectronica.gob.es).
"""

import sys
import os
import re
import time
import json
import argparse
from typing import Optional, Dict, Any, List
import httpx

DEFAULT_BASE_URL = "https://icp.administracionelectronica.gob.es/icpplus"

PROVINCE_CODES = {
    "barcelona": "08", "madrid": "28", "valencia": "46", "alicante": "03",
    "sevilla": "41", "malaga": "29", "málaga": "29", "murcia": "30",
    "baleares": "07", "las palmas": "35", "tenerife": "38", "zaragoza": "50",
    "bizkaia": "48", "tarragona": "43", "girona": "17", "granada": "18",
    "coruña": "15", "toledo": "45", "almeria": "04", "cadiz": "11"
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Origin": "https://icp.administracionelectronica.gob.es",
    "Referer": "https://icp.administracionelectronica.gob.es/icpplus/index.html"
}


def check_slot_availability(province: str = "Barcelona", procedure: str = "TOMA DE HUELLA", proxy: Optional[str] = None) -> Dict[str, Any]:
    prov_key = province.lower().strip()
    prov_code = PROVINCE_CODES.get(prov_key, "08")
    
    result = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "province": province,
        "procedure": procedure,
        "slots_available": False,
        "status": "unknown",
        "message": "",
        "latency_ms": 0,
        "proxy": proxy or os.environ.get("PROXY_URL") or "Direct Connection"
    }

    client_kwargs = {
        "headers": HEADERS,
        "timeout": httpx.Timeout(20.0, connect=12.0),
        "follow_redirects": True
    }
    if proxy:
        client_kwargs["proxy"] = proxy
    elif os.environ.get("PROXY_URL"):
        client_kwargs["proxy"] = os.environ.get("PROXY_URL")

    t0 = time.time()
    try:
        with httpx.Client(**client_kwargs) as client:
            # Step 1: Open session with province code
            step1_url = f"{DEFAULT_BASE_URL}/citar"
            step1_data = {
                "form": f"/icpplus/citar?sede={prov_code}",
                "btnAceptar": "Aceptar"
            }
            r1 = client.post(step1_url, data=step1_data)
            if r1.status_code != 200:
                result["status"] = "error"
                result["message"] = f"Failed to access province portal (HTTP {r1.status_code})"
                result["latency_ms"] = int((time.time() - t0) * 1000)
                return result

            html1 = r1.text

            # Match procedure code
            match = re.search(r'<option[^>]*value="([^"]+)"[^>]*>([^<]*' + re.escape(procedure) + r'[^<]*)</option>', html1, re.IGNORECASE)
            if not match:
                # Fallback to key terms
                for term in ["TOMA DE HUELLA", "HUELLA", "TIE", "RECOGIDA"]:
                    match = re.search(r'<option[^>]*value="([^"]+)"[^>]*>([^<]*' + re.escape(term) + r'[^<]*)</option>', html1, re.IGNORECASE)
                    if match:
                        break

            if not match:
                result["status"] = "error"
                result["message"] = f"Procedure '{procedure}' not found for {province}."
                result["latency_ms"] = int((time.time() - t0) * 1000)
                return result

            proc_code = match.group(1)
            proc_title = match.group(2).strip()

            # Identify select name: tramiteGrupo[0] (police) or tramiteGrupo[1] (extranjería)
            select_name = "tramiteGrupo[0]"
            before = html1[:match.start()]
            found_sels = re.findall(r'<select[^>]*name="([^"]+)"', before, re.IGNORECASE)
            if found_sels:
                select_name = found_sels[-1]

            step2_data = {
                select_name: proc_code,
                "btnAceptar": "Aceptar"
            }
            # Include default office if required
            sede_m = re.search(r'<select[^>]*name="sIdSede"[^>]*>.*?<option[^>]*value="([^"]+)"', html1, re.DOTALL | re.IGNORECASE)
            if sede_m:
                step2_data["sIdSede"] = sede_m.group(1)

            # Step 2: Query availability
            r2 = client.post(step1_url, data=step2_data)
            html2 = r2.text.lower()
            result["latency_ms"] = int((time.time() - t0) * 1000)

            # Verdict
            if "no hay citas disponibles" in html2 or "en este momento no hay citas" in html2:
                result["slots_available"] = False
                result["status"] = "no_slots"
                result["message"] = "En este momento no hay citas disponibles para este trámite."
            elif "ha superado el número de intentos" in html2 or "acceso restringido" in html2:
                result["slots_available"] = False
                result["status"] = "rate_limited"
                result["message"] = "Acceso restringido temporalmente por el cortafuegos estatal."
            elif "entrar" in html2 or "txtidcitado" in html2 or "cita" in html2:
                result["slots_available"] = True
                result["status"] = "available"
                result["message"] = f"🔥 ¡CITAS DISPONIBLES EN {province.upper()}! El portal ha habilitado el botón 'Entrar'."
            else:
                result["status"] = "unknown"
                result["message"] = f"Respuesta inesperada del portal (HTTP {r2.status_code})"

    except Exception as e:
        result["status"] = "connection_failed"
        result["message"] = f"Connection dropped by Red SARA: {e}"
        result["latency_ms"] = int((time.time() - t0) * 1000)

    return result


def main():
    parser = argparse.ArgumentParser(description="Find Available Cita Previa Bookings")
    parser.add_argument("--province", type=str, default="Barcelona", help="Province to check (default: Barcelona)")
    parser.add_argument("--procedure", type=str, default="TOMA DE HUELLA", help="Procedure keyword (default: TOMA DE HUELLA)")
    parser.add_argument("--proxy", type=str, default=None, help="Proxy URL (optional)")
    parser.add_argument("--loop", action="store_true", help="Continuously scan until slots open")
    parser.add_argument("--interval", type=int, default=30, help="Interval in seconds between scans (default: 30)")
    args = parser.parse_args()

    print("=" * 65)
    print(" 🇪🇸 CITA PREVIA - SLOT AVAILABILITY FINDER")
    print(f" 📍 Target:    {args.province.upper()} — {args.procedure}")
    print(f" 🌐 Proxy:     {args.proxy or 'System / Direct'}")
    print("=" * 65)

    while True:
        res = check_slot_availability(args.province, args.procedure, args.proxy)
        status_icon = "🔥" if res["slots_available"] else ("⚠️" if res["status"] == "no_slots" else "❌")
        print(f"[{res['timestamp']}] {status_icon} Status: {res['status'].upper()} ({res['latency_ms']}ms) ➔ {res['message']}")

        if res["slots_available"]:
            print("\n🎉 SLOTS DETECTED! READY TO PROCEED WITH AUTO-BOOKING.")
            if not args.loop:
                break

        if not args.loop:
            break

        time.sleep(args.interval)


if __name__ == "__main__":
    main()
