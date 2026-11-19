#!/usr/bin/env python3
"""
Inspect Spanish Government Portal (Cita Previa Extranjería)
Deep diagnostic script to inspect the portal's backend responses,
HTTP headers, cookies (JSESSIONID), form fields, offices, and procedures.
"""

import os
import sys
import re
import time
import json
import argparse
from typing import Dict, Any, List, Optional
import httpx

DEFAULT_BASE_URL = "https://icp.administracionelectronica.gob.es/icpplus"
DEFAULT_PORTAL_URL = f"{DEFAULT_BASE_URL}/index.html"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Cache-Control": "max-age=0",
    "Origin": "https://icp.administracionelectronica.gob.es",
    "Referer": "https://icp.administracionelectronica.gob.es/icpplus/index.html"
}

PROVINCE_CODES = {
    "albacete": "02", "alicante": "03", "almeria": "04", "almería": "04",
    "araba": "01", "alava": "01", "álava": "01", "asturias": "33", "avila": "05", "ávila": "05",
    "badajoz": "06", "barcelona": "08", "bizkaia": "48", "burgos": "09",
    "caceres": "10", "cáceres": "10", "cadiz": "11", "cádiz": "11", "cantabria": "39",
    "castellon": "12", "castellón": "12", "ceuta": "51", "ciudad real": "13",
    "cordoba": "14", "córdoba": "14", "coruña": "15", "a coruña": "15",
    "cuenca": "16", "gipuzkoa": "20", "guipuzcoa": "20", "guipúzcoa": "20",
    "girona": "17", "granada": "18", "guadalajara": "19", "huelva": "21",
    "huesca": "22", "illes balears": "07", "baleares": "07", "jaen": "23", "jaén": "23",
    "la rioja": "26", "las palmas": "35", "leon": "24", "león": "24",
    "lleida": "25", "lugo": "27", "madrid": "28", "malaga": "29", "málaga": "29",
    "melilla": "52", "murcia": "30", "navarra": "31", "ourense": "32",
    "palencia": "34", "pontevedra": "36", "salamanca": "37",
    "santa cruz de tenerife": "38", "tenerife": "38", "segovia": "40",
    "sevilla": "41", "soria": "42", "tarragona": "43", "teruel": "44",
    "toledo": "45", "valencia": "46", "valladolid": "47", "zamora": "49",
    "zaragoza": "50"
}


def resolve_proxy(proxy_arg: Optional[str] = None) -> Optional[str]:
    p = proxy_arg or os.environ.get("PROXY_URL") or os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY")
    if p and p.strip():
        return p.strip()
    # Check if local Tor HTTP tunnel is available
    import socket
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(0.3)
            if s.connect_ex(('127.0.0.1', 9080)) == 0:
                return "http://127.0.0.1:9080"
            if s.connect_ex(('127.0.0.1', 9050)) == 0:
                return "socks5://127.0.0.1:9050"
    except Exception:
        pass
    return None


def inspect_portal(province: str = "Barcelona", procedure_keyword: str = "TOMA DE HUELLA", proxy: Optional[str] = None) -> Dict[str, Any]:
    effective_proxy = resolve_proxy(proxy)
    results: Dict[str, Any] = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
        "target_province": province,
        "proxy_used": effective_proxy or "Direct (No Proxy)",
        "steps": {}
    }

    prov_code = PROVINCE_CODES.get(province.lower().strip(), "08")

    client_kwargs = {
        "headers": HEADERS,
        "timeout": httpx.Timeout(35.0, connect=20.0, read=25.0),
        "follow_redirects": True
    }
    if effective_proxy:
        client_kwargs["proxy"] = effective_proxy

    with httpx.Client(**client_kwargs) as client:
        # Check Public IP as seen by outside
        try:
            ip_resp = client.get("https://api.ipify.org?format=json", timeout=10.0)
            results["outbound_ip"] = ip_resp.json().get("ip")
        except Exception as e:
            results["outbound_ip"] = f"Could not determine ({e})"

        # ==========================================
        # STEP 1: Entry Page & Province Listing
        # ==========================================
        t0 = time.time()
        try:
            r0 = client.get(DEFAULT_PORTAL_URL)
            step1_ms = int((time.time() - t0) * 1000)
            
            # Extract cookies & headers
            cookies = dict(r0.cookies)
            headers = dict(r0.headers)

            # Parse provinces from dropdown
            provinces = []
            for m in re.finditer(r'<option[^>]*value="([^"]+)"[^>]*>([^<]+)</option>', r0.text, re.IGNORECASE):
                val = m.group(1).strip()
                label = m.group(2).strip()
                if val and "/citar" in val:
                    provinces.append({"value": val, "name": label})

            results["steps"]["step1_entry"] = {
                "status_code": r0.status_code,
                "latency_ms": step1_ms,
                "jsessionid": cookies.get("JSESSIONID", "None assigned"),
                "server_header": headers.get("server", "Hidden"),
                "content_length": len(r0.text),
                "provinces_count": len(provinces),
                "provinces_sample": [p["name"] for p in provinces[:5]]
            }
        except Exception as e:
            results["steps"]["step1_entry"] = {"error": str(e), "latency_ms": int((time.time() - t0) * 1000)}
            return results

        # ==========================================
        # STEP 2: Province Selected (Post to /citar)
        # ==========================================
        t1 = time.time()
        step1_url = f"{DEFAULT_BASE_URL}/citar"
        step1_data = {
            "form": f"/icpplus/citar?sede={prov_code}",
            "btnAceptar": "Aceptar"
        }
        try:
            r1 = client.post(step1_url, data=step1_data)
            step2_ms = int((time.time() - t1) * 1000)
            html1 = r1.text

            # Parse Offices dropdown (sIdSede)
            offices = []
            sede_select_match = re.search(r'<select[^>]*name="sIdSede"[^>]*>(.*?)</select>', html1, re.DOTALL | re.IGNORECASE)
            if sede_select_match:
                for opt in re.finditer(r'<option[^>]*value="([^"]*)"[^>]*>([^<]+)</option>', sede_select_match.group(1)):
                    offices.append({"value": opt.group(1).strip(), "name": opt.group(2).strip()})

            # Parse Policía Nacional Procedures (tramiteGrupo[0])
            police_procs = []
            pol_select = re.search(r'<select[^>]*name="tramiteGrupo\[0\]"[^>]*>(.*?)</select>', html1, re.DOTALL | re.IGNORECASE)
            if pol_select:
                for opt in re.finditer(r'<option[^>]*value="([^"]+)"[^>]*>([^<]+)</option>', pol_select.group(1)):
                    police_procs.append({"code": opt.group(1).strip(), "title": opt.group(2).strip()})

            # Parse Extranjería Procedures (tramiteGrupo[1])
            extranjeria_procs = []
            ext_select = re.search(r'<select[^>]*name="tramiteGrupo\[1\]"[^>]*>(.*?)</select>', html1, re.DOTALL | re.IGNORECASE)
            if ext_select:
                for opt in re.finditer(r'<option[^>]*value="([^"]+)"[^>]*>([^<]+)</option>', ext_select.group(1)):
                    extranjeria_procs.append({"code": opt.group(1).strip(), "title": opt.group(2).strip()})

            # Parse hidden inputs
            hidden_inputs = {}
            for h in re.finditer(r'<input[^>]*type="hidden"[^>]*name="([^"]+)"[^>]*value="([^"]*)"', html1, re.IGNORECASE):
                hidden_inputs[h.group(1)] = h.group(2)

            results["steps"]["step2_province_page"] = {
                "status_code": r1.status_code,
                "latency_ms": step2_ms,
                "province": province,
                "province_code": prov_code,
                "offices_count": len(offices),
                "offices": offices,
                "police_procedures_count": len(police_procs),
                "police_procedures": police_procs,
                "extranjeria_procedures_count": len(extranjeria_procs),
                "extranjeria_procedures": extranjeria_procs,
                "hidden_form_inputs": hidden_inputs
            }
        except Exception as e:
            results["steps"]["step2_province_page"] = {"error": str(e), "latency_ms": int((time.time() - t1) * 1000)}
            return results

        # ==========================================
        # STEP 3: Submit Procedure Check
        # ==========================================
        t2 = time.time()
        # Find matching procedure code
        matched_code = None
        matched_name = None
        target_group = "tramiteGrupo[0]"

        all_available = [(p, "tramiteGrupo[0]") for p in police_procs] + [(e, "tramiteGrupo[1]") for e in extranjeria_procs]
        for p_item, grp in all_available:
            if procedure_keyword.upper() in p_item["title"].upper():
                matched_code = p_item["code"]
                matched_name = p_item["title"]
                target_group = grp
                break

        if not matched_code and all_available:
            # Fallback to first available police procedure
            matched_code = all_available[0][0]["code"]
            matched_name = all_available[0][0]["title"]
            target_group = all_available[0][1]

        step3_data = {
            target_group: matched_code or "",
            "btnAceptar": "Aceptar"
        }
        if offices:
            step3_data["sIdSede"] = offices[0]["value"]

        try:
            r2 = client.post(step1_url, data=step3_data)
            step3_ms = int((time.time() - t2) * 1000)
            html2 = r2.text

            has_no_slots = "no hay citas disponibles" in html2.lower() or "en este momento no hay citas" in html2.lower()
            is_rate_limited = "ha superado el número de intentos" in html2.lower() or "acceso restringido" in html2.lower()
            has_entrar_button = bool(re.search(r'btnEntrar|Entrar', html2, re.IGNORECASE))
            has_solicitante_form = bool(re.search(r'txtIdCitado|rdbTipoDoc', html2, re.IGNORECASE))

            results["steps"]["step3_procedure_query"] = {
                "status_code": r2.status_code,
                "latency_ms": step3_ms,
                "tested_procedure_code": matched_code,
                "tested_procedure_name": matched_name,
                "has_no_slots_warning": has_no_slots,
                "is_rate_limited": is_rate_limited,
                "has_entrar_button": has_entrar_button,
                "has_applicant_form": has_solicitante_form,
                "page_title": re.search(r'<title>([^<]+)</title>', html2, re.IGNORECASE).group(1) if re.search(r'<title>([^<]+)</title>', html2, re.IGNORECASE) else "Unknown",
                "response_snippet": html2[:400].strip()
            }
        except Exception as e:
            results["steps"]["step3_procedure_query"] = {"error": str(e), "latency_ms": int((time.time() - t2) * 1000)}

    return results


def main():
    parser = argparse.ArgumentParser(description="Live Spanish Portal Backend Inspector")
    parser.add_argument("--province", type=str, default="Barcelona", help="Province to inspect (default: Barcelona)")
    parser.add_argument("--procedure", type=str, default="TOMA DE HUELLA", help="Procedure keyword (default: TOMA DE HUELLA)")
    parser.add_argument("--proxy", type=str, default=None, help="Proxy URL (e.g. http://127.0.0.1:9080)")
    parser.add_argument("--json", action="store_true", help="Output pure JSON format")
    args = parser.parse_args()

    data = inspect_portal(province=args.province, procedure_keyword=args.procedure, proxy=args.proxy)

    if args.json:
        print(json.dumps(data, indent=2, ensure_ascii=False))
        return

    print("=" * 75)
    print(" 🇪🇸 SPANISH GOVERNMENT PORTAL (CITA PREVIA) BACKEND INSPECTION REPORT")
    print("=" * 75)
    print(f"🕒 Timestamp:     {data['timestamp']}")
    print(f"📍 Province:      {data['target_province']}")
    print(f"🌐 Outbound IP:   {data.get('outbound_ip')}")
    print(f"🛡️ Proxy Used:    {data['proxy_used']}")
    print("-" * 75)

    s1 = data["steps"].get("step1_entry", {})
    print("\n[STEP 1: ENTRY PORTAL /citar]")
    if "error" in s1:
        print(f"  ❌ Error: {s1['error']}")
    else:
        print(f"  Status Code:       HTTP {s1.get('status_code')} (Latency: {s1.get('latency_ms')}ms)")
        print(f"  Server Backend:    {s1.get('server_header')}")
        print(f"  Session Cookie:    {s1.get('jsessionid')}")
        print(f"  Provinces Found:   {s1.get('provinces_count')} provinces loaded")

    s2 = data["steps"].get("step2_province_page", {})
    print(f"\n[STEP 2: PROVINCE DETAILS - {data['target_province'].upper()}]")
    if "error" in s2:
        print(f"  ❌ Error: {s2['error']}")
    else:
        print(f"  Status Code:       HTTP {s2.get('status_code')} (Latency: {s2.get('latency_ms')}ms)")
        print(f"  Offices in City:   {s2.get('offices_count')} comisarías / oficinas found")
        for o in s2.get("offices", [])[:4]:
            print(f"    • [{o['value'] or 'ALL'}] {o['name']}")
        if len(s2.get("offices", [])) > 4:
            print(f"    ... and {len(s2.get('offices', [])) - 4} more offices")

        print(f"\n  Policía Nacional Procedures ({s2.get('police_procedures_count')} available):")
        for p in s2.get("police_procedures", []):
            print(f"    • Code: {p['code']} | {p['title']}")

        print(f"\n  Extranjería Procedures ({s2.get('extranjeria_procedures_count')} available):")
        for e in s2.get("extranjeria_procedures", []):
            print(f"    • Code: {e['code']} | {e['title']}")

    s3 = data["steps"].get("step3_procedure_query", {})
    print(f"\n[STEP 3: PROCEDURE QUERY - {s3.get('tested_procedure_name') or 'N/A'}]")
    if "error" in s3:
        print(f"  ❌ Error: {s3['error']}")
    else:
        print(f"  Status Code:       HTTP {s3.get('status_code')} (Latency: {s3.get('latency_ms')}ms)")
        print(f"  No Slots Warning:  {'⚠️ Sí (No hay citas disponibles)' if s3.get('has_no_slots_warning') else '🟢 NO WARNING'}")
        print(f"  Rate Limited:      {'🚨 SÍ (Bloqueo temporal de IP)' if s3.get('is_rate_limited') else '🟢 NO'}")
        print(f"  Entrar Button:     {'✅ Presente (Listo para entrar)' if s3.get('has_entrar_button') else '❌ No presente'}")
        print(f"  Applicant Form:    {'✅ Formulario de datos desplegado' if s3.get('has_applicant_form') else 'No mostrado aún'}")

    print("\n" + "=" * 75)


if __name__ == "__main__":
    main()
