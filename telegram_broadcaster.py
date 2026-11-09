"""
Telegram Broadcaster for Cita Previa Extranjería
Sends formatted broadcast alerts to Telegram Channels, Groups, or individual Chat IDs
when appointment slots become available.
"""

import datetime
from typing import List, Union, Optional
import httpx


def format_alert_message(
    province: str,
    procedure: str,
    office: Optional[str] = None,
    portal_url: str = "https://icp.administracionelectronica.gob.es/icpplus/index.html",
    custom_note: Optional[str] = None
) -> str:
    """Format an urgent appointment slot alert in HTML."""
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = "🚨🚨 <b>¡CITAS DISPONIBLES DETECTADAS!</b> 🚨🚨\n\n"
    msg += f"📍 <b>Provincia:</b> {province.upper()}\n"
    msg += f"📋 <b>Trámite:</b> {procedure}\n"
    if office:
        msg += f"🏢 <b>Oficina:</b> {office}\n"
    msg += f"⏰ <b>Detectado:</b> <code>{now_str}</code>\n\n"
    if custom_note:
        msg += f"ℹ️ <i>{custom_note}</i>\n\n"
    msg += "⚡ <b>¡Entra de inmediato antes de que se agoten los turnos!</b>\n\n"
    msg += f"🔗 <a href=\"{portal_url}\"><b>Abrir Portal Oficial de Citas</b></a>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += "🔔 <i>Alerta automática en tiempo real</i>"
    return msg


def broadcast_telegram_message(
    bot_token: str,
    chat_ids: Union[str, List[str]],
    text: str,
    disable_web_page_preview: bool = True
) -> dict:
    """
    Broadcasts message to one or multiple Telegram chat_ids / channels.
    `chat_ids` can be a single string (e.g. '@my_channel' or '-100123456789, 987654')
    or a list of strings.
    """
    if not bot_token or not bot_token.strip():
        return {"status": "error", "message": "Bot token is missing"}

    # Normalize targets
    targets = []
    if isinstance(chat_ids, str):
        # Allow comma/space-separated string
        for item in chat_ids.replace(";", ",").split(","):
            cleaned = item.strip()
            if cleaned:
                targets.append(cleaned)
    elif isinstance(chat_ids, list):
        targets = [str(t).strip() for t in chat_ids if str(t).strip()]

    if not targets:
        return {"status": "error", "message": "No valid chat_id or channel specified"}

    url = f"https://api.telegram.org/bot{bot_token.strip()}/sendMessage"
    results = {"success": [], "failed": []}

    with httpx.Client(timeout=10.0) as client:
        for target in targets:
            try:
                payload = {
                    "chat_id": target,
                    "text": text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": disable_web_page_preview
                }
                resp = client.post(url, json=payload)
                resp_json = resp.json()
                if resp.status_code == 200 and resp_json.get("ok"):
                    results["success"].append(target)
                else:
                    err_desc = resp_json.get("description", f"HTTP {resp.status_code}")
                    results["failed"].append({"target": target, "error": err_desc})
            except Exception as e:
                results["failed"].append({"target": target, "error": str(e)})

    total_sent = len(results["success"])
    total_failed = len(results["failed"])
    status = "success" if total_sent > 0 else "failed"

    return {
        "status": status,
        "sent_count": total_sent,
        "failed_count": total_failed,
        "results": results
    }


def send_test_broadcast(bot_token: str, chat_ids: Union[str, List[str]]) -> dict:
    """Send a diagnostic ping to test Telegram bot credentials and channel permissions."""
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    test_message = (
        "🤖 <b>Cita Previa Extranjería - Monitor Broadcast Test</b>\n\n"
        "✅ <b>¡Conexión Exitosa!</b>\n"
        "El bot está correctamente configurado con permisos para transmitir en este canal/chat.\n\n"
        f"⏰ <b>Hora:</b> <code>{now_str}</code>\n"
        "📡 <i>Las alertas de citas abiertas llegarán aquí inmediatamente cuando sean detectadas.</i>"
    )
    return broadcast_telegram_message(bot_token, chat_ids, test_message)

