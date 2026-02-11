# bot_supervision.py
# Requisitos:
#   pip install -U python-telegram-bot==21.6 gspread google-auth pillow
#
# Ejecutar (Windows / PowerShell):
#   $env:BOT_TOKEN="TU_TOKEN"
#   $env:SHEET_ID="TU_SHEET_ID"
#   $env:SHEET_TAB_PLANTILLAS="Plantillas"
#   $env:SHEET_TAB_SUPERVISIONES="Supervisiones"
#   $env:GOOGLE_CREDS_JSON_TEXT=(Get-Content google_creds.json -Raw)   # recomendado en Railway
#   python bot_supervision.py
#
# IMPORTANTE:
# - Para que el bot reciba mensajes en grupos: @BotFather -> Group Privacy -> Turn OFF
# - En GRUPOS, Telegram NO permite request_location=True.
#   Este bot pide que envíen ubicación MANUALMENTE (clip -> ubicación).

import os
import re
import json
import uuid
import time
import sys
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials

from PIL import Image, ImageDraw, ImageFont  # watermark

from telegram import (
    Update,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
    InputMediaPhoto,
    InputMediaVideo,
)
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

# =========================
# CAPTURA DE ERRORES "SILENCIOSOS" (Railway/PTB)
# =========================
def _install_global_exception_handlers() -> None:
    """
    Captura:
    - Excepciones no manejadas del proceso (sys.excepthook)
    - Excepciones no manejadas en el event loop de asyncio
    Esto ayuda a ver el "error real" que provoca que el bot se detenga y PTB haga Application.stop().
    """
    def _excepthook(exc_type, exc, tb):
        logging.critical("UNHANDLED EXCEPTION (sys.excepthook)", exc_info=(exc_type, exc, tb))
        try:
            sys.stdout.flush()
            sys.stderr.flush()
        except Exception:
            pass

    sys.excepthook = _excepthook

    try:
        loop = asyncio.get_event_loop()
    except Exception:
        loop = None

    if loop:
        def _loop_exception_handler(_loop, context):
            # context puede incluir: 'message', 'exception', etc.
            logging.critical("UNHANDLED ASYNCIO ERROR: %s", context.get("message", ""))
            exc = context.get("exception")
            if exc:
                logging.critical("Exception:", exc_info=exc)
            else:
                logging.critical("Context: %s", context)
            try:
                sys.stdout.flush()
                sys.stderr.flush()
            except Exception:
                pass

        loop.set_exception_handler(_loop_exception_handler)

_install_global_exception_handlers()

# =========================
# ENV
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()

SHEET_ID = os.getenv("SHEET_ID", "").strip()
SHEET_TAB_PLANTILLAS = os.getenv("SHEET_TAB_PLANTILLAS", "Plantillas").strip()
SHEET_TAB_SUPERVISIONES = os.getenv("SHEET_TAB_SUPERVISIONES", "Supervisiones").strip()

# En Railway: NO subas google_creds.json al repo.
# Usa GOOGLE_CREDS_JSON_TEXT (contenido JSON completo).
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON", "google_creds.json").strip()
GOOGLE_CREDS_JSON_TEXT = os.getenv("GOOGLE_CREDS_JSON_TEXT", "").strip()

# Config persistencia (Railway filesystem es efímero; recomendado: /tmp/group_links.json)
CONFIG_PATH = os.getenv("CONFIG_PATH", "group_links.json").strip()

MAX_MEDIA_PER_BUCKET = 8  # fotos + videos

# Watermark
ENABLE_WATERMARK_PHOTOS = os.getenv("ENABLE_WATERMARK_PHOTOS", "true").lower() in ("1", "true", "yes", "y")
WM_DIR = os.getenv("WM_DIR", "wm_tmp").strip()  # Railway recomendado: /tmp/wm_tmp
WM_FONT_SIZE = int(os.getenv("WM_FONT_SIZE", "22"))

# =========================
# STATES
# =========================
(
    S_SUPERVISOR,
    S_OPERADOR,
    S_CODIGO,
    S_TIPO,
    S_UBICACION,
    S_FACHADA_MEDIA,
    S_MENU_PRINCIPAL,
    S_MENU_CABLEADO,
    S_MENU_CUADRILLA,
    S_CARGA_MEDIA_BUCKET,
    S_ASK_OBS,
    S_WRITE_OBS,
    S_FINAL_TEXT,
) = range(13)

# =========================
# MENUS / OPCIONES
# =========================
SUPERVISORES = ["NELSON CECCATO", "HARNOL CASTAÑEDA", "EDGAR GARCIA", "JASSER RAFAELE"]
OPERADORES = ["WIN", "TU FIBRA"]

CABLEADO_ITEMS = [
    ("1. CTO", "CTO"),
    ("2. POSTE", "POSTE"),
    ("3. RUTA", "RUTA"),
    ("4. FALSO TRAMO", "FALSO_TRAMO"),
    ("5. ANCLAJE", "ANCLAJE"),
    ("6. RESERVA DOMICILIO", "RESERVA"),
    ("7. ROSETA", "ROSETA"),
    ("8. EQUIPOS", "EQUIPOS"),
    ("9. FINALIZAR EVIDENCIAS", "FIN_CABLEADO"),
]

CUADRILLA_ITEMS = [
    ("1. FOTO TECNICOS", "FOTO_TECNICOS"),
    ("2. SCTR", "SCTR"),
    ("3. ATS", "ATS"),
    ("4. LICENCIA", "LICENCIA"),
    ("5. UNIDAD", "UNIDAD"),
    ("6. SOAT", "SOAT"),
    ("7. HERRAMIENTAS", "HERRAMIENTAS"),
    ("8. KIT DE FIBRA", "KIT_FIBRA"),
    ("9. ESCALERA TELESCOPICA", "ESCALERA_TEL"),
    ("10. ESCALERA INTERNOS", "ESCALERA_INT"),
    ("11. BOTIQUIN", "BOTIQUIN"),
    ("12. FINALIZAR EVIDENCIAS", "FIN_CUADRILLA"),
]

MAIN_MENU = [
    ("🏗️EVIDENCIAS DE CABLEADO", "MENU_CABLEADO"),
    ("👷‍♂️EVIDENCIAS DE CUADRILLA", "MENU_CUADRILLA"),
    ("🚨EVIDENCIAS OPCIONALES", "MENU_OPCIONALES"),
    ("✅FINALIZAR SUPERVISION", "FINALIZAR"),
]

CABLEADO_PATTERN = r"^(CTO|POSTE|RUTA|FALSO_TRAMO|ANCLAJE|RESERVA|ROSETA|EQUIPOS|FIN_CABLEADO)$"
CUADRILLA_PATTERN = r"^(FOTO_TECNICOS|SCTR|ATS|LICENCIA|UNIDAD|SOAT|HERRAMIENTAS|KIT_FIBRA|ESCALERA_TEL|ESCALERA_INT|BOTIQUIN|FIN_CUADRILLA)$"

# =========================
# CONFIG: links Auditorías -> Evidencias
# =========================
GROUP_CFG: Dict[str, Any] = {"evidencias": {}, "links": {}}

def load_cfg() -> None:
    global GROUP_CFG
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                GROUP_CFG = json.load(f)
        except Exception:
            GROUP_CFG = {"evidencias": {}, "links": {}}
    else:
        GROUP_CFG = {"evidencias": {}, "links": {}}

def save_cfg() -> None:
    try:
        d = os.path.dirname(CONFIG_PATH)
        if d:
            os.makedirs(d, exist_ok=True)
    except Exception:
        pass

    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(GROUP_CFG, f, ensure_ascii=False, indent=2)

def in_group(update: Update) -> bool:
    chat = update.effective_chat
    return chat is not None and chat.type in ("group", "supergroup")

# =========================
# Helpers UI
# =========================
def kb_inline(options: List[Tuple[str, str]], cols: int = 2) -> InlineKeyboardMarkup:
    rows = []
    row = []
    for label, data in options:
        row.append(InlineKeyboardButton(label, callback_data=data))
        if len(row) == cols:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(rows)

def evidence_controls_keyboard() -> InlineKeyboardMarkup:
    return kb_inline(
        [("➕ CARGAR MAS", "ADD_MORE"), ("✅ EVIDENCIAS COMPLETAS", "DONE_MEDIA")],
        cols=1,
    )

def chunk_list(lst: List[Any], n: int) -> List[List[Any]]:
    return [lst[i:i + n] for i in range(0, len(lst), n)]

async def send_message(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, reply_markup=None):
    await context.application.bot.send_message(
        chat_id=update.effective_chat.id,
        text=text,
        reply_markup=reply_markup,
    )

async def safe_edit_or_send(query, text: str, reply_markup=None):
    try:
        await query.edit_message_text(text=text, reply_markup=reply_markup)
    except Exception:
        await query.get_bot().send_message(chat_id=query.message.chat_id, text=text, reply_markup=reply_markup)

# =========================
# Session state
# =========================
def sess(context: ContextTypes.DEFAULT_TYPE) -> Dict[str, Any]:
    if "s" not in context.user_data:
        context.user_data["s"] = {
            "origin_chat_id": None,
            "supervisor": None,
            "operador": None,
            "codigo": None,
            "tipo": None,
            "location": None,  # (lat, lon)
            "final_text": "",

            # media items: {"type":"photo|video", "file_id": "...", "wm_file": "...optional local path"}
            "fachada": {"media": []},
            "cableado": {},   # bucket -> {media:[], obs:""}
            "cuadrilla": {},  # bucket -> {media:[], obs:""}
            "opcionales": {"media": [], "obs": ""},
            "current_section": None,
            "current_bucket": None,

            # plantilla data (from sheet)
            "plantilla_uuid": "",
            "plantilla_tecnico": "",
            "plantilla_contrata": "",
            "plantilla_distrito": "",
            "plantilla_gestor": "",
        }
    return context.user_data["s"]

def ensure_bucket(s: Dict[str, Any], section: str, bucket: Optional[str]) -> Dict[str, Any]:
    if section == "fachada":
        return s["fachada"]
    if section == "opcionales":
        return s["opcionales"]
    if section in ("cableado", "cuadrilla"):
        if not bucket:
            raise ValueError("bucket requerido")
        if bucket not in s[section]:
            s[section][bucket] = {"media": [], "obs": ""}
        return s[section][bucket]
    raise ValueError("section inválida")

def cleanup_wm_dir_if_empty() -> None:
    try:
        if os.path.isdir(WM_DIR) and not os.listdir(WM_DIR):
            os.rmdir(WM_DIR)
    except Exception:
        pass

def cleanup_session_temp_files(s_: Dict[str, Any]) -> None:
    """Borra archivos watermark temporales registrados en los items."""
    try:
        for section in ("fachada",):
            for item in s_.get(section, {}).get("media", []):
                p = item.get("wm_file")
                if p and os.path.exists(p):
                    try:
                        os.remove(p)
                    except Exception:
                        pass

        for sec in ("cableado", "cuadrilla"):
            for _, data in s_.get(sec, {}).items():
                for item in data.get("media", []):
                    p = item.get("wm_file")
                    if p and os.path.exists(p):
                        try:
                            os.remove(p)
                        except Exception:
                            pass

        for item in s_.get("opcionales", {}).get("media", []):
            p = item.get("wm_file")
            if p and os.path.exists(p):
                try:
                    os.remove(p)
                except Exception:
                    pass
    finally:
        cleanup_wm_dir_if_empty()

# =========================
# Media extraction + watermark
# =========================
def extract_media_from_message(update: Update) -> Optional[Dict[str, str]]:
    msg = update.message
    if not msg:
        return None

    if msg.photo:
        return {"type": "photo", "file_id": msg.photo[-1].file_id}

    if msg.video:
        return {"type": "video", "file_id": msg.video.file_id}

    if msg.document and (msg.document.mime_type or "").startswith("video/"):
        return {"type": "video", "file_id": msg.document.file_id}

    return None

def _fmt_latlon(lat: Optional[float], lon: Optional[float]) -> str:
    if lat is None or lon is None:
        return "Lat/Lon: N/D"
    return f"Lat/Lon: {lat:.6f}, {lon:.6f}"

def _try_load_font(size: int) -> ImageFont.FreeTypeFont:
    # Railway suele no tener Arial. Probamos Arial y si no, default.
    for font_name in ("arial.ttf", "Arial.ttf"):
        try:
            return ImageFont.truetype(font_name, size)
        except Exception:
            continue
    return ImageFont.load_default()

async def apply_watermark_photo_if_needed(
    app: Application,
    file_id: str,
    lat: Optional[float],
    lon: Optional[float],
    sent_dt_local: str
) -> Tuple[str, Optional[str]]:
    """
    Devuelve (file_id_original, path_local_watermarked_or_none)
    - En fotos: descarga, coloca texto y guarda en WM_DIR para re-enviar como archivo local.
    """
    if not ENABLE_WATERMARK_PHOTOS:
        return file_id, None

    try:
        os.makedirs(WM_DIR, exist_ok=True)

        tg_file = await app.bot.get_file(file_id)
        local_in = os.path.join(WM_DIR, f"in_{int(time.time()*1000)}.jpg")
        local_out = os.path.join(WM_DIR, f"wm_{int(time.time()*1000)}.jpg")

        await tg_file.download_to_drive(custom_path=local_in)

        im = Image.open(local_in).convert("RGB")
        draw = ImageDraw.Draw(im)
        font = _try_load_font(WM_FONT_SIZE)

        text = f"{sent_dt_local} | {_fmt_latlon(lat, lon)}"

        padding = 10
        try:
            bbox = draw.textbbox((0, 0), text, font=font)
            tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
        except Exception:
            tw, th = int(draw.textlength(text, font=font)), WM_FONT_SIZE + 6

        x = 10
        y = im.height - th - padding*2 - 10
        rect = [x - 5, y - 5, x + tw + padding, y + th + padding]

        draw.rectangle(rect, fill=(0, 0, 0))
        draw.text((x + 5, y + 5), text, font=font, fill=(255, 255, 255))

        im.save(local_out, "JPEG", quality=90)

        try:
            os.remove(local_in)
        except Exception:
            pass

        return file_id, local_out
    except Exception as e:
        logging.warning(f"No se pudo aplicar watermark: {e}")
        return file_id, None

# =========================
# Google Sheets helper
# =========================
_GS_CACHE: Dict[str, Any] = {"client": None, "p_headers": None, "s_headers": None}

def ensure_google_creds_file() -> None:
    """
    Compat: crea el archivo GOOGLE_CREDS_JSON (default google_creds.json)
    usando el contenido de GOOGLE_CREDS_JSON_TEXT.
    (En Railway preferimos NO usar archivo y usar from_service_account_info.)
    """
    if GOOGLE_CREDS_JSON_TEXT and not os.path.exists(GOOGLE_CREDS_JSON):
        try:
            d = os.path.dirname(GOOGLE_CREDS_JSON)
            if d:
                os.makedirs(d, exist_ok=True)
        except Exception:
            pass
        with open(GOOGLE_CREDS_JSON, "w", encoding="utf-8") as f:
            f.write(GOOGLE_CREDS_JSON_TEXT)

def _gs_ready() -> bool:
    if not SHEET_ID:
        return False
    if GOOGLE_CREDS_JSON_TEXT:
        return True
    return os.path.exists(GOOGLE_CREDS_JSON)

def gs_clear_cache() -> None:
    _GS_CACHE.update({"client": None, "p_headers": None, "s_headers": None})

# ✅ CAMBIO PRINCIPAL: usar from_service_account_info cuando hay GOOGLE_CREDS_JSON_TEXT
def gs_client() -> gspread.Client:
    if _GS_CACHE["client"] is not None:
        return _GS_CACHE["client"]

    if not _gs_ready():
        raise RuntimeError("Google Sheets no está configurado (SHEET_ID o GOOGLE_CREDS_JSON_TEXT/archivo).")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    # Preferir JSON texto (Railway)
    if GOOGLE_CREDS_JSON_TEXT:
        info = json.loads(GOOGLE_CREDS_JSON_TEXT)

        # Arreglo típico: Railway puede dejar \\n literal en private_key
        pk = info.get("private_key", "")
        if isinstance(pk, str) and "\\n" in pk:
            info["private_key"] = pk.replace("\\n", "\n")

        creds = Credentials.from_service_account_info(info, scopes=scopes)
    else:
        ensure_google_creds_file()
        creds = Credentials.from_service_account_file(GOOGLE_CREDS_JSON, scopes=scopes)

    client = gspread.authorize(creds)
    _GS_CACHE["client"] = client
    return client

def gs_ws(tab_name: str):
    client = gs_client()
    sh = client.open_by_key(SHEET_ID)
    return sh.worksheet(tab_name)

def gs_headers(tab_name: str) -> List[str]:
    if tab_name == SHEET_TAB_PLANTILLAS and _GS_CACHE["p_headers"] is not None:
        return _GS_CACHE["p_headers"]
    if tab_name == SHEET_TAB_SUPERVISIONES and _GS_CACHE["s_headers"] is not None:
        return _GS_CACHE["s_headers"]

    ws = gs_ws(tab_name)
    headers = ws.row_values(1)
    headers = [h.strip() for h in headers if h is not None]

    if tab_name == SHEET_TAB_PLANTILLAS:
        _GS_CACHE["p_headers"] = headers
    else:
        _GS_CACHE["s_headers"] = headers
    return headers

def gs_append_dict(tab_name: str, data: Dict[str, Any]) -> None:
    ws = gs_ws(tab_name)
    headers = gs_headers(tab_name)

    row = []
    for h in headers:
        val = data.get(h, "")
        if val is None:
            val = ""
        row.append(str(val))

    ws.append_row(row, value_input_option="USER_ENTERED")

def gs_find_last_row_index_by_criteria(tab_name: str, criteria: Dict[str, str]) -> Optional[int]:
    """
    Busca la ÚLTIMA fila que coincida exactamente con criteria en columnas existentes.
    Retorna el índice de fila (1-based) o None.
    """
    ws = gs_ws(tab_name)
    headers = gs_headers(tab_name)
    header_to_idx = {h: i for i, h in enumerate(headers)}  # 0-based

    values = ws.get_all_values()
    if not values or len(values) < 2:
        return None

    last_match = None
    for r in range(2, len(values) + 1):
        row = values[r - 1]
        ok = True
        for k, v in criteria.items():
            if k not in header_to_idx:
                ok = False
                break
            idx = header_to_idx[k]
            cell = row[idx] if idx < len(row) else ""
            if str(cell).strip() != str(v).strip():
                ok = False
                break
        if ok:
            last_match = r

    return last_match

def gs_delete_row(tab_name: str, row_index: int) -> None:
    ws = gs_ws(tab_name)
    ws.delete_rows(row_index)

PERU_TZ = timezone(timedelta(hours=-5))

def now_peru_str() -> str:
    dt = datetime.now(PERU_TZ)
    return dt.strftime("%Y-%m-%d %H:%M:%S")

# =========================
# Plantillas: template + parse
# =========================
PLANTILLA_TEXT = (
    "📌 Copia/pega esta plantilla y envíala COMPLETA en un solo mensaje.\n\n"
    "Tipo de supervisión:\n"
    "Tipificación:\n"
    "Teléfono:\n"
    "DNI:\n"
    "Cliente:\n"
    "Código pedido:\n"
    "Dirección:\n"
    "Distrito:\n"
    "Plan:\n"
    "CTO1:\n"
    "Técnico:\n"
    "Contrata:\n"
    "Gestor:\n"
)

def parse_plantilla(text: str) -> Dict[str, str]:
    """
    Extrae campos básicos. No falla si faltan.
    Se enfoca en: CodigoPedido, Tecnico, Contrata, Distrito, Gestor.
    """
    def pick(label: str) -> str:
        m = re.search(rf"(?im)^{re.escape(label)}\s*:\s*(.+)$", text.strip(), re.MULTILINE)
        return (m.group(1).strip() if m else "")

    return {
        "CodigoPedido": pick("Código pedido") or pick("Codigo pedido"),
        "Tecnico": pick("Técnico") or pick("Tecnico"),
        "Contrata": pick("Contrata"),
        "Distrito": pick("Distrito"),
        "Gestor": pick("Gestor"),
    }

async def cmd_plantilla(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not in_group(update):
        await send_message(update, context, "Usa /plantilla dentro del grupo.")
        return
    await send_message(update, context, PLANTILLA_TEXT)

async def auto_capture_plantilla(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Se ejecuta para mensajes de texto en grupos.
    Si detecta "Código pedido:", intenta guardar en Google Sheet Plantillas.
    """
    if not in_group(update):
        return

    msg = update.message
    if not msg or not msg.text:
        return

    text = msg.text.strip()
    if not re.search(r"(?im)c[oó]digo\s+pedido\s*:", text):
        return

    data = parse_plantilla(text)
    codigo = data.get("CodigoPedido", "").strip()
    if not codigo:
        await send_message(update, context, "⚠️ Detecté una plantilla, pero falta 'Código pedido:'. Corrige y reenvía.")
        return

    if not _gs_ready():
        await send_message(update, context, "⚠️ Google Sheets no está configurado (SHEET_ID/credenciales).")
        return

    plantilla_uuid = str(uuid.uuid4())

    row = {
        "FechaPlantilla": now_peru_str(),
        "ChatID": str(update.effective_chat.id),
        "UsuarioID": str(update.effective_user.id if update.effective_user else ""),
        "CódigoPedido": codigo,
        "Técnico": data.get("Tecnico", ""),
        "Contrata": data.get("Contrata", ""),
        "Distrito": data.get("Distrito", ""),
        "Gestor": data.get("Gestor", ""),
        "PlantillaRaw": text,
        "PlantillaUUID": plantilla_uuid,
    }

    try:
        gs_append_dict(SHEET_TAB_PLANTILLAS, row)
        await send_message(update, context, f"✅ Plantilla guardada.\nCódigoPedido: {codigo}\nUUID: {plantilla_uuid}")
    except Exception as e:
        logging.exception("Error guardando plantilla")
        await send_message(update, context, f"❌ No pude guardar la plantilla en Sheets.\nDetalle: {e}")

async def cmd_cancelar_plantilla(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /cancelar_plantilla <CODIGO>
    Borra la última plantilla en Plantillas que coincida exactamente con:
      - ChatID (grupo)
      - UsuarioID (quien ejecuta)
      - CódigoPedido (argumento)
    Luego envía plantilla en blanco.
    """
    if not in_group(update):
        await send_message(update, context, "Usa /cancelar_plantilla dentro del grupo.")
        return

    args = context.args or []
    if not args:
        await send_message(update, context, "Uso: /cancelar_plantilla <CODIGO_PEDIDO>")
        return

    codigo = " ".join(args).strip()

    if not _gs_ready():
        await send_message(update, context, "⚠️ Google Sheets no está configurado (SHEET_ID/credenciales).")
        return

    chat_id = str(update.effective_chat.id)
    user_id = str(update.effective_user.id if update.effective_user else "")

    criteria = {"ChatID": chat_id, "UsuarioID": user_id, "CódigoPedido": codigo}

    try:
        row_idx = gs_find_last_row_index_by_criteria(SHEET_TAB_PLANTILLAS, criteria)
        if not row_idx:
            await send_message(update, context, f"⚠️ No encontré una plantilla para CódigoPedido {codigo} (de tu usuario).")
            return

        gs_delete_row(SHEET_TAB_PLANTILLAS, row_idx)
        await send_message(update, context, f"✅ Plantilla eliminada para CódigoPedido {codigo}.\nVuelve a enviarla corregida 👇\n\n{PLANTILLA_TEXT}")
    except Exception as e:
        logging.exception("Error borrando plantilla")
        await send_message(update, context, f"❌ No pude eliminar la plantilla.\nDetalle: {e}")

async def cmd_reload_sheet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    gs_clear_cache()
    await send_message(update, context, "✅ Cache de Google Sheets recargado (headers/worksheet).")

# =========================
# Buscar plantilla por CódigoPedido para /inicio
# =========================
def gs_fetch_last_plantilla_for_codigo(codigo: str) -> Optional[Dict[str, str]]:
    """
    Devuelve dict con:
      Técnico, Contrata, Distrito, Gestor, PlantillaUUID
    buscando la última fila en Plantillas con CódigoPedido == codigo.
    """
    ws = gs_ws(SHEET_TAB_PLANTILLAS)
    headers = gs_headers(SHEET_TAB_PLANTILLAS)
    header_to_idx = {h: i for i, h in enumerate(headers)}

    values = ws.get_all_values()
    if not values or len(values) < 2:
        return None

    idx_cod = header_to_idx.get("CódigoPedido")
    if idx_cod is None:
        return None

    idx_tecnico = header_to_idx.get("Técnico")
    idx_contrata = header_to_idx.get("Contrata")
    idx_distrito = header_to_idx.get("Distrito")
    idx_gestor = header_to_idx.get("Gestor")
    idx_uuid = header_to_idx.get("PlantillaUUID")

    last = None
    for r in range(2, len(values) + 1):
        row = values[r - 1]
        cell = row[idx_cod] if idx_cod < len(row) else ""
        if str(cell).strip() == str(codigo).strip():
            last = row

    if not last:
        return None

    def safe(idx: Optional[int]) -> str:
        if idx is None:
            return ""
        return last[idx].strip() if idx < len(last) else ""

    return {
        "Técnico": safe(idx_tecnico),
        "Contrata": safe(idx_contrata),
        "Distrito": safe(idx_distrito),
        "Gestor": safe(idx_gestor),
        "PlantillaUUID": safe(idx_uuid),
    }

# =========================
# CONFIG COMMANDS
# =========================
async def _set_evidencias(update: Update, context: ContextTypes.DEFAULT_TYPE, key: str):
    if not in_group(update):
        await send_message(update, context, "Este comando debe ejecutarse dentro del grupo Evidencias correspondiente.")
        return
    GROUP_CFG.setdefault("evidencias", {})
    GROUP_CFG["evidencias"][key] = update.effective_chat.id
    save_cfg()
    await send_message(update, context, f"✅ Evidencias '{key}' configurado. chat_id={update.effective_chat.id}")

async def set_evidencias_rafael(update: Update, context: ContextTypes.DEFAULT_TYPE): await _set_evidencias(update, context, "rafael")
async def set_evidencias_edgar(update: Update, context: ContextTypes.DEFAULT_TYPE): await _set_evidencias(update, context, "edgar")
async def set_evidencias_harnol(update: Update, context: ContextTypes.DEFAULT_TYPE): await _set_evidencias(update, context, "harnol")
async def set_evidencias_nelson(update: Update, context: ContextTypes.DEFAULT_TYPE): await _set_evidencias(update, context, "nelson")
async def set_evidencias_pruebas(update: Update, context: ContextTypes.DEFAULT_TYPE): await _set_evidencias(update, context, "pruebas")

async def _link_from_auditorias(update: Update, context: ContextTypes.DEFAULT_TYPE, key: str):
    if not in_group(update):
        await send_message(update, context, "Este comando debe ejecutarse dentro del grupo AUDITORIAS correspondiente.")
        return

    ev_id = GROUP_CFG.get("evidencias", {}).get(key)
    if ev_id is None:
        await send_message(update, context, f"⚠️ Primero configura Evidencias '{key}' con /set_evidencias_{key} en ese grupo.")
        return

    aud_id = update.effective_chat.id
    GROUP_CFG.setdefault("links", {})
    GROUP_CFG["links"][str(aud_id)] = ev_id
    save_cfg()

    await send_message(update, context, f"✅ Link creado.\nAUDITORIAS chat_id={aud_id}\n➡️ EVIDENCIAS '{key}' chat_id={ev_id}")

async def link_rafael(update: Update, context: ContextTypes.DEFAULT_TYPE): await _link_from_auditorias(update, context, "rafael")
async def link_edgar(update: Update, context: ContextTypes.DEFAULT_TYPE): await _link_from_auditorias(update, context, "edgar")
async def link_harnol(update: Update, context: ContextTypes.DEFAULT_TYPE): await _link_from_auditorias(update, context, "harnol")
async def link_nelson(update: Update, context: ContextTypes.DEFAULT_TYPE): await _link_from_auditorias(update, context, "nelson")
async def link_pruebas(update: Update, context: ContextTypes.DEFAULT_TYPE): await _link_from_auditorias(update, context, "pruebas")

async def ver_links(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ev = GROUP_CFG.get("evidencias", {})
    links = GROUP_CFG.get("links", {})

    lines = ["🧩 CONFIG LINKS\n"]
    lines.append("📌 Evidencias configuradas:")
    for k in ["rafael", "edgar", "harnol", "nelson", "pruebas"]:
        lines.append(f"• {k}: {'✅' if ev.get(k) else '❌'}")

    lines.append("\n📌 Links Auditorías ➜ Evidencias:")
    if not links:
        lines.append("• (sin links)")
    else:
        for a, e in links.items():
            lines.append(f"• AUD {a} ➜ EVI {e}")

    await send_message(update, context, "\n".join(lines))

# =========================
# FLOW: /inicio
# =========================
async def inicio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not in_group(update):
        await send_message(update, context, "Este bot se usa desde un grupo AUDITORIAS_... (no en privado).")
        return ConversationHandler.END

    # Reset sesión
    context.user_data.pop("s", None)
    s_ = sess(context)
    s_["origin_chat_id"] = update.effective_chat.id

    await send_message(
        update,
        context,
        "PASO 1 - NOMBRE DEL SUPERVISOR",
        reply_markup=kb_inline([(x, f"SUP_{i}") for i, x in enumerate(SUPERVISORES)], cols=2),
    )
    return S_SUPERVISOR

async def on_pick_supervisor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    s_ = sess(context)

    m = re.match(r"SUP_(\d+)", query.data or "")
    if not m:
        return S_SUPERVISOR

    s_["supervisor"] = SUPERVISORES[int(m.group(1))]

    await safe_edit_or_send(
        query,
        "PASO 2 - OPERADOR / CUADRILLA",
        reply_markup=kb_inline([(x, f"OP_{i}") for i, x in enumerate(OPERADORES)], cols=2),
    )
    return S_OPERADOR

async def on_pick_operador(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    s_ = sess(context)

    m = re.match(r"OP_(\d+)", query.data or "")
    if not m:
        return S_OPERADOR

    s_["operador"] = OPERADORES[int(m.group(1))]

    await safe_edit_or_send(
        query,
        "PASO 3 - INGRESA CÓDIGO DE PEDIDO\n\n✅ Puede ser números o letras.",
        reply_markup=None,
    )
    return S_CODIGO

# =========================
# RESCATE: detectar código si el ConversationHandler se desincroniza
# =========================
def looks_like_codigo(text: str) -> bool:
    t = (text or "").strip()
    return 3 <= len(t) <= 30 and re.match(r"^[A-Za-z0-9_-]+$", t) is not None

async def codigo_global(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Rescate: si el ConversationHandler no toma el código, lo capturamos aquí."""
    if not in_group(update):
        return
    if not update.message or not update.message.text:
        return

    s_ = context.user_data.get("s")
    if not s_:
        return

    # Solo si hay sesión activa del mismo chat y todavía no hay código
    if s_.get("origin_chat_id") != update.effective_chat.id:
        return
    if s_.get("codigo"):
        return

    text = update.message.text.strip()
    if not looks_like_codigo(text):
        return

    logging.info("⚠️ Código capturado por handler GLOBAL (rescate).")
    await on_codigo(update, context)

async def on_codigo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logging.info("✅ on_codigo() ENTER")
    logging.info(f"Texto recibido: {repr(update.message.text)}")
    s_ = sess(context)

    codigo = (update.message.text or "").strip()
    logging.info(f"Codigo parseado: {codigo}")

    if not codigo:
        await send_message(update, context, "❌ Código vacío. Intenta nuevamente.")
        return S_CODIGO

    s_["codigo"] = codigo

    # Buscar plantilla en Sheets (si está configurado)
    if _gs_ready():
        try:
            found = gs_fetch_last_plantilla_for_codigo(codigo)
            if found:
                s_["plantilla_uuid"] = found.get("PlantillaUUID", "")
                s_["plantilla_tecnico"] = found.get("Técnico", "")
                s_["plantilla_contrata"] = found.get("Contrata", "")
                s_["plantilla_distrito"] = found.get("Distrito", "")
                s_["plantilla_gestor"] = found.get("Gestor", "")
        except Exception as e:
            logging.warning(f"No se pudo leer plantilla de Sheets: {e}")

    await send_message(
        update,
        context,
        "PASO 4 - TIPO DE SUPERVISIÓN",
        reply_markup=kb_inline(
            [("🔥SUPERVISION EN CALIENTE", "TIPO_CALIENTE"), ("🧊SUPERVISION EN FRIO", "TIPO_FRIO")],
            cols=1,
        ),
    )
    return S_TIPO

async def on_pick_tipo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    s_ = sess(context)

    if query.data == "TIPO_CALIENTE":
        s_["tipo"] = "CALIENTE"
    elif query.data == "TIPO_FRIO":
        s_["tipo"] = "FRIO"
    else:
        return S_TIPO

    await safe_edit_or_send(
        query,
        "PASO 5 - REPORTA TU UBICACIÓN\n\n"
        "📌 En grupos, Telegram no permite solicitar ubicación con botón.\n"
        "✅ Envía tu ubicación así:\n"
        "1) Pulsa el clip 📎\n"
        "2) Ubicación\n"
        "3) Enviar ubicación actual",
        reply_markup=None,
    )

    kb = ReplyKeyboardMarkup(
        [[KeyboardButton("📍 ENVIAR UBICACION (manual)")]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    await context.application.bot.send_message(chat_id=query.message.chat_id, text="👇", reply_markup=kb)
    return S_UBICACION

async def on_location(update: Update, context: ContextTypes.DEFAULT_TYPE):
    s_ = sess(context)
    loc = update.message.location
    if not loc:
        await send_message(update, context, "❌ No recibí ubicación. Envíala con 📎 -> Ubicación -> Enviar ubicación actual.")
        return S_UBICACION

    s_["location"] = (loc.latitude, loc.longitude)
    s_["current_section"] = "fachada"
    s_["current_bucket"] = None

    await send_message(
        update,
        context,
        f"PASO 6 - EVIDENCIA DE FACHADA\n📸🎥 Carga entre 1 a {MAX_MEDIA_PER_BUCKET} archivos (fotos o videos).",
        reply_markup=ReplyKeyboardRemove(),
    )
    return S_FACHADA_MEDIA

# =========================
# Media (fotos + videos)
# =========================
async def on_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    s_ = sess(context)
    section = s_.get("current_section")
    bucket = s_.get("current_bucket")

    if not section:
        return

    item = extract_media_from_message(update)
    if not item:
        await send_message(update, context, "❌ Solo se aceptan fotos o videos.")
        return S_CARGA_MEDIA_BUCKET if section != "fachada" else S_FACHADA_MEDIA

    b = ensure_bucket(s_, section, bucket)
    media_list = b["media"]

    if len(media_list) >= MAX_MEDIA_PER_BUCKET:
        await send_message(
            update,
            context,
            f"⚠️ Límite alcanzado ({MAX_MEDIA_PER_BUCKET}). Presiona ✅ EVIDENCIAS COMPLETAS.",
            reply_markup=evidence_controls_keyboard(),
        )
        return S_CARGA_MEDIA_BUCKET if section != "fachada" else S_FACHADA_MEDIA

    # Watermark solo para fotos
    if item["type"] == "photo" and ENABLE_WATERMARK_PHOTOS:
        lat, lon = s_.get("location") if s_.get("location") else (None, None)
        sent_dt = now_peru_str()
        _, wm_path = await apply_watermark_photo_if_needed(
            context.application,
            item["file_id"],
            lat,
            lon,
            sent_dt_local=sent_dt,
        )
        if wm_path:
            item["wm_file"] = wm_path

    media_list.append(item)

    await send_message(
        update,
        context,
        f"✅ Guardado ({len(media_list)}/{MAX_MEDIA_PER_BUCKET}).",
        reply_markup=evidence_controls_keyboard(),
    )
    return S_FACHADA_MEDIA if section == "fachada" else S_CARGA_MEDIA_BUCKET

async def on_add_more_or_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    s_ = sess(context)

    section = s_.get("current_section")
    bucket = s_.get("current_bucket")
    b = ensure_bucket(s_, section, bucket)

    if query.data == "ADD_MORE":
        await safe_edit_or_send(query, "📸🎥 Envía el siguiente archivo (foto o video).", reply_markup=None)
        return S_FACHADA_MEDIA if section == "fachada" else S_CARGA_MEDIA_BUCKET

    if query.data == "DONE_MEDIA":
        if len(b["media"]) < 1:
            await safe_edit_or_send(query, "⚠️ Debes cargar al menos 1 archivo antes de completar.", reply_markup=None)
            return S_FACHADA_MEDIA if section == "fachada" else S_CARGA_MEDIA_BUCKET

        if section == "fachada":
            await safe_edit_or_send(query, "PASO 7 - ELEGIR SIGUIENTE PASO", reply_markup=kb_inline(MAIN_MENU, cols=1))
            return S_MENU_PRINCIPAL

        await safe_edit_or_send(
            query,
            "¿Deseas ingresar Observación?",
            reply_markup=kb_inline([("SI", "OBS_SI"), ("NO", "OBS_NO")], cols=2),
        )
        return S_ASK_OBS

    return S_MENU_PRINCIPAL

async def on_obs_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    s_ = sess(context)
    section = s_.get("current_section")

    if query.data == "OBS_SI":
        await safe_edit_or_send(query, "📝 Escribe tu observación:", reply_markup=None)
        return S_WRITE_OBS

    if query.data == "OBS_NO":
        if section == "cableado":
            await safe_edit_or_send(query, "QUE EVIDENCIAS DESEAS CARGAR (CABLEADO)", reply_markup=kb_inline(CABLEADO_ITEMS, cols=2))
            return S_MENU_CABLEADO
        if section == "cuadrilla":
            await safe_edit_or_send(query, "QUE EVIDENCIAS DESEAS CARGAR (CUADRILLA)", reply_markup=kb_inline(CUADRILLA_ITEMS, cols=2))
            return S_MENU_CUADRILLA
        if section == "opcionales":
            await safe_edit_or_send(query, "PASO 7 - ELEGIR SIGUIENTE PASO", reply_markup=kb_inline(MAIN_MENU, cols=1))
            return S_MENU_PRINCIPAL

    return S_MENU_PRINCIPAL

async def on_write_obs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    s_ = sess(context)
    section = s_.get("current_section")
    bucket = s_.get("current_bucket")
    obs = (update.message.text or "").strip()

    b = ensure_bucket(s_, section, bucket)

    # Permitimos cualquier caracter (-, *, etc.)
    if b.get("obs"):
        b["obs"] = (b["obs"].rstrip() + "\n" + obs).strip()
    else:
        b["obs"] = obs

    if section == "cableado":
        await send_message(update, context, "✅ Observación guardada.\n\nQUE EVIDENCIAS DESEAS CARGAR (CABLEADO)", reply_markup=kb_inline(CABLEADO_ITEMS, cols=2))
        return S_MENU_CABLEADO
    if section == "cuadrilla":
        await send_message(update, context, "✅ Observación guardada.\n\nQUE EVIDENCIAS DESEAS CARGAR (CUADRILLA)", reply_markup=kb_inline(CUADRILLA_ITEMS, cols=2))
        return S_MENU_CUADRILLA
    if section == "opcionales":
        await send_message(update, context, "✅ Observación guardada.\n\nPASO 7 - ELEGIR SIGUIENTE PASO", reply_markup=kb_inline(MAIN_MENU, cols=1))
        return S_MENU_PRINCIPAL

    await send_message(update, context, "✅ Observación guardada.")
    return S_MENU_PRINCIPAL

# =========================
# Menú principal + submenús
# =========================
async def on_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    s_ = sess(context)

    if query.data == "MENU_CABLEADO":
        s_["current_section"] = "cableado"
        s_["current_bucket"] = None
        await safe_edit_or_send(query, "QUE EVIDENCIAS DESEAS CARGAR (CABLEADO)", reply_markup=kb_inline(CABLEADO_ITEMS, cols=2))
        return S_MENU_CABLEADO

    if query.data == "MENU_CUADRILLA":
        s_["current_section"] = "cuadrilla"
        s_["current_bucket"] = None
        await safe_edit_or_send(query, "QUE EVIDENCIAS DESEAS CARGAR (CUADRILLA)", reply_markup=kb_inline(CUADRILLA_ITEMS, cols=2))
        return S_MENU_CUADRILLA

    if query.data == "MENU_OPCIONALES":
        s_["current_section"] = "opcionales"
        s_["current_bucket"] = None
        await safe_edit_or_send(query, f"🚨 EVIDENCIAS OPCIONALES\n📸🎥 Carga entre 1 a {MAX_MEDIA_PER_BUCKET} archivos.", reply_markup=None)
        return S_CARGA_MEDIA_BUCKET

    if query.data == "FINALIZAR":
        await safe_edit_or_send(query, "INGRESAR OBSERVACIONES FINALES\n(Escribe el texto final)", reply_markup=None)
        return S_FINAL_TEXT

    return S_MENU_PRINCIPAL

async def on_menu_cableado(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    s_ = sess(context)

    data = query.data or ""
    if data == "FIN_CABLEADO":
        await safe_edit_or_send(query, "PASO 7 - ELEGIR SIGUIENTE PASO", reply_markup=kb_inline(MAIN_MENU, cols=1))
        return S_MENU_PRINCIPAL

    s_["current_section"] = "cableado"
    s_["current_bucket"] = data
    ensure_bucket(s_, "cableado", data)

    await safe_edit_or_send(query, f"🏗️ CABLEADO - {data}\n📸🎥 Carga entre 1 a {MAX_MEDIA_PER_BUCKET} archivos.", reply_markup=None)
    return S_CARGA_MEDIA_BUCKET

async def on_menu_cuadrilla(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    s_ = sess(context)

    data = query.data or ""
    if data == "FIN_CUADRILLA":
        await safe_edit_or_send(query, "PASO 7 - ELEGIR SIGUIENTE PASO", reply_markup=kb_inline(MAIN_MENU, cols=1))
        return S_MENU_PRINCIPAL

    s_["current_section"] = "cuadrilla"
    s_["current_bucket"] = data
    ensure_bucket(s_, "cuadrilla", data)

    await safe_edit_or_send(query, f"👷‍♂️ CUADRILLA - {data}\n📸🎥 Carga entre 1 a {MAX_MEDIA_PER_BUCKET} archivos.", reply_markup=None)
    return S_CARGA_MEDIA_BUCKET

# =========================
# Finalización: enviar a 1 grupo Evidencias según grupo origen
# + guardado en Google Sheet "Supervisiones"
# =========================
def build_summary(s_: Dict[str, Any]) -> str:
    lat, lon = s_["location"] if s_["location"] else (None, None)
    maps = f"https://maps.google.com/?q={lat},{lon}" if lat is not None else "No disponible"
    extra = ""
    if s_.get("plantilla_tecnico") or s_.get("plantilla_contrata") or s_.get("plantilla_distrito") or s_.get("plantilla_gestor"):
        extra = (
            "\n🧩 Datos de Plantilla:\n"
            f"• Técnico: {s_.get('plantilla_tecnico','')}\n"
            f"• Contrata: {s_.get('plantilla_contrata','')}\n"
            f"• Distrito: {s_.get('plantilla_distrito','')}\n"
            f"• Gestor: {s_.get('plantilla_gestor','')}\n"
            f"• PlantillaUUID: {s_.get('plantilla_uuid','')}\n"
        )
    return (
        "📋 SUPERVISIÓN FINALIZADA\n\n"
        f"👷 Supervisor: {s_['supervisor']}\n"
        f"🏢 Operador: {s_['operador']}\n"
        f"🧾 Código de pedido: {s_['codigo']}\n"
        f"🔥 Tipo de supervisión: {s_['tipo']}\n\n"
        f"📍 Ubicación:\n{maps}\n"
        f"{extra}\n"
        "📝 Observaciones finales:\n"
        f"{s_['final_text']}"
    )

def to_input_media(item: Dict[str, str]):
    if item["type"] == "photo":
        return InputMediaPhoto(item["file_id"])
    return InputMediaVideo(item["file_id"])

async def send_media_section(app: Application, chat_id: int, title: str, media_items: List[Dict[str, str]]):
    if not media_items:
        return
    await app.bot.send_message(chat_id=chat_id, text=title)

    batch: List[Dict[str, str]] = []
    for it in media_items:
        if it.get("type") == "photo" and it.get("wm_file") and os.path.exists(it["wm_file"]):
            if batch:
                for chunk in chunk_list(batch, 10):
                    media = [to_input_media(x) for x in chunk]
                    await app.bot.send_media_group(chat_id=chat_id, media=media)
                batch = []
            with open(it["wm_file"], "rb") as f:
                await app.bot.send_photo(chat_id=chat_id, photo=f)
        else:
            batch.append(it)

    if batch:
        for chunk in chunk_list(batch, 10):
            media = [to_input_media(x) for x in chunk]
            await app.bot.send_media_group(chat_id=chat_id, media=media)

def map_obs_columns() -> Dict[Tuple[str, str], str]:
    return {
        ("cableado", "CTO"): "Observaciones CTO",
        ("cableado", "POSTE"): "Observaciones POSTE",
        ("cableado", "RUTA"): "Observaciones RUTA",
        ("cableado", "FALSO_TRAMO"): "Observaciones FALSO TRAMO",
        ("cableado", "ANCLAJE"): "Observaciones ANCLAJE",
        ("cableado", "RESERVA"): "Observaciones RESERVA DOMICILIO",
        ("cableado", "ROSETA"): "Observaciones ROSETA",
        ("cableado", "EQUIPOS"): "Observaciones EQUIPOS",

        ("cuadrilla", "FOTO_TECNICOS"): "Observaciones TECNICOS",
        ("cuadrilla", "SCTR"): "Observaciones SCTR",
        ("cuadrilla", "ATS"): "Observaciones ATS",
        ("cuadrilla", "LICENCIA"): "Observaciones LICENCIA",
        ("cuadrilla", "UNIDAD"): "Observaciones UNIDAD",
        ("cuadrilla", "SOAT"): "Observaciones SOAT",
        ("cuadrilla", "HERRAMIENTAS"): "Observaciones HERRAMIENTAS",
        ("cuadrilla", "KIT_FIBRA"): "Observaciones KIT DE FIBRA",
        ("cuadrilla", "ESCALERA_TEL"): "Observaciones ESCALERA TELESCOPICA",
        ("cuadrilla", "ESCALERA_INT"): "Observaciones ESCALERA INTERNOS",
        ("cuadrilla", "BOTIQUIN"): "Observaciones BOTIQUIN",
    }

def build_supervisiones_row(s_: Dict[str, Any]) -> Dict[str, Any]:
    row: Dict[str, Any] = {}
    row["Técnico"] = s_.get("plantilla_tecnico", "")
    row["Tipo de supervisión"] = s_.get("tipo", "")
    row["Código de pedido"] = s_.get("codigo", "")
    row["Contrata"] = s_.get("plantilla_contrata", "")
    row["Fecha"] = now_peru_str()
    row["Distrito"] = s_.get("plantilla_distrito", "")
    row["Supervisor"] = s_.get("supervisor", "")
    row["Gestor"] = s_.get("plantilla_gestor", "")
    row["Resultado"] = ""  # manual
    row["Correo"] = ""     # manual

    m = map_obs_columns()
    for bucket, data in s_.get("cableado", {}).items():
        col = m.get(("cableado", bucket))
        if col:
            row[col] = data.get("obs", "")

    for bucket, data in s_.get("cuadrilla", {}).items():
        col = m.get(("cuadrilla", bucket))
        if col:
            row[col] = data.get("obs", "")

    row["Observaciones ADICIONALES"] = s_.get("opcionales", {}).get("obs", "")
    row["Observaciones FINALES"] = s_.get("final_text", "")

    # Nota: si tu hoja no tiene PlantillaUUID, no pasa nada (se ignorará porque el header manda).
    row["PlantillaUUID"] = s_.get("plantilla_uuid", "")

    return row

async def on_final_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    s_ = sess(context)
    s_["final_text"] = (update.message.text or "").strip()

    origin_chat_id = s_.get("origin_chat_id")
    if origin_chat_id is None:
        await send_message(update, context, "❌ No se detectó el grupo de origen. Inicia con /inicio en el grupo AUDITORIAS.")
        cleanup_session_temp_files(s_)
        context.user_data.pop("s", None)
        return ConversationHandler.END

    dest_evidencias_id = GROUP_CFG.get("links", {}).get(str(origin_chat_id))
    if not dest_evidencias_id:
        await send_message(
            update,
            context,
            "⚠️ Este grupo AUDITORIAS no está enlazado a un grupo Evidencias.\n\n"
            "Configura así:\n"
            "1) En el grupo Evidencias ejecuta /set_evidencias_<nombre>\n"
            "2) En ESTE grupo AUDITORIAS ejecuta /link_<nombre>\n"
            "3) Verifica con /ver_links",
        )
        cleanup_session_temp_files(s_)
        context.user_data.pop("s", None)
        return ConversationHandler.END

    summary = build_summary(s_)
    await context.application.bot.send_message(chat_id=dest_evidencias_id, text=summary)

    await send_media_section(context.application, dest_evidencias_id, "🧱 FACHADA", s_["fachada"]["media"])

    for bucket, data in s_["cableado"].items():
        title = f"🏗️ CABLEADO - {bucket}"
        if data.get("obs"):
            title += f"\n📝 Obs: {data['obs']}"
        await send_media_section(context.application, dest_evidencias_id, title, data.get("media", []))

    for bucket, data in s_["cuadrilla"].items():
        title = f"👷‍♂️ CUADRILLA - {bucket}"
        if data.get("obs"):
            title += f"\n📝 Obs: {data['obs']}"
        await send_media_section(context.application, dest_evidencias_id, title, data.get("media", []))

    opc = s_["opcionales"]
    if opc.get("media"):
        title = "🚨 OPCIONALES"
        if opc.get("obs"):
            title += f"\n📝 Obs: {opc['obs']}"
        await send_media_section(context.application, dest_evidencias_id, title, opc["media"])

    # ✅ CAMBIO: logs claros para ver si realmente intenta y si falla
    if _gs_ready():
        try:
            payload = build_supervisiones_row(s_)
            logging.info(f"🟦 Intentando guardar en '{SHEET_TAB_SUPERVISIONES}' con codigo={s_.get('codigo')}")
            gs_append_dict(SHEET_TAB_SUPERVISIONES, payload)
            logging.info("✅ Guardado en Sheets OK (Supervisiones).")
        except Exception as e:
            logging.exception("❌ Error guardando supervisión en Sheets")
            await send_message(update, context, f"⚠️ Supervisión enviada a Evidencias, pero NO pude guardar en Sheets.\nDetalle: {e}")

    await send_message(update, context, f"✅ SE FINALIZÓ SUPERVISIÓN DE CÓDIGO {s_['codigo']}\n📤 Enviado a Evidencias y registrado en Sheets.")
    cleanup_session_temp_files(s_)
    context.user_data.pop("s", None)
    return ConversationHandler.END

# =========================
# Cancelar supervisión
# =========================
async def cancelar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    s_ = context.user_data.get("s")
    if s_:
        cleanup_session_temp_files(s_)
    context.user_data.pop("s", None)
    await send_message(update, context, "❌ Proceso cancelado. Puedes usar /inicio cuando quieras.", reply_markup=ReplyKeyboardRemove())
    return ConversationHandler.END

# =========================
# Error handler
# =========================
async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    logging.exception("Unhandled error:", exc_info=context.error)

def main():
    load_cfg()

    if not BOT_TOKEN:
        raise SystemExit("Configura BOT_TOKEN como variable de entorno en Railway o en tu entorno local.")

    app = Application.builder().token(BOT_TOKEN).build()
    app.add_error_handler(on_error)

    # ---- comandos config links
    app.add_handler(CommandHandler("set_evidencias_rafael", set_evidencias_rafael))
    app.add_handler(CommandHandler("set_evidencias_edgar", set_evidencias_edgar))
    app.add_handler(CommandHandler("set_evidencias_harnol", set_evidencias_harnol))
    app.add_handler(CommandHandler("set_evidencias_nelson", set_evidencias_nelson))
    app.add_handler(CommandHandler("set_evidencias_pruebas", set_evidencias_pruebas))

    app.add_handler(CommandHandler("link_rafael", link_rafael))
    app.add_handler(CommandHandler("link_edgar", link_edgar))
    app.add_handler(CommandHandler("link_harnol", link_harnol))
    app.add_handler(CommandHandler("link_nelson", link_nelson))
    app.add_handler(CommandHandler("link_pruebas", link_pruebas))
    app.add_handler(CommandHandler("ver_links", ver_links))

    # ---- comandos sheets/plantillas
    app.add_handler(CommandHandler("plantilla", cmd_plantilla))
    app.add_handler(CommandHandler("cancelar_plantilla", cmd_cancelar_plantilla))
    app.add_handler(CommandHandler("reload_sheet", cmd_reload_sheet))

    media_filter = (
        filters.PHOTO
        | filters.VIDEO
        | filters.Document.MimeType("video/mp4")
        | filters.Document.MimeType("video/quicktime")
        | filters.Document.MimeType("video/x-matroska")
        | filters.Document.MimeType("video/webm")
        | filters.Document.MimeType("video/*")
    )

    conv = ConversationHandler(
        entry_points=[CommandHandler("inicio", inicio)],
        per_chat=True,
        per_user=True,
        states={
            S_SUPERVISOR: [CallbackQueryHandler(on_pick_supervisor, pattern=r"^SUP_\d+$")],
            S_OPERADOR: [CallbackQueryHandler(on_pick_operador, pattern=r"^OP_\d+$")],
            S_CODIGO: [MessageHandler(filters.TEXT & ~filters.COMMAND, on_codigo)],
            S_TIPO: [CallbackQueryHandler(on_pick_tipo, pattern=r"^TIPO_")],
            S_UBICACION: [MessageHandler(filters.LOCATION, on_location)],

            S_FACHADA_MEDIA: [
                MessageHandler(media_filter, on_media),
                CallbackQueryHandler(on_add_more_or_done, pattern=r"^(ADD_MORE|DONE_MEDIA)$"),
            ],

            S_MENU_PRINCIPAL: [
                CallbackQueryHandler(on_main_menu, pattern=r"^(MENU_.*|FINALIZAR)$")
            ],

            S_MENU_CABLEADO: [CallbackQueryHandler(on_menu_cableado, pattern=CABLEADO_PATTERN)],
            S_MENU_CUADRILLA: [CallbackQueryHandler(on_menu_cuadrilla, pattern=CUADRILLA_PATTERN)],

            S_CARGA_MEDIA_BUCKET: [
                MessageHandler(media_filter, on_media),
                CallbackQueryHandler(on_add_more_or_done, pattern=r"^(ADD_MORE|DONE_MEDIA)$"),
            ],

            S_ASK_OBS: [CallbackQueryHandler(on_obs_choice, pattern=r"^OBS_")],
            S_WRITE_OBS: [MessageHandler(filters.TEXT & ~filters.COMMAND, on_write_obs)],

            S_FINAL_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, on_final_text)],
        },
        fallbacks=[CommandHandler("cancelar", cancelar)],
        allow_reentry=True,
    )

    # 0) Primero el ConversationHandler
    app.add_handler(conv, group=0)

    # 1) Rescate de código (si el ConversationHandler no lo toma)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, codigo_global), group=1)

    # 2) Captura de plantilla (va después del rescate)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, auto_capture_plantilla), group=2)

    logging.info("✅ Bot iniciado. Polling...")

    # =========================
    # RUN POLLING PROTEGIDO
    # =========================
    try:
        # close_loop=False lo mantengo como lo tenías para evitar cambios de comportamiento.
        app.run_polling(close_loop=False, drop_pending_updates=True)
    except Exception as e:
        logging.critical("FATAL ERROR: el bot se detuvo por una excepción no manejada.", exc_info=e)
        raise

if __name__ == "__main__":
    main()

