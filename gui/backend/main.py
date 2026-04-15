from fastapi import FastAPI, BackgroundTasks, HTTPException, UploadFile, File, Header, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Any, Dict, List, Optional
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import json
import requests
import sys
import os
import concurrent.futures
import inspect
import threading
import time
import uuid
import socket
import re
import tkinter as tk
from tkinter import filedialog
from copy import deepcopy
from datetime import datetime, timezone
from urllib.parse import unquote, urlparse

# Force local directory priority to use the latest modified functions
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, PROJECT_ROOT)
import tiktokautouploader.function as uploader_runtime
from tiktokautouploader.function import stop_all_uploads, reset_stop_signal

app = FastAPI()

DEFAULT_ACCOUNTS_DB = {
    "Admin": [],
    "Patrick": ["VelmoraSkin", "Novacique", "TrivexaCore"],
    "Asis": ["Kairothix", "Zyrelune", "OrphicDerma"],
    "Ace": ["NuvyraCell", "CalystrixLab", "ElyndorGlow", "MyravaLabs"],
    "Benser": ["jbnsrr.xx"],
}

ADMIN_OPERATOR = "admin"

ACCOUNTS_DB_PATH = os.path.join(os.path.dirname(__file__), "accounts_db.json")
DROPPED_UPLOADS_DIR = os.path.join(os.path.dirname(__file__), "uploaded_videos")
os.makedirs(DROPPED_UPLOADS_DIR, exist_ok=True)

NODE_ID = (os.getenv("NODE_ID", socket.gethostname()) or socket.gethostname()).strip()
SYNC_BASE_URL = (os.getenv("SYNC_BASE_URL", "").strip() or os.getenv("CENTRAL_BASE_URL", "").strip()).rstrip("/")
SYNC_TOKEN = os.getenv("SYNC_TOKEN", "change-me-sync-token").strip()
SYNC_TIMEOUT_SECONDS = max(2.0, float(os.getenv("SYNC_TIMEOUT_SECONDS", "12")))
SYNC_LOG_WARNING_COOLDOWN_SECONDS = 12

CENTRAL_SYNC_ENABLED = bool(SYNC_BASE_URL)
last_sync_warning = {"message": "", "at": 0.0}

# Enable CORS for frontend development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

class VideoItem(BaseModel):
    video: str
    desc: str
    hashtags: Optional[List[str]] = None
    schedule: Optional[str] = None
    day: Optional[int] = None
    sound_name: Optional[str] = None
    sound_aud_vol: Optional[str] = "mix"

class UploadConfig(BaseModel):
    videos: List[VideoItem]
    accountname: str
    operator: str = ""
    hashtags: List[str]
    headless: bool = False
    stealth: bool = True
    schedule: Optional[str] = None
    day: Optional[int] = None
    sound_name: str = "random"
    sound_aud_vol: str = "mix"
    search_mode: str = "favorites"


class NamePayload(BaseModel):
    name: str

# AI Generation Logic
LONGCAT_API_KEY = "ak_2qp2h31Vm2LB3AX3XH5Hb3Ve9ZJ1Y"
LONGCAT_URL = "https://api.longcat.chat/openai/v1/chat/completions"

def generate_ai_caption(prompt_type="haircare"):
    headers = {
        "Authorization": f"Bearer {LONGCAT_API_KEY}",
        "Content-Type": "application/json"
    }
    
    prompt = "Write a short, engaging Taglish TikTok caption for Maikalian hair products. Focus on smoothness and mini treatment results. DO NOT include hashtags."
    
    data = {
        "model": "LongCat-Flash-Lite",
        "messages": [
            {"role": "system", "content": "You are a professional TikTok creator for Maikalian. Output ONLY text and emojis. NO hashtags."},
            {"role": "user", "content": prompt}
        ],
        "max_tokens": 100,
        "temperature": 0.7
    }
    try:
        response = requests.post(LONGCAT_URL, headers=headers, json=data, timeout=120)
        response.raise_for_status()
        result = response.json()
        return result['choices'][0]['message']['content']
    except Exception as e:
        return f"Error generating caption: {str(e)}"


def _normalize_accounts_db(raw_data):
    normalized = {}
    for operator, accounts in (raw_data or {}).items():
        operator_name = str(operator).strip()
        if not operator_name:
            continue

        clean_accounts = []
        for account in accounts if isinstance(accounts, list) else []:
            account_name = str(account).strip().lstrip("@")
            if account_name and account_name not in clean_accounts:
                clean_accounts.append(account_name)

        normalized[operator_name] = clean_accounts

    if "Admin" not in normalized:
        normalized["Admin"] = []

    return normalized


def _require_admin(x_operator: Optional[str]):
    actor = (x_operator or "").strip().lower()
    if actor != ADMIN_OPERATOR:
        raise HTTPException(status_code=403, detail="Admin access required for this action.")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sync_headers(extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    headers = {"X-Sync-Token": SYNC_TOKEN}
    if extra:
        headers.update(extra)
    return headers


def _log_sync_warning(message: str):
    now = time.time()
    recent = last_sync_warning
    if (
        recent.get("message") == message
        and (now - float(recent.get("at", 0))) < SYNC_LOG_WARNING_COOLDOWN_SECONDS
    ):
        return
    recent["message"] = message
    recent["at"] = now
    _append_log(f"[SYNC] {message}", severity="error", emit_telemetry=False)


def _sync_request(
    method: str,
    path: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    payload: Optional[Dict[str, Any]] = None,
    timeout: Optional[float] = None,
):
    if not CENTRAL_SYNC_ENABLED:
        raise RuntimeError("Central sync is not configured on this node (SYNC_BASE_URL missing).")

    url = f"{SYNC_BASE_URL}{path}"
    try:
        response = requests.request(
            method=method.upper(),
            url=url,
            headers=_sync_headers({"Content-Type": "application/json"}),
            params=params,
            json=payload,
            timeout=timeout or SYNC_TIMEOUT_SECONDS,
        )
    except Exception as exc:
        raise RuntimeError(f"Central sync request failed ({url}): {exc}") from exc

    content_type = str(response.headers.get("content-type") or "").lower()
    if "application/json" in content_type:
        data = response.json()
    else:
        data = {"raw": response.text}
    return response.status_code, data


def _sync_request_or_http_error(
    method: str,
    path: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    payload: Optional[Dict[str, Any]] = None,
    expected_statuses: Optional[List[int]] = None,
    detail_on_fail: str = "Central sync request failed.",
):
    expected = expected_statuses or [200]
    try:
        status_code, data = _sync_request(method, path, params=params, payload=payload)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"{detail_on_fail} {exc}") from exc

    if status_code not in expected:
        detail = data.get("detail") if isinstance(data, dict) else None
        raise HTTPException(status_code=status_code, detail=detail or detail_on_fail)

    return data


def load_accounts_db():
    if CENTRAL_SYNC_ENABLED:
        try:
            status_code, data = _sync_request("GET", "/sync/operators")
            if status_code == 200 and isinstance(data, dict):
                operators = _normalize_accounts_db(data.get("operators", {}))
                if operators:
                    return operators
            detail = data.get("detail") if isinstance(data, dict) else "Unexpected response."
            raise RuntimeError(f"HTTP {status_code}: {detail}")
        except Exception as exc:
            _log_sync_warning(f"Unable to fetch operators from central: {exc}")
            raise HTTPException(
                status_code=503,
                detail="Central operator service is unavailable. Verify SYNC_BASE_URL and central server health.",
            ) from exc

    if not os.path.exists(ACCOUNTS_DB_PATH):
        save_accounts_db(DEFAULT_ACCOUNTS_DB)
        return deepcopy(DEFAULT_ACCOUNTS_DB)

    try:
        with open(ACCOUNTS_DB_PATH, "r", encoding="utf-8") as db_file:
            data = json.load(db_file)
    except Exception:
        save_accounts_db(DEFAULT_ACCOUNTS_DB)
        return deepcopy(DEFAULT_ACCOUNTS_DB)

    normalized = _normalize_accounts_db(data)
    if not normalized:
        save_accounts_db(DEFAULT_ACCOUNTS_DB)
        return deepcopy(DEFAULT_ACCOUNTS_DB)

    return normalized


def save_accounts_db(data):
    normalized = _normalize_accounts_db(data)
    if CENTRAL_SYNC_ENABLED:
        _sync_request_or_http_error(
            "PUT",
            "/sync/operators",
            payload={"operators": normalized},
            expected_statuses=[200],
            detail_on_fail="Failed to update operators in central sync service.",
        )
        return

    with open(ACCOUNTS_DB_PATH, "w", encoding="utf-8") as db_file:
        json.dump(normalized, db_file, indent=2)


def add_operator_to_db(name: str):
    operator_name = name.strip()
    if not operator_name:
        raise HTTPException(status_code=400, detail="Operator name is required.")

    data = load_accounts_db()
    if operator_name in data:
        raise HTTPException(status_code=409, detail="Operator already exists.")

    data[operator_name] = []
    save_accounts_db(data)
    return data


def add_account_to_operator(operator: str, account_name: str):
    clean_operator = operator.strip()
    clean_account = account_name.strip().lstrip("@")

    if not clean_operator:
        raise HTTPException(status_code=400, detail="Operator is required.")
    if not clean_account:
        raise HTTPException(status_code=400, detail="Account name is required.")

    data = load_accounts_db()
    if clean_operator not in data:
        raise HTTPException(status_code=404, detail="Operator not found.")
    if clean_account in data[clean_operator]:
        raise HTTPException(status_code=409, detail="Account already exists for this operator.")

    data[clean_operator].append(clean_account)
    save_accounts_db(data)
    return data


def remove_account_from_operator(operator: str, account_name: str):
    clean_operator = operator.strip()
    clean_account = account_name.strip().lstrip("@")

    if not clean_operator:
        raise HTTPException(status_code=400, detail="Operator is required.")
    if not clean_account:
        raise HTTPException(status_code=400, detail="Account name is required.")

    data = load_accounts_db()
    if clean_operator not in data:
        raise HTTPException(status_code=404, detail="Operator not found.")
    if clean_account not in data[clean_operator]:
        raise HTTPException(status_code=404, detail="Account not found for this operator.")

    data[clean_operator].remove(clean_account)
    save_accounts_db(data)
    return data


def authenticate_account(account_name: str):
    clean_account = account_name.strip().lstrip("@")
    if not clean_account:
        raise HTTPException(status_code=400, detail="Account name is required.")

    cookie_path = uploader_runtime._cookie_file(clean_account)
    cookie_exists_before = os.path.exists(cookie_path)

    try:
        cookies = uploader_runtime._load_or_create_cookies(clean_account, proxy=None)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Authentication failed: {e}")

    return {
        "account": clean_account,
        "cookie_path": cookie_path,
        "cookie_exists_before": cookie_exists_before,
        "cookie_exists_after": os.path.exists(cookie_path),
        "cookie_count": len(cookies) if isinstance(cookies, list) else 0,
    }


def _read_cookie_file(account_name: str) -> List[Dict[str, Any]]:
    cookie_path = uploader_runtime._cookie_file(account_name)
    if not os.path.exists(cookie_path):
        return []
    try:
        with open(cookie_path, "r", encoding="utf-8") as cookie_file:
            payload = json.load(cookie_file)
            return payload if isinstance(payload, list) else []
    except Exception:
        return []


def _write_cookie_file(account_name: str, cookies: List[Dict[str, Any]]):
    cookie_path = uploader_runtime._cookie_file(account_name)
    cookie_dir = os.path.dirname(cookie_path)
    if cookie_dir:
        os.makedirs(cookie_dir, exist_ok=True)
    with open(cookie_path, "w", encoding="utf-8") as cookie_file:
        json.dump(cookies, cookie_file, indent=2)


def _push_shared_cookie_to_central(account_name: str, cookies: List[Dict[str, Any]], operator: str = ""):
    if not CENTRAL_SYNC_ENABLED:
        return
    payload = {
        "cookies": cookies,
        "source_operator": (operator or "").strip(),
        "node_id": NODE_ID,
        "updated_at": _utc_now_iso(),
    }
    _sync_request_or_http_error(
        "PUT",
        f"/sync/cookies/{account_name}",
        payload=payload,
        expected_statuses=[200],
        detail_on_fail=f"Failed to sync cookies for @{account_name} to central service.",
    )


def _fetch_shared_cookie_from_central(account_name: str) -> List[Dict[str, Any]]:
    if not CENTRAL_SYNC_ENABLED:
        raise HTTPException(
            status_code=503,
            detail="Shared-cookie mode is enabled by product policy, but this node has no SYNC_BASE_URL configured.",
        )

    data = _sync_request_or_http_error(
        "GET",
        f"/sync/cookies/{account_name}",
        expected_statuses=[200],
        detail_on_fail=f"Failed to fetch shared cookie for @{account_name} from central service.",
    )
    cookies = data.get("cookies") if isinstance(data, dict) else None
    if not isinstance(cookies, list) or not cookies:
        raise HTTPException(
            status_code=400,
            detail=f"Shared cookie for @{account_name} is missing or invalid in central service.",
        )
    return cookies


def _ensure_shared_cookie_for_upload(account_name: str, operator: str = "") -> Dict[str, Any]:
    clean_account = account_name.strip().lstrip("@")
    if not clean_account:
        raise HTTPException(status_code=400, detail="Account name is required.")

    cookies = _fetch_shared_cookie_from_central(clean_account)
    _write_cookie_file(clean_account, cookies)
    return {
        "account": clean_account,
        "cookie_count": len(cookies),
        "source": "central-sync",
        "operator": (operator or "").strip(),
    }

@app.get("/generate-caption")
async def get_ai_caption():
    caption = generate_ai_caption()
    return {"caption": caption}


@app.get("/node-config")
async def get_node_config():
    return {
        "node_id": NODE_ID,
        "sync_enabled": CENTRAL_SYNC_ENABLED,
        "sync_base_url": SYNC_BASE_URL if CENTRAL_SYNC_ENABLED else "",
        "sync_mode": "strict-cookie-required",
        "uploads_allowed": CENTRAL_SYNC_ENABLED,
        "uploads_blocked_reason": "" if CENTRAL_SYNC_ENABLED else "Set SYNC_BASE_URL to central server before launching uploads.",
    }


@app.get("/operators")
async def get_operators():
    return {"operators": load_accounts_db()}


@app.post("/operators")
async def create_operator(payload: NamePayload, x_operator: Optional[str] = Header(default=None, alias="X-Operator")):
    _require_admin(x_operator)
    data = add_operator_to_db(payload.name)
    return {"operators": data}


@app.post("/operators/{operator}/accounts")
async def create_account_for_operator(operator: str, payload: NamePayload, x_operator: Optional[str] = Header(default=None, alias="X-Operator")):
    _require_admin(x_operator)
    data = add_account_to_operator(operator, payload.name)
    return {"operators": data}


@app.delete("/operators/{operator}/accounts/{account}")
async def delete_account_for_operator(operator: str, account: str, x_operator: Optional[str] = Header(default=None, alias="X-Operator")):
    _require_admin(x_operator)
    data = remove_account_from_operator(operator, account)
    return {"operators": data}


@app.post("/operators/{operator}/accounts/{account}/authenticate")
async def authenticate_operator_account(operator: str, account: str, x_operator: Optional[str] = Header(default=None, alias="X-Operator")):
    _require_admin(x_operator)
    data = load_accounts_db()
    clean_operator = operator.strip()
    clean_account = account.strip().lstrip("@")
    if clean_operator not in data:
        raise HTTPException(status_code=404, detail="Operator not found.")
    if clean_account not in data[clean_operator]:
        raise HTTPException(status_code=404, detail="Account not found for this operator.")

    auth = authenticate_account(clean_account)
    cookies = _read_cookie_file(clean_account)
    if CENTRAL_SYNC_ENABLED:
        _push_shared_cookie_to_central(clean_account, cookies, operator=clean_operator)
    return {"auth": auth}

@app.get("/pick-videos")
def pick_videos():
    try:
        root = tk.Tk()
        root.withdraw()
        # Ensure dialog is on top
        root.attributes("-topmost", True)
        file_paths = filedialog.askopenfilenames(
            title="Select Videos for Upload",
            filetypes=[("Video Files", "*.mp4 *.mov *.avi *.mkv"), ("All Files", "*.*")]
        )
        root.destroy()
        return {"paths": list(file_paths)}
    except Exception as e:
        return {"paths": [], "error": str(e)}


@app.post("/resolve-dropped-videos")
async def resolve_dropped_videos(files: List[UploadFile] = File(...)):
    if not files:
        raise HTTPException(status_code=400, detail="No files were uploaded.")

    saved_paths = []
    for index, upload in enumerate(files):
        original_name = os.path.basename((upload.filename or f"dropped_{index}.mp4").replace("\\", "/"))
        base_name, ext = os.path.splitext(original_name)
        safe_base = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in (base_name or "video")).strip("_")
        safe_base = safe_base or "video"
        safe_ext = (ext or ".mp4").lower()
        stamp = time.strftime("%Y%m%d_%H%M%S")
        unique_name = f"{stamp}_{int(time.time() * 1000) % 1000000}_{index}_{safe_base}{safe_ext}"
        output_path = os.path.abspath(os.path.join(DROPPED_UPLOADS_DIR, unique_name))

        with open(output_path, "wb") as output_file:
            while True:
                chunk = await upload.read(1024 * 1024)
                if not chunk:
                    break
                output_file.write(chunk)

        saved_paths.append(output_path)
        await upload.close()

    return {"paths": saved_paths}

@app.get("/open-file")
def open_file(path: str):
    try:
        if os.path.exists(path):
            os.startfile(path)
            return {"status": "success"}
        else:
            return {"status": "error", "message": "File not found"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# Global state for logs and in-memory local session telemetry.
execution_logs = []
is_uploading = False
progress_lock = threading.Lock()
operator_session_history = {}
telemetry_status_meta = {"last_sent_at": 0.0, "last_fingerprint": ""}
SYNC_STATUS_THROTTLE_SECONDS = max(0.35, float(os.getenv("SYNC_STATUS_THROTTLE_SECONDS", "1.0")))


def _empty_progress():
    return {
        "session_id": "",
        "node_id": NODE_ID,
        "operator": "",
        "accountname": "",
        "status": "idle",
        "total": 0,
        "completed": 0,
        "success": 0,
        "failed": 0,
        "active": 0,
        "pending": 0,
        "percent": 0,
        "started_at": None,
        "updated_at": None,
        "items": [],
    }


current_progress = _empty_progress()


def _timestamp():
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _dispatch_async(fn, *args, **kwargs):
    worker = threading.Thread(target=fn, args=args, kwargs=kwargs, daemon=True)
    worker.start()


def _sanitize_telemetry_message(message: str) -> str:
    text = str(message or "")
    if not text:
        return ""

    def _replace_path(match: re.Match) -> str:
        raw = match.group(0).strip().strip('"').strip("'")
        suffix = ""
        while raw and raw[-1] in ".,;:!?)]}":
            suffix = raw[-1] + suffix
            raw = raw[:-1]
        basename = os.path.basename(raw.replace("/", "\\").rstrip("\\")) or "<file>"
        return f"{basename}{suffix}"

    text = re.sub(r"[A-Za-z]:\\[^\s\"']+", _replace_path, text)
    text = re.sub(r"(?:^|(?<=\s))/[^\s\"']+", _replace_path, text)
    return text


def _snapshot_state():
    with progress_lock:
        return list(execution_logs), is_uploading, deepcopy(current_progress)


def _post_telemetry(endpoint: str, payload: Dict[str, Any]):
    if not CENTRAL_SYNC_ENABLED:
        return
    try:
        status_code, data = _sync_request("POST", endpoint, payload=payload, timeout=6)
        if status_code != 200:
            detail = data.get("detail") if isinstance(data, dict) else f"HTTP {status_code}"
            _log_sync_warning(f"Telemetry endpoint {endpoint} rejected event: {detail}")
    except Exception as exc:
        _log_sync_warning(f"Telemetry endpoint {endpoint} unavailable: {exc}")


def _telemetry_payload(
    event_type: str,
    severity: str,
    message: str,
    progress: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    snapshot = progress if progress is not None else _snapshot_state()[2]
    return {
        "node_id": NODE_ID,
        "operator": str(snapshot.get("operator") or "").strip(),
        "account": str(snapshot.get("accountname") or "").strip(),
        "session_id": str(snapshot.get("session_id") or "").strip(),
        "event_type": str(event_type or "status").strip().lower(),
        "severity": str(severity or "info").strip().lower(),
        "message": _sanitize_telemetry_message(message),
        "percent": int(snapshot.get("percent") or 0),
        "completed": int(snapshot.get("completed") or 0),
        "failed": int(snapshot.get("failed") or 0),
        "pending": int(snapshot.get("pending") or 0),
        "total": int(snapshot.get("total") or 0),
        "success": int(snapshot.get("success") or 0),
        "active": int(snapshot.get("active") or 0),
        "status": str(snapshot.get("status") or "").strip().lower(),
        "timestamp": _timestamp(),
    }


def _emit_telemetry_log(message: str, severity: str = "info"):
    if not CENTRAL_SYNC_ENABLED:
        return
    payload = _telemetry_payload("log", severity, message)
    _dispatch_async(_post_telemetry, "/telemetry/logs", payload)


def _emit_telemetry_status(
    event_type: str = "status",
    *,
    force: bool = False,
    message: str = "",
    severity: str = "info",
    progress: Optional[Dict[str, Any]] = None,
):
    if not CENTRAL_SYNC_ENABLED:
        return

    snapshot = progress if progress is not None else _snapshot_state()[2]
    payload = _telemetry_payload(event_type, severity, message, progress=snapshot)
    fingerprint = "|".join(
        [
            str(payload.get("session_id") or ""),
            str(payload.get("status") or ""),
            str(payload.get("completed") or 0),
            str(payload.get("failed") or 0),
            str(payload.get("pending") or 0),
            str(payload.get("percent") or 0),
            str(payload.get("event_type") or ""),
        ]
    )

    now = time.time()
    with progress_lock:
        should_skip = (
            not force
            and telemetry_status_meta["last_fingerprint"] == fingerprint
            and (now - telemetry_status_meta["last_sent_at"]) < SYNC_STATUS_THROTTLE_SECONDS
        )
        if should_skip:
            return
        telemetry_status_meta["last_fingerprint"] = fingerprint
        telemetry_status_meta["last_sent_at"] = now

    _dispatch_async(_post_telemetry, "/telemetry/status", payload)


def _append_log(message: str, *, severity: str = "info", emit_telemetry: bool = True):
    with progress_lock:
        execution_logs.append(message)
    if emit_telemetry:
        _emit_telemetry_log(message, severity=severity)


def _recalculate_progress_locked():
    items = current_progress["items"]
    total = current_progress["total"]

    current_progress["success"] = sum(1 for item in items if item["status"] == "SUCCESS")
    current_progress["failed"] = sum(1 for item in items if item["status"] in {"FAILED", "STOPPED"})
    current_progress["active"] = sum(1 for item in items if item["status"] == "RUNNING")
    current_progress["completed"] = sum(1 for item in items if item["status"] in {"SUCCESS", "FAILED", "STOPPED"})
    current_progress["pending"] = max(total - current_progress["completed"] - current_progress["active"], 0)
    current_progress["percent"] = round(sum(item["percent"] for item in items) / total) if total else 0
    current_progress["updated_at"] = _timestamp()


def _initialize_progress(config: UploadConfig):
    global current_progress

    session_id = uuid.uuid4().hex
    items = []
    for idx, video in enumerate(config.videos, start=1):
        items.append(
            {
                "id": idx,
                "name": os.path.basename(video.video),
                "status": "QUEUED",
                "percent": 0,
                "message": "Waiting for browser slot",
            }
        )

    with progress_lock:
        current_progress = {
            "session_id": session_id,
            "node_id": NODE_ID,
            "operator": (config.operator or "").strip(),
            "accountname": config.accountname,
            "status": "running",
            "total": len(config.videos),
            "completed": 0,
            "success": 0,
            "failed": 0,
            "active": 0,
            "pending": len(config.videos),
            "percent": 0,
            "started_at": _timestamp(),
            "updated_at": _timestamp(),
            "items": items,
        }
        _recalculate_progress_locked()

    _emit_telemetry_status("session_started", force=True, message=f"Session started for @{config.accountname}")


def _progress_checkpoint(message: str):
    message_lower = message.lower()
    checkpoints = [
        ("loading cookies for account", 8),
        ("uploading to account", 12),
        ("captcha detected", 18),
        ("captcha solved", 28),
        ("description and hashtags added", 40),
        ("tik tok done loading file onto servers", 52),
        ("done scheduling video", 64),
        ("added sound", 76),
        ("copyright check complete", 88),
        ("confirming upload via 'post now' modal", 94),
        ("success detected via", 97),
        ("done uploading video", 100),
    ]

    for phrase, percent in checkpoints:
        if phrase in message_lower:
            return percent

    return None


def _update_progress_item(item_index: int, status=None, percent=None, message=None):
    updated = False
    with progress_lock:
        if not (1 <= item_index <= len(current_progress["items"])):
            return

        item = current_progress["items"][item_index - 1]
        if status is not None:
            item["status"] = status
        if percent is not None:
            item["percent"] = max(item["percent"], min(percent, 100))
        if message is not None:
            item["message"] = message

        _recalculate_progress_locked()
        updated = True

    if updated:
        _emit_telemetry_status("status", message=message or "", force=False)


def _apply_progress_message(item_index: int, message: str):
    updated = False
    with progress_lock:
        if not (1 <= item_index <= len(current_progress["items"])):
            return

        item = current_progress["items"][item_index - 1]
        if item["status"] == "QUEUED":
            item["status"] = "RUNNING"

        checkpoint = _progress_checkpoint(message)
        if checkpoint is not None:
            item["percent"] = max(item["percent"], checkpoint)

        item["message"] = message
        _recalculate_progress_locked()
        updated = True

    if updated:
        _emit_telemetry_status("status", message=message, force=False)


def _finalize_progress(stopped=False):
    final_status = "finished"
    with progress_lock:
        for item in current_progress["items"]:
            if stopped and item["status"] == "QUEUED":
                item["status"] = "STOPPED"
                item["message"] = "Stopped before upload started"

        if stopped:
            current_progress["status"] = "stopped"
        elif current_progress["failed"] and not current_progress["success"]:
            current_progress["status"] = "failed"
        else:
            current_progress["status"] = "finished"

        final_status = current_progress["status"]
        _recalculate_progress_locked()

    event_type = {
        "stopped": "session_stopped",
        "failed": "session_failed",
        "finished": "session_finished",
    }.get(final_status, "session_finished")
    severity = "error" if final_status == "failed" else "info"
    _emit_telemetry_status(event_type, force=True, severity=severity, message=f"Session {final_status}")


def _record_operator_session(operator_name: str):
    clean_operator = (operator_name or "").strip()
    if not clean_operator:
        return

    with progress_lock:
        snapshot = deepcopy(current_progress)
        entry = {
            "session_id": snapshot.get("session_id", ""),
            "node_id": snapshot.get("node_id", NODE_ID),
            "operator": clean_operator,
            "accountname": snapshot.get("accountname", ""),
            "status": snapshot.get("status", "idle"),
            "total": snapshot.get("total", 0),
            "completed": snapshot.get("completed", 0),
            "success": snapshot.get("success", 0),
            "failed": snapshot.get("failed", 0),
            "pending": snapshot.get("pending", 0),
            "percent": snapshot.get("percent", 0),
            "started_at": snapshot.get("started_at"),
            "updated_at": snapshot.get("updated_at"),
            "items": snapshot.get("items", []),
        }

        history = operator_session_history.setdefault(clean_operator, [])
        history.insert(0, entry)
        del history[25:]


def _resolve_sound_name(sound_name: Optional[str]) -> Optional[str]:
    if sound_name is None:
        return None

    normalized = sound_name.strip()
    if not normalized or normalized.lower() == "none":
        return None

    return normalized


def _file_url_to_path(raw_path: str) -> str:
    value = str(raw_path or "").strip()
    if not value.lower().startswith("file://"):
        return value

    parsed = urlparse(value)
    decoded_path = unquote(parsed.path or "")

    # Windows local file URL: file:///C:/path/to/video.mp4
    if decoded_path.startswith("/") and len(decoded_path) > 2 and decoded_path[2] == ":":
        decoded_path = decoded_path[1:]

    if parsed.netloc:
        # UNC path: file://server/share/file.mp4
        return f"\\\\{parsed.netloc}{decoded_path.replace('/', '\\')}"

    return decoded_path.replace("/", "\\")


def _resolve_video_path(raw_path: str) -> str:
    cleaned = _file_url_to_path(str(raw_path or "").strip().strip('"').strip("'"))
    if not cleaned:
        return ""

    if os.path.isabs(cleaned):
        return os.path.abspath(cleaned)

    # Allow relative paths rooted at current working directory.
    if os.path.exists(cleaned):
        return os.path.abspath(cleaned)

    # Also support project-root-relative input.
    project_candidate = os.path.abspath(os.path.join(PROJECT_ROOT, cleaned))
    if os.path.exists(project_candidate):
        return project_candidate

    return os.path.abspath(cleaned)


def _build_upload_job(video_item: VideoItem, config: UploadConfig) -> dict:
    schedule_time = video_item.schedule if video_item.schedule is not None else config.schedule
    schedule_day = video_item.day if video_item.day is not None else config.day

    if not schedule_time or schedule_day is None:
        schedule_time = None
        schedule_day = None

    sound_name = video_item.sound_name if video_item.sound_name is not None else config.sound_name

    return {
        "video": _resolve_video_path(video_item.video),
        "description": video_item.desc,
        "accountname": config.accountname,
        "hashtags": video_item.hashtags if video_item.hashtags else config.hashtags,
        "headless": config.headless,
        "tile_windows": False,
        "window_index": None,
        "window_count": None,
        "stealth": config.stealth,
        "schedule": schedule_time,
        "day": schedule_day,
        "sound_name": _resolve_sound_name(sound_name),
        "sound_aud_vol": video_item.sound_aud_vol or config.sound_aud_vol,
        "search_mode": config.search_mode,
        "suppressprint": False,
        "copyrightcheck": False,
    }


def _run_single_upload(job_index: int, total_jobs: int, job: dict, log_callback):
    label = os.path.basename(job["video"])
    if uploader_runtime.FORCE_STOP:
        _update_progress_item(job_index, status="STOPPED", message="Stopped before upload started")
        return f"STOPPED: {label}"

    _update_progress_item(job_index, status="RUNNING", percent=6, message="Launching browser")
    mode_label = "AUTO-GRID" if job.get("tile_windows") else "NORMAL"
    log_callback(f"Starting browser {job_index}/{total_jobs}: {label} [{mode_label}]")

    def upload_logger(message: str):
        log_callback(f"[{label}] {message}")
        _apply_progress_message(job_index, message)

    try:
        # Always resolve the callable from runtime module to avoid stale imported
        # symbols when the uploader file changes during rapid iterations.
        upload_fn = uploader_runtime.upload_tiktok
        call_kwargs = dict(job)
        upload_params = inspect.signature(upload_fn).parameters
        supports_log_callback = "log_callback" in upload_params
        call_kwargs = {k: v for k, v in job.items() if k in upload_params}

        if supports_log_callback:
            upload_fn(**call_kwargs, log_callback=upload_logger)
        else:
            log_callback(f"[{label}] Detailed step logs unavailable in this uploader build. Continuing with basic progress.")
            upload_fn(**call_kwargs)

        _update_progress_item(job_index, status="SUCCESS", percent=100, message="Upload completed")
        return f"SUCCESS: {label}"
    except TypeError as e:
        # Safe fallback when the running process still has an older upload_tiktok
        # object cached without the new callback parameter.
        if "unexpected keyword argument 'log_callback'" in str(e):
            log_callback(f"[{label}] Older uploader detected in memory. Retrying without step callback.")
            upload_fn(**call_kwargs)
            _update_progress_item(job_index, status="SUCCESS", percent=100, message="Upload completed")
            return f"SUCCESS: {label}"

        _update_progress_item(job_index, status="FAILED", percent=100, message=str(e))
        return f"FAILED: {label} | Error: {str(e)}"
    except Exception as e:
        _update_progress_item(job_index, status="FAILED", percent=100, message=str(e))
        return f"FAILED: {label} | Error: {str(e)}"

def run_uploads(config: UploadConfig):
    global is_uploading
    reset_stop_signal()
    with progress_lock:
        is_uploading = True
    _initialize_progress(config)
    operator_label = (config.operator or "Unknown").strip() or "Unknown"

    def op_log(message: str):
        upper = str(message or "").upper()
        severity = "error" if ("FAILED" in upper or "FATAL" in upper or "ERROR" in upper) else "info"
        _append_log(f"[{operator_label}] {message}", severity=severity)

    # Product rule: Headless toggle ON means visual auto-grid mode.
    headless_toggle = bool(config.headless)
    effective_headless = False if headless_toggle else config.headless
    tile_windows = headless_toggle
    op_log(
        f"DISPLAY MODE: HeadlessToggle={config.headless}, "
        f"EffectiveHeadless={effective_headless}, TileWindows={tile_windows}"
    )
    op_log(f"INITIALIZING: {len(config.videos)} videos for @{config.accountname}")
    op_log(f"CONFIG: Headless={config.headless}, Stealth={config.stealth}, Sound='{config.sound_name}'")

    try:
        def ui_logger(msg):
            op_log(msg)

        upload_jobs = [_build_upload_job(video_item=v, config=config) for v in config.videos]
        max_workers = min(len(upload_jobs), 5) or 1
        total_jobs = len(upload_jobs)

        for idx, job in enumerate(upload_jobs, start=1):
            job["headless"] = effective_headless
            job["tile_windows"] = tile_windows
            job["window_index"] = idx - 1
            job["window_count"] = total_jobs

        op_log(f"MODE: multi_post.py parallel engine ({max_workers} browser(s))")

        futures = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            for idx, job in enumerate(upload_jobs, start=1):
                if uploader_runtime.FORCE_STOP:
                    op_log("STOPPED: Session interrupted before submitting remaining uploads.")
                    break

                futures.append(
                    executor.submit(_run_single_upload, idx, len(upload_jobs), job, ui_logger)
                )

            for future in concurrent.futures.as_completed(futures):
                op_log(future.result())
    except Exception as e:
        op_log(f"FATAL ERROR: {str(e)}")
    finally:
        with progress_lock:
            is_uploading = False
        _finalize_progress(stopped=uploader_runtime.FORCE_STOP)
        _record_operator_session(operator_label)
        op_log("SESSION ENDED: Workspace released.")

@app.post("/start-upload")
async def start_upload(config: UploadConfig, background_tasks: BackgroundTasks):
    operator_label = (config.operator or "").strip()
    if operator_label.lower() == ADMIN_OPERATOR:
        raise HTTPException(status_code=403, detail="Admin account is monitoring-only and cannot launch uploads.")
    if not operator_label:
        config.operator = "Unknown"
        operator_label = "Unknown"

    # Strict shared-cookie policy: upload is blocked when central cookie is unavailable.
    cookie_result = _ensure_shared_cookie_for_upload(config.accountname, operator=operator_label)
    _append_log(
        f"[{operator_label}] Shared cookie loaded for @{cookie_result['account']} ({cookie_result['cookie_count']} cookies).",
        severity="info",
    )

    missing_videos = []
    for index, video in enumerate(config.videos, start=1):
        original_input = video.video
        resolved_path = _resolve_video_path(video.video)
        video.video = resolved_path
        if not os.path.exists(resolved_path):
            missing_videos.append(
                {
                    "index": index,
                    "input": str(original_input or ""),
                    "resolved": resolved_path,
                }
            )

    if missing_videos:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "One or more video paths are invalid. Re-import the videos using + Add or drag/drop from the client PC.",
                "missing": missing_videos,
            },
        )

    background_tasks.add_task(run_uploads, config)
    return {"status": "started"}

@app.post("/stop-upload")
async def stop_upload():
    stop_all_uploads()
    return {"status": "stopping"}

@app.get("/logs")
async def get_logs(operator: Optional[str] = Query(default=None)):
    actor = (operator or "").strip()
    if not actor:
        return {"logs": [], "is_uploading": False, "progress": _empty_progress()}

    if actor.lower() == ADMIN_OPERATOR and CENTRAL_SYNC_ENABLED:
        try:
            logs_payload = _sync_request_or_http_error(
                "GET",
                "/admin/logs",
                params={},
                expected_statuses=[200],
                detail_on_fail="Failed to read admin logs from central service.",
            )
            overview_payload = _sync_request_or_http_error(
                "GET",
                "/admin/overview",
                expected_statuses=[200],
                detail_on_fail="Failed to read admin overview from central service.",
            )
            live_sessions = overview_payload.get("live_status", []) if isinstance(overview_payload, dict) else []
            progress = _empty_progress()
            uploading = bool(live_sessions)
            if live_sessions:
                session = live_sessions[0]
                progress.update(
                    {
                        "session_id": session.get("session_id") or "",
                        "node_id": session.get("node_id") or "",
                        "operator": session.get("operator") or "",
                        "accountname": session.get("account") or session.get("accountname") or "",
                        "status": session.get("status") or "running",
                        "total": int(session.get("total") or 0),
                        "completed": int(session.get("completed") or 0),
                        "success": int(session.get("success") or 0),
                        "failed": int(session.get("failed") or 0),
                        "active": int(session.get("active") or 0),
                        "pending": int(session.get("pending") or 0),
                        "percent": int(session.get("percent") or 0),
                        "started_at": session.get("started_at"),
                        "updated_at": session.get("timestamp") or session.get("updated_at"),
                    }
                )

            return {
                "logs": logs_payload.get("logs", []) if isinstance(logs_payload, dict) else [],
                "is_uploading": uploading,
                "progress": progress,
            }
        except HTTPException:
            # Fall back to local snapshot if central telemetry is temporarily unavailable.
            pass

    logs, uploading, progress = _snapshot_state()
    if actor.lower() != ADMIN_OPERATOR:
        tag = f"[{actor}] "
        logs = [line for line in logs if line.startswith(tag)]
        if (progress.get("operator", "").strip().lower() != actor.lower()):
            progress = _empty_progress()
            uploading = False

    return {"logs": logs, "is_uploading": uploading, "progress": progress}


@app.get("/admin/overview")
async def admin_overview(x_operator: Optional[str] = Header(default=None, alias="X-Operator")):
    _require_admin(x_operator)

    if CENTRAL_SYNC_ENABLED:
        return _sync_request_or_http_error(
            "GET",
            "/admin/overview",
            expected_statuses=[200],
            detail_on_fail="Failed to read central admin overview.",
        )

    accounts_db = load_accounts_db()
    logs, uploading, progress = _snapshot_state()

    with progress_lock:
        history_snapshot = deepcopy(operator_session_history)

    users = []
    for operator, accounts in accounts_db.items():
        if operator.strip().lower() == ADMIN_OPERATOR:
            continue

        latest = (history_snapshot.get(operator) or [None])[0]
        running_now = (
            uploading
            and progress.get("operator", "").strip().lower() == operator.strip().lower()
            and progress.get("total", 0) > 0
        )
        error_count = sum(
            1
            for line in logs
            if line.startswith(f"[{operator}] ") and ("FAILED" in line or "FATAL" in line or "ERROR" in line)
        )

        users.append(
            {
                "operator": operator,
                "accounts": accounts,
                "account_count": len(accounts),
                "running": running_now,
                "error_count": error_count,
                "latest_session": latest,
            }
        )

    recent_errors = [line for line in logs if ("FAILED" in line or "FATAL" in line or "ERROR" in line)]
    recent_errors = recent_errors[-120:]

    return {
        "users": users,
        "recent_errors": recent_errors,
    }


@app.get("/admin/logs")
async def admin_logs(
    operator: Optional[str] = Query(default=None),
    x_operator: Optional[str] = Header(default=None, alias="X-Operator"),
):
    _require_admin(x_operator)

    if CENTRAL_SYNC_ENABLED:
        params = {"operator": operator} if operator else {}
        return _sync_request_or_http_error(
            "GET",
            "/admin/logs",
            params=params,
            expected_statuses=[200],
            detail_on_fail="Failed to read central admin logs.",
        )

    logs, _, _ = _snapshot_state()
    selected = (operator or "").strip()
    if selected:
        logs = [line for line in logs if line.startswith(f"[{selected}] ")]
    return {"logs": logs, "count": len(logs)}

@app.post("/clear-logs")
async def clear_logs(x_operator: Optional[str] = Header(default=None, alias="X-Operator")):
    _require_admin(x_operator)
    global execution_logs, current_progress, operator_session_history
    with progress_lock:
        execution_logs = []
        current_progress = _empty_progress()
        operator_session_history = {}
    return {"status": "cleared"}

@app.get("/")
async def read_index():
    return FileResponse(os.path.join(os.path.dirname(__file__), "..", "frontend", "index.html"))

# Mount the rest of the frontend files (CSS, etc)
app.mount("/", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "..", "frontend")), name="static")

if __name__ == "__main__":
    import uvicorn
    # Use "main:app" string format and reload=True for dynamic updates
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
