from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from fastapi import FastAPI, Header, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional
import json
import os
import threading


app = FastAPI(title="GML Central Sync + Monitor")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "data")
OPERATORS_DB_PATH = os.path.join(DATA_DIR, "operators_db.json")
COOKIES_DB_PATH = os.path.join(DATA_DIR, "cookies_db.json")
os.makedirs(DATA_DIR, exist_ok=True)

SYNC_TOKEN = os.getenv("SYNC_TOKEN", "change-me-sync-token").strip()
MAX_SESSIONS_PER_OPERATOR = max(1, int(os.getenv("CENTRAL_MAX_SESSIONS_PER_OPERATOR", "25")))
MAX_LOG_EVENTS = max(100, int(os.getenv("CENTRAL_MAX_LOG_EVENTS", "5000")))
RUNNING_STALE_SECONDS = max(30, int(os.getenv("CENTRAL_RUNNING_STALE_SECONDS", "180")))

DEFAULT_OPERATORS_DB = {
    "Admin": [],
    "Patrick": ["VelmoraSkin", "Novacique", "TrivexaCore"],
    "Asis": ["Kairothix", "Zyrelune", "OrphicDerma"],
    "Ace": ["NuvyraCell", "CalystrixLab", "ElyndorGlow", "MyravaLabs"],
    "Benser": ["jbnsrr.xx"],
}

state_lock = threading.Lock()
telemetry_logs: List[Dict[str, Any]] = []
latest_status_by_operator: Dict[str, Dict[str, Any]] = {}
session_history_by_operator: Dict[str, List[Dict[str, Any]]] = {}
stream_clients: List[WebSocket] = []


class OperatorsPayload(BaseModel):
    operators: Dict[str, List[str]] = Field(default_factory=dict)


class CookieSyncPayload(BaseModel):
    cookies: List[Dict[str, Any]] = Field(default_factory=list)
    source_operator: str = ""
    node_id: str = ""
    updated_at: Optional[str] = None


class TelemetryEvent(BaseModel):
    node_id: str = ""
    operator: str = ""
    account: str = ""
    session_id: str = ""
    event_type: str = "status"
    severity: str = "info"
    message: str = ""
    percent: float = 0
    completed: int = 0
    failed: int = 0
    pending: int = 0
    timestamp: Optional[str] = None
    total: Optional[int] = None
    success: Optional[int] = None
    active: Optional[int] = None
    status: Optional[str] = None


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: str, fallback: Any) -> Any:
    if not os.path.exists(path):
        return deepcopy(fallback)
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return deepcopy(fallback)


def _write_json(path: str, data: Any):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def _normalize_operator_name(name: str) -> str:
    return str(name or "").strip()


def _normalize_account_name(name: str) -> str:
    return str(name or "").strip().lstrip("@")


def _normalize_operators_db(raw: Dict[str, Any]) -> Dict[str, List[str]]:
    normalized: Dict[str, List[str]] = {}
    for operator, accounts in (raw or {}).items():
        clean_operator = _normalize_operator_name(operator)
        if not clean_operator:
            continue

        clean_accounts: List[str] = []
        for account in accounts if isinstance(accounts, list) else []:
            clean_account = _normalize_account_name(account)
            if clean_account and clean_account not in clean_accounts:
                clean_accounts.append(clean_account)

        normalized[clean_operator] = clean_accounts

    if "Admin" not in normalized:
        normalized["Admin"] = []
    return normalized


def _load_operators() -> Dict[str, List[str]]:
    data = _normalize_operators_db(_read_json(OPERATORS_DB_PATH, DEFAULT_OPERATORS_DB))
    if not data:
        data = deepcopy(DEFAULT_OPERATORS_DB)
    return data


def _save_operators(data: Dict[str, List[str]]) -> Dict[str, List[str]]:
    normalized = _normalize_operators_db(data)
    _write_json(OPERATORS_DB_PATH, normalized)
    return normalized


def _load_cookie_store() -> Dict[str, Dict[str, Any]]:
    raw = _read_json(COOKIES_DB_PATH, {})
    if not isinstance(raw, dict):
        return {}
    clean: Dict[str, Dict[str, Any]] = {}
    for account, payload in raw.items():
        clean_account = _normalize_account_name(account)
        if not clean_account or not isinstance(payload, dict):
            continue
        cookies = payload.get("cookies")
        clean[clean_account] = {
            "cookies": cookies if isinstance(cookies, list) else [],
            "updated_at": str(payload.get("updated_at") or ""),
            "source_operator": str(payload.get("source_operator") or ""),
            "node_id": str(payload.get("node_id") or ""),
        }
    return clean


def _save_cookie_store(data: Dict[str, Dict[str, Any]]):
    _write_json(COOKIES_DB_PATH, data)


def _require_token(x_sync_token: Optional[str]):
    expected = SYNC_TOKEN
    received = (x_sync_token or "").strip()
    if expected and received != expected:
        raise HTTPException(status_code=401, detail="Invalid or missing X-Sync-Token.")


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _parse_timestamp(ts: str) -> Optional[datetime]:
    value = str(ts or "").strip()
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            pass
    return None


def _is_running_status(status_event: Optional[Dict[str, Any]]) -> bool:
    if not status_event:
        return False
    status = str(status_event.get("status") or "").strip().lower()
    event_type = str(status_event.get("event_type") or "").strip().lower()
    if status in {"finished", "failed", "stopped", "idle"}:
        return False
    if event_type in {"session_finished", "session_failed", "session_stopped", "idle"}:
        return False
    timestamp = _parse_timestamp(str(status_event.get("timestamp") or ""))
    if timestamp is None:
        return True
    age_seconds = (datetime.now(timezone.utc) - timestamp.astimezone(timezone.utc)).total_seconds()
    return age_seconds <= RUNNING_STALE_SECONDS


def _event_to_dict(event: TelemetryEvent) -> Dict[str, Any]:
    return {
        "node_id": str(event.node_id or "").strip(),
        "operator": str(event.operator or "").strip(),
        "account": _normalize_account_name(event.account),
        "session_id": str(event.session_id or "").strip(),
        "event_type": str(event.event_type or "status").strip().lower(),
        "severity": str(event.severity or "info").strip().lower(),
        "message": str(event.message or "").strip(),
        "percent": max(0.0, min(100.0, _safe_float(event.percent, 0.0))),
        "completed": max(0, _safe_int(event.completed, 0)),
        "failed": max(0, _safe_int(event.failed, 0)),
        "pending": max(0, _safe_int(event.pending, 0)),
        "total": max(0, _safe_int(event.total, 0)) if event.total is not None else None,
        "success": max(0, _safe_int(event.success, 0)) if event.success is not None else None,
        "active": max(0, _safe_int(event.active, 0)) if event.active is not None else None,
        "status": str(event.status or "").strip().lower() or None,
        "timestamp": str(event.timestamp or "").strip() or _utc_now_iso(),
        "ingested_at": _utc_now_iso(),
    }


def _as_log_line(event: Dict[str, Any]) -> str:
    timestamp = event.get("timestamp") or ""
    operator = event.get("operator") or "Unknown"
    node_id = event.get("node_id") or "node"
    severity = str(event.get("severity") or "info").upper()
    message = event.get("message") or ""
    return f"[{timestamp}] [{operator}] [{node_id}] [{severity}] {message}"


def _finalize_event(event: Dict[str, Any]) -> bool:
    status = str(event.get("status") or "").lower()
    event_type = str(event.get("event_type") or "").lower()
    return status in {"finished", "failed", "stopped"} or event_type in {
        "session_finished",
        "session_failed",
        "session_stopped",
    }


async def _broadcast(payload: Dict[str, Any]):
    stale: List[WebSocket] = []
    for client in list(stream_clients):
        try:
            await client.send_json(payload)
        except Exception:
            stale.append(client)
    for client in stale:
        if client in stream_clients:
            stream_clients.remove(client)


@app.get("/health")
async def health():
    return {"status": "ok", "timestamp": _utc_now_iso()}


@app.get("/sync/operators")
async def get_sync_operators(x_sync_token: Optional[str] = Header(default=None, alias="X-Sync-Token")):
    _require_token(x_sync_token)
    return {"operators": _load_operators()}


@app.put("/sync/operators")
async def put_sync_operators(payload: OperatorsPayload, x_sync_token: Optional[str] = Header(default=None, alias="X-Sync-Token")):
    _require_token(x_sync_token)
    operators = _save_operators(payload.operators)
    await _broadcast({"channel": "operators", "operators": operators})
    return {"operators": operators}


@app.get("/sync/cookies/{account}")
async def get_sync_cookie(account: str, x_sync_token: Optional[str] = Header(default=None, alias="X-Sync-Token")):
    _require_token(x_sync_token)
    clean_account = _normalize_account_name(account)
    if not clean_account:
        raise HTTPException(status_code=400, detail="Account name is required.")

    store = _load_cookie_store()
    payload = store.get(clean_account)
    if payload is None or not isinstance(payload.get("cookies"), list) or not payload["cookies"]:
        raise HTTPException(status_code=404, detail="No shared cookie available for this account.")

    return {
        "account": clean_account,
        "cookies": payload["cookies"],
        "cookie_count": len(payload["cookies"]),
        "updated_at": payload.get("updated_at"),
        "source_operator": payload.get("source_operator"),
        "node_id": payload.get("node_id"),
    }


@app.put("/sync/cookies/{account}")
async def put_sync_cookie(
    account: str,
    payload: CookieSyncPayload,
    x_sync_token: Optional[str] = Header(default=None, alias="X-Sync-Token"),
):
    _require_token(x_sync_token)
    clean_account = _normalize_account_name(account)
    if not clean_account:
        raise HTTPException(status_code=400, detail="Account name is required.")
    if not isinstance(payload.cookies, list) or not payload.cookies:
        raise HTTPException(status_code=400, detail="Cookie payload is required.")

    store = _load_cookie_store()
    store[clean_account] = {
        "cookies": payload.cookies,
        "updated_at": payload.updated_at or _utc_now_iso(),
        "source_operator": str(payload.source_operator or "").strip(),
        "node_id": str(payload.node_id or "").strip(),
    }
    _save_cookie_store(store)

    await _broadcast(
        {
            "channel": "cookie",
            "account": clean_account,
            "updated_at": store[clean_account]["updated_at"],
            "source_operator": store[clean_account]["source_operator"],
            "node_id": store[clean_account]["node_id"],
        }
    )
    return {"status": "ok", "account": clean_account, "cookie_count": len(payload.cookies)}


@app.post("/telemetry/logs")
async def ingest_log(event: TelemetryEvent, x_sync_token: Optional[str] = Header(default=None, alias="X-Sync-Token")):
    _require_token(x_sync_token)
    event_data = _event_to_dict(event)

    with state_lock:
        telemetry_logs.append(event_data)
        if len(telemetry_logs) > MAX_LOG_EVENTS:
            del telemetry_logs[: len(telemetry_logs) - MAX_LOG_EVENTS]

    await _broadcast({"channel": "telemetry_log", "event": event_data})
    return {"status": "ok"}


@app.post("/telemetry/status")
async def ingest_status(event: TelemetryEvent, x_sync_token: Optional[str] = Header(default=None, alias="X-Sync-Token")):
    _require_token(x_sync_token)
    event_data = _event_to_dict(event)
    operator = str(event_data.get("operator") or "").strip()

    with state_lock:
        if operator:
            latest_status_by_operator[operator] = event_data
            if _finalize_event(event_data):
                history = session_history_by_operator.setdefault(operator, [])
                history.insert(0, deepcopy(event_data))
                del history[MAX_SESSIONS_PER_OPERATOR:]

    await _broadcast({"channel": "telemetry_status", "event": event_data})
    return {"status": "ok"}


@app.get("/admin/logs")
async def get_admin_logs(
    operator: Optional[str] = Query(default=None),
    limit: int = Query(default=400, ge=1, le=2000),
    x_sync_token: Optional[str] = Header(default=None, alias="X-Sync-Token"),
):
    _require_token(x_sync_token)

    clean_operator = str(operator or "").strip()
    with state_lock:
        if clean_operator:
            logs = [event for event in telemetry_logs if str(event.get("operator") or "") == clean_operator]
        else:
            logs = list(telemetry_logs)

    logs = logs[-limit:]
    return {
        "logs": [_as_log_line(event) for event in logs],
        "events": logs,
        "count": len(logs),
    }


@app.get("/admin/overview")
async def get_admin_overview(x_sync_token: Optional[str] = Header(default=None, alias="X-Sync-Token")):
    _require_token(x_sync_token)
    operators_db = _load_operators()

    with state_lock:
        logs_snapshot = deepcopy(telemetry_logs)
        status_snapshot = deepcopy(latest_status_by_operator)
        history_snapshot = deepcopy(session_history_by_operator)

    users: List[Dict[str, Any]] = []
    for operator, accounts in operators_db.items():
        if str(operator).strip().lower() == "admin":
            continue

        current_status = status_snapshot.get(operator)
        running = _is_running_status(current_status)
        latest_finished = (history_snapshot.get(operator) or [None])[0]
        latest_session = current_status if running and current_status else latest_finished

        error_count = sum(
            1
            for event in logs_snapshot
            if str(event.get("operator") or "") == operator
            and str(event.get("severity") or "").lower() in {"error", "fatal"}
        )

        users.append(
            {
                "operator": operator,
                "accounts": accounts if isinstance(accounts, list) else [],
                "account_count": len(accounts if isinstance(accounts, list) else []),
                "running": running,
                "error_count": error_count,
                "latest_session": latest_session,
                "latest_finished_session": latest_finished,
            }
        )

    recent_error_events = [
        event
        for event in logs_snapshot
        if str(event.get("severity") or "").lower() in {"error", "fatal"}
    ][-120:]
    recent_errors = [_as_log_line(event) for event in recent_error_events]

    live_status = [status for status in status_snapshot.values() if _is_running_status(status)]
    active_nodes = sorted({str(status.get("node_id") or "").strip() for status in live_status if status.get("node_id")})

    return {
        "users": users,
        "recent_errors": recent_errors,
        "recent_error_events": recent_error_events,
        "live_status": live_status,
        "active_nodes": active_nodes,
        "generated_at": _utc_now_iso(),
    }


@app.websocket("/admin/stream")
async def admin_stream(websocket: WebSocket, token: str = Query(default="")):
    expected = SYNC_TOKEN
    if expected and str(token or "").strip() != expected:
        await websocket.close(code=1008)
        return

    await websocket.accept()
    stream_clients.append(websocket)

    try:
        await websocket.send_json(
            {
                "channel": "hello",
                "timestamp": _utc_now_iso(),
                "message": "Central stream connected",
            }
        )
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        if websocket in stream_clients:
            stream_clients.remove(websocket)


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("CENTRAL_PORT", "8100"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
