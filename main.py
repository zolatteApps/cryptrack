import asyncio
import json
import os
import pathlib
import requests
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Any, List
from dotenv import load_dotenv

load_dotenv()

# ── Supabase client (optional — falls back to state.json if not configured) ──
_supabase = None

def _get_supabase():
    global _supabase
    if _supabase is None:
        url = os.environ.get("SUPABASE_URL", "")
        key = os.environ.get("SUPABASE_KEY", "")
        if url and key:
            from supabase import create_client
            _supabase = create_client(url, key)
    return _supabase

STATE_FILE = pathlib.Path("state.json")


def _read_state() -> dict:
    db = _get_supabase()
    if db:
        try:
            result = db.table("app_state").select("*").eq("id", 1).single().execute()
            if result.data:
                return {
                    "positions":  result.data.get("positions", []) or [],
                    "alerts":     result.data.get("alerts", []) or [],
                    "ntfy_topic": result.data.get("ntfy_topic", "") or "",
                }
        except Exception as e:
            print(f"[supabase] read error: {e}")
    # File fallback
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return {"positions": [], "alerts": [], "ntfy_topic": ""}


def _write_state(data: dict) -> None:
    db = _get_supabase()
    if db:
        try:
            db.table("app_state").upsert({
                "id":         1,
                "positions":  data.get("positions", []),
                "alerts":     data.get("alerts", []),
                "ntfy_topic": data.get("ntfy_topic", ""),
            }).execute()
            return
        except Exception as e:
            print(f"[supabase] write error: {e}")
    # File fallback
    STATE_FILE.write_text(json.dumps(data, indent=2))


# ── Background alert checker ─────────────────────────────────────────────────

def _calc_pnl(pos: dict, mark: float) -> tuple[float, float]:
    direction = 1 if pos["side"] == "LONG" else -1
    qty = abs(pos["size"]) / pos["entryPrice"]
    pnl = direction * (mark - pos["entryPrice"]) * qty
    margin = abs(pos["size"]) / pos["leverage"]
    roe = (pnl / margin * 100) if margin else 0
    return pnl, roe


def _ntfy_send(topic: str, title: str, message: str) -> None:
    payload = json.dumps(
        {"topic": topic, "title": title, "message": message, "priority": 4, "tags": ["bell"]},
        ensure_ascii=False,
    ).encode("utf-8")
    requests.post(
        "https://ntfy.sh",
        data=payload,
        headers={"Content-Type": "application/json; charset=utf-8"},
        timeout=10,
    )


async def _alert_checker_loop():
    """Runs every 30 s — fires ntfy alerts even when the browser is closed."""
    await asyncio.sleep(5)  # short delay so server finishes starting up
    while True:
        try:
            state = _read_state()
            positions  = state.get("positions", [])
            alerts     = state.get("alerts", [])
            ntfy_topic = state.get("ntfy_topic", "") or os.environ.get("NTFY_TOPIC", "")

            if positions and alerts and ntfy_topic:
                # Fetch all mark prices in one call
                resp = requests.get(
                    "https://fapi.binance.com/fapi/v1/premiumIndex", timeout=10
                )
                resp.raise_for_status()
                mark_map = {item["symbol"]: float(item["markPrice"]) for item in resp.json()}

                changed = False
                for alert in alerts:
                    if alert.get("triggered"):
                        continue
                    pos = next(
                        (p for p in positions if p["id"] == alert.get("positionId")), None
                    )
                    if not pos:
                        continue
                    mark = mark_map.get(pos["symbol"])
                    if mark is None:
                        continue

                    pnl, roe = _calc_pnl(pos, mark)
                    cond = alert["condition"]
                    threshold = alert["threshold"]
                    hit = (cond == "gt" and pnl >= threshold) or \
                          (cond == "lt" and pnl <= threshold)
                    if not hit:
                        continue

                    alert["triggered"] = True
                    changed = True

                    pnl_str  = f"{'+' if pnl >= 0 else ''}{pnl:.2f}"
                    roe_str  = f"{'+' if roe >= 0 else ''}{roe:.1f}%"
                    cond_str = "≥" if cond == "gt" else "≤"
                    dir_emoji  = "📈" if pnl >= 0 else "📉"
                    side_emoji = "🟢" if pos["side"] == "LONG" else "🔴"
                    label = alert.get("label", f"{pos['symbol']} {pos['side']}")

                    title   = f"{dir_emoji} {label} · {pnl_str} USDT ({roe_str})"
                    message = (
                        f"Alert: PnL {cond_str} ${abs(threshold):.2f}  |  "
                        f"{side_emoji} {pos['side']}  Entry {pos['entryPrice']}  |  "
                        f"Mark {mark:.7f}  |  ROE {roe_str}"
                    )

                    try:
                        _ntfy_send(ntfy_topic, title, message)
                        print(f"[alert] fired: {title}")
                    except Exception as e:
                        print(f"[alert] ntfy error: {e}")

                if changed:
                    state["alerts"] = alerts
                    _write_state(state)

        except Exception as e:
            print(f"[alert-checker] error: {e}")

        await asyncio.sleep(30)


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(_alert_checker_loop())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(title="Crypto Trades Monitor", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/", include_in_schema=False)
def root():
    return FileResponse("static/index.html")


# ── State endpoints ───────────────────────────────────────────────────────────
class StateModel(BaseModel):
    positions: List[Any] = []
    alerts: List[Any] = []
    ntfy_topic: str = ""


@app.get("/api/state")
def get_state():
    state = _read_state()
    if not state.get("ntfy_topic"):
        state["ntfy_topic"] = os.environ.get("NTFY_TOPIC", "")
    return state


@app.post("/api/state")
def save_state(body: StateModel):
    _write_state(body.model_dump())
    return {"ok": True}


# ── Cron endpoint (for Vercel cron / external schedulers) ────────────────────
@app.post("/api/check-alerts")
def check_alerts_endpoint():
    """Called by Vercel cron every minute (or QStash every 30s)."""
    state = _read_state()
    positions  = state.get("positions", [])
    alerts     = state.get("alerts", [])
    ntfy_topic = state.get("ntfy_topic", "") or os.environ.get("NTFY_TOPIC", "")

    if not positions or not alerts or not ntfy_topic:
        return {"fired": 0, "reason": "nothing to check"}

    resp = requests.get("https://fapi.binance.com/fapi/v1/premiumIndex", timeout=10)
    resp.raise_for_status()
    mark_map = {item["symbol"]: float(item["markPrice"]) for item in resp.json()}

    fired = 0
    changed = False
    for alert in alerts:
        if alert.get("triggered"):
            continue
        pos = next((p for p in positions if p["id"] == alert.get("positionId")), None)
        if not pos:
            continue
        mark = mark_map.get(pos["symbol"])
        if mark is None:
            continue

        pnl, roe = _calc_pnl(pos, mark)
        cond = alert["condition"]
        threshold = alert["threshold"]
        hit = (cond == "gt" and pnl >= threshold) or (cond == "lt" and pnl <= threshold)
        if not hit:
            continue

        alert["triggered"] = True
        changed = True
        fired += 1

        pnl_str  = f"{'+' if pnl >= 0 else ''}{pnl:.2f}"
        roe_str  = f"{'+' if roe >= 0 else ''}{roe:.1f}%"
        cond_str = "≥" if cond == "gt" else "≤"
        dir_emoji  = "📈" if pnl >= 0 else "📉"
        side_emoji = "🟢" if pos["side"] == "LONG" else "🔴"
        label = alert.get("label", f"{pos['symbol']} {pos['side']}")

        title   = f"{dir_emoji} {label} · {pnl_str} USDT ({roe_str})"
        message = (
            f"Alert: PnL {cond_str} ${abs(threshold):.2f}  |  "
            f"{side_emoji} {pos['side']}  Entry {pos['entryPrice']}  |  "
            f"Mark {mark:.7f}  |  ROE {roe_str}"
        )
        try:
            _ntfy_send(ntfy_topic, title, message)
        except Exception as e:
            print(f"[check-alerts] ntfy error: {e}")

    if changed:
        state["alerts"] = alerts
        _write_state(state)

    return {"fired": fired}


# ── Existing endpoints ────────────────────────────────────────────────────────
@app.get("/api/config")
def get_config():
    return {"ntfy_topic": os.environ.get("NTFY_TOPIC", "")}


@app.get("/api/markprices")
def mark_prices():
    try:
        resp = requests.get("https://fapi.binance.com/fapi/v1/premiumIndex", timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


class NotifyRequest(BaseModel):
    title: str
    message: str
    topic: str = ""


@app.post("/api/notify")
def notify(body: NotifyRequest):
    topic = body.topic or os.environ.get("NTFY_TOPIC", "")
    if not topic:
        raise HTTPException(status_code=503, detail="No ntfy topic configured")
    payload = json.dumps(
        {"topic": topic, "title": body.title, "message": body.message,
         "priority": 4, "tags": ["bell"]},
        ensure_ascii=False,
    ).encode("utf-8")
    resp = requests.post(
        "https://ntfy.sh",
        data=payload,
        headers={"Content-Type": "application/json; charset=utf-8"},
        timeout=10,
    )
    if not resp.ok:
        raise HTTPException(status_code=502, detail=resp.text)
    return {"sent": True, "topic": topic}


@app.get("/api/messages")
def get_messages(topic: str = Query(...), since: str = "1h"):
    if not topic:
        raise HTTPException(status_code=400, detail="topic required")
    try:
        resp = requests.get(
            f"https://ntfy.sh/{topic}/json",
            params={"poll": "1", "since": since},
            timeout=10,
        )
        messages = []
        for line in resp.text.strip().split("\n"):
            line = line.strip()
            if line:
                try:
                    messages.append(json.loads(line))
                except Exception:
                    pass
        messages.sort(key=lambda m: m.get("time", 0), reverse=True)
        return messages[:20]
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))
