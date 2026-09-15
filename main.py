"""
main.py — SarrafBot FastAPI Ingestion Node
==========================================
Thin web layer. Receives WhatsApp webhooks, validates them, and ENQUEUES a job
onto the Redis (Upstash) queue via ARQ. Returns 200 OK to WhatsApp in <200ms.
The heavy lifting (Gemini, Supabase, replies) happens in worker.py.

Run with:  uvicorn main:app --reload
The worker runs separately:  arq worker.WorkerSettings
"""

import os
import sys
import asyncio
import logging
import datetime
from urllib.parse import urlparse
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response, HTTPException
from fastapi.responses import PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from arq import create_pool
from arq.connections import RedisSettings
from supabase import create_client, Client

# Windows asyncio + TLS fix (Upstash rediss://). Harmless on Linux.
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("sarrafbot.ingest")

VERIFY_TOKEN = os.getenv("WHATSAPP_VERIFY_TOKEN", "hisaab_verify")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
FREE_HISTORY_DAYS = 90


def _redis_settings() -> RedisSettings:
    u = urlparse(os.getenv("REDIS_URL"))
    return RedisSettings(
        host=u.hostname, port=u.port or 6379,
        password=u.password, ssl=True,
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Open one shared ARQ Redis pool for the app's lifetime. Resilient to
    Redis being unavailable (e.g. local dev without a Redis subscription) --
    the app still starts, but webhook/reminder endpoints will explicitly
    report unavailable rather than silently dropping messages."""
    try:
        app.state.arq = await create_pool(_redis_settings())
        logger.info("Connected to Redis queue.")
    except Exception as exc:  # noqa: BLE001
        app.state.arq = None
        logger.warning(
            "Redis unavailable at startup (%s) -- webhook/reminder endpoints "
            "will report unavailable until it's back. Dashboard API endpoints "
            "are unaffected.", exc
        )
    yield
    if app.state.arq is not None:
        await app.state.arq.aclose()


app = FastAPI(title="SarrafBot Ingestion", version="3.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://emaan-gul.github.io",
        "http://localhost:8000",
        "http://127.0.0.1:8000",
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# --------------------------------------------------------------------------- #
# Endpoints
# --------------------------------------------------------------------------- #
@app.get("/webhook")
async def verify_webhook(request: Request):
    """WhatsApp webhook verification handshake."""
    p = request.query_params
    if p.get("hub.mode") == "subscribe" and p.get("hub.verify_token") == VERIFY_TOKEN:
        return PlainTextResponse(p.get("hub.challenge", ""))
    return PlainTextResponse("Verification failed", status_code=403)


@app.post("/webhook")
async def webhook(request: Request):
    """Receive WhatsApp messages and ENQUEUE them for the worker."""
    try:
        payload = await request.json()
        arq = app.state.arq
        for entry in payload.get("entry", []):
            for change in entry.get("changes", []):
                value = change.get("value", {})
                for msg in value.get("messages", []):
                    user = msg.get("from")
                    wamid = msg.get("id")
                    mtype = msg.get("type")
                    if not user or not wamid:
                        continue

                    if mtype == "text":
                        await arq.enqueue_job("process_message", user, wamid,
                                              text=msg["text"]["body"])
                    elif mtype in ("audio", "voice"):
                        await arq.enqueue_job("process_message", user, wamid,
                                              media_id=msg[mtype]["id"])
                    elif mtype == "image":
                        await arq.enqueue_job("process_message", user, wamid,
                                              text=msg["image"].get("caption"),
                                              media_id=msg["image"]["id"])
                    elif mtype == "document":
                        await arq.enqueue_job("process_message", user, wamid,
                                              text=msg["document"].get("caption"),
                                              media_id=msg["document"]["id"])
                    elif mtype in ("unsupported", "system", "reaction", "ephemeral"):
                        logger.info("Ignoring %s event from %s", mtype, user)
                        continue
                    else:
                        # Unknown type: enqueue with no media so worker replies politely.
                        await arq.enqueue_job("process_message", user, wamid,
                                              text=None)
                    logger.info("Enqueued %s job for %s (wamid %s)", mtype, user, wamid)
    except Exception as exc:  # noqa: BLE001
        logger.error("Webhook parse/enqueue error [%s]: %s", type(exc).__name__, exc)
    # Always 200 fast so WhatsApp doesn't retry.
    return {"status": "queued"}


@app.post("/trigger-reminders")
@app.get("/trigger-reminders")
async def trigger_reminders():
    """Enqueue the daily reminder sweep onto the worker."""
    job = await app.state.arq.enqueue_job("run_reminders")
    return {"status": "enqueued", "job_id": job.job_id if job else None}


@app.get("/")
async def health():
    return {"status": "alive", "service": "sarrafbot-ingest"}


def _valid_dashboard_token(token: str):
    """Look up a dashboard token; return its row if it exists and has not
    expired, else None. Shared by both the exchange and data endpoints."""
    rows = (
        supabase.table("dashboard_tokens")
        .select("user_phone, expires_at")
        .eq("token", token)
        .limit(1)
        .execute()
        .data
    )
    if not rows:
        return None
    expires_at = rows[0].get("expires_at")
    try:
        exp = datetime.datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
        if exp < datetime.datetime.now(datetime.timezone.utc):
            return None
    except Exception:  # noqa: BLE001
        return None
    return rows[0]


@app.get("/api/dashboard/data")
async def dashboard_data(token: str):
    """Return a snapshot of the authenticated user's data for the
    dashboard: balance, recent transactions, budgets, savings goals,
    active reminders, and current tier. Read-only -- editing happens
    through the bot, not here, so this stays a thin, low-risk endpoint.
    Token is passed directly as a query param (not a cookie) -- simpler
    and avoids cross-site cookie quirks across the github.io/railway.app
    domain split, at the cost of the token being visible in this one
    network request each time (24-hour expiry, read-only, low-risk)."""
    row = _valid_dashboard_token(token)
    if not row:
        raise HTTPException(status_code=401, detail="Invalid or expired link")
    user = row["user_phone"]
    tier_rows = (
        supabase.table("subscriptions")
        .select("tier, expires_at")
        .eq("user_phone", user)
        .limit(1)
        .execute()
        .data
    )
    tier = "free"
    if tier_rows and tier_rows[0].get("tier") == "premium":
        exp = tier_rows[0].get("expires_at")
        if not exp:
            tier = "premium"
        else:
            try:
                exp_dt = datetime.datetime.fromisoformat(exp.replace("Z", "+00:00"))
                if exp_dt >= datetime.datetime.now(datetime.timezone.utc):
                    tier = "premium"
            except Exception:  # noqa: BLE001
                pass

    if tier == "free":
        cutoff = (datetime.date.today() - datetime.timedelta(days=FREE_HISTORY_DAYS)).isoformat()
    else:
        cutoff = "2000-01-01"

    expenses = (
        supabase.table("expenses")
        .select("date, type, category, description, amount")
        .eq("user_phone", user)
        .gte("date", cutoff)
        .order("date", desc=True)
        .execute()
        .data
        or []
    )
    income_total = sum((r.get("amount") or 0) for r in expenses if r.get("type") == "income")
    expense_total = sum((r.get("amount") or 0) for r in expenses if r.get("type") == "expense")

    budgets_raw = (
        supabase.table("budgets")
        .select("category, amount, period")
        .eq("user_phone", user)
        .execute()
        .data
        or []
    )
    # Each budget's "spent" is scoped to that budget's OWN period (daily/
    # weekly/monthly) -- mirrors processor.py's _budget_period_bounds so a
    # transaction from a previous month never counts against this month's
    # budget, matching what the bot itself reports in chat.
    budgets = []
    today = datetime.date.today()
    for b in budgets_raw:
        period = (b.get("period") or "monthly").lower()
        if period == "daily":
            period_start = today.isoformat()
        elif period == "weekly":
            period_start = (today - datetime.timedelta(days=today.weekday())).isoformat()
        else:
            period_start = today.replace(day=1).isoformat()
        period_rows = (
            supabase.table("expenses")
            .select("amount, type")
            .eq("user_phone", user)
            .eq("category", b.get("category"))
            .gte("date", period_start)
            .execute()
            .data
            or []
        )
        spent = sum((r.get("amount") or 0) for r in period_rows if r.get("type") == "expense")
        budgets.append({**b, "spent": spent})
    goals = (
        supabase.table("savings_goals")
        .select("goal_name, target_amount, saved_amount")
        .eq("user_phone", user)
        .order("created_at", desc=True)
        .execute()
        .data
        or []
    )
    reminders = (
        supabase.table("reminders")
        .select("title, due_date, recurrence")
        .eq("user_phone", user)
        .eq("is_completed", False)
        .order("due_date")
        .execute()
        .data
        or []
    )

    return {
        "tier": tier,
        "balance": {
            "income": income_total,
            "expense": expense_total,
            "net": income_total - expense_total,
        },
        "transactions": expenses[:50],
        "budgets": budgets,
        "goals": goals,
        "reminders": reminders,
    }
