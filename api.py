# api.py
import os
from typing import Optional

from fastapi import FastAPI, Header, HTTPException, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client
from datetime import datetime, timezone

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", os.environ.get("SUPABASE_ANON_KEY", "")).strip()
if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("SUPABASE_URL / SUPABASE_*_KEY not set")

API_BEARER_TOKEN = os.environ.get("API_BEARER_TOKEN", "").strip()

sb = create_client(SUPABASE_URL, SUPABASE_KEY)

app = FastAPI(title="E2T API")

# CORS: open for now; lock down later if you host the frontend somewhere specific.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # e.g. ["https://yourdomain.com"]
    allow_methods=["*"],
    allow_headers=["*"],
)


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def auth(authorization: Optional[str] = Header(None)):
    """
    Optional bearer auth.
    If API_BEARER_TOKEN is set in Heroku, require a valid "Authorization: Bearer <token>".
    If not set, open access.
    """
    if not API_BEARER_TOKEN:
        return
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")
    token = authorization.split(" ", 1)[1]
    if token != API_BEARER_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid bearer token")


@app.get("/health")
def health():
    return {"ok": True, "ts": _now_iso()}


def fetch_counts():
    try:
        rows = sb.table("e2t_counts").select("*").limit(1).execute().data or []
        return rows[0] if rows else {"active": 0, "blown": 0, "purchases_api": 0, "plan50k": 0, "baseline": 0}
    except Exception:
        return {"active": 0, "blown": 0, "purchases_api": 0, "plan50k": 0, "baseline": 0}


def fetch_baseline_at():
    try:
        rows = sb.table("e2t_baseline").select("baseline_at").order("baseline_at", desc=True).limit(1).execute().data or []
        return rows[0]["baseline_at"] if rows else None
    except Exception:
        return None


def fetch_table_sorted(name: str, order_col: Optional[str] = None, desc: bool = True, limit: Optional[int] = None, extra_select: str = "*"):
    q = sb.table(name).select(extra_select)
    if order_col:
        q = q.order(order_col, desc=desc)
    if limit:
        q = q.limit(limit)
    return q.execute().data or []


@app.get("/data/latest")
def data_latest(
    _=Depends(auth),
    limit_active: int = Query(500, ge=1, le=5000),
    limit_blown: int = Query(200, ge=0, le=5000),
    limit_purchases: int = Query(200, ge=0, le=5000),
    limit_plan50k: int = Query(100, ge=0, le=5000),
):
    """
    Returns everything the frontend needs in one shot.
    Active is sorted by pct_change desc (NULLs last).
    """
    counts = fetch_counts()
    baseline_at = fetch_baseline_at()

    # NB: supabase-py orders NULLs last by default when desc=True (that’s what we want)
    active = fetch_table_sorted(
        "e2t_active",
        order_col="pct_change",
        desc=True,
        limit=limit_active,
        extra_select="account_id,customer_name,country,plan,balance,equity,open_pnl,pct_change,updated_at",
    )
    blown = fetch_table_sorted(
        "e2t_blown",
        order_col="updated_at",
        desc=True,
        limit=limit_blown,
        extra_select="account_id,customer_name,country,plan,balance,equity,open_pnl,updated_at",
    )
    purchases = fetch_table_sorted(
        "e2t_purchases_api",
        order_col="updated_at",
        desc=True,
        limit=limit_purchases,
        extra_select="account_id,customer_name,country,plan,balance,equity,open_pnl,group_name,updated_at",
    )
    plan50k = fetch_table_sorted(
        "e2t_plan50k",
        order_col="updated_at",
        desc=True,
        limit=limit_plan50k,
        extra_select="account_id,customer_name,country,plan,balance,equity,open_pnl,updated_at",
    )

    return {
        "ts": _now_iso(),
        "baseline_at": baseline_at,
        "counts": counts,
        "active": active,
        "blown": blown,
        "purchases_api": purchases,
        "plan50k": plan50k,
    }
