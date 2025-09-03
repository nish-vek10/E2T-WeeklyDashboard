# api.py
import os
from typing import Optional
from fastapi import FastAPI, Depends, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client, Client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]  # server-side only
API_BEARER_TOKEN = os.environ.get("API_BEARER_TOKEN", "").strip()

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
app = FastAPI(title="E2T API", version="1.0.0")

# CORS (adjust origins if you want to lock it down)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

def require_auth(authorization: Optional[str] = Header(None)):
    if not API_BEARER_TOKEN:
        return  # auth disabled
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")
    token = authorization.split(" ", 1)[1]
    if token != API_BEARER_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid token")

def _count(table: str) -> int:
    # use count='exact' trick; supabase-py returns .count
    res = sb.table(table).select("account_id", count="exact").execute()
    return res.count or 0

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/counts")
def counts(dep=Depends(require_auth)):
    return {
        "active": _count("e2t_active"),
        "blown": _count("e2t_blown"),
        "purchases_api": _count("e2t_purchases_api"),
        "plan50k": _count("e2t_plan50k"),
        "baseline": _count("e2t_baseline"),
    }

@app.get("/active")
def active(limit: int = 100, offset: int = 0, dep=Depends(require_auth)):
    # Sort by pct_change desc, NULLS LAST; then by updated_at desc for stable order
    q = (sb.table("e2t_active")
          .select("*")
          .order("pct_change", desc=True, nullsfirst=False)
          .order("updated_at", desc=True)
          .range(offset, offset + limit - 1))
    return q.execute().data

@app.get("/blown")
def blown(limit: int = 100, offset: int = 0, dep=Depends(require_auth)):
    q = (sb.table("e2t_blown")
          .select("*")
          .order("updated_at", desc=True)
          .range(offset, offset + limit - 1))
    return q.execute().data

@app.get("/purchases")
def purchases(limit: int = 100, offset: int = 0, dep=Depends(require_auth)):
    q = (sb.table("e2t_purchases_api")
          .select("*")
          .order("updated_at", desc=True)
          .range(offset, offset + limit - 1))
    return q.execute().data

@app.get("/plan50k")
def plan50k(limit: int = 100, offset: int = 0, dep=Depends(require_auth)):
    q = (sb.table("e2t_plan50k")
          .select("*")
          .order("updated_at", desc=True)
          .range(offset, offset + limit - 1))
    return q.execute().data

@app.get("/data/latest")
def latest(limit: int = 100, dep=Depends(require_auth)):
    c = {
        "active": _count("e2t_active"),
        "blown": _count("e2t_blown"),
        "purchases_api": _count("e2t_purchases_api"),
        "plan50k": _count("e2t_plan50k"),
        "baseline": _count("e2t_baseline"),
    }
    top = (sb.table("e2t_active")
             .select("*")
             .order("pct_change", desc=True, nullsfirst=False)
             .order("updated_at", desc=True)
             .range(0, max(0, limit - 1))
             .execute()
             .data)
    latest_ts = None
    if top:
        latest_ts = max([row.get("updated_at") for row in top if row.get("updated_at")], default=None)
    return {"counts": c, "active_top": top, "updated_at": latest_ts}
