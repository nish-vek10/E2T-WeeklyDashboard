# worker.py
import os
import sys
import time
import math
import json
import requests
import pandas as pd
from typing import Optional, Tuple, Dict, Any
from datetime import datetime, timedelta, timezone

# === Supabase / PostgREST client ===
# Requires: pip install supabase
from supabase import create_client, Client

# -------------------------
# Environment configuration
# -------------------------
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", os.environ.get("SUPABASE_ANON_KEY", "")).strip()
if not SUPABASE_URL or not SUPABASE_KEY:
    print("[FATAL] SUPABASE_URL / SUPABASE_*_KEY not set.", file=sys.stderr)
    sys.exit(1)

# CRM source table in Supabase
CRM_TABLE = os.environ.get("CRM_TABLE", "lv_tpaccount").strip()

# Sirix API
API_URL = "https://restapi-real3.sirixtrader.com/api/UserStatus/GetUserTransactions"
SIRIX_TOKEN = os.environ.get("SIRIX_TOKEN", "t1_a7xeQOJPnfBzuCncH60yjLFu").strip()

# Scheduling
TEST_MODE = os.environ.get("E2T_TEST_MODE", "false").lower() == "true"   # True => seed baseline immediately
RUN_NOW_ON_START = os.environ.get("E2T_RUN_NOW", "true").lower() == "true"
RATE_DELAY_SEC = float(os.environ.get("E2T_RATE_DELAY_SEC", "0.2"))      # per-account sleep for API politeness

# Optional timezone label for logs (logic uses UTC internally)
E2T_TZ_LABEL = os.environ.get("E2T_TZ_LABEL", "UTC")

# Tables
TABLE_ACTIVE = "e2t_active"
TABLE_BLOWN = "e2t_blown"
TABLE_PURCHASES = "e2t_purchases_api"
TABLE_PLAN50K = "e2t_plan50k"
TABLE_BASELINE = "e2t_baseline"

# --- CRM column names in Supabase (all lowercase, confirmed) ---
CRM_COL_ACCOUNT_ID = "lv_name"
CRM_COL_CUSTOMER   = "lv_accountidname"
CRM_COL_TEMP_NAME  = "lv_tempname"

# Init client
sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# -------------------------
# Time helpers (UTC always)
# -------------------------
def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def now_iso_utc() -> str:
    return now_utc().isoformat()

def get_monday_noon(dt_local: datetime) -> datetime:
    """Return Monday 12:00 of the week containing dt_local (Monday=0)."""
    monday = dt_local - timedelta(days=dt_local.weekday())
    return monday.replace(hour=12, minute=0, second=0, microsecond=0)

def need_new_week(baseline_at_dt: Optional[datetime], now_dt: datetime) -> bool:
    """True if no baseline or it's from a previous competition window (starts Monday 12:00 UTC)."""
    if baseline_at_dt is None:
        return True
    monday_noon = get_monday_noon(now_dt)
    return baseline_at_dt < monday_noon

def next_2h_tick_wallclock(now_dt: datetime) -> datetime:
    """Next even 2h boundary (00,02,...,22) at wall clock in UTC."""
    next_hour = ((now_dt.hour // 2) + 1) * 2
    day = now_dt.date()
    if next_hour >= 24:
        next_hour -= 24
        day = day + timedelta(days=1)
    return datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc).replace(hour=next_hour)

# -------------------------
# CRM loading + Purchases filter
# -------------------------
def load_crm_filtered_df() -> pd.DataFrame:
    """
    Load CRM rows from Supabase table {CRM_TABLE}, applying the SAME first filter you used locally:
    - drop rows where temp name contains 'purchases' (case-insensitive)
    Uses server-side filter when available, else falls back to client-side.
    Returns dataframe with columns: lv_name, lv_accountidname, lv_tempname
    """
    cols = f"{CRM_COL_ACCOUNT_ID}, {CRM_COL_CUSTOMER}, {CRM_COL_TEMP_NAME}"
    q = sb.table(CRM_TABLE).select(cols)

    try:
        # Prefer server-side filter (fast + reduces payload)
        rows = q.not_.ilike(CRM_COL_TEMP_NAME, "%purchases%").execute().data or []
        filtered_count = len(rows)
        print(f"[CRM] Loaded {filtered_count:,} rows after server-side Purchases filter.")
    except Exception as e:
        # Some older supabase-py builds may not expose not_.ilike; fall back to client filter
        print(f"[CRM] Server-side filter not available ({e}); using client-side filter.")
        all_rows = q.execute().data or []
        before = len(all_rows)
        rows = [r for r in all_rows if "purchases" not in str(r.get(CRM_COL_TEMP_NAME, "")).lower()]
        after = len(rows)
        print(f"[CRM] Loaded {before:,} -> {after:,} after client-side Purchases filter.")

    df = pd.DataFrame(rows)
    # Ensure columns exist (defensive)
    for c in (CRM_COL_ACCOUNT_ID, CRM_COL_CUSTOMER, CRM_COL_TEMP_NAME):
        if c not in df.columns:
            df[c] = None

    return df.reset_index(drop=True)

# -------------------------
# Sirix fetch (EXACT mirror)
# -------------------------
def fetch_sirix_data(user_id: Any) -> Optional[Dict[str, Any]]:
    """Fetch Country, Plan, Balance, Equity, OpenPnL + blown flag + API GroupName (exactly like your script)."""
    try:
        if user_id is None or (isinstance(user_id, float) and math.isnan(user_id)):
            return None

        clean_user_id = str(int(float(user_id))).strip()

        headers = {
            "Authorization": f"Bearer {SIRIX_TOKEN}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        payload = {
            "UserID": clean_user_id,
            "GetOpenPositions": False,
            "GetPendingPositions": False,
            "GetClosePositions": False,
            "GetMonetaryTransactions": True,  # REQUIRED for blown detection + plan
        }

        resp = requests.post(API_URL, headers=headers, json=payload, timeout=20)
        if resp.status_code != 200:
            print(f"[!] API {resp.status_code} for {clean_user_id}")
            return None
        data = resp.json() or {}

        # Country
        country = (data.get("UserData") or {}).get("UserDetails", {}).get("Country")

        # Account Balance
        bal = (data.get("UserData") or {}).get("AccountBalance") or {}
        balance = bal.get("Balance")
        equity = bal.get("Equity")
        open_pnl = bal.get("OpenPnL")

        # GroupName (API-side Purchases safety filter)
        group_info = (data.get("UserData") or {}).get("GroupInfo") or {}
        group_name = group_info.get("GroupName")
        is_purchase_group = "purchase" in str(group_name or "").lower()

        # Monetary transactions (for blown + plan)
        txns = data.get("MonetaryTransactions") or []
        blown_up = any("zero balance" in str(t.get("Comment", "")).lower() for t in txns)

        plan = None
        for t in txns:
            if str(t.get("Comment", "")).lower().startswith("initial balance"):
                plan = t.get("Amount")
                break

        return {
            "Country": country,
            "Plan": plan,
            "Balance": balance,
            "Equity": equity,
            "OpenPnL": open_pnl,
            "BlownUp": blown_up,
            "GroupName": group_name,
            "IsPurchaseGroup": is_purchase_group,
        }
    except Exception as e:
        print(f"[!] fetch_sirix_data exception for UserID={user_id}: {e}")
        return None

# -------------------------
# DB helpers
# -------------------------
def upsert_row(table: str, row: Dict[str, Any]) -> None:
    """Upsert one row and always refresh updated_at."""
    row = {**row, "updated_at": now_iso_utc()}
    sb.table(table).upsert(row, on_conflict="account_id").execute()

def delete_if_exists(table: str, account_id: str) -> None:
    sb.table(table).delete().eq("account_id", account_id).execute()

def move_exclusive(account_id: str, target_table: str) -> None:
    """Ensure account_id only exists in target_table by removing from all others."""
    others = {TABLE_ACTIVE, TABLE_BLOWN, TABLE_PURCHASES, TABLE_PLAN50K} - {target_table}
    for t in others:
        delete_if_exists(t, account_id)

def classify_and_payload(row_from_crm: Dict[str, Any],
                         sirix: Optional[Dict[str, Any]],
                         pct_change: Optional[float]) -> Tuple[str, Dict[str, Any]]:
    """
    EXACT classification rules from your local script.
    Returns (table_name, payload) where payload matches the target schema.
    """
    account_id = str(row_from_crm.get(CRM_COL_ACCOUNT_ID))
    payload = {
        "account_id": account_id,
        "customer_name": row_from_crm.get(CRM_COL_CUSTOMER),
        "country": (sirix or {}).get("Country") if sirix else None,
        "plan": (sirix or {}).get("Plan") if sirix else None,
        "balance": (sirix or {}).get("Balance") if sirix else None,
        "equity": (sirix or {}).get("Equity") if sirix else None,
        "open_pnl": (sirix or {}).get("OpenPnL") if sirix else None,
        "pct_change": pct_change,
    }

    # 1) Blown-up (MonetaryTransactions comment includes "Zero Balance")
    if sirix and sirix.get("BlownUp"):
        return (TABLE_BLOWN, payload)

    # 2) Purchases by API GroupName
    if sirix and sirix.get("IsPurchaseGroup"):
        payload["group_name"] = sirix.get("GroupName")
        return (TABLE_PURCHASES, payload)

    # 3) Plan = 50000 (exact)
    plan_val = None
    if sirix:
        try:
            if sirix.get("Plan") is not None:
                plan_val = float(sirix["Plan"])
        except (TypeError, ValueError):
            plan_val = None
    if plan_val is not None and abs(plan_val - 50000.0) < 1e-6:
        return (TABLE_PLAN50K, payload)

    # 4) Otherwise Active
    return (TABLE_ACTIVE, payload)

# -------------------------
# Baseline helpers (DB)
# -------------------------
def get_current_baseline_at() -> Optional[datetime]:
    """Return the latest baseline_at (max) from e2t_baseline, or None if empty."""
    res = sb.table(TABLE_BASELINE).select("baseline_at").order("baseline_at", desc=True).limit(1).execute()
    rows = res.data or []
    if not rows:
        return None
    try:
        return datetime.fromisoformat(rows[0]["baseline_at"].replace("Z", "+00:00"))
    except Exception:
        return None

def load_baseline_map() -> Dict[str, float]:
    """account_id -> baseline_equity"""
    res = sb.table(TABLE_BASELINE).select("account_id, baseline_equity").execute()
    rows = res.data or []
    out: Dict[str, float] = {}
    for r in rows:
        try:
            out[str(r["account_id"])] = float(r["baseline_equity"])
        except Exception:
            pass
    return out

# -------------------------
# Runs
# -------------------------
def seed_baseline(now_utc_iso: str) -> None:
    """
    Baseline: iterate CRM rows, classify each account into the correct bucket,
    and write baseline rows ONLY for Active accounts (exactly like your local script).
    """
    print("[BASELINE] Seeding weekly baseline… (streaming writes)")
    df = load_crm_filtered_df()
    total = len(df)
    print(f"[BASELINE] Filtered CRM rows: {total}")

    flushed = 0
    for i, row in df.iterrows():
        user_id = row.get(CRM_COL_ACCOUNT_ID)
        print(f"[{i+1}/{total}] {user_id}")
        sirix = fetch_sirix_data(user_id)

        # Baseline run: pct_change is None
        table, payload = classify_and_payload(row, sirix, None)
        upsert_row(table, payload)
        move_exclusive(payload["account_id"], table)

        # Only Active rows get baseline_equity
        if table == TABLE_ACTIVE:
            eq = (sirix or {}).get("Equity")
            if eq is not None:
                upsert_row(TABLE_BASELINE, {
                    "account_id": payload["account_id"],
                    "baseline_equity": float(eq),
                    "baseline_at": now_utc_iso,
                })

        flushed += 1
        if RATE_DELAY_SEC > 0:
            time.sleep(RATE_DELAY_SEC)
        if flushed % 300 == 0 or flushed == total:
            print(f"[BASELINE] Flushed {flushed}/{total}")

def run_update() -> None:
    """
    2-hour update: compute pct_change vs baseline for Active-classified rows only.
    Other buckets get pct_change=None.
    """
    print("[UPDATE] Running 2h update…")
    base = load_baseline_map()

    df = load_crm_filtered_df()
    total = len(df)
    print(f"[UPDATE] Filtered CRM rows: {total}")

    for i, row in df.iterrows():
        user_id = row.get(CRM_COL_ACCOUNT_ID)
        sirix = fetch_sirix_data(user_id)

        # Classify first
        table, payload = classify_and_payload(row, sirix, None)

        # Only Active rows get pct_change (if baseline present)
        if table == TABLE_ACTIVE and sirix:
            equity = sirix.get("Equity")
            base_eq = base.get(str(user_id))
            pct_change = None
            if base_eq not in (None, 0) and equity not in (None,):
                try:
                    pct_change = ((equity - base_eq) / base_eq) * 100.0
                except Exception:
                    pct_change = None
            payload["pct_change"] = pct_change

        upsert_row(table, payload)
        move_exclusive(payload["account_id"], table)

        if RATE_DELAY_SEC > 0:
            time.sleep(RATE_DELAY_SEC)

# -------------------------
# Main scheduler
# -------------------------
def main():
    print(f"[SERVICE] E2T worker running. TZ={E2T_TZ_LABEL}, TEST_MODE={TEST_MODE}, RUN_NOW_ON_START={RUN_NOW_ON_START}")

    # Determine baseline state from DB
    baseline_at = get_current_baseline_at()
    now_dt = now_utc()

    # TEST mode: seed baseline immediately if missing/outdated
    if TEST_MODE:
        if baseline_at is None or need_new_week(baseline_at, now_dt):
            print("[TEST MODE] Baseline missing/outdated -> seeding now.")
            seed_baseline(now_iso_utc())
        next_run = next_2h_tick_wallclock(now_utc())
    else:
        # REAL weekly behavior
        if baseline_at is None or need_new_week(baseline_at, now_dt):
            target = get_monday_noon(now_dt)
            if now_dt >= target:
                print("[SCHED] Seeding new weekly baseline now.")
                seed_baseline(now_iso_utc())
                next_run = next_2h_tick_wallclock(now_utc())
            else:
                secs = (target - now_dt).total_seconds()
                hh = int(secs // 3600); mm = int((secs % 3600) // 60); ss = int(secs % 60)
                print(f"[SCHED] Waiting until Monday 12:00 to seed baseline (~{hh}h {mm}m {ss}s).")
                time.sleep(max(5.0, secs))
                seed_baseline(now_iso_utc())
                next_run = next_2h_tick_wallclock(now_utc())
        else:
            next_run = next_2h_tick_wallclock(now_utc())

    # Optional one-off 'run now' update
    if RUN_NOW_ON_START:
        print("[RUN-NOW] Performing one immediate fetch now (then resume 2h schedule).")
        baseline_at = get_current_baseline_at()
        if TEST_MODE:
            run_update()
        else:
            if baseline_at is None or need_new_week(baseline_at, now_utc()):
                print("[RUN-NOW] Baseline missing/outdated; running update anyway (pct_change may be None).")
            run_update()
        next_run = next_2h_tick_wallclock(now_utc())

    # Loop forever on 2h schedule
    while True:
        now_dt = now_utc()
        if next_run > now_dt:
            secs = (next_run - now_dt).total_seconds()
            hh = int(secs // 3600); mm = int((secs % 3600) // 60); ss = int(secs % 60)
            print(f"[SCHED] Next run at {next_run.isoformat()} (in {hh:02d}:{mm:02d}:{ss:02d}).")
            time.sleep(secs)

        baseline_at = get_current_baseline_at()
        if baseline_at is None or need_new_week(baseline_at, now_utc()):
            print("[SCHED] Baseline missing/outdated on wake; switching to baseline seeding.")
            seed_baseline(now_iso_utc())
        else:
            run_update()

        next_run = next_2h_tick_wallclock(now_utc())

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[EXIT] Stopped by user.")
