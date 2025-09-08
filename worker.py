# worker.py
# --------------------------------------------------------------------
# E2T background worker (Heroku)
#
# This version **does not** use the 'postgrest' Python client.
# It calls Supabase PostgREST directly with `requests`, which keeps
# everything synchronous and avoids async/coroutine issues.
#
# Key points:
#  - Uses SUPABASE_* env vars for auth
#  - Provides tiny helpers: pg_select / pg_upsert / pg_delete
#  - Paginates CRM with limit/offset
#  - Retries transient network errors with backoff
#  - Keeps your Sirix API logic & classification unchanged
# --------------------------------------------------------------------

import os
import sys
import time
import math
import json
import requests
import pandas as pd
from typing import Optional, Tuple, Dict, Any, List
from datetime import datetime, timedelta, timezone
from collections import Counter
import random  # for jitter in backoff

# -------------------------
# Environment configuration
# -------------------------
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", os.environ.get("SUPABASE_ANON_KEY", "")).strip()
if not SUPABASE_URL or not SUPABASE_KEY:
    print("[FATAL] SUPABASE_URL / SUPABASE_*_KEY not set.", file=sys.stderr)
    sys.exit(1)

# Base REST endpoint and default headers for PostgREST
BASE_REST = f"{SUPABASE_URL}/rest/v1"
PG_HEADERS_BASE = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Accept": "application/json",
    "Content-Type": "application/json",
    # Optional: specify schema headers (default is public)
    "Accept-Profile": "public",
    "Content-Profile": "public",
}

CRM_TABLE = os.environ.get("CRM_TABLE", "lv_tpaccount").strip()

API_URL = os.environ.get("SIRIX_API_URL", "https://restapi-real3.sirixtrader.com/api/UserStatus/GetUserTransactions").strip()
SIRIX_TOKEN = os.environ.get("SIRIX_TOKEN", "").strip()

TEST_MODE = os.environ.get("E2T_TEST_MODE", "false").lower() == "true"
RUN_NOW_ON_START = os.environ.get("E2T_RUN_NOW", "true").lower() == "true"
RATE_DELAY_SEC = float(os.environ.get("E2T_RATE_DELAY_SEC", "0.2"))
E2T_TZ_LABEL = os.environ.get("E2T_TZ_LABEL", "UTC")

# Destination tables
TABLE_ACTIVE     = "e2t_active"
TABLE_BLOWN      = "e2t_blown"
TABLE_PURCHASES  = "e2t_purchases_api"
TABLE_PLAN50K    = "e2t_plan50k"
TABLE_BASELINE   = "e2t_baseline"

# CRM column names (all lowercase in Supabase)
CRM_COL_ACCOUNT_ID = "lv_name"
CRM_COL_CUSTOMER   = "lv_accountidname"
CRM_COL_TEMP_NAME  = "lv_tempname"


# -------------------------
# Tiny PostgREST helpers
# -------------------------
def _retryable(err_text: str) -> bool:
    """Heuristic: which network-ish errors should we retry?"""
    signals = (
        "RemoteProtocolError", "ConnectionResetError", "ServerDisconnected",
        "ReadTimeout", "WriteError", "PoolTimeout", "Timed out",
        "Connection reset", "EOF", "temporarily unavailable",
    )
    et = err_text or ""
    return any(s in et for s in signals)


def pg_select(
    table: str,
    select: str,
    *,
    filters: Dict[str, str] | None = None,
    order: str | None = None,
    desc: bool = False,
    limit: int | None = None,
    offset: int | None = None
) -> List[Dict[str, Any]]:
    """
    Generic SELECT from PostgREST.
    - `filters` must use PostgREST syntax values (e.g., {"account_id": "eq.123"})
      We assemble the querystring like: ?select=...&account_id=eq.123
    - `order` becomes 'order=col.asc/desc'
    - `limit`/`offset` paginate the result
    Returns a list[dict].
    """
    params: Dict[str, Any] = {"select": select}
    if order:
        params["order"] = f"{order}.{'desc' if desc else 'asc'}"
    if limit is not None:
        params["limit"] = limit
    if offset is not None:
        params["offset"] = offset
    if filters:
        params.update(filters)

    backoff = 0.5
    for attempt in range(1, 7):
        try:
            r = requests.get(f"{BASE_REST}/{table}", headers=PG_HEADERS_BASE, params=params, timeout=30)
            if r.status_code in (200, 206):  # 206 = partial content (range)
                return r.json() or []
            if r.status_code == 406:  # Not Acceptable can mean "no rows" with certain selects
                return []
            r.raise_for_status()
        except Exception as e:
            msg = str(e)
            if attempt == 6 or not _retryable(msg):
                print(f"[ERROR] pg_select {table}: {msg[:200]}")
                raise
            time.sleep(backoff * (1.0 + random.random() * 0.3))
            backoff = min(backoff * 2, 10.0)
    return []


def pg_select_all(table: str, select: str, *, filters: Dict[str, str] | None = None, order: str | None = None, desc: bool = False, page_size: int = 1000) -> List[Dict[str, Any]]:
    """Fetch **all** rows by paging with limit/offset until empty."""
    out: List[Dict[str, Any]] = []
    offset = 0
    while True:
        chunk = pg_select(table, select, filters=filters, order=order, desc=desc, limit=page_size, offset=offset)
        if not chunk:
            break
        out.extend(chunk)
        if len(chunk) < page_size:
            break
        offset += page_size
    return out


def pg_upsert(table: str, row: dict, on_conflict: str = "account_id") -> None:
    """
    UPSERT via PostgREST:
      - POST with Prefer: resolution=merge-duplicates
      - on_conflict=col_name(s)
    """
    params = {"on_conflict": on_conflict}
    headers = {**PG_HEADERS_BASE, "Prefer": "resolution=merge-duplicates"}
    backoff = 0.5
    for attempt in range(1, 7):
        try:
            r = requests.post(f"{BASE_REST}/{table}", headers=headers, params=params, json=row, timeout=30)
            if r.status_code in (200, 201, 204):
                return
            r.raise_for_status()
        except Exception as e:
            msg = str(e)
            if attempt == 6 or not _retryable(msg):
                print(f"[ERROR] pg_upsert {table}: {msg[:200]} | row={str(row)[:180]}")
                return
            time.sleep(backoff * (1.0 + random.random() * 0.3))
            backoff = min(backoff * 2, 10.0)


def pg_delete(table: str, filters: Dict[str, str]) -> None:
    """DELETE rows matching the given PostgREST filters, e.g. {'account_id': 'eq.123'}"""
    params: Dict[str, str] = {}
    params.update(filters)
    backoff = 0.5
    for attempt in range(1, 7):
        try:
            r = requests.delete(f"{BASE_REST}/{table}", headers=PG_HEADERS_BASE, params=params, timeout=30)
            if r.status_code in (200, 204):
                return
            r.raise_for_status()
        except Exception as e:
            msg = str(e)
            if attempt == 6 or not _retryable(msg):
                print(f"[ERROR] pg_delete {table}: {msg[:200]} | filters={filters}")
                return
            time.sleep(backoff * (1.0 + random.random() * 0.3))
            backoff = min(backoff * 2, 10.0)


# -------------------------
# Time helpers (UTC always)
# -------------------------
def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def now_iso_utc() -> str:
    return now_utc().isoformat()

def get_monday_noon(dt_local: datetime) -> datetime:
    """Return the Monday 12:00 for the week containing dt_local."""
    monday = dt_local - timedelta(days=dt_local.weekday())
    return monday.replace(hour=12, minute=0, second=0, microsecond=0)

def need_new_week(baseline_at_dt: Optional[datetime], now_dt: datetime) -> bool:
    """True if baseline is missing or older than this week's Monday noon."""
    if baseline_at_dt is None:
        return True
    monday_noon = get_monday_noon(now_dt)
    return baseline_at_dt < monday_noon

def next_2h_tick_wallclock(now_dt: datetime) -> datetime:
    """Round forward to the next 2-hour wallclock (00, 02, 04, ...)."""
    next_hour = ((now_dt.hour // 2) + 1) * 2
    day = now_dt.date()
    if next_hour >= 24:
        next_hour -= 24
        day = day + timedelta(days=1)
    # Make a tz-aware datetime at the next 2-hour boundary
    return datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc).replace(hour=next_hour)


# -------------------------
# CRM loader with pagination
# -------------------------
def fetch_crm_chunk(offset: int, limit: int) -> List[Dict[str, Any]]:
    """
    Fetch a CRM chunk [offset, offset+limit).
    Try server-side NOT ILIKE '%purchases%' on CRM_COL_TEMP_NAME; if that fails,
    fetch unfiltered and filter client-side.
    """
    cols = f"{CRM_COL_ACCOUNT_ID},{CRM_COL_CUSTOMER},{CRM_COL_TEMP_NAME}"
    try:
        # PostgREST filter syntax: <col>=not.ilike.*purchases*
        data = pg_select(
            CRM_TABLE,
            cols,
            filters={CRM_COL_TEMP_NAME: "not.ilike.*purchases*"},
            limit=limit,
            offset=offset,
        )
        return data
    except Exception:
        data = pg_select(CRM_TABLE, cols, limit=limit, offset=offset)
        return [r for r in data if "purchases" not in str(r.get(CRM_COL_TEMP_NAME, "")).lower()]


def load_crm_filtered_df(page_size: int = 1000, hard_limit: Optional[int] = None) -> pd.DataFrame:
    """
    Load ALL CRM rows via pagination, filtering out 'Purchases' rows (case-insensitive),
    returning a dataframe with lowercase CRM columns.
    """
    rows: List[Dict[str, Any]] = []
    offset = 0
    total_loaded = 0
    while True:
        chunk = fetch_crm_chunk(offset, page_size)
        if not chunk:
            break
        rows.extend(chunk)
        total_loaded += len(chunk)
        print(f"[CRM] Loaded chunk: {len(chunk)} rows (total {total_loaded})")
        offset += page_size
        if hard_limit is not None and total_loaded >= hard_limit:
            rows = rows[:hard_limit]
            break

    if not rows:
        print(f"[WARN] CRM table '{CRM_TABLE}' returned 0 rows after filter.")
        return pd.DataFrame(columns=[CRM_COL_ACCOUNT_ID, CRM_COL_CUSTOMER, CRM_COL_TEMP_NAME])

    df = pd.DataFrame(rows)
    for c in [CRM_COL_ACCOUNT_ID, CRM_COL_CUSTOMER, CRM_COL_TEMP_NAME]:
        if c not in df.columns:
            df[c] = None
    df = df.reset_index(drop=True)
    print(f"[CRM] Loaded {len(df):,} rows after server-side Purchases filter (with pagination).")
    return df


# -------------------------
# Sirix fetch
# -------------------------
def fetch_sirix_data(user_id: Any) -> Optional[Dict[str, Any]]:
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
            "GetMonetaryTransactions": True,
        }

        resp = requests.post(API_URL, headers=headers, json=payload, timeout=20)
        if resp.status_code != 200:
            print(f"[!] API {resp.status_code} for {clean_user_id}")
            return None
        data = resp.json() or {}

        country = (data.get("UserData") or {}).get("UserDetails", {}).get("Country")
        bal = (data.get("UserData") or {}).get("AccountBalance") or {}
        balance = bal.get("Balance")
        equity = bal.get("Equity")
        open_pnl = bal.get("OpenPnL")

        group_info = (data.get("UserData") or {}).get("GroupInfo") or {}
        group_name = group_info.get("GroupName")
        is_purchase_group = "purchase" in str(group_name or "").lower()

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
# DB helpers (table ops)
# -------------------------
def upsert_row(table: str, row: dict, on_conflict: str = "account_id") -> None:
    """UPSERT one row with retry/backoff (via pg_upsert)."""
    pg_upsert(table, row, on_conflict=on_conflict)

def delete_if_exists(table: str, account_id: str) -> None:
    """DELETE by account_id."""
    pg_delete(table, {"account_id": f"eq.{account_id}"})

def move_exclusive(account_id: str, target_table: str) -> None:
    """
    Move an account exclusively into one table:
      - delete from other destination tables
      - keep only in target_table
    """
    others = {TABLE_ACTIVE, TABLE_BLOWN, TABLE_PURCHASES, TABLE_PLAN50K} - {target_table}
    for t in others:
        delete_if_exists(t, account_id)


def classify_and_payload(row_from_crm: Dict[str, Any],
                         sirix: Optional[Dict[str, Any]],
                         pct_change: Optional[float]) -> Tuple[str, Dict[str, Any]]:
    """
    Decide which table to upsert into + build the payload row.
    Priority:
      1) Blown-up (by Sirix MonetaryTransactions)
      2) Purchases group (by Sirix GroupName)
      3) Plan = 50000
      4) Otherwise Active
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

    # 1) Blown-up
    if sirix and sirix.get("BlownUp"):
        return (TABLE_BLOWN, payload)

    # 2) Purchases by API GroupName
    if sirix and sirix.get("IsPurchaseGroup"):
        payload["group_name"] = sirix.get("GroupName")
        return (TABLE_PURCHASES, payload)

    # 3) Plan = 50000
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
    """Return the most recent baseline_at (as datetime) or None."""
    rows = pg_select(TABLE_BASELINE, "baseline_at", order="baseline_at", desc=True, limit=1)
    if not rows:
        return None
    try:
        return datetime.fromisoformat((rows[0]["baseline_at"] or "").replace("Z", "+00:00"))
    except Exception:
        return None


def load_baseline_map() -> Dict[str, float]:
    """
    Load ALL baseline rows -> {account_id: baseline_equity}
    Uses `pg_select_all` to ensure we don’t stop at 1,000 rows.
    """
    rows = pg_select_all(TABLE_BASELINE, "account_id,baseline_equity")
    out: Dict[str, float] = {}
    for r in rows:
        try:
            out[str(r["account_id"])] = float(r["baseline_equity"])
        except Exception:
            pass
    return out


# -------------------------
# Runs with verbose logging
# -------------------------
def seed_baseline(now_utc_iso: str) -> None:
    """
    Full crawl (for weekly baseline):
      - Fetch CRM list
      - Fetch Sirix for each
      - Classify into tables
      - For ACTIVE: write baseline (equity, baseline_at)
    """
    print("[BASELINE] Seeding weekly baseline…")
    df = load_crm_filtered_df()
    total = len(df)
    if total == 0:
        print("[BASELINE] No CRM rows to process.")
        return

    blown = purchases = plan50k = active = 0
    start_ts = time.time()

    for i, row in df.iterrows():
        user_id = row.get(CRM_COL_ACCOUNT_ID)
        print(f"[{i+1}/{total}] Fetching UserID: {user_id} ...")
        sirix = fetch_sirix_data(user_id)

        table, payload = classify_and_payload(row, sirix, None)

        if table == TABLE_BLOWN:
            blown += 1
            print(f"    ↳ [BLOWN-UP] UserID {user_id} -> BlownUp table.")
        elif table == TABLE_PURCHASES:
            purchases += 1
            print(f"    ↳ [PURCHASES(API)] UserID {user_id} -> Purchases table (GroupName='{(sirix or {}).get('GroupName')}').")
        elif table == TABLE_PLAN50K:
            plan50k += 1
            print(f"    ↳ [PLAN=50000] UserID {user_id} -> Plan50000 table.")
        else:
            active += 1

        upsert_row(table, payload)
        move_exclusive(payload["account_id"], table)

        # Baseline equity only for Active
        if table == TABLE_ACTIVE:
            eq = (sirix or {}).get("Equity")
            if eq is not None:
                upsert_row(TABLE_BASELINE, {
                    "account_id": payload["account_id"],
                    "baseline_equity": float(eq),
                    "baseline_at": now_utc_iso,
                })

        if RATE_DELAY_SEC > 0:
            time.sleep(RATE_DELAY_SEC)

    elapsed = int(time.time() - start_ts)
    mm, ss = divmod(elapsed, 60)

    print("\n===== SUMMARY (BASELINE) =====")
    print(f"Processed      : {total}")
    print(f"Blown-up       : {blown}")
    print(f"Purchases(API) : {purchases}")
    print(f"Plan=50000     : {plan50k}")
    print(f"Active (final) : {active}")
    print(f"[PROCESS COMPLETE] Run time: {mm:02d}:{ss:02d} (MM:SS)")


def run_update() -> None:
    """
    Incremental 2h update:
      - Load baseline map
      - Re-fetch CRM and Sirix
      - Compute pct_change for ACTIVE (vs baseline_equity)
      - Upsert to destination tables
    """
    print("[UPDATE] Running 2h update…")
    base = load_baseline_map()

    df = load_crm_filtered_df()
    total = len(df)
    if total == 0:
        print("[UPDATE] No CRM rows to process.")
        return

    blown = purchases = plan50k = active = 0
    pct_samples: List[float] = []
    start_ts = time.time()

    for i, row in df.iterrows():
        user_id = row.get(CRM_COL_ACCOUNT_ID)
        print(f"[{i+1}/{total}] Fetching UserID: {user_id} ...")
        sirix = fetch_sirix_data(user_id)

        table, payload = classify_and_payload(row, sirix, None)

        # Only Active rows get pct_change
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
            if pct_change is not None:
                pct_samples.append(float(pct_change))

        if table == TABLE_BLOWN:
            blown += 1
            print(f"    ↳ [BLOWN-UP] UserID {user_id} -> BlownUp table.")
        elif table == TABLE_PURCHASES:
            purchases += 1
            print(f"    ↳ [PURCHASES(API)] UserID {user_id} -> Purchases table (GroupName='{(sirix or {}).get('GroupName')}').")
        elif table == TABLE_PLAN50K:
            plan50k += 1
            print(f"    ↳ [PLAN=50000] UserID {user_id} -> Plan50000 table.")
        else:
            active += 1

        upsert_row(table, payload)
        move_exclusive(payload["account_id"], table)

        if RATE_DELAY_SEC > 0:
            time.sleep(RATE_DELAY_SEC)

    # Print Top3 % change from the run
    top3 = sorted([x for x in pct_samples if x is not None], reverse=True)[:3]
    elapsed = int(time.time() - start_ts)
    mm, ss = divmod(elapsed, 60)

    print("\n===== SUMMARY (UPDATE) =====")
    print(f"Processed      : {total}")
    print(f"Blown-up       : {blown}")
    print(f"Purchases(API) : {purchases}")
    print(f"Plan=50000     : {plan50k}")
    print(f"Active (final) : {active}")
    print(f"Top3 PctChange : {top3 if top3 else '[]'}")
    print(f"[PROCESS COMPLETE] Run time: {mm:02d}:{ss:02d} (MM:SS)")


# -------------------------
# Main scheduler
# -------------------------
def main():
    print(f"[SERVICE] E2T worker running. TZ={E2T_TZ_LABEL}, TEST_MODE={TEST_MODE}, RUN_NOW_ON_START={RUN_NOW_ON_START}")

    baseline_at = get_current_baseline_at()
    now_dt = now_utc()

    if TEST_MODE:
        if baseline_at is None or need_new_week(baseline_at, now_dt):
            print("[TEST MODE] Baseline missing/outdated -> seeding now.")
            seed_baseline(now_iso_utc())
        next_run = next_2h_tick_wallclock(now_utc())
    else:
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

    if RUN_NOW_ON_START:
        print("[RUN-NOW] Performing one immediate fetch now (then resume 2h schedule).")
        baseline_at = get_current_baseline_at()
        if not TEST_MODE and (baseline_at is None or need_new_week(baseline_at, now_utc())):
            print("[RUN-NOW] Baseline missing/outdated; running update anyway (pct_change may be None).")
        run_update()
        next_run = next_2h_tick_wallclock(now_utc())

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
        if not SIRIX_TOKEN:
            print("[FATAL] SIRIX_TOKEN is not set in Heroku config vars.", file=sys.stderr)
            sys.exit(1)
        main()
    except KeyboardInterrupt:
        print("\n[EXIT] Stopped by user.")
