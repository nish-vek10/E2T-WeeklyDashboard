# worker.py
import os
import sys
import time
import math
import json
import requests
import pandas as pd
from collections import Counter
from typing import Optional, Tuple, Dict, Any, List
from datetime import datetime, timedelta, timezone

# === Supabase client ===
# pip install supabase
from supabase import create_client, Client

# -------------------------
# Env / Config
# -------------------------
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", os.environ.get("SUPABASE_ANON_KEY", "")).strip()
if not SUPABASE_URL or not SUPABASE_KEY:
    print("[FATAL] SUPABASE_URL / SUPABASE_*_KEY not set.", file=sys.stderr)
    sys.exit(1)

# CRM table & column names in Supabase (all lowercase)
CRM_TABLE = os.environ.get("CRM_TABLE", "lv_tpaccount").strip()
CRM_COL_ACCOUNT_ID = "lv_name"
CRM_COL_CUSTOMER   = "lv_accountidname"
CRM_COL_TEMP_NAME  = "lv_tempname"

# Sirix API
API_URL = os.environ.get("SIRIX_API_URL", "https://restapi-real3.sirixtrader.com/api/UserStatus/GetUserTransactions").strip()
SIRIX_TOKEN = os.environ.get("SIRIX_TOKEN", "").strip()

if not SIRIX_TOKEN:
    print("[WARN] SIRIX_TOKEN env var is empty. Using requests without auth will fail.")

# Scheduling flags
TEST_MODE = os.environ.get("E2T_TEST_MODE", "false").lower() == "true"    # if True, seed baseline immediately if missing/outdated
RUN_NOW_ON_START = os.environ.get("E2T_RUN_NOW", "true").lower() == "true"
RATE_DELAY_SEC = float(os.environ.get("E2T_RATE_DELAY_SEC", "0.2"))

# Logging verbosity (leave True to mimic your local prints)
VERBOSE = os.environ.get("E2T_VERBOSE", "true").lower() == "true"

# Tables
TABLE_ACTIVE         = "e2t_active"
TABLE_BLOWN          = "e2t_blown"
TABLE_PURCHASES      = "e2t_purchases_api"
TABLE_PLAN50K        = "e2t_plan50k"
TABLE_BASELINE       = "e2t_baseline"
TABLE_ACTIVE_SORTED  = "e2t_active_sorted"  # snapshot sorted by pct_change desc (NULLS LAST)
TABLE_COUNTS         = "e2t_counts"         # single-row summary counts (optional)

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# -------------------------
# Time helpers
# -------------------------
def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def now_iso_utc() -> str:
    return now_utc().isoformat()

def get_monday_noon(dt_local: datetime) -> datetime:
    monday = dt_local - timedelta(days=dt_local.weekday())
    return monday.replace(hour=12, minute=0, second=0, microsecond=0)

def need_new_week(baseline_at_dt: Optional[datetime], now_dt: datetime) -> bool:
    if baseline_at_dt is None:
        return True
    monday_noon = get_monday_noon(now_dt)
    return baseline_at_dt < monday_noon

def next_2h_tick_wallclock(now_dt: datetime) -> datetime:
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
    Load CRM rows from {CRM_TABLE} with the SAME first filter you used locally:
    - drop rows where {CRM_COL_TEMP_NAME} ILIKE '%purchases%'
    Uses server-side not_.ilike when available; falls back to client-side filtering.
    Returns columns: lv_name, lv_accountidname, lv_tempname
    """
    cols = f"{CRM_COL_ACCOUNT_ID}, {CRM_COL_CUSTOMER}, {CRM_COL_TEMP_NAME}"
    try:
        q = sb.table(CRM_TABLE).select(cols)
        # try server-side filter
        rows = q.not_.ilike(CRM_COL_TEMP_NAME, "%purchases%").execute().data or []
        if VERBOSE:
            print(f"[CRM] Loaded {len(rows):,} rows after server-side Purchases filter.")
    except Exception:
        # fallback: fetch all + filter in python
        all_rows = sb.table(CRM_TABLE).select(cols).execute().data or []
        rows = [r for r in all_rows if "purchases" not in str(r.get(CRM_COL_TEMP_NAME, "")).lower()]
        if VERBOSE:
            print(f"[CRM] Loaded {len(all_rows):,} rows -> {len(rows):,} after client-side Purchases filter.")

    df = pd.DataFrame(rows, columns=[CRM_COL_ACCOUNT_ID, CRM_COL_CUSTOMER, CRM_COL_TEMP_NAME])
    df = df.reset_index(drop=True)
    return df

# -------------------------
# Sirix fetch (mirrors your local logic)
# -------------------------
def fetch_sirix_data(user_id: Any) -> Optional[Dict[str, Any]]:
    """Return Country, Plan, Balance, Equity, OpenPnL + BlownUp + GroupName + IsPurchaseGroup."""
    try:
        if user_id is None or (isinstance(user_id, float) and math.isnan(user_id)):
            return None

        clean_user_id = str(int(float(user_id))).strip()

        headers = {
            "Authorization": f"Bearer {SIRIX_TOKEN}" if SIRIX_TOKEN else "",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        payload = {
            "UserID": clean_user_id,
            "GetOpenPositions": False,
            "GetPendingPositions": False,
            "GetClosePositions": False,
            "GetMonetaryTransactions": True,  # required for Plan + blown detection
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
# DB helpers
# -------------------------
def upsert_row(table: str, row: Dict[str, Any]) -> None:
    row = {**row, "updated_at": now_iso_utc()}
    sb.table(table).upsert(row, on_conflict="account_id").execute()

def delete_if_exists(table: str, account_id: str) -> None:
    sb.table(table).delete().eq("account_id", account_id).execute()

def move_exclusive(account_id: str, target_table: str) -> None:
    """Ensure account_id exists only in target_table by removing it from the others."""
    others = {TABLE_ACTIVE, TABLE_BLOWN, TABLE_PURCHASES, TABLE_PLAN50K} - {target_table}
    for t in others:
        delete_if_exists(t, account_id)

def classify_and_payload(row_from_crm: Dict[str, Any],
                         sirix: Optional[Dict[str, Any]],
                         pct_change: Optional[float]) -> Tuple[str, Dict[str, Any]]:
    """Mirror your exact classification rules and shape payload to table schema."""
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

    if sirix and sirix.get("BlownUp"):
        return (TABLE_BLOWN, payload)

    if sirix and sirix.get("IsPurchaseGroup"):
        payload["group_name"] = sirix.get("GroupName")
        return (TABLE_PURCHASES, payload)

    plan_val = None
    if sirix and sirix.get("Plan") is not None:
        try:
            plan_val = float(sirix["Plan"])
        except (TypeError, ValueError):
            plan_val = None
    if plan_val is not None and abs(plan_val - 50000.0) < 1e-6:
        return (TABLE_PLAN50K, payload)

    return (TABLE_ACTIVE, payload)

# -------------------------
# Baseline helpers
# -------------------------
def get_current_baseline_at() -> Optional[datetime]:
    res = sb.table(TABLE_BASELINE).select("baseline_at").order("baseline_at", desc=True).limit(1).execute()
    rows = res.data or []
    if not rows:
        return None
    try:
        return datetime.fromisoformat(rows[0]["baseline_at"].replace("Z", "+00:00"))
    except Exception:
        return None

def load_baseline_map() -> Dict[str, float]:
    res = sb.table(TABLE_BASELINE).select("account_id, baseline_equity").execute()
    rows = res.data or []
    out = {}
    for r in rows:
        try:
            out[str(r["account_id"])] = float(r["baseline_equity"])
        except Exception:
            pass
    return out

# -------------------------
# Post-run helpers (sorted snapshot + counts)
# -------------------------
def refresh_active_sorted_and_counts() -> None:
    # Build sorted Active snapshot
    sorted_rows = sb.table(TABLE_ACTIVE).select(
        "account_id, customer_name, country, plan, balance, equity, open_pnl, pct_change, updated_at"
    ).order("pct_change", desc=True, nulls_last=True).execute().data or []

    # Clear e2t_active_sorted and insert fresh
    # (use a wide filter to match all rows; PostgREST requires a predicate)
    sb.table(TABLE_ACTIVE_SORTED).delete().gte("updated_at", "1900-01-01T00:00:00Z").execute()

    # Insert in chunks
    CHUNK = 1000
    for i in range(0, len(sorted_rows), CHUNK):
        sb.table(TABLE_ACTIVE_SORTED).insert(sorted_rows[i:i+CHUNK]).execute()

    # Update counts (single row)
    counts = {
        "active":         sb.table(TABLE_ACTIVE).select("account_id", count="exact").execute().count or 0,
        "blown":          sb.table(TABLE_BLOWN).select("account_id", count="exact").execute().count or 0,
        "purchases_api":  sb.table(TABLE_PURCHASES).select("account_id", count="exact").execute().count or 0,
        "plan50k":        sb.table(TABLE_PLAN50K).select("account_id", count="exact").execute().count or 0,
        "baseline":       sb.table(TABLE_BASELINE).select("account_id", count="exact").execute().count or 0,
        "updated_at":     now_iso_utc(),
    }
    sb.table(TABLE_COUNTS).upsert(counts).execute()

# -------------------------
# Runs
# -------------------------
def seed_baseline(now_utc_iso: str) -> None:
    print("[BASELINE] Seeding weekly baseline…")
    df = load_crm_filtered_df()
    total = len(df)
    print(f"[BASELINE] Filtered CRM rows: {total}")

    seen_ids: List[str] = []
    blown_ct = purchases_ct = plan50k_ct = active_ct = 0
    start_ts = time.time()

    for i, row in df.iterrows():
        user_id = row.get(CRM_COL_ACCOUNT_ID)
        if VERBOSE:
            print(f"[{i+1}/{total}] Fetching UserID: {user_id} ...")
        sirix = fetch_sirix_data(user_id)

        # Baseline run: pct_change=None
        table, payload = classify_and_payload(row, sirix, None)

        # Classification logs (like your local script)
        if sirix and sirix.get("BlownUp"):
            if VERBOSE: print(f"    ↳ [BLOWN-UP] UserID {user_id} -> BlownUp bucket.")
            blown_ct += 1
        elif sirix and sirix.get("IsPurchaseGroup"):
            if VERBOSE: print(f"    ↳ [PURCHASES(API)] UserID {user_id} -> Purchases bucket (GroupName='{sirix.get('GroupName')}').")
            purchases_ct += 1
        else:
            plan_val = None
            if sirix and sirix.get("Plan") is not None:
                try: plan_val = float(sirix["Plan"])
                except Exception: plan_val = None
            if plan_val is not None and abs(plan_val - 50000.0) < 1e-6:
                if VERBOSE: print(f"    ↳ [PLAN=50000] UserID {user_id} -> Plan50000 bucket.")
                plan50k_ct += 1
            else:
                active_ct += 1

        upsert_row(table, payload)
        move_exclusive(payload["account_id"], table)

        # Only Active rows seed baseline_equity
        if table == TABLE_ACTIVE:
            eq = (sirix or {}).get("Equity")
            if eq is not None:
                upsert_row(TABLE_BASELINE, {
                    "account_id": payload["account_id"],
                    "baseline_equity": float(eq),
                    "baseline_at": now_utc_iso,
                })

        seen_ids.append(str(user_id))
        if RATE_DELAY_SEC > 0:
            time.sleep(RATE_DELAY_SEC)

    # Post-run artifacts
    refresh_active_sorted_and_counts()

    elapsed = int(time.time() - start_ts)
    mm, ss = divmod(elapsed, 60)

    # Summary (like your local print)
    dup_counts = Counter(seen_ids)
    duplicates = {uid: cnt for uid, cnt in dup_counts.items() if cnt > 1}

    print("\n[OK] Baseline pass complete.")
    print("\n===== SUMMARY =====")
    print(f"Total processed: {len(seen_ids)}")
    print(f"Unique IDs     : {len(dup_counts)}")
    print(f"Duplicates     : {len(duplicates)}")
    if duplicates:
        for uid, cnt in duplicates.items():
            print(f" - {uid} ({cnt} times)")
    print(f"Blown-up       : {blown_ct} (table: {TABLE_BLOWN})")
    print(f"Purchases(API) : {purchases_ct} (table: {TABLE_PURCHASES})")
    print(f"Plan=50000     : {plan50k_ct} (table: {TABLE_PLAN50K})")
    print(f"Active (final) : {active_ct} (table: {TABLE_ACTIVE})")
    print(f"[PROCESS COMPLETE] Run time: {mm:02d}:{ss:02d} (MM:SS)")

def run_update() -> None:
    print("[UPDATE] Running 2h update…")
    base = load_baseline_map()

    df = load_crm_filtered_df()
    total = len(df)
    print(f"[UPDATE] Filtered CRM rows: {total}")

    seen_ids: List[str] = []
    blown_ct = purchases_ct = plan50k_ct = active_ct = 0
    start_ts = time.time()
    active_pct_list: List[float] = []

    for i, row in df.iterrows():
        user_id = row.get(CRM_COL_ACCOUNT_ID)
        if VERBOSE:
            print(f"[{i+1}/{total}] Fetching UserID: {user_id} ...")
        sirix = fetch_sirix_data(user_id)

        # classify first (pct_change will be filled for Active only)
        table, payload = classify_and_payload(row, sirix, None)

        # logs as per your script
        if sirix and sirix.get("BlownUp"):
            if VERBOSE: print(f"    ↳ [BLOWN-UP] UserID {user_id} -> BlownUp bucket.")
            blown_ct += 1
        elif sirix and sirix.get("IsPurchaseGroup"):
            if VERBOSE: print(f"    ↳ [PURCHASES(API)] UserID {user_id} -> Purchases bucket (GroupName='{sirix.get('GroupName')}').")
            purchases_ct += 1
        else:
            plan_val = None
            if sirix and sirix.get("Plan") is not None:
                try: plan_val = float(sirix["Plan"])
                except Exception: plan_val = None
            if plan_val is not None and abs(plan_val - 50000.0) < 1e-6:
                if VERBOSE: print(f"    ↳ [PLAN=50000] UserID {user_id} -> Plan50000 bucket.")
                plan50k_ct += 1
            else:
                # Active: compute pct_change if baseline present
                if sirix:
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
                        active_pct_list.append(pct_change)
                active_ct += 1

        upsert_row(table, payload)
        move_exclusive(payload["account_id"], table)

        seen_ids.append(str(user_id))
        if RATE_DELAY_SEC > 0:
            time.sleep(RATE_DELAY_SEC)

    # After processing, refresh sorted snapshot and counts
    refresh_active_sorted_and_counts()

    # Sorting note (like your local print)
    top3 = []
    if active_pct_list:
        top3 = sorted(active_pct_list, reverse=True)[:3]
    print(f"[SORT] Active sorted by pct_change (desc, NULLS LAST). Top3 PctChange: {top3}")

    # Summary
    elapsed = int(time.time() - start_ts)
    mm, ss = divmod(elapsed, 60)
    dup_counts = Counter(seen_ids)
    duplicates = {uid: cnt for uid, cnt in dup_counts.items() if cnt > 1}

    print("\n[OK] Update pass complete.")
    print("\n===== SUMMARY =====")
    print(f"Total processed: {len(seen_ids)}")
    print(f"Unique IDs     : {len(dup_counts)}")
    print(f"Duplicates     : {len(duplicates)}")
    if duplicates:
        for uid, cnt in duplicates.items():
            print(f" - {uid} ({cnt} times)")
    print(f"Blown-up       : {blown_ct} (table: {TABLE_BLOWN})")
    print(f"Purchases(API) : {purchases_ct} (table: {TABLE_PURCHASES})")
    print(f"Plan=50000     : {plan50k_ct} (table: {TABLE_PLAN50K})")
    print(f"Active (final) : {active_ct} (table: {TABLE_ACTIVE})")
    print(f"[PROCESS COMPLETE] Run time: {mm:02d}:{ss:02d} (MM:SS)")

# -------------------------
# Main loop
# -------------------------
def main():
    print(f"[SERVICE] E2T worker running. TEST_MODE={TEST_MODE}, RUN_NOW_ON_START={RUN_NOW_ON_START}")

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
                hh = int(secs // 3600)
                mm = int((secs % 3600) // 60)
                ss = int(secs % 60)
                print(f"[SCHED] Waiting until Monday 12:00 to seed baseline (~{hh}h {mm}m {ss}s).")
                time.sleep(max(5.0, secs))
                seed_baseline(now_iso_utc())
                next_run = next_2h_tick_wallclock(now_utc())
        else:
            next_run = next_2h_tick_wallclock(now_utc())

    if RUN_NOW_ON_START:
        print("[RUN-NOW] Performing one immediate fetch now (then resume 2h schedule).")
        baseline_at = get_current_baseline_at()
        run_update()
        next_run = next_2h_tick_wallclock(now_utc())

    while True:
        now_dt = now_utc()
        if next_run > now_dt:
            secs = (next_run - now_dt).total_seconds()
            hh = int(secs // 3600)
            mm = int((secs % 3600) // 60)
            ss = int(secs % 60)
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
