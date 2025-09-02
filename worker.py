import os, time, requests
from datetime import datetime, timedelta, timezone
import pandas as pd
from supabase import create_client

# --- config via env ---
SUPABASE_URL  = os.environ["SUPABASE_URL"]
SUPABASE_KEY  = os.environ["SUPABASE_SERVICE_KEY"]
SIRIX_TOKEN   = os.environ["SIRIX_TOKEN"]
TEST_MODE     = os.getenv("TEST_MODE", "false").lower() == "true"

API_URL = "https://restapi-real3.sirixtrader.com/api/UserStatus/GetUserTransactions"
sb = create_client(SUPABASE_URL, SUPABASE_KEY)

# === Helpers mapped from your code ===
def get_monday_noon(dt):
    monday = dt - timedelta(days=dt.weekday())
    return datetime(monday.year, monday.month, monday.day, 12, 0, 0, tzinfo=dt.tzinfo)

def need_new_week(baseline_at, now):
    if baseline_at is None:
        return True
    return baseline_at < get_monday_noon(now)

def fetch_sirix(user_id: str):
    headers = {"Authorization": f"Bearer {SIRIX_TOKEN}", "Content-Type": "application/json", "Accept": "application/json"}
    payload = {
        "UserID": str(int(float(user_id))).strip(),
        "GetOpenPositions": False,
        "GetPendingPositions": False,
        "GetClosePositions": False,
        "GetMonetaryTransactions": True
    }
    r = requests.post(API_URL, headers=headers, json=payload, timeout=20)
    if r.status_code != 200:
        print(f"[!] API {r.status_code} for {user_id}")
        return None
    d = r.json()

    country = (d.get("UserData") or {}).get("UserDetails", {}).get("Country")
    bal = (d.get("UserData") or {}).get("AccountBalance") or {}
    balance, equity, open_pnl = bal.get("Balance"), bal.get("Equity"), bal.get("OpenPnL")
    group_name = ((d.get("UserData") or {}).get("GroupInfo") or {}).get("GroupName")
    is_purchase = "purchase" in str(group_name or "").lower()
    txns = d.get("MonetaryTransactions") or []
    blown = any("zero balance" in str(t.get("Comment","")).lower() for t in txns)
    plan = None
    for t in txns:
        if str(t.get("Comment","")).lower().startswith("initial balance"):
            plan = t.get("Amount"); break

    return {
        "Country": country, "Plan": plan, "Balance": balance, "Equity": equity, "OpenPnL": open_pnl,
        "GroupName": group_name, "IsPurchaseGroup": is_purchase, "BlownUp": blown,
    }

# === Baseline in DB ===
def load_baseline_map():
    data = sb.table("baselines").select("account_id, baseline_equity, baseline_at").execute().data or []
    base_map = {str(r["account_id"]): float(r["baseline_equity"]) for r in data}
    baseline_at = max((datetime.fromisoformat(r["baseline_at"]) for r in data), default=None)
    return baseline_at, base_map

def save_baseline_map(baseline_at, base_map):
    rows = [{"account_id": k, "baseline_equity": float(v), "baseline_at": baseline_at.isoformat()} for k, v in base_map.items()]
    if rows:
        sb.table("baselines").upsert(rows, on_conflict="account_id").execute()
        print(f"[OK] Baseline saved at {baseline_at} for {len(rows)} accounts.")

# === Read input (Supabase instead of Excel) ===
def load_input_df():
    # pull all rows; you can add paging if huge
    rows = sb.table("crm_input").select("lv_tempname, lv_accountidname, lv_name").execute().data or []
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    # your filter: remove 'Purchases' in Lv_TempName
    before = len(df)
    df = df[~df['lv_tempname'].fillna('').str.contains('Purchases', case=False)].reset_index(drop=True)
    print(f"[INFO] Loaded {before:,} input rows -> {len(df):,} after filter.")
    return df

def run_once(mode, baseline_map, baseline_at):
    df = load_input_df()
    results = []
    total = len(df)
    for i, row in df.iterrows():
        user_id = row.get("lv_name")
        print(f"[{i+1}/{total}] {user_id}")
        s = fetch_sirix(user_id)

        if not s:
            pct = None
            equity = None
        else:
            equity = s.get("Equity")
            if mode == "baseline":
                if equity is not None:
                    baseline_map[str(user_id)] = float(equity)
                pct = None
            else:
                base_eq = baseline_map.get(str(user_id))
                pct = ((equity - base_eq) / base_eq) * 100 if (base_eq not in (None,0) and equity is not None) else None

        results.append({
            "customer_name": row.get("lv_accountidname"),
            "account_id": str(row.get("lv_name")),
            "country": s.get("Country") if s else None,
            "plan": s.get("Plan") if s else None,
            "balance": s.get("Balance") if s else None,
            "equity": equity,
            "open_pnl": s.get("OpenPnL") if s else None,
            "pct_change": pct
        })
        time.sleep(0.2)  # keep your rate-limit

    # create a run
    run_id = sb.table("runs").insert({"run_at": datetime.now(timezone.utc).isoformat()}).execute().data[0]["id"]

    # sort like your Excel did (desc, NaN last)
    if mode == "update":
        results.sort(key=lambda r: (float("-inf") if r["pct_change"] is None else r["pct_change"]), reverse=True)

    # bulk upsert
    payload = [{**r, "run_id": run_id} for r in results]
    CHUNK = 1000
    for i in range(0, len(payload), CHUNK):
        sb.table("active_results").upsert(payload[i:i+CHUNK]).execute()

    if mode == "baseline":
        save_baseline_map(baseline_at, baseline_map)

    print(f"[OK] saved run {run_id} with {len(results)} rows")

def main():
    now = datetime.now(timezone.utc)
    baseline_at, base_map = load_baseline_map()

    if TEST_MODE:
        if baseline_at is None:
            baseline_at = now
            run_once("baseline", base_map, baseline_at)
        run_once("update", base_map, baseline_at)
        return

    if baseline_at is None or need_new_week(baseline_at, now):
        baseline_at = now
        run_once("baseline", base_map, baseline_at)
    else:
        run_once("update", base_map, baseline_at)

if __name__ == "__main__":
    main()
