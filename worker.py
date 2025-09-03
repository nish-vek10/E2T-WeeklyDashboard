import os, time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import requests
from supabase import create_client, Client

# ----- Config from Heroku env -----
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]  # service role key ONLY on server
SIRIX_API_URL = os.environ.get("SIRIX_API_URL", "https://restapi-real3.sirixtrader.com/api/UserStatus/GetUserTransactions")
SIRIX_TOKEN = os.environ["SIRIX_TOKEN"]

# ----- Clients / constants -----
sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
LONDON = ZoneInfo("Europe/London")

# ----- Time helpers -----
def monday_noon_london_utc(now_utc: datetime) -> datetime:
    ldn = now_utc.astimezone(LONDON)
    monday = ldn - timedelta(days=ldn.weekday())
    monday_noon_ldn = datetime(monday.year, monday.month, monday.day, 12, 0, tzinfo=LONDON)
    return monday_noon_ldn.astimezone(timezone.utc)

def need_new_week(baseline_at_utc: datetime | None, now_utc: datetime) -> bool:
    if baseline_at_utc is None:
        return True
    return baseline_at_utc < monday_noon_london_utc(now_utc)

def next_even_hour_utc(now_utc: datetime) -> datetime:
    h = now_utc.hour
    tgt = now_utc.replace(minute=0, second=0, microsecond=0)
    if now_utc.minute == 0 and h % 2 == 0:
        return tgt
    add = 1 if (h % 2 == 1) else 2
    return tgt + timedelta(hours=add)

# ----- Sirix fetch -----
def fetch_sirix(uid: str):
    hdr = {"Authorization": f"Bearer {SIRIX_TOKEN}", "Content-Type": "application/json", "Accept": "application/json"}
    payload = {
        "UserID": str(uid),
        "GetOpenPositions": False,
        "GetPendingPositions": False,
        "GetClosePositions": False,
        "GetMonetaryTransactions": True
    }
    r = requests.post(SIRIX_API_URL, headers=hdr, json=payload, timeout=20)
    if r.status_code != 200:
        print(f"[SIRIX] {uid} -> HTTP {r.status_code}")
        return None
    data = r.json()
    country = (data.get("UserData") or {}).get("UserDetails", {}).get("Country")
    bal = (data.get("UserData") or {}).get("AccountBalance") or {}
    balance, equity, open_pnl = bal.get("Balance"), bal.get("Equity"), bal.get("OpenPnL")
    group_name = ((data.get("UserData") or {}).get("GroupInfo") or {}).get("GroupName")
    is_purchase_group = "purchase" in str(group_name or "").lower()
    txns = data.get("MonetaryTransactions") or []
    blown = any("zero balance" in str(t.get("Comment","")).lower() for t in txns)
    plan = next((t.get("Amount") for t in txns if str(t.get("Comment","")).lower().startswith("initial balance")), None)
    return {
        "Country": country, "Plan": plan, "Balance": balance, "Equity": equity, "OpenPnL": open_pnl,
        "BlownUp": blown, "GroupName": group_name, "IsPurchaseGroup": is_purchase_group
    }

# ----- Supabase helpers -----
def upsert(table: str, rows: list[dict], on_conflict="account_id", chunk=500):
    for i in range(0, len(rows), chunk):
        batch = rows[i:i+chunk]
        if not batch: continue
        now = datetime.utcnow().isoformat()
        for r in batch: r["updated_at"] = now
        sb.table(table).upsert(batch, on_conflict=on_conflict).execute()

def get_baseline_map():
    res = sb.table("e2t_baseline").select("account_id, baseline_equity, baseline_at").execute()
    rows = res.data or []
    baseline_at = None
    base = {}
    for r in rows:
        base[str(r["account_id"])] = float(r["baseline_equity"])
        t = r.get("baseline_at")
        if t:
            dt = datetime.fromisoformat(str(t).replace("Z","+00:00"))
            baseline_at = max(baseline_at or dt, dt)
    return baseline_at, base

def seed_baseline(now_utc: datetime, accounts: list[dict]):
    print("[BASELINE] Seeding weekly baseline…")
    baseline_rows, active_rows = [], []
    for acc in accounts:
        uid = str(acc["account_id"])
        s = fetch_sirix(uid); time.sleep(0.2)
        if not s: continue
        eq = s.get("Equity")
        if eq is not None:
            baseline_rows.append({"account_id": uid, "baseline_equity": float(eq), "baseline_at": now_utc.isoformat()})
        active_rows.append({
            "account_id": uid, "customer_name": acc.get("customer_name"),
            "country": s.get("Country"), "plan": s.get("Plan"),
            "balance": s.get("Balance"), "equity": s.get("Equity"), "open_pnl": s.get("OpenPnL"),
            "pct_change": None
        })
    if baseline_rows: upsert("e2t_baseline", baseline_rows)
    if active_rows:   upsert("e2t_active", active_rows)
    print(f"[BASELINE] Seeded {len(baseline_rows)} baselines.")

# ----- Main update -----
def run_update():
    now_utc = datetime.now(timezone.utc)
    accs = (sb.table("e2t_accounts").select("account_id, customer_name").execute().data) or []
    if not accs:
        print("[WARN] e2t_accounts is empty. Seed it first."); return

    baseline_at, base = get_baseline_map()
    if need_new_week(baseline_at, now_utc):
        seed_baseline(now_utc, accs)
        baseline_at, base = get_baseline_map()

    rows_active, rows_blown, rows_purch, rows_50k = [], [], [], []
    for i, acc in enumerate(accs, 1):
        uid = str(acc["account_id"]); nm = acc.get("customer_name")
        print(f"[{i}/{len(accs)}] {uid}")
        s = fetch_sirix(uid); time.sleep(0.2)
        if not s: continue

        if s.get("BlownUp"):
            rows_blown.append({"account_id": uid, "customer_name": nm, "country": s["Country"],
                               "plan": s["Plan"], "balance": s["Balance"], "equity": s["Equity"], "open_pnl": s["OpenPnL"]})
            continue
        if s.get("IsPurchaseGroup"):
            rows_purch.append({"account_id": uid, "customer_name": nm, "country": s["Country"],
                               "plan": s["Plan"], "balance": s["Balance"], "equity": s["Equity"], "open_pnl": s["OpenPnL"],
                               "group_name": s.get("GroupName")})
            continue

        plan_val = None
        try: plan_val = float(s.get("Plan")) if s.get("Plan") is not None else None
        except: pass
        if plan_val is not None and abs(plan_val - 50000.0) < 1e-6:
            rows_50k.append({"account_id": uid, "customer_name": nm, "country": s["Country"],
                             "plan": s["Plan"], "balance": s["Balance"], "equity": s["Equity"], "open_pnl": s["OpenPnL"]})
            continue

        eq = s.get("Equity"); pct = None; base_eq = base.get(uid)
        if base_eq not in (None, 0) and eq is not None:
            try: pct = ((eq - base_eq) / base_eq) * 100.0
            except: pct = None

        rows_active.append({"account_id": uid, "customer_name": nm, "country": s["Country"],
                            "plan": s["Plan"], "balance": s["Balance"], "equity": s["Equity"], "open_pnl": s["OpenPnL"],
                            "pct_change": pct})

    if rows_active: upsert("e2t_active", rows_active)
    if rows_blown:  upsert("e2t_blown_up", rows_blown)
    if rows_purch:  upsert("e2t_purchases_api", rows_purch)
    if rows_50k:    upsert("e2t_plan_50k", rows_50k)

# ----- Loop -----
def main():
    print("[SERVICE] E2T worker running.")
    while True:
        run_update()
        nxt = next_even_hour_utc(datetime.now(timezone.utc))
        sleep_s = max(60, (nxt - datetime.now(timezone.utc)).total_seconds())
        hh, mm = int(sleep_s//3600), int((sleep_s%3600)//60)
        print(f"[SCHED] Next run at {nxt.isoformat()} (in {hh:02d}:{mm:02d}).")
        time.sleep(sleep_s)

if __name__ == "__main__":
    main()
