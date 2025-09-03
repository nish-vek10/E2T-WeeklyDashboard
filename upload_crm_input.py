import os
import pandas as pd
from supabase import create_client


# Read Supabase creds from environment variables (do NOT replace the names below)
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

# Path to your Excel (same as before)
EXCEL = r"C:\Users\anish\OneDrive\Desktop\Anish\CRM API\CRM Dashboard\finalCleanOutput\Lv_tpaccount.xlsx"

def main():
    print("[INFO] Reading Excel…")
    # Read as TEXT to avoid type issues
    df = pd.read_excel(EXCEL, dtype=str)

    # Check required columns exist
    needed = {"Lv_TempName", "lv_accountidName", "Lv_name"}
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise SystemExit(f"Excel is missing expected columns: {missing}\nAvailable: {list(df.columns)}")

    # Remove 'Purchases' rows (your rule)
    before = len(df)
    df = df[~df['Lv_TempName'].fillna('').str.contains('Purchases', case=False)].reset_index(drop=True)
    print(f"[CLEAN] Filtered Purchases: {before} -> {len(df)} rows")

    # Build rows for e2t_accounts
    # account_id = Lv_name, customer_name = lv_accountidName
    rows = []
    for _, r in df.iterrows():
        uid = (r.get("Lv_name") or "").strip()
        if not uid or uid.lower() == "nan":
            continue
        rows.append({
            "account_id": uid,
            "customer_name": (r.get("lv_accountidName") or "").strip() or None
        })

    print(f"[INFO] Preparing to upsert {len(rows)} rows to e2t_accounts…")
    sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

    # Upsert in chunks
    CHUNK = 500
    for i in range(0, len(rows), CHUNK):
        batch = rows[i:i+CHUNK]
        if batch:
            sb.table("e2t_accounts").upsert(batch, on_conflict="account_id").execute()
            print(f"[OK] Upserted {i+len(batch)}/{len(rows)}")

    print("[DONE] Seeding complete.")

if __name__ == "__main__":
    main()
