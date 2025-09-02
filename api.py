import os
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client

app = FastAPI()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])

# allow your frontends (add your prod domain when you have it)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "https://<your-frontend-app>.herokuapp.com",
    ],
    allow_methods=["GET"],
    allow_headers=["*"],
)

@app.get("/data/latest")
def latest():
    runs = sb.table("runs").select("id").order("id", desc=True).limit(1).execute().data
    if not runs:
        return JSONResponse([], headers={"Cache-Control": "no-store"})
    run_id = runs[0]["id"]
    rows = (
        sb.table("active_results")
          .select("customer_name,account_id,country,plan,balance,equity,open_pnl,pct_change")
          .eq("run_id", run_id)
          .execute()
          .data
    )
    # ensure sorted by pct desc (just in case)
    rows.sort(key=lambda r: (float("-inf") if r["pct_change"] is None else r["pct_change"]), reverse=True)
    return JSONResponse(rows, headers={"Cache-Control": "no-store"})
