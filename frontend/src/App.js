// frontend/src/App.js
import React, { useEffect, useState, useMemo } from "react";

const API_BASE = process.env.REACT_APP_API_BASE || ""; // we'll set this in Heroku for prod

function fmtNumber(v, digits = 2) {
  if (v === null || v === undefined || v === "") return "";
  const n = Number(v);
  if (Number.isNaN(n)) return String(v);
  return n.toLocaleString(undefined, { minimumFractionDigits: digits, maximumFractionDigits: digits });
}
function fmtPct(v) {
  if (v === null || v === undefined || Number.isNaN(Number(v))) return "";
  const n = Number(v);
  const sign = n > 0 ? "+" : (n < 0 ? "" : "");
  return sign + n.toFixed(2) + "%";
}

export default function App() {
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState("");
  const [q, setQ] = useState("");

  async function load() {
    setLoading(true);
    setErr("");
    try {
      const res = await fetch(`${API_BASE}/data/latest?ts=${Date.now()}`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      setRows(Array.isArray(data) ? data : []);
    } catch (e) {
      setErr(String(e));
      setRows([]);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => { load(); }, []);

  const filtered = useMemo(() => {
    const qq = q.trim().toLowerCase();
    if (!qq) return rows;
    return rows.filter(r =>
      Object.values(r).some(v => String(v ?? "").toLowerCase().includes(qq))
    );
  }, [rows, q]);

  return (
    <div style={{ padding: 24, fontFamily: "system-ui, Segoe UI, Roboto, sans-serif" }}>
      <h1 style={{ marginTop: 0 }}>E2T Weekly Leaderboard</h1>

      <div style={{ display: "flex", gap: 12, alignItems: "center", marginBottom: 16 }}>
        <input
          value={q}
          onChange={e => setQ(e.target.value)}
          placeholder="Search…"
          style={{ padding: "8px 10px", borderRadius: 6, border: "1px solid #ccc", width: 260 }}
        />
        <button onClick={load} style={{ padding: "8px 12px", borderRadius: 6, border: "1px solid #ccc", cursor: "pointer" }}>
          Refresh
        </button>
        <div style={{ color: "#666" }}>
          API: <code>{API_BASE || "(same origin)"}</code>
        </div>
      </div>

      {loading && <div>Loading…</div>}
      {err && <div style={{ color: "crimson" }}>Error: {err}</div>}

      {!loading && !err && (
        <div style={{ overflowX: "auto" }}>
          <table style={{ borderCollapse: "collapse", width: "100%", background: "#fff" }}>
            <thead>
              <tr style={{ background: "#111", color: "#fff" }}>
                {["RANK","NAME","NET %","CAPITAL ($)","COUNTRY","ACCOUNT ID"].map((h, i) => (
                  <th key={i} style={{ textAlign: i===1 ? "left" : "center", padding: "10px 8px", fontWeight: 700 }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {filtered.length === 0 ? (
                <tr><td colSpan={6} style={{ padding: 16, color: "#666" }}>No rows</td></tr>
              ) : (
                filtered.map((r, i) => (
                  <tr key={`${r.account_id}-${i}`} style={{ borderTop: "1px solid #eee" }}>
                    <td style={{ textAlign: "center", padding: "8px" }}>{i + 1}</td>
                    <td style={{ textAlign: "left",   padding: "8px" }}>{r.customer_name || ""}</td>
                    <td style={{ textAlign: "center", padding: "8px", fontWeight: 700, color: (r.pct_change ?? 0) > 0 ? "#1e8e3e" : ((r.pct_change ?? 0) < 0 ? "#d93025" : "#222") }}>
                      {fmtPct(r.pct_change)}
                    </td>
                    <td style={{ textAlign: "center", padding: "8px" }}>{fmtNumber(r.plan, 0)}</td>
                    <td style={{ textAlign: "center", padding: "8px" }}>{r.country || ""}</td>
                    <td style={{ textAlign: "center", padding: "8px", color: "#666" }}>{r.account_id || ""}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
