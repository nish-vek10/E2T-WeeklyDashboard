// frontend/src/App.js
import React, { useEffect, useState, useMemo } from "react";
import "./App.css";

// Supabase REST (read-only)
const SUPABASE_URL  = process.env.REACT_APP_SUPABASE_URL;
const SUPABASE_ANON = process.env.REACT_APP_SUPABASE_ANON_KEY;

const SB_SELECT =
  "account_id,customer_name,country,plan,balance,equity,open_pnl,pct_change,updated_at";

const SB_ACTIVE_URL = `${SUPABASE_URL}/rest/v1/e2t_active?select=${encodeURIComponent(
  SB_SELECT
)}&order=pct_change.desc.nullslast&limit=500`;


const API_BASE = process.env.REACT_APP_API_BASE || ""; // set in Heroku for prod, blank for same-origin

// === Helpers ===
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
function numVal(v) {
  const n = Number(v);
  return Number.isNaN(n) ? null : n;
}
function pad2(n){ return String(n).padStart(2, "0"); }

// === Countdown helpers ===
function getThisMondayNoon(d = new Date()) {
  const day = d.getDay();               // 0=Sun,1=Mon
  const diffToMonday = (day + 6) % 7;   // days since Monday
  const monday = new Date(d);
  monday.setDate(d.getDate() - diffToMonday);
  monday.setHours(12, 0, 0, 0);         // 12:00 local
  return monday;
}
function getNextResetTarget(now = new Date()) {
  const thisMondayNoon = getThisMondayNoon(now);
  if (now < thisMondayNoon) return thisMondayNoon; // this week's Monday 12:00
  const next = new Date(thisMondayNoon);
  next.setDate(thisMondayNoon.getDate() + 7);      // next Monday 12:00
  return next;
}
function diffToDHMS(target, now = new Date()) {
  let ms = Math.max(0, target - now);
  const totalSec = Math.floor(ms / 1000);
  const d = Math.floor(totalSec / 86400);
  const h = Math.floor((totalSec % 86400) / 3600);
  const m = Math.floor((totalSec % 3600) / 60);
  const s = totalSec % 60;
  return { d, h, m, s };
}

// flags
function getFlagOnly(countryName) {
  const countryMap = {
    "Albania": "al","Australia": "au","Bahrain": "bh","Bangladesh": "bd","Benin": "bj",
    "Botswana": "bw","Burkina Faso": "bf","Burundi": "bi","Cameroon": "cm","Canada": "ca",
    "Colombia": "co","Cote D'Ivoire": "ci","Cyprus": "cy","Egypt": "eg","Ethiopia": "et",
    "France": "fr","Germany": "de","Ghana": "gh","India": "in","Ireland": "ie",
    "Israel": "il","Jordan": "jo","Kenya": "ke","Lesotho": "ls","Malaysia": "my",
    "Malta": "mt","Nepal": "np","Netherlands": "nl","Nigeria": "ng","Pakistan": "pk",
    "Saudi Arabia": "sa","Senegal": "sn","Singapore": "sg","Somalia": "so",
    "South Africa": "za","Spain": "es","Swaziland": "sz","Tanzania": "tz","Uganda": "ug",
    "United Arab Emirates": "ae","United Kingdom": "gb","Uzbekistan": "uz","Zambia": "zm",
    "Zimbabwe": "zw"
  };
  const code = countryMap[countryName];
  if (!code) return countryName || "";
  return (
    <img
      src={'https://flagcdn.com/w40/' + code + '.png'}
      title={countryName || ""}
      alt={countryName || ""}
      style={{
        width: "38px",
        height: "28px",
        objectFit: "cover",
        borderRadius: "3px",
        boxShadow: "0 0 3px rgba(1,2,2,5)"
      }}
    />
  );
}

function shortName(full) {
  if (!full) return "";
  const parts = String(full).trim().split(/\s+/).filter(Boolean);
  if (parts.length === 0) return "";
  const capWord = (s) => s.charAt(0).toUpperCase() + s.slice(1).toLowerCase();
  const first = capWord(parts[0]);
  const last = parts[parts.length - 1] || "";
  const lastInitial = last ? last[0].toUpperCase() + "." : "";
  return lastInitial ? `${first} ${lastInitial}` : first;
}

// Top-3 row styles (by GLOBAL rank)
const rowStyleForRank = (r) => {
  if (r === 0) return { background: "#fff9ec" };
  if (r === 1) return { background: "#f5f7ff" };
  if (r === 2) return { background: "#f7fff5" };
  return {};
};
const rowHeightForRank = (r) => {
  if (r === 0) return 45;
  if (r === 1) return 43;
  if (r === 2) return 41;
  return 42;
};
const accentForRank = (r) => {
  if (r === 0) return "#F4C430";
  if (r === 1) return "#B0B7C3";
  if (r === 2) return "#CD7F32";
  return "transparent";
};
const rankBadge = (r) => {
  if (r === 0) return <span style={{ fontWeight: 800, fontSize: "22px" }}>🥇</span>;
  if (r === 1) return <span style={{ fontWeight: 800, fontSize: "22px" }}>🥈</span>;
  if (r === 2) return <span style={{ fontWeight: 800, fontSize: "22px" }}>🥉</span>;
  return null;
};

// === Schedule helper: next EVEN hour :30 ===
function msUntilNextEvenHour30(now = new Date()) {
  const t = new Date(now);
  t.setSeconds(0, 0);
  if (t.getHours() % 2 === 0 && t.getMinutes() < 30) {
    const cand = new Date(t);
    cand.setMinutes(30, 0, 0);
    return cand - now;
  }
  const addHours = (t.getHours() % 2 === 1) ? 1 : 2;
  const cand = new Date(t.getTime() + addHours * 3600 * 1000);
  cand.setMinutes(30, 0, 0);
  return cand - now;
}

export default function App() {
  const [originalData, setOriginalData] = useState([]);
  const [data, setData] = useState([]);
  const [searchQuery, setSearchQuery] = useState("");

  const [target, setTarget] = useState(getNextResetTarget());
  const [tleft, setTleft] = useState(diffToDHMS(target));

  const prizeMap = {
    1: "$10,000 Funded Account + $250 Cash",
    2: "$5,000 Funded Account + $150 Cash",
    3: "$2,500 Funded Account + $75 Cash",
    4: "$1,000 Instant Funded Upgrade",
    5: "$1,000 Instant Funded Upgrade",
    6: "$1,000 Instant Funded Upgrade",
    7: "$1,000 Instant Funded Upgrade",
    8: "$1,000 Instant Funded Upgrade",
    9: "$1,000 Instant Funded Upgrade",
    10: "$1,000 Instant Funded Upgrade",
  };

  async function loadData()
  {
      try
      {
        if (!SUPABASE_URL || !SUPABASE_ANON) {
          throw new Error("Missing REACT_APP_SUPABASE_URL or REACT_APP_SUPABASE_ANON_KEY");
        }

        const res = await fetch(SB_ACTIVE_URL, {
          headers: {
            apikey: SUPABASE_ANON,
            Authorization: `Bearer ${SUPABASE_ANON}`,
          },
        });
        if (!res.ok) {
          const txt = await res.text();
          throw new Error(`HTTP ${res.status}: ${txt}`);
        }

        const rows = await res.json();

        // Defensive client-side sort (NULLs last)
        rows.sort((a, b) => {
          const av = Number.isFinite(Number(a.pct_change)) ? Number(a.pct_change) : -Infinity;
          const bv = Number.isFinite(Number(b.pct_change)) ? Number(b.pct_change) : -Infinity;
          return bv - av;
        });

        const norm = (r) => ({
          customer_name: r.customer_name ?? "",
          account_id: r.account_id ?? "",
          country: r.country ?? "",
          plan: r.plan ?? null,
          balance: r.balance ?? null,
          equity: r.equity ?? null,
          open_pnl: r.open_pnl ?? null,
          pct_change: r.pct_change ?? null,
          updated_at: r.updated_at ?? null,
        });

        const data = Array.isArray(rows) ? rows.map(norm) : [];
        setOriginalData(data);
        setData(data);
      }

      catch (e)
      {
        console.error("[loadData] error:", e);
        setOriginalData([]);
        setData([]);
      }
  }


  useEffect(() => { loadData(); }, []);

  // Re-fetch at every even hour :30
  useEffect(() => {
    let cancelled = false;
    let timeoutId;
    function arm() {
      const ms = msUntilNextEvenHour30();
      timeoutId = setTimeout(async () => {
        if (cancelled) return;
        await loadData();
        arm();
      }, ms);
    }
    arm();
    return () => { cancelled = true; if (timeoutId) clearTimeout(timeoutId); };
  }, []);

  // Countdown tick
  useEffect(() => {
    const id = setInterval(() => {
      const now = new Date();
      if (now >= target) {
        const nextT = getNextResetTarget(now);
        setTarget(nextT);
        setTleft(diffToDHMS(nextT, now));
      } else {
        setTleft(diffToDHMS(target, now));
      }
    }, 1000);
    return () => clearInterval(id);
  }, [target]);

  // build global rank index (by API account_id)
  const globalRankById = useMemo(() => {
    const m = Object.create(null);
    for (let i = 0; i < originalData.length; i++) {
      const id = String(originalData[i]["account_id"] ?? "");
      if (id) m[id] = i;
    }
    return m;
  }, [originalData]);

  const handleSearch = (e) => {
    const q = e.target.value.toLowerCase();
    setSearchQuery(q);
    if (!q) { setData(originalData); return; }
    const filtered = originalData.filter(row =>
      Object.values(row).some(val => String(val ?? "").toLowerCase().includes(q))
    );
    setData(filtered);
  };

  const top30Data = useMemo(() => originalData.slice(0, 30), [originalData]);
  const rowsToRender = useMemo(() => (searchQuery ? data : top30Data), [searchQuery, data, top30Data]);

  const centerWrap = { maxWidth: 1300, margin: "0 auto" };
  const gradientTheadStyle = {
    background: "linear-gradient(135deg, #0f0f0f 0%, #222 60%, #d4af37 100%)",
    color: "#fff"
  };
  const visibleForPrizes = top30Data.slice(0, 10);

  return (
    <div style={{ padding: "20px", fontFamily: "'Segoe UI', sans-serif", background: "#fafafa" }}>
      <h1
        style={{
          fontSize: "3.0rem",
          fontWeight: "900",
          marginBottom: "16px",
          fontFamily: "'Aptos Display', 'Segoe UI', sans-serif",
          letterSpacing: "0.7px",
          textAlign: "center",
          textTransform: "uppercase",
          lineHeight: "1.15",
          background: "linear-gradient(90deg, #111 0%, #d4af37 25%, #111 50%, #d4af37 75%, #111 100%)",
          backgroundSize: "300% 100%",
          WebkitBackgroundClip: "text",
          backgroundClip: "text",
          color: "transparent",
          animation: "gradientShift 6s ease-in-out infinite"
        }}
      >
        E2T WORLD CUP COMPETITION
      </h1>

      <div style={{ ...centerWrap }}>
        <div style={{ marginBottom: "16px", display: "flex", gap: "12px", alignItems: "center", justifyContent: "center" }}>
          <input
            type="text"
            placeholder="Search..."
            value={searchQuery}
            onChange={handleSearch}
            style={{
              padding: "10px 14px",
              width: "260px",
              border: "1px solid #ccc",
              borderRadius: "6px",
              fontSize: "14px",
              fontFamily: "'Segoe UI', sans-serif",
              boxShadow: "1px 1px 5px rgba(0,0,0,2)",
              outline: "none"
            }}
          />
        </div>
      </div>

      <div style={{ display: "flex", gap: 18, alignItems: "flex-start", ...centerWrap }}>
        {/* PRIZES */}
        <div style={{ flex: "0 0 260px" }}>
          <table
            style={{
              width: "100%",
              borderCollapse: "collapse",
              fontSize: 13,
              background: "#fff",
              boxShadow: "0 1px 6px rgba(0,0,0,2)",
              borderRadius: 8,
              overflow: "hidden"
            }}
          >
            <thead style={gradientTheadStyle}>
              <tr>
                <th style={{ padding: "10px 8px", fontWeight: 900, textAlign: "left", fontSize: 14 }}>PRIZES</th>
                <th style={{ padding: "10px 8px", fontWeight: 900, textAlign: "right", fontSize: 14 }}>Amount</th>
              </tr>
            </thead>
            <tbody>
              {visibleForPrizes.length === 0 && (
                <tr><td colSpan={2} style={{ padding: 10, color: "#777" }}>No data</td></tr>
              )}
              {visibleForPrizes.map((row, idx) => {
                const globalRank = idx;
                const zebra = { background: idx % 2 === 0 ? "#ffffff" : "#fafafa" };
                const highlight = rowStyleForRank(globalRank);
                const rowStyle = { ...zebra, ...highlight };
                const prize = prizeMap[globalRank + 1] || "";

                const rh = rowHeightForRank(globalRank);
                let fs = "13px", fw = 500;
                if (globalRank === 0) { fs = "15px"; fw = 800; }
                else if (globalRank === 1) { fs = "14px"; fw = 700; }
                else if (globalRank === 2) { fs = "13.5px"; fw = 600; }

                return (
                  <tr key={idx} style={rowStyle}>
                    <td style={{
                      height: rh,
                      lineHeight: rh + "px",
                      padding: 0,
                      fontWeight: 800,
                      borderLeft: `6px solid ${accentForRank(globalRank)}`
                    }}>
                      {rankBadge(globalRank) || (globalRank + 1)}
                    </td>
                    <td style={{
                      height: rh,
                      lineHeight: rh + "px",
                      padding: 0,
                      fontSize: fs,
                      fontWeight: fw,
                      textAlign: "right",
                      whiteSpace: "nowrap",
                      overflow: "hidden",
                      textOverflow: "ellipsis"
                    }}>
                      {prize}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>

        {/* LEADERBOARD */}
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ overflowX: "auto", maxHeight: "70vh", overflowY: "auto" }}>
            <table
              border="1"
              cellPadding="5"
              style={{
                width: "100%",
                borderCollapse: "collapse",
                textAlign: "center",
                fontFamily: "'Aptos Display', 'Segoe UI', sans-serif",
                fontSize: "14px",
                background: "#fff",
                borderRadius: 8,
                overflow: "hidden"
              }}
            >
              <thead style={gradientTheadStyle}>
                <tr>
                  {["RANK", "NAME", "NET %", "CAPITAL ($)", "COUNTRY"].map((label, idx) => (
                    <th
                      key={idx}
                      style={{ fontWeight: 1000, fontSize: "16px", padding: "10px 6px", whiteSpace: "nowrap", color: "#fff" }}
                    >
                      {label}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {rowsToRender.length === 0 ? (
                  <tr>
                    <td colSpan={5} style={{ padding: 20, color: "#777" }}>
                      No records found.
                    </td>
                  </tr>
                ) : (
                  rowsToRender.map((row, rowIndex) => {
                    const id = String(row["account_id"] ?? "");
                    const globalRank = globalRankById[id];
                    const displayRank = (globalRank >= 0 && Number.isInteger(globalRank)) ? globalRank + 1 : "";

                    const zebra = { background: rowIndex % 2 === 0 ? "#ffffff" : "#f9f9f9" };
                    const highlight = rowStyleForRank(globalRank);
                    const rowStyle = { ...zebra, ...highlight };

                    let rowFontSize = "14px";
                    let rowFontWeight = 400;
                    if (globalRank === 0) { rowFontSize = "17px"; rowFontWeight = 800; }
                    else if (globalRank === 1) { rowFontSize = "16px"; rowFontWeight = 700; }
                    else if (globalRank === 2) { rowFontSize = "15px"; rowFontWeight = 600; }

                    const leftAccent = accentForRank(globalRank);

                    const n = numVal(row["pct_change"]);
                    const pctColor = n == null ? "#222" : (n > 0 ? "#1e8e3e" : (n < 0 ? "#d93025" : "#222"));
                    let pctFont = rowFontSize;
                    if (globalRank === 0) pctFont = "calc(17px + 6px)";
                    else if (globalRank === 1) pctFont = "calc(16px + 4px)";
                    else if (globalRank === 2) pctFont = "calc(15px + 2px)";

                    const cellBase = { whiteSpace: "nowrap", fontSize: rowFontSize, fontWeight: rowFontWeight };

                    return (
                      <tr key={id || rowIndex} style={rowStyle}>
                        <td style={{ ...cellBase, fontWeight: 800, borderLeft: `8px solid ${leftAccent}` }}>
                          <span style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
                            {rankBadge(globalRank) || displayRank}
                          </span>
                        </td>
                        <td style={cellBase}>{shortName(row["customer_name"])}</td>
                        <td style={cellBase}>
                          <span style={{ color: pctColor, fontWeight: 800, fontSize: pctFont }}>
                            {fmtPct(n)}
                          </span>
                        </td>
                        <td style={cellBase}>{fmtNumber(row["plan"], 0)}</td>
                        <td style={cellBase}>{getFlagOnly(row["country"])}</td>
                      </tr>
                    );
                  })
                )}
              </tbody>
            </table>
          </div>
        </div>

        {/* COUNTDOWN */}
        <div style={{ flex: "0 0 260px" }}>
          <table
            style={{
              width: "100%",
              borderCollapse: "collapse",
              fontSize: 13,
              background: "#fff",
              boxShadow: "0 1px 6px rgba(0,0,0,2)",
              borderRadius: 8,
              overflow: "hidden"
            }}
          >
            <thead style={gradientTheadStyle}>
              <tr>
                <th colSpan={4} style={{ padding: "10px 8px", fontWeight: 900, textAlign: "center", fontSize: 14 }}>
                  LEADERBOARD WEEKLY RESET
                </th>
              </tr>
            </thead>
            <tbody>
              <tr style={{ background: "#fafafa" }}>
                <td style={{ padding: "8px 6px", fontWeight: 700, textAlign: "center" }}>DD</td>
                <td style={{ padding: "8px 6px", fontWeight: 700, textAlign: "center" }}>HH</td>
                <td style={{ padding: "8px 6px", fontWeight: 700, textAlign: "center" }}>MM</td>
                <td style={{ padding: "8px 6px", fontWeight: 700, textAlign: "center" }}>SS</td>
              </tr>
              <tr>
                <td style={{ padding: "10px 6px", textAlign: "center", fontWeight: 900, fontSize: 18 }}>{pad2(tleft.d)}</td>
                <td style={{ padding: "10px 6px", textAlign: "center", fontWeight: 900, fontSize: 18 }}>{pad2(tleft.h)}</td>
                <td style={{ padding: "10px 6px", textAlign: "center", fontWeight: 900, fontSize: 18 }}>{pad2(tleft.m)}</td>
                <td style={{ padding: "10px 6px", textAlign: "center", fontWeight: 900, fontSize: 18 }}>{pad2(tleft.s)}</td>
              </tr>
              <tr>
                <td colSpan={4} style={{ padding: "8px 6px", textAlign: "center", color: "#666", fontSize: 12 }}>
                  NEXT RESET: MONDAY 12:00PM BST
                </td>
              </tr>
            </tbody>
          </table>
          <div style={{ marginTop: 8, color: "#666", fontSize: 12, textAlign: "center" }}>
            API: <code>{API_BASE || "(same origin)"}</code>
          </div>
        </div>
      </div>
    </div>
  );
}
