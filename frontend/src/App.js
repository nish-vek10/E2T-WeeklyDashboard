// frontend/src/App.js
import React, { useEffect, useState, useMemo, useLayoutEffect, useRef } from "react";
import "./App.css";
import countries from "i18n-iso-countries";
import enLocale from "i18n-iso-countries/langs/en.json";

countries.registerLocale(enLocale);

// Supabase REST (read-only)
const SUPABASE_URL  = process.env.REACT_APP_SUPABASE_URL;
const SUPABASE_ANON = process.env.REACT_APP_SUPABASE_ANON_KEY;

const SB_SELECT =
  "account_id,customer_name,country,plan,balance,equity,open_pnl,pct_change,updated_at";

const SB_ACTIVE_URL = `${SUPABASE_URL}/rest/v1/e2t_active?select=${encodeURIComponent(
  SB_SELECT
)}&order=pct_change.desc.nullslast&limit=500`;

// NOTE: We no longer render API_BASE anywhere (you asked to hide it)
const API_BASE = process.env.REACT_APP_API_BASE || "";

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

// London timezone label (BST/GMT) for the reset caption
function getLondonTZAbbrev(d = new Date()) {
  try {
    const parts = new Intl.DateTimeFormat("en-GB", {
      hour: "2-digit",
      minute: "2-digit",
      timeZone: "Europe/London",
      timeZoneName: "short",
    }).formatToParts(d);
    const tz = parts.find(p => p.type === "timeZoneName")?.value || "";
    // Normalize "GMT+1" etc. to "GMT"
    return tz.replace(/^GMT(?:[+-]\d+)?$/, "GMT");
  } catch {
    return "GMT/BST";
  }
}

// flags
// --- Flag helpers (robust country-name → ISO alpha-2) ---
const COUNTRY_ALIASES = {
  "uk": "United Kingdom",
  "u.k.": "United Kingdom",
  "gb": "United Kingdom",
  "great britain": "United Kingdom",
  "britain": "United Kingdom",
  "uae": "United Arab Emirates",
  "u.a.e.": "United Arab Emirates",
  "usa": "United States of America",
  "u.s.a.": "United States of America",
  "united states": "United States of America",
  "us": "United States of America",
  "russia": "Russian Federation",
  "kyrgyzstan": "Kyrgyz Republic",
  "czech republic": "Czechia",
  "ivory coast": "Côte d'Ivoire",
  "cote d'ivoire": "Côte d'Ivoire",
  "côte d'ivoire": "Côte d'Ivoire",
  "dr congo": "Congo, Democratic Republic of the",
  "democratic republic of the congo": "Congo, Democratic Republic of the",
  "republic of the congo": "Congo",
  "swaziland": "Eswatini",
  "cape verde": "Cabo Verde",
  "palestine": "Palestine, State of",
  "iran": "Iran, Islamic Republic of",
  "syria": "Syrian Arab Republic",
  "moldova": "Moldova, Republic of",
  "venezuela": "Venezuela, Bolivarian Republic of",
  "bolivia": "Bolivia, Plurinational State of",
  "laos": "Lao People's Democratic Republic",
  "brunei": "Brunei Darussalam",
  "vietnam": "Viet Nam",
  "south korea": "Korea, Republic of",
  "north korea": "Korea, Democratic People's Republic of",
  "macau": "Macao",
  "hong kong": "Hong Kong",
  "burma": "Myanmar",
  "myanmar": "Myanmar",
  "north macedonia": "North Macedonia",
  "são tomé and príncipe": "Sao Tome and Principe",
  "sao tome and principe": "Sao Tome and Principe",
  "micronesia": "Micronesia, Federated States of",
  "st kitts and nevis": "Saint Kitts and Nevis",
  "saint kitts and nevis": "Saint Kitts and Nevis",
  "st lucia": "Saint Lucia",
  "saint lucia": "Saint Lucia",
  "st vincent and the grenadines": "Saint Vincent and the Grenadines",
  "saint vincent and the grenadines": "Saint Vincent and the Grenadines",
  "antigua": "Antigua and Barbuda",
  "bahamas": "Bahamas",
  "gambia": "Gambia",
  "bahrein": "Bahrain",
  "netherlands the": "Netherlands",
  "republic of ireland": "Ireland",
  "eswatini": "Eswatini",
  "kosovo": "Kosovo",
  "tz": "tanzania united republic of",
  "tz": "united republic of tanzania"
};

function resolveCountryAlpha2(rawName) {
  if (!rawName) return null;
  const raw = String(rawName).trim();
  if (!raw) return null;

  // direct lookup
  let code = countries.getAlpha2Code(raw, "en");

  // alias lookup
  if (!code) {
    const alias = COUNTRY_ALIASES[raw.toLowerCase()];
    if (alias) {
      code = countries.getAlpha2Code(alias, "en") || (alias.toLowerCase() === "kosovo" ? "XK" : null);
    }
  }

  // punctuation/spacing cleanup & retry
  if (!code) {
    const cleaned = raw.replace(/[().]/g, "").replace(/\s+/g, " ").trim();
    code = countries.getAlpha2Code(cleaned, "en");
  }

  return code ? code.toLowerCase() : null;
}

function getFlagOnly(countryName) {
  const code = resolveCountryAlpha2(countryName);
  if (!code) return countryName || "";
  return (
    <img
      src={`https://flagcdn.com/w40/${code}.png`}
      title={countryName || ""}
      alt={countryName || ""}
      loading="lazy"
      style={{
        width: "38px",
        height: "28px",
        objectFit: "cover",
        borderRadius: "3px",
        boxShadow: "0 0 3px rgba(0,0,0,0.6)"
      }}
      onError={(e) => {
        // Graceful fallback if some edge-case 404s
        e.currentTarget.style.display = "none";
        e.currentTarget.insertAdjacentText("afterend", countryName || "");
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

// Top-3 row styles (dark tints)
const rowStyleForRank = (r) => {
  if (r === 0) return { background: "#1a1505" }; // gold tint
  if (r === 1) return { background: "#0f1420" }; // silver/blue tint
  if (r === 2) return { background: "#0f1a12" }; // bronze/green tint
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

// ===== Mobile Leaderboard Cards (phone-only UI) =====
function MobileLeaderboardCards({ rows, rowsTop30, globalRankById }) {
  const list = rows && rows.length ? rows : rowsTop30;

  return (
    <div role="list" style={{ display: "grid", gap: 10 }}>
      {list.map((row, idx) => {
        const id = String(row.account_id ?? "");
        const globalRank = globalRankById[id];
        const displayRank =
          globalRank >= 0 && Number.isInteger(globalRank) ? globalRank + 1 : "";

        const n = numVal(row.pct_change);
        const pctColor =
          n == null ? "#eaeaea" : n > 0 ? "#34c759" : n < 0 ? "#ff453a" : "#eaeaea";

        /* top-3 subtle highlight */
        const bg =
          globalRank === 0
            ? "linear-gradient(135deg,#1a1505 0%,#161616 100%)"
            : globalRank === 1
            ? "linear-gradient(135deg,#0f1420 0%,#161616 100%)"
            : globalRank === 2
            ? "linear-gradient(135deg,#0f1a12 0%,#161616 100%)"
            : "#181818";

        return (
          <div
            key={id || idx}
            role="listitem"
            style={{
              background: bg,
              border: "1px solid #2a2a2a",
              borderRadius: 12,
              padding: 12,
              display: "grid",
              gap: 8,
            }}
          >
            {/* Row 1: rank + name + country flag */}
            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
              <div
                style={{
                  minWidth: 32,
                  height: 32,
                  borderRadius: 8,
                  background: "#101010",
                  border:
                    globalRank <= 2 ? "1px solid rgba(212,175,55,0.35)" : "1px solid #222",
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  fontWeight: 800,
                  color: globalRank <= 2 ? "#d4af37" : "#eaeaea",
                }}
                aria-label={`Rank ${displayRank}`}
              >
                {rankBadge(globalRank) || displayRank}
              </div>

              <div
                style={{
                  fontWeight: 700,
                  fontSize: 14,
                  color: "#f2f2f2",
                  flex: 1,
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                  whiteSpace: "nowrap",
                }}
                title={row.customer_name}
              >
                {shortName(row.customer_name)}
              </div>

              <div>{getFlagOnly(row.country)}</div>
            </div>

            {/* Row 2: Net % + Capital */}
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "1fr auto",
                alignItems: "center",
                gap: 10,
              }}
            >
              <div
                style={{
                  display: "flex",
                  gap: 8,
                  flexWrap: "wrap",
                  alignItems: "center",
                  fontSize: 12.5,
                }}
              >
                <span
                  style={{
                    background: "#101010",
                    border: "1px solid #2a2a2a",
                    borderRadius: 8,
                    padding: "4px 8px",
                  }}
                >
                  <span style={{ opacity: 0.65, marginRight: 6 }}>Capital</span>
                  <strong style={{ color: "#eaeaea" }}>{fmtNumber(row.plan, 0)}</strong>
                </span>
              </div>

              <span
                style={{
                  justifySelf: "end",
                  background: "#101010",
                  border: "1px solid #2a2a2a",
                  borderRadius: 999,
                  padding: "6px 10px",
                  fontWeight: 900,
                  color: pctColor,
                  minWidth: 80,
                  textAlign: "center",
                }}
                aria-label="Net percent change"
              >
                {fmtPct(n)}
              </span>
            </div>
          </div>
        );
      })}
    </div>
  );
}


export default function App() {
  const [originalData, setOriginalData] = useState([]);
  const [data, setData] = useState([]);
  const [searchQuery, setSearchQuery] = useState("");

  const [target, setTarget] = useState(getNextResetTarget());
  const [tleft, setTleft] = useState(diffToDHMS(target));

    // Measure the real header height so the sticky strip matches exactly
    const theadRef = useRef(null);
    const [headerH, setHeaderH] = useState(46);

    useLayoutEffect(() => {
      function measure() {
        if (theadRef.current) {
          const h = Math.round(theadRef.current.getBoundingClientRect().height);
          if (h > 0) setHeaderH(h);
        }
      }
      measure();
      window.addEventListener("resize", measure);
      return () => window.removeEventListener("resize", measure);
    }, []);

    // Detect mobile viewport (<= 768px) to switch prize labels
    const [isMobile, setIsMobile] = useState(() =>
    typeof window !== "undefined"
    ? window.matchMedia("(max-width: 768px)").matches
    : false
    );
    useEffect(() => {
    if (typeof window === "undefined") return;
    const mq = window.matchMedia("(max-width: 768px)");
    const onChange = (e) => setIsMobile(e.matches);
    mq.addEventListener?.("change", onChange);
    mq.addListener?.(onChange); // Safari fallback
    return () => {
    mq.removeEventListener?.("change", onChange);
    mq.removeListener?.(onChange);
    };
    }, []);

    // Desktop: full amounts with commas
    const PRIZES_DESKTOP = {
      1: "$10,000 Funded Account",
      2: "$5,000 Funded Account",
      3: "$2,500 Funded Account",
      4: "$1,000 Instant Funded Upgrade",
      5: "$1,000 Instant Funded Upgrade",
      6: "$1,000 Instant Funded Upgrade",
      7: "$1,000 Instant Funded Upgrade",
      8: "$1,000 Instant Funded Upgrade",
      9: "$1,000 Instant Funded Upgrade",
      10: "$1,000 Instant Funded Upgrade",
    };

    // Mobile: short K-form
    const PRIZES_MOBILE = {
      1: "$10K Account",
      2: "$5K Account",
      3: "$2.5K Account",
      4: "$1K Account Upgrade",
      5: "$1K Account Upgrade",
      6: "$1K Account Upgrade",
      7: "$1K Account Upgrade",
      8: "$1K Account Upgrade",
      9: "$1K Account Upgrade",
      10: "$1K Account Upgrade",
    };

    // Use mobile map on phones, desktop map otherwise
    const prizeMap = isMobile ? PRIZES_MOBILE : PRIZES_DESKTOP;


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
  // Height of the header strip (matches <th> height)
  // const LEADERBOARD_HEADER_H = 46; // px (tweak 44–48 if needed)


  // Sticky header cells for the leaderboard table
  const stickyThBase = {
    position: "sticky",
    top: 0,             // sticks to the top of the scroll container
    zIndex: 5,          // stay above table rows
    // small shadow so the header doesn't visually merge with rows as you scroll
    boxShadow: "0 2px 0 rgba(0,0,0,0.4)"
  };

  const visibleForPrizes = top30Data.slice(0, 10);

  // Live tz label (recomputed each render thanks to the ticking countdown)
  const londonTZ = getLondonTZAbbrev();

  return (
    <div
      style={{
        padding: "20px",
        fontFamily: "Switzer, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, 'Helvetica Neue', sans-serif",
        background: "transparent",          // inherit global dark background
        color: "#eaeaea"
      }}
    >
      <h1
        style={{
          fontSize: "3.0rem",
          fontWeight: 900,
          marginBottom: "16px",
          fontFamily: "Switzer, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, 'Helvetica Neue', sans-serif",
          letterSpacing: "0.7px",
          textAlign: "center",
          textTransform: "uppercase",
          lineHeight: "1.15",
          background: "linear-gradient(90deg, #eee 0%, #d4af37 25%, #eee 50%, #d4af37 75%, #eee 100%)",
          backgroundSize: "300% 100%",
          WebkitBackgroundClip: "text",
          backgroundClip: "text",
          color: "transparent",
          animation: "gradientShift 6s ease-in-out infinite"
        }}
      >
        E2T WEEKLY LEADERBOARD
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
              border: "1px solid #2a2a2a",
              borderRadius: "6px",
              fontSize: "14px",
              fontFamily: "inherit",
              boxShadow: "0 0 0 rgba(0,0,0,0)",
              outline: "none",
              background: "#111",
              color: "#eaeaea"
            }}
          />
        </div>
      </div>

      <div className="layout-3col" style={{ ...centerWrap }}>
        {/* PRIZES */}
        <div className="col-prizes">
          <table
            style={{
              width: "100%",
              borderCollapse: "collapse",
              fontSize: 13,
              background: "#121212",
              boxShadow: "0 1px 6px rgba(0,0,0,0.6)",
              borderRadius: 8,
              overflow: "hidden",
              color: "#eaeaea"
            }}
          >

              {/* NEW: fix narrow first column so the text column gets more room */}
              <colgroup>
                <col style={{ width: 56 }} />  {/* rank/medal strip */}
                <col />                        {/* amount takes the remaining width */}
              </colgroup>

            <thead style={gradientTheadStyle}>
              <tr>
                <th style={{ padding: "10px 8px", fontWeight: 900, textAlign: "left", fontSize: 14 }}>PRIZES</th>
                <th style={{ padding: "10px 8px", fontWeight: 900, textAlign: "right", fontSize: 14 }}>AMOUNT</th>
              </tr>
            </thead>
            <tbody>
              {visibleForPrizes.length === 0 && (
                <tr><td colSpan={2} style={{ padding: 10, color: "#999" }}>No data</td></tr>
              )}
              {visibleForPrizes.map((row, idx) => {
                const globalRank = idx;
                const zebra = { background: idx % 2 === 0 ? "#121212" : "#0f0f0f" };
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
                      paddingLeft: 8,
                      fontWeight: 800,
                      borderLeft: `6px solid ${accentForRank(globalRank)}`
                    }}>
                      {rankBadge(globalRank) || (globalRank + 1)}
                    </td>
                    <td style={{
                      height: rh,
                      lineHeight: rh + "px",
                      padding: 0,
                      paddingRight: 12,
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
        <div className="col-leaderboard">
          <div className="desktopOnly" style={{ overflowX: "auto", maxHeight: "70vh", overflowY: "auto" }}>

            {/* STICKY GRADIENT STRIP (behind header text) */}
            <div
              style={{
              position: "sticky",
              top: 0,
              height: headerH, // 46px from your constant
              background: "linear-gradient(135deg, #0f0f0f 0%, #222 60%, #d4af37 100%)",
              borderTopLeftRadius: 8,
              borderTopRightRadius: 8,
              zIndex: 4,
              marginBottom: -(headerH - 1), // pull table up so <th> sits on this
              pointerEvents: "none" // don't block scroll/hover
          }}
        />

            <table
              cellPadding="5"
              style={{
                width: "100%",
                borderCollapse: "separate",
                borderSpacing: 0,             // keeps visuals identical to "collapse"
                textAlign: "center",
                fontFamily: "inherit",
                fontSize: "14px",
                backgroundColor: "#121212",
                borderRadius: 8,
                borderTopLeftRadius: 0,
                borderTopRightRadius: 0,
                overflow: "visible",
                color: "#eaeaea",
                border: "none"
              }}
            >

              <thead ref={theadRef}>
                <tr>
                  {["RANK", "NAME", "NET %", "CAPITAL ($)", "COUNTRY"].map((label, idx, arr) => (
                    <th
                      key={idx}
                      style={{
                      ...stickyThBase,              // position: "sticky", top: 0, zIndex, shadow
                      background: "transparent",     // let the table’s gradient show through
                      color: "#fff",
                      // typography
                      fontWeight: 1000,
                      fontSize: "16px",
                      padding: "10px 6px",
                      whiteSpace: "nowrap",

                      // ensure there are no visible seams between cells
                      border: "none",

                      // rounded ends to match your other panels
                      borderTopLeftRadius:  idx === 0 ? 8 : 0,
                      borderTopRightRadius: idx === arr.length - 1 ? 8 : 0
                    }}
                  >
                      {label}
                    </th>
                  ))}
                </tr>
              </thead>

              <tbody>
                {rowsToRender.length === 0 ? (
                  <tr>
                    <td colSpan={5} style={{ padding: 20, color: "#999" }}>
                      No records found.
                    </td>
                  </tr>
                ) : (
                  rowsToRender.map((row, rowIndex) => {
                    const id = String(row["account_id"] ?? "");
                    const globalRank = globalRankById[id];
                    const displayRank = (globalRank >= 0 && Number.isInteger(globalRank)) ? globalRank + 1 : "";

                    const zebra = { background: rowIndex % 2 === 0 ? "#121212" : "#0f0f0f" };
                    const highlight = rowStyleForRank(globalRank);
                    const rowStyle = { ...zebra, ...highlight };

                    let rowFontSize = "14px";
                    let rowFontWeight = 400;
                    if (globalRank === 0) { rowFontSize = "17px"; rowFontWeight = 800; }
                    else if (globalRank === 1) { rowFontSize = "16px"; rowFontWeight = 700; }
                    else if (globalRank === 2) { rowFontSize = "15px"; rowFontWeight = 600; }

                    const leftAccent = accentForRank(globalRank);

                    const n = numVal(row["pct_change"]);
                    const pctColor = n == null ? "#eaeaea" : (n > 0 ? "#34c759" : (n < 0 ? "#ff453a" : "#eaeaea"));
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

          {/* ===== Mobile cards (phone-only), uses same data ===== */}
          <div className="mobileOnly">
            <MobileLeaderboardCards
              rows={searchQuery ? data : []}
              rowsTop30={top30Data}
              globalRankById={globalRankById}
            />
          </div>
        </div>

        {/* COUNTDOWN */}
        <div className="col-countdown">
          <table
            style={{
              width: "100%",
              borderCollapse: "collapse",
              fontSize: 13,
              background: "#121212",
              boxShadow: "0 1px 6px rgba(0,0,0,0.6)",
              borderRadius: 8,
              overflow: "hidden",
              color: "#eaeaea"
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
              <tr style={{ background: "#0f0f0f" }}>
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
                <td colSpan={4} style={{ padding: "8px 6px", textAlign: "center", color: "#aaa", fontSize: 12 }}>
                  NEXT RESET: MONDAY 12:00 PM {londonTZ}
                </td>
              </tr>
            </tbody>
          </table>

          {/* Removed the API footer per your request */}
        </div>
      </div>
    </div>
  );
}