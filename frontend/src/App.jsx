import { useState, useEffect, useCallback } from "react";
import {
  LineChart, Line, AreaChart, Area,
  XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, ReferenceLine
} from "recharts";

// ─── Config ────────────────────────────────────────────────────────────────
const API = "http://localhost:8000";
const POLL_MS = 5000;

const SEV_COLOR = {
  CRITICAL: "#ff2d2d",
  HIGH:     "#ff6b00",
  MEDIUM:   "#f5c400",
  LOW:      "#4ade80",
};

const COMMODITY_ICONS = {
  wheat: "🌾", rice: "🍚", maize: "🌽",
  soybeans: "🫘", palm_oil: "🌴", sugar: "🍬",
  barley: "🌿",
};

// ─── API hooks ──────────────────────────────────────────────────────────────
function useApi(path, interval = POLL_MS) {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);

  const fetch_ = useCallback(() => {
    fetch(`${API}${path}`)
      .then(r => r.json())
      .then(d => { setData(d); setError(null); })
      .catch(e => setError(e.message));
  }, [path]);

  useEffect(() => {
    fetch_();
    const id = setInterval(fetch_, interval);
    return () => clearInterval(id);
  }, [fetch_, interval]);

  return { data, error };
}

// ─── Components ─────────────────────────────────────────────────────────────

function SeverityBadge({ severity }) {
  const color = SEV_COLOR[severity] || "#888";
  return (
    <span style={{
      display: "inline-block",
      padding: "2px 8px",
      borderRadius: "2px",
      fontSize: "11px",
      fontWeight: 700,
      letterSpacing: "0.08em",
      color: color,
      border: `1px solid ${color}`,
      background: `${color}18`,
    }}>
      {severity}
    </span>
  );
}

function PulseRing({ active }) {
  return active ? (
    <span style={{
      display: "inline-block",
      width: 10, height: 10,
      borderRadius: "50%",
      background: "#ff2d2d",
      boxShadow: "0 0 0 0 rgba(255,45,45,0.6)",
      animation: "pulse 1.4s infinite",
      verticalAlign: "middle",
      marginRight: 8,
    }} />
  ) : (
    <span style={{
      display: "inline-block",
      width: 10, height: 10,
      borderRadius: "50%",
      background: "#4ade80",
      verticalAlign: "middle",
      marginRight: 8,
    }} />
  );
}

function StatusBar({ health }) {
  if (!health) return null;
  const backends = health.backends || {};
  return (
    <div style={{
      display: "flex", gap: 24, alignItems: "center",
      padding: "6px 0", borderBottom: "1px solid #1e2a1e",
      marginBottom: 24, fontSize: 11,
      color: "#5a7a5a", letterSpacing: "0.06em",
    }}>
      <span style={{ color: "#2a4a2a", fontWeight: 700 }}>
        FOOD PRICE SENTINEL
      </span>
      {Object.entries(backends).map(([name, s]) => (
        <span key={name} style={{ display: "flex", alignItems: "center", gap: 6 }}>
          <span style={{
            width: 6, height: 6, borderRadius: "50%",
            background: s.status === "ok" ? "#4ade80" : "#ff2d2d",
            display: "inline-block",
          }} />
          <span style={{ textTransform: "uppercase" }}>{name}</span>
          {s.latency_ms && (
            <span style={{ color: "#2a4a2a" }}>{s.latency_ms.toFixed(0)}ms</span>
          )}
        </span>
      ))}
      <span style={{ marginLeft: "auto", color: "#2a4a2a" }}>
        {new Date().toISOString().replace("T", " ").slice(0, 19)} UTC
      </span>
    </div>
  );
}

function StatCard({ label, value, sub, accent }) {
  return (
    <div style={{
      background: "#080f08",
      border: "1px solid #1a2a1a",
      borderLeft: `3px solid ${accent || "#1a3a1a"}`,
      padding: "16px 20px",
      borderRadius: "2px",
    }}>
      <div style={{
        fontSize: 11, color: "#3a5a3a",
        letterSpacing: "0.1em", textTransform: "uppercase",
        marginBottom: 8, fontFamily: "'Barlow Condensed', sans-serif",
      }}>
        {label}
      </div>
      <div style={{
        fontSize: 32, fontWeight: 700,
        color: accent || "#4ade80",
        fontFamily: "'IBM Plex Mono', monospace",
        lineHeight: 1,
      }}>
        {value ?? "—"}
      </div>
      {sub && (
        <div style={{ fontSize: 11, color: "#2a4a2a", marginTop: 6 }}>
          {sub}
        </div>
      )}
    </div>
  );
}

function AlertCard({ alert }) {
  const color = SEV_COLOR[alert.severity] || "#888";
  const pct = alert.pct_deviation_usd;
  return (
    <div style={{
      background: "#080f08",
      border: `1px solid ${color}40`,
      borderLeft: `3px solid ${color}`,
      padding: "14px 18px",
      borderRadius: "2px",
      animation: "slideIn 0.3s ease",
    }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <span style={{ fontSize: 20 }}>
            {COMMODITY_ICONS[alert.commodity] || "📊"}
          </span>
          <div>
            <div style={{
              fontSize: 15, fontWeight: 700,
              color: "#c8e6c8", textTransform: "uppercase",
              letterSpacing: "0.05em",
              fontFamily: "'Barlow Condensed', sans-serif",
            }}>
              {alert.commodity}
            </div>
            <div style={{ fontSize: 11, color: "#3a5a3a" }}>
              {alert.region} · {alert.model_version}
            </div>
          </div>
        </div>
        <SeverityBadge severity={alert.severity} />
      </div>

      <div style={{
        display: "grid", gridTemplateColumns: "1fr 1fr 1fr",
        gap: 12, marginTop: 14,
      }}>
        <div>
          <div style={{ fontSize: 10, color: "#3a5a3a", letterSpacing: "0.08em" }}>CURRENT</div>
          <div style={{ fontSize: 16, color: "#c8e6c8", fontFamily: "'IBM Plex Mono', monospace" }}>
            ${alert.current_price_usd?.toFixed(2)}
          </div>
        </div>
        <div>
          <div style={{ fontSize: 10, color: "#3a5a3a", letterSpacing: "0.08em" }}>BASELINE</div>
          <div style={{ fontSize: 16, color: "#6a8a6a", fontFamily: "'IBM Plex Mono', monospace" }}>
            ${alert.baseline_price_usd?.toFixed(2)}
          </div>
        </div>
        <div>
          <div style={{ fontSize: 10, color: "#3a5a3a", letterSpacing: "0.08em" }}>DEVIATION</div>
          <div style={{
            fontSize: 16, fontFamily: "'IBM Plex Mono', monospace",
            color: pct > 0 ? "#ff6b00" : "#4ade80",
          }}>
            {pct > 0 ? "+" : ""}{pct?.toFixed(1)}%
          </div>
        </div>
      </div>

      {alert.contributing_factors?.length > 0 && (
        <div style={{ marginTop: 10, display: "flex", gap: 6, flexWrap: "wrap" }}>
          {alert.contributing_factors.map(f => (
            <span key={f} style={{
              fontSize: 10, padding: "2px 7px",
              background: "#0f1f0f", border: "1px solid #1a3a1a",
              borderRadius: "2px", color: "#5a8a5a",
              letterSpacing: "0.05em",
            }}>
              {f.replace(/_/g, " ")}
            </span>
          ))}
        </div>
      )}

      <div style={{ marginTop: 10, fontSize: 10, color: "#2a4a2a" }}>
        score: {alert.anomaly_score?.toFixed(4)} ·{" "}
        {new Date(alert.triggered_at).toISOString().replace("T", " ").slice(0, 19)} UTC
      </div>
    </div>
  );
}

function HistoryChart({ history }) {
  if (!history?.records?.length) return (
    <div style={{
      height: 200, display: "flex", alignItems: "center",
      justifyContent: "center", color: "#2a4a2a", fontSize: 12,
      letterSpacing: "0.08em",
    }}>
      AWAITING DATA
    </div>
  );

  const data = [...history.records]
    .reverse()
    .map(r => ({
      time: new Date(r.detected_at).toISOString().slice(11, 19),
      score: parseFloat(r.anomaly_score) || 0,
      severity: r.severity,
    }));

  return (
    <ResponsiveContainer width="100%" height={200}>
      <AreaChart data={data} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
        <defs>
          <linearGradient id="scoreGrad" x1="0" y1="0" x2="0" y2="1">
            <stop offset="5%" stopColor="#4ade80" stopOpacity={0.3} />
            <stop offset="95%" stopColor="#4ade80" stopOpacity={0} />
          </linearGradient>
        </defs>
        <CartesianGrid strokeDasharray="3 3" stroke="#0f1f0f" />
        <XAxis dataKey="time" tick={{ fill: "#2a4a2a", fontSize: 10 }} tickLine={false} />
        <YAxis domain={["dataMin - 0.05", "dataMax + 0.05"]} tick={{ fill: "#2a4a2a", fontSize: 10 }} tickLine={false} />
        <Tooltip
          contentStyle={{
            background: "#080f08", border: "1px solid #1a3a1a",
            borderRadius: 2, fontSize: 11, color: "#4ade80",
          }}
        />
        <ReferenceLine y={0.55} stroke="#f5c400" strokeDasharray="4 4" strokeWidth={1} />
        <ReferenceLine y={0.70} stroke="#ff6b00" strokeDasharray="4 4" strokeWidth={1} />
        <ReferenceLine y={0.85} stroke="#ff2d2d" strokeDasharray="4 4" strokeWidth={1} />
        <Area
          type="monotone" dataKey="score"
          stroke="#4ade80" strokeWidth={1.5}
          fill="url(#scoreGrad)"
          dot={false}
        />
      </AreaChart>
    </ResponsiveContainer>
  );
}

function CommodityRow({ commodity, count, avgScore }) {
  const bar = Math.min(100, (count / 10) * 100);
  return (
    <div style={{
      display: "flex", alignItems: "center", gap: 12,
      padding: "8px 0", borderBottom: "1px solid #0f1f0f",
    }}>
      <span style={{ fontSize: 16, width: 24 }}>
        {COMMODITY_ICONS[commodity] || "📊"}
      </span>
      <span style={{
        fontSize: 12, color: "#7aaa7a", width: 90,
        textTransform: "uppercase", letterSpacing: "0.06em",
        fontFamily: "'Barlow Condensed', sans-serif",
      }}>
        {commodity}
      </span>
      <div style={{
        flex: 1, height: 4, background: "#0f1f0f",
        borderRadius: 2, overflow: "hidden",
      }}>
        <div style={{
          width: `${bar}%`, height: "100%",
          background: avgScore > 0.7 ? "#ff6b00" : "#4ade80",
          borderRadius: 2, transition: "width 0.6s ease",
        }} />
      </div>
      <span style={{
        fontSize: 11, color: "#3a5a3a", width: 30,
        textAlign: "right", fontFamily: "'IBM Plex Mono', monospace",
      }}>
        {count}
      </span>
    </div>
  );
}

// ─── Main App ───────────────────────────────────────────────────────────────
export default function App() {
  const { data: health }  = useApi("/health", 30000);
  const { data: alerts }  = useApi("/alerts/active");
  const { data: stats }   = useApi("/history/stats?days=1", POLL_MS);
  const { data: history } = useApi("/history?days=1&page_size=100", POLL_MS);

  const hasAlerts = alerts?.count > 0;
  const anomalyCount = stats?.total_anomalies ?? 0;

  return (
    <>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;700&family=Barlow+Condensed:wght@400;600;700&display=swap');

        * { box-sizing: border-box; margin: 0; padding: 0; }

        body {
          background: #040a04;
          color: #7aaa7a;
          font-family: 'IBM Plex Mono', monospace;
          min-height: 100vh;
        }

        body::before {
          content: '';
          position: fixed;
          inset: 0;
          background:
            radial-gradient(ellipse at 20% 50%, #0a1f0a22 0%, transparent 60%),
            radial-gradient(ellipse at 80% 20%, #0f2a0f18 0%, transparent 50%);
          pointer-events: none;
          z-index: 0;
        }

        #root { position: relative; z-index: 1; }

        @keyframes pulse {
          0%   { box-shadow: 0 0 0 0 rgba(255,45,45,0.6); }
          70%  { box-shadow: 0 0 0 8px rgba(255,45,45,0); }
          100% { box-shadow: 0 0 0 0 rgba(255,45,45,0); }
        }

        @keyframes slideIn {
          from { opacity: 0; transform: translateY(-8px); }
          to   { opacity: 1; transform: translateY(0); }
        }

        @keyframes scanline {
          0%   { transform: translateY(-100%); }
          100% { transform: translateY(100vh); }
        }

        ::-webkit-scrollbar { width: 4px; }
        ::-webkit-scrollbar-track { background: #040a04; }
        ::-webkit-scrollbar-thumb { background: #1a3a1a; border-radius: 2px; }
      `}</style>

      <div style={{ maxWidth: 1400, margin: "0 auto", padding: "20px 24px" }}>

        {/* Status bar */}
        <StatusBar health={health} />

        {/* Header */}
        <div style={{
          display: "flex", alignItems: "baseline",
          justifyContent: "space-between", marginBottom: 28,
        }}>
          <div>
            <h1 style={{
              fontFamily: "'Barlow Condensed', sans-serif",
              fontSize: 36, fontWeight: 700,
              color: "#c8e6c8", letterSpacing: "0.04em",
              lineHeight: 1,
            }}>
              <PulseRing active={hasAlerts} />
              SENTINEL
            </h1>
            <div style={{
              fontSize: 11, color: "#3a5a3a",
              letterSpacing: "0.12em", marginTop: 4,
            }}>
              FOOD PRICE ANOMALY MONITOR · REAL-TIME
            </div>
          </div>
          <div style={{
            fontSize: 11, color: "#2a4a2a",
            textAlign: "right", letterSpacing: "0.06em",
          }}>
            <div>POLLING {(POLL_MS/1000).toFixed(0)}s INTERVAL</div>
            <div style={{ color: hasAlerts ? "#ff2d2d" : "#2a4a2a", marginTop: 2 }}>
              {hasAlerts ? `${alerts.count} ACTIVE ALERT${alerts.count > 1 ? "S" : ""}` : "ALL CLEAR"}
            </div>
          </div>
        </div>

        {/* Stat cards */}
        <div style={{
          display: "grid",
          gridTemplateColumns: "repeat(4, 1fr)",
          gap: 12, marginBottom: 24,
        }}>
          <StatCard
            label="Anomalies (24h)"
            value={anomalyCount}
            sub={`${stats?.total_alerts_sent ?? 0} alerts sent`}
            accent={anomalyCount > 10 ? "#ff6b00" : "#4ade80"}
          />
          <StatCard
            label="Active Alerts"
            value={alerts?.count ?? 0}
            sub="in Valkey cache"
            accent={hasAlerts ? "#ff2d2d" : "#4ade80"}
          />
          <StatCard
            label="Suppressed"
            value={stats?.total_suppressed ?? 0}
            sub="by dedup gate"
            accent="#f5c400"
          />
          <StatCard
            label="Avg Score"
            value={stats?.by_severity?.[0]?.avg_score?.toFixed(4) ?? "—"}
            sub={stats?.by_severity?.[0]?.severity ?? "no data"}
            accent="#7aaa7a"
          />
        </div>

        {/* Main grid */}
        <div style={{
          display: "grid",
          gridTemplateColumns: "1fr 340px",
          gap: 16,
        }}>

          {/* Left column */}
          <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>

            {/* Chart */}
            <div style={{
              background: "#080f08",
              border: "1px solid #1a2a1a",
              borderRadius: "2px", padding: "18px 20px",
            }}>
              <div style={{
                fontSize: 11, color: "#3a5a3a",
                letterSpacing: "0.1em", marginBottom: 16,
                fontFamily: "'Barlow Condensed', sans-serif",
                textTransform: "uppercase",
              }}>
                Anomaly Score Timeline · 24h
              </div>
              <HistoryChart history={history} />
              <div style={{
                display: "flex", gap: 20, marginTop: 12, fontSize: 10,
                color: "#2a4a2a",
              }}>
                <span>
                  <span style={{ color: "#f5c400" }}>── </span>MEDIUM (0.55)
                </span>
                <span>
                  <span style={{ color: "#ff6b00" }}>── </span>HIGH (0.70)
                </span>
                <span>
                  <span style={{ color: "#ff2d2d" }}>── </span>CRITICAL (0.85)
                </span>
              </div>
            </div>

            {/* Commodity breakdown */}
            <div style={{
              background: "#080f08",
              border: "1px solid #1a2a1a",
              borderRadius: "2px", padding: "18px 20px",
            }}>
              <div style={{
                fontSize: 11, color: "#3a5a3a",
                letterSpacing: "0.1em", marginBottom: 14,
                fontFamily: "'Barlow Condensed', sans-serif",
                textTransform: "uppercase",
              }}>
                Anomalies by Commodity · 24h
              </div>
              {stats?.top_commodities?.length > 0 ? (
                stats.top_commodities.map(c => (
                  <CommodityRow
                    key={c.commodity}
                    commodity={c.commodity}
                    count={c.anomaly_count}
                    avgScore={stats?.by_severity?.[0]?.avg_score || 0}
                  />
                ))
              ) : (
                <div style={{ fontSize: 11, color: "#2a4a2a", padding: "20px 0" }}>
                  NO ANOMALIES IN LAST 24H
                </div>
              )}
            </div>
          </div>

          {/* Right column — alerts feed */}
          <div style={{
            background: "#080f08",
            border: "1px solid #1a2a1a",
            borderRadius: "2px", padding: "18px 16px",
            maxHeight: "calc(100vh - 260px)",
            overflowY: "auto",
          }}>
            <div style={{
              fontSize: 11, color: "#3a5a3a",
              letterSpacing: "0.1em", marginBottom: 14,
              fontFamily: "'Barlow Condensed', sans-serif",
              textTransform: "uppercase",
              display: "flex", justifyContent: "space-between",
            }}>
              <span>Active Alerts</span>
              <span style={{ color: hasAlerts ? "#ff2d2d" : "#2a4a2a" }}>
                {alerts?.count ?? 0}
              </span>
            </div>

            {alerts?.count > 0 ? (
              <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
                {alerts.alerts.map(a => (
                  <AlertCard key={a.alert_id} alert={a} />
                ))}
              </div>
            ) : (
              <div style={{
                display: "flex", flexDirection: "column",
                alignItems: "center", justifyContent: "center",
                height: 200, gap: 12,
              }}>
                <div style={{ fontSize: 28 }}>🟢</div>
                <div style={{
                  fontSize: 11, color: "#2a4a2a",
                  letterSpacing: "0.12em",
                }}>
                  ALL CLEAR
                </div>
                <div style={{ fontSize: 10, color: "#1a3a1a", textAlign: "center" }}>
                  No active alerts.<br/>
                  Monitoring {Object.keys(COMMODITY_ICONS).length} commodities.
                </div>
              </div>
            )}

            {/* Recent history below alerts */}
            {history?.records?.length > 0 && (
              <>
                <div style={{
                  fontSize: 11, color: "#3a5a3a",
                  letterSpacing: "0.1em",
                  margin: "20px 0 12px",
                  fontFamily: "'Barlow Condensed', sans-serif",
                  textTransform: "uppercase",
                  borderTop: "1px solid #0f1f0f",
                  paddingTop: 16,
                }}>
                  Recent Events
                </div>
                <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                  {history.records.slice(0, 20).map(r => (
                    <div key={r.id} style={{
                      display: "flex", justifyContent: "space-between",
                      alignItems: "center",
                      padding: "5px 0",
                      borderBottom: "1px solid #080f08",
                      fontSize: 11,
                    }}>
                      <span style={{ color: "#5a8a5a" }}>
                        {COMMODITY_ICONS[r.commodity] || "·"} {r.commodity}
                      </span>
                      <span style={{
                        fontFamily: "'IBM Plex Mono', monospace",
                        color: r.is_anomaly ? SEV_COLOR[r.severity] || "#888" : "#2a4a2a",
                        fontSize: 10,
                      }}>
                        {parseFloat(r.anomaly_score || 0).toFixed(4)}
                      </span>
                    </div>
                  ))}
                </div>
              </>
            )}
          </div>
        </div>

        {/* Footer */}
        <div style={{
          marginTop: 20, padding: "12px 0",
          borderTop: "1px solid #0f1f0f",
          display: "flex", justifyContent: "space-between",
          fontSize: 10, color: "#1a3a1a", letterSpacing: "0.06em",
        }}>
          <span>FOOD PRICE SENTINEL v1.0.0</span>
          <span>MODEL {health ? "1.0.0" : "—"} · ISOLATION FOREST</span>
          <span>
            {health?.status === "healthy" ? "● ALL SYSTEMS NOMINAL" : "◐ DEGRADED MODE"}
          </span>
        </div>
      </div>
    </>
  );
}
