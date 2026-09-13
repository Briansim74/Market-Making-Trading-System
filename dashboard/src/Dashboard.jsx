import { useEffect, useState } from "react";

export default function Dashboard() {
  const [snapshot, setSnapshot] = useState(null);

  useEffect(() => {
    const ws = new WebSocket("ws://localhost:8000/ws");

    ws.onmessage = (msg) => {
      const event = JSON.parse(msg.data);
      if (event.type === "snapshot") {
        setSnapshot(event.data);
      }
    };

    return () => ws.close();
  }, []);

  if (!snapshot) return <div>Loading...</div>;

  const t = snapshot.title;
  const m = snapshot.market;
  const s = snapshot.signals;
  const re = snapshot.regime;
  const q = snapshot.quotes;
  const e = snapshot.execution;
  const r = snapshot.risk;
  const sys = snapshot.system;

  return (
    <div style={styles.container}>

      {/* TITLE */}
      <div style={styles.title}>
        {t.header.split(" | ").map((part, i) => (
          <span key={i}>
            {i > 0 && " | "}
            <span style={styles.italic}>{part}</span>
          </span>
        ))}
        {" | "}
        <span style={styles.italic}>{t.regime}</span>
        {" | "}
        <span style={styles.italic}>
          {"pnl="}
          {t.pnl_pct > 0 ? "+" : ""}
          {t.pnl_pct.toFixed(4)}%
        </span>
      </div>

      {/* TABLE */}
      <table style={styles.table}>

        {/* HEADERS */}
        <thead>
          <tr>
            <th style={styles.headerLeft}>
              Metric
            </th>

            <th style={styles.headerRight}>
              Value
            </th>
          </tr>
        </thead>

        <tbody>

          {/* MARKET */}
          <Section title="MARKET" />

          <Row
            label="Mid"
            value={m.mid.toFixed(10)}
          />

          <Row
            label="Microprice"
            value={m.microprice.toFixed(10)}
          />

          <Row
            label="Spread"
            value={m.spread.toFixed(10)}
          />

          <Row
            label="Best Bid / Size"
            value={`${m.best_bid.toFixed(10)} (${m.bid_size.toFixed(4)})`}
          />

          <Row
            label="Best Ask / Size"
            value={`${m.best_ask.toFixed(10)} (${m.ask_size.toFixed(4)})`}
          />

          <Row
            label="EWMA Vol"
            value={m.ewma_vol.toExponential(2)}
          />

          <Row
            label="Order Imbalance"
            value={m.order_imbalance.toFixed(4)}
          />

          <Row
            label="Trade Imbalance"
            value={m.trade_imbalance.toFixed(4)}
          />

          <Row
            label="Last Trade"
            value={m.trade}
          />

          {/* REGIME */}
          <Section title="REGIME" />

          <Row
            label="Regime"
            value={re.regime}
          />

          <Row
            label="Confidence"
            value={re.confidence.toFixed(3)}
          />

          {/* SIGNALS */}
          <Section title="SIGNALS" />

          <Row
            label="Spread Multiplier"
            value={s.spread_multiplier.toFixed(2)}
          />

          <Row
            label="Inventory Target"
            value={s.inventory_target.toFixed(2)}
          />

          <Row
            label="Alpha Trade Imb"
            value={s.alpha_trade_imb.toFixed(2)}
          />

          <Row
            label="Alpha Struct"
            value={s.alpha_struct.toFixed(2)}
          />

          <Row
            label="Alpha Residual"
            value={s.alpha_residual.toFixed(2)}
          />

          <Row
            label="Fair Value"
            value={s.fair.toFixed(10)}
          />

          <Row
            label="Inventory Skew"
            value={s.skew.toFixed(4)}
          />

          <Row
            label="Toxicity"
            value={s.tox.toFixed(10)}
          />

          <Row
            label="Residual Signal Quality"
            value={s.residual_signal_quality.toFixed(2)}
          />

          <Row
            label="Toxicity Signal Quality"
            value={s.toxicity_signal_quality.toFixed(10)}
          />

          <Row
            label="Reservation"
            value={s.reservation.toFixed(10)}
          />

          {/* QUOTES */}
          <Section title="QUOTES" />

          <Row
            label="My Spread"
            value={`${q.my_spread.toFixed(10)}`}
          />

          <Row
            label="My Bid / Size"
            value={`${q.my_bid.toFixed(10)} (${q.current_bid_size.toFixed(4)})`}
          />

          <Row
            label="My Ask / Size"
            value={`${q.my_ask.toFixed(10)} (${q.current_ask_size.toFixed(4)})`}
          />


          {/* EXECUTION */}
          <Section title="EXECUTION" />

          <Row
            label="Queue Ahead / Bid"
            value={`${q.my_bid.toFixed(10)} (${e.bid_queue.toFixed(4)})`}
          />

          <Row
            label="Queue Ahead / Ask"
            value={`${q.my_ask.toFixed(10)} (${e.ask_queue.toFixed(4)})`}
          />

          <Row
            label="Queue Pressure / Bid"
            value={`${q.my_bid.toFixed(10)} (${e.bid_pressure.toFixed(4)})`}
          />

          <Row
            label="Queue Pressure / Ask"
            value={`${q.my_ask.toFixed(10)} (${e.ask_pressure.toFixed(4)})`}
          />

          <Row
            label="Open Orders"
            value={e.buy_order}
          />

          <Row
            label=""
            value={e.sell_order}
          />

          <Row
            label="Last Fill Candidate"
            value={e.last_fill_candidate}
          />

          <Row
            label="Last Order Update"
            value={e.last_order_update}
          />

          {/* RISK */}
          <Section title="RISK" />

          <Row
            label="Inventory"
            value={
              <InventoryRisk inventory={r.inventory} />
            }
          />

          <Row
            label="Realized PnL"
            value={
              <PnL value={r.realized_pnl} />
            }
          />

          <Row
            label="Unrealized PnL"
            value={
              <PnL value={r.unrealized_pnl} />
            }
          />

          <Row
            label="Fees Paid"
            value={
              <PnL value={-r.fees_paid} />
            }
          />

          <Row
            label="Total PnL"
            value={
              <PnL value={r.total_pnl} />
            }
          />

          <Row
            label="Risk"
            value={
              <CenteredInventoryBar inv={r.inventory} />
            }
          />

          {/* SYSTEM */}
          <Section title="SYSTEM" />

          <Row
            label="Time"
            value={sys.time}
          />

          <Row
            label="Last Trade ts"
            value={sys.last_trade_ts}
          />

          <Row
            label="Last Depth ts"
            value={sys.last_depth_ts}
          />

          <Row
            label="Trade Latency"
            value={sys.trade_latency}
          />

          <Row
            label="Depth Latency"
            value={sys.depth_latency}
          />

          <Row
            label="Exchange Latency"
            value={sys.exchange_latency}
          />

        </tbody>
      </table>
    </div>
  );
}


// -------------------------
// ROW
// -------------------------

function Row({ label, value }) {
  return (
    <tr>
      <td style={styles.metric}>
        {label}
      </td>

      <td style={styles.value}>
        {value}
      </td>
    </tr>
  );
}

// -------------------------
// SECTION
// -------------------------

function Section({ title, value }) {
  return (
    <tr>
      <td style={styles.section}>{title}</td>
      <td style={styles.value}></td>
    </tr>
  );
}

// -------------------------
// PNL COLOR
// -------------------------

function PnL({ value }) {

  let color = "#c1c1c1f4";
  let prefix = "";

  if (value > 0) {
    color = "#22c55e";
    prefix = "▲ ";
  }

  if (value < 0) {
    color = "#b02b2b";
    prefix = "▼ ";
  }

  return (
    <span
      style={{
        color
      }}
    >
      {prefix}
      {value.toFixed(10)}
    </span>
  );
}

// -------------------------
// INVENTORY COLOR
// -------------------------

function InventoryRisk({ inventory, limit = 10 }) {

  const intensity =
    Math.min(Math.abs(inventory) / limit, 1.0);

  let color = "#22c55e";
  let prefix = "";

  if (intensity >= 0.3 && intensity < 0.7) {
    color = "#eaa208";
    prefix = "▲ ";
  }

  if (intensity >= 0.7) {
    color = "#b02b2b";
    prefix = "▲ ";
  }

  return (
    <span
      style={{
        color
      }}
    >
      {prefix}
      {inventory.toFixed(10)}
    </span>
  );
}

// -------------------------
// CENTERED INVENTORY BAR
// -------------------------

function CenteredInventoryBar({
  inv,
  maxInv = 10,
  width = 21,
}) {

  const half = Math.floor(width / 2);

  const scaled =
    Math.round((inv / maxInv) * half);

  let chars = Array(width).fill(" ");

  chars[half] = "|";

  if (scaled > 0) {
    for (
      let i = half + 1;
      i <= half + scaled;
      i++
    ) {
      chars[i] = "█";
    }
  }

  else if (scaled < 0) {
    for (
      let i = half - 1;
      i >= half + scaled;
      i--
    ) {
      chars[i] = "█";
    }
  }

  const left =
    chars.slice(0, half).join("");

  const center =
    chars[half];

  const right =
    chars.slice(half + 1).join("");

  return (
    <div
      style={{
        display: "flex",
        whiteSpace: "pre",
        fontFamily: "monospace",
      }}
    >
      <span style={{ color: "#b02b2b" }}>
        {left}
      </span>

      <span style={{ color: "#c1c1c1f4" }}>
        {center}
      </span>

      <span style={{ color: "#22c55e" }}>
        {right}
      </span>
    </div>
  );
}

const styles = {

  loading: {
    background: "#0f172a",
    color: "#ffffff",
    minHeight: "100vh",
    padding: 20,
    fontFamily: "monospace",
  },

  container: {
    background: "#0f172a",
    color: "#e5e7eb",
    minHeight: "100vh",
    padding: 20,
    fontFamily: "monospace",
  },

  title: {
    marginBottom: 12,
    color: "#c1c1c1f4",
    fontSize: 17,
  },

  italic: {
    fontStyle: "italic",
  },

  table: {
    width: "100%",
    borderCollapse: "collapse",
    background: "#111827",
    border: "2px solid #334155",
  },

  headerRight: {
    color: "#ffffff",
    textAlign: "left",
    padding: "0px 25px",
    fontSize: 16,
    borderBottom: "2px solid #334155",
  },

  headerLeft: {
    color: "#ffffff",
    textAlign: "left",
    padding: "10px",
    fontSize: 16,
    borderBottom: "2px solid #334155",
    borderRight: "2px solid #334155",
  },

  section: {
    color: "#f2fa15",
    textAlign: "left",
    paddingTop: "14px",
    paddingLeft: "10px",
    fontSize: 17,
    borderRight: "2px solid #334155",
  },

  metric: {
    fontSize: 16,
    width: "210px",
    padding: "0px 10px",
    borderRight: "2px solid #334155",
    textAlign: "left",
    color: "#72e5ff",
    whiteSpace: "pre",
  },

  value: {
    textAlign: "left",
    color: "#c1c1c1f4",
    whiteSpace: "pre",
    fontSize: 16,
    padding: "0px 25px",
  },
};