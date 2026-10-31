import { useEffect, useState } from "react";

export default function Dashboard() {
  const [snapshot, setSnapshot] = useState(emptySnapshot);
  const [activeTab, setActiveTab] = useState("maker");

  useEffect(() => {
    const ws = new WebSocket(`ws://${window.location.hostname}:8000/ws`);

    ws.onmessage = (msg) => {
      const event = JSON.parse(msg.data);
      if (event.type === "snapshot") {
        setSnapshot(event.data);
      }
    };

    return () => ws.close();
  }, []);

  return (
    <div style={styles.container}>

      {/* ========================= */}
      {/* TABS */}
      {/* ========================= */}

      {/* TITLE */}
        <div style={styles.title}>
          {snapshot.title.header.split(" | ").map((part, i) => (
            <span key={i}>
              {i > 0 && " | "}
              <span style={styles.italic}>{part}</span>
            </span>
          ))}
          {" | "}
          <span style={styles.italic}>{snapshot.title.regime}</span>
          {" | "}
          <span style={styles.italic}>
            {"pnl="}
            {snapshot.title.pnl_pct > 0 ? "+" : ""}
            {snapshot.title.pnl_pct.toFixed(4)}%
          </span>
        </div>

      <div style={styles.tabs}>

        <button
          style={activeTab === "maker" ? styles.activeTab : styles.tab}
          onClick={() => setActiveTab("maker")}
        >
          MAKER
        </button>

        <button
          style={activeTab === "hedge" ? styles.activeTab : styles.tab}
          onClick={() => setActiveTab("hedge")}
        >
          HEDGE
        </button>

      </div>

      {/* ========================= */}
      {/* MAKER DASHBOARD */}
      {/* ========================= */}

      {activeTab === "maker" && (
        <>

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
              label="Instrument"
              value={snapshot.maker.market.instrument}
            />

            <Row
              label="Mid"
              value={snapshot.maker.market.mid.toFixed(10)}
            />

            <Row
              label="Microprice"
              value={snapshot.maker.market.microprice.toFixed(10)}
            />

            <Row
              label="Spread"
              value={snapshot.maker.market.spread.toFixed(10)}
            />

            <Row
              label="Best Bid / Size"
              value={`${snapshot.maker.market.best_bid.toFixed(10)} (${snapshot.maker.market.bid_size.toFixed(4)})`}
            />

            <Row
              label="Best Ask / Size"
              value={`${snapshot.maker.market.best_ask.toFixed(10)} (${snapshot.maker.market.ask_size.toFixed(4)})`}
            />

            <Row
              label="EWMA Vol"
              value={snapshot.maker.market.ewma_vol.toExponential(2)}
            />

            <Row
              label="Order Imbalance"
              value={snapshot.maker.market.order_imbalance.toFixed(4)}
            />

            <Row
              label="Trade Imbalance"
              value={snapshot.maker.market.trade_imbalance.toFixed(4)}
            />

            <Row
              label="Last Trade"
              value={snapshot.maker.market.trade}
            />

            {/* REGIME */}
            <Section title="REGIME" />

            <Row
              label="Regime"
              value={snapshot.maker.regime.regime}
            />

            <Row
              label="Confidence"
              value={snapshot.maker.regime.confidence.toFixed(3)}
            />

            {/* SIGNALS */}
            <Section title="SIGNALS" />

            <Row
              label="Spread Multiplier"
              value={snapshot.maker.signals.spread_multiplier.toFixed(2)}
            />

            <Row
              label="Inventory Target"
              value={snapshot.maker.signals.inventory_target.toFixed(2)}
            />

            <Row
              label="Alpha Trade Imb"
              value={snapshot.maker.signals.alpha_trade_imb.toFixed(2)}
            />

            <Row
              label="Alpha Struct"
              value={snapshot.maker.signals.alpha_struct.toFixed(2)}
            />

            <Row
              label="Alpha Residual"
              value={snapshot.maker.signals.alpha_residual.toFixed(2)}
            />

            <Row
              label="Fair Value"
              value={snapshot.maker.signals.fair.toFixed(10)}
            />

            <Row
              label="Inventory Skew"
              value={snapshot.maker.signals.skew.toFixed(4)}
            />

            <Row
              label="Toxicity"
              value={snapshot.maker.signals.tox.toFixed(10)}
            />

            <Row
              label="Residual Signal Quality"
              value={snapshot.maker.signals.residual_signal_quality.toFixed(2)}
            />

            <Row
              label="Toxicity Signal Quality"
              value={snapshot.maker.signals.toxicity_signal_quality.toFixed(10)}
            />

            <Row
              label="Reservation"
              value={snapshot.maker.signals.reservation.toFixed(10)}
            />

            {/* QUOTES */}
            <Section title="QUOTES" />

            <Row
              label="My Spread"
              value={`${snapshot.maker.quotes.my_spread.toFixed(10)}`}
            />

            <Row
              label="My Bid / Size"
              value={`${snapshot.maker.quotes.my_bid.toFixed(10)} (${snapshot.maker.quotes.current_bid_size.toFixed(4)})`}
            />

            <Row
              label="My Ask / Size"
              value={`${snapshot.maker.quotes.my_ask.toFixed(10)} (${snapshot.maker.quotes.current_ask_size.toFixed(4)})`}
            />


            {/* EXECUTION */}
            <Section title="EXECUTION" />

            <Row
              label="Queue Ahead / Bid"
              value={`${snapshot.maker.quotes.my_bid.toFixed(10)} (${snapshot.maker.execution.bid_queue.toFixed(4)})`}
            />

            <Row
              label="Queue Ahead / Ask"
              value={`${snapshot.maker.quotes.my_ask.toFixed(10)} (${snapshot.maker.execution.ask_queue.toFixed(4)})`}
            />

            <Row
              label="Queue Pressure / Bid"
              value={`${snapshot.maker.quotes.my_bid.toFixed(10)} (${snapshot.maker.execution.bid_pressure.toFixed(4)})`}
            />

            <Row
              label="Queue Pressure / Ask"
              value={`${snapshot.maker.quotes.my_ask.toFixed(10)} (${snapshot.maker.execution.ask_pressure.toFixed(4)})`}
            />

            <Row
              label="Open Orders"
              value={snapshot.maker.execution.buy_order}
            />

            <Row
              label=""
              value={snapshot.maker.execution.sell_order}
            />

            <Row
              label="Last Fill Candidate"
              value={snapshot.maker.execution.last_fill_candidate}
            />

            <Row
              label="Last Order Update"
              value={snapshot.maker.execution.last_order_update}
            />

            {/* RISK */}
            <Section title="RISK" />

            <Row
              label="Inventory"
              value={
                <InventoryRisk inventory={snapshot.maker.risk.inventory} />
              }
            />

            <Row
              label="Realized PnL"
              value={
                <PnL value={snapshot.maker.risk.realized_pnl} />
              }
            />

            <Row
              label="Unrealized PnL"
              value={
                <PnL value={snapshot.maker.risk.unrealized_pnl} />
              }
            />

            <Row
              label="Fees Paid"
              value={
                <PnL value={-snapshot.maker.risk.fees_paid} />
              }
            />

            <Row
              label="Total PnL"
              value={
                <PnL value={snapshot.maker.risk.total_pnl} />
              }
            />

            <Row
              label="Risk"
              value={
                <CenteredInventoryBar inv={snapshot.maker.risk.inventory} />
              }
            />

            {/* SYSTEM */}
            <Section title="SYSTEM" />

            <Row
              label="Time"
              value={snapshot.maker.system.time}
            />

            <Row
              label="Last Trade ts"
              value={snapshot.maker.system.last_trade_ts}
            />

            <Row
              label="Last Depth ts"
              value={snapshot.maker.system.last_depth_ts}
            />

            <Row
              label="Trade Latency"
              value={snapshot.maker.system.trade_latency}
            />

            <Row
              label="Depth Latency"
              value={snapshot.maker.system.depth_latency}
            />

            <Row
              label="Exchange Latency"
              value={snapshot.maker.system.exchange_latency}
            />

          </tbody>
        </table>
      </>
      )}

      {/* ========================= */}
      {/* HEDGE DASHBOARD */}
      {/* ========================= */}

      {activeTab === "hedge" && (
        <>

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

            {/* SIGNALS1 */}
            <Section title="SIGNALS1" />

            <Row
              label="Maker Delta"
              value={snapshot.hedge.signals1.maker_delta.toFixed(10)}
            />

            <Row
              label="Hedge Delta"
              value={snapshot.hedge.signals1.hedge_delta.toFixed(10)}
            />

            <Row
              label="Net Delta"
              value={snapshot.hedge.signals1.net_delta.toFixed(10)}
            />

            <Row
              label="Target Hedge Ratio"
              value={snapshot.hedge.signals1.target_hedge_ratio.toFixed(10)}
            />

            <Row
              label="Target Hedge"
              value={snapshot.hedge.signals1.target_hedge.toFixed(10)}
            />

            <Row
              label="Hedge Decision"
              value={snapshot.hedge.signals1.hedge_decision}
            />

            <Row
              label="Target Hedge Delta"
              value={snapshot.hedge.signals1.target_hedge.toFixed(10)}
            />

            {/* MARKET */}
            <Section title="MARKET" />

            <Row
              label="Instrument"
              value={snapshot.hedge.market.instrument}
            />

            <Row
              label="Mid"
              value={snapshot.hedge.market.mid.toFixed(10)}
            />

            <Row
              label="Microprice"
              value={snapshot.hedge.market.microprice.toFixed(10)}
            />

            <Row
              label="Spread"
              value={snapshot.hedge.market.spread.toFixed(10)}
            />

            <Row
              label="Best Bid / Size"
              value={`${snapshot.hedge.market.best_bid.toFixed(10)} (${snapshot.hedge.market.bid_size.toFixed(4)})`}
            />

            <Row
              label="Best Ask / Size"
              value={`${snapshot.hedge.market.best_ask.toFixed(10)} (${snapshot.hedge.market.ask_size.toFixed(4)})`}
            />

            <Row
              label="EWMA Vol"
              value={snapshot.hedge.market.ewma_vol.toExponential(2)}
            />

            <Row
              label="Order Imbalance"
              value={snapshot.hedge.market.order_imbalance.toFixed(4)}
            />

            <Row
              label="Trade Imbalance"
              value={snapshot.hedge.market.trade_imbalance.toFixed(4)}
            />

            <Row
              label="Last Trade"
              value={snapshot.hedge.market.trade}
            />

            <Row
              label="Basis"
              value={snapshot.hedge.market.basis.toFixed(10)}
            />

            <Row
              label="Funding"
              value={snapshot.hedge.market.funding_rate.toFixed(8)}
            />

            <Row
              label="Next Funding Time"
              value={snapshot.hedge.market.next_funding_ts}
            />

            {/* REGIME */}
            <Section title="REGIME" />

            <Row
              label="Regime"
              value={snapshot.hedge.regime.regime}
            />

            <Row
              label="Confidence"
              value={snapshot.hedge.regime.confidence.toFixed(3)}
            />

            {/* SIGNALS */}
            <Section title="SIGNALS" />

            <Row
              label="Spread Multiplier"
              value={snapshot.hedge.signals.spread_multiplier.toFixed(2)}
            />

            <Row
              label="Inventory Target"
              value={snapshot.hedge.signals.inventory_target.toFixed(2)}
            />

            <Row
              label="Alpha Trade Imb"
              value={snapshot.hedge.signals.alpha_trade_imb.toFixed(2)}
            />

            <Row
              label="Alpha Struct"
              value={snapshot.hedge.signals.alpha_struct.toFixed(2)}
            />

            <Row
              label="Alpha Residual"
              value={snapshot.hedge.signals.alpha_residual.toFixed(2)}
            />

            <Row
              label="Fair Value"
              value={snapshot.hedge.signals.fair.toFixed(10)}
            />

            <Row
              label="Inventory Skew"
              value={snapshot.hedge.signals.skew.toFixed(4)}
            />

            <Row
              label="Toxicity"
              value={snapshot.hedge.signals.tox.toFixed(10)}
            />

            <Row
              label="Residual Signal Quality"
              value={snapshot.hedge.signals.residual_signal_quality.toFixed(2)}
            />

            <Row
              label="Toxicity Signal Quality"
              value={snapshot.hedge.signals.toxicity_signal_quality.toFixed(10)}
            />

            <Row
              label="Reservation"
              value={snapshot.hedge.signals.reservation.toFixed(10)}
            />

            {/* QUOTES */}
            <Section title="QUOTES" />

            <Row
              label="My Spread"
              value={`${snapshot.hedge.quotes.my_spread.toFixed(10)}`}
            />

            <Row
              label="My Bid / Size"
              value={`${snapshot.hedge.quotes.my_bid.toFixed(10)} (${snapshot.hedge.quotes.current_bid_size.toFixed(4)})`}
            />

            <Row
              label="My Ask / Size"
              value={`${snapshot.hedge.quotes.my_ask.toFixed(10)} (${snapshot.hedge.quotes.current_ask_size.toFixed(4)})`}
            />


            {/* EXECUTION */}
            <Section title="EXECUTION" />

            <Row
              label="Queue Ahead / Bid"
              value={`${snapshot.hedge.quotes.my_bid.toFixed(10)} (${snapshot.hedge.execution.bid_queue.toFixed(4)})`}
            />

            <Row
              label="Queue Ahead / Ask"
              value={`${snapshot.hedge.quotes.my_ask.toFixed(10)} (${snapshot.hedge.execution.ask_queue.toFixed(4)})`}
            />

            <Row
              label="Queue Pressure / Bid"
              value={`${snapshot.hedge.quotes.my_bid.toFixed(10)} (${snapshot.hedge.execution.bid_pressure.toFixed(4)})`}
            />

            <Row
              label="Queue Pressure / Ask"
              value={`${snapshot.hedge.quotes.my_ask.toFixed(10)} (${snapshot.hedge.execution.ask_pressure.toFixed(4)})`}
            />

            <Row
              label="Open Orders"
              value={snapshot.hedge.execution.buy_order}
            />

            <Row
              label=""
              value={snapshot.hedge.execution.sell_order}
            />

            <Row
              label="Last Fill Candidate"
              value={snapshot.hedge.execution.last_fill_candidate}
            />

            <Row
              label="Last Order Update"
              value={snapshot.hedge.execution.last_order_update}
            />

            {/* RISK */}
            <Section title="RISK" />

            <Row
              label="Inventory"
              value={
                <InventoryRisk inventory={snapshot.hedge.risk.inventory} />
              }
            />

            <Row
              label="Realized PnL"
              value={
                <PnL value={snapshot.hedge.risk.realized_pnl} />
              }
            />

            <Row
              label="Unrealized PnL"
              value={
                <PnL value={snapshot.hedge.risk.unrealized_pnl} />
              }
            />

            <Row
              label="Fees Paid"
              value={
                <PnL value={-snapshot.hedge.risk.fees_paid} />
              }
            />

            <Row
              label="Total PnL"
              value={
                <PnL value={snapshot.hedge.risk.total_pnl} />
              }
            />

            <Row
              label="Risk"
              value={
                <CenteredInventoryBar inv={snapshot.hedge.risk.inventory} />
              }
            />

            {/* SYSTEM */}
            <Section title="SYSTEM" />

            <Row
              label="Time"
              value={snapshot.hedge.system.time}
            />

            <Row
              label="Last Trade ts"
              value={snapshot.hedge.system.last_trade_ts}
            />

            <Row
              label="Last Depth ts"
              value={snapshot.hedge.system.last_depth_ts}
            />

            <Row
              label="Trade Latency"
              value={snapshot.hedge.system.trade_latency}
            />

            <Row
              label="Depth Latency"
              value={snapshot.hedge.system.depth_latency}
            />

            <Row
              label="Exchange Latency"
              value={snapshot.hedge.system.exchange_latency}
            />

          </tbody>
        </table>
        </>
      )}
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

  // -------------------------
  // TABS
  // -------------------------

  tabs: {
    display: "flex",
    gap: "8px",
    marginBottom: "15px",
  },

  tab: {
    background: "#1e293b",
    color: "#94a3b8",
    border: "1px solid #334155",
    padding: "10px 25px",
    cursor: "pointer",
    fontFamily: "monospace",
    fontSize: "15px",
  },

  activeTab: {
    background: "#334155",
    color: "#ffffff",
    border: "1px solid #72e5ff",
    padding: "10px 25px",
    cursor: "pointer",
    fontFamily: "monospace",
    fontSize: "15px",
    fontWeight: "bold",
  },

  // -------------------------
  // TABS
  // -------------------------

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

const emptySnapshot = {
  title: {
    header: "loading...",
    regime: "unknown_regime",
    pnl_pct: 0,
  },

  maker : {
     market: {
      instrument: "—",
      mid: 0,
      microprice: 0,
      spread: 0,
      best_bid: 0,
      bid_size: 0,
      best_ask: 0,
      ask_size: 0,
      ewma_vol: 0,
      order_imbalance: 0,
      trade_imbalance: 0,
      trade: "—",
    },

    signals: {
      spread_multiplier: 0,
      inventory_target: 0,
      alpha_trade_imb: 0,
      alpha_struct: 0,
      alpha_residual: 0,
      fair: 0,
      skew: 0,
      tox: 0,
      residual_signal_quality: 0,
      toxicity_signal_quality: 0,
      reservation: 0,
    },

    regime: {
      regime: "unknown_regime",
      confidence: 0,
    },

    quotes: {
      my_spread: 0,
      my_bid: 0,
      current_bid_size: 0,
      my_ask: 0,
      current_ask_size: 0,
    },

    execution: {
      bid_queue: 0,
      ask_queue: 0,
      bid_pressure: 0,
      ask_pressure: 0,
      buy_order: "—",
      sell_order: "—",
      last_fill_candidate: "—",
      last_order_update: "—",
    },

    risk: {
      inventory: 0,
      realized_pnl: 0,
      unrealized_pnl: 0,
      fees_paid: 0,
      total_pnl: 0,
    },

    system: {
      time: "—",
      last_trade_ts: "—",
      last_depth_ts: "—",
      trade_latency: "—",
      depth_latency: "—",
      exchange_latency: "—",
    },
  },
  hedge : {
    signals1: {
      maker_delta: 0,
      hedge_delta: 0,
      net_delta: 0,
      target_hedge_ratio: 0,
      target_hedge: 0,
      hedge_decision: "—",
      target_hedge: 0,
    },

     market: {
      instrument: "—",
      mid: 0,
      microprice: 0,
      spread: 0,
      best_bid: 0,
      bid_size: 0,
      best_ask: 0,
      ask_size: 0,
      ewma_vol: 0,
      order_imbalance: 0,
      trade_imbalance: 0,
      trade: "—",
      basis: 0,
      funding_rate: 0,
      next_funding_ts: "—",
    },

    signals: {
      spread_multiplier: 0,
      inventory_target: 0,
      alpha_trade_imb: 0,
      alpha_struct: 0,
      alpha_residual: 0,
      fair: 0,
      skew: 0,
      tox: 0,
      residual_signal_quality: 0,
      toxicity_signal_quality: 0,
      reservation: 0,
    },

    regime: {
      regime: "unknown_regime",
      confidence: 0,
    },

    quotes: {
      my_spread: 0,
      my_bid: 0,
      current_bid_size: 0,
      my_ask: 0,
      current_ask_size: 0,
    },

    execution: {
      bid_queue: 0,
      ask_queue: 0,
      bid_pressure: 0,
      ask_pressure: 0,
      buy_order: "—",
      sell_order: "—",
      last_fill_candidate: "—",
      last_order_update: "—",
    },

    risk: {
      inventory: 0,
      realized_pnl: 0,
      unrealized_pnl: 0,
      fees_paid: 0,
      total_pnl: 0,
    },

    system: {
      time: "—",
      last_trade_ts: "—",
      last_depth_ts: "—",
      trade_latency: "—",
      depth_latency: "—",
      exchange_latency: "—",
    },
  }
};