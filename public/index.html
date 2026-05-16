<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>POLYMARKET ARB // BTC⇄ETH</title>
<link rel="preconnect" href="https://fonts.googleapis.com"/>
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;700&family=Bebas+Neue&display=swap" rel="stylesheet"/>
<script src="/socket.io/socket.io.js"></script>
<style>
  :root {
    --bg:       #080c10;
    --panel:    #0d1219;
    --border:   #1a2535;
    --border2:  #243044;
    --text:     #c8d8e8;
    --dim:      #4a6070;
    --green:    #00e676;
    --red:      #ff3d5a;
    --amber:    #ffb300;
    --cyan:     #00cfff;
    --purple:   #9c6aff;
    --entry:    #ff6b35;
    --exit:     #00e676;
    --font:     'JetBrains Mono', monospace;
    --display:  'Bebas Neue', sans-serif;
  }

  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

  body {
    background: var(--bg);
    color: var(--text);
    font-family: var(--font);
    font-size: 12px;
    min-height: 100vh;
    overflow-x: hidden;
  }

  /* Scanline overlay */
  body::before {
    content: '';
    position: fixed;
    inset: 0;
    background: repeating-linear-gradient(
      0deg,
      transparent,
      transparent 2px,
      rgba(0,0,0,0.08) 2px,
      rgba(0,0,0,0.08) 4px
    );
    pointer-events: none;
    z-index: 999;
  }

  /* ── Header ── */
  header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 12px 20px;
    border-bottom: 1px solid var(--border);
    background: linear-gradient(90deg, #0d1219 0%, #0f1820 100%);
    position: sticky;
    top: 0;
    z-index: 10;
  }

  .brand {
    font-family: var(--display);
    font-size: 22px;
    letter-spacing: 3px;
    color: var(--cyan);
    text-shadow: 0 0 20px rgba(0,207,255,0.4);
  }

  .brand span { color: var(--amber); }

  .status-bar {
    display: flex;
    gap: 24px;
    align-items: center;
  }

  .status-item {
    display: flex;
    flex-direction: column;
    align-items: flex-end;
  }

  .status-label {
    font-size: 9px;
    color: var(--dim);
    letter-spacing: 1px;
    text-transform: uppercase;
  }

  .status-value {
    font-size: 16px;
    font-weight: 700;
    color: var(--cyan);
  }

  .status-value.positive { color: var(--green); }
  .status-value.negative { color: var(--red); }

  .dot {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: var(--green);
    box-shadow: 0 0 8px var(--green);
    animation: pulse 1.5s ease-in-out infinite;
  }
  .dot.offline { background: var(--red); box-shadow: 0 0 8px var(--red); animation: none; }

  @keyframes pulse {
    0%,100% { opacity: 1; }
    50%      { opacity: 0.4; }
  }

  /* ── Layout ── */
  main {
    display: grid;
    grid-template-columns: 1fr 1fr;
    grid-template-rows: auto auto 1fr;
    gap: 12px;
    padding: 12px 16px;
    max-width: 1600px;
    margin: 0 auto;
  }

  /* ── Panel ── */
  .panel {
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: 4px;
    overflow: hidden;
  }

  .panel-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 8px 14px;
    background: rgba(255,255,255,0.02);
    border-bottom: 1px solid var(--border);
  }

  .panel-title {
    font-size: 10px;
    font-weight: 700;
    letter-spacing: 2px;
    text-transform: uppercase;
    color: var(--dim);
  }

  .panel-badge {
    font-size: 9px;
    padding: 2px 6px;
    border-radius: 2px;
    background: rgba(0,207,255,0.1);
    color: var(--cyan);
    border: 1px solid rgba(0,207,255,0.2);
  }

  /* ── Markets Grid ── */
  #markets-panel { grid-column: 1 / -1; }

  .markets-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(340px, 1fr));
    gap: 1px;
    background: var(--border);
  }

  .market-window {
    background: var(--panel);
    padding: 10px 14px;
    transition: background 0.2s;
  }

  .market-window.has-signal { background: rgba(255,107,53,0.04); }

  .mw-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 8px;
  }

  .mw-tf {
    font-size: 10px;
    font-weight: 700;
    color: var(--amber);
    letter-spacing: 1px;
  }

  .mw-window {
    font-size: 9px;
    color: var(--dim);
  }

  .price-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 6px;
    margin-bottom: 8px;
  }

  .price-card {
    background: rgba(255,255,255,0.02);
    border: 1px solid var(--border);
    border-radius: 3px;
    padding: 6px 8px;
  }

  .price-card-label {
    font-size: 9px;
    color: var(--dim);
    margin-bottom: 3px;
    letter-spacing: 0.5px;
  }

  .price-row {
    display: flex;
    gap: 8px;
    align-items: baseline;
  }

  .price-ask { color: var(--red);   font-size: 13px; font-weight: 700; }
  .price-bid { color: var(--green); font-size: 13px; font-weight: 700; }
  .price-sep { color: var(--dim);   font-size: 11px; }

  .gap-row {
    display: flex;
    gap: 8px;
    margin-top: 6px;
  }

  .gap-pill {
    flex: 1;
    padding: 4px 8px;
    border-radius: 3px;
    font-size: 10px;
    font-weight: 700;
    text-align: center;
    border: 1px solid var(--border);
    color: var(--dim);
    background: rgba(255,255,255,0.01);
    transition: all 0.3s;
  }

  .gap-pill.signal-entry {
    color: var(--entry);
    border-color: var(--entry);
    background: rgba(255,107,53,0.08);
    box-shadow: 0 0 12px rgba(255,107,53,0.15);
    animation: flashEntry 0.6s ease-out;
  }

  .gap-pill.signal-exit {
    color: var(--exit);
    border-color: var(--exit);
    background: rgba(0,230,118,0.08);
    box-shadow: 0 0 12px rgba(0,230,118,0.15);
  }

  @keyframes flashEntry {
    0%   { box-shadow: 0 0 30px rgba(255,107,53,0.6); }
    100% { box-shadow: 0 0 12px rgba(255,107,53,0.15); }
  }

  /* ── Open Trades ── */
  #open-panel { grid-column: 1; }
  #closed-panel { grid-column: 2; }

  .trades-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 11px;
  }

  .trades-table th {
    padding: 6px 10px;
    text-align: left;
    font-size: 9px;
    font-weight: 700;
    letter-spacing: 1.5px;
    text-transform: uppercase;
    color: var(--dim);
    border-bottom: 1px solid var(--border);
    background: rgba(255,255,255,0.01);
    white-space: nowrap;
  }

  .trades-table td {
    padding: 6px 10px;
    border-bottom: 1px solid var(--border);
    color: var(--text);
    white-space: nowrap;
  }

  .trades-table tr:last-child td { border-bottom: none; }
  .trades-table tr:hover td { background: rgba(255,255,255,0.015); }

  .trade-id    { font-size: 10px; color: var(--purple); font-weight: 700; }
  .trade-type  { font-size: 9px;  color: var(--cyan); }
  .trade-tf    { font-size: 9px;  color: var(--amber); }
  .pnl-pos     { color: var(--green); font-weight: 700; }
  .pnl-neg     { color: var(--red);   font-weight: 700; }
  .pnl-zero    { color: var(--dim); }

  .empty-state {
    padding: 20px;
    text-align: center;
    color: var(--dim);
    font-size: 11px;
  }

  /* ── Log Panel ── */
  #log-panel { grid-column: 1 / -1; }

  #log-container {
    height: 180px;
    overflow-y: auto;
    padding: 8px 14px;
    scroll-behavior: smooth;
  }

  #log-container::-webkit-scrollbar { width: 4px; }
  #log-container::-webkit-scrollbar-track { background: transparent; }
  #log-container::-webkit-scrollbar-thumb { background: var(--border2); border-radius: 2px; }

  .log-line {
    padding: 2px 0;
    font-size: 11px;
    color: var(--dim);
    line-height: 1.5;
    border-bottom: 1px solid rgba(255,255,255,0.02);
    opacity: 0;
    animation: fadeIn 0.3s forwards;
  }

  @keyframes fadeIn { to { opacity: 1; } }

  .log-line.info   { color: var(--text); }
  .log-line.entry  { color: var(--entry); }
  .log-line.exit   { color: var(--green); }
  .log-line.warn   { color: var(--amber); }
  .log-line.error  { color: var(--red); }

  /* ── Ticker tape ── */
  .ticker-row {
    display: flex;
    gap: 2px;
    overflow: hidden;
    padding: 4px 14px;
    background: rgba(0,0,0,0.3);
    border-top: 1px solid var(--border);
  }

  .ticker-item {
    font-size: 10px;
    white-space: nowrap;
    color: var(--dim);
    padding: 0 6px;
  }

  .ticker-item.flash {
    animation: tickFlash 0.5s ease-out;
  }

  @keyframes tickFlash {
    0%   { color: var(--cyan); }
    100% { color: var(--dim); }
  }

  /* Utility */
  .tag {
    display: inline-block;
    font-size: 8px;
    padding: 1px 4px;
    border-radius: 2px;
    font-weight: 700;
    letter-spacing: 0.5px;
    text-transform: uppercase;
  }
  .tag-open   { background: rgba(0,207,255,0.15); color: var(--cyan); border: 1px solid rgba(0,207,255,0.3); }
  .tag-closed { background: rgba(0,230,118,0.10); color: var(--green); border: 1px solid rgba(0,230,118,0.2); }
  .tag-loss   { background: rgba(255,61,90,0.10);  color: var(--red);  border: 1px solid rgba(255,61,90,0.2); }

  @media (max-width: 900px) {
    main { grid-template-columns: 1fr; }
    #open-panel, #closed-panel, #log-panel { grid-column: 1; }
  }
</style>
</head>
<body>

<header>
  <div class="brand">POLY<span>ARB</span> // BTC⇄ETH</div>
  <div class="status-bar">
    <div class="status-item">
      <span class="status-label">Balance</span>
      <span class="status-value" id="hdr-balance">$—</span>
    </div>
    <div class="status-item">
      <span class="status-label">Total P&L</span>
      <span class="status-value" id="hdr-pnl">$—</span>
    </div>
    <div class="status-item">
      <span class="status-label">Open</span>
      <span class="status-value" id="hdr-open">0</span>
    </div>
    <div class="status-item">
      <span class="status-label">WS</span>
      <div class="dot offline" id="ws-dot"></div>
    </div>
  </div>
</header>

<main>

  <!-- Markets -->
  <div class="panel" id="markets-panel">
    <div class="panel-header">
      <span class="panel-title">Live Markets</span>
      <span class="panel-badge" id="last-update">—</span>
    </div>
    <div class="markets-grid" id="markets-grid">
      <div class="market-window">
        <div class="mw-header">
          <span class="mw-tf">—</span>
          <span class="mw-window">Waiting for data…</span>
        </div>
      </div>
    </div>
  </div>

  <!-- Open Trades -->
  <div class="panel" id="open-panel">
    <div class="panel-header">
      <span class="panel-title">Open Trades</span>
      <span class="panel-badge" id="open-count">0</span>
    </div>
    <div id="open-trades-body">
      <div class="empty-state">No open trades</div>
    </div>
  </div>

  <!-- Closed Trades -->
  <div class="panel" id="closed-panel">
    <div class="panel-header">
      <span class="panel-title">Closed Trades</span>
      <span class="panel-badge" id="closed-count">0</span>
    </div>
    <div id="closed-trades-body">
      <div class="empty-state">No closed trades yet</div>
    </div>
  </div>

  <!-- Log -->
  <div class="panel" id="log-panel">
    <div class="panel-header">
      <span class="panel-title">System Log</span>
      <span class="panel-badge" id="log-count">0</span>
    </div>
    <div id="log-container"></div>
  </div>

</main>

<script>
const socket = io();
let logCount = 0;
const MAX_LOGS = 200;

// ── Connection state ──────────────────────────────────────────────────────────
socket.on('connect',    () => setWsDot(true));
socket.on('disconnect', () => setWsDot(false));

function setWsDot(online) {
  const dot = document.getElementById('ws-dot');
  dot.classList.toggle('offline', !online);
}

// ── Snapshot ──────────────────────────────────────────────────────────────────
socket.on('snapshot', (data) => {
  renderHeader(data);
  renderMarkets(data.windows || []);
  renderOpenTrades(data.openTrades || []);
  renderClosedTrades(data.closedTrades || []);
  document.getElementById('last-update').textContent =
    new Date(data.updatedAt).toLocaleTimeString();
});

socket.on('trade_entered', () => { /* snapshot will follow */ });
socket.on('trade_closed',  () => { /* snapshot will follow */ });

// ── Log ───────────────────────────────────────────────────────────────────────
socket.on('log', (line) => {
  appendLog(line);
});

function appendLog(line) {
  const el = document.createElement('div');
  el.className = 'log-line ' + classifyLog(line);
  el.textContent = line;
  const container = document.getElementById('log-container');
  container.appendChild(el);
  logCount++;
  document.getElementById('log-count').textContent = logCount;

  // Cap logs
  while (container.children.length > MAX_LOGS) {
    container.removeChild(container.firstChild);
  }
  container.scrollTop = container.scrollHeight;
}

function classifyLog(line) {
  if (line.includes('ENTRY')) return 'entry';
  if (line.includes('EXIT'))  return 'exit';
  if (line.includes('⚠️') || line.includes('Refund')) return 'warn';
  if (line.includes('error') || line.includes('Error')) return 'error';
  if (line.includes('✅') || line.includes('🚀') || line.includes('connected')) return 'info';
  return '';
}

// ── Header ────────────────────────────────────────────────────────────────────
function renderHeader(data) {
  setText('hdr-balance', `$${fmt2(data.balance)}`);

  const pnlEl = document.getElementById('hdr-pnl');
  pnlEl.textContent = `${data.totalPnl >= 0 ? '+' : ''}$${fmt2(data.totalPnl)}`;
  pnlEl.className = 'status-value ' + (data.totalPnl > 0 ? 'positive' : data.totalPnl < 0 ? 'negative' : '');

  setText('hdr-open',    data.openTrades.length);
  setText('open-count',  data.openTrades.length);
  setText('closed-count',data.closedTrades.length);
}

// ── Markets ───────────────────────────────────────────────────────────────────
function renderMarkets(windows) {
  const grid = document.getElementById('markets-grid');
  if (!windows.length) return;

  grid.innerHTML = windows.map(w => {
    const eg1 = w.entryGap1;
    const eg2 = w.entryGap2;
    const xg1 = w.exitGap1;
    const xg2 = w.exitGap2;

    const signalEntry = (eg1 !== null && eg1 <= -0.20) || (eg2 !== null && eg2 <= -0.20);

    const gapClass1e = eg1 !== null && eg1 <= -0.20 ? 'signal-entry' : '';
    const gapClass2e = eg2 !== null && eg2 <= -0.20 ? 'signal-entry' : '';
    const gapClass1x = xg1 !== null && xg1 >= 0.05  ? 'signal-exit'  : '';
    const gapClass2x = xg2 !== null && xg2 >= 0.05  ? 'signal-exit'  : '';

    return `
    <div class="market-window ${signalEntry ? 'has-signal' : ''}">
      <div class="mw-header">
        <span class="mw-tf">${w.tf.toUpperCase()}</span>
        <span class="mw-window">ws:${w.windowStart}</span>
      </div>
      <div class="price-grid">
        <div class="price-card">
          <div class="price-card-label">BTC ↑</div>
          <div class="price-row">
            <span class="price-ask">${fmtP(w.btcUpAsk)}</span>
            <span class="price-sep">/</span>
            <span class="price-bid">${fmtP(w.btcUpBid)}</span>
          </div>
        </div>
        <div class="price-card">
          <div class="price-card-label">BTC ↓</div>
          <div class="price-row">
            <span class="price-ask">${fmtP(w.btcDnAsk)}</span>
            <span class="price-sep">/</span>
            <span class="price-bid">${fmtP(w.btcDnBid)}</span>
          </div>
        </div>
        <div class="price-card">
          <div class="price-card-label">ETH ↑</div>
          <div class="price-row">
            <span class="price-ask">${fmtP(w.ethUpAsk)}</span>
            <span class="price-sep">/</span>
            <span class="price-bid">${fmtP(w.ethUpBid)}</span>
          </div>
        </div>
        <div class="price-card">
          <div class="price-card-label">ETH ↓</div>
          <div class="price-row">
            <span class="price-ask">${fmtP(w.ethDnAsk)}</span>
            <span class="price-sep">/</span>
            <span class="price-bid">${fmtP(w.ethDnBid)}</span>
          </div>
        </div>
      </div>
      <div class="gap-row">
        <div class="gap-pill ${gapClass1e}" title="Entry gap: BTC↑ask + ETH↓ask - 1">
          E₁ ${eg1 !== null ? (eg1 >= 0 ? '+' : '') + eg1 : '—'}
        </div>
        <div class="gap-pill ${gapClass2e}" title="Entry gap: ETH↑ask + BTC↓ask - 1">
          E₂ ${eg2 !== null ? (eg2 >= 0 ? '+' : '') + eg2 : '—'}
        </div>
        <div class="gap-pill ${gapClass1x}" title="Exit gap: BTC↑bid + ETH↓bid - 1">
          X₁ ${xg1 !== null ? (xg1 >= 0 ? '+' : '') + xg1 : '—'}
        </div>
        <div class="gap-pill ${gapClass2x}" title="Exit gap: ETH↑bid + BTC↓bid - 1">
          X₂ ${xg2 !== null ? (xg2 >= 0 ? '+' : '') + xg2 : '—'}
        </div>
      </div>
    </div>`;
  }).join('');
}

// ── Trades ────────────────────────────────────────────────────────────────────
function renderOpenTrades(trades) {
  const el = document.getElementById('open-trades-body');
  if (!trades.length) {
    el.innerHTML = '<div class="empty-state">No open trades</div>';
    return;
  }
  el.innerHTML = `<table class="trades-table">
    <thead><tr>
      <th>ID</th><th>Type</th><th>TF</th><th>Cost</th><th>Float P&L</th><th>Age</th>
    </tr></thead>
    <tbody>${trades.map(t => {
      const pnl = t.floatingPnl ?? 0;
      const pnlClass = pnl > 0 ? 'pnl-pos' : pnl < 0 ? 'pnl-neg' : 'pnl-zero';
      const age = Math.floor((Date.now() - new Date(t.openedAt)) / 1000);
      return `<tr>
        <td class="trade-id">${t.id}</td>
        <td class="trade-type">${t.type}</td>
        <td class="trade-tf">${t.tf}</td>
        <td>$${fmt2(t.entryCost)}</td>
        <td class="${pnlClass}">${pnl >= 0 ? '+' : ''}$${fmt2(pnl)}</td>
        <td>${formatAge(age)}</td>
      </tr>`;
    }).join('')}</tbody>
  </table>`;
}

function renderClosedTrades(trades) {
  const el = document.getElementById('closed-trades-body');
  if (!trades.length) {
    el.innerHTML = '<div class="empty-state">No closed trades yet</div>';
    return;
  }
  const sorted = [...trades].reverse();
  el.innerHTML = `<table class="trades-table">
    <thead><tr>
      <th>ID</th><th>Type</th><th>TF</th><th>P&L</th><th>Entry Gap</th><th>Exit Gap</th>
    </tr></thead>
    <tbody>${sorted.map(t => {
      const pnl = t.realizedPnl ?? 0;
      const pnlClass = pnl > 0 ? 'pnl-pos' : pnl < 0 ? 'pnl-neg' : 'pnl-zero';
      return `<tr>
        <td class="trade-id">${t.id}</td>
        <td class="trade-type">${t.type}</td>
        <td class="trade-tf">${t.tf}</td>
        <td class="${pnlClass}">${pnl >= 0 ? '+' : ''}$${fmt2(pnl)}</td>
        <td style="color:var(--entry)">${t.entryGap}</td>
        <td style="color:var(--exit)">${t.exitGap ?? '—'}</td>
      </tr>`;
    }).join('')}</tbody>
  </table>`;
}

// ── Helpers ───────────────────────────────────────────────────────────────────
function setText(id, val) {
  const el = document.getElementById(id);
  if (el) el.textContent = val;
}

function fmt2(n) {
  return (typeof n === 'number' ? n : parseFloat(n) || 0).toFixed(2);
}

function fmtP(n) {
  if (!n || n === 0) return '—';
  return parseFloat(n).toFixed(3);
}

function formatAge(seconds) {
  if (seconds < 60)   return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds/60)}m`;
  return `${Math.floor(seconds/3600)}h`;
}
</script>
</body>
</html>
