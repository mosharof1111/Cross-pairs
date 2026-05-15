'use strict';

const fetch = require('node-fetch');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');

const GAMMA     = 'https://gamma-api.polymarket.com';
const CLOB_REST = 'https://clob.polymarket.com';
const CLOB_WS   = 'wss://ws-subscriptions-clob.polymarket.com/ws/market';
const TRADES_FILE = path.join(__dirname, 'trades.json');

const WINDOW_SIZE      = 300;   // 5m only
const WINDOW_TRADE_CUTOFF = 270; // stop new buys/flips at 4:30 (30s before end)
const SHARES           = 50;
const STARTING_BALANCE = 1000;

// Ladder: [btcMinPrice, ethMaxPrice]
// BTC↑ >= btcMin AND ETH↑ < ethMax → buy ETH↑ at this level
const LADDER = [
  { btcMin: 0.70, ethMax: 0.40 },
  { btcMin: 0.80, ethMax: 0.50 },
  { btcMin: 0.90, ethMax: 0.60 },
  { btcMin: 0.97, ethMax: 0.67 },
];

let state = { balance: STARTING_BALANCE, openTrades: [], closedTrades: [], totalPnl: 0 };
const priceBook   = {};
const marketCache = {};  // key: windowStart timestamp

// Per-window trading state
// windowState[windowStart] = {
//   side: 'UP' | 'DOWN' | null,
//   levelsHit: Set of level indexes already bought this side
//   shares: total shares currently held
//   buys: [ { shares, price, cost } ]   — current open position legs
//   flips: number of flips done
//   stopped: bool — true after 4:30
// }
const windowState = {};

let emitFn = () => {};
let logFn  = () => {};

// ── Persistence ───────────────────────────────────────────────────────────────
function loadState() {
  try {
    if (fs.existsSync(TRADES_FILE)) {
      const raw = JSON.parse(fs.readFileSync(TRADES_FILE, 'utf8'));
      state = { ...state, ...raw };
      if (state.openTrades.length > 0) {
        log(`♻️  Refunding ${state.openTrades.length} open trade(s)`);
        for (const t of state.openTrades) state.balance += t.totalCost;
        state.openTrades = [];
        saveState();
      }
    }
  } catch (e) { log(`⚠️  State: ${e.message}`); }
}
function saveState() { fs.writeFileSync(TRADES_FILE, JSON.stringify(state, null, 2)); }

function log(msg) {
  const line = `[${new Date().toISOString().replace('T',' ').slice(0,19)}] ${msg}`;
  console.log(line); logFn(line);
}

function currentWindowStart() {
  return Math.floor(Math.floor(Date.now() / 1000) / WINDOW_SIZE) * WINDOW_SIZE;
}

function getWS(ws) {
  if (!windowState[ws]) {
    windowState[ws] = { side: null, levelsHit: new Set(), shares: 0, buys: [], flips: 0, stopped: false };
  }
  return windowState[ws];
}

// ── Price helpers ─────────────────────────────────────────────────────────────
// Use mid price (avg of bid and ask) or best available
function getPrice(tid) {
  const b = priceBook[tid];
  if (!b) return 0;
  if (b.bid > 0 && b.ask > 0) return (b.bid + b.ask) / 2;
  return b.bid || b.ask || 0;
}

// ── Token extraction ──────────────────────────────────────────────────────────
function extractTokenIds(mkt) {
  if (!mkt) return null;
  let ids = mkt.clobTokenIds ?? mkt.clob_token_ids;
  if (typeof ids === 'string') { try { ids = JSON.parse(ids); } catch (_) { ids = null; } }
  let outcomes = mkt.outcomes;
  if (typeof outcomes === 'string') { try { outcomes = JSON.parse(outcomes); } catch (_) { outcomes = null; } }
  if (Array.isArray(ids) && ids.length >= 2 && ids[0] && ids[1]) {
    if (Array.isArray(outcomes) && outcomes.length >= 2) {
      const upIdx = outcomes.findIndex(o => /up/i.test(String(o)));
      const dnIdx = outcomes.findIndex(o => /down/i.test(String(o)));
      if (upIdx >= 0 && dnIdx >= 0)
        return { upToken: String(ids[upIdx]), dnToken: String(ids[dnIdx]) };
    }
    return { upToken: String(ids[0]), dnToken: String(ids[1]) };
  }
  if (Array.isArray(mkt.tokens) && mkt.tokens.length >= 2) {
    const up = mkt.tokens.find(t => /up|yes/i.test(t.outcome ?? ''));
    const dn = mkt.tokens.find(t => /down|no/i.test(t.outcome ?? ''));
    if (up?.token_id && dn?.token_id) return { upToken: up.token_id, dnToken: dn.token_id };
    return { upToken: mkt.tokens[0].token_id, dnToken: mkt.tokens[1].token_id };
  }
  return null;
}

// ── Market discovery ──────────────────────────────────────────────────────────
async function getJson(url) {
  try {
    const res = await fetch(url, { timeout: 10000 });
    if (!res.ok) return null;
    const data = await res.json();
    return Array.isArray(data) ? (data[0] ?? null) : (data ?? null);
  } catch (_) { return null; }
}

function seedFromMarket(mkt, tokens) {
  const bestAsk = parseFloat(mkt.bestAsk ?? 0) || 0;
  const bestBid = parseFloat(mkt.bestBid ?? 0) || 0;
  let prices = mkt.outcomePrices;
  if (typeof prices === 'string') { try { prices = JSON.parse(prices); } catch (_) { prices = null; } }
  let outcomes = mkt.outcomes;
  if (typeof outcomes === 'string') { try { outcomes = JSON.parse(outcomes); } catch (_) { outcomes = null; } }
  if (bestAsk > 0 || bestBid > 0) {
    priceBook[tokens.upToken] = { bid: bestBid, ask: bestAsk };
    priceBook[tokens.dnToken] = { bid: Math.max(0, 1 - bestAsk), ask: Math.min(1, 1 - bestBid) };
  } else if (Array.isArray(prices) && Array.isArray(outcomes)) {
    const upIdx = outcomes.findIndex(o => /up/i.test(String(o)));
    const dnIdx = outcomes.findIndex(o => /down/i.test(String(o)));
    if (upIdx >= 0 && dnIdx >= 0) {
      const up = parseFloat(prices[upIdx]) || 0;
      const dn = parseFloat(prices[dnIdx]) || 0;
      if (up > 0) priceBook[tokens.upToken] = { bid: Math.max(0, up - 0.01), ask: Math.min(1, up + 0.01) };
      if (dn > 0) priceBook[tokens.dnToken] = { bid: Math.max(0, dn - 0.01), ask: Math.min(1, dn + 0.01) };
    }
  }
}

async function findTokensForSlug(slug) {
  const event = await getJson(`${GAMMA}/events/slug/${slug}`);
  if (event?.markets?.length) {
    const mkt = event.markets.find(m => m.acceptingOrders !== false) ?? event.markets[0];
    if (mkt) {
      const tokens = extractTokenIds(mkt);
      if (tokens) { seedFromMarket(mkt, tokens); return tokens; }
    }
  }
  const mkt = await getJson(`${GAMMA}/markets/slug/${slug}`);
  if (mkt) {
    const tokens = extractTokenIds(mkt);
    if (tokens) { seedFromMarket(mkt, tokens); return tokens; }
  }
  return null;
}

async function refreshMarkets() {
  const currentWs = currentWindowStart();
  if (marketCache[currentWs]) return;

  log(`🔍 Finding 5m markets…`);
  const offsets = [0, 1, -1, 2, -2];

  for (const offset of offsets) {
    const ts      = currentWs + offset * WINDOW_SIZE;
    const btcSlug = `btc-updown-5m-${ts}`;
    const ethSlug = `eth-updown-5m-${ts}`;

    const [btcTokens, ethTokens] = await Promise.all([
      findTokensForSlug(btcSlug),
      findTokensForSlug(ethSlug),
    ]);

    if (!btcTokens || !ethTokens) continue;

    marketCache[currentWs] = {
      windowStart: ts,
      btcUp: btcTokens.upToken, btcDn: btcTokens.dnToken,
      ethUp: ethTokens.upToken, ethDn: ethTokens.dnToken,
      btcSlug, ethSlug,
    };

    log(`✅ 5m ws=${ts} | BTC↑ ${btcTokens.upToken.slice(0,12)}… ETH↑ ${ethTokens.upToken.slice(0,12)}…`);
    for (const tid of [btcTokens.upToken, btcTokens.dnToken, ethTokens.upToken, ethTokens.dnToken])
      subscribeToken(tid);
    break;
  }
}

// ── REST price polling ────────────────────────────────────────────────────────
async function pollPrices() {
  const w = marketCache[currentWindowStart()];
  if (!w) return;
  const tids = [w.btcUp, w.btcDn, w.ethUp, w.ethDn];
  let updated = 0;
  await Promise.all(tids.map(async tid => {
    try {
      const [ar, br] = await Promise.all([
        fetch(`${CLOB_REST}/price?token_id=${tid}&side=BUY`,  { timeout: 5000 }),
        fetch(`${CLOB_REST}/price?token_id=${tid}&side=SELL`, { timeout: 5000 }),
      ]);
      const ask = parseFloat((await ar.json()).price ?? 0) || 0;
      const bid = parseFloat((await br.json()).price ?? 0) || 0;
      if (ask > 0 || bid > 0) { priceBook[tid] = { bid, ask }; updated++; }
    } catch (_) {}
  }));
  if (updated > 0) log(`💲 REST poll: ${updated}/4 prices updated`);
}

async function fetchFreshPrice(tid) {
  try {
    const r = await fetch(`${CLOB_REST}/price?token_id=${tid}&side=BUY`, { timeout: 5000 });
    return parseFloat((await r.json()).price ?? 0) || 0;
  } catch (_) { return 0; }
}

// ── WebSocket ─────────────────────────────────────────────────────────────────
let ws = null, wsReady = false;
const pendingSubs = new Set();

function connectWS() {
  log('🔌 Connecting CLOB WebSocket…');
  ws = new WebSocket(CLOB_WS);
  ws.on('open', () => {
    wsReady = true; log('✅ WebSocket connected');
    for (const t of pendingSubs) _sub(t); pendingSubs.clear();
  });
  ws.on('message', raw => {
    try {
      const msgs = JSON.parse(raw);
      (Array.isArray(msgs) ? msgs : [msgs]).forEach(handleWsMsg);
    } catch (_) {}
  });
  ws.on('close', () => { wsReady = false; log('⚡ WS closed — retry 5s'); setTimeout(connectWS, 5000); });
  ws.on('error', e => log(`⚠️  WS: ${e.message}`));
}
function _sub(tid) { ws.send(JSON.stringify({ assets_ids: [tid], type: 'market' })); }
function subscribeToken(tid) {
  if (!tid) return;
  if (!wsReady || ws?.readyState !== WebSocket.OPEN) { pendingSubs.add(tid); return; }
  _sub(tid);
}
function handleWsMsg(msg) {
  const tid = msg.asset_id ?? msg.token_id ?? msg.market;
  if (!tid) return;
  if (Array.isArray(msg.bids) || Array.isArray(msg.asks)) {
    const bid = parseFloat(msg.bids?.[0]?.price ?? 0) || 0;
    const ask = parseFloat(msg.asks?.[0]?.price ?? 0) || 0;
    if (bid > 0 || ask > 0) priceBook[tid] = { bid, ask };
    return;
  }
  const bid = parseFloat(msg.bid ?? msg.best_bid ?? 0) || 0;
  const ask = parseFloat(msg.ask ?? msg.best_ask ?? 0) || 0;
  if (bid > 0 || ask > 0) priceBook[tid] = { bid, ask };
}

// ── Trading logic ─────────────────────────────────────────────────────────────
function tradeId() { return `T${Date.now().toString(36).toUpperCase()}`; }

async function checkWindow(w) {
  const nowSec  = Math.floor(Date.now() / 1000);
  const elapsed = nowSec - w.windowStart;
  const ws      = getWS(w.windowStart);

  // Stop new activity after 4:30
  if (elapsed >= WINDOW_TRADE_CUTOFF) {
    if (!ws.stopped) {
      ws.stopped = true;
      log(`⏱️  Window 5m ws=${w.windowStart} — cutoff reached, holding for resolution`);
    }
    return;
  }

  const btcUp = getPrice(w.btcUp);
  const btcDn = getPrice(w.btcDn);  // = 1 - btcUp roughly
  const ethUp = getPrice(w.ethUp);
  const ethDn = getPrice(w.ethDn);

  if (!btcUp || !ethUp) return;

  // ── Flip check ──────────────────────────────────────────────────────────────
  // If holding ETH↑ and BTC↓ >= 0.70 → flip to ETH↓
  if (ws.side === 'UP' && ws.shares > 0 && btcDn >= 0.70) {
    await flip(w, ws, 'DOWN', ethUp, ethDn);
    return;
  }
  // If holding ETH↓ and BTC↑ >= 0.70 → flip to ETH↑
  if (ws.side === 'DOWN' && ws.shares > 0 && btcUp >= 0.70) {
    await flip(w, ws, 'UP', ethUp, ethDn);
    return;
  }

  // ── Ladder buys ─────────────────────────────────────────────────────────────
  if (btcUp >= 0.70 && (ws.side === 'UP' || ws.side === null)) {
    // Check each ladder level
    for (let i = 0; i < LADDER.length; i++) {
      if (ws.levelsHit.has(i)) continue; // already bought this level
      const { btcMin, ethMax } = LADDER[i];
      if (btcUp >= btcMin && ethUp < ethMax) {
        buyEth(w, ws, 'UP', i, ethUp);
        break; // one buy per tick
      }
    }
  } else if (btcDn >= 0.70 && (ws.side === 'DOWN' || ws.side === null)) {
    for (let i = 0; i < LADDER.length; i++) {
      if (ws.levelsHit.has(i)) continue;
      const { btcMin, ethMax } = LADDER[i];
      if (btcDn >= btcMin && ethDn < ethMax) {
        buyEth(w, ws, 'DOWN', i, ethDn);
        break;
      }
    }
  }
}

function buyEth(w, ws, side, levelIdx, ethPrice) {
  const cost = ethPrice * SHARES;
  if (state.balance < cost) { log(`💸 Low balance $${state.balance.toFixed(2)}`); return; }

  ws.side = side;
  ws.levelsHit.add(levelIdx);
  ws.shares += SHARES;
  ws.buys.push({ shares: SHARES, price: ethPrice, cost });
  state.balance -= cost;

  const token = side === 'UP' ? w.ethUp : w.ethDn;
  const id = tradeId();
  const openTrade = {
    id, windowStart: w.windowStart, side,
    token, shares: SHARES, entryPrice: ethPrice,
    totalCost: cost, openedAt: new Date().toISOString(),
    level: levelIdx + 1, floatingPnl: 0,
  };
  state.openTrades.push(openTrade);
  saveState();

  log(`🟢 BUY ETH${side} L${levelIdx+1} [${id}] price=${ethPrice.toFixed(3)} shares=${SHARES} cost=$${cost.toFixed(2)} totalShares=${ws.shares} bal=$${state.balance.toFixed(2)}`);
  emitFn('trade_entered', openTrade);
}

async function flip(w, ws, newSide, ethUp, ethDn) {
  log(`🔄 FLIP → ETH${newSide} (flip #${ws.flips + 1})`);

  // Sell all current position at current market price
  const oldSide  = ws.side;
  const oldToken = oldSide === 'UP' ? w.ethUp : w.ethDn;
  const sellPrice = getPrice(oldToken);
  const sellProceeds = sellPrice * ws.shares;

  // Close all open trades for this window/side
  const toClose = state.openTrades.filter(t => t.windowStart === w.windowStart && t.side === oldSide);
  for (const t of toClose) {
    const pnl = sellPrice * t.shares - t.totalCost;
    state.balance += sellPrice * t.shares;
    state.totalPnl += pnl;
    state.closedTrades.push({
      ...t, exitPrice: sellPrice, exitProceeds: +(sellPrice * t.shares).toFixed(2),
      realizedPnl: +pnl.toFixed(4), closedAt: new Date().toISOString(), exitReason: 'FLIP',
    });
    log(`  📤 Sold ETH${oldSide} [${t.id}] price=${sellPrice.toFixed(3)} pnl=$${pnl.toFixed(2)}`);
  }
  state.openTrades = state.openTrades.filter(t => !(t.windowStart === w.windowStart && t.side === oldSide));

  // Reset window state for new side
  ws.side = null;
  ws.shares = 0;
  ws.buys = [];
  ws.levelsHit = new Set();
  ws.flips++;
  saveState();

  // Now buy first level on new side immediately if condition met
  const btcUp = getPrice(w.btcUp);
  const btcDn = getPrice(w.btcDn);
  const ethPrice = newSide === 'UP' ? ethUp : ethDn;
  const btcPrice = newSide === 'UP' ? btcUp : btcDn;

  if (btcPrice >= 0.70) {
    for (let i = 0; i < LADDER.length; i++) {
      const { btcMin, ethMax } = LADDER[i];
      if (btcPrice >= btcMin && ethPrice < ethMax) {
        buyEth(w, ws, newSide, i, ethPrice);
        break;
      }
    }
  }

  emitFn('snapshot', buildDashboardSnapshot());
}

// ── Resolution at window end ──────────────────────────────────────────────────
async function checkResolution() {
  const nowSec = Math.floor(Date.now() / 1000);

  for (const [wsTs, w] of Object.entries(marketCache)) {
    const windowEnd = w.windowStart + WINDOW_SIZE;
    if (nowSec < windowEnd + 30) continue; // wait 30s after window end

    const ws = windowState[w.windowStart];
    if (!ws || ws.shares === 0) continue;

    const tradesInWindow = state.openTrades.filter(t => t.windowStart === w.windowStart);
    if (!tradesInWindow.length) continue;

    log(`⏰ Resolving window ws=${w.windowStart}…`);

    // Fetch fresh resolved prices
    const [upPrice, dnPrice] = await Promise.all([
      fetchFreshPrice(w.ethUp),
      fetchFreshPrice(w.ethDn),
    ]);

    for (const t of tradesInWindow) {
      const resolvedPrice = t.side === 'UP'
        ? (upPrice || getPrice(w.ethUp))
        : (dnPrice || getPrice(w.ethDn));
      const proceeds = resolvedPrice * t.shares;
      const pnl = proceeds - t.totalCost;
      state.balance += proceeds;
      state.totalPnl += pnl;
      state.closedTrades.push({
        ...t, exitPrice: resolvedPrice, exitProceeds: +proceeds.toFixed(2),
        realizedPnl: +pnl.toFixed(4), closedAt: new Date().toISOString(), exitReason: 'RESOLVED',
      });
      log(`${pnl >= 0 ? '🟢' : '🔴'} RESOLVED ETH${t.side} [${t.id}] price=${resolvedPrice.toFixed(3)} pnl=$${pnl.toFixed(2)} bal=$${state.balance.toFixed(2)}`);
      emitFn('trade_closed', t);
    }

    state.openTrades = state.openTrades.filter(t => t.windowStart !== w.windowStart);
    if (ws) ws.shares = 0;
    saveState();

    // Clean up old window from cache
    delete marketCache[wsTs];
    delete windowState[w.windowStart];
  }
}

// ── Update floating PnL ───────────────────────────────────────────────────────
function updateFloating() {
  for (const t of state.openTrades) {
    const price = t.side === 'UP'
      ? getPrice(marketCache[currentWindowStart()]?.ethUp)
      : getPrice(marketCache[currentWindowStart()]?.ethDn);
    if (price > 0) t.floatingPnl = +(price * t.shares - t.totalCost).toFixed(4);
  }
}

// ── Dashboard snapshot ────────────────────────────────────────────────────────
function buildDashboardSnapshot() {
  const w = marketCache[currentWindowStart()];
  const ws = w ? getWS(w.windowStart) : null;
  const nowSec  = Math.floor(Date.now() / 1000);
  const elapsed = w ? nowSec - w.windowStart : 0;
  const remaining = w ? Math.max(0, WINDOW_SIZE - elapsed) : 0;

  return {
    balance:     +state.balance.toFixed(2),
    totalPnl:    +state.totalPnl.toFixed(2),
    openTrades:  state.openTrades,
    closedTrades: state.closedTrades.slice(-30),
    updatedAt:   new Date().toISOString(),
    window: w ? {
      windowStart: w.windowStart,
      elapsed, remaining,
      stopped: ws?.stopped ?? false,
      side: ws?.side ?? null,
      totalShares: ws?.shares ?? 0,
      flips: ws?.flips ?? 0,
      levelsHit: ws ? [...ws.levelsHit] : [],
      btcUpPrice:  +getPrice(w.btcUp).toFixed(3),
      btcDnPrice:  +getPrice(w.btcDn).toFixed(3),
      ethUpPrice:  +getPrice(w.ethUp).toFixed(3),
      ethDnPrice:  +getPrice(w.ethDn).toFixed(3),
    } : null,
  };
}

// ── Prune old market cache ────────────────────────────────────────────────────
function prune() {
  const current = currentWindowStart();
  for (const key of Object.keys(marketCache))
    if (Number(key) < current - WINDOW_SIZE) delete marketCache[key];
}

// ── Main loop ─────────────────────────────────────────────────────────────────
let timer = null, pollTimer = null;

async function tick() {
  try {
    prune();
    await refreshMarkets();
    const w = marketCache[currentWindowStart()];
    if (w) {
      updateFloating();
      await checkWindow(w);
      await checkResolution();
    }
    emitFn('snapshot', buildDashboardSnapshot());
  } catch (e) { log(`⚠️  tick: ${e.message}`); }
}

async function start(emit, logEmit) {
  emitFn = emit; logFn = logEmit;
  log('🚀 Polymarket ETH Ladder Bot (5m windows)');
  loadState(); connectWS(); await tick();
  timer     = setInterval(tick, 5000);
  pollTimer = setInterval(pollPrices, 10000);
  log(`💰 Balance: $${state.balance.toFixed(2)}`);
}
function stop() { clearInterval(timer); clearInterval(pollTimer); ws?.terminate(); }
module.exports = { start, stop, buildDashboardSnapshot };
