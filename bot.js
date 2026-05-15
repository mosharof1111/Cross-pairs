'use strict';

const fetch = require('node-fetch');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');

const GAMMA     = 'https://gamma-api.polymarket.com';
const CLOB_WS   = 'wss://ws-subscriptions-clob.polymarket.com/ws/market';
const TRADES_FILE = path.join(__dirname, 'trades.json');

const WINDOW_SIZE         = 300;  // 5 minutes
const WINDOW_TRADE_CUTOFF = 270;  // stop at 4:30
const SHARES              = 50;
const STARTING_BALANCE    = 1000;

const LADDER = [
  { btcMin: 0.70, ethMax: 0.40 },
  { btcMin: 0.80, ethMax: 0.50 },
  { btcMin: 0.90, ethMax: 0.60 },
  { btcMin: 0.97, ethMax: 0.67 },
];

let state = { balance: STARTING_BALANCE, openTrades: [], closedTrades: [], totalPnl: 0 };
const priceBook   = {};
// marketCache keyed by actual slug timestamp (ts), not by currentWindowStart
// So we can look up: find entry where ts === currentWindowStart()
const marketCache = {};

// windowState keyed by slug timestamp
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

// Find the cached market for the current window
// Tries exact match first, then offset ±1 ±2
function getCurrentMarket() {
  const cws = currentWindowStart();
  // exact match
  if (marketCache[cws]) return marketCache[cws];
  // offset matches (clock skew)
  for (const offset of [1, -1, 2, -2]) {
    const ts = cws + offset * WINDOW_SIZE;
    if (marketCache[ts]) return marketCache[ts];
  }
  return null;
}

function getWS(ts) {
  if (!windowState[ts]) {
    windowState[ts] = { side: null, levelsHit: new Set(), shares: 0, buys: [], flips: 0, stopped: false };
  }
  return windowState[ts];
}

// ── Price helpers ─────────────────────────────────────────────────────────────
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
    log(`  💲 Seeded BTC↑ bid=${bestBid} ask=${bestAsk}`);
  } else if (Array.isArray(prices) && Array.isArray(outcomes)) {
    const upIdx = outcomes.findIndex(o => /up/i.test(String(o)));
    const dnIdx = outcomes.findIndex(o => /down/i.test(String(o)));
    if (upIdx >= 0 && dnIdx >= 0) {
      const up = parseFloat(prices[upIdx]) || 0;
      const dn = parseFloat(prices[dnIdx]) || 0;
      if (up > 0) priceBook[tokens.upToken] = { bid: Math.max(0, up - 0.01), ask: Math.min(1, up + 0.01) };
      if (dn > 0) priceBook[tokens.dnToken] = { bid: Math.max(0, dn - 0.01), ask: Math.min(1, dn + 0.01) };
      log(`  💲 Seeded from outcomePrices Up=${up} Dn=${dn}`);
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

let discovering = false;
async function refreshMarkets() {
  if (discovering) return;
  // Already have a market for current window?
  if (getCurrentMarket()) return;

  discovering = true;
  const cws = currentWindowStart();
  log(`🔍 Finding 5m market for window ${cws}…`);

  try {
    for (const offset of [0, 1, -1, 2, -2, 3]) {
      const ts      = cws + offset * WINDOW_SIZE;
      const btcSlug = `btc-updown-5m-${ts}`;
      const ethSlug = `eth-updown-5m-${ts}`;

      const [btcTokens, ethTokens] = await Promise.all([
        findTokensForSlug(btcSlug),
        findTokensForSlug(ethSlug),
      ]);

      if (!btcTokens || !ethTokens) continue;

      // Store with ts as key so getCurrentMarket() can find it
      marketCache[ts] = {
        windowStart: ts,
        btcUp: btcTokens.upToken, btcDn: btcTokens.dnToken,
        ethUp: ethTokens.upToken, ethDn: ethTokens.dnToken,
        btcSlug, ethSlug,
      };

      log(`✅ 5m found ts=${ts} | ${btcSlug}`);
      log(`   BTC↑ ${btcTokens.upToken.slice(0,12)}… ETH↑ ${ethTokens.upToken.slice(0,12)}…`);

      for (const tid of [btcTokens.upToken, btcTokens.dnToken, ethTokens.upToken, ethTokens.dnToken])
        subscribeToken(tid);

      break;
    }
  } finally {
    discovering = false;
  }

  if (!getCurrentMarket()) log(`⚠️  5m: no market found for window ${cws} — will retry`);
}

// ── REST price polling ────────────────────────────────────────────────────────

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
  // last_trade_price event
  if (msg.event_type === "last_trade_price" && msg.price) {
    const tid = msg.asset_id ?? msg.token_id;
    if (tid) {
      const p = parseFloat(msg.price) || 0;
      if (p > 0) { priceBook[tid] = { bid: p - 0.001, ask: p + 0.001 }; }
    }
    return;
  }
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
  const wst     = getWS(w.windowStart);

  if (elapsed >= WINDOW_TRADE_CUTOFF) {
    if (!wst.stopped) {
      wst.stopped = true;
      log(`⏱️  Cutoff reached — holding for resolution`);
    }
    return;
  }

  const btcUp = getPrice(w.btcUp);
  const btcDn = getPrice(w.btcDn);
  const ethUp = getPrice(w.ethUp);
  const ethDn = getPrice(w.ethDn);

  if (!btcUp || !ethUp) return;

  // ── Flip check ──────────────────────────────────────────────────────────────
  if (wst.side === 'UP' && wst.shares > 0 && btcDn >= 0.70) {
    await flip(w, wst, 'DOWN', ethUp, ethDn);
    return;
  }
  if (wst.side === 'DOWN' && wst.shares > 0 && btcUp >= 0.70) {
    await flip(w, wst, 'UP', ethUp, ethDn);
    return;
  }

  // ── Ladder buys ─────────────────────────────────────────────────────────────
  if (btcUp >= 0.70 && (wst.side === 'UP' || wst.side === null)) {
    for (let i = 0; i < LADDER.length; i++) {
      if (wst.levelsHit.has(i)) continue;
      const { btcMin, ethMax } = LADDER[i];
      if (btcUp >= btcMin && ethUp < ethMax) {
        buyEth(w, wst, 'UP', i, ethUp);
        break;
      }
    }
  } else if (btcDn >= 0.70 && (wst.side === 'DOWN' || wst.side === null)) {
    for (let i = 0; i < LADDER.length; i++) {
      if (wst.levelsHit.has(i)) continue;
      const { btcMin, ethMax } = LADDER[i];
      if (btcDn >= btcMin && ethDn < ethMax) {
        buyEth(w, wst, 'DOWN', i, ethDn);
        break;
      }
    }
  }
}

function buyEth(w, wst, side, levelIdx, ethPrice) {
  const cost = ethPrice * SHARES;
  if (state.balance < cost) { log(`💸 Low balance $${state.balance.toFixed(2)}`); return; }
  wst.side = side;
  wst.levelsHit.add(levelIdx);
  wst.shares += SHARES;
  wst.buys.push({ shares: SHARES, price: ethPrice, cost });
  state.balance -= cost;
  const id = tradeId();
  const t = {
    id, windowStart: w.windowStart, side,
    token: side === 'UP' ? w.ethUp : w.ethDn,
    shares: SHARES, entryPrice: ethPrice, totalCost: cost,
    openedAt: new Date().toISOString(), level: levelIdx + 1, floatingPnl: 0,
  };
  state.openTrades.push(t);
  saveState();
  log(`🟢 BUY ETH${side} L${levelIdx+1} [${id}] price=${ethPrice.toFixed(3)} shares=${SHARES} totalShares=${wst.shares} bal=$${state.balance.toFixed(2)}`);
  emitFn('trade_entered', t);
}

async function flip(w, wst, newSide, ethUp, ethDn) {
  log(`🔄 FLIP → ETH${newSide} (flip #${wst.flips + 1})`);
  const oldSide  = wst.side;
  const oldToken = oldSide === 'UP' ? w.ethUp : w.ethDn;
  const sellPrice = getPrice(oldToken);

  const toClose = state.openTrades.filter(t => t.windowStart === w.windowStart && t.side === oldSide);
  for (const t of toClose) {
    const proceeds = sellPrice * t.shares;
    const pnl = proceeds - t.totalCost;
    state.balance += proceeds;
    state.totalPnl += pnl;
    state.closedTrades.push({
      ...t, exitPrice: sellPrice, exitProceeds: +proceeds.toFixed(2),
      realizedPnl: +pnl.toFixed(4), closedAt: new Date().toISOString(), exitReason: 'FLIP',
    });
    log(`  📤 Sold ETH${oldSide} [${t.id}] price=${sellPrice.toFixed(3)} pnl=$${pnl.toFixed(2)}`);
  }
  state.openTrades = state.openTrades.filter(t => !(t.windowStart === w.windowStart && t.side === oldSide));

  wst.side = null;
  wst.shares = 0;
  wst.buys = [];
  wst.levelsHit = new Set();
  wst.flips++;
  saveState();

  // Immediately buy first valid ladder level on new side
  const btcPrice = newSide === 'UP' ? getPrice(w.btcUp) : getPrice(w.btcDn);
  const ethPrice = newSide === 'UP' ? ethUp : ethDn;
  if (btcPrice >= 0.70) {
    for (let i = 0; i < LADDER.length; i++) {
      const { btcMin, ethMax } = LADDER[i];
      if (btcPrice >= btcMin && ethPrice < ethMax) {
        buyEth(w, wst, newSide, i, ethPrice);
        break;
      }
    }
  }
}

// ── Resolution ────────────────────────────────────────────────────────────────
async function checkResolution() {
  const nowSec = Math.floor(Date.now() / 1000);
  for (const [tsKey, w] of Object.entries(marketCache)) {
    if (nowSec < w.windowStart + WINDOW_SIZE + 30) continue;
    const tradesInWindow = state.openTrades.filter(t => t.windowStart === w.windowStart);
    if (!tradesInWindow.length) { delete marketCache[tsKey]; continue; }

    log(`⏰ Resolving ws=${w.windowStart}…`);
    const upPrice = getPrice(w.ethUp);
    const dnPrice = getPrice(w.ethDn);

    for (const t of tradesInWindow) {
      const rp = t.side === 'UP'
        ? (upPrice || getPrice(w.ethUp))
        : (dnPrice || getPrice(w.ethDn));
      const proceeds = rp * t.shares;
      const pnl = proceeds - t.totalCost;
      state.balance += proceeds;
      state.totalPnl += pnl;
      state.closedTrades.push({
        ...t, exitPrice: rp, exitProceeds: +proceeds.toFixed(2),
        realizedPnl: +pnl.toFixed(4), closedAt: new Date().toISOString(), exitReason: 'RESOLVED',
      });
      log(`${pnl >= 0 ? '🟢' : '🔴'} RESOLVED ETH${t.side} [${t.id}] price=${rp.toFixed(3)} pnl=$${pnl.toFixed(2)} bal=$${state.balance.toFixed(2)}`);
    }
    state.openTrades = state.openTrades.filter(t => t.windowStart !== w.windowStart);
    delete windowState[w.windowStart];
    delete marketCache[tsKey];
    saveState();
  }
}

// ── Floating PnL ──────────────────────────────────────────────────────────────
function updateFloating() {
  const w = getCurrentMarket();
  if (!w) return;
  for (const t of state.openTrades) {
    const price = t.side === 'UP' ? getPrice(w.ethUp) : getPrice(w.ethDn);
    if (price > 0) t.floatingPnl = +(price * t.shares - t.totalCost).toFixed(4);
  }
}

// ── Dashboard snapshot ────────────────────────────────────────────────────────
function buildDashboardSnapshot() {
  const w   = getCurrentMarket();
  const nowSec = Math.floor(Date.now() / 1000);
  const wst = w ? getWS(w.windowStart) : null;
  const elapsed   = w ? nowSec - w.windowStart : 0;
  const remaining = w ? Math.max(0, WINDOW_SIZE - elapsed) : 0;

  return {
    balance:      +state.balance.toFixed(2),
    totalPnl:     +state.totalPnl.toFixed(2),
    openTrades:   state.openTrades,
    closedTrades: state.closedTrades.slice(-30),
    updatedAt:    new Date().toISOString(),
    window: w ? {
      windowStart:  w.windowStart,
      elapsed, remaining,
      stopped:      wst?.stopped ?? false,
      side:         wst?.side ?? null,
      totalShares:  wst?.shares ?? 0,
      flips:        wst?.flips ?? 0,
      levelsHit:    wst ? [...wst.levelsHit] : [],
      btcUpPrice:  +getPrice(w.btcUp).toFixed(3),
      btcDnPrice:  +getPrice(w.btcDn).toFixed(3),
      ethUpPrice:  +getPrice(w.ethUp).toFixed(3),
      ethDnPrice:  +getPrice(w.ethDn).toFixed(3),
    } : null,
  };
}

function prune() {
  const cws = currentWindowStart();
  for (const key of Object.keys(marketCache))
    if (Number(key) < cws - WINDOW_SIZE * 2) delete marketCache[key];
}

// ── Main loop ─────────────────────────────────────────────────────────────────
let timer = null, pollTimer = null;

async function tick() {
  try {
    prune();
    await refreshMarkets();
    const w = getCurrentMarket();
    if (w) {
      updateFloating();
      await checkWindow(w);
      await checkResolution();
    }
    emitFn("snapshot", buildDashboardSnapshot());
  } catch (e) { log(`⚠️  tick: ${e.message}`); }
}

async function start(emit, logEmit) {
  emitFn = emit; logFn = logEmit;
  log('🚀 Polymarket ETH Ladder Bot (5m)');
  loadState(); connectWS(); await tick();
  timer     = setInterval(tick, 5000);
  // Emit prices every second using latest priceBook (WS updates instantly)
  setInterval(function() {
    const w = getCurrentMarket();
    if (!w) return;
    emitFn("prices", {
      btcUpPrice: +getPrice(w.btcUp).toFixed(3),
      btcDnPrice: +getPrice(w.btcDn).toFixed(3),
      ethUpPrice: +getPrice(w.ethUp).toFixed(3),
      ethDnPrice: +getPrice(w.ethDn).toFixed(3),
    });
  }, 1000);

  log(`💰 Balance: $${state.balance.toFixed(2)}`);
}
function stop() { clearInterval(timer); ws?.terminate(); }
module.exports = { start, stop, buildDashboardSnapshot };
