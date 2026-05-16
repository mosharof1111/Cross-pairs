'use strict';

const fetch = require('node-fetch');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');

const GAMMA      = 'https://gamma-api.polymarket.com';
const CLOB_REST  = 'https://clob.polymarket.com';
const CLOB_WS    = 'wss://ws-subscriptions-clob.polymarket.com/ws/market';
const TRADES_FILE = path.join(__dirname, 'trades.json');

const WINDOW_SIZE      = 300;
const ENTRY_AMOUNT     = 10;    // $10 per entry
const ENTRY_STEP       = 0.02;  // place limit every 0.02 below current
const TP_MULTIPLIER    = 2.0;   // TP = entry price x 2
const STARTING_BALANCE = 1000;

let state = { balance: STARTING_BALANCE, openTrades: [], closedTrades: [], totalPnl: 0 };
const priceBook   = {};
const marketCache = {};
const windowState = {};
let emitFn = () => {};
let logFn  = () => {};

function loadState() {
  try {
    if (fs.existsSync(TRADES_FILE)) {
      const raw = JSON.parse(fs.readFileSync(TRADES_FILE, 'utf8'));
      state = { ...state, ...raw };
      if (state.openTrades.length > 0) {
        log(`♻️  Refunding ${state.openTrades.length} open trade(s)`);
        for (const t of state.openTrades) state.balance += t.cost;
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

async function findMarketForTs(ts) {
  for (const offset of [0, 1, -1, 2, -2]) {
    const t       = ts + offset * WINDOW_SIZE;
    const btcSlug = `btc-updown-5m-${t}`;
    const event   = await getJson(`${GAMMA}/events/slug/${btcSlug}`);
    if (event?.markets?.length) {
      const mkt    = event.markets.find(m => m.acceptingOrders !== false) ?? event.markets[0];
      const tokens = extractTokenIds(mkt);
      if (tokens) { seedFromMarket(mkt, tokens); return { ts: t, tokens, slug: btcSlug }; }
    }
    const mkt2 = await getJson(`${GAMMA}/markets/slug/${btcSlug}`);
    if (mkt2) {
      const tokens = extractTokenIds(mkt2);
      if (tokens) { seedFromMarket(mkt2, tokens); return { ts: t, tokens, slug: btcSlug }; }
    }
  }
  return null;
}

let discovering = false;
async function refreshMarkets() {
  if (discovering) return;
  discovering = true;
  const cws = currentWindowStart();
  try {
    if (!marketCache[cws]) {
      log(`🔍 Finding current window ${cws}…`);
      const res = await findMarketForTs(cws);
      if (res) {
        marketCache[res.ts] = { windowStart: res.ts, btcUp: res.tokens.upToken, btcDn: res.tokens.dnToken, slug: res.slug };
        log(`✅ Found ts=${res.ts} | ${res.slug}`);
        // Initialise window state when market first found
        if (!windowState[res.ts]) initWindowState(res.ts);
      }
    }
  } finally { discovering = false; }
}

// ── Window state ──────────────────────────────────────────────────────────────
// For each window track:
// upLevels: Map of levelPrice -> { filled, tp, shares, cost, tpHit, id }
// dnLevels: Map of levelPrice -> { filled, tp, shares, cost, tpHit, id }
// upBasePrice: price when window was discovered (to build levels from)
// dnBasePrice: same for down side

function initWindowState(ts) {
  windowState[ts] = {
    upLevels: new Map(),  // key = entry price string
    dnLevels: new Map(),
    upBasePrice: 0,
    dnBasePrice: 0,
    resolved: false,
  };
}

// ── REST price polling ────────────────────────────────────────────────────────
async function pollPricesForWindow(w) {
  if (!w) return;
  await Promise.all([w.btcUp, w.btcDn].map(async tid => {
    try {
      const [ar, br] = await Promise.all([
        fetch(`${CLOB_REST}/price?token_id=${tid}&side=BUY`,  { timeout: 3000 }),
        fetch(`${CLOB_REST}/price?token_id=${tid}&side=SELL`, { timeout: 3000 }),
      ]);
      const ask = parseFloat((await ar.json()).price ?? 0) || 0;
      const bid = parseFloat((await br.json()).price ?? 0) || 0;
      if (ask > 0 || bid > 0) priceBook[tid] = { bid, ask };
    } catch (_) {}
  }));
}

async function pollPrices() {
  const cws = currentWindowStart();
  const w = marketCache[cws];
  if (w) await pollPricesForWindow(w);
}

// ── WebSocket ─────────────────────────────────────────────────────────────────
let ws = null, wsReady = false;
const pendingSubs = new Set();
function connectWS() {
  ws = new WebSocket(CLOB_WS);
  ws.on('open', () => {
    wsReady = true; log('✅ WebSocket connected');
    for (const t of pendingSubs) _sub(t); pendingSubs.clear();
  });
  ws.on('close', () => { wsReady = false; log('⚡ WS closed — retry 5s'); setTimeout(connectWS, 5000); });
  ws.on('error', e => log(`⚠️  WS: ${e.message}`));
}
function _sub(tid) { ws.send(JSON.stringify({ assets_ids: [tid], type: 'market' })); }

function tradeId() { return `T${Date.now().toString(36).toUpperCase()}`; }

// Round price to nearest 0.02
function roundToStep(price) {
  return Math.round(price / ENTRY_STEP) * ENTRY_STEP;
}

// ── Main trading logic ────────────────────────────────────────────────────────
function checkWindow(w) {
  const wst = windowState[w.windowStart];
  if (!wst || wst.resolved) return;

  const upPrice = getPrice(w.btcUp);
  const dnPrice = getPrice(w.btcDn);
  if (!upPrice || !dnPrice) return;

  // Set base price on first tick
  if (!wst.upBasePrice && upPrice > 0) {
    wst.upBasePrice = upPrice;
    log(`📌 UP base price set: ${upPrice.toFixed(3)}`);
  }
  if (!wst.dnBasePrice && dnPrice > 0) {
    wst.dnBasePrice = dnPrice;
    log(`📌 DN base price set: ${dnPrice.toFixed(3)}`);
  }

  // Check UP side
  if (wst.upBasePrice > 0) checkSide(w, wst, 'UP', upPrice);
  // Check DN side
  if (wst.dnBasePrice > 0) checkSide(w, wst, 'DOWN', dnPrice);
}

function checkSide(w, wst, side, currentPrice) {
  const levels    = side === 'UP' ? wst.upLevels : wst.dnLevels;
  const basePrice = side === 'UP' ? wst.upBasePrice : wst.dnBasePrice;
  const token     = side === 'UP' ? w.btcUp : w.btcDn;

  // Generate all levels from base down to current price (every 0.02)
  // Level price = basePrice - 0.02, basePrice - 0.04, etc.
  let levelPrice = +(roundToStep(basePrice) - ENTRY_STEP).toFixed(3);
  while (levelPrice >= Math.max(0.02, currentPrice - ENTRY_STEP)) {
    const key = levelPrice.toFixed(3);
    if (!levels.has(key)) {
      // New level — add it as pending
      levels.set(key, { entryPrice: levelPrice, tp: +(levelPrice * TP_MULTIPLIER).toFixed(3), filled: false, tpHit: false, shares: 0, cost: 0, proceeds: 0, id: null });
    }
    levelPrice = +(levelPrice - ENTRY_STEP).toFixed(3);
    if (levelPrice < 0.02) break;
  }

  // Check fills — level fills when price drops TO or BELOW that level
  for (const [key, lvl] of levels) {
    if (lvl.filled) continue;
    if (currentPrice <= lvl.entryPrice) {
      // Fill this level
      const shares = ENTRY_AMOUNT / lvl.entryPrice;
      const cost   = ENTRY_AMOUNT;
      if (state.balance < cost) { log(`💸 Low balance`); continue; }
      lvl.filled = true;
      lvl.shares = +shares.toFixed(4);
      lvl.cost   = cost;
      lvl.id     = tradeId();
      state.balance -= cost;
      const trade = {
        id: lvl.id, windowStart: w.windowStart, side,
        entryPrice: lvl.entryPrice, tp: lvl.tp,
        shares: lvl.shares, cost,
        openedAt: new Date().toISOString(), floatingPnl: 0,
      };
      state.openTrades.push(trade);
      saveState();
      log(`🟢 FILL ${side} [${lvl.id}] entry=${lvl.entryPrice.toFixed(3)} tp=${lvl.tp.toFixed(3)} shares=${lvl.shares.toFixed(2)} cost=$${cost} bal=$${state.balance.toFixed(2)}`);
      emitFn('trade_entered', trade);
    }
  }

  // Check TPs — TP hits when price rises to or above tp price
  for (const [key, lvl] of levels) {
    if (!lvl.filled || lvl.tpHit) continue;
    if (currentPrice >= lvl.tp) {
      lvl.tpHit    = true;
      lvl.proceeds = +(lvl.tp * lvl.shares).toFixed(2);
      const pnl    = lvl.proceeds - lvl.cost;
      state.balance   += lvl.proceeds;
      state.totalPnl  += pnl;
      state.openTrades = state.openTrades.filter(t => t.id !== lvl.id);
      state.closedTrades.push({
        id: lvl.id, windowStart: w.windowStart, side,
        entryPrice: lvl.entryPrice, tp: lvl.tp,
        exitPrice: lvl.tp, shares: lvl.shares, cost: lvl.cost,
        proceeds: lvl.proceeds, realizedPnl: +pnl.toFixed(4),
        closedAt: new Date().toISOString(), exitReason: 'TP',
      });
      saveState();
      log(`🎯 TP HIT ${side} [${lvl.id}] entry=${lvl.entryPrice.toFixed(3)} tp=${lvl.tp.toFixed(3)} proceeds=$${lvl.proceeds} pnl=$${pnl.toFixed(2)} bal=$${state.balance.toFixed(2)}`);
      emitFn('trade_closed', lvl);
    }
  }

  // Update floating pnl on open trades
  for (const t of state.openTrades.filter(t => t.windowStart === w.windowStart && t.side === side)) {
    t.floatingPnl = +((currentPrice - t.entryPrice) * t.shares).toFixed(4);
  }
}

// ── Resolution ────────────────────────────────────────────────────────────────
async function checkResolution() {
  const nowSec = Math.floor(Date.now() / 1000);
  for (const [tsStr, wst] of Object.entries(windowState)) {
    const ts = Number(tsStr);
    if (wst.resolved) continue;
    if (nowSec < ts + WINDOW_SIZE + 30) continue;
    const w = marketCache[ts];
    if (!w) continue;

    log(`⏰ Resolving window ts=${ts}…`);
    await pollPricesForWindow(w);
    const upPrice = getPrice(w.btcUp);
    const dnPrice = getPrice(w.btcDn);
    log(`   Resolved: BTC↑=${upPrice.toFixed(3)} BTC↓=${dnPrice.toFixed(3)}`);

    const tradesInWindow = state.openTrades.filter(t => t.windowStart === ts);
    let windowPnl = 0;

    for (const t of tradesInWindow) {
      const resolvedPrice = t.side === 'UP' ? upPrice : dnPrice;
      const proceeds = resolvedPrice * t.shares;
      const pnl = proceeds - t.cost;
      windowPnl += pnl;
      state.balance  += proceeds;
      state.totalPnl += pnl;
      state.closedTrades.push({
        ...t, exitPrice: resolvedPrice, proceeds: +proceeds.toFixed(2),
        realizedPnl: +pnl.toFixed(4), closedAt: new Date().toISOString(), exitReason: 'RESOLVED',
      });
      log(`${pnl >= 0 ? '🟢' : '🔴'} RESOLVED ${t.side} [${t.id}] entry=${t.entryPrice.toFixed(3)} resolved=${resolvedPrice.toFixed(3)} pnl=$${pnl.toFixed(2)}`);
    }

    state.openTrades = state.openTrades.filter(t => t.windowStart !== ts);
    wst.resolved = true;
    saveState();

    const upFilled = [...wst.upLevels.values()].filter(l => l.filled).length;
    const dnFilled = [...wst.dnLevels.values()].filter(l => l.filled).length;
    const upTP     = [...wst.upLevels.values()].filter(l => l.tpHit).length;
    const dnTP     = [...wst.dnLevels.values()].filter(l => l.tpHit).length;
    log(`📊 SUMMARY ts=${ts} UP filled=${upFilled} tp=${upTP} | DN filled=${dnFilled} tp=${dnTP} | windowPnl=$${windowPnl.toFixed(2)} bal=$${state.balance.toFixed(2)}`);
    delete marketCache[ts];
  }
}

// ── Dashboard snapshot ────────────────────────────────────────────────────────
function buildDashboardSnapshot() {
  const cws    = currentWindowStart();
  const nowSec = Math.floor(Date.now() / 1000);
  const w      = marketCache[cws];
  const wst    = windowState[cws];
  const upPrice = w ? getPrice(w.btcUp) : 0;
  const dnPrice = w ? getPrice(w.btcDn) : 0;

  // Build level arrays for dashboard
  const upLevels = wst ? [...wst.upLevels.entries()].map(([k, v]) => ({
    entryPrice: v.entryPrice, tp: v.tp, filled: v.filled, tpHit: v.tpHit,
    shares: v.shares, cost: v.cost, proceeds: v.proceeds, id: v.id,
  })).sort((a, b) => b.entryPrice - a.entryPrice) : [];

  const dnLevels = wst ? [...wst.dnLevels.entries()].map(([k, v]) => ({
    entryPrice: v.entryPrice, tp: v.tp, filled: v.filled, tpHit: v.tpHit,
    shares: v.shares, cost: v.cost, proceeds: v.proceeds, id: v.id,
  })).sort((a, b) => b.entryPrice - a.entryPrice) : [];

  return {
    balance:      +state.balance.toFixed(2),
    totalPnl:     +state.totalPnl.toFixed(2),
    openTrades:   state.openTrades,
    closedTrades: state.closedTrades.slice(-50),
    updatedAt:    new Date().toISOString(),
    window: w ? {
      windowStart: cws,
      elapsed:   nowSec - cws,
      remaining: Math.max(0, WINDOW_SIZE - (nowSec - cws)),
      upPrice:   +upPrice.toFixed(3),
      dnPrice:   +dnPrice.toFixed(3),
      upBasePrice: wst?.upBasePrice ?? 0,
      dnBasePrice: wst?.dnBasePrice ?? 0,
      upLevels,
      dnLevels,
    } : null,
  };
}

function prune() {
  const cws = currentWindowStart();
  for (const key of Object.keys(marketCache))
    if (Number(key) < cws - WINDOW_SIZE * 2) delete marketCache[key];
}

let timer = null;
async function tick() {
  try {
    prune();
    await refreshMarkets();
    await pollPrices();
    const w = marketCache[currentWindowStart()];
    if (w) checkWindow(w);
    await checkResolution();
    emitFn('snapshot', buildDashboardSnapshot());
  } catch (e) { log(`⚠️  tick: ${e.message}`); }
}

async function start(emit, logEmit) {
  emitFn = emit; logFn = logEmit;
  log('🚀 BTC 5m Limit Ladder — $10/entry step=0.02 tp=2x');
  loadState(); connectWS(); await tick();
  timer = setInterval(tick, 5000);
  setInterval(async function() {
    await pollPrices();
    const w = marketCache[currentWindowStart()];
    if (!w) return;
    emitFn('prices', {
      upPrice: +getPrice(w.btcUp).toFixed(3),
      dnPrice: +getPrice(w.btcDn).toFixed(3),
    });
  }, 2000);
  log(`💰 Balance: $${state.balance.toFixed(2)}`);
}
function stop() { clearInterval(timer); ws?.terminate(); }
module.exports = { start, stop, buildDashboardSnapshot };
