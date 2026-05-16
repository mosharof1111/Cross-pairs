'use strict';

const fetch = require('node-fetch');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');

const GAMMA      = 'https://gamma-api.polymarket.com';
const CLOB_REST  = 'https://clob.polymarket.com';
const CLOB_WS    = 'wss://ws-subscriptions-clob.polymarket.com/ws/market';
const TRADES_FILE = path.join(__dirname, 'trades.json');

const WINDOW_SIZE         = 300;
const WINDOW_TRADE_CUTOFF = 270;
const SHARES              = 50;
const STARTING_BALANCE    = 1000;
const ENTRY_GAP           = 0.10;
const TP_GAP              = 0.02;
const FLIP_LEVEL          = 0.70;

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

function getCurrentMarket() {
  const cws = currentWindowStart();
  if (marketCache[cws]) return marketCache[cws];
  for (const offset of [-1, -2]) {
    const ts = cws + offset * WINDOW_SIZE;
    if (marketCache[ts]) return marketCache[ts];
  }
  return null;
}

function getWS(ts) {
  if (!windowState[ts]) {
    windowState[ts] = { side: null, hasOpenPosition: false, flips: 0, entries: 0, tpHits: 0, stopped: false };
  }
  return windowState[ts];
}

function getPrice(tid) {
  const b = priceBook[tid];
  if (!b) return 0;
  if (b.bid > 0 && b.ask > 0) return (b.bid + b.ask) / 2;
  return b.bid || b.ask || 0;
}

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
  discovering = true;
  const cws    = currentWindowStart();
  const nextWs = cws + WINDOW_SIZE;
  const nowSec = Math.floor(Date.now() / 1000);
  const elapsed = nowSec - cws;
  const needCurrent = !getCurrentMarket();
  const needNext    = elapsed >= 240 && !marketCache[nextWs];
  const toFetch = [];
  if (needCurrent) toFetch.push({ ts: cws, label: 'current' });
  if (needNext)    toFetch.push({ ts: nextWs, label: 'next' });
  if (!toFetch.length) { discovering = false; return; }
  try {
    for (const { ts, label } of toFetch) {
      if (marketCache[ts]) continue;
      log(`🔍 Finding 5m ${label} window ${ts}…`);
      for (const offset of [0, 1, -1, 2, -2]) {
        const t        = ts + offset * WINDOW_SIZE;
        const btcSlug  = `btc-updown-5m-${t}`;
        const ethSlug  = `eth-updown-5m-${t}`;
        const [btcTokens, ethTokens] = await Promise.all([
          findTokensForSlug(btcSlug),
          findTokensForSlug(ethSlug),
        ]);
        if (!btcTokens || !ethTokens) continue;
        marketCache[t] = {
          windowStart: t,
          btcUp: btcTokens.upToken, btcDn: btcTokens.dnToken,
          ethUp: ethTokens.upToken, ethDn: ethTokens.dnToken,
          btcSlug, ethSlug,
        };
        log(`✅ 5m ${label} found ts=${t} | ${btcSlug}`);
        break;
      }
      if (!marketCache[ts]) log(`⚠️  5m ${label} not found — will retry`);
    }
  } finally { discovering = false; }
}

// ── REST price polling ────────────────────────────────────────────────────────
async function pollPrices() {
  const w = getCurrentMarket();
  if (!w) return;
  const tids = [w.btcUp, w.btcDn, w.ethUp, w.ethDn];
  await Promise.all(tids.map(async tid => {
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

// ── WebSocket ─────────────────────────────────────────────────────────────────
let ws = null, wsReady = false;
const pendingSubs = new Set();
function connectWS() {
  ws = new WebSocket(CLOB_WS);
  ws.on('open', () => { wsReady = true; log('✅ WebSocket connected'); for (const t of pendingSubs) _sub(t); pendingSubs.clear(); });
  ws.on('close', () => { wsReady = false; log('⚡ WS closed — retry 5s'); setTimeout(connectWS, 5000); });
  ws.on('error', e => log(`⚠️  WS: ${e.message}`));
}
function _sub(tid) { ws.send(JSON.stringify({ assets_ids: [tid], type: 'market' })); }
function subscribeToken(tid) {
  if (!tid) return;
  if (!wsReady || ws?.readyState !== WebSocket.OPEN) { pendingSubs.add(tid); return; }
  _sub(tid);
}

// ── Trade helpers ─────────────────────────────────────────────────────────────
function tradeId() { return `T${Date.now().toString(36).toUpperCase()}`; }

// ── Main trading logic ────────────────────────────────────────────────────────
async function checkWindow(w) {
  const nowSec  = Math.floor(Date.now() / 1000);
  const elapsed = nowSec - w.windowStart;
  const wst     = getWS(w.windowStart);

  if (elapsed >= WINDOW_TRADE_CUTOFF) {
    if (!wst.stopped) { wst.stopped = true; log(`⏱️  Cutoff — holding for resolution`); }
    return;
  }

  const btcUp = getPrice(w.btcUp);
  const btcDn = getPrice(w.btcDn);
  const ethUp = getPrice(w.ethUp);
  const ethDn = getPrice(w.ethDn);
  if (!btcUp || !ethUp) return;

  log(`📊 BTC↑=${btcUp.toFixed(3)} BTC↓=${btcDn.toFixed(3)} ETH↑=${ethUp.toFixed(3)} ETH↓=${ethDn.toFixed(3)} side=${wst.side || 'none'} open=${wst.hasOpenPosition}`);

  // ── TP check — close position when gap equalizes ──────────────────────────
  if (wst.hasOpenPosition) {
    const openTrade = state.openTrades.find(t => t.windowStart === w.windowStart);
    if (openTrade) {
      const ethPrice = openTrade.side === 'UP' ? ethUp : ethDn;
      const btcPrice = openTrade.side === 'UP' ? btcUp : btcDn;
      const currentGap = btcPrice - ethPrice;

      // Update floating pnl
      openTrade.floatingPnl = +((ethPrice - openTrade.entryPrice) * openTrade.shares).toFixed(4);

      if (currentGap <= TP_GAP) {
        // Take profit — prices equalized
        closeTrade(openTrade, ethPrice, 'TP', w, wst);
        return;
      }
    }
  }

  // ── Flip check ────────────────────────────────────────────────────────────
  if (wst.hasOpenPosition) {
    const openTrade = state.openTrades.find(t => t.windowStart === w.windowStart);
    if (openTrade) {
      if (openTrade.side === 'UP' && btcDn >= FLIP_LEVEL) {
        const ethPrice = getPrice(w.ethUp);
        closeTrade(openTrade, ethPrice, 'FLIP', w, wst);
        wst.flips++;
        wst.side = 'DOWN';
        enterTrade(w, wst, 'DOWN', ethDn);
        return;
      }
      if (openTrade.side === 'DOWN' && btcUp >= FLIP_LEVEL) {
        const ethPrice = getPrice(w.ethDn);
        closeTrade(openTrade, ethPrice, 'FLIP', w, wst);
        wst.flips++;
        wst.side = 'UP';
        enterTrade(w, wst, 'UP', ethUp);
        return;
      }
    }
  }

  // ── Entry check — only if no open position ────────────────────────────────
  if (!wst.hasOpenPosition) {
    if (btcUp >= FLIP_LEVEL && (ethUp < btcUp - ENTRY_GAP)) {
      wst.side = 'UP';
      enterTrade(w, wst, 'UP', ethUp);
    } else if (btcDn >= FLIP_LEVEL && (ethDn < btcDn - ENTRY_GAP)) {
      wst.side = 'DOWN';
      enterTrade(w, wst, 'DOWN', ethDn);
    }
  }
}

function enterTrade(w, wst, side, ethPrice) {
  const cost = ethPrice * SHARES;
  if (state.balance < cost) { log(`💸 Low balance $${state.balance.toFixed(2)}`); return; }
  const id = tradeId();
  const t = {
    id, windowStart: w.windowStart, side,
    token: side === 'UP' ? w.ethUp : w.ethDn,
    shares: SHARES, entryPrice: ethPrice, totalCost: cost,
    openedAt: new Date().toISOString(), floatingPnl: 0,
  };
  state.balance -= cost;
  state.openTrades.push(t);
  wst.hasOpenPosition = true;
  wst.entries++;
  saveState();
  log(`🟢 ENTRY ETH${side} [${id}] price=${ethPrice.toFixed(3)} cost=$${cost.toFixed(2)} bal=$${state.balance.toFixed(2)} entries=${wst.entries}`);
  emitFn('trade_entered', t);
}

function closeTrade(t, exitPrice, reason, w, wst) {
  const proceeds = exitPrice * t.shares;
  const pnl = proceeds - t.totalCost;
  state.balance += proceeds;
  state.totalPnl += pnl;
  state.openTrades = state.openTrades.filter(x => x.id !== t.id);
  state.closedTrades.push({
    ...t, exitPrice, exitProceeds: +proceeds.toFixed(2),
    realizedPnl: +pnl.toFixed(4), closedAt: new Date().toISOString(), exitReason: reason,
  });
  wst.hasOpenPosition = false;
  if (reason === 'TP') wst.tpHits++;
  saveState();
  log(`${pnl >= 0 ? '🟢' : '🔴'} ${reason} ETH${t.side} [${t.id}] entry=${t.entryPrice.toFixed(3)} exit=${exitPrice.toFixed(3)} pnl=$${pnl.toFixed(2)} bal=$${state.balance.toFixed(2)}`);
  emitFn('trade_closed', t);
}

// ── Resolution ────────────────────────────────────────────────────────────────
async function checkResolution() {
  const nowSec = Math.floor(Date.now() / 1000);
  for (const [tsKey, w] of Object.entries(marketCache)) {
    if (nowSec < w.windowStart + WINDOW_SIZE + 30) continue;
    const tradesInWindow = state.openTrades.filter(t => t.windowStart === w.windowStart);
    if (!tradesInWindow.length) { delete marketCache[tsKey]; delete windowState[w.windowStart]; continue; }
    log(`⏰ Resolving ws=${w.windowStart}…`);
    await pollPrices();
    const upPrice = getPrice(w.ethUp);
    const dnPrice = getPrice(w.ethDn);
    for (const t of tradesInWindow) {
      const rp = t.side === 'UP' ? upPrice : dnPrice;
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
    delete marketCache[tsKey];
    delete windowState[w.windowStart];
    saveState();
  }
}

// ── Dashboard snapshot ────────────────────────────────────────────────────────
function buildDashboardSnapshot() {
  const w      = getCurrentMarket();
  const nowSec = Math.floor(Date.now() / 1000);
  const wst    = w ? getWS(w.windowStart) : null;
  const elapsed   = w ? Math.max(0, nowSec - w.windowStart) : 0;
  const remaining = w ? Math.max(0, WINDOW_SIZE - elapsed) : 0;
  const openTrade = w ? state.openTrades.find(t => t.windowStart === w.windowStart) : null;
  const btcUp = w ? getPrice(w.btcUp) : 0;
  const btcDn = w ? getPrice(w.btcDn) : 0;
  const ethUp = w ? getPrice(w.ethUp) : 0;
  const ethDn = w ? getPrice(w.ethDn) : 0;
  return {
    balance:      +state.balance.toFixed(2),
    totalPnl:     +state.totalPnl.toFixed(2),
    openTrades:   state.openTrades,
    closedTrades: state.closedTrades.slice(-50),
    updatedAt:    new Date().toISOString(),
    window: w ? {
      windowStart: w.windowStart, elapsed, remaining,
      stopped:     wst?.stopped ?? false,
      side:        wst?.side ?? null,
      hasOpenPosition: wst?.hasOpenPosition ?? false,
      flips:       wst?.flips ?? 0,
      entries:     wst?.entries ?? 0,
      tpHits:      wst?.tpHits ?? 0,
      btcUpPrice:  +btcUp.toFixed(3),
      btcDnPrice:  +btcDn.toFixed(3),
      ethUpPrice:  +ethUp.toFixed(3),
      ethDnPrice:  +ethDn.toFixed(3),
      gapUp:       btcUp > 0 && ethUp > 0 ? +(btcUp - ethUp).toFixed(3) : null,
      gapDn:       btcDn > 0 && ethDn > 0 ? +(btcDn - ethDn).toFixed(3) : null,
      openTrade:   openTrade ? {
        id: openTrade.id, side: openTrade.side,
        entryPrice: openTrade.entryPrice,
        floatingPnl: openTrade.floatingPnl ?? 0,
        totalCost: openTrade.totalCost,
      } : null,
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
    const w = getCurrentMarket();
    if (w) {
      await checkWindow(w);
      await checkResolution();
    }
    emitFn('snapshot', buildDashboardSnapshot());
  } catch (e) { log(`⚠️  tick: ${e.message}`); }
}

async function start(emit, logEmit) {
  emitFn = emit; logFn = logEmit;
  log('🚀 Polymarket ETH Gap Bot (5m) — gap=0.10 tp=0.02');
  loadState(); connectWS(); await tick();
  timer = setInterval(tick, 5000);
  setInterval(async function() {
    await pollPrices();
    const w = getCurrentMarket();
    if (!w) return;
    emitFn('prices', {
      btcUpPrice: +getPrice(w.btcUp).toFixed(3),
      btcDnPrice: +getPrice(w.btcDn).toFixed(3),
      ethUpPrice: +getPrice(w.ethUp).toFixed(3),
      ethDnPrice: +getPrice(w.ethDn).toFixed(3),
      gapUp: +(getPrice(w.btcUp) - getPrice(w.ethUp)).toFixed(3),
      gapDn: +(getPrice(w.btcDn) - getPrice(w.ethDn)).toFixed(3),
    });
  }, 2000);
  log(`💰 Balance: $${state.balance.toFixed(2)}`);
}
function stop() { clearInterval(timer); ws?.terminate(); }
module.exports = { start, stop, buildDashboardSnapshot };
