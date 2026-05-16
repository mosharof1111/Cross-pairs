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
const BTC_MIN             = 0.15;
const FLIP_LEVEL          = 0.70;

const GAP_LEVELS = [0.10, 0.20, 0.30];

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
    windowState[ts] = {
      side: null, levelsHit: new Set(),
      flips: 0, entries: 0, tpHits: 0, stopped: false,
    };
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
        const t       = ts + offset * WINDOW_SIZE;
        const btcSlug = `btc-updown-5m-${t}`;
        const ethSlug = `eth-updown-5m-${t}`;
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

async function pollPrices() {
  const w = getCurrentMarket();
  if (!w) return;
  await Promise.all([w.btcUp, w.btcDn, w.ethUp, w.ethDn].map(async tid => {
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
function subscribeToken(tid) {
  if (!tid) return;
  if (!wsReady || ws?.readyState !== WebSocket.OPEN) { pendingSubs.add(tid); return; }
  _sub(tid);
}

function tradeId() { return `T${Date.now().toString(36).toUpperCase()}`; }

function openTradesForWindow(windowStart) {
  return state.openTrades.filter(t => t.windowStart === windowStart);
}

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

  const gapUp = btcUp - ethUp;
  const gapDn = btcDn - ethDn;

  log(`📊 BTC↑=${btcUp.toFixed(3)} BTC↓=${btcDn.toFixed(3)} ETH↑=${ethUp.toFixed(3)} ETH↓=${ethDn.toFixed(3)} gapUp=${gapUp.toFixed(3)} gapDn=${gapDn.toFixed(3)} side=${wst.side||'none'}`);

  const openTrades = openTradesForWindow(w.windowStart);

  if (openTrades.length > 0) {
    const ethPrice = wst.side === 'UP' ? ethUp : ethDn;
    for (const t of openTrades) {
      t.floatingPnl = +((ethPrice - t.entryPrice) * t.shares).toFixed(4);
    }
  }

  if (openTrades.length > 0) {
    if (wst.side === 'UP' && btcDn >= FLIP_LEVEL) {
      log(`🔄 FLIP UP→DOWN`);
      closeAllTrades(openTrades, ethUp, 'FLIP', w, wst);
      wst.levelsHit = new Set();
      wst.side = null;
      wst.flips++;
      if (gapDn >= GAP_LEVELS[0]) { wst.side = 'DOWN'; checkLevels(w, wst, 'DOWN', btcDn, ethDn, gapDn); }
      return;
    }
    if (wst.side === 'DOWN' && btcUp >= FLIP_LEVEL) {
      log(`🔄 FLIP DOWN→UP`);
      closeAllTrades(openTrades, ethDn, 'FLIP', w, wst);
      wst.levelsHit = new Set();
      wst.side = null;
      wst.flips++;
      if (gapUp >= GAP_LEVELS[0]) { wst.side = 'UP'; checkLevels(w, wst, 'UP', btcUp, ethUp, gapUp); }
      return;
    }
  }

  if (btcUp >= BTC_MIN && (wst.side === 'UP' || wst.side === null)) {
    if (gapUp >= GAP_LEVELS[0]) {
      wst.side = 'UP';
      checkLevels(w, wst, 'UP', btcUp, ethUp, gapUp);
    }
  } else if (btcDn >= BTC_MIN && (wst.side === 'DOWN' || wst.side === null)) {
    if (gapDn >= GAP_LEVELS[0]) {
      wst.side = 'DOWN';
      checkLevels(w, wst, 'DOWN', btcDn, ethDn, gapDn);
    }
  }
}

function checkLevels(w, wst, side, btcPrice, ethPrice, gap) {
  for (let i = 0; i < GAP_LEVELS.length; i++) {
    if (wst.levelsHit.has(i)) continue;
    if (gap >= GAP_LEVELS[i]) {
      enterTrade(w, wst, side, ethPrice, i);
    }
  }
}

function enterTrade(w, wst, side, ethPrice, levelIdx) {
  const cost = ethPrice * SHARES;
  if (state.balance < cost) { log(`💸 Low balance $${state.balance.toFixed(2)}`); return; }
  const id = tradeId();
  const t = {
    id, windowStart: w.windowStart, side,
    token: side === 'UP' ? w.ethUp : w.ethDn,
    shares: SHARES, entryPrice: ethPrice, totalCost: cost,
    openedAt: new Date().toISOString(), level: levelIdx + 1, floatingPnl: 0,
  };
  state.balance -= cost;
  state.openTrades.push(t);
  wst.levelsHit.add(levelIdx);
  wst.entries++;
  saveState();
  log(`🟢 ENTRY ETH${side} L${levelIdx+1} [${id}] gap>=0.${(levelIdx+1)*10} price=${ethPrice.toFixed(3)} cost=$${cost.toFixed(2)} bal=$${state.balance.toFixed(2)}`);
  emitFn('trade_entered', t);
}

function closeAllTrades(trades, exitPrice, reason, w, wst) {
  let totalPnl = 0;
  for (const t of trades) {
    const proceeds = exitPrice * t.shares;
    const pnl = proceeds - t.totalCost;
    totalPnl += pnl;
    state.balance += proceeds;
    state.totalPnl += pnl;
    state.closedTrades.push({
      ...t, exitPrice, exitProceeds: +proceeds.toFixed(2),
      realizedPnl: +pnl.toFixed(4), closedAt: new Date().toISOString(), exitReason: reason,
    });
    emitFn('trade_closed', t);
  }
  state.openTrades = state.openTrades.filter(t => t.windowStart !== w.windowStart);
  saveState();
  log(`${totalPnl >= 0 ? '🟢' : '🔴'} ${reason} closed ${trades.length} trade(s) price=${exitPrice.toFixed(3)} totalPnl=$${totalPnl.toFixed(2)} bal=$${state.balance.toFixed(2)}`);
}

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
      log(`${pnl >= 0 ? '🟢' : '🔴'} RESOLVED ETH${t.side} L${t.level} [${t.id}] price=${rp.toFixed(3)} pnl=$${pnl.toFixed(2)} bal=$${state.balance.toFixed(2)}`);
    }
    state.openTrades = state.openTrades.filter(t => t.windowStart !== w.windowStart);
    delete marketCache[tsKey];
    delete windowState[w.windowStart];
    saveState();
  }
}

function buildDashboardSnapshot() {
  const w      = getCurrentMarket();
  const nowSec = Math.floor(Date.now() / 1000);
  const wst    = w ? getWS(w.windowStart) : null;
  const elapsed   = w ? Math.max(0, nowSec - w.windowStart) : 0;
  const remaining = w ? Math.max(0, WINDOW_SIZE - elapsed) : 0;
  const openTrades = w ? openTradesForWindow(w.windowStart) : [];
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
      levelsHit:   wst ? [...wst.levelsHit] : [],
      openCount:   openTrades.length,
      flips:       wst?.flips ?? 0,
      entries:     wst?.entries ?? 0,
      tpHits:      wst?.tpHits ?? 0,
      btcUpPrice:  +btcUp.toFixed(3),
      btcDnPrice:  +btcDn.toFixed(3),
      ethUpPrice:  +ethUp.toFixed(3),
      ethDnPrice:  +ethDn.toFixed(3),
      gapUp:       btcUp > 0 && ethUp > 0 ? +(btcUp - ethUp).toFixed(3) : null,
      gapDn:       btcDn > 0 && ethDn > 0 ? +(btcDn - ethDn).toFixed(3) : null,
      openTrades:  openTrades,
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
  log('🚀 Polymarket ETH Gap Bot (5m) — levels=0.10/0.20/0.30 resolved only');
  loadState(); connectWS(); await tick();
  timer = setInterval(tick, 5000);
  setInterval(async function() {
    await pollPrices();
    const w = getCurrentMarket();
    if (!w) return;
    const btcUp = getPrice(w.btcUp);
    const btcDn = getPrice(w.btcDn);
    const ethUp = getPrice(w.ethUp);
    const ethDn = getPrice(w.ethDn);
    emitFn('prices', {
      btcUpPrice: +btcUp.toFixed(3),
      btcDnPrice: +btcDn.toFixed(3),
      ethUpPrice: +ethUp.toFixed(3),
      ethDnPrice: +ethDn.toFixed(3),
      gapUp: +(btcUp - ethUp).toFixed(3),
      gapDn: +(btcDn - ethDn).toFixed(3),
    });
  }, 2000);
  log(`💰 Balance: $${state.balance.toFixed(2)}`);
}
function stop() { clearInterval(timer); ws?.terminate(); }
module.exports = { start, stop, buildDashboardSnapshot };
