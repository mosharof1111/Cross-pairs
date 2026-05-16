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
const BUY_SHARES       = 100;
const LADDER_SHARES    = 10;
const LADDER_STEP      = 0.04;
const LADDER_LEVELS    = 10;
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
  const cws    = currentWindowStart();
  const nextWs = cws + WINDOW_SIZE;
  try {
    if (!marketCache[cws]) {
      log(`🔍 Finding current window ${cws}…`);
      const res = await findMarketForTs(cws);
      if (res) {
        marketCache[res.ts] = { windowStart: res.ts, btcUp: res.tokens.upToken, btcDn: res.tokens.dnToken, slug: res.slug };
        log(`✅ Current found ts=${res.ts} | ${res.slug}`);
      }
    }
    if (!marketCache[nextWs] && !windowState[nextWs]) {
      log(`🔍 Finding next window ${nextWs}…`);
      const res = await findMarketForTs(nextWs);
      if (res) {
        marketCache[res.ts] = { windowStart: res.ts, btcUp: res.tokens.upToken, btcDn: res.tokens.dnToken, slug: res.slug };
        log(`✅ Next found ts=${res.ts} | ${res.slug}`);
        await buyBothSides(res.ts);
      }
    }
  } finally { discovering = false; }
}

async function pollPricesForWindow(w) {
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
  for (const ts of [cws, cws + WINDOW_SIZE]) {
    const w = marketCache[ts];
    if (w) await pollPricesForWindow(w);
  }
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

async function buyBothSides(ts) {
  const w = marketCache[ts];
  if (!w) return;
  if (windowState[ts]?.bought) return;

  await pollPricesForWindow(w);

  const upPrice = getPrice(w.btcUp);
  const dnPrice = getPrice(w.btcDn);

  if (!upPrice || !dnPrice) {
    log(`⚠️  No price for next window ${ts} yet — will retry`);
    return;
  }

  const upCost    = upPrice * BUY_SHARES;
  const dnCost    = dnPrice * BUY_SHARES;
  const totalCost = upCost + dnCost;

  if (state.balance < totalCost) {
    log(`💸 Low balance $${state.balance.toFixed(2)} need $${totalCost.toFixed(2)}`);
    return;
  }

  const ladderUp = [];
  const ladderDn = [];
  for (let i = 0; i < LADDER_LEVELS; i++) {
    ladderUp.push({ level: i + 1, sellPrice: +(upPrice + (i + 1) * LADDER_STEP).toFixed(3), shares: LADDER_SHARES, sold: false, proceeds: 0 });
    ladderDn.push({ level: i + 1, sellPrice: +(dnPrice + (i + 1) * LADDER_STEP).toFixed(3), shares: LADDER_SHARES, sold: false, proceeds: 0 });
  }

  windowState[ts] = {
    entryUp: upPrice, entryDn: dnPrice,
    sharesUp: BUY_SHARES, sharesDn: BUY_SHARES,
    remainingUp: BUY_SHARES, remainingDn: BUY_SHARES,
    ladderUp, ladderDn,
    bought: true, resolved: false,
    upCost, dnCost,
  };

  state.balance -= totalCost;
  state.openTrades.push({
    id: `W${ts}`, windowStart: ts, type: 'PRE_BUY',
    entryUp: upPrice, entryDn: dnPrice,
    sharesUp: BUY_SHARES, sharesDn: BUY_SHARES,
    upCost, dnCost, totalCost,
    openedAt: new Date().toISOString(),
  });

  saveState();
  log(`🟢 PRE-BUY ts=${ts} BTC↑ ${BUY_SHARES}@${upPrice.toFixed(3)}=$${upCost.toFixed(2)} BTC↓ ${BUY_SHARES}@${dnPrice.toFixed(3)}=$${dnCost.toFixed(2)} total=$${totalCost.toFixed(2)} bal=$${state.balance.toFixed(2)}`);
  log(`   Ladder UP:  sell 10 @ ${ladderUp.map(l => l.sellPrice).join(', ')}`);
  log(`   Ladder DN:  sell 10 @ ${ladderDn.map(l => l.sellPrice).join(', ')}`);
  emitFn('pre_buy', windowState[ts]);
}

function checkLadders() {
  const nowSec = Math.floor(Date.now() / 1000);
  for (const tsStr of Object.keys(windowState)) {
    const ts  = Number(tsStr);
    const wst = windowState[ts];
    if (!wst.bought || wst.resolved) continue;
    if (nowSec < ts) continue;
    const w = marketCache[ts];
    if (!w) continue;
    const upPrice = getPrice(w.btcUp);
    const dnPrice = getPrice(w.btcDn);
    for (const rung of wst.ladderUp) {
      if (rung.sold) continue;
      if (upPrice >= rung.sellPrice) {
        rung.sold     = true;
        rung.proceeds = +(rung.sellPrice * rung.shares).toFixed(2);
        wst.remainingUp -= rung.shares;
        state.balance   += rung.proceeds;
        log(`📤 SELL UP L${rung.level} [ts=${ts}] price=${upPrice.toFixed(3)} >= ${rung.sellPrice.toFixed(3)} proceeds=$${rung.proceeds} bal=$${state.balance.toFixed(2)}`);
      }
    }
    for (const rung of wst.ladderDn) {
      if (rung.sold) continue;
      if (dnPrice >= rung.sellPrice) {
        rung.sold     = true;
        rung.proceeds = +(rung.sellPrice * rung.shares).toFixed(2);
        wst.remainingDn -= rung.shares;
        state.balance   += rung.proceeds;
        log(`📤 SELL DN L${rung.level} [ts=${ts}] price=${dnPrice.toFixed(3)} >= ${rung.sellPrice.toFixed(3)} proceeds=$${rung.proceeds} bal=$${state.balance.toFixed(2)}`);
      }
    }
  }
}

async function checkResolution() {
  const nowSec = Math.floor(Date.now() / 1000);
  for (const tsStr of Object.keys(windowState)) {
    const ts  = Number(tsStr);
    const wst = windowState[ts];
    if (!wst.bought || wst.resolved) continue;
    if (nowSec < ts + WINDOW_SIZE + 30) continue;
    const w = marketCache[ts];
    if (!w) continue;
    log(`⏰ Resolving window ts=${ts}…`);
    await pollPricesForWindow(w);
    const upPrice = getPrice(w.btcUp);
    const dnPrice = getPrice(w.btcDn);
    log(`   Resolved: BTC↑=${upPrice.toFixed(3)} BTC↓=${dnPrice.toFixed(3)}`);
    let totalPnl = 0;
    if (wst.remainingUp > 0) {
      const proceeds = upPrice * wst.remainingUp;
      const cost = wst.entryUp * wst.remainingUp;
      const pnl  = proceeds - cost;
      totalPnl  += pnl;
      state.balance += proceeds;
      log(`${pnl >= 0 ? '🟢' : '🔴'} RESOLVED UP ${wst.remainingUp} shares @ ${upPrice.toFixed(3)} pnl=$${pnl.toFixed(2)}`);
    }
    if (wst.remainingDn > 0) {
      const proceeds = dnPrice * wst.remainingDn;
      const cost = wst.entryDn * wst.remainingDn;
      const pnl  = proceeds - cost;
      totalPnl  += pnl;
      state.balance += proceeds;
      log(`${pnl >= 0 ? '🟢' : '🔴'} RESOLVED DN ${wst.remainingDn} shares @ ${dnPrice.toFixed(3)} pnl=$${pnl.toFixed(2)}`);
    }
    const ladderUpProceeds = wst.ladderUp.filter(r => r.sold).reduce((s, r) => s + r.proceeds, 0);
    const ladderDnProceeds = wst.ladderDn.filter(r => r.sold).reduce((s, r) => s + r.proceeds, 0);
    const ladderUpCost     = wst.entryUp * (BUY_SHARES - wst.remainingUp);
    const ladderDnCost     = wst.entryDn * (BUY_SHARES - wst.remainingDn);
    const ladderPnl        = (ladderUpProceeds - ladderUpCost) + (ladderDnProceeds - ladderDnCost);
    totalPnl += ladderPnl;
    state.totalPnl += totalPnl;
    wst.resolved        = true;
    wst.resolvedUpPrice = upPrice;
    wst.resolvedDnPrice = dnPrice;
    wst.totalPnl        = +totalPnl.toFixed(4);
    state.openTrades = state.openTrades.filter(t => t.windowStart !== ts);
    state.closedTrades.push({
      id: `W${ts}`, windowStart: ts, type: 'WINDOW',
      entryUp: wst.entryUp, entryDn: wst.entryDn,
      resolvedUpPrice: upPrice, resolvedDnPrice: dnPrice,
      ladderUpHits: wst.ladderUp.filter(r => r.sold).length,
      ladderDnHits: wst.ladderDn.filter(r => r.sold).length,
      totalPnl: +totalPnl.toFixed(2),
      closedAt: new Date().toISOString(),
    });
    saveState();
    log(`📊 WINDOW SUMMARY ts=${ts} UP hits=${wst.ladderUp.filter(r=>r.sold).length}/10 DN hits=${wst.ladderDn.filter(r=>r.sold).length}/10 totalPnl=$${totalPnl.toFixed(2)} bal=$${state.balance.toFixed(2)}`);
    delete marketCache[ts];
  }
}

function buildDashboardSnapshot() {
  const cws    = currentWindowStart();
  const nowSec = Math.floor(Date.now() / 1000);
  const curW   = marketCache[cws];
  const curUp  = curW ? getPrice(curW.btcUp) : 0;
  const curDn  = curW ? getPrice(curW.btcDn) : 0;
  const nextWs = cws + WINDOW_SIZE;
  const nextW  = marketCache[nextWs];
  const nextUp = nextW ? getPrice(nextW.btcUp) : 0;
  const nextDn = nextW ? getPrice(nextW.btcDn) : 0;
  const nextWst  = windowState[nextWs];
  const activeWst = windowState[cws];
  return {
    balance:      +state.balance.toFixed(2),
    totalPnl:     +state.totalPnl.toFixed(2),
    openTrades:   state.openTrades,
    closedTrades: state.closedTrades.slice(-30),
    updatedAt:    new Date().toISOString(),
    current: {
      windowStart: cws,
      elapsed:   nowSec - cws,
      remaining: Math.max(0, WINDOW_SIZE - (nowSec - cws)),
      btcUpPrice: +curUp.toFixed(3),
      btcDnPrice: +curDn.toFixed(3),
      wst: activeWst ? {
        entryUp: activeWst.entryUp, entryDn: activeWst.entryDn,
        remainingUp: activeWst.remainingUp, remainingDn: activeWst.remainingDn,
        ladderUp: activeWst.ladderUp, ladderDn: activeWst.ladderDn,
        resolved: activeWst.resolved,
      } : null,
    },
    next: {
      windowStart: nextWs,
      btcUpPrice: +nextUp.toFixed(3),
      btcDnPrice: +nextDn.toFixed(3),
      bought:   nextWst?.bought  ?? false,
      entryUp:  nextWst?.entryUp ?? null,
      entryDn:  nextWst?.entryDn ?? null,
      ladderUp: nextWst?.ladderUp ?? null,
      ladderDn: nextWst?.ladderDn ?? null,
    },
  };
}

function prune() {
  const cws = currentWindowStart();
  for (const key of Object.keys(marketCache))
    if (Number(key) < cws - WINDOW_SIZE * 3) delete marketCache[key];
}

let timer = null;
async function tick() {
  try {
    prune();
    await refreshMarkets();
    await pollPrices();
    checkLadders();
    await checkResolution();
    const nextWs = currentWindowStart() + WINDOW_SIZE;
    if (marketCache[nextWs] && !windowState[nextWs]?.bought) {
      await buyBothSides(nextWs);
    }
    emitFn('snapshot', buildDashboardSnapshot());
  } catch (e) { log(`⚠️  tick: ${e.message}`); }
}

async function start(emit, logEmit) {
  emitFn = emit; logFn = logEmit;
  log('🚀 BTC 5m Pre-Market Bot — buy 100 each side + sell ladder 10x0.04');
  loadState(); connectWS(); await tick();
  timer = setInterval(tick, 5000);
  setInterval(async function() {
    await pollPrices();
    const snap = buildDashboardSnapshot();
    emitFn('prices', {
      curUp:  snap.current.btcUpPrice,
      curDn:  snap.current.btcDnPrice,
      nextUp: snap.next.btcUpPrice,
      nextDn: snap.next.btcDnPrice,
    });
  }, 2000);
  log(`💰 Balance: $${state.balance.toFixed(2)}`);
}
function stop() { clearInterval(timer); ws?.terminate(); }
module.exports = { start, stop, buildDashboardSnapshot };
