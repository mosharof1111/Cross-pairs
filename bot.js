'use strict';

const fetch = require('node-fetch');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');

const GAMMA      = 'https://gamma-api.polymarket.com';
const CLOB_REST  = 'https://clob.polymarket.com';
const CLOB_WS    = 'wss://ws-subscriptions-clob.polymarket.com/ws/market';
const TRADES_FILE = path.join(__dirname, 'trades.json');

const WINDOW_SIZE        = 300;
const ENTRY_PRICE        = 0.32;
const ENTRY_AMOUNT       = 10;
const FIRST_SL           = 0.05;
const RECOVERY_TRIGGER   = 0.65;
const RECOVERY_PRICE     = 0.65;
const RECOVERY_TP        = 0.99;
const RECOVERY_SL        = 0.45;
const FIRST_TP           = 0.99;
const RECOVERY_TARGET    = 15;
const STARTING_BALANCE   = 1000;

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
        await placePreMarketOrders(res.ts);
      }
    }
  } finally { discovering = false; }
}

async function placePreMarketOrders(ts) {
  const w = marketCache[ts];
  if (!w || windowState[ts]) return;
  await pollPricesForWindow(w);
  const upPrice = getPrice(w.btcUp);
  const dnPrice = getPrice(w.btcDn);
  if (!upPrice || !dnPrice) { log(`⚠️  No price for next window ${ts} — will retry`); return; }
  windowState[ts] = {
    phase: 'PENDING',
    filledSide: null,
    firstTrade: null,
    // recoveries is an array — multiple recoveries allowed
    recoveries: [],
    upLimitPrice: ENTRY_PRICE,
    dnLimitPrice: ENTRY_PRICE,
    upCancelled: false,
    dnCancelled: false,
    resolved: false,
    // track if opposite side has gone below recovery trigger after last recovery closed
    recoveryArmed: false,
  };
  log(`📋 PRE-MARKET ts=${ts} | BUY UP @ ${ENTRY_PRICE} AND BUY DOWN @ ${ENTRY_PRICE}`);
  emitFn('snapshot', buildDashboardSnapshot());
}

function tradeId() { return `T${Date.now().toString(36).toUpperCase()}`; }

function activeRecovery(wst) {
  return wst.recoveries.find(r => !r.closed) || null;
}

function checkWindow(w) {
  const wst = windowState[w.windowStart];
  if (!wst || wst.resolved) return;
  const upPrice = getPrice(w.btcUp);
  const dnPrice = getPrice(w.btcDn);
  if (!upPrice || !dnPrice) return;

  // Update floating pnl for first trade
  if (wst.firstTrade && !wst.firstTrade.closed) {
    const fp = wst.filledSide === 'UP' ? upPrice : dnPrice;
    wst.firstTrade.floatingPnl = +((fp - wst.firstTrade.entryPrice) * wst.firstTrade.shares).toFixed(4);
    const ot = state.openTrades.find(t => t.id === wst.firstTrade.id);
    if (ot) ot.floatingPnl = wst.firstTrade.floatingPnl;
  }

  // Update floating pnl for active recovery
  const ar = activeRecovery(wst);
  if (ar) {
    const rp = ar.side === 'UP' ? upPrice : dnPrice;
    ar.floatingPnl = +((rp - ar.entryPrice) * ar.shares).toFixed(4);
    const ot = state.openTrades.find(t => t.id === ar.id);
    if (ot) ot.floatingPnl = ar.floatingPnl;
  }

  if (wst.phase === 'PENDING')       checkLimitFills(w, wst, upPrice, dnPrice);
  if (wst.phase === 'FILLED')        checkFirstPosition(w, wst, upPrice, dnPrice);
  if (wst.phase === 'RECOVERY')      checkRecoveryPosition(w, wst, upPrice, dnPrice);
  if (wst.phase === 'RECOVERY_WAIT') checkRecoveryTrigger(w, wst, upPrice, dnPrice);
}

function checkLimitFills(w, wst, upPrice, dnPrice) {
  if (!wst.upCancelled && upPrice <= wst.upLimitPrice) { fillFirst(w, wst, 'UP', upPrice); return; }
  if (!wst.dnCancelled && dnPrice <= wst.dnLimitPrice) { fillFirst(w, wst, 'DOWN', dnPrice); return; }
}

function fillFirst(w, wst, side, price) {
  if (state.balance < ENTRY_AMOUNT) { log(`💸 Low balance`); return; }
  const shares = ENTRY_AMOUNT / price;
  const id = tradeId();
  wst.filledSide = side;
  wst.phase      = 'FILLED';
  if (side === 'UP') { wst.dnCancelled = true; log(`❌ Cancelled DN limit`); }
  else               { wst.upCancelled = true; log(`❌ Cancelled UP limit`); }
  wst.firstTrade = { id, side, entryPrice: price, shares: +shares.toFixed(4), cost: ENTRY_AMOUNT, sl: FIRST_SL, tp: FIRST_TP, closed: false, floatingPnl: 0 };
  state.balance -= ENTRY_AMOUNT;
  const trade = { id, windowStart: w.windowStart, side, type: 'FIRST', entryPrice: price, sl: FIRST_SL, tp: FIRST_TP, shares: +shares.toFixed(4), cost: ENTRY_AMOUNT, openedAt: new Date().toISOString(), floatingPnl: 0 };
  state.openTrades.push(trade);
  saveState();
  log(`🟢 FILLED ${side} [${id}] @ ${price.toFixed(3)} shares=${shares.toFixed(2)} cost=$${ENTRY_AMOUNT} sl=${FIRST_SL} tp=${FIRST_TP} bal=$${state.balance.toFixed(2)}`);
  emitFn('trade_entered', trade);
}

function checkFirstPosition(w, wst, upPrice, dnPrice) {
  const ft       = wst.firstTrade;
  const curPrice = ft.side === 'UP' ? upPrice : dnPrice;
  const oppPrice = ft.side === 'UP' ? dnPrice : upPrice;
  if (curPrice >= ft.tp) { closeFirstPosition(w, wst, ft.tp, 'TP'); return; }
  if (curPrice <= ft.sl) { closeFirstPosition(w, wst, ft.sl, 'SL'); }
  // Check recovery trigger — opposite side reaches 0.65
  if (!activeRecovery(wst) && oppPrice >= RECOVERY_TRIGGER) {
    placeRecovery(w, wst, oppPrice);
  }
}

function checkRecoveryTrigger(w, wst, upPrice, dnPrice) {
  const oppPrice = wst.filledSide === 'UP' ? dnPrice : upPrice;
  if (!activeRecovery(wst) && oppPrice >= RECOVERY_TRIGGER) {
    placeRecovery(w, wst, oppPrice);
  }
}

function closeFirstPosition(w, wst, exitPrice, reason) {
  const ft      = wst.firstTrade;
  if (ft.closed) return;
  const proceeds = exitPrice * ft.shares;
  const pnl     = proceeds - ft.cost;
  state.balance  += proceeds;
  state.totalPnl += pnl;
  ft.closed = true;
  state.openTrades = state.openTrades.filter(t => t.id !== ft.id);
  state.closedTrades.push({ id: ft.id, windowStart: w.windowStart, side: ft.side, type: 'FIRST', entryPrice: ft.entryPrice, exitPrice, shares: ft.shares, cost: ft.cost, proceeds: +proceeds.toFixed(2), realizedPnl: +pnl.toFixed(4), closedAt: new Date().toISOString(), exitReason: reason });
  saveState();
  log(`${pnl >= 0 ? '🟢' : '🔴'} FIRST ${reason} ${ft.side} [${ft.id}] entry=${ft.entryPrice.toFixed(3)} exit=${exitPrice.toFixed(3)} pnl=$${pnl.toFixed(2)} bal=$${state.balance.toFixed(2)}`);
  if (!activeRecovery(wst)) wst.phase = 'RECOVERY_WAIT';
}

function placeRecovery(w, wst, oppPrice) {
  // Use fixed RECOVERY_PRICE (0.65) not live price for consistent math
  const buyPrice       = Math.min(oppPrice, RECOVERY_PRICE);
  const profitPerShare = RECOVERY_TP - buyPrice;
  if (profitPerShare <= 0) { log(`⚠️  Recovery profit <= 0`); return; }
  const shares       = RECOVERY_TARGET / profitPerShare;
  const recoveryCost = +(shares * buyPrice).toFixed(2);
  if (state.balance < recoveryCost) { log(`💸 Low balance for recovery — need $${recoveryCost.toFixed(2)}`); return; }
  const recoverySide = wst.filledSide === 'UP' ? 'DOWN' : 'UP';
  const id           = tradeId();
  const recNum       = wst.recoveries.length + 1;
  const rec = { id, side: recoverySide, entryPrice: buyPrice, shares: +shares.toFixed(4), cost: recoveryCost, sl: RECOVERY_SL, tp: RECOVERY_TP, closed: false, floatingPnl: 0, num: recNum };
  wst.recoveries.push(rec);
  wst.phase = 'RECOVERY';
  state.balance -= recoveryCost;
  const trade = { id, windowStart: w.windowStart, side: recoverySide, type: `RECOVERY#${recNum}`, entryPrice: buyPrice, sl: RECOVERY_SL, tp: RECOVERY_TP, shares: +shares.toFixed(4), cost: recoveryCost, openedAt: new Date().toISOString(), floatingPnl: 0 };
  state.openTrades.push(trade);
  saveState();
  log(`🔄 RECOVERY#${recNum} ${recoverySide} [${id}] @ ${buyPrice.toFixed(3)} shares=${shares.toFixed(2)} cost=$${recoveryCost.toFixed(2)} target=$${RECOVERY_TARGET} sl=${RECOVERY_SL} tp=${RECOVERY_TP} bal=$${state.balance.toFixed(2)}`);
  emitFn('trade_entered', trade);
}

function checkRecoveryPosition(w, wst, upPrice, dnPrice) {
  const ar = activeRecovery(wst);
  if (!ar) { wst.phase = wst.firstTrade?.closed ? 'RECOVERY_WAIT' : 'FILLED'; return; }
  const curPrice = ar.side === 'UP' ? upPrice : dnPrice;
  const oppPrice = ar.side === 'UP' ? dnPrice : upPrice;

  if (curPrice >= ar.tp) {
    closeRecovery(w, wst, ar, ar.tp, 'TP');
    // After TP — if first trade still open, go back to FILLED phase to watch it
    // Also re-arm for another recovery if opposite drops back to 0.65
    wst.phase = wst.firstTrade && !wst.firstTrade.closed ? 'FILLED' : 'RECOVERY_WAIT';
    return;
  }
  if (curPrice <= ar.sl) {
    closeRecovery(w, wst, ar, ar.sl, 'SL');
    wst.phase = wst.firstTrade && !wst.firstTrade.closed ? 'FILLED' : 'RECOVERY_WAIT';
    return;
  }

  // Check if another recovery needed while this one is open — NO, only one active at a time
  // But check first position TP/SL while recovery is running
  if (wst.firstTrade && !wst.firstTrade.closed) {
    const fp = wst.filledSide === 'UP' ? upPrice : dnPrice;
    if (fp >= wst.firstTrade.tp) closeFirstPosition(w, wst, wst.firstTrade.tp, 'TP');
    else if (fp <= wst.firstTrade.sl) closeFirstPosition(w, wst, wst.firstTrade.sl, 'SL');
  }

  // Check if opposite drops back to trigger again for NEXT recovery after this closes
  // (handled after close above)
}

function closeRecovery(w, wst, rec, exitPrice, reason) {
  const proceeds = exitPrice * rec.shares;
  const pnl     = proceeds - rec.cost;
  state.balance  += proceeds;
  state.totalPnl += pnl;
  rec.closed = true;
  state.openTrades = state.openTrades.filter(t => t.id !== rec.id);
  state.closedTrades.push({ id: rec.id, windowStart: w.windowStart, side: rec.side, type: `RECOVERY#${rec.num}`, entryPrice: rec.entryPrice, exitPrice, shares: rec.shares, cost: rec.cost, proceeds: +proceeds.toFixed(2), realizedPnl: +pnl.toFixed(4), closedAt: new Date().toISOString(), exitReason: reason });
  saveState();
  log(`${pnl >= 0 ? '🟢' : '🔴'} RECOVERY#${rec.num} ${reason} ${rec.side} [${rec.id}] entry=${rec.entryPrice.toFixed(3)} exit=${exitPrice.toFixed(3)} pnl=$${pnl.toFixed(2)} bal=$${state.balance.toFixed(2)}`);
}

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
      const rp  = t.side === 'UP' ? upPrice : dnPrice;
      const pro = rp * t.shares;
      const pnl = pro - t.cost;
      windowPnl      += pnl;
      state.balance  += pro;
      state.totalPnl += pnl;
      state.closedTrades.push({ ...t, exitPrice: rp, proceeds: +pro.toFixed(2), realizedPnl: +pnl.toFixed(4), closedAt: new Date().toISOString(), exitReason: 'RESOLVED' });
      log(`${pnl >= 0 ? '🟢' : '🔴'} RESOLVED ${t.type} ${t.side} [${t.id}] resolved=${rp.toFixed(3)} pnl=$${pnl.toFixed(2)}`);
    }
    state.openTrades = state.openTrades.filter(t => t.windowStart !== ts);
    wst.resolved = true;
    saveState();
    const totalRec = wst.recoveries.length;
    const recTP = wst.recoveries.filter(r => r.closed).length;
    log(`📊 SUMMARY ts=${ts} recoveries=${totalRec} tp=${recTP} windowPnl=$${windowPnl.toFixed(2)} bal=$${state.balance.toFixed(2)}`);
    delete marketCache[ts];
  }
}

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
  for (const ts of [cws, cws + WINDOW_SIZE]) {
    const w = marketCache[ts];
    if (w) await pollPricesForWindow(w);
  }
}

let ws = null, wsReady = false;
const pendingSubs = new Set();
function connectWS() {
  ws = new WebSocket(CLOB_WS);
  ws.on('open', () => { wsReady = true; log('✅ WebSocket connected'); for (const t of pendingSubs) _sub(t); pendingSubs.clear(); });
  ws.on('close', () => { wsReady = false; log('⚡ WS closed — retry 5s'); setTimeout(connectWS, 5000); });
  ws.on('error', e => log(`⚠️  WS: ${e.message}`));
}
function _sub(tid) { ws.send(JSON.stringify({ assets_ids: [tid], type: 'market' })); }

function buildDashboardSnapshot() {
  const cws    = currentWindowStart();
  const nowSec = Math.floor(Date.now() / 1000);
  const curW   = marketCache[cws];
  const curWst = windowState[cws];
  const nextWs = cws + WINDOW_SIZE;
  const nextW  = marketCache[nextWs];
  const nextWst = windowState[nextWs];
  return {
    balance:      +state.balance.toFixed(2),
    totalPnl:     +state.totalPnl.toFixed(2),
    openTrades:   state.openTrades,
    closedTrades: state.closedTrades.slice(-30),
    updatedAt:    new Date().toISOString(),
    current: curW ? {
      windowStart: cws,
      elapsed:   nowSec - cws,
      remaining: Math.max(0, WINDOW_SIZE - (nowSec - cws)),
      upPrice:   +getPrice(curW.btcUp).toFixed(3),
      dnPrice:   +getPrice(curW.btcDn).toFixed(3),
      wst: curWst ? {
        phase:       curWst.phase,
        filledSide:  curWst.filledSide,
        upCancelled: curWst.upCancelled,
        dnCancelled: curWst.dnCancelled,
        firstTrade:  curWst.firstTrade,
        recoveries:  curWst.recoveries,
        activeRecovery: activeRecovery(curWst),
      } : null,
    } : null,
    next: nextW ? {
      windowStart: nextWs,
      upPrice:    +getPrice(nextW.btcUp).toFixed(3),
      dnPrice:    +getPrice(nextW.btcDn).toFixed(3),
      phase:      nextWst?.phase ?? null,
    } : null,
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
    const nextWs = currentWindowStart() + WINDOW_SIZE;
    if (marketCache[nextWs] && !windowState[nextWs]) await placePreMarketOrders(nextWs);
    const w = marketCache[currentWindowStart()];
    if (w) checkWindow(w);
    await checkResolution();
    emitFn('snapshot', buildDashboardSnapshot());
  } catch (e) { log(`⚠️  tick: ${e.message}`); }
}

async function start(emit, logEmit) {
  emitFn = emit; logFn = logEmit;
  log('🚀 BTC 5m Sniper — limit@0.32 SL=0.05 Recovery@0.65 TP=0.99 unlimited recoveries');
  loadState(); connectWS(); await tick();
  timer = setInterval(tick, 5000);
  setInterval(async function() {
    await pollPrices();
    const cws = currentWindowStart();
    const curW = marketCache[cws];
    const nextW = marketCache[cws + WINDOW_SIZE];
    emitFn('prices', {
      upPrice:     curW  ? +getPrice(curW.btcUp).toFixed(3)  : 0,
      dnPrice:     curW  ? +getPrice(curW.btcDn).toFixed(3)  : 0,
      nextUpPrice: nextW ? +getPrice(nextW.btcUp).toFixed(3) : 0,
      nextDnPrice: nextW ? +getPrice(nextW.btcDn).toFixed(3) : 0,
    });
  }, 2000);
  log(`💰 Balance: $${state.balance.toFixed(2)}`);
}
function stop() { clearInterval(timer); ws?.terminate(); }
module.exports = { start, stop, buildDashboardSnapshot };
