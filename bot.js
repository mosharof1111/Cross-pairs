'use strict';

const fetch   = require('node-fetch');
const WebSocket = require('ws');
const ethers  = require('ethers');
const fs      = require('fs');
const path    = require('path');

const GAMMA      = 'https://gamma-api.polymarket.com';
const CLOB_REST  = 'https://clob.polymarket.com';
const TRADES_FILE = path.join(__dirname, 'trades.json');

const QUICKNODE_WSS     = 'wss://magical-thrumming-darkness.matic.quiknode.pro/1200504c2577a6812ec891b7f3ea2c5e4ee2bc55/';
const CHAINLINK_BTC_USD = '0xc907E116054Ad103354f2D350FD2514433D57F6f';
const CHAINLINK_ABI = [
  'event AnswerUpdated(int256 indexed current, uint256 indexed roundId, uint256 updatedAt)',
  'function latestRoundData() view returns (uint80 roundId, int256 answer, uint256 startedAt, uint256 updatedAt, uint80 answeredInRound)',
];

const WINDOW_SIZE        = 300;
const ORACLE_ARB_AMOUNT  = 50;    // $ per signal
const ORACLE_ARB_TRIGGER = 90;    // seconds before window end
const ORACLE_MIN_EDGE    = 0.30;  // only buy if token price below this
const STARTING_BALANCE   = 1000;

let state = { balance: STARTING_BALANCE, openTrades: [], closedTrades: [], totalPnl: 0 };
const priceBook   = {};
const marketCache = {};
const windowState = {};

let chainlinkBtcPrice  = 0;
let chainlinkUpdatedAt = 0;
let windowOpenBtcPrice = {};
let chainlinkHistory   = [];
let oracleProvider     = null;
let oracleFeed         = null;

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

// ── Chainlink Oracle ──────────────────────────────────────────────────────────
async function connectOracle() {
  try {
    log('🔗 Connecting Chainlink BTC/USD via QuickNode…');
    oracleProvider = new ethers.providers.WebSocketProvider(QUICKNODE_WSS);
    oracleFeed     = new ethers.Contract(CHAINLINK_BTC_USD, CHAINLINK_ABI, oracleProvider);
    const data = await oracleFeed.latestRoundData();
    chainlinkBtcPrice  = data.answer.toNumber() / 1e8;
    chainlinkUpdatedAt = data.updatedAt.toNumber();
    pushChainlinkHistory();
    recordWindowOpenPrice();
    log(`✅ Chainlink connected — BTC/USD = $${chainlinkBtcPrice.toFixed(2)}`);
    oracleFeed.on('AnswerUpdated', (current, roundId, updatedAt) => {
      const newPrice = current.toNumber() / 1e8;
      const oldPrice = chainlinkBtcPrice;
      chainlinkBtcPrice  = newPrice;
      chainlinkUpdatedAt = updatedAt.toNumber();
      const change = newPrice - oldPrice;
      pushChainlinkHistory();
      const threshold = getDynamicThreshold();
      log(`⚡ Chainlink BTC/USD = $${newPrice.toFixed(2)} (${change >= 0 ? '+' : ''}$${change.toFixed(2)}) | threshold=$${threshold.toFixed(2)}`);
      checkOracleArb();
    });
    oracleProvider._websocket.on('close', () => {
      log('⚡ QuickNode WS closed — reconnecting in 5s…');
      setTimeout(connectOracle, 5000);
    });
    oracleProvider._websocket.on('error', e => log(`⚠️  QuickNode: ${e.message}`));
  } catch (e) {
    log(`⚠️  Oracle error: ${e.message} — retry in 10s`);
    setTimeout(connectOracle, 10000);
  }
}

function recordWindowOpenPrice() {
  const cws = currentWindowStart();
  if (!windowOpenBtcPrice[cws] && chainlinkBtcPrice > 0) {
    windowOpenBtcPrice[cws] = chainlinkBtcPrice;
    log(`📌 Window ${cws} open BTC = $${chainlinkBtcPrice.toFixed(2)}`);
  }
}

function pushChainlinkHistory() {
  if (chainlinkBtcPrice <= 0) return;
  const nowSec = Math.floor(Date.now() / 1000);
  chainlinkHistory.push({ ts: nowSec, price: chainlinkBtcPrice });
  chainlinkHistory = chainlinkHistory.filter(h => h.ts >= nowSec - 1800);
}

function getDynamicThreshold() {
  const MIN = 5;
  if (chainlinkHistory.length < 2) return MIN;
  const moves = [];
  const nowSec = Math.floor(Date.now() / 1000);
  for (let i = 0; i < 6; i++) {
    const wEnd   = nowSec - i * WINDOW_SIZE;
    const wStart = wEnd - WINDOW_SIZE;
    const slice  = chainlinkHistory.filter(h => h.ts >= wStart && h.ts <= wEnd);
    if (slice.length < 2) continue;
    const prices = slice.map(h => h.price);
    const move   = Math.abs(Math.max(...prices) - Math.min(...prices));
    if (move > 0) moves.push(move);
  }
  if (!moves.length) return MIN;
  const avg = moves.reduce((s, m) => s + m, 0) / moves.length;
  return +Math.max(MIN, avg * 0.5).toFixed(2);
}

function checkOracleArb() {
  const nowSec    = Math.floor(Date.now() / 1000);
  const cws       = currentWindowStart();
  const remaining = WINDOW_SIZE - (nowSec - cws);
  if (remaining > ORACLE_ARB_TRIGGER || remaining < 5) return;

  const w = marketCache[cws];
  if (!w) return;

  const openPrice = windowOpenBtcPrice[cws];
  if (!openPrice || chainlinkBtcPrice === 0) return;

  const wst = windowState[cws] || {};
  if (wst.oracleArbPlaced) return;

  const btcWentUp  = chainlinkBtcPrice > openPrice;
  const diff       = Math.abs(chainlinkBtcPrice - openPrice);
  const threshold  = getDynamicThreshold();

  if (diff < threshold) {
    log(`📊 Oracle: diff=$${diff.toFixed(2)} < threshold=$${threshold.toFixed(2)} — skip`);
    return;
  }

  const winningToken = btcWentUp ? w.btcUp : w.btcDn;
  const winningPrice = getPrice(winningToken);
  const label        = btcWentUp ? 'UP' : 'DOWN';

  log(`🎯 ORACLE SIGNAL: BTC $${openPrice.toFixed(2)} → $${chainlinkBtcPrice.toFixed(2)} = ${label} wins | token=${winningPrice.toFixed(3)} | ${remaining}s left`);

  if (winningPrice <= 0 || winningPrice > ORACLE_MIN_EDGE) {
    log(`⚠️  Token at ${winningPrice.toFixed(3)} > ${ORACLE_MIN_EDGE} — too late`);
    return;
  }

  if (state.balance < ORACLE_ARB_AMOUNT) { log(`💸 Low balance`); return; }

  const shares = ORACLE_ARB_AMOUNT / winningPrice;
  const id     = `T${Date.now().toString(36).toUpperCase()}`;
  state.balance -= ORACLE_ARB_AMOUNT;

  const trade = {
    id, windowStart: cws, side: label, type: 'ORACLE_ARB',
    entryPrice: winningPrice, tp: 0.99, sl: 0,
    shares: +shares.toFixed(4), cost: ORACLE_ARB_AMOUNT,
    openedAt: new Date().toISOString(), floatingPnl: 0,
    oracleBtcOpen: openPrice, oracleBtcClose: chainlinkBtcPrice,
    diff: +diff.toFixed(2), threshold: +threshold.toFixed(2),
  };
  state.openTrades.push(trade);
  if (!windowState[cws]) windowState[cws] = {};
  windowState[cws].oracleArbPlaced = true;
  windowState[cws].oracleArbId    = id;
  saveState();
  log(`🚀 ORACLE ARB ${label} [${id}] token=${winningPrice.toFixed(3)} shares=${shares.toFixed(2)} cost=$${ORACLE_ARB_AMOUNT} bal=$${state.balance.toFixed(2)}`);
  emitFn('snapshot', buildDashboardSnapshot());
}

// ── Market discovery ──────────────────────────────────────────────────────────
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
      const res = await findMarketForTs(cws);
      if (res) {
        marketCache[res.ts] = { windowStart: res.ts, btcUp: res.tokens.upToken, btcDn: res.tokens.dnToken, slug: res.slug };
        log(`✅ Found ts=${res.ts} | ${res.slug}`);
        recordWindowOpenPrice();
      }
    }
  } finally { discovering = false; }
}

// ── REST price polling ────────────────────────────────────────────────────────
async function pollPrices() {
  const w = marketCache[currentWindowStart()];
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

// ── Resolution ────────────────────────────────────────────────────────────────
async function checkResolution() {
  const nowSec = Math.floor(Date.now() / 1000);
  const cws    = currentWindowStart();
  for (const [tsStr, wst] of Object.entries(windowState)) {
    const ts = Number(tsStr);
    if (wst.resolved) continue;
    if (nowSec < ts + WINDOW_SIZE + 30) continue;
    const w = marketCache[ts];
    if (!w) { wst.resolved = true; continue; }
    log(`⏰ Resolving window ts=${ts}…`);
    await Promise.all([w.btcUp, w.btcDn].map(async tid => {
      try {
        const r = await fetch(`${CLOB_REST}/price?token_id=${tid}&side=BUY`, { timeout: 4000 });
        const p = parseFloat((await r.json()).price ?? 0) || 0;
        if (p > 0) priceBook[tid] = { bid: p - 0.005, ask: p + 0.005 };
      } catch (_) {}
    }));
    const upPrice = getPrice(w.btcUp);
    const dnPrice = getPrice(w.btcDn);
    log(`   Resolved: BTC↑=${upPrice.toFixed(3)} BTC↓=${dnPrice.toFixed(3)}`);
    const tradesInWindow = state.openTrades.filter(t => t.windowStart === ts);
    for (const t of tradesInWindow) {
      const rp  = t.side === 'UP' ? upPrice : dnPrice;
      const pro = rp * t.shares;
      const pnl = pro - t.cost;
      state.balance  += pro;
      state.totalPnl += pnl;
      state.closedTrades.push({
        ...t, exitPrice: rp, proceeds: +pro.toFixed(2),
        realizedPnl: +pnl.toFixed(4),
        closedAt: new Date().toISOString(), exitReason: 'RESOLVED',
      });
      log(`${pnl >= 0 ? '🟢' : '🔴'} RESOLVED ${t.side} [${t.id}] entry=${t.entryPrice.toFixed(3)} resolved=${rp.toFixed(3)} pnl=$${pnl.toFixed(2)} bal=$${state.balance.toFixed(2)}`);
    }
    state.openTrades = state.openTrades.filter(t => t.windowStart !== ts);
    wst.resolved = true;
    delete windowOpenBtcPrice[ts];
    delete marketCache[ts];
    saveState();
  }
}

// ── Update floating pnl ───────────────────────────────────────────────────────
function updateFloating() {
  const w = marketCache[currentWindowStart()];
  if (!w) return;
  for (const t of state.openTrades) {
    const p = t.side === 'UP' ? getPrice(w.btcUp) : getPrice(w.btcDn);
    if (p > 0) t.floatingPnl = +((p - t.entryPrice) * t.shares).toFixed(4);
  }
}

// ── Dashboard snapshot ────────────────────────────────────────────────────────
function buildDashboardSnapshot() {
  const cws    = currentWindowStart();
  const nowSec = Math.floor(Date.now() / 1000);
  const w      = marketCache[cws];
  const wst    = windowState[cws] || {};
  const upPrice = w ? getPrice(w.btcUp) : 0;
  const dnPrice = w ? getPrice(w.btcDn) : 0;
  const openInWindow = state.openTrades.filter(t => t.windowStart === cws);
  return {
    balance:      +state.balance.toFixed(2),
    totalPnl:     +state.totalPnl.toFixed(2),
    openTrades:   state.openTrades,
    closedTrades: state.closedTrades.slice(-50),
    oracle: {
      btcPrice:   +chainlinkBtcPrice.toFixed(2),
      updatedAt:  chainlinkUpdatedAt,
      windowOpen: windowOpenBtcPrice[cws] ? +windowOpenBtcPrice[cws].toFixed(2) : null,
      direction:  chainlinkBtcPrice > 0 && windowOpenBtcPrice[cws]
                    ? (chainlinkBtcPrice > windowOpenBtcPrice[cws] ? 'UP' : 'DOWN')
                    : null,
      diff:       windowOpenBtcPrice[cws]
                    ? +(chainlinkBtcPrice - windowOpenBtcPrice[cws]).toFixed(2)
                    : null,
      threshold:  +getDynamicThreshold().toFixed(2),
    },
    window: w ? {
      windowStart:      cws,
      elapsed:          nowSec - cws,
      remaining:        Math.max(0, WINDOW_SIZE - (nowSec - cws)),
      upPrice:          +upPrice.toFixed(3),
      dnPrice:          +dnPrice.toFixed(3),
      oracleArbPlaced:  wst.oracleArbPlaced || false,
      openCount:        openInWindow.length,
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
    recordWindowOpenPrice();
    pushChainlinkHistory();
    updateFloating();
    checkOracleArb();
    await checkResolution();
    emitFn('snapshot', buildDashboardSnapshot());
  } catch (e) { log(`⚠️  tick: ${e.message}`); }
}

async function start(emit, logEmit) {
  emitFn = emit; logFn = logEmit;
  log('🚀 BTC Oracle Arb Bot — Chainlink BTC/USD → Polymarket 5m');
  log(`   $${ORACLE_ARB_AMOUNT} per signal | trigger=${ORACLE_ARB_TRIGGER}s before end | min edge < ${ORACLE_MIN_EDGE} | dynamic threshold`);
  loadState(); await connectOracle(); await tick();
  timer = setInterval(tick, 5000);
  setInterval(async function() {
    await pollPrices();
    updateFloating();
    const w = marketCache[currentWindowStart()];
    emitFn('prices', {
      upPrice:  w ? +getPrice(w.btcUp).toFixed(3) : 0,
      dnPrice:  w ? +getPrice(w.btcDn).toFixed(3) : 0,
      btcPrice: +chainlinkBtcPrice.toFixed(2),
    });
  }, 2000);
  log(`💰 Balance: $${state.balance.toFixed(2)}`);
}
function stop() { clearInterval(timer); if (oracleProvider) oracleProvider.destroy(); }
module.exports = { start, stop, buildDashboardSnapshot };
