'use strict';

const fetch   = require('node-fetch');
const WebSocket = require('ws');
const ethers  = require('ethers');
const fs      = require('fs');
const path    = require('path');

const GAMMA      = 'https://gamma-api.polymarket.com';
const CLOB_REST  = 'https://clob.polymarket.com';
const TRADES_FILE = path.join(__dirname, 'trades.json');

const BINANCE_WS        = 'wss://stream.binance.com:9443/ws/btcusdt@aggTrade';
const QUICKNODE_WSS     = 'wss://magical-thrumming-darkness.matic.quiknode.pro/1200504c2577a6812ec891b7f3ea2c5e4ee2bc55/';
const CHAINLINK_BTC_USD = '0xc907E116054Ad103354f2D350FD2514433D57F6f';
const ANSWER_UPDATED_TOPIC = '0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f';
const CHAINLINK_ABI = [
  'function latestRoundData() view returns (uint80 roundId, int256 answer, uint256 startedAt, uint256 updatedAt, uint80 answeredInRound)',
];

const WINDOW_SIZE      = 300;
const SIGNAL_INTERVAL  = 30;
const TRADE_SHARES     = 20;
const ENTRY_AMOUNT     = 10;
const HISTORY_WINDOW   = 900;
const BUCKET_SIZE      = 30;
const STARTING_BALANCE = 1000;

let state = { balance: STARTING_BALANCE, openTrades: [], closedTrades: [], totalPnl: 0 };
const priceBook    = {};
const marketCache  = {};
const windowState  = {};

let binanceBtcPrice    = 0;
let chainlinkBtcPrice  = 0;
let chainlinkUpdatedAt = 0;
let priceHistory       = [];
let lastSignalCheck    = 0;

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

function tradeId() { return `T${Date.now().toString(36).toUpperCase()}`; }

// ── Binance WebSocket ─────────────────────────────────────────────────────────
let binanceWs      = null;
let binanceLastLog = 0;
let binanceLastPx  = 0;

function connectBinance() {
  if (binanceWs) { try { binanceWs.terminate(); } catch(_){} }
  log('🔗 Connecting Binance BTC/USDT aggTrade…');
  binanceWs = new WebSocket(BINANCE_WS);

  binanceWs.on('open', () => log('✅ Binance WS connected'));

  binanceWs.on('message', (raw) => {
    try {
      const msg   = JSON.parse(raw);
      const price = parseFloat(msg.p);
      if (!price || price <= 0) return;
      binanceBtcPrice = price;
      const nowSec = Math.floor(Date.now() / 1000);
      priceHistory.push({ ts: nowSec, price });
      priceHistory = priceHistory.filter(h => h.ts >= nowSec - HISTORY_WINDOW);
      if (Math.abs(price - binanceLastPx) >= 1 && nowSec - binanceLastLog >= 5) {
        log(`💹 Binance BTC = $${price.toFixed(2)} (${price >= binanceLastPx ? '+' : ''}$${(price - binanceLastPx).toFixed(2)})`);
        binanceLastPx  = price;
        binanceLastLog = nowSec;
      }
    } catch (_) {}
  });

  binanceWs.on('close', () => {
    log('⚡ Binance WS closed — reconnecting in 3s…');
    setTimeout(connectBinance, 3000);
  });
  binanceWs.on('error', e => log(`⚠️  Binance: ${e.message}`));
}

// ── Chainlink WebSocket — label only ─────────────────────────────────────────
let chainlinkWs = null;

function connectChainlink() {
  if (chainlinkWs) { try { chainlinkWs.terminate(); } catch(_){} }
  chainlinkWs = new WebSocket(QUICKNODE_WSS);
  chainlinkWs.on('open', () => {
    log('✅ QuickNode WS connected');
    chainlinkWs.send(JSON.stringify({
      jsonrpc: '2.0', id: 1,
      method: 'eth_subscribe',
      params: ['logs', { address: CHAINLINK_BTC_USD, topics: [ANSWER_UPDATED_TOPIC] }],
    }));
  });
  chainlinkWs.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw);
      if (msg.id === 1 && msg.result) { log(`✅ Chainlink subscription: ${msg.result}`); return; }
      if (msg.method === 'eth_subscription' && msg.params?.result) {
        const priceBig = BigInt(msg.params.result.topics[1]);
        const MAX = BigInt('0x7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff');
        const price = priceBig > MAX
          ? Number(priceBig - BigInt('0x10000000000000000000000000000000000000000000000000000000000000000')) / 1e8
          : Number(priceBig) / 1e8;
        if (price > 0 && price < 1000000) {
          chainlinkBtcPrice  = price;
          chainlinkUpdatedAt = Math.floor(Date.now() / 1000);
          log(`⛓️  Chainlink BTC = $${price.toFixed(2)}`);
        }
      }
    } catch (_) {}
  });
  chainlinkWs.on('close', () => { log('⚡ QuickNode closed — retry 5s'); setTimeout(connectChainlink, 5000); });
  chainlinkWs.on('error', e => log(`⚠️  QuickNode: ${e.message}`));
}

async function fetchInitialChainlinkPrice() {
  try {
    const provider = new ethers.providers.JsonRpcProvider(QUICKNODE_WSS.replace('wss://', 'https://'));
    const feed = new ethers.Contract(CHAINLINK_BTC_USD, CHAINLINK_ABI, provider);
    const data = await feed.latestRoundData();
    chainlinkBtcPrice  = data.answer.toNumber() / 1e8;
    chainlinkUpdatedAt = data.updatedAt.toNumber();
    log(`📡 Initial Chainlink BTC = $${chainlinkBtcPrice.toFixed(2)}`);
  } catch (e) { log(`⚠️  Initial Chainlink: ${e.message}`); }
}

// ── Signal logic ──────────────────────────────────────────────────────────────
function getAverage30sMove() {
  const nowSec = Math.floor(Date.now() / 1000);
  const moves  = [];
  for (let i = 0; i < HISTORY_WINDOW / BUCKET_SIZE; i++) {
    const bucketEnd   = nowSec - i * BUCKET_SIZE;
    const bucketStart = bucketEnd - BUCKET_SIZE;
    const inBucket    = priceHistory.filter(h => h.ts >= bucketStart && h.ts < bucketEnd);
    if (inBucket.length < 2) continue;
    const prices = inBucket.map(h => h.price);
    const move   = Math.abs(Math.max(...prices) - Math.min(...prices));
    if (move > 0) moves.push(move);
  }
  if (!moves.length) return 0;
  return moves.reduce((s, m) => s + m, 0) / moves.length;
}

function getLast30sMove() {
  const nowSec   = Math.floor(Date.now() / 1000);
  const inBucket = priceHistory.filter(h => h.ts >= nowSec - BUCKET_SIZE);
  if (inBucket.length < 2) return { change: 0, absChange: 0, direction: null, first: 0, last: 0 };
  const first     = inBucket[0].price;
  const last      = inBucket[inBucket.length - 1].price;
  const change    = last - first;
  const absChange = Math.abs(change);
  const direction = change > 0 ? 'UP' : 'DOWN';
  return { change, absChange, direction, first, last };
}

function checkSignal() {
  const nowSec = Math.floor(Date.now() / 1000);
  if (nowSec - lastSignalCheck < SIGNAL_INTERVAL) return;
  lastSignalCheck = nowSec;

  const w = marketCache[currentWindowStart()];
  if (!w) return;

  if (priceHistory.length < 10) {
    log(`⏳ Not enough price history yet (${priceHistory.length} ticks)`);
    return;
  }

  const avg  = getAverage30sMove();
  const last = getLast30sMove();

  if (!last.direction || last.absChange === 0) {
    log(`📊 Signal check: avg=$${avg.toFixed(2)} last30s=$${(last.absChange||0).toFixed(2)} — no move`);
    return;
  }

  log(`📊 Signal check: avg30s=$${avg.toFixed(2)} | last30s=${last.direction} $${last.absChange.toFixed(2)} | ${last.absChange > avg ? '✅ ABOVE AVG — firing reversion' : '❌ below avg'}`);

  if (last.absChange <= avg) return;

  // ── MEAN REVERSION — buy opposite direction ───────────────────────────────
  const reversed = last.direction === 'UP' ? 'DOWN' : 'UP';
  placeTrade(w, reversed, last.absChange, avg);
}

function placeTrade(w, direction, move, avg) {
  if (state.balance < ENTRY_AMOUNT) { log(`💸 Low balance`); return; }

  const token = direction === 'UP' ? w.btcUp : w.btcDn;
  const price = getPrice(token);
  if (!price || price <= 0) { log(`⚠️  No token price for ${direction}`); return; }

  const cost = +(price * TRADE_SHARES).toFixed(2);
  if (state.balance < cost) { log(`💸 Low balance for $${cost}`); return; }

  const id  = tradeId();
  const cws = currentWindowStart();
  state.balance -= cost;

  const trade = {
    id, windowStart: cws, side: direction, type: 'REVERSION',
    entryPrice: price, shares: TRADE_SHARES, cost,
    btcMove: +move.toFixed(2), avgMove: +avg.toFixed(2),
    btcPriceAtEntry: +binanceBtcPrice.toFixed(2),
    openedAt: new Date().toISOString(), floatingPnl: 0,
  };
  state.openTrades.push(trade);
  if (!windowState[cws]) windowState[cws] = { trades: 0 };
  windowState[cws].trades++;
  saveState();

  log(`🚀 REVERSION ${direction} [${id}] token=${price.toFixed(3)} shares=${TRADE_SHARES} cost=$${cost} | BTC moved ${move > 0 ? '+' : ''}$${move.toFixed(2)} > avg=$${avg.toFixed(2)} → expect reversion | bal=$${state.balance.toFixed(2)}`);
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
      }
    }
  } finally { discovering = false; }
}

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

async function checkResolution() {
  const nowSec = Math.floor(Date.now() / 1000);
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
    let windowPnl = 0;
    for (const t of tradesInWindow) {
      const rp  = t.side === 'UP' ? upPrice : dnPrice;
      const pro = rp * t.shares;
      const pnl = pro - t.cost;
      windowPnl      += pnl;
      state.balance  += pro;
      state.totalPnl += pnl;
      state.closedTrades.push({
        ...t, exitPrice: rp, proceeds: +pro.toFixed(2),
        realizedPnl: +pnl.toFixed(4),
        closedAt: new Date().toISOString(), exitReason: 'RESOLVED',
      });
      log(`${pnl >= 0 ? '🟢' : '🔴'} RESOLVED ${t.side} [${t.id}] entry=${t.entryPrice.toFixed(3)} resolved=${rp.toFixed(3)} pnl=$${pnl.toFixed(2)}`);
    }
    state.openTrades = state.openTrades.filter(t => t.windowStart !== ts);
    wst.resolved = true;
    delete marketCache[ts];
    saveState();
    log(`📊 WINDOW SUMMARY ts=${ts} trades=${wst.trades||0} windowPnl=$${windowPnl.toFixed(2)} bal=$${state.balance.toFixed(2)}`);
  }
}

function updateFloating() {
  const w = marketCache[currentWindowStart()];
  if (!w) return;
  for (const t of state.openTrades) {
    const p = t.side === 'UP' ? getPrice(w.btcUp) : getPrice(w.btcDn);
    if (p > 0) t.floatingPnl = +((p - t.entryPrice) * t.shares).toFixed(4);
  }
}

function buildDashboardSnapshot() {
  const cws    = currentWindowStart();
  const nowSec = Math.floor(Date.now() / 1000);
  const w      = marketCache[cws];
  const wst    = windowState[cws] || {};
  const upPrice = w ? getPrice(w.btcUp) : 0;
  const dnPrice = w ? getPrice(w.btcDn) : 0;
  const avg     = getAverage30sMove();
  const last    = getLast30sMove();
  return {
    balance:      +state.balance.toFixed(2),
    totalPnl:     +state.totalPnl.toFixed(2),
    openTrades:   state.openTrades,
    closedTrades: state.closedTrades.slice(-50),
    signal: {
      binancePrice:   +binanceBtcPrice.toFixed(2),
      chainlinkPrice: +chainlinkBtcPrice.toFixed(2),
      avg30sMove:     +avg.toFixed(2),
      last30sMove:    +(last.absChange||0).toFixed(2),
      last30sDir:     last.direction || null,
      last30sChange:  +(last.change||0).toFixed(2),
      isSignal:       last.absChange > avg && avg > 0,
      reversedDir:    last.direction ? (last.direction === 'UP' ? 'DOWN' : 'UP') : null,
      historyCount:   priceHistory.length,
      nextCheckIn:    SIGNAL_INTERVAL - ((nowSec - lastSignalCheck) % SIGNAL_INTERVAL),
    },
    window: w ? {
      windowStart:      cws,
      elapsed:          nowSec - cws,
      remaining:        Math.max(0, WINDOW_SIZE - (nowSec - cws)),
      upPrice:          +upPrice.toFixed(3),
      dnPrice:          +dnPrice.toFixed(3),
      tradesThisWindow: wst.trades || 0,
      openInWindow:     state.openTrades.filter(t => t.windowStart === cws).length,
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
    updateFloating();
    checkSignal();
    await checkResolution();
    emitFn('snapshot', buildDashboardSnapshot());
  } catch (e) { log(`⚠️  tick: ${e.message}`); }
}

async function start(emit, logEmit) {
  emitFn = emit; logFn = logEmit;
  log('🚀 BTC Mean Reversion Bot — Binance 30s signal | reverse direction | 20 shares | every 30s');
  log(`   History: ${HISTORY_WINDOW/60}min | bucket: ${BUCKET_SIZE}s | $${ENTRY_AMOUNT}/trade | multi-fire`);
  loadState();
  await fetchInitialChainlinkPrice();
  connectBinance();
  connectChainlink();
  await tick();
  timer = setInterval(tick, 5000);
  setInterval(async function() {
    await pollPrices();
    updateFloating();
    const w = marketCache[currentWindowStart()];
    emitFn('prices', {
      binancePrice:   +binanceBtcPrice.toFixed(2),
      chainlinkPrice: +chainlinkBtcPrice.toFixed(2),
      upPrice:        w ? +getPrice(w.btcUp).toFixed(3) : 0,
      dnPrice:        w ? +getPrice(w.btcDn).toFixed(3) : 0,
    });
  }, 2000);
  log(`💰 Balance: $${state.balance.toFixed(2)}`);
}

function stop() {
  clearInterval(timer);
  if (binanceWs)   { try { binanceWs.terminate();   } catch(_){} }
  if (chainlinkWs) { try { chainlinkWs.terminate(); } catch(_){} }
}

module.exports = { start, stop, buildDashboardSnapshot };
