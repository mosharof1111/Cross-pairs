'use strict';

const fetch     = require('node-fetch');
const WebSocket = require('ws');
const fs        = require('fs');
const path      = require('path');

const GAMMA      = 'https://gamma-api.polymarket.com';
const CLOB_REST  = 'https://clob.polymarket.com';
const TRADES_FILE  = path.join(__dirname, 'trades.json');
const CONFIG_FILE  = path.join(__dirname, 'config.json');

const BINANCE_STREAMS = {
  BTC:  'wss://stream.binance.com:9443/ws/btcusdt@aggTrade',
  ETH:  'wss://stream.binance.com:9443/ws/ethusdt@aggTrade',
  SOL:  'wss://stream.binance.com:9443/ws/solusdt@aggTrade',
  DOGE: 'wss://stream.binance.com:9443/ws/dogeusdt@aggTrade',
};

// ── Default config — editable from dashboard ──────────────────────────────────
const DEFAULT_CONFIG = {
  shares:          100,
  moveMultiplier:  0.5,
  blockSize:       30,
  tokenMin:        0.10,
  tokenMax:        0.90,
  trendBuckets:    3,
  exitAtSecond:    290,   // sell everything at this second of the window
  historyWindow:   900,
  markets: {
    'btc-5m':  true,
    'eth-5m':  true,
    'sol-5m':  true,
    'doge-5m': true,
  },
};

const MARKET_SLUGS = {
  'btc-5m':  'btc-updown-5m',
  'eth-5m':  'eth-updown-5m',
  'sol-5m':  'sol-updown-5m',
  'doge-5m': 'doge-updown-5m',
};

const MARKET_ASSETS = {
  'btc-5m': 'BTC', 'eth-5m': 'ETH', 'sol-5m': 'SOL', 'doge-5m': 'DOGE',
};

const CRYPTO_FEE_RATE  = 0.018;
const STARTING_BALANCE = 2000;
const WINDOW_SIZE      = 300;

let config = { ...DEFAULT_CONFIG };

let state = {
  balance:      STARTING_BALANCE,
  openTrades:   [],
  closedTrades: [],
  totalPnl:     0,
  totalFees:    0,
};

let botRunning = false;

const priceBook        = {};
const marketCache      = {};
const windowState      = {};
const lastCheckedBlock = { 'btc-5m': -1, 'eth-5m': -1, 'sol-5m': -1, 'doge-5m': -1 };
const lastOpenedSide   = { 'btc-5m': null, 'eth-5m': null, 'sol-5m': null, 'doge-5m': null };
const exitFiredWindow  = { 'btc-5m': -1, 'eth-5m': -1, 'sol-5m': -1, 'doge-5m': -1 };

const priceHistory   = { BTC: [], ETH: [], SOL: [], DOGE: [] };
const binancePrices  = { BTC: 0, ETH: 0, SOL: 0, DOGE: 0 };
const binanceLastLog = { BTC: 0, ETH: 0, SOL: 0, DOGE: 0 };
const binanceLastPx  = { BTC: 0, ETH: 0, SOL: 0, DOGE: 0 };
const binanceWs      = { BTC: null, ETH: null, SOL: null, DOGE: null };

let emitFn = () => {};
let logFn  = () => {};

// ── Config persistence ────────────────────────────────────────────────────────
function loadConfig() {
  try {
    if (fs.existsSync(CONFIG_FILE)) {
      const raw = JSON.parse(fs.readFileSync(CONFIG_FILE, 'utf8'));
      config = { ...DEFAULT_CONFIG, ...raw };
      config.markets = { ...DEFAULT_CONFIG.markets, ...(raw.markets || {}) };
      log(`⚙️  Config loaded: shares=${config.shares} mult=${config.moveMultiplier} block=${config.blockSize}s exit=${config.exitAtSecond}s`);
    }
  } catch (e) { log(`⚠️  Config load: ${e.message}`); }
}
function saveConfig() { fs.writeFileSync(CONFIG_FILE, JSON.stringify(config, null, 2)); }

function calcFee(shares, price) {
  return +(shares * CRYPTO_FEE_RATE * price * (1 - price)).toFixed(4);
}
function addMoney(a, b) { return +((a * 100 + b * 100) / 100).toFixed(2); }
function subMoney(a, b) { return +((a * 100 - b * 100) / 100).toFixed(2); }

function loadState() {
  try {
    if (fs.existsSync(TRADES_FILE)) {
      const raw = JSON.parse(fs.readFileSync(TRADES_FILE, 'utf8'));
      state = { ...state, ...raw };
      if (state.openTrades.length > 0) {
        log(`♻️  Refunding ${state.openTrades.length} open trade(s) on restart`);
        for (const t of state.openTrades) state.balance = addMoney(state.balance, t.cost);
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
function currentBlockNumber() {
  return Math.floor(Math.floor(Date.now() / 1000) / config.blockSize);
}
function windowElapsed() {
  return Math.floor(Date.now() / 1000) - currentWindowStart();
}

function getPrice(tid) {
  const b = priceBook[tid];
  if (!b) return 0;
  if (b.bid > 0 && b.ask > 0) return (b.bid + b.ask) / 2;
  return b.bid || b.ask || 0;
}

function tradeId() { return `T${Date.now().toString(36).toUpperCase()}`; }

// ── Binance WebSocket ─────────────────────────────────────────────────────────
function connectBinance(asset) {
  const url = BINANCE_STREAMS[asset];
  if (binanceWs[asset]) { try { binanceWs[asset].terminate(); } catch(_){} }
  log(`🔗 Connecting Binance ${asset}/USDT…`);
  const ws = new WebSocket(url);
  ws.on('open', () => log(`✅ Binance ${asset} WS connected`));
  ws.on('message', (raw) => {
    try {
      const msg   = JSON.parse(raw);
      const price = parseFloat(msg.p);
      if (!price || price <= 0) return;
      binancePrices[asset] = price;
      const nowSec = Math.floor(Date.now() / 1000);
      priceHistory[asset].push({ ts: nowSec, price });
      priceHistory[asset] = priceHistory[asset].filter(h => h.ts >= nowSec - config.historyWindow);
      const logThresh = Math.max(price * 0.0005, 0.00001);
      if (Math.abs(price - binanceLastPx[asset]) >= logThresh && nowSec - binanceLastLog[asset] >= 5) {
        const change  = price - binanceLastPx[asset];
        const dec     = asset === 'DOGE' ? 5 : asset === 'SOL' ? 3 : 2;
        log(`💹 Binance ${asset} = $${price.toFixed(dec)} (${change >= 0 ? '+' : ''}$${change.toFixed(dec)})`);
        binanceLastPx[asset]  = price;
        binanceLastLog[asset] = nowSec;
      }
    } catch (_) {}
  });
  ws.on('close', () => {
    log(`⚡ Binance ${asset} WS closed — reconnecting in 3s…`);
    setTimeout(() => connectBinance(asset), 3000);
  });
  ws.on('error', e => log(`⚠️  Binance ${asset}: ${e.message}`));
  binanceWs[asset] = ws;
}

// ── Signal logic ──────────────────────────────────────────────────────────────
function getAverageMove(asset) {
  const nowSec     = Math.floor(Date.now() / 1000);
  const hist       = priceHistory[asset].filter(h => h.ts >= nowSec - config.historyWindow);
  const numBuckets = Math.floor(config.historyWindow / config.blockSize);
  const moves      = [];
  for (let i = 0; i < numBuckets; i++) {
    const bucketEnd   = nowSec - i * config.blockSize;
    const bucketStart = bucketEnd - config.blockSize;
    const inBucket    = hist.filter(h => h.ts >= bucketStart && h.ts < bucketEnd);
    if (inBucket.length < 2) continue;
    const prices = inBucket.map(h => h.price);
    const move   = Math.abs(Math.max(...prices) - Math.min(...prices));
    if (move > 0) moves.push(move);
  }
  if (!moves.length) return 0;
  return moves.reduce((s, m) => s + m, 0) / moves.length;
}

function getLastBlockMove(asset) {
  const blockNum    = currentBlockNumber();
  const bucketEnd   = blockNum * config.blockSize;
  const bucketStart = bucketEnd - config.blockSize;
  const inBucket    = priceHistory[asset].filter(h => h.ts >= bucketStart && h.ts < bucketEnd);
  if (inBucket.length < 2) return { absChange: 0, direction: null };
  const first  = inBucket[0].price;
  const last   = inBucket[inBucket.length - 1].price;
  const change = last - first;
  return { absChange: Math.abs(change), direction: change > 0 ? 'UP' : 'DOWN' };
}

function isTrending(asset) {
  const nowSec     = Math.floor(Date.now() / 1000);
  const directions = [];
  for (let i = 0; i < config.trendBuckets; i++) {
    const bucketEnd   = nowSec - i * config.blockSize;
    const bucketStart = bucketEnd - config.blockSize;
    const inBucket    = priceHistory[asset].filter(h => h.ts >= bucketStart && h.ts < bucketEnd);
    if (inBucket.length < 2) continue;
    const first = inBucket[0].price;
    const last  = inBucket[inBucket.length - 1].price;
    directions.push(last > first ? 'UP' : 'DOWN');
  }
  if (directions.length < config.trendBuckets) return false;
  return directions.every(d => d === directions[0]);
}

// ── 4:50 exit — sell everything for this market ───────────────────────────────
function checkWindowExit() {
  const elapsed = windowElapsed();
  const cws     = currentWindowStart();
  if (elapsed < config.exitAtSecond) return;

  for (const marketId of Object.keys(MARKET_ASSETS)) {
    if (!config.markets[marketId]) continue;
    // Only fire once per window per market
    if (exitFiredWindow[marketId] === cws) continue;

    const openForMarket = state.openTrades.filter(t => t.marketId === marketId);
    if (!openForMarket.length) continue;

    exitFiredWindow[marketId] = cws;
    log(`⏰ [${marketId}] ${config.exitAtSecond}s exit — selling ${openForMarket.length} position(s)`);

    const closedNow = [];
    for (const t of openForMarket) {
      const tokenId  = t.side === 'UP' ? t.upToken : t.dnToken;
      const curPrice = getPrice(tokenId);
      if (curPrice <= 0) { log(`⚠️  [${marketId}] No price for [${t.id}] at exit`); continue; }
      const proceeds = +(curPrice * t.shares).toFixed(2);
      const pnl      = +(proceeds - t.cost).toFixed(4);
      state.balance   = addMoney(state.balance, proceeds);
      state.totalPnl  = +(state.totalPnl + pnl).toFixed(4);
      state.closedTrades.push({
        ...t, exitPrice: curPrice, proceeds, realizedPnl: pnl,
        closedAt: new Date().toISOString(), exitReason: 'WINDOW_EXIT',
      });
      closedNow.push({ t, curPrice, pnl });
    }
    const closedIds = new Set(closedNow.map(x => x.t.id));
    state.openTrades = state.openTrades.filter(t => !closedIds.has(t.id));
    // Reset last opened side for new window
    lastOpenedSide[marketId] = null;
    saveState();
    for (const { t, curPrice, pnl } of closedNow) {
      log(`${pnl >= 0 ? '🟢' : '🔴'} [${marketId}] EXIT@${config.exitAtSecond}s ${t.side} [${t.id}] entry=${t.entryPrice.toFixed(3)} exit=${curPrice.toFixed(3)} pnl=$${pnl.toFixed(2)} bal=$${state.balance.toFixed(2)}`);
    }
    emitFn('snapshot', buildDashboardSnapshot());
  }
}

// ── Signal check — opposite side only ────────────────────────────────────────
function checkSignals() {
  if (!botRunning) return;
  const elapsed = windowElapsed();
  // Don't open new positions after exit threshold
  if (elapsed >= config.exitAtSecond) return;

  for (const marketId of Object.keys(MARKET_ASSETS)) {
    if (!config.markets[marketId]) continue;
    const asset    = MARKET_ASSETS[marketId];
    const blockNum = currentBlockNumber();

    if (blockNum <= lastCheckedBlock[marketId]) continue;
    lastCheckedBlock[marketId] = blockNum;

    const cws      = currentWindowStart();
    const cacheKey = `${marketId}:${cws}`;
    const w        = marketCache[cacheKey];
    if (!w) continue;
    if (priceHistory[asset].length < 10) continue;

    const avg      = getAverageMove(asset);
    if (avg === 0) continue;

    const last     = getLastBlockMove(asset);
    const required = avg * config.moveMultiplier;

    if (!last.direction || last.absChange === 0) continue;
    if (last.absChange <= required) {
      log(`📊 [${marketId}] ${last.absChange.toFixed(5)} < need ${required.toFixed(5)} — skip`);
      continue;
    }
    if (isTrending(asset)) {
      log(`📊 [${marketId}] TRENDING — skip`);
      continue;
    }

    const newDirection = last.direction === 'UP' ? 'DOWN' : 'UP';
    const token        = newDirection === 'UP' ? w.upToken : w.dnToken;
    const tokenPrice   = getPrice(token);

    if (tokenPrice < config.tokenMin || tokenPrice > config.tokenMax) {
      log(`📊 [${marketId}] token=${tokenPrice.toFixed(3)} outside range — skip`);
      continue;
    }

    // ── Opposite side filter ──────────────────────────────────────────────────
    const lastSide = lastOpenedSide[marketId];
    if (lastSide !== null && lastSide === newDirection) {
      log(`📊 [${marketId}] SKIP — need opposite of ${lastSide}, got ${newDirection}`);
      continue;
    }

    log(`📊 [${marketId}] avg=${avg.toFixed(5)} last=${last.direction} ${last.absChange.toFixed(5)} | token=${tokenPrice.toFixed(3)} ✅ SIGNAL ${newDirection}`);

    // Step 1 — open new position
    const opened = placeTrade(marketId, w, cws, newDirection, last.absChange, avg, tokenPrice);

    // Step 2 — sell previous opposite position immediately
    if (opened) sellPreviousPosition(marketId, newDirection);
  }
}

function placeTrade(marketId, w, cws, direction, move, avg, tokenPrice) {
  const shares    = config.shares;
  const rawCost   = +(tokenPrice * shares).toFixed(2);
  const fee       = calcFee(shares, tokenPrice);
  const totalCost = +(rawCost + fee).toFixed(2);

  if (state.balance < totalCost) {
    log(`💸 [${marketId}] Low balance $${state.balance.toFixed(2)} need $${totalCost}`);
    return false;
  }

  const id = tradeId();
  state.balance   = subMoney(state.balance, totalCost);
  state.totalFees = +(state.totalFees + fee).toFixed(4);

  const trade = {
    id, marketId, windowStart: cws,
    asset: MARKET_ASSETS[marketId], side: direction,
    entryPrice: tokenPrice, shares, rawCost, fee, cost: totalCost,
    upToken: w.upToken, dnToken: w.dnToken,
    move: +move.toFixed(6), avg: +avg.toFixed(6),
    assetPriceAtEntry: +binancePrices[MARKET_ASSETS[marketId]].toFixed(6),
    openedAt: new Date().toISOString(), floatingPnl: 0,
    exitReason: null,
  };

  state.openTrades.push(trade);
  lastOpenedSide[marketId] = direction;

  const wstKey = `${marketId}:${cws}`;
  if (!windowState[wstKey]) windowState[wstKey] = { trades: 0 };
  windowState[wstKey].trades++;
  saveState();

  log(`🚀 [${marketId}] BUY ${direction} [${id}] token=${tokenPrice.toFixed(3)} shares=${shares} cost=$${rawCost} fee=$${fee} total=$${totalCost} | bal=$${state.balance.toFixed(2)}`);
  emitFn('snapshot', buildDashboardSnapshot());
  return true;
}

// ── Sell previous position when new opposite signal fires ─────────────────────
function sellPreviousPosition(marketId, newDirection) {
  const oppositeDir = newDirection === 'UP' ? 'DOWN' : 'UP';
  // Sell only the most recent opposite position — not all
  const toClose = state.openTrades.filter(
    t => t.marketId === marketId && t.side === oppositeDir
  );
  if (!toClose.length) return;

  const closedNow = [];
  for (const t of toClose) {
    const tokenId  = t.side === 'UP' ? t.upToken : t.dnToken;
    const curPrice = getPrice(tokenId);
    if (curPrice <= 0) { log(`⚠️  [${marketId}] No price for [${t.id}]`); continue; }
    const proceeds = +(curPrice * t.shares).toFixed(2);
    const pnl      = +(proceeds - t.cost).toFixed(4);
    state.balance   = addMoney(state.balance, proceeds);
    state.totalPnl  = +(state.totalPnl + pnl).toFixed(4);
    state.closedTrades.push({
      ...t, exitPrice: curPrice, proceeds, realizedPnl: pnl,
      closedAt: new Date().toISOString(), exitReason: 'FLIPPED',
    });
    closedNow.push({ t, curPrice, pnl });
  }

  const closedIds = new Set(closedNow.map(x => x.t.id));
  state.openTrades = state.openTrades.filter(t => !closedIds.has(t.id));
  saveState();

  for (const { t, curPrice, pnl } of closedNow) {
    log(`${pnl >= 0 ? '🟢' : '🔴'} [${marketId}] FLIPPED ${t.side}→${newDirection} [${t.id}] entry=${t.entryPrice.toFixed(3)} exit=${curPrice.toFixed(3)} pnl=$${pnl.toFixed(2)} bal=$${state.balance.toFixed(2)}`);
  }
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

async function findMarketForTs(slugBase, ts) {
  for (const offset of [0, 1, -1, 2, -2]) {
    const t    = ts + offset * WINDOW_SIZE;
    const slug = `${slugBase}-${t}`;
    const event = await getJson(`${GAMMA}/events/slug/${slug}`);
    if (event?.markets?.length) {
      const mkt    = event.markets.find(m => m.acceptingOrders !== false) ?? event.markets[0];
      const tokens = extractTokenIds(mkt);
      if (tokens) { seedFromMarket(mkt, tokens); return { ts: t, tokens, slug }; }
    }
    const mkt2 = await getJson(`${GAMMA}/markets/slug/${slug}`);
    if (mkt2) {
      const tokens = extractTokenIds(mkt2);
      if (tokens) { seedFromMarket(mkt2, tokens); return { ts: t, tokens, slug }; }
    }
  }
  return null;
}

let discovering = false;
async function refreshMarkets() {
  if (discovering) return;
  discovering = true;
  try {
    for (const [marketId, enabled] of Object.entries(config.markets)) {
      if (!enabled) continue;
      const cws      = currentWindowStart();
      const cacheKey = `${marketId}:${cws}`;
      if (marketCache[cacheKey]) continue;
      const slugBase = MARKET_SLUGS[marketId];
      const res = await findMarketForTs(slugBase, cws);
      if (res) {
        marketCache[cacheKey] = {
          marketId, windowStart: cws,
          upToken: res.tokens.upToken, dnToken: res.tokens.dnToken,
          slug: res.slug,
        };
        log(`✅ [${marketId}] Found ts=${res.ts} | ${res.slug}`);
      }
    }
  } finally { discovering = false; }
}

async function pollPrices() {
  const tokens = new Set();
  for (const [marketId, enabled] of Object.entries(config.markets)) {
    if (!enabled) continue;
    const cws = currentWindowStart();
    const w   = marketCache[`${marketId}:${cws}`];
    if (w) { tokens.add(w.upToken); tokens.add(w.dnToken); }
  }
  for (const t of state.openTrades) {
    if (t.upToken) tokens.add(t.upToken);
    if (t.dnToken) tokens.add(t.dnToken);
  }
  await Promise.all([...tokens].map(async tid => {
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

function updateFloating() {
  for (const t of state.openTrades) {
    if (!t.upToken || !t.dnToken) continue;
    const tokenId = t.side === 'UP' ? t.upToken : t.dnToken;
    const p = getPrice(tokenId);
    if (p > 0) t.floatingPnl = +((p - t.entryPrice) * t.shares).toFixed(4);
  }
}

function priceDec(asset) {
  if (asset === 'DOGE') return 5;
  if (asset === 'SOL')  return 3;
  return 2;
}

function buildSignalSnapshot(marketId) {
  const asset    = MARKET_ASSETS[marketId];
  const cws      = currentWindowStart();
  const cacheKey = `${marketId}:${cws}`;
  const w        = marketCache[cacheKey];
  const wst      = windowState[cacheKey] || {};
  const nowSec   = Math.floor(Date.now() / 1000);
  const elapsed  = nowSec - cws;
  const avg      = getAverageMove(asset);
  const last     = getLastBlockMove(asset);
  const trending = isTrending(asset);
  const required = avg * config.moveMultiplier;
  const upPrice  = w ? getPrice(w.upToken) : 0;
  const dnPrice  = w ? getPrice(w.dnToken) : 0;
  const blockNum = currentBlockNumber();
  const dec      = priceDec(asset);
  const openForMarket = state.openTrades.filter(t => t.marketId === marketId);
  return {
    marketId, asset,
    windowStart:  cws,
    elapsed,
    remaining:    Math.max(0, WINDOW_SIZE - elapsed),
    blockElapsed: nowSec - (blockNum * config.blockSize),
    dec,
    assetPrice:   +binancePrices[asset].toFixed(dec),
    upPrice:      +upPrice.toFixed(3),
    dnPrice:      +dnPrice.toFixed(3),
    avg:          +avg.toFixed(dec),
    required:     +required.toFixed(dec),
    lastMove:     +(last.absChange||0).toFixed(dec),
    lastDir:      last.direction,
    reversedDir:  last.direction ? (last.direction === 'UP' ? 'DOWN' : 'UP') : null,
    isSignal:     last.absChange > required && avg > 0 && !trending &&
                  elapsed < config.exitAtSecond &&
                  lastOpenedSide[marketId] !== (last.direction === 'UP' ? 'DOWN' : 'UP'),
    trending,
    lastOpenedSide: lastOpenedSide[marketId],
    historyCount: priceHistory[asset].length,
    tradesThisWindow: wst.trades || 0,
    upCount:  openForMarket.filter(t => t.side === 'UP').length,
    dnCount:  openForMarket.filter(t => t.side === 'DOWN').length,
    enabled:  !!config.markets[marketId],
    exitMode: elapsed >= config.exitAtSecond,
  };
}

function buildDashboardSnapshot() {
  return {
    balance:    +state.balance.toFixed(2),
    totalPnl:   +state.totalPnl.toFixed(2),
    totalFees:  +state.totalFees.toFixed(2),
    openTrades: state.openTrades,
    closedTrades: state.closedTrades.slice(-80),
    botRunning,
    config,
    markets: {
      'btc-5m':  buildSignalSnapshot('btc-5m'),
      'eth-5m':  buildSignalSnapshot('eth-5m'),
      'sol-5m':  buildSignalSnapshot('sol-5m'),
      'doge-5m': buildSignalSnapshot('doge-5m'),
    },
  };
}

function prune() {
  const nowSec = Math.floor(Date.now() / 1000);
  for (const key of Object.keys(marketCache)) {
    const [, tsStr] = key.split(':');
    if (Number(tsStr) < nowSec - WINDOW_SIZE * 3) delete marketCache[key];
  }
}

let timer = null;
async function tick() {
  try {
    prune();
    await refreshMarkets();
    await pollPrices();
    updateFloating();
    if (botRunning) {
      checkWindowExit();
      checkSignals();
    }
    emitFn('snapshot', buildDashboardSnapshot());
  } catch (e) { log(`⚠️  tick: ${e.message}`); }
}

// ── Config update from dashboard ──────────────────────────────────────────────
function updateConfig(newCfg) {
  const prev = { ...config };
  if (newCfg.shares         !== undefined) config.shares         = Math.max(1,   Math.min(1000, Number(newCfg.shares)));
  if (newCfg.moveMultiplier !== undefined) config.moveMultiplier = Math.max(0.1, Math.min(5,    Number(newCfg.moveMultiplier)));
  if (newCfg.blockSize      !== undefined) config.blockSize      = Math.max(5,   Math.min(300,  Number(newCfg.blockSize)));
  if (newCfg.tokenMin       !== undefined) config.tokenMin       = Math.max(0.01,Math.min(0.49, Number(newCfg.tokenMin)));
  if (newCfg.tokenMax       !== undefined) config.tokenMax       = Math.max(0.51,Math.min(0.99, Number(newCfg.tokenMax)));
  if (newCfg.trendBuckets   !== undefined) config.trendBuckets   = Math.max(1,   Math.min(10,   Number(newCfg.trendBuckets)));
  if (newCfg.exitAtSecond   !== undefined) config.exitAtSecond   = Math.max(60,  Math.min(299,  Number(newCfg.exitAtSecond)));
  if (newCfg.historyWindow  !== undefined) config.historyWindow  = Math.max(300, Math.min(3600, Number(newCfg.historyWindow)));
  if (newCfg.markets        !== undefined) config.markets        = { ...config.markets, ...newCfg.markets };
  saveConfig();
  log(`⚙️  Config updated: shares=${config.shares} mult=${config.moveMultiplier} block=${config.blockSize}s exit=${config.exitAtSecond}s`);
  emitFn('snapshot', buildDashboardSnapshot());
}

async function start(emit, logEmit) {
  emitFn = emit; logFn = logEmit;
  loadConfig();
  loadState();
  log('🚀 HYDRA — BTC+ETH+SOL+DOGE 5m | opposite-only | sell-on-flip | window exit');
  log(`   Config: shares=${config.shares} mult=${config.moveMultiplier} block=${config.blockSize}s exit=${config.exitAtSecond}s`);
  for (const asset of Object.keys(BINANCE_STREAMS)) connectBinance(asset);
  await tick();
  timer = setInterval(tick, 1000);
  setInterval(async function() {
    await pollPrices();
    updateFloating();
    emitFn('prices', {
      btcPrice:  +binancePrices.BTC.toFixed(2),
      ethPrice:  +binancePrices.ETH.toFixed(2),
      solPrice:  +binancePrices.SOL.toFixed(3),
      dogePrice: +binancePrices.DOGE.toFixed(5),
    });
  }, 2000);
  log(`💰 Balance: $${state.balance.toFixed(2)}`);
}

function setBotRunning(val) {
  botRunning = val;
  log(val ? '▶️  Bot STARTED' : '⏹️  Bot STOPPED');
  emitFn('snapshot', buildDashboardSnapshot());
}

function stop() {
  clearInterval(timer);
  for (const asset of Object.keys(binanceWs)) {
    if (binanceWs[asset]) { try { binanceWs[asset].terminate(); } catch(_){} }
  }
}

module.exports = { start, stop, buildDashboardSnapshot, updateConfig, setBotRunning };
