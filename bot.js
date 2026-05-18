'use strict';

const fetch     = require('node-fetch');
const WebSocket = require('ws');
const fs        = require('fs');
const path      = require('path');

const GAMMA      = 'https://gamma-api.polymarket.com';
const CLOB_REST  = 'https://clob.polymarket.com';
const TRADES_FILE = path.join(__dirname, 'trades.json');

const BINANCE_BTC_WS = 'wss://stream.binance.com:9443/ws/btcusdt@aggTrade';
const BINANCE_ETH_WS = 'wss://stream.binance.com:9443/ws/ethusdt@aggTrade';

const MARKETS = {
  'btc-5m':  { asset: 'BTC', slug: 'btc-updown-5m',  windowSize: 300,  blockSize: 30,  historyWindow: 900,  shares: 50 },
  'eth-5m':  { asset: 'ETH', slug: 'eth-updown-5m',  windowSize: 300,  blockSize: 30,  historyWindow: 900,  shares: 50 },
  'btc-15m': { asset: 'BTC', slug: 'btc-updown-15m', windowSize: 900,  blockSize: 90,  historyWindow: 2700, shares: 50 },
  'eth-15m': { asset: 'ETH', slug: 'eth-updown-15m', windowSize: 900,  blockSize: 90,  historyWindow: 2700, shares: 50 },
};

const TOKEN_MIN        = 0.10;
const TOKEN_MAX        = 0.90;
const MOVE_MULTIPLIER  = 0.5;
const TREND_BUCKETS    = 3;
const STARTING_BALANCE = 2000;
const STOP_LOSS        = 0.03;
const TAKE_PROFIT      = 0.99;
const CRYPTO_FEE_RATE  = 0.018;
const LAST_MOVE_WINDOW = { 30: 20, 90: 60 };

let state = {
  balance:      STARTING_BALANCE,
  openTrades:   [],
  closedTrades: [],
  totalPnl:     0,
  pnl5m:        0,
  pnl15m:       0,
  totalFees:    0,
};

const priceBook    = {};
const marketCache  = {};
const windowState  = {};
const firedBlocks  = {};

const priceHistory   = { BTC: [], ETH: [] };
const binancePrices  = { BTC: 0, ETH: 0 };
const binanceLastLog = { BTC: 0, ETH: 0 };
const binanceLastPx  = { BTC: 0, ETH: 0 };

let emitFn = () => {};
let logFn  = () => {};

function calcFee(shares, price) {
  return +(shares * CRYPTO_FEE_RATE * price * (1 - price)).toFixed(4);
}

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

function currentWindowStart(windowSize) {
  return Math.floor(Math.floor(Date.now() / 1000) / windowSize) * windowSize;
}
function currentBlockNumber(blockSize) {
  return Math.floor(Math.floor(Date.now() / 1000) / blockSize);
}

function getPrice(tid) {
  const b = priceBook[tid];
  if (!b) return 0;
  if (b.bid > 0 && b.ask > 0) return (b.bid + b.ask) / 2;
  return b.bid || b.ask || 0;
}

function tradeId() { return `T${Date.now().toString(36).toUpperCase()}`; }

// ── Binance WebSocket ─────────────────────────────────────────────────────────
let btcWs = null, ethWs = null;

function connectBinance(asset, url) {
  let wsRef = asset === 'BTC' ? btcWs : ethWs;
  if (wsRef) { try { wsRef.terminate(); } catch(_){} }
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
      priceHistory[asset] = priceHistory[asset].filter(h => h.ts >= nowSec - 2700);
      const threshold = asset === 'BTC' ? 1 : 0.5;
      if (Math.abs(price - binanceLastPx[asset]) >= threshold && nowSec - binanceLastLog[asset] >= 5) {
        const change = price - binanceLastPx[asset];
        log(`💹 Binance ${asset} = $${price.toFixed(2)} (${change >= 0 ? '+' : ''}$${change.toFixed(2)})`);
        binanceLastPx[asset]  = price;
        binanceLastLog[asset] = nowSec;
      }
      checkSignalsForAsset(asset);
    } catch (_) {}
  });
  ws.on('close', () => {
    log(`⚡ Binance ${asset} WS closed — reconnecting in 3s…`);
    setTimeout(() => connectBinance(asset, url), 3000);
  });
  ws.on('error', e => log(`⚠️  Binance ${asset}: ${e.message}`));
  if (asset === 'BTC') btcWs = ws;
  else ethWs = ws;
}

// ── Signal logic ──────────────────────────────────────────────────────────────
function getAverageMove(asset, blockSize, historyWindow) {
  const nowSec     = Math.floor(Date.now() / 1000);
  const hist       = priceHistory[asset].filter(h => h.ts >= nowSec - historyWindow);
  const numBuckets = Math.floor(historyWindow / blockSize);
  const moves      = [];
  for (let i = 0; i < numBuckets; i++) {
    const bucketEnd   = nowSec - i * blockSize;
    const bucketStart = bucketEnd - blockSize;
    const inBucket    = hist.filter(h => h.ts >= bucketStart && h.ts < bucketEnd);
    if (inBucket.length < 2) continue;
    const prices = inBucket.map(h => h.price);
    const move   = Math.abs(Math.max(...prices) - Math.min(...prices));
    if (move > 0) moves.push(move);
  }
  if (!moves.length) return 0;
  return moves.reduce((s, m) => s + m, 0) / moves.length;
}

function getRecentMove(asset, seconds) {
  const nowSec   = Math.floor(Date.now() / 1000);
  const inWindow = priceHistory[asset].filter(h => h.ts >= nowSec - seconds);
  if (inWindow.length < 2) return { change: 0, absChange: 0, direction: null };
  const first  = inWindow[0].price;
  const last   = inWindow[inWindow.length - 1].price;
  const change = last - first;
  return { change, absChange: Math.abs(change), direction: change > 0 ? 'UP' : 'DOWN' };
}

function isTrending(asset, blockSize) {
  const nowSec     = Math.floor(Date.now() / 1000);
  const directions = [];
  for (let i = 0; i < TREND_BUCKETS; i++) {
    const bucketEnd   = nowSec - i * blockSize;
    const bucketStart = bucketEnd - blockSize;
    const inBucket    = priceHistory[asset].filter(h => h.ts >= bucketStart && h.ts < bucketEnd);
    if (inBucket.length < 2) continue;
    const first = inBucket[0].price;
    const last  = inBucket[inBucket.length - 1].price;
    directions.push(last > first ? 'UP' : 'DOWN');
  }
  if (directions.length < TREND_BUCKETS) return false;
  return directions.every(d => d === directions[0]);
}

function checkSignalsForAsset(asset) {
  for (const [marketId, cfg] of Object.entries(MARKETS)) {
    if (cfg.asset !== asset) continue;

    const cws      = currentWindowStart(cfg.windowSize);
    const cacheKey = `${marketId}:${cws}`;
    const w        = marketCache[cacheKey];
    if (!w) continue;
    if (priceHistory[asset].length < 10) continue;

    const blockNum = currentBlockNumber(cfg.blockSize);
    const blockKey = `${marketId}:${blockNum}`;

    // Hard stop — already fired this block
    if (firedBlocks[blockKey]) continue;

    // Evaluate ALL conditions before touching firedBlocks
    const avg = getAverageMove(cfg.asset, cfg.blockSize, cfg.historyWindow);
    if (avg === 0) continue;

    const lookback = LAST_MOVE_WINDOW[cfg.blockSize] || Math.floor(cfg.blockSize * 0.66);
    const last     = getRecentMove(cfg.asset, lookback);
    const required = avg * MOVE_MULTIPLIER;

    if (!last.direction || last.absChange === 0) continue;
    if (last.absChange <= required) continue;

    if (isTrending(cfg.asset, cfg.blockSize)) {
      const trendKey = `${blockKey}:trendlog`;
      if (!firedBlocks[trendKey]) {
        log(`📊 [${marketId}] TRENDING — skip`);
        firedBlocks[trendKey] = true;
      }
      continue;
    }

    const reversed   = last.direction === 'UP' ? 'DOWN' : 'UP';
    const token      = reversed === 'UP' ? w.upToken : w.dnToken;
    const tokenPrice = getPrice(token);

    if (tokenPrice < TOKEN_MIN || tokenPrice > TOKEN_MAX) continue;

    // All conditions passed — claim block NOW
    // Double check in case another message slipped through
    if (firedBlocks[blockKey]) continue;
    firedBlocks[blockKey] = true;

    const nowSec  = Math.floor(Date.now() / 1000);
    const elapsed = nowSec - (blockNum * cfg.blockSize);
    log(`📊 [${marketId}] ${last.direction} $${last.absChange.toFixed(2)} > 0.5x=$${required.toFixed(2)} | token=${tokenPrice.toFixed(3)} | ${elapsed}s into block ✅`);
    placeTrade(marketId, cfg, w, cws, reversed, last.absChange, avg, required, tokenPrice, elapsed);
  }
}

function placeTrade(marketId, cfg, w, cws, direction, move, avg, required, tokenPrice, elapsedInBlock) {
  const rawCost   = +(tokenPrice * cfg.shares).toFixed(2);
  const fee       = calcFee(cfg.shares, tokenPrice);
  const totalCost = +(rawCost + fee).toFixed(2);
  if (state.balance < totalCost) { log(`💸 Low balance for ${marketId}`); return; }
  const id    = tradeId();
  const is15m = cfg.windowSize === 900;
  state.balance   -= totalCost;
  state.totalFees += fee;
  const trade = {
    id, marketId, windowStart: cws, windowSize: cfg.windowSize,
    asset: cfg.asset, side: direction, type: 'REVERSION',
    entryPrice: tokenPrice, shares: cfg.shares,
    rawCost, fee, cost: totalCost,
    sl: STOP_LOSS, tp: TAKE_PROFIT,
    upToken: w.upToken,
    dnToken: w.dnToken,
    btcMove: +move.toFixed(2), avgMove: +avg.toFixed(2),
    required: +required.toFixed(2),
    assetPriceAtEntry: +binancePrices[cfg.asset].toFixed(2),
    elapsedInBlock: elapsedInBlock || 0,
    openedAt: new Date().toISOString(), floatingPnl: 0,
    timeframe: is15m ? '15m' : '5m',
    exitReason: null,
  };
  state.openTrades.push(trade);
  const wstKey = `${marketId}:${cws}`;
  if (!windowState[wstKey]) windowState[wstKey] = { trades: 0 };
  windowState[wstKey].trades++;
  saveState();
  log(`🚀 [${marketId}] REVERSION ${direction} [${id}] token=${tokenPrice.toFixed(3)} shares=${cfg.shares} cost=$${rawCost} fee=$${fee} total=$${totalCost} | SL=${STOP_LOSS} TP=${TAKE_PROFIT} | ${elapsedInBlock}s into block | bal=$${state.balance.toFixed(2)}`);
  emitFn('snapshot', buildDashboardSnapshot());
}

// ── TP/SL ─────────────────────────────────────────────────────────────────────
function checkTPSL() {
  const toClose = [];
  for (const t of state.openTrades) {
    if (!t.upToken || !t.dnToken) continue;
    const tokenId  = t.side === 'UP' ? t.upToken : t.dnToken;
    const curPrice = getPrice(tokenId);
    if (curPrice <= 0) continue;
    t.floatingPnl = +((curPrice - t.entryPrice) * t.shares).toFixed(4);
    if      (curPrice >= TAKE_PROFIT) toClose.push({ trade: t, exitPrice: curPrice, reason: 'TP' });
    else if (curPrice <= STOP_LOSS)   toClose.push({ trade: t, exitPrice: curPrice, reason: 'SL' });
  }
  for (const { trade: t, exitPrice, reason } of toClose) {
    const proceeds  = +(exitPrice * t.shares).toFixed(2);
    const pnl       = +(proceeds - t.cost).toFixed(4);
    state.balance  += proceeds;
    state.totalPnl += pnl;
    if (t.timeframe === '15m') state.pnl15m += pnl;
    else                       state.pnl5m  += pnl;
    state.openTrades = state.openTrades.filter(x => x.id !== t.id);
    state.closedTrades.push({
      ...t, exitPrice, proceeds, realizedPnl: pnl,
      closedAt: new Date().toISOString(), exitReason: reason,
    });
    saveState();
    log(`${pnl >= 0 ? '🟢' : '🔴'} [${t.marketId}] ${reason} ${t.side} [${t.id}] entry=${t.entryPrice.toFixed(3)} exit=${exitPrice.toFixed(3)} fee=$${t.fee} pnl=$${pnl.toFixed(2)} bal=$${state.balance.toFixed(2)}`);
    emitFn('snapshot', buildDashboardSnapshot());
  }
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

async function findMarketForTs(slugBase, ts, windowSize) {
  for (const offset of [0, 1, -1, 2, -2]) {
    const t    = ts + offset * windowSize;
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
    for (const [marketId, cfg] of Object.entries(MARKETS)) {
      const cws      = currentWindowStart(cfg.windowSize);
      const cacheKey = `${marketId}:${cws}`;
      if (marketCache[cacheKey]) continue;
      const res = await findMarketForTs(cfg.slug, cws, cfg.windowSize);
      if (res) {
        marketCache[cacheKey] = {
          marketId, windowStart: cws, windowSize: cfg.windowSize,
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
  for (const [marketId, cfg] of Object.entries(MARKETS)) {
    const cws = currentWindowStart(cfg.windowSize);
    const w   = marketCache[`${marketId}:${cws}`];
    if (w) { tokens.add(w.upToken); tokens.add(w.dnToken); }
  }
  // Always poll open trade tokens regardless of window
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

// ── Resolution — uses token IDs from trades, not marketCache ─────────────────
async function checkResolution() {
  const nowSec = Math.floor(Date.now() / 1000);
  for (const [key, wst] of Object.entries(windowState)) {
    if (wst.resolved) continue;
    const [marketId, tsStr] = key.split(':');
    const ts  = Number(tsStr);
    const cfg = MARKETS[marketId];
    if (!cfg) continue;
    if (nowSec < ts + cfg.windowSize + 30) continue;

    const tradesInWindow = state.openTrades.filter(t => t.marketId === marketId && t.windowStart === ts);
    if (!tradesInWindow.length) { wst.resolved = true; continue; }

    log(`⏰ [${marketId}] Resolving ts=${ts} (${tradesInWindow.length} trades)…`);

    // Collect all token IDs directly from trades
    const allTokens = new Set();
    for (const t of tradesInWindow) {
      if (t.upToken) allTokens.add(t.upToken);
      if (t.dnToken) allTokens.add(t.dnToken);
    }

    // Fetch resolution prices
    const resolvedPrices = {};
    await Promise.all([...allTokens].map(async tid => {
      try {
        const r = await fetch(`${CLOB_REST}/price?token_id=${tid}&side=BUY`, { timeout: 4000 });
        const p = parseFloat((await r.json()).price ?? 0) || 0;
        if (p > 0) resolvedPrices[tid] = p;
      } catch (_) {}
    }));

    const sample = tradesInWindow[0];
    log(`   [${marketId}] UP=${resolvedPrices[sample?.upToken]?.toFixed(3)||'?'} DN=${resolvedPrices[sample?.dnToken]?.toFixed(3)||'?'}`);

    let windowPnl = 0;
    for (const t of tradesInWindow) {
      const tokenId = t.side === 'UP' ? t.upToken : t.dnToken;
      const rp      = resolvedPrices[tokenId];
      if (!rp || rp <= 0) {
        log(`⚠️  [${marketId}] No price for [${t.id}] — skipping`);
        continue;
      }
      const pro = +(rp * t.shares).toFixed(2);
      const pnl = +(pro - t.cost).toFixed(4);
      windowPnl      += pnl;
      state.balance  += pro;
      state.totalPnl += pnl;
      if (t.timeframe === '15m') state.pnl15m += pnl;
      else                       state.pnl5m  += pnl;
      state.closedTrades.push({
        ...t, exitPrice: rp, proceeds: pro, realizedPnl: pnl,
        closedAt: new Date().toISOString(), exitReason: 'RESOLVED',
      });
      log(`${pnl >= 0 ? '🟢' : '🔴'} [${marketId}] RESOLVED ${t.side} [${t.id}] entry=${t.entryPrice.toFixed(3)} exit=${rp.toFixed(3)} fee=$${t.fee} pnl=$${pnl.toFixed(2)}`);
    }
    state.openTrades = state.openTrades.filter(t => !(t.marketId === marketId && t.windowStart === ts));
    wst.resolved = true;
    delete marketCache[key];
    saveState();
    log(`📊 [${marketId}] SUMMARY trades=${wst.trades||0} windowPnl=$${windowPnl.toFixed(2)} bal=$${state.balance.toFixed(2)}`);
  }
}

function updateFloating() {
  for (const t of state.openTrades) {
    if (!t.upToken || !t.dnToken) continue;
    const tokenId = t.side === 'UP' ? t.upToken : t.dnToken;
    const p = getPrice(tokenId);
    if (p > 0) t.floatingPnl = +((p - t.entryPrice) * t.shares).toFixed(4);
  }
}

function buildSignalSnapshot(marketId) {
  const cfg      = MARKETS[marketId];
  const cws      = currentWindowStart(cfg.windowSize);
  const cacheKey = `${marketId}:${cws}`;
  const w        = marketCache[cacheKey];
  const wst      = windowState[cacheKey] || {};
  const nowSec   = Math.floor(Date.now() / 1000);
  const avg      = getAverageMove(cfg.asset, cfg.blockSize, cfg.historyWindow);
  const lookback = LAST_MOVE_WINDOW[cfg.blockSize] || Math.floor(cfg.blockSize * 0.66);
  const last     = getRecentMove(cfg.asset, lookback);
  const trending = isTrending(cfg.asset, cfg.blockSize);
  const required = avg * MOVE_MULTIPLIER;
  const upPrice  = w ? getPrice(w.upToken) : 0;
  const dnPrice  = w ? getPrice(w.dnToken) : 0;
  const blockNum = currentBlockNumber(cfg.blockSize);
  const blockKey = `${marketId}:${blockNum}`;
  return {
    marketId,
    asset:        cfg.asset,
    timeframe:    cfg.windowSize === 900 ? '15m' : '5m',
    windowStart:  cws,
    elapsed:      nowSec - cws,
    remaining:    Math.max(0, cfg.windowSize - (nowSec - cws)),
    windowSize:   cfg.windowSize,
    blockSize:    cfg.blockSize,
    blockElapsed: nowSec - (blockNum * cfg.blockSize),
    blockFired:   !!firedBlocks[blockKey],
    assetPrice:   +binancePrices[cfg.asset].toFixed(2),
    upPrice:      +upPrice.toFixed(3),
    dnPrice:      +dnPrice.toFixed(3),
    avg:          +avg.toFixed(2),
    required:     +required.toFixed(2),
    lastMove:     +(last.absChange||0).toFixed(2),
    lastDir:      last.direction,
    reversedDir:  last.direction ? (last.direction === 'UP' ? 'DOWN' : 'UP') : null,
    isSignal:     last.absChange > required && avg > 0 && !trending && !firedBlocks[blockKey],
    trending,
    historyCount: priceHistory[cfg.asset].length,
    tradesThisWindow: wst.trades || 0,
  };
}

function buildDashboardSnapshot() {
  return {
    balance:      +state.balance.toFixed(2),
    totalPnl:     +state.totalPnl.toFixed(2),
    pnl5m:        +state.pnl5m.toFixed(2),
    pnl15m:       +state.pnl15m.toFixed(2),
    totalFees:    +state.totalFees.toFixed(4),
    openTrades:   state.openTrades,
    closedTrades: state.closedTrades.slice(-60),
    markets: {
      'btc-5m':  buildSignalSnapshot('btc-5m'),
      'eth-5m':  buildSignalSnapshot('eth-5m'),
      'btc-15m': buildSignalSnapshot('btc-15m'),
      'eth-15m': buildSignalSnapshot('eth-15m'),
    },
  };
}

function prune() {
  const nowSec = Math.floor(Date.now() / 1000);
  for (const key of Object.keys(marketCache)) {
    const [marketId, tsStr] = key.split(':');
    const cfg = MARKETS[marketId];
    if (cfg && Number(tsStr) < nowSec - cfg.windowSize * 3) delete marketCache[key];
  }
  // Prune fired blocks older than 4 hours
  const cutoff = Math.floor(nowSec / 30) - 480;
  for (const key of Object.keys(firedBlocks)) {
    const parts    = key.split(':');
    const blockNum = Number(parts[parts.length - 1]);
    if (!isNaN(blockNum) && blockNum < cutoff) delete firedBlocks[key];
  }
}

let timer = null;
async function tick() {
  try {
    prune();
    await refreshMarkets();
    await pollPrices();
    updateFloating();
    checkTPSL();
    await checkResolution();
    emitFn('snapshot', buildDashboardSnapshot());
  } catch (e) { log(`⚠️  tick: ${e.message}`); }
}

async function start(emit, logEmit) {
  emitFn = emit; logFn = logEmit;
  log('🚀 Multi-Market Reversion Bot — single fire per block | fees + SL/TP');
  log(`   SL=${STOP_LOSS} | TP=${TAKE_PROFIT} | fee=1.8%×p×(1-p) | balance=$${STARTING_BALANCE}`);
  log('   5m: 30s blocks | 15min history | 50 shares');
  log('   15m: 90s blocks | 45min history | 50 shares');
  loadState();
  connectBinance('BTC', BINANCE_BTC_WS);
  connectBinance('ETH', BINANCE_ETH_WS);
  await tick();
  timer = setInterval(tick, 5000);
  setInterval(async function() {
    await pollPrices();
    updateFloating();
    checkTPSL();
    emitFn('prices', {
      btcPrice: +binancePrices.BTC.toFixed(2),
      ethPrice: +binancePrices.ETH.toFixed(2),
    });
  }, 2000);
  log(`💰 Balance: $${state.balance.toFixed(2)}`);
}

function stop() {
  clearInterval(timer);
  if (btcWs) { try { btcWs.terminate(); } catch(_){} }
  if (ethWs) { try { ethWs.terminate(); } catch(_){} }
}

module.exports = { start, stop, buildDashboardSnapshot };
