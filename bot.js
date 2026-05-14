'use strict';

const fetch = require('node-fetch');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');

// ─── Constants ────────────────────────────────────────────────────────────────
const GAMMA_API = 'https://gamma-api.polymarket.com';
const CLOB_WS   = 'wss://ws-subscriptions-clob.polymarket.com/ws/market';
const TRADES_FILE = path.join(__dirname, 'trades.json');

const WINDOW_SIZES = { '15m': 900, '4h': 14400 };
const ENTRY_THRESHOLD = -0.20;   // ask-based gap must be <= this
const EXIT_THRESHOLD  =  0.05;   // bid-based gap must be >= this
const SHARES = 50;
const STARTING_BALANCE = 1000;

// ─── State ────────────────────────────────────────────────────────────────────
let state = {
  balance: STARTING_BALANCE,
  openTrades: [],   // { id, tf, type, btcToken, ethToken, btcAsk, ethAsk, entryCost, shares, openedAt }
  closedTrades: [],
  totalPnl: 0,
};

// Live price book: tokenId → { bid, ask }
const priceBook = {};

// Market cache: tf → windowStart → { btcUp, btcDn, ethUp, ethDn }
//   each entry: { tokenId, conditionId, slug }
const marketCache = {};

let emitFn   = () => {};  // injected by server
let logFn    = () => {};  // injected by server

// ─── Persistence ──────────────────────────────────────────────────────────────
function loadState() {
  try {
    if (fs.existsSync(TRADES_FILE)) {
      const raw = JSON.parse(fs.readFileSync(TRADES_FILE, 'utf8'));
      state.balance     = raw.balance     ?? STARTING_BALANCE;
      state.openTrades  = raw.openTrades  ?? [];
      state.closedTrades= raw.closedTrades?? [];
      state.totalPnl    = raw.totalPnl    ?? 0;

      // Refund open trades on restart (demo safety net)
      if (state.openTrades.length > 0) {
        log(`♻️  Refunding ${state.openTrades.length} open trade(s) from previous session`);
        for (const t of state.openTrades) {
          state.balance += t.entryCost;
          log(`  ↩ Refunded $${t.entryCost.toFixed(2)} for trade ${t.id}`);
        }
        state.openTrades = [];
        saveState();
      }
    }
  } catch (e) {
    log(`⚠️  State load error: ${e.message} — starting fresh`);
  }
}

function saveState() {
  fs.writeFileSync(TRADES_FILE, JSON.stringify(state, null, 2));
}

// ─── Logging ──────────────────────────────────────────────────────────────────
function log(msg) {
  const ts = new Date().toISOString().replace('T', ' ').slice(0, 19);
  const line = `[${ts}] ${msg}`;
  console.log(line);
  logFn(line);
}

// ─── Slug / Window Helpers ────────────────────────────────────────────────────
function currentWindowStart(tf) {
  const size = WINDOW_SIZES[tf];
  const now  = Math.floor(Date.now() / 1000);
  return Math.floor(now / size) * size;
}

function slug(asset, tf, windowStart) {
  // e.g. btc-updown-15m-1716825600
  return `${asset.toLowerCase()}-updown-${tf}-${windowStart}`;
}

// ─── Gamma API ────────────────────────────────────────────────────────────────
async function fetchMarket(slugStr) {
  try {
    const url = `${GAMMA_API}/markets?slug=${slugStr}`;
    const res = await fetch(url, { timeout: 8000 });
    if (!res.ok) return null;
    const data = await res.json();
    const markets = Array.isArray(data) ? data : data.markets ?? [];
    return markets[0] ?? null;
  } catch (e) {
    log(`⚠️  Gamma fetch error (${slugStr}): ${e.message}`);
    return null;
  }
}

// Returns { upToken, dnToken } from a market object
function extractTokens(market) {
  if (!market || !Array.isArray(market.tokens)) return null;
  const up = market.tokens.find(t => /up/i.test(t.outcome));
  const dn = market.tokens.find(t => /down|dn/i.test(t.outcome));
  if (!up || !dn) {
    // fallback: index 0 = Yes/Up, index 1 = No/Down
    return { upToken: market.tokens[0]?.token_id, dnToken: market.tokens[1]?.token_id };
  }
  return { upToken: up.token_id, dnToken: dn.token_id };
}

// ─── WebSocket Price Feed ─────────────────────────────────────────────────────
let ws = null;
let wsReady = false;
const pendingSubscriptions = new Set();

function connectWebSocket() {
  log('🔌 Connecting to Polymarket CLOB WebSocket…');
  ws = new WebSocket(CLOB_WS);

  ws.on('open', () => {
    wsReady = true;
    log('✅ WebSocket connected');
    // Re-subscribe any tokens already known
    for (const tokenId of pendingSubscriptions) {
      subscribeToken(tokenId);
    }
    pendingSubscriptions.clear();
  });

  ws.on('message', (raw) => {
    try {
      const msgs = JSON.parse(raw);
      const arr = Array.isArray(msgs) ? msgs : [msgs];
      for (const msg of arr) {
        handleWsMessage(msg);
      }
    } catch (_) {}
  });

  ws.on('close', () => {
    wsReady = false;
    log('⚡ WebSocket closed — reconnecting in 5s');
    setTimeout(connectWebSocket, 5000);
  });

  ws.on('error', (e) => {
    log(`⚠️  WebSocket error: ${e.message}`);
  });
}

function subscribeToken(tokenId) {
  if (!tokenId) return;
  if (!wsReady || !ws || ws.readyState !== WebSocket.OPEN) {
    pendingSubscriptions.add(tokenId);
    return;
  }
  ws.send(JSON.stringify({
    assets_ids: [tokenId],
    type: 'market'
  }));
}

function handleWsMessage(msg) {
  // Price update: { asset_id, bid, ask, ...}  or event_type: price_change
  const tokenId = msg.asset_id ?? msg.token_id ?? msg.market;
  if (!tokenId) return;

  const bid = parseFloat(msg.bid ?? msg.best_bid ?? 0) || 0;
  const ask = parseFloat(msg.ask ?? msg.best_ask ?? 0) || 0;

  if (bid > 0 || ask > 0) {
    priceBook[tokenId] = { bid, ask };
  }
}

// ─── Market Discovery & Subscription ─────────────────────────────────────────
async function refreshMarkets() {
  for (const tf of Object.keys(WINDOW_SIZES)) {
    const ws_ = currentWindowStart(tf);
    const cacheKey = `${tf}-${ws_}`;
    if (marketCache[cacheKey]) continue;  // already fetched this window

    log(`🔍 Fetching markets for ${tf} window ${ws_}…`);

    const [btcMkt, ethMkt] = await Promise.all([
      fetchMarket(slug('btc', tf, ws_)),
      fetchMarket(slug('eth', tf, ws_)),
    ]);

    const btcTokens = extractTokens(btcMkt);
    const ethTokens = extractTokens(ethMkt);

    if (!btcTokens || !ethTokens) {
      log(`⚠️  Could not extract tokens for ${tf} window ${ws_} — will retry`);
      continue;
    }

    marketCache[cacheKey] = {
      windowStart: ws_,
      tf,
      btcUp: btcTokens.upToken,
      btcDn: btcTokens.dnToken,
      ethUp: ethTokens.upToken,
      ethDn: ethTokens.dnToken,
      btcSlug: slug('btc', tf, ws_),
      ethSlug: slug('eth', tf, ws_),
    };

    // Seed price book from market data (stale but better than nothing)
    for (const tok of (btcMkt?.tokens ?? [])) {
      if (tok.token_id && (tok.price > 0)) {
        priceBook[tok.token_id] = priceBook[tok.token_id] ?? { bid: 0, ask: 0 };
      }
    }

    // Subscribe to live prices
    for (const tid of [btcTokens.upToken, btcTokens.dnToken, ethTokens.upToken, ethTokens.dnToken]) {
      subscribeToken(tid);
    }

    log(`✅ ${tf} window ${ws_}: BTC↑${btcTokens.upToken?.slice(0,8)}… ETH↑${ethTokens.upToken?.slice(0,8)}…`);
  }
}

// ─── Price Helpers ────────────────────────────────────────────────────────────
function getAsk(tokenId) {
  return priceBook[tokenId]?.ask ?? 0;
}
function getBid(tokenId) {
  return priceBook[tokenId]?.bid ?? 0;
}

// ─── Arbitrage Logic ──────────────────────────────────────────────────────────
function tradeId() {
  return `T${Date.now().toString(36).toUpperCase()}`;
}

function checkEntry(window_) {
  const { tf, windowStart, btcUp, btcDn, ethUp, ethDn } = window_;

  const btcUpAsk = getAsk(btcUp);
  const btcDnAsk = getAsk(btcDn);
  const ethUpAsk = getAsk(ethUp);
  const ethDnAsk = getAsk(ethDn);

  // Validate — no zeros/missing
  const allValid = [btcUpAsk, btcDnAsk, ethUpAsk, ethDnAsk].every(p => p > 0);
  if (!allValid) return;

  // Check if we already have an open trade for this window
  const alreadyOpen = state.openTrades.some(t => t.windowStart === windowStart && t.tf === tf);
  if (alreadyOpen) return;

  // Gap 1: BTC-Up + ETH-Down
  const gap1 = btcUpAsk + ethDnAsk - 1;
  // Gap 2: ETH-Up + BTC-Down
  const gap2 = ethUpAsk + btcDnAsk - 1;

  if (gap1 <= ENTRY_THRESHOLD) {
    enterTrade(window_, 'BTC_UP+ETH_DN', btcUp, ethDn, btcUpAsk, ethDnAsk, gap1);
  } else if (gap2 <= ENTRY_THRESHOLD) {
    enterTrade(window_, 'ETH_UP+BTC_DN', ethUp, btcDn, ethUpAsk, btcDnAsk, gap2);
  }
}

function enterTrade(window_, type, legAToken, legBToken, legAAsk, legBAsk, entryGap) {
  const entryCost = (legAAsk + legBAsk) * SHARES;
  if (state.balance < entryCost) {
    log(`💸 Insufficient balance ($${state.balance.toFixed(2)}) for trade cost $${entryCost.toFixed(2)}`);
    return;
  }

  const trade = {
    id: tradeId(),
    tf: window_.tf,
    windowStart: window_.windowStart,
    type,
    legAToken,
    legBToken,
    legAAsk,
    legBAsk,
    entryCost,
    shares: SHARES,
    entryGap: entryGap.toFixed(4),
    openedAt: new Date().toISOString(),
    floatingPnl: 0,
  };

  state.balance -= entryCost;
  state.openTrades.push(trade);
  saveState();

  log(`🟢 ENTRY [${trade.id}] ${type} | tf=${window_.tf} | gap=${entryGap.toFixed(4)} | cost=$${entryCost.toFixed(2)} | bal=$${state.balance.toFixed(2)}`);
  emitFn('trade_entered', trade);
}

function checkExits() {
  for (const trade of [...state.openTrades]) {
    const bidA = getBid(trade.legAToken);
    const bidB = getBid(trade.legBToken);

    if (bidA <= 0 || bidB <= 0) continue;

    const exitProceeds = (bidA + bidB) * trade.shares;
    const exitGap = bidA + bidB - 1;
    const pnl = exitProceeds - trade.entryCost;

    // Update floating PnL
    trade.floatingPnl = parseFloat(pnl.toFixed(4));

    if (exitGap >= EXIT_THRESHOLD) {
      closeTrade(trade, exitGap, exitProceeds, pnl);
    }
  }
}

function closeTrade(trade, exitGap, exitProceeds, pnl) {
  state.openTrades = state.openTrades.filter(t => t.id !== trade.id);
  state.balance += exitProceeds;
  state.totalPnl += pnl;

  const closed = {
    ...trade,
    exitGap: exitGap.toFixed(4),
    exitProceeds,
    realizedPnl: parseFloat(pnl.toFixed(4)),
    closedAt: new Date().toISOString(),
  };

  state.closedTrades.push(closed);
  saveState();

  const sign = pnl >= 0 ? '🟢' : '🔴';
  log(`${sign} EXIT  [${trade.id}] ${trade.type} | exitGap=${exitGap.toFixed(4)} | pnl=$${pnl.toFixed(2)} | bal=$${state.balance.toFixed(2)}`);
  emitFn('trade_closed', closed);
}

// ─── Dashboard Data Builder ───────────────────────────────────────────────────
function buildDashboardSnapshot() {
  const windows = [];

  for (const [key, w] of Object.entries(marketCache)) {
    const btcUpAsk = getAsk(w.btcUp);
    const btcDnAsk = getAsk(w.btcDn);
    const ethUpAsk = getAsk(w.ethUp);
    const ethDnAsk = getAsk(w.ethDn);
    const btcUpBid = getBid(w.btcUp);
    const btcDnBid = getBid(w.btcDn);
    const ethUpBid = getBid(w.ethUp);
    const ethDnBid = getBid(w.ethDn);

    const entryGap1 = (btcUpAsk > 0 && ethDnAsk > 0) ? btcUpAsk + ethDnAsk - 1 : null;
    const entryGap2 = (ethUpAsk > 0 && btcDnAsk > 0) ? ethUpAsk + btcDnAsk - 1 : null;
    const exitGap1  = (btcUpBid > 0 && ethDnBid > 0)  ? btcUpBid + ethDnBid  - 1 : null;
    const exitGap2  = (ethUpBid > 0 && btcDnBid > 0)  ? ethUpBid + btcDnBid  - 1 : null;

    windows.push({
      key,
      tf: w.tf,
      windowStart: w.windowStart,
      btcUpAsk, btcDnAsk, ethUpAsk, ethDnAsk,
      btcUpBid, btcDnBid, ethUpBid, ethDnBid,
      entryGap1: entryGap1 !== null ? +entryGap1.toFixed(4) : null,
      entryGap2: entryGap2 !== null ? +entryGap2.toFixed(4) : null,
      exitGap1:  exitGap1  !== null ? +exitGap1.toFixed(4)  : null,
      exitGap2:  exitGap2  !== null ? +exitGap2.toFixed(4)  : null,
    });
  }

  return {
    balance: +state.balance.toFixed(2),
    totalPnl: +state.totalPnl.toFixed(2),
    openTrades: state.openTrades,
    closedTrades: state.closedTrades.slice(-20),
    windows,
    updatedAt: new Date().toISOString(),
  };
}

// ─── Main Loop ────────────────────────────────────────────────────────────────
let loopTimer = null;

async function tick() {
  try {
    await refreshMarkets();

    for (const w of Object.values(marketCache)) {
      // Only process current window
      const currentWs = currentWindowStart(w.tf);
      if (w.windowStart !== currentWs) continue;
      checkEntry(w);
    }

    checkExits();

    emitFn('snapshot', buildDashboardSnapshot());
  } catch (e) {
    log(`⚠️  Tick error: ${e.message}`);
  }
}

async function start(emit, logEmit) {
  emitFn = emit;
  logFn  = logEmit;

  log('🚀 Polymarket Arb Bot starting…');
  loadState();
  connectWebSocket();

  await tick();
  loopTimer = setInterval(tick, 5000);  // tick every 5s

  log(`💰 Starting balance: $${state.balance.toFixed(2)}`);
}

function stop() {
  if (loopTimer) clearInterval(loopTimer);
  if (ws) ws.terminate();
}

module.exports = { start, stop, buildDashboardSnapshot };
