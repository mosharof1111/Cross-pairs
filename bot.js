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
const ENTRY_THRESHOLD = -0.20;
const EXIT_THRESHOLD  =  0.05;
const SHARES = 50;
const STARTING_BALANCE = 1000;

// ─── State ────────────────────────────────────────────────────────────────────
let state = {
  balance: STARTING_BALANCE,
  openTrades: [],
  closedTrades: [],
  totalPnl: 0,
};

const priceBook = {};   // tokenId → { bid, ask }
const marketCache = {}; // `${tf}-${windowStart}` → window object

let emitFn = () => {};
let logFn  = () => {};

// ─── Persistence ──────────────────────────────────────────────────────────────
function loadState() {
  try {
    if (fs.existsSync(TRADES_FILE)) {
      const raw = JSON.parse(fs.readFileSync(TRADES_FILE, 'utf8'));
      state.balance      = raw.balance      ?? STARTING_BALANCE;
      state.openTrades   = raw.openTrades   ?? [];
      state.closedTrades = raw.closedTrades ?? [];
      state.totalPnl     = raw.totalPnl     ?? 0;

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

function makeSlug(asset, tf, windowStart) {
  return `${asset.toLowerCase()}-updown-${tf}-${windowStart}`;
}

// ─── Gamma API ────────────────────────────────────────────────────────────────
async function fetchMarketBySlug(slugStr) {
  try {
    const url = `${GAMMA_API}/markets?slug=${encodeURIComponent(slugStr)}`;
    const res = await fetch(url, { timeout: 10000 });
    if (!res.ok) {
      log(`⚠️  Gamma HTTP ${res.status} for: ${slugStr}`);
      return null;
    }
    const data = await res.json();
    const list = Array.isArray(data) ? data : (data.markets ?? []);
    if (list.length === 0) {
      log(`⚠️  Gamma empty result for: ${slugStr}`);
      return null;
    }
    return list[0];
  } catch (e) {
    log(`⚠️  Gamma fetch error (${slugStr}): ${e.message}`);
    return null;
  }
}

// Extract Up/Down token IDs from a Gamma market object.
// Primary: clobTokenIds[0] = Up, clobTokenIds[1] = Down
function extractTokenIds(market) {
  if (!market) return null;

  // ── Primary: clobTokenIds ──────────────────────────────────────────────────
  if (Array.isArray(market.clobTokenIds) && market.clobTokenIds.length >= 2) {
    const up = market.clobTokenIds[0];
    const dn = market.clobTokenIds[1];
    if (up && dn) return { upToken: String(up), dnToken: String(dn) };
  }

  // ── Fallback: tokens array with outcome labels ─────────────────────────────
  if (Array.isArray(market.tokens) && market.tokens.length >= 2) {
    const upTok = market.tokens.find(t => /up|yes|higher/i.test(t.outcome ?? t.name ?? ''));
    const dnTok = market.tokens.find(t => /down|no|lower/i.test(t.outcome ?? t.name ?? ''));
    if (upTok?.token_id && dnTok?.token_id) {
      return { upToken: upTok.token_id, dnToken: dnTok.token_id };
    }
    const t0 = market.tokens[0]?.token_id;
    const t1 = market.tokens[1]?.token_id;
    if (t0 && t1) return { upToken: t0, dnToken: t1 };
  }

  // ── snake_case variant ─────────────────────────────────────────────────────
  if (Array.isArray(market.clob_token_ids) && market.clob_token_ids.length >= 2) {
    return { upToken: String(market.clob_token_ids[0]), dnToken: String(market.clob_token_ids[1]) };
  }

  // Log available keys to help debug further if still failing
  log(`⚠️  extractTokenIds failed. Keys: ${Object.keys(market).join(', ')}`);
  log(`    clobTokenIds=${JSON.stringify(market.clobTokenIds)} tokens=${JSON.stringify(market.tokens?.slice(0,2))}`);
  return null;
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
    for (const tokenId of pendingSubscriptions) _sendSubscribe(tokenId);
    pendingSubscriptions.clear();
  });

  ws.on('message', (raw) => {
    try {
      const msgs = JSON.parse(raw);
      const arr = Array.isArray(msgs) ? msgs : [msgs];
      for (const msg of arr) handleWsMessage(msg);
    } catch (_) {}
  });

  ws.on('close', () => {
    wsReady = false;
    log('⚡ WebSocket closed — reconnecting in 5s');
    setTimeout(connectWebSocket, 5000);
  });

  ws.on('error', (e) => log(`⚠️  WebSocket error: ${e.message}`));
}

function _sendSubscribe(tokenId) {
  ws.send(JSON.stringify({ assets_ids: [tokenId], type: 'market' }));
}

function subscribeToken(tokenId) {
  if (!tokenId) return;
  if (!wsReady || !ws || ws.readyState !== WebSocket.OPEN) {
    pendingSubscriptions.add(tokenId);
    return;
  }
  _sendSubscribe(tokenId);
}

function handleWsMessage(msg) {
  const tokenId = msg.asset_id ?? msg.token_id ?? msg.market;
  if (!tokenId) return;
  const bid = parseFloat(msg.bid ?? msg.best_bid ?? 0) || 0;
  const ask = parseFloat(msg.ask ?? msg.best_ask ?? 0) || 0;
  if (bid > 0 || ask > 0) priceBook[tokenId] = { bid, ask };
}

// ─── Market Discovery ─────────────────────────────────────────────────────────
async function refreshMarkets() {
  for (const tf of Object.keys(WINDOW_SIZES)) {
    const ws_ = currentWindowStart(tf);
    const cacheKey = `${tf}-${ws_}`;
    if (marketCache[cacheKey]) continue;

    const btcSlug = makeSlug('btc', tf, ws_);
    const ethSlug = makeSlug('eth', tf, ws_);

    log(`🔍 Fetching ${tf} window ${ws_}`);

    const [btcMkt, ethMkt] = await Promise.all([
      fetchMarketBySlug(btcSlug),
      fetchMarketBySlug(ethSlug),
    ]);

    const btcTokens = extractTokenIds(btcMkt);
    const ethTokens = extractTokenIds(ethMkt);

    if (!btcTokens || !ethTokens) {
      log(`⚠️  Token extract failed for ${tf}/${ws_} — will retry next tick`);
      continue;
    }

    marketCache[cacheKey] = {
      tf, windowStart: ws_,
      btcUp: btcTokens.upToken, btcDn: btcTokens.dnToken,
      ethUp: ethTokens.upToken, ethDn: ethTokens.dnToken,
      btcSlug, ethSlug,
    };

    log(`✅ Cached ${tf}/${ws_} | BTC↑ ${btcTokens.upToken.slice(0,10)}… ETH↑ ${ethTokens.upToken.slice(0,10)}…`);

    for (const tid of [btcTokens.upToken, btcTokens.dnToken, ethTokens.upToken, ethTokens.dnToken]) {
      subscribeToken(tid);
    }
  }
}

// ─── Price Helpers ────────────────────────────────────────────────────────────
function getAsk(tokenId) { return priceBook[tokenId]?.ask ?? 0; }
function getBid(tokenId) { return priceBook[tokenId]?.bid ?? 0; }

// ─── Arbitrage Logic ──────────────────────────────────────────────────────────
function tradeId() { return `T${Date.now().toString(36).toUpperCase()}`; }

function checkEntry(window_) {
  const { tf, windowStart, btcUp, btcDn, ethUp, ethDn } = window_;
  const btcUpAsk = getAsk(btcUp), btcDnAsk = getAsk(btcDn);
  const ethUpAsk = getAsk(ethUp), ethDnAsk = getAsk(ethDn);

  if (![btcUpAsk, btcDnAsk, ethUpAsk, ethDnAsk].every(p => p > 0)) return;

  const alreadyOpen = state.openTrades.some(t => t.windowStart === windowStart && t.tf === tf);
  if (alreadyOpen) return;

  const gap1 = btcUpAsk + ethDnAsk - 1;
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
    log(`💸 Insufficient balance ($${state.balance.toFixed(2)}) for $${entryCost.toFixed(2)}`);
    return;
  }
  const trade = {
    id: tradeId(), tf: window_.tf, windowStart: window_.windowStart,
    type, legAToken, legBToken, legAAsk, legBAsk,
    entryCost, shares: SHARES,
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
    trade.floatingPnl = parseFloat(pnl.toFixed(4));
    if (exitGap >= EXIT_THRESHOLD) closeTrade(trade, exitGap, exitProceeds, pnl);
  }
}

function closeTrade(trade, exitGap, exitProceeds, pnl) {
  state.openTrades = state.openTrades.filter(t => t.id !== trade.id);
  state.balance += exitProceeds;
  state.totalPnl += pnl;
  const closed = { ...trade, exitGap: exitGap.toFixed(4), exitProceeds, realizedPnl: parseFloat(pnl.toFixed(4)), closedAt: new Date().toISOString() };
  state.closedTrades.push(closed);
  saveState();
  const sign = pnl >= 0 ? '🟢' : '🔴';
  log(`${sign} EXIT [${trade.id}] ${trade.type} | exitGap=${exitGap.toFixed(4)} | pnl=$${pnl.toFixed(2)} | bal=$${state.balance.toFixed(2)}`);
  emitFn('trade_closed', closed);
}

// ─── Dashboard Snapshot ───────────────────────────────────────────────────────
function buildDashboardSnapshot() {
  const windows = Object.values(marketCache).map(w => {
    const btcUpAsk = getAsk(w.btcUp), btcDnAsk = getAsk(w.btcDn);
    const ethUpAsk = getAsk(w.ethUp), ethDnAsk = getAsk(w.ethDn);
    const btcUpBid = getBid(w.btcUp), btcDnBid = getBid(w.btcDn);
    const ethUpBid = getBid(w.ethUp), ethDnBid = getBid(w.ethDn);
    return {
      key: `${w.tf}-${w.windowStart}`, tf: w.tf, windowStart: w.windowStart,
      btcUpAsk, btcDnAsk, ethUpAsk, ethDnAsk,
      btcUpBid, btcDnBid, ethUpBid, ethDnBid,
      entryGap1: (btcUpAsk > 0 && ethDnAsk > 0) ? +(btcUpAsk + ethDnAsk - 1).toFixed(4) : null,
      entryGap2: (ethUpAsk > 0 && btcDnAsk > 0) ? +(ethUpAsk + btcDnAsk - 1).toFixed(4) : null,
      exitGap1:  (btcUpBid > 0 && ethDnBid > 0) ? +(btcUpBid + ethDnBid  - 1).toFixed(4) : null,
      exitGap2:  (ethUpBid > 0 && btcDnBid > 0) ? +(ethUpBid + btcDnBid  - 1).toFixed(4) : null,
    };
  });
  return {
    balance: +state.balance.toFixed(2), totalPnl: +state.totalPnl.toFixed(2),
    openTrades: state.openTrades, closedTrades: state.closedTrades.slice(-20),
    windows, updatedAt: new Date().toISOString(),
  };
}

// ─── Main Loop ────────────────────────────────────────────────────────────────
let loopTimer = null;

async function tick() {
  try {
    await refreshMarkets();
    for (const w of Object.values(marketCache)) {
      if (w.windowStart !== currentWindowStart(w.tf)) continue;
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
  loopTimer = setInterval(tick, 5000);
  log(`💰 Starting balance: $${state.balance.toFixed(2)}`);
}

function stop() {
  if (loopTimer) clearInterval(loopTimer);
  if (ws) ws.terminate();
}

module.exports = { start, stop, buildDashboardSnapshot };
