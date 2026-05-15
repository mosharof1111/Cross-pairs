'use strict';

const fetch = require('node-fetch');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');

// ─── Constants ────────────────────────────────────────────────────────────────
const GAMMA_API  = 'https://gamma-api.polymarket.com';
const CLOB_WS    = 'wss://ws-subscriptions-clob.polymarket.com/ws/market';
const TRADES_FILE = path.join(__dirname, 'trades.json');

const TIMEFRAMES = ['15m', '4h'];
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

const priceBook   = {};  // tokenId → { bid, ask }
const marketCache = {};  // `${tf}-${windowStart}` → window object

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

// ─── Window Helpers ───────────────────────────────────────────────────────────
function currentWindowStart(tf) {
  const size = WINDOW_SIZES[tf];
  return Math.floor(Math.floor(Date.now() / 1000) / size) * size;
}

// ─── Gamma API ────────────────────────────────────────────────────────────────
// Search for active markets whose slug contains the search string.
// Returns array of market objects.
async function searchMarkets(slugContains) {
  try {
    const url = `${GAMMA_API}/markets?slug_contains=${encodeURIComponent(slugContains)}&active=true&closed=false&acceptingOrders=true&limit=10`;
    const res = await fetch(url, { timeout: 10000 });
    if (!res.ok) {
      log(`⚠️  Gamma HTTP ${res.status} for slug_contains=${slugContains}`);
      return [];
    }
    const data = await res.json();
    return Array.isArray(data) ? data : (data.markets ?? []);
  } catch (e) {
    log(`⚠️  Gamma searchMarkets error (${slugContains}): ${e.message}`);
    return [];
  }
}

// Extract Up/Down token IDs from a Gamma market object.
// clobTokenIds is a JSON-stringified array: "[\"tokenA\",\"tokenB\"]"
// index 0 = Up/Yes, index 1 = Down/No
function extractTokenIds(market) {
  if (!market) return null;

  // ── Primary: clobTokenIds (may be a JSON string or array) ─────────────────
  let ids = market.clobTokenIds;
  if (typeof ids === 'string') {
    try { ids = JSON.parse(ids); } catch (_) { ids = null; }
  }
  if (Array.isArray(ids) && ids.length >= 2 && ids[0] && ids[1]) {
    return { upToken: String(ids[0]), dnToken: String(ids[1]) };
  }

  // ── Fallback: tokens array ─────────────────────────────────────────────────
  if (Array.isArray(market.tokens) && market.tokens.length >= 2) {
    const upTok = market.tokens.find(t => /up|yes|higher/i.test(t.outcome ?? t.name ?? ''));
    const dnTok = market.tokens.find(t => /down|no|lower/i.test(t.outcome ?? t.name ?? ''));
    if (upTok?.token_id && dnTok?.token_id) return { upToken: upTok.token_id, dnToken: dnTok.token_id };
    const t0 = market.tokens[0]?.token_id, t1 = market.tokens[1]?.token_id;
    if (t0 && t1) return { upToken: t0, dnToken: t1 };
  }

  log(`⚠️  extractTokenIds failed for slug=${market.slug}. Keys: ${Object.keys(market).join(', ')}`);
  log(`    clobTokenIds raw: ${JSON.stringify(market.clobTokenIds)}`);
  return null;
}

// Extract the unix timestamp from a slug like "btc-updown-15m-1778806800"
function timestampFromSlug(slug) {
  const m = slug.match(/-(\d{9,11})$/);
  return m ? parseInt(m[1], 10) : null;
}

// ─── Market Discovery ─────────────────────────────────────────────────────────
async function refreshMarkets() {
  for (const tf of TIMEFRAMES) {
    const currentWs = currentWindowStart(tf);
    const cacheKey  = `${tf}-${currentWs}`;
    if (marketCache[cacheKey]) continue;

    log(`🔍 Searching active ${tf} markets…`);

    // Search BTC and ETH in parallel
    const [btcList, ethList] = await Promise.all([
      searchMarkets(`btc-updown-${tf}`),
      searchMarkets(`eth-updown-${tf}`),
    ]);

    log(`   BTC results: ${btcList.length} | ETH results: ${ethList.length}`);
    if (btcList.length) log(`   BTC slugs: ${btcList.map(m => m.slug).join(', ')}`);
    if (ethList.length) log(`   ETH slugs: ${ethList.map(m => m.slug).join(', ')}`);

    // Pick the market whose slug timestamp matches current window.
    // If no exact match, pick the most recent accepting-orders market.
    const btcMkt = pickBestMarket(btcList, tf, currentWs);
    const ethMkt = pickBestMarket(ethList, tf, currentWs);

    if (!btcMkt || !ethMkt) {
      log(`⚠️  No matching ${tf} market found — will retry`);
      continue;
    }

    const btcTokens = extractTokenIds(btcMkt);
    const ethTokens = extractTokenIds(ethMkt);

    if (!btcTokens || !ethTokens) {
      log(`⚠️  Token extract failed for ${tf} — will retry`);
      continue;
    }

    // Use the timestamp from the slug as the canonical windowStart
    const btcWs = timestampFromSlug(btcMkt.slug) ?? currentWs;

    marketCache[cacheKey] = {
      tf,
      windowStart: btcWs,
      btcUp: btcTokens.upToken,
      btcDn: btcTokens.dnToken,
      ethUp: ethTokens.upToken,
      ethDn: ethTokens.dnToken,
      btcSlug: btcMkt.slug,
      ethSlug: ethMkt.slug,
    };

    log(`✅ Cached ${tf} ws=${btcWs} | BTC=${btcMkt.slug} | ETH=${ethMkt.slug}`);
    log(`   BTC↑ ${btcTokens.upToken.slice(0,12)}… BTC↓ ${btcTokens.dnToken.slice(0,12)}…`);
    log(`   ETH↑ ${ethTokens.upToken.slice(0,12)}… ETH↓ ${ethTokens.dnToken.slice(0,12)}…`);

    for (const tid of [btcTokens.upToken, btcTokens.dnToken, ethTokens.upToken, ethTokens.dnToken]) {
      subscribeToken(tid);
    }
  }
}

// Pick the best market from a list for a given timeframe and expected window start
function pickBestMarket(list, tf, expectedWs) {
  if (!list.length) return null;

  // 1. Exact slug timestamp match
  const exact = list.find(m => {
    const ts = timestampFromSlug(m.slug);
    return ts === expectedWs;
  });
  if (exact) return exact;

  // 2. Closest future window that is accepting orders
  const accepting = list.filter(m => m.acceptingOrders !== false);
  if (accepting.length) {
    // sort by slug timestamp descending (newest first)
    accepting.sort((a, b) => {
      const ta = timestampFromSlug(a.slug) ?? 0;
      const tb = timestampFromSlug(b.slug) ?? 0;
      return tb - ta;
    });
    return accepting[0];
  }

  // 3. Any market
  return list[0];
}

// ─── WebSocket Price Feed ─────────────────────────────────────────────────────
let ws      = null;
let wsReady = false;
const pendingSubscriptions = new Set();

function connectWebSocket() {
  log('🔌 Connecting to Polymarket CLOB WebSocket…');
  ws = new WebSocket(CLOB_WS);

  ws.on('open', () => {
    wsReady = true;
    log('✅ WebSocket connected');
    for (const tid of pendingSubscriptions) _sendSubscribe(tid);
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

// ─── Stale window cleanup ─────────────────────────────────────────────────────
function pruneStaleWindows() {
  for (const key of Object.keys(marketCache)) {
    const w = marketCache[key];
    const currentWs = currentWindowStart(w.tf);
    // Keep up to 1 window behind (for open trades), prune older
    if (w.windowStart < currentWs - WINDOW_SIZES[w.tf]) {
      delete marketCache[key];
    }
  }
}

// ─── Price Helpers ────────────────────────────────────────────────────────────
function getAsk(tokenId) { return priceBook[tokenId]?.ask ?? 0; }
function getBid(tokenId) { return priceBook[tokenId]?.bid ?? 0; }

// ─── Arbitrage Logic ──────────────────────────────────────────────────────────
function tradeId() { return `T${Date.now().toString(36).toUpperCase()}`; }

function checkEntry(w) {
  const btcUpAsk = getAsk(w.btcUp), btcDnAsk = getAsk(w.btcDn);
  const ethUpAsk = getAsk(w.ethUp), ethDnAsk = getAsk(w.ethDn);

  // Reject if any price is missing (NO mid fallback)
  if (![btcUpAsk, btcDnAsk, ethUpAsk, ethDnAsk].every(p => p > 0)) return;

  const alreadyOpen = state.openTrades.some(t => t.windowStart === w.windowStart && t.tf === w.tf);
  if (alreadyOpen) return;

  const gap1 = btcUpAsk + ethDnAsk - 1;  // BTC↑ + ETH↓
  const gap2 = ethUpAsk + btcDnAsk - 1;  // ETH↑ + BTC↓

  if (gap1 <= ENTRY_THRESHOLD) {
    enterTrade(w, 'BTC_UP+ETH_DN', w.btcUp, w.ethDn, btcUpAsk, ethDnAsk, gap1);
  } else if (gap2 <= ENTRY_THRESHOLD) {
    enterTrade(w, 'ETH_UP+BTC_DN', w.ethUp, w.btcDn, ethUpAsk, btcDnAsk, gap2);
  }
}

function enterTrade(w, type, legAToken, legBToken, legAAsk, legBAsk, entryGap) {
  const entryCost = (legAAsk + legBAsk) * SHARES;
  if (state.balance < entryCost) {
    log(`💸 Insufficient balance ($${state.balance.toFixed(2)}) for $${entryCost.toFixed(2)}`);
    return;
  }
  const trade = {
    id: tradeId(), tf: w.tf, windowStart: w.windowStart, type,
    legAToken, legBToken, legAAsk, legBAsk,
    entryCost, shares: SHARES,
    entryGap: entryGap.toFixed(4),
    openedAt: new Date().toISOString(),
    floatingPnl: 0,
  };
  state.balance -= entryCost;
  state.openTrades.push(trade);
  saveState();
  log(`🟢 ENTRY [${trade.id}] ${type} | tf=${w.tf} | gap=${entryGap.toFixed(4)} | cost=$${entryCost.toFixed(2)} | bal=$${state.balance.toFixed(2)}`);
  emitFn('trade_entered', trade);
}

function checkExits() {
  for (const trade of [...state.openTrades]) {
    const bidA = getBid(trade.legAToken), bidB = getBid(trade.legBToken);
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
      btcSlug: w.btcSlug, ethSlug: w.ethSlug,
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
    pruneStaleWindows();
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
