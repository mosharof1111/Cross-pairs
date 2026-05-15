'use strict';

const fetch = require('node-fetch');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');

const GAMMA     = 'https://gamma-api.polymarket.com';
const CLOB_REST = 'https://clob.polymarket.com';
const CLOB_WS   = 'wss://ws-subscriptions-clob.polymarket.com/ws/market';
const TRADES_FILE = path.join(__dirname, 'trades.json');

// 5m = 300s windows, 15m = 900s windows
const TIMEFRAMES = [
  { tf: '5m',  size: 300 },
  { tf: '15m', size: 900 },
];

const ENTRY_THRESHOLD = -0.10;
const EXIT_THRESHOLD  =  0.02;
const SHARES          = 50;
const STARTING_BALANCE = 1000;

let state = { balance: STARTING_BALANCE, openTrades: [], closedTrades: [], totalPnl: 0 };
const priceBook   = {};
const marketCache = {};
let emitFn = () => {};
let logFn  = () => {};

function loadState() {
  try {
    if (fs.existsSync(TRADES_FILE)) {
      const raw = JSON.parse(fs.readFileSync(TRADES_FILE, 'utf8'));
      state = { ...state, ...raw };
      if (state.openTrades.length > 0) {
        log(`♻️  Refunding ${state.openTrades.length} open trade(s)`);
        for (const t of state.openTrades) state.balance += t.entryCost;
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

function currentWindowStart(size) {
  return Math.floor(Math.floor(Date.now() / 1000) / size) * size;
}

// ── Token extraction ──────────────────────────────────────────────────────────
// outcomes = ["Up","Down"] tells us which clobTokenIds index is Up vs Down
function extractTokenIds(mkt) {
  if (!mkt) return null;

  let ids = mkt.clobTokenIds ?? mkt.clob_token_ids;
  if (typeof ids === 'string') { try { ids = JSON.parse(ids); } catch (_) { ids = null; } }

  let outcomes = mkt.outcomes;
  if (typeof outcomes === 'string') { try { outcomes = JSON.parse(outcomes); } catch (_) { outcomes = null; } }

  if (Array.isArray(ids) && ids.length >= 2 && ids[0] && ids[1]) {
    // Use outcomes array to find correct Up/Down index
    if (Array.isArray(outcomes) && outcomes.length >= 2) {
      const upIdx = outcomes.findIndex(o => /up/i.test(String(o)));
      const dnIdx = outcomes.findIndex(o => /down/i.test(String(o)));
      if (upIdx >= 0 && dnIdx >= 0) {
        log(`  📋 outcomes=${JSON.stringify(outcomes)} → up=ids[${upIdx}] dn=ids[${dnIdx}]`);
        return { upToken: String(ids[upIdx]), dnToken: String(ids[dnIdx]) };
      }
    }
    // Fallback: index 0 = Up per Polymarket default
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

// ── Market discovery ──────────────────────────────────────────────────────────
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
    // bestBid/bestAsk refer to the Up token
    priceBook[tokens.upToken] = { bid: bestBid, ask: bestAsk };
    priceBook[tokens.dnToken] = { bid: Math.max(0, 1 - bestAsk), ask: Math.min(1, 1 - bestBid) };
    log(`  💲 Seeded Up bid=${bestBid} ask=${bestAsk}`);
  } else if (Array.isArray(prices) && prices.length >= 2 && Array.isArray(outcomes)) {
    const upIdx = outcomes.findIndex(o => /up/i.test(String(o)));
    const dnIdx = outcomes.findIndex(o => /down/i.test(String(o)));
    if (upIdx >= 0 && dnIdx >= 0) {
      const up = parseFloat(prices[upIdx]) || 0;
      const dn = parseFloat(prices[dnIdx]) || 0;
      if (up > 0) priceBook[tokens.upToken] = { bid: Math.max(0, up - 0.01), ask: Math.min(1, up + 0.01) };
      if (dn > 0) priceBook[tokens.dnToken] = { bid: Math.max(0, dn - 0.01), ask: Math.min(1, dn + 0.01) };
      log(`  💲 Seeded from outcomePrices: Up=${up} Dn=${dn}`);
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

async function refreshMarkets() {
  for (const { tf, size } of TIMEFRAMES) {
    const currentWs = currentWindowStart(size);
    const cacheKey  = `${tf}-${currentWs}`;
    if (marketCache[cacheKey]) continue;

    log(`🔍 Finding ${tf} markets…`);
    const offsets = [0, 1, -1, 2, -2];
    let found = false;

    for (const offset of offsets) {
      const ts      = currentWs + offset * size;
      const btcSlug = `btc-updown-${tf}-${ts}`;
      const ethSlug = `eth-updown-${tf}-${ts}`;

      const [btcTokens, ethTokens] = await Promise.all([
        findTokensForSlug(btcSlug),
        findTokensForSlug(ethSlug),
      ]);

      if (!btcTokens || !ethTokens) continue;

      marketCache[cacheKey] = {
        tf, size, windowStart: ts,
        btcUp: btcTokens.upToken, btcDn: btcTokens.dnToken,
        ethUp: ethTokens.upToken, ethDn: ethTokens.dnToken,
        btcSlug, ethSlug,
      };

      log(`✅ ${tf} ws=${ts} | ${btcSlug}`);
      log(`   BTC↑ ${btcTokens.upToken.slice(0,14)}… ETH↑ ${ethTokens.upToken.slice(0,14)}…`);

      for (const tid of [btcTokens.upToken, btcTokens.dnToken, ethTokens.upToken, ethTokens.dnToken])
        subscribeToken(tid);

      found = true;
      break;
    }
    if (!found) log(`⚠️  ${tf}: not found yet`);
  }
}

// ── REST price polling ────────────────────────────────────────────────────────
async function pollPrices() {
  const tids = [...new Set(
    Object.values(marketCache).flatMap(w => [w.btcUp, w.btcDn, w.ethUp, w.ethDn].filter(Boolean))
  )];
  if (!tids.length) return;

  let updated = 0;
  await Promise.all(tids.map(async tid => {
    try {
      const [ar, br] = await Promise.all([
        fetch(`${CLOB_REST}/price?token_id=${tid}&side=BUY`,  { timeout: 5000 }),
        fetch(`${CLOB_REST}/price?token_id=${tid}&side=SELL`, { timeout: 5000 }),
      ]);
      const ask = parseFloat((await ar.json()).price ?? 0) || 0;
      const bid = parseFloat((await br.json()).price ?? 0) || 0;
      if (ask > 0 || bid > 0) { priceBook[tid] = { bid, ask }; updated++; }
    } catch (_) {}
  }));
  if (updated > 0) log(`💲 REST poll: updated ${updated}/${tids.length} prices`);
}

// ── WebSocket ─────────────────────────────────────────────────────────────────
let ws = null, wsReady = false;
const pendingSubs = new Set();

function connectWS() {
  log('🔌 Connecting CLOB WebSocket…');
  ws = new WebSocket(CLOB_WS);
  ws.on('open', () => {
    wsReady = true; log('✅ WebSocket connected');
    for (const t of pendingSubs) _sub(t); pendingSubs.clear();
  });
  ws.on('message', raw => {
    try {
      const msgs = JSON.parse(raw);
      (Array.isArray(msgs) ? msgs : [msgs]).forEach(handleWsMsg);
    } catch (_) {}
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
function handleWsMsg(msg) {
  const tid = msg.asset_id ?? msg.token_id ?? msg.market;
  if (!tid) return;
  if (Array.isArray(msg.bids) || Array.isArray(msg.asks)) {
    const bid = parseFloat(msg.bids?.[0]?.price ?? 0) || 0;
    const ask = parseFloat(msg.asks?.[0]?.price ?? 0) || 0;
    if (bid > 0 || ask > 0) priceBook[tid] = { bid, ask };
    return;
  }
  const bid = parseFloat(msg.bid ?? msg.best_bid ?? 0) || 0;
  const ask = parseFloat(msg.ask ?? msg.best_ask ?? 0) || 0;
  if (bid > 0 || ask > 0) priceBook[tid] = { bid, ask };
}

// ── Price helpers ─────────────────────────────────────────────────────────────
const getAsk = tid => priceBook[tid]?.ask ?? 0;
const getBid = tid => priceBook[tid]?.bid ?? 0;

// ── Arb logic ─────────────────────────────────────────────────────────────────
function tradeId() { return `T${Date.now().toString(36).toUpperCase()}`; }

function checkEntry(w) {
  const bUA = getAsk(w.btcUp), bDA = getAsk(w.btcDn);
  const eUA = getAsk(w.ethUp), eDA = getAsk(w.ethDn);

  if (![bUA,bDA,eUA,eDA].every(p => p > 0)) return;
  if (state.openTrades.some(t => t.windowStart===w.windowStart && t.tf===w.tf)) return;

  const g1 = bUA + eDA - 1;  // BTC↑ask + ETH↓ask - 1
  const g2 = eUA + bDA - 1;  // ETH↑ask + BTC↓ask - 1

  log(`📊 ${w.tf} gaps: E1=${g1.toFixed(4)} E2=${g2.toFixed(4)}`);

  if (g1 <= ENTRY_THRESHOLD) enterTrade(w,'BTC_UP+ETH_DN',w.btcUp,w.ethDn,bUA,eDA,g1);
  else if (g2 <= ENTRY_THRESHOLD) enterTrade(w,'ETH_UP+BTC_DN',w.ethUp,w.btcDn,eUA,bDA,g2);
}

function enterTrade(w,type,lA,lB,askA,askB,gap) {
  const cost=(askA+askB)*SHARES;
  if (state.balance<cost) { log(`💸 Low balance $${state.balance.toFixed(2)}`); return; }
  const t={id:tradeId(),tf:w.tf,windowStart:w.windowStart,type,
    legAToken:lA,legBToken:lB,legAAsk:askA,legBAsk:askB,
    entryCost:cost,shares:SHARES,entryGap:gap.toFixed(4),
    openedAt:new Date().toISOString(),floatingPnl:0};
  state.balance-=cost; state.openTrades.push(t); saveState();
  log(`🟢 ENTRY [${t.id}] ${type} tf=${w.tf} gap=${gap.toFixed(4)} cost=$${cost.toFixed(2)} bal=$${state.balance.toFixed(2)}`);
  emitFn('trade_entered',t);
}

function checkExits() {
  for (const t of [...state.openTrades]) {
    const bA=getBid(t.legAToken), bB=getBid(t.legBToken);
    if (!bA||!bB) continue;
    const proceeds=(bA+bB)*t.shares, xGap=bA+bB-1, pnl=proceeds-t.entryCost;
    t.floatingPnl=+pnl.toFixed(4);
    if (xGap>=EXIT_THRESHOLD) {
      state.openTrades=state.openTrades.filter(x=>x.id!==t.id);
      state.balance+=proceeds; state.totalPnl+=pnl;
      state.closedTrades.push({...t,exitGap:xGap.toFixed(4),exitProceeds:proceeds,
        realizedPnl:+pnl.toFixed(4),closedAt:new Date().toISOString()});
      saveState();
      log(`${pnl>=0?'🟢':'🔴'} EXIT [${t.id}] ${t.type} tf=${t.tf} pnl=$${pnl.toFixed(2)} bal=$${state.balance.toFixed(2)}`);
      emitFn('trade_closed',t);
    }
  }
}

// ── Dashboard snapshot ────────────────────────────────────────────────────────
function buildDashboardSnapshot() {
  const windows = Object.values(marketCache).map(w => {
    const bUA=getAsk(w.btcUp), bDA=getAsk(w.btcDn), eUA=getAsk(w.ethUp), eDA=getAsk(w.ethDn);
    const bUB=getBid(w.btcUp), bDB=getBid(w.btcDn), eUB=getBid(w.ethUp), eDB=getBid(w.ethDn);
    return {
      key:`${w.tf}-${w.windowStart}`, tf:w.tf, windowStart:w.windowStart,
      btcSlug:w.btcSlug, ethSlug:w.ethSlug,
      btcUpAsk:bUA, btcDnAsk:bDA, ethUpAsk:eUA, ethDnAsk:eDA,
      btcUpBid:bUB, btcDnBid:bDB, ethUpBid:eUB, ethDnBid:eDB,
      entryGap1:(bUA>0&&eDA>0)?+(bUA+eDA-1).toFixed(4):null,
      entryGap2:(eUA>0&&bDA>0)?+(eUA+bDA-1).toFixed(4):null,
      exitGap1: (bUB>0&&eDB>0)?+(bUB+eDB-1).toFixed(4):null,
      exitGap2: (eUB>0&&bDB>0)?+(eUB+bDB-1).toFixed(4):null,
    };
  });
  return {
    balance:+state.balance.toFixed(2), totalPnl:+state.totalPnl.toFixed(2),
    openTrades:state.openTrades, closedTrades:state.closedTrades.slice(-20),
    windows, updatedAt:new Date().toISOString(),
  };
}

function prune() {
  for (const [k,w] of Object.entries(marketCache))
    if (w.windowStart < currentWindowStart(w.size) - w.size) delete marketCache[k];
}

// ── Main loop ─────────────────────────────────────────────────────────────────
let timer=null, pollTimer=null;

async function tick() {
  try {
    prune();
    await refreshMarkets();
    for (const w of Object.values(marketCache)) {
      if (w.windowStart !== currentWindowStart(w.size)) continue;
      checkEntry(w);
    }
    checkExits();
    emitFn('snapshot', buildDashboardSnapshot());
  } catch(e) { log(`⚠️  tick: ${e.message}`); }
}

async function start(emit, logEmit) {
  emitFn=emit; logFn=logEmit;
  log('🚀 Polymarket Arb Bot (5m + 15m)');
  loadState(); connectWS(); await tick();
  timer     = setInterval(tick, 5000);
  pollTimer = setInterval(pollPrices, 10000);
  log(`💰 Balance: $${state.balance.toFixed(2)}`);
}
function stop() { clearInterval(timer); clearInterval(pollTimer); ws?.terminate(); }
module.exports = { start, stop, buildDashboardSnapshot };
