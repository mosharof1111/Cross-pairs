'use strict';

require('dotenv').config();
const fetch     = require('node-fetch');
const WebSocket = require('ws');
const fs        = require('fs');
const path      = require('path');
const ethers    = require('ethers');
const crypto    = require('crypto');

// ── Environment ───────────────────────────────────────────────────────────────
const PRIVATE_KEY    = process.env.PRIVATE_KEY    || '';
const FUNDER_ADDRESS = process.env.FUNDER_ADDRESS || '';
const POLYGON_RPC    = process.env.POLYGON_RPC    || 'https://polygon-rpc.com';
const TRADE_MODE     = (process.env.TRADE_MODE    || 'demo').toLowerCase();
const SIGNATURE_TYPE = process.env.SIGNATURE_TYPE || '2';

const IS_LIVE = TRADE_MODE === 'live';

const GAMMA     = 'https://gamma-api.polymarket.com';
const CLOB_REST = 'https://clob.polymarket.com';
const CHAIN_ID  = 137;

const TRADES_FILE = path.join(__dirname, 'trades.json');
const CONFIG_FILE = path.join(__dirname, 'config.json');
const EQUITY_FILE = path.join(__dirname, 'equity.json');

const BINANCE_STREAMS = {
  BTC:  'wss://stream.binance.com:9443/ws/btcusdt@aggTrade',
  ETH:  'wss://stream.binance.com:9443/ws/ethusdt@aggTrade',
  SOL:  'wss://stream.binance.com:9443/ws/solusdt@aggTrade',
  DOGE: 'wss://stream.binance.com:9443/ws/dogeusdt@aggTrade',
};

const DEFAULT_CONFIG = {
  shares:         5,
  moveMultiplier: 0.5,
  blockSize:      30,
  tokenMin:       0.10,
  tokenMax:       0.90,
  trendBuckets:   3,
  exitAtSecond:   295,
  takeProfit:     0.99,
  historyWindow:  900,
  markets: {
    'btc-5m': true, 'eth-5m': true,
    'sol-5m': true, 'doge-5m': true,
  },
};

const MARKET_SLUGS  = {
  'btc-5m': 'btc-updown-5m', 'eth-5m': 'eth-updown-5m',
  'sol-5m': 'sol-updown-5m', 'doge-5m': 'doge-updown-5m',
};
const MARKET_ASSETS = {
  'btc-5m': 'BTC', 'eth-5m': 'ETH', 'sol-5m': 'SOL', 'doge-5m': 'DOGE',
};

const CRYPTO_FEE_RATE  = 0.018;
const STARTING_BALANCE = IS_LIVE ? 150 : 2000;
const WINDOW_SIZE      = 300;

const CTF_EXCHANGE = '0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E';

let config   = { ...DEFAULT_CONFIG };
let apiCreds = null;
let wallet   = null;

let state = {
  balance:      STARTING_BALANCE,
  openTrades:   [],
  closedTrades: [],
  totalPnl:     0,
  totalFees:    0,
};

let equityCurve = [];
let botRunning  = false;

const priceBook        = {};
const marketCache      = {};
const windowState      = {};
const lastCheckedBlock = { 'btc-5m': -1, 'eth-5m': -1, 'sol-5m': -1, 'doge-5m': -1 };
const exitFiredWindow  = { 'btc-5m': -1, 'eth-5m': -1, 'sol-5m': -1, 'doge-5m': -1 };

const priceHistory   = { BTC: [], ETH: [], SOL: [], DOGE: [] };
const binancePrices  = { BTC: 0, ETH: 0, SOL: 0, DOGE: 0 };
const binanceLastLog = { BTC: 0, ETH: 0, SOL: 0, DOGE: 0 };
const binanceLastPx  = { BTC: 0, ETH: 0, SOL: 0, DOGE: 0 };
const binanceWs      = { BTC: null, ETH: null, SOL: null, DOGE: null };

let emitFn = () => {};
let logFn  = () => {};

// ── Safe math ─────────────────────────────────────────────────────────────────
function addMoney(a, b) { return +((a * 100 + b * 100) / 100).toFixed(2); }
function subMoney(a, b) { return +((a * 100 - b * 100) / 100).toFixed(2); }
function calcFee(shares, price) {
  return +(shares * CRYPTO_FEE_RATE * price * (1 - price)).toFixed(4);
}

// ── Persistence ───────────────────────────────────────────────────────────────
function loadConfig() {
  try {
    if (fs.existsSync(CONFIG_FILE)) {
      const raw = JSON.parse(fs.readFileSync(CONFIG_FILE, 'utf8'));
      config = { ...DEFAULT_CONFIG, ...raw };
      config.markets = { ...DEFAULT_CONFIG.markets, ...(raw.markets || {}) };
    }
  } catch (e) { log(`⚠️  Config: ${e.message}`); }
}
function saveConfig() { fs.writeFileSync(CONFIG_FILE, JSON.stringify(config, null, 2)); }

function loadState() {
  try {
    if (fs.existsSync(TRADES_FILE)) {
      const raw = JSON.parse(fs.readFileSync(TRADES_FILE, 'utf8'));
      state = { ...state, ...raw };
      if (state.openTrades.length > 0) {
        log(`♻️  Found ${state.openTrades.length} open trade(s) from previous session`);
        if (!IS_LIVE) {
          for (const t of state.openTrades) state.balance = addMoney(state.balance, t.cost);
          state.openTrades = [];
          saveState();
        }
      }
    }
  } catch (e) { log(`⚠️  State: ${e.message}`); }
}
function saveState() { fs.writeFileSync(TRADES_FILE, JSON.stringify(state, null, 2)); }

function loadEquity() {
  try {
    if (fs.existsSync(EQUITY_FILE)) {
      equityCurve = JSON.parse(fs.readFileSync(EQUITY_FILE, 'utf8'));
      if (!Array.isArray(equityCurve)) equityCurve = [];
      if (equityCurve.length > 500) equityCurve = equityCurve.slice(-500);
    }
  } catch (_) { equityCurve = []; }
}
function saveEquity() { fs.writeFileSync(EQUITY_FILE, JSON.stringify(equityCurve)); }

function recordEquity() {
  equityCurve.push({ ts: Date.now(), balance: +state.balance.toFixed(2) });
  if (equityCurve.length > 500) equityCurve = equityCurve.slice(-500);
  saveEquity();
}

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
function priceDec(asset) {
  if (asset === 'DOGE') return 5;
  if (asset === 'SOL')  return 3;
  return 2;
}

// ── Wallet init ───────────────────────────────────────────────────────────────
function initWallet() {
  if (!PRIVATE_KEY) { log('⚠️  No PRIVATE_KEY set'); return; }
  wallet = new ethers.Wallet(PRIVATE_KEY);
  log(`🔑 Wallet: ${wallet.address}`);
  log(`💼 Funder: ${FUNDER_ADDRESS}`);
  log(`🔏 Signature type: ${SIGNATURE_TYPE}`);
}

// ── L1 auth — EIP-712 ClobAuth ────────────────────────────────────────────────
async function initApiCreds() {
  try {
    const timestamp = Math.floor(Date.now() / 1000).toString();
    const nonce     = Math.floor(Math.random() * 1e10).toString();

    const domain = {
      name:    'ClobAuthDomain',
      version: '1',
      chainId: CHAIN_ID,
    };
    const types = {
      ClobAuth: [
        { name: 'address',   type: 'address' },
        { name: 'timestamp', type: 'string'  },
        { name: 'nonce',     type: 'uint256' },
        { name: 'message',   type: 'string'  },
      ],
    };
    const value = {
      address:   wallet.address,
      timestamp: timestamp,
      nonce:     parseInt(nonce),
      message:   'This message attests that I control the given wallet',
    };

    const sig = await wallet._signTypedData(domain, types, value);

    const headers = {
      'POLY_ADDRESS':        FUNDER_ADDRESS,
      'POLY_SIGNATURE':      sig,
      'POLY_TIMESTAMP':      timestamp,
      'POLY_NONCE':          nonce,
      'POLY_SIGNATURE_TYPE': SIGNATURE_TYPE,
      'Content-Type':        'application/json',
    };

    // Try GET first — load existing creds
    const res = await fetch(`${CLOB_REST}/auth/api-key`, { headers, timeout: 10000 });
    if (res.ok) {
      apiCreds = await res.json();
      log(`✅ API creds loaded: ${apiCreds.apiKey}`);
      return;
    }

    // POST — create new creds
    const res2 = await fetch(`${CLOB_REST}/auth/api-key`, {
      method: 'POST', headers, timeout: 10000,
    });
    if (res2.ok) {
      apiCreds = await res2.json();
      log(`✅ API creds created: ${apiCreds.apiKey}`);
    } else {
      const err = await res2.text();
      log(`⚠️  API creds failed: ${err}`);
    }
  } catch (e) { log(`⚠️  initApiCreds: ${e.message}`); }
}

// ── L2 HMAC headers ───────────────────────────────────────────────────────────
function buildL2Headers(method, path, body = '') {
  if (!apiCreds) throw new Error('API creds not initialized');
  const timestamp = Math.floor(Date.now() / 1000).toString();
  const nonce     = Math.floor(Math.random() * 1e10).toString();
  const message   = timestamp + method.toUpperCase() + path + body;
  const hmac = crypto.createHmac('sha256', Buffer.from(apiCreds.secret, 'base64'))
    .update(message).digest('base64');
  return {
    'POLY_ADDRESS':        FUNDER_ADDRESS,
    'POLY_SIGNATURE':      hmac,
    'POLY_TIMESTAMP':      timestamp,
    'POLY_NONCE':          nonce,
    'POLY_API_KEY':        apiCreds.apiKey,
    'POLY_PASSPHRASE':     apiCreds.passphrase,
    'POLY_SIGNATURE_TYPE': SIGNATURE_TYPE,
    'Content-Type':        'application/json',
  };
}

// ── EIP-712 order signing ─────────────────────────────────────────────────────
async function buildSignedOrder(tokenId, side, price, size) {
  if (!wallet) throw new Error('No wallet');
  const makerAmount = side === 'BUY'
    ? Math.round(price * size * 1e6)
    : Math.round(size * 1e6);
  const takerAmount = side === 'BUY'
    ? Math.round(size * 1e6)
    : Math.round(price * size * 1e6);
  const nonce      = Math.floor(Date.now() / 1000);
  const feeRateBps = Math.round(CRYPTO_FEE_RATE * price * (1 - price) * 10000);

  const orderData = {
    salt:          nonce.toString(),
    maker:         FUNDER_ADDRESS,
    signer:        wallet.address,
    taker:         '0x0000000000000000000000000000000000000000',
    tokenId:       tokenId,
    makerAmount:   makerAmount.toString(),
    takerAmount:   takerAmount.toString(),
    expiration:    '0',
    nonce:         '0',
    feeRateBps:    feeRateBps.toString(),
    side:          side === 'BUY' ? '0' : '1',
    signatureType: SIGNATURE_TYPE,
  };

  const domain = {
    name:              'Polymarket CTF Exchange',
    version:           '1',
    chainId:           CHAIN_ID,
    verifyingContract: CTF_EXCHANGE,
  };
  const types = {
    Order: [
      { name: 'salt',          type: 'uint256' },
      { name: 'maker',         type: 'address' },
      { name: 'signer',        type: 'address' },
      { name: 'taker',         type: 'address' },
      { name: 'tokenId',       type: 'uint256' },
      { name: 'makerAmount',   type: 'uint256' },
      { name: 'takerAmount',   type: 'uint256' },
      { name: 'expiration',    type: 'uint256' },
      { name: 'nonce',         type: 'uint256' },
      { name: 'feeRateBps',    type: 'uint256' },
      { name: 'side',          type: 'uint8'   },
      { name: 'signatureType', type: 'uint8'   },
    ],
  };

  const signature = await wallet._signTypedData(domain, types, {
    salt:          BigInt(orderData.salt),
    maker:         orderData.maker,
    signer:        orderData.signer,
    taker:         orderData.taker,
    tokenId:       BigInt(tokenId),
    makerAmount:   BigInt(orderData.makerAmount),
    takerAmount:   BigInt(orderData.takerAmount),
    expiration:    BigInt(orderData.expiration),
    nonce:         BigInt(orderData.nonce),
    feeRateBps:    BigInt(orderData.feeRateBps),
    side:          parseInt(orderData.side),
    signatureType: parseInt(orderData.signatureType),
  });

  return { ...orderData, signature };
}

// ── Place FOK buy ─────────────────────────────────────────────────────────────
async function placeRealBuyOrder(tokenId, price, size) {
  try {
    const order   = await buildSignedOrder(tokenId, 'BUY', price, size);
    const bodyStr = JSON.stringify({ order, orderType: 'FOK' });
    const headers = buildL2Headers('POST', '/order', bodyStr);
    const res     = await fetch(`${CLOB_REST}/order`, {
      method: 'POST', headers, body: bodyStr, timeout: 8000,
    });
    const data = await res.json();
    if (data.success || data.orderID || data.id) {
      log(`✅ BUY filled: ${data.orderID||data.id} token=...${tokenId.toString().slice(-6)} price=${price} size=${size}`);
      return { success: true, orderId: data.orderID || data.id };
    } else {
      log(`⚠️  BUY failed: ${JSON.stringify(data)}`);
      return { success: false };
    }
  } catch (e) {
    log(`⚠️  placeRealBuyOrder: ${e.message}`);
    return { success: false };
  }
}

// ── Place GTC limit sell at TP ────────────────────────────────────────────────
async function placeRealSellOrder(tokenId, price, size) {
  try {
    const order   = await buildSignedOrder(tokenId, 'SELL', price, size);
    const bodyStr = JSON.stringify({ order, orderType: 'GTC' });
    const headers = buildL2Headers('POST', '/order', bodyStr);
    const res     = await fetch(`${CLOB_REST}/order`, {
      method: 'POST', headers, body: bodyStr, timeout: 8000,
    });
    const data = await res.json();
    if (data.success || data.orderID || data.id) {
      log(`✅ SELL limit placed @ ${price}: ${data.orderID||data.id}`);
      return { success: true, orderId: data.orderID || data.id };
    } else {
      log(`⚠️  SELL failed: ${JSON.stringify(data)}`);
      return { success: false };
    }
  } catch (e) {
    log(`⚠️  placeRealSellOrder: ${e.message}`);
    return { success: false };
  }
}

// ── Batch sell — POST /orders — 4:55 exit ────────────────────────────────────
async function placeBatchSellOrders(trades) {
  if (!trades.length) return;
  try {
    const orders = [];
    for (const t of trades) {
      const tokenId  = t.side === 'UP' ? t.upToken : t.dnToken;
      const curPrice = getPrice(tokenId);
      if (curPrice <= 0) continue;
      const order = await buildSignedOrder(tokenId, 'SELL', curPrice, t.shares);
      orders.push({ order, orderType: 'FOK' });
    }
    if (!orders.length) return;
    const bodyStr = JSON.stringify(orders);
    const headers = buildL2Headers('POST', '/orders', bodyStr);
    const res = await fetch(`${CLOB_REST}/orders`, {
      method: 'POST', headers, body: bodyStr, timeout: 10000,
    });
    const data = await res.json();
    log(`✅ Batch sell: ${orders.length} orders | ${JSON.stringify(data).slice(0,80)}`);
    return data;
  } catch (e) { log(`⚠️  placeBatchSellOrders: ${e.message}`); }
}

// ── Cancel TP order ───────────────────────────────────────────────────────────
async function cancelOrder(orderId) {
  try {
    const bodyStr = JSON.stringify({ orderID: orderId });
    const headers = buildL2Headers('DELETE', '/order', bodyStr);
    await fetch(`${CLOB_REST}/order`, {
      method: 'DELETE', headers, body: bodyStr, timeout: 5000,
    });
  } catch (e) { log(`⚠️  cancelOrder ${orderId}: ${e.message}`); }
}

// ── Fetch real balance ────────────────────────────────────────────────────────
async function fetchRealBalance() {
  try {
    if (!apiCreds) return;
    const headers = buildL2Headers('GET', '/balance', '');
    const res  = await fetch(`${CLOB_REST}/balance`, { headers, timeout: 5000 });
    const data = await res.json();
    if (data.balance !== undefined) {
      state.balance = +parseFloat(data.balance).toFixed(2);
      log(`💰 Real balance: $${state.balance}`);
    }
  } catch (e) { log(`⚠️  fetchRealBalance: ${e.message}`); }
}

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
        const change = price - binanceLastPx[asset];
        const dec    = priceDec(asset);
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

function checkSignals() {
  if (!botRunning) return;
  if (windowElapsed() >= config.exitAtSecond) return;

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
      log(`📊 [${marketId}] ${last.absChange.toFixed(5)} < ${required.toFixed(5)} — skip`);
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

    log(`📊 [${marketId}] avg=${avg.toFixed(5)} last=${last.direction} ${last.absChange.toFixed(5)} | token=${tokenPrice.toFixed(3)} ✅ SIGNAL ${newDirection}`);
    placeTrade(marketId, w, cws, newDirection, last.absChange, avg, tokenPrice);
  }
}

async function placeTrade(marketId, w, cws, direction, move, avg, tokenPrice) {
  const shares    = config.shares;
  const rawCost   = +(tokenPrice * shares).toFixed(2);
  const fee       = calcFee(shares, tokenPrice);
  const totalCost = +(rawCost + fee).toFixed(2);

  if (state.balance < totalCost) {
    log(`💸 [${marketId}] Low balance $${state.balance} need $${totalCost}`);
    return;
  }

  const tokenId = direction === 'UP' ? w.upToken : w.dnToken;

  if (IS_LIVE) {
    if (!apiCreds) {
      log(`❌ [${marketId}] API creds not ready — skipping`);
      return;
    }
    log(`📤 [${marketId}] FOK BUY ${direction} token=...${tokenId.toString().slice(-6)} price=${tokenPrice} size=${shares}`);
    const result = await placeRealBuyOrder(tokenId, tokenPrice, shares);
    if (!result.success) {
      log(`❌ [${marketId}] BUY rejected — skipping`);
      return;
    }
    // Place TP sell immediately after fill
    const tpResult  = await placeRealSellOrder(tokenId, config.takeProfit, shares);
    const tpOrderId = tpResult.success ? tpResult.orderId : null;

    state.balance   = subMoney(state.balance, totalCost);
    state.totalFees = +(state.totalFees + fee).toFixed(4);

    const id = tradeId();
    state.openTrades.push({
      id, marketId, windowStart: cws,
      asset: MARKET_ASSETS[marketId], side: direction,
      entryPrice: tokenPrice, shares, rawCost, fee, cost: totalCost,
      tp: config.takeProfit, upToken: w.upToken, dnToken: w.dnToken,
      tokenId, tpOrderId,
      move: +move.toFixed(6), avg: +avg.toFixed(6),
      assetPriceAtEntry: +binancePrices[MARKET_ASSETS[marketId]].toFixed(6),
      openedAt: new Date().toISOString(), floatingPnl: 0,
      exitReason: null, isReal: true,
    });
    const wstKey = `${marketId}:${cws}`;
    if (!windowState[wstKey]) windowState[wstKey] = { trades: 0 };
    windowState[wstKey].trades++;
    recordEquity(); saveState();
    log(`🚀 [${marketId}] REAL ${direction} [${id}] token=${tokenPrice} shares=${shares} cost=$${rawCost} fee=$${fee} TP=${tpOrderId||'failed'} bal=$${state.balance}`);

  } else {
    state.balance   = subMoney(state.balance, totalCost);
    state.totalFees = +(state.totalFees + fee).toFixed(4);
    const id = tradeId();
    state.openTrades.push({
      id, marketId, windowStart: cws,
      asset: MARKET_ASSETS[marketId], side: direction,
      entryPrice: tokenPrice, shares, rawCost, fee, cost: totalCost,
      tp: config.takeProfit, upToken: w.upToken, dnToken: w.dnToken,
      tokenId,
      move: +move.toFixed(6), avg: +avg.toFixed(6),
      assetPriceAtEntry: +binancePrices[MARKET_ASSETS[marketId]].toFixed(6),
      openedAt: new Date().toISOString(), floatingPnl: 0,
      exitReason: null, isReal: false,
    });
    const wstKey = `${marketId}:${cws}`;
    if (!windowState[wstKey]) windowState[wstKey] = { trades: 0 };
    windowState[wstKey].trades++;
    recordEquity(); saveState();
    log(`🚀 [${marketId}] DEMO ${direction} [${id}] token=${tokenPrice} shares=${shares} cost=$${rawCost} fee=$${fee} bal=$${state.balance}`);
  }

  emitFn('snapshot', buildDashboardSnapshot());
}

async function checkTP() {
  const toClose = [];
  for (const t of state.openTrades) {
    if (!t.upToken || !t.dnToken) continue;
    const tokenId  = t.side === 'UP' ? t.upToken : t.dnToken;
    const curPrice = getPrice(tokenId);
    if (curPrice <= 0) continue;
    t.floatingPnl = +((curPrice - t.entryPrice) * t.shares).toFixed(4);
    if (curPrice >= config.takeProfit) toClose.push({ trade: t, exitPrice: curPrice });
  }
  for (const { trade: t, exitPrice } of toClose) {
    await closeTrade(t, exitPrice, 'TP');
  }
}

async function closeTrade(t, exitPrice, reason) {
  const proceeds = +(exitPrice * t.shares).toFixed(2);
  const pnl      = +(proceeds - t.cost).toFixed(4);
  state.balance   = addMoney(state.balance, proceeds);
  state.totalPnl  = +(state.totalPnl + pnl).toFixed(4);
  state.openTrades = state.openTrades.filter(x => x.id !== t.id);
  state.closedTrades.push({
    ...t, exitPrice, proceeds, realizedPnl: pnl,
    closedAt: new Date().toISOString(), exitReason: reason,
  });
  recordEquity(); saveState();
  log(`${pnl >= 0 ? '🟢' : '🔴'} [${t.marketId}] ${reason} ${t.side} [${t.id}] entry=${t.entryPrice.toFixed(3)} exit=${exitPrice.toFixed(3)} pnl=$${pnl.toFixed(2)} bal=$${state.balance.toFixed(2)}`);
  emitFn('snapshot', buildDashboardSnapshot());
}

async function checkWindowExit() {
  const elapsed = windowElapsed();
  const cws     = currentWindowStart();
  if (elapsed < config.exitAtSecond) return;

  for (const marketId of Object.keys(MARKET_ASSETS)) {
    if (!config.markets[marketId]) continue;
    if (exitFiredWindow[marketId] === cws) continue;
    const openForMarket = state.openTrades.filter(t => t.marketId === marketId);
    if (!openForMarket.length) { exitFiredWindow[marketId] = cws; continue; }
    exitFiredWindow[marketId] = cws;
    log(`⏰ [${marketId}] ${config.exitAtSecond}s EXIT — closing ${openForMarket.length} position(s)`);

    if (IS_LIVE && apiCreds) {
      for (const t of openForMarket) {
        if (t.tpOrderId) { await cancelOrder(t.tpOrderId); }
      }
      await placeBatchSellOrders(openForMarket);
      await fetchRealBalance();
    }

    for (const t of openForMarket) {
      const tokenId  = t.side === 'UP' ? t.upToken : t.dnToken;
      const curPrice = getPrice(tokenId);
      await closeTrade(t, curPrice > 0 ? curPrice : t.entryPrice, 'WINDOW_EXIT');
    }
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
  let prices   = mkt.outcomePrices;
  let outcomes = mkt.outcomes;
  if (typeof prices   === 'string') { try { prices   = JSON.parse(prices);   } catch (_) { prices   = null; } }
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
      const res = await findMarketForTs(MARKET_SLUGS[marketId], cws);
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
    elapsed, remaining: Math.max(0, WINDOW_SIZE - elapsed),
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
    isSignal:     last.absChange > required && avg > 0 && !trending && elapsed < config.exitAtSecond,
    trending,
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
    balance:      +state.balance.toFixed(2),
    totalPnl:     +state.totalPnl.toFixed(2),
    totalFees:    +state.totalFees.toFixed(2),
    openTrades:   state.openTrades,
    closedTrades: state.closedTrades.slice(-80),
    equityCurve,
    botRunning,
    isLive:       IS_LIVE,
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

function updateConfig(newCfg) {
  if (newCfg.shares         !== undefined) config.shares         = Math.max(1,    Math.min(1000, Number(newCfg.shares)));
  if (newCfg.moveMultiplier !== undefined) config.moveMultiplier = Math.max(0.1,  Math.min(5,    Number(newCfg.moveMultiplier)));
  if (newCfg.blockSize      !== undefined) config.blockSize      = Math.max(5,    Math.min(300,  Number(newCfg.blockSize)));
  if (newCfg.tokenMin       !== undefined) config.tokenMin       = Math.max(0.01, Math.min(0.49, Number(newCfg.tokenMin)));
  if (newCfg.tokenMax       !== undefined) config.tokenMax       = Math.max(0.51, Math.min(0.99, Number(newCfg.tokenMax)));
  if (newCfg.trendBuckets   !== undefined) config.trendBuckets   = Math.max(1,    Math.min(10,   Number(newCfg.trendBuckets)));
  if (newCfg.exitAtSecond   !== undefined) config.exitAtSecond   = Math.max(60,   Math.min(299,  Number(newCfg.exitAtSecond)));
  if (newCfg.takeProfit     !== undefined) config.takeProfit     = Math.max(0.50, Math.min(0.99, Number(newCfg.takeProfit)));
  if (newCfg.historyWindow  !== undefined) config.historyWindow  = Math.max(300,  Math.min(3600, Number(newCfg.historyWindow)));
  if (newCfg.markets        !== undefined) config.markets        = { ...config.markets, ...newCfg.markets };
  saveConfig();
  log(`⚙️  Config: shares=${config.shares} mult=${config.moveMultiplier} block=${config.blockSize}s tp=${config.takeProfit} exit=${config.exitAtSecond}s`);
  emitFn('snapshot', buildDashboardSnapshot());
}

function setBotRunning(val) {
  botRunning = val;
  log(val ? '▶️  Bot STARTED' : '⏹️  Bot STOPPED');
  emitFn('snapshot', buildDashboardSnapshot());
}

let timer = null;
async function tick() {
  try {
    prune();
    await refreshMarkets();
    await pollPrices();
    updateFloating();
    if (botRunning) {
      await checkWindowExit();
      checkSignals();
      await checkTP();
    }
    if (IS_LIVE && apiCreds && Math.floor(Date.now() / 1000) % 30 === 0) {
      await fetchRealBalance();
    }
    emitFn('snapshot', buildDashboardSnapshot());
  } catch (e) { log(`⚠️  tick: ${e.message}`); }
}

async function start(emit, logEmit) {
  emitFn = emit; logFn = logEmit;
  loadConfig(); loadState(); loadEquity();
  log(`🚀 HYDRA ${IS_LIVE ? '🔴 LIVE' : '🟡 DEMO'} — BTC+ETH+SOL+DOGE 5m`);
  log(`   shares=${config.shares} mult=${config.moveMultiplier} block=${config.blockSize}s tp=${config.takeProfit} exit=${config.exitAtSecond}s`);
  log(`   Mode: ${IS_LIVE ? 'REAL TRADING' : 'DEMO'} | sig_type=${SIGNATURE_TYPE}`);

  if (IS_LIVE) {
    if (!PRIVATE_KEY || !FUNDER_ADDRESS) {
      log('❌ PRIVATE_KEY or FUNDER_ADDRESS missing'); return;
    }
    initWallet();
    await initApiCreds();
    if (apiCreds) await fetchRealBalance();
    else log('⚠️  Running without API creds — orders will fail until auth is fixed');
  } else {
    log(`💰 Demo balance: $${state.balance}`);
  }

  for (const asset of Object.keys(BINANCE_STREAMS)) connectBinance(asset);
  await tick();
  timer = setInterval(tick, 1000);
  setInterval(async function() {
    await pollPrices();
    updateFloating();
    if (botRunning) await checkTP();
    emitFn('prices', {
      btcPrice:  +binancePrices.BTC.toFixed(2),
      ethPrice:  +binancePrices.ETH.toFixed(2),
      solPrice:  +binancePrices.SOL.toFixed(3),
      dogePrice: +binancePrices.DOGE.toFixed(5),
    });
  }, 2000);
}

function stop() {
  clearInterval(timer);
  for (const asset of Object.keys(binanceWs)) {
    if (binanceWs[asset]) { try { binanceWs[asset].terminate(); } catch(_){} }
  }
}

module.exports = { start, stop, buildDashboardSnapshot, updateConfig, setBotRunning };
