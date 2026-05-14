'use strict';

const express   = require('express');
const http      = require('http');
const { Server } = require('socket.io');
const path      = require('path');
const bot       = require('./bot');

const PORT = process.env.PORT || 3000;

const app    = express();
const server = http.createServer(app);
const io     = new Server(server, { cors: { origin: '*' } });

app.use(express.static(path.join(__dirname, 'public')));

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ─── Socket.io ────────────────────────────────────────────────────────────────
io.on('connection', (socket) => {
  console.log(`[socket] client connected: ${socket.id}`);
  // Send current snapshot immediately on connect
  const snap = bot.buildDashboardSnapshot();
  socket.emit('snapshot', snap);

  socket.on('disconnect', () => {
    console.log(`[socket] client disconnected: ${socket.id}`);
  });
});

// Bot emit helpers
function emit(event, data) {
  io.emit(event, data);
}

function logEmit(line) {
  io.emit('log', line);
}

// ─── Start ────────────────────────────────────────────────────────────────────
server.listen(PORT, () => {
  console.log(`[server] listening on http://localhost:${PORT}`);
  bot.start(emit, logEmit);
});

process.on('SIGTERM', () => {
  bot.stop();
  server.close();
});
