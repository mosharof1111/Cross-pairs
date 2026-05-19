const { start, stop, buildDashboardSnapshot, updateConfig, setBotRunning } = require('./bot');
const express = require('express');
const http    = require('http');
const { Server } = require('socket.io');
const path    = require('path');

const app    = express();
const server = http.createServer(app);
const io     = new Server(server);

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

app.post('/config', (req, res) => {
  try { updateConfig(req.body); res.json({ ok: true }); }
  catch (e) { res.status(400).json({ error: e.message }); }
});
app.post('/start', (req, res) => { setBotRunning(true);  res.json({ ok: true }); });
app.post('/stop',  (req, res) => { setBotRunning(false); res.json({ ok: true }); });

const logs = [];
io.on('connection', (socket) => {
  socket.emit('snapshot', buildDashboardSnapshot());
  logs.slice(-100).forEach(l => socket.emit('log', l));
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`🚀 Server on port ${PORT}`));

start(
  (event, data) => io.emit(event, data),
  (line) => { logs.push(line); if (logs.length > 500) logs.shift(); io.emit('log', line); }
);
