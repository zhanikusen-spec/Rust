// Rust: Остров — сервер (раздаёт игру и принимает WebSocket-игроков)
const http = require('http'), fs = require('fs'), path = require('path'), zlib = require('zlib');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const MAX = +process.env.MAX_PLAYERS || 50;
const INDEX = path.join(__dirname, 'index.html');
const players = new Map();
let nextId = 0;

const server = http.createServer((req, res) => {
  const u = req.url.split('?')[0];
  if (u === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: 1, players: players.size, max: MAX }));
    return;
  }
  if (u === '/' || u === '/index.html') {
    const h = { 'Content-Type': 'text/html; charset=utf-8' };
    const src = fs.createReadStream(INDEX);
    src.on('error', () => { res.writeHead(500); res.end('index.html not found'); });
    if (/\bgzip\b/.test(req.headers['accept-encoding'] || '')) {
      h['Content-Encoding'] = 'gzip';
      res.writeHead(200, h);
      src.pipe(zlib.createGzip()).pipe(res);
    } else {
      res.writeHead(200, h);
      src.pipe(res);
    }
    return;
  }
  res.writeHead(404);
  res.end('not found');
});

const wss = new WebSocketServer({ server, maxPayload: 16 * 1024 });

const send = (ws, o) => { if (ws.readyState === 1) { try { ws.send(JSON.stringify(o)); } catch (e) {} } };
const broadcast = (o, except) => {
  const d = JSON.stringify(o);
  for (const p of players.values()) if (p !== except && p.ws.readyState === 1) { try { p.ws.send(d); } catch (e) {} }
};
const clean = n => String(n || '').replace(/[<>&"'`]/g, '').trim().slice(0, 20) || 'Игрок';

wss.on('connection', ws => {
  let me = null;
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', raw => {
    let m;
    try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m.t !== 'string') return;

    if (m.t === 'join') {
      if (me) return;
      if (players.size >= MAX) { send(ws, { t: 'full' }); ws.close(); return; }
      me = { id: ++nextId, ws, name: clean(m.name), s: null, dead: 0, lastDied: 0 };
      players.set(me.id, me);
      send(ws, {
        t: 'welcome', id: me.id,
        players: [...players.values()].filter(p => p !== me).map(p => ({ id: p.id, name: p.name, s: p.s }))
      });
      broadcast({ t: 'join', id: me.id, name: me.name }, me);
      console.log('+', me.name, me.id, 'онлайн:', players.size);
      return;
    }
    if (!me) return;

    if (m.t === 's') {
      m.id = me.id;
      me.s = m;
      me.dead = m.d ? 1 : 0;
      broadcast(m, me);
    } else if (m.t === 'hit') {
      const target = players.get(+m.id);
      if (!target || target === me || target.dead || me.dead) return;
      const d = Math.max(0, Math.min(120, +m.d || 0));
      if (d > 0) send(target.ws, { t: 'dmg', d, from: me.id });
    } else if (m.t === 'died') {
      const now = Date.now();
      if (now - me.lastDied < 2000) return;
      me.lastDied = now;
      me.dead = 1;
      const killer = players.get(+m.by);
      const k = killer && killer !== me ? killer : null;
      const feed = { t: 'feed', k: k ? k.name : null, v: me.name, kid: k ? k.id : 0, vid: me.id };
      for (const p of players.values()) send(p.ws, feed);
    }
  });

  ws.on('close', () => {
    if (!me) return;
    players.delete(me.id);
    broadcast({ t: 'leave', id: me.id });
    console.log('-', me.name, me.id, 'онлайн:', players.size);
  });
  ws.on('error', () => {});
});

// отключаем «мёртвые» соединения и не даём Render усыпить активное соединение
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch (e) {}
  }
}, 25000);

server.listen(PORT, () => console.log('Сервер запущен на порту', PORT));
