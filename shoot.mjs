// Deterministic headless screenshot via CDP (real clock — virtual-time races MapLibre's
// async tile paint). No deps: Node 26 has global fetch + WebSocket.
// Usage: node shoot.mjs <url> <out.png> [waitMs]
const [,, URL_ARG, OUT, WAIT = '7000'] = process.argv;

const list = await (await fetch('http://localhost:9222/json')).json();
const page = list.find(t => t.type === 'page') || list[0];
const ws = new WebSocket(page.webSocketDebuggerUrl);
await new Promise(r => ws.addEventListener('open', r, {once:true}));

let id = 0; const pending = new Map();
ws.addEventListener('message', e => {
  const m = JSON.parse(e.data);
  if (m.id && pending.has(m.id)) { pending.get(m.id)(m.result); pending.delete(m.id); }
});
const cmd = (method, params={}) => new Promise(res => { const i = ++id;
  pending.set(i, res); ws.send(JSON.stringify({id:i, method, params})); });

await cmd('Page.enable');
await cmd('Runtime.enable');
await cmd('Page.navigate', {url: URL_ARG});
await new Promise(r => setTimeout(r, +WAIT));    // real time for tiles + heat to paint
if (process.env.EVAL) {                            // optional: run JS, then settle
  const ev = await cmd('Runtime.evaluate', {expression: process.env.EVAL});
  console.log('eval:', JSON.stringify(ev.result));
  await new Promise(r => setTimeout(r, 1200));
}
const shot = await cmd('Page.captureScreenshot', {format:'png'});
const fs = await import('node:fs');
fs.writeFileSync(OUT, Buffer.from(shot.data, 'base64'));
console.log('wrote', OUT, fs.statSync(OUT).size, 'bytes');
ws.close();
process.exit(0);
