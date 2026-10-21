'use strict';

const http = require('node:http');
const https = require('node:https');
const dgram = require('node:dgram');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const crypto = require('node:crypto');
const { WebSocketServer } = require('ws');
const { execFile, exec } = require('node:child_process');
const { ensureCert } = require('./cert');

const PORT = Number(process.argv[2]) || 8081;
const STUN_PORT = Number(process.argv[3]) || 3478;
// HTTPS 端口：给手机用。http://局域网IP 不是安全上下文，OPFS/落盘 API 不可用，
// 手机走 https 才能边收边写盘（自签证书，手机上点一次"继续访问"即可）。
const HTTPS_PORT = Number(process.argv[4]) || (PORT + 100);
const PUBLIC = path.join(__dirname, 'public');
const CERT_DIR = path.join(__dirname, '.certs');
const PID_FILE = path.join(__dirname, '.server.pid');
const STARTED_AT = new Date();

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.ico': 'image/x-icon',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
};

/* ---------------- 中转上传 / 直链下载 ----------------
 * 手机端收大文件走原生下载最稳（可暂停续传、不占内存、不崩）。
 * 发送端浏览器把文件流式上传到本服务（O(1) 内存），手机拿普通 https 链接下载。
 * 文件放系统临时目录，token 用 16 字节随机数，链接只经信令下发给接收方。
 */
const UPLOAD_DIR = path.join(os.tmpdir(), 'p2p-transfer-uploads');
try { fs.mkdirSync(UPLOAD_DIR, { recursive: true }); } catch {}

const uploads = new Map(); // token -> { name, size, path, createdAt, downloaded, requested }

// 已删除中转文件的“完成墓碑”。
// 为什么需要：手机点下载后页面进后台，轮询被浏览器节流/暂停；等它回到前台时，
// 电脑端早已发现下载完成并删掉中转文件，手机再查状态只会得到“不存在”，
// 于是误判成“没开始下载”并提示重试（点了还会下到 404 文本）。
// 保留 10 分钟，让手机端任何时候回来都能明确知道“已完成”。
const tombstones = new Map(); // token -> { name, size, at }
const TOMBSTONE_MS = 10 * 60 * 1000;

function deleteAllUploads() {
  for (const f of uploads.values()) {
    try { fs.unlinkSync(f.path); } catch {}
  }
  uploads.clear();
  tombstones.clear();
}

// 上传超过 1 小时没被删（接收方取消/断连）自动清理，避免残留占磁盘
setInterval(() => {
  const now = Date.now();
  for (const [token, f] of uploads) {
    if (now - f.createdAt > 60 * 60 * 1000) {
      try { fs.unlinkSync(f.path); } catch {}
      uploads.delete(token);
    }
  }
  for (const [token, t] of tombstones) {
    if (now - t.at > TOMBSTONE_MS) tombstones.delete(token);
  }
}, 10 * 60 * 1000).unref();

/* ---------------- 静态文件 ---------------- */

function serveStatic(req, res) {
  let rel = decodeURIComponent((req.url || '/').split('?')[0]);
  if (rel === '/' || rel === '') rel = 'index.html';
  rel = rel.replace(/^\/+/, '');

  const full = path.join(PUBLIC, rel);
  if (!full.startsWith(PUBLIC)) return send404(res);

  fs.stat(full, (err, st) => {
    if (err || !st.isFile()) return send404(res);
    res.writeHead(200, {
      'Content-Type': MIME[path.extname(full)] || 'application/octet-stream',
      'Content-Length': st.size,
      'Cache-Control': 'no-store',
    });
    fs.createReadStream(full).pipe(res);
  });
}

function send404(res) {
  res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('404');
}

// 直链下载：支持 Range 断点续传，Content-Disposition 强制走浏览器下载管理器。
// 手机浏览器中断后重下会带 Range，我们按区间返回；服务到文件末尾就算"下载完成"。
function serveDownload(req, res, f) {
  let st;
  try { st = fs.statSync(f.path); } catch { return send404(res); }
  const total = st.size;
  const disp = `attachment; filename*=UTF-8''${encodeURIComponent(f.name)}`;
  const range = req.headers.range;
  f.requested = true;   // 有下载请求进来，说明浏览器已开始下载

  let start = 0;
  let end = total - 1;
  let status = 200;

  if (range) {
    const m = /^bytes=(\d*)-(\d*)$/.exec(String(range).trim());
    if (m && (m[1] !== '' || m[2] !== '')) {
      if (m[1] === '') {
        const n = Number(m[2]);
        start = Math.max(0, total - n);
        end = total - 1;
      } else {
        start = Number(m[1]);
        end = m[2] === '' ? total - 1 : Math.min(Number(m[2]), total - 1);
      }
      if (isNaN(start) || isNaN(end) || start < 0 || start >= total || end < start) {
        res.writeHead(416, { 'Content-Range': `bytes */${total}` });
        return res.end();
      }
      status = 206;
    } else {
      res.writeHead(416, { 'Content-Range': `bytes */${total}` });
      return res.end();
    }
  }

  const headers = {
    'Content-Type': 'application/octet-stream',
    'Accept-Ranges': 'bytes',
    'Content-Disposition': disp,
    'Cache-Control': 'no-store',
    'Content-Length': end - start + 1,
  };
  if (status === 206) headers['Content-Range'] = `bytes ${start}-${end}/${total}`;
  res.writeHead(status, headers);

  const stream = fs.createReadStream(f.path, { start, end });
  stream.on('error', () => { try { res.destroy(); } catch {} });
  stream.pipe(res);
  if (end === total - 1) {
    res.on('finish', () => { f.downloaded = true; });
  }
}

function jsonRes(res, code, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(code, {
    'Content-Type': 'application/json; charset=utf-8',
    'Content-Length': Buffer.byteLength(body),
    'Cache-Control': 'no-store',
  });
  res.end(body);
}

/* ---------------- 控制接口 ---------------- */

// Windows 把网络分成 专用 / 公用 / 域。防火墙的默认策略是"公用网络一律拦入站"，
// 而很多人装 Node 时只放行了"专用"，于是换成手机热点、酒店 WiFi 就连不上了。
// 这里主动把每个网卡的网络类别查出来，直接告诉用户该不该放行。
const CATEGORY_NAME = { 0: '公用', 1: '专用', 2: '域' };
let netCategory = new Map(); // 网卡名 -> 0|1|2

function refreshNetCategory() {
  if (process.platform !== 'win32') return;
  execFile(
    'powershell',
    [
      '-NoProfile',
      '-NonInteractive',
      '-Command',
      // 中文网卡名必须强制 UTF-8 输出，否则 Node 按 UTF-8 解出来是乱码，匹配不上
      '[Console]::OutputEncoding=[System.Text.Encoding]::UTF8; ' +
        'Get-NetConnectionProfile | Select-Object InterfaceAlias,NetworkCategory | ConvertTo-Json -Compress',
    ],
    { windowsHide: true, timeout: 5000 },
    (err, stdout) => {
      if (err || !stdout) return;
      try {
        const arr = [].concat(JSON.parse(stdout));
        const m = new Map();
        for (const p of arr) {
          if (p && p.InterfaceAlias != null) m.set(p.InterfaceAlias, Number(p.NetworkCategory));
        }
        netCategory = m;
      } catch {}
    }
  );
}

// 列出本机所有可被局域网访问的地址，前端直接展示，省得用户自己 ipconfig
function localUrls() {
  const out = [];
  const seen = new Set();
  for (const [name, list] of Object.entries(os.networkInterfaces())) {
    for (const ni of list || []) {
      if (ni.family !== 'IPv4' || ni.internal) continue;
      if (seen.has(ni.address)) continue;
      seen.add(ni.address);
      const cat = netCategory.has(name) ? netCategory.get(name) : null;
      out.push({
        name,
        ip: ni.address,
        url: `http://${ni.address}:${PORT}`,
        httpsUrl: `https://${ni.address}:${HTTPS_PORT}`,
        category: cat,
        categoryName: cat === null ? null : CATEGORY_NAME[cat],
        // 公用网络的入站默认被拦，除非防火墙规则覆盖到公用
        risky: cat === 0,
      });
    }
  }
  return out;
}

// 服务就绪后用默认浏览器打开本机页面。只在 start.bat 设置了 OPEN_BROWSER 时触发，
// 避免直接 `npm start` 或跑测试时也弹浏览器。
function openBrowser(url) {
  try {
    if (process.platform === 'win32') {
      // Windows 下 start 是 cmd 内置命令，必须经 cmd 调用；空引号是窗口标题占位
      exec(`start "" "${url}"`, { windowsHide: true }, () => {});
    } else if (process.platform === 'darwin') {
      execFile('open', [url], () => {});
    } else {
      execFile('xdg-open', [url], () => {});
    }
  } catch {}
}

// 是否来自本机。关闭服务的权限只给本机，别人打开网页关不掉你的服务器。
// 注意两个点：
//  1) 授权判定必须用 socket 的真实来源地址，绝不能信 X-Forwarded-For —— 那个头
//     任何客户端都能随便填，填个 127.0.0.1 就能伪装成"本机"远程关掉服务；
//  2) 本机浏览器用局域网 IP（如 http://192.168.43.37:8081）打开页面时，连接的
//     来源地址是那块网卡自己的 IP 而不是 127.0.0.1，所以要把"本机网卡地址列表"
//     也算成本机，否则用户会被迫只能用 localhost 打开才能关服务。
function rawClientIp(req) {
  return (req.socket.remoteAddress || '').replace(/^::ffff:/, '');
}

function isOwnAddress(ip) {
  if (!ip) return false;
  for (const list of Object.values(os.networkInterfaces())) {
    for (const ni of list || []) {
      if (ni.family === 'IPv4' && ni.address === ip) return true;
    }
  }
  return false;
}

function isLocalRequest(req) {
  const ip = rawClientIp(req);
  return ip === '127.0.0.1' || ip === '::1' || ip.startsWith('127.') || isOwnAddress(ip);
}

let shuttingDown = false;

function shutdown(reason) {
  if (shuttingDown) return;
  shuttingDown = true;

  console.log(`\n  正在关闭（${reason}）…`);

  // 先给所有已连接的浏览器发个通知，让页面知道服务要没了
  for (const ws of wss.clients) {
    try {
      send(ws, '1009', { reason });
      ws.close(1001, 'server shutdown');
    } catch {}
  }

  clearInterval(sweep);
  try { stunSock && stunSock.close(); } catch {}

  let closed = 0;
  const onClose = () => {
    if (++closed >= 2) {
      deleteAllUploads();
      removePidFile();
      console.log('  已停止。');
      process.exit(0);
    }
  };
  try { server.close(onClose); } catch { onClose(); }
  try { httpsServer.close(onClose); } catch { onClose(); }

  // 兜底：2 秒内没退干净就强退，避免端口残留
  setTimeout(() => {
    removePidFile();
    process.exit(0);
  }, 2000).unref();
}

function removePidFile() {
  try {
    if (fs.existsSync(PID_FILE)) fs.unlinkSync(PID_FILE);
  } catch {}
}

function handleApi(req, res) {
  const url = (req.url || '').split('?')[0];

  // 极简自检页：纯 HTML，不加载任何 JS/CSS。
  // 手机能打开这一页 => 网络和端口都是通的，问题一定在别的环节。
  // 打不开 => 网络层就没通，跟代码无关。
  if (url === '/__diag') {
    const ip = clientIp(req);
    const key = bucketKey(ip);
    const peers = buckets.get(key) ? buckets.get(key).size : 0;
    const html = `<!doctype html>
<html lang="zh-CN"><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>连接自检</title>
<body style="margin:0;padding:26px;background:#0d1117;color:#e6edf3;
             font:16px/1.75 -apple-system,'Segoe UI',system-ui,sans-serif">
<h1 style="font-size:22px;color:#3fb950;margin:0 0 6px">✓ 网络是通的</h1>
<p style="color:#8b949e;margin:0 0 22px">
  能看到这一页，说明手机已经连上服务器，端口、防火墙、路由都没问题。<br>
  如果正式页面却打不开，那是页面本身的事，不是网络。
</p>
<div style="background:#161b22;border:1px solid #2d333b;border-radius:10px;padding:16px 18px">
  <div style="margin-bottom:10px"><span style="color:#6e7681">你的来源 IP　</span>
    <b style="font-family:monospace;color:#58a6ff">${ip}</b></div>
  <div style="margin-bottom:10px"><span style="color:#6e7681">所属分组　　</span>
    <b style="font-family:monospace;color:#58a6ff">${key}</b></div>
  <div><span style="color:#6e7681">同组在线设备</span>
    <b style="font-family:monospace;color:#58a6ff">${peers}</b>
    <span style="color:#6e7681;font-size:13px">（不含你自己）</span></div>
</div>
<p style="margin-top:22px">
  <a href="/" style="display:inline-block;padding:11px 20px;border-radius:8px;
     background:#58a6ff;color:#0d1117;text-decoration:none;font-weight:600">
     进入传输页面 →</a>
</p>
<p style="color:#6e7681;font-size:13px;margin-top:26px">
  判断方法：这个地址能打开、主页打不开 → 网络没问题；<br>
  两个都打不开 → 手机跟电脑不在同一个网络里。
</p>
</body></html>`;
    res.writeHead(200, {
      'Content-Type': 'text/html; charset=utf-8',
      'Cache-Control': 'no-store',
    });
    return res.end(html);
  }

  if (url.startsWith('/dl/')) {
    if (req.method !== 'GET') return jsonRes(res, 405, { ok: false, error: '只接受 GET' });
    const token = url.slice(4).split('?')[0];
    const f = uploads.get(token);
    if (!f) return send404(res);
    return serveDownload(req, res, f);
  }

  if (url === '/api/upload') {
    if (req.method !== 'POST') return jsonRes(res, 405, { ok: false, error: '只接受 POST' });
    // 手机↔手机也走本机中转（两台手机都不是"部署机"，不能按本机限制）。
    // 改为总量上限：待下载的中转文件总共不超过 8GB，防止塞满磁盘。
    const size = Number(req.headers['x-file-size'] || 0);
    if (!size || size <= 0) {
      return jsonRes(res, 400, { ok: false, error: '缺少文件名或大小' });
    }
    if (size > 8 * 1024 ** 3) {
      return jsonRes(res, 413, { ok: false, error: '单文件超过 8GB 上限' });
    }
    let inFlight = 0;
    for (const f of uploads.values()) inFlight += f.size;
    if (inFlight + size > 8 * 1024 ** 3) {
      return jsonRes(res, 507, { ok: false, error: '中转空间不足：待下载的文件太多，请等对方下完或稍后再试' });
    }
    let name = '';
    try { name = decodeURIComponent(String(req.headers['x-file-name'] || '')); } catch { name = String(req.headers['x-file-name'] || ''); }
    if (!name) {
      return jsonRes(res, 400, { ok: false, error: '缺少文件名' });
    }

    const token = crypto.randomBytes(16).toString('hex');
    const fpath = path.join(UPLOAD_DIR, token);
    const out = fs.createWriteStream(fpath);

    const fail = () => { try { fs.unlinkSync(fpath); } catch {} };
    out.on('error', () => { fail(); try { res.destroy(); } catch {} });
    req.on('error', fail);

    req.pipe(out);
    out.on('finish', () => {
      uploads.set(token, { name, size, path: fpath, createdAt: Date.now(), downloaded: false, requested: false });
      console.log(`  已接收中转上传: ${name} (${(size / 1048576).toFixed(1)} MB) → /dl/${token}`);
      jsonRes(res, 200, { ok: true, token, path: `/dl/${token}`, name, size });
    });
    return;
  }

  if (url === '/api/dl-status') {
    const token = String(new URL(req.url, 'http://x').searchParams.get('token') || '');
    const f = uploads.get(token);
    if (f) {
      return jsonRes(res, 200, {
        ok: true,
        downloaded: !!f.downloaded,
        started: !!f.requested,
        name: f.name,
        size: f.size,
      });
    }
    // 文件已被清理：如果它是“下载完成后被删”的，靠墓碑告诉调用方已完成
    const t = tombstones.get(token);
    if (t) {
      return jsonRes(res, 200, {
        ok: true,
        downloaded: true,
        deleted: true,
        started: true,
        name: t.name,
        size: t.size,
      });
    }
    return jsonRes(res, 200, { ok: false, downloaded: false, started: false });
  }

  if (url === '/api/delete') {
    if (req.method !== 'POST') return jsonRes(res, 405, { ok: false, error: '只接受 POST' });
    // 不限本机：token 是 16 字节随机数，只有经信令拿到链接的收发双方才可能持有，
    // 持有即有权删除（发送端取消、接收端取消时双方都可能发起清理）。
    const token = String(new URL(req.url, 'http://x').searchParams.get('token') || '');
    const f = uploads.get(token);
    if (!f) return jsonRes(res, 404, { ok: false, error: '不存在或已清理' });
    try { fs.unlinkSync(f.path); } catch {}
    // 已完整下载过的才留墓碑；中途取消/超时清理的不留（对端应看到“已结束”）
    if (f.downloaded) {
      tombstones.set(token, { name: f.name, size: f.size, at: Date.now() });
    }
    uploads.delete(token);
    return jsonRes(res, 200, { ok: true });
  }

  if (url === '/api/info') {
    return jsonRes(res, 200, {
      port: PORT,
      httpsPort: HTTPS_PORT,
      pid: process.pid,
      startedAt: STARTED_AT.toISOString(),
      uptimeSec: Math.round((Date.now() - STARTED_AT.getTime()) / 1000),
      urls: localUrls(),
      localOnly: isLocalRequest(req),
      clients: wss.clients.size,
      stunPort: stunOn ? STUN_PORT : null,
      node: process.version,
    });
  }

  if (url === '/api/shutdown') {
    if (req.method !== 'POST') {
      return jsonRes(res, 405, { ok: false, error: '只接受 POST' });
    }
    if (!isLocalRequest(req)) {
      return jsonRes(res, 403, {
        ok: false,
        error: '只有运行服务的那台机器才能关闭，远程设备不行',
      });
    }
    // 要求自定义头，挡住跨站表单/图片之类的伪造请求
    if (req.headers['x-shutdown'] !== '1') {
      return jsonRes(res, 400, { ok: false, error: '缺少确认头' });
    }
    jsonRes(res, 200, { ok: true, message: '服务正在关闭' });
    setTimeout(() => shutdown('网页内点击关闭'), 120);
    return;
  }

  return jsonRes(res, 404, { ok: false, error: 'unknown api' });
}

const requestHandler = (req, res) => {
  const p = (req.url || '').split('?')[0];
  if (p.startsWith('/api/') || p === '/__diag' || p.startsWith('/dl/')) return handleApi(req, res);
  serveStatic(req, res);
};

const server = http.createServer(requestHandler);

// 生成自签证书，SAN 覆盖 localhost + 所有本机 IPv4。换网络 IP 变了会自动重生成。
// 只收 IPv4：局域网传输基本走 IPv4，IPv6 加进 SAN 反而要处理压缩格式，收益极低。
const certHosts = new Set(['localhost', '127.0.0.1']);
for (const list of Object.values(os.networkInterfaces())) {
  for (const ni of list || []) {
    if (ni.family === 'IPv4' && ni.address) certHosts.add(ni.address);
  }
}
const { key, cert, fresh: certFresh } = ensureCert(CERT_DIR, [...certHosts]);
const httpsServer = https.createServer({ key, cert }, requestHandler);

/* ---------------- 内置 STUN ---------------- */

// 为什么必须要它：
// 浏览器出于隐私，会把 host 候选里的局域网 IP 换成 mDNS 名字（如 4f2a….local），
// 由对方的浏览器去解析。而 mDNS 走 UDP 5353 组播，跨设备时经常被 Windows 防火墙
// 或安卓系统的 mDNS 实现挡掉 → 两台真机永远"连接中"，同机两个标签页却正常。
// 有了 STUN，双方都能拿到带真实 IP 的 srflx 候选，直接绕开 mDNS 解析。
// 局域网用不到公网 STUN，自己起一个 20 行的就够了（RFC 5389 Binding）。
const STUN_MAGIC = 0x2112a442;
const XMASK = [0x21, 0x12, 0xa4, 0x42];
let stunOn = false;
let stunSock = null;

function xorMappedAttr(ip, port) {
  const b = Buffer.alloc(12);
  b.writeUInt16BE(0x0020, 0); // XOR-MAPPED-ADDRESS
  b.writeUInt16BE(8, 2);
  b[5] = 0x01; // IPv4
  b.writeUInt16BE(port ^ 0x2112, 6);
  const parts = ip.split('.');
  for (let i = 0; i < 4; i++) b[8 + i] = (Number(parts[i]) ^ XMASK[i]) & 0xff;
  return b;
}

function mappedAttr(ip, port) {
  const b = Buffer.alloc(12);
  b.writeUInt16BE(0x0001, 0); // MAPPED-ADDRESS（老实现兼容）
  b.writeUInt16BE(8, 2);
  b[5] = 0x01;
  b.writeUInt16BE(port, 6);
  ip.split('.').forEach((n, i) => { b[8 + i] = Number(n) & 0xff; });
  return b;
}

function startStun(port) {
  const sock = dgram.createSocket({ type: 'udp4' });

  sock.on('error', (err) => {
    stunOn = false;
    console.log(`  ⚠ STUN 未启动（UDP ${port}）：${err.message}`);
    console.log('    没有 STUN 时，手机和电脑之间可能一直"连接中"，只能同机测试。');
    console.log('');
    try { sock.close(); } catch {}
  });

  sock.on('message', (msg, rinfo) => {
    // Binding Request = 0x0001，且必须带 magic cookie
    if (msg.length < 20 || msg.readUInt16BE(0) !== 0x0001) return;
    if (msg.readUInt32BE(4) !== STUN_MAGIC) return;
    if (!/^\d+\.\d+\.\d+\.\d+$/.test(rinfo.address)) return; // 只处理 IPv4

    const body = Buffer.concat([xorMappedAttr(rinfo.address, rinfo.port), mappedAttr(rinfo.address, rinfo.port)]);
    const head = Buffer.alloc(20);
    head.writeUInt16BE(0x0101, 0); // Binding Success Response
    head.writeUInt16BE(body.length, 2);
    head.writeUInt32BE(STUN_MAGIC, 4);
    msg.copy(head, 8, 8, 20); // 原样回传 transaction id
    try {
      sock.send(Buffer.concat([head, body]), rinfo.port, rinfo.address);
    } catch {}
  });

  sock.bind(port, '0.0.0.0', () => {
    stunOn = true;
    console.log('  STUN 已启动:  udp://0.0.0.0:' + port + '（给浏览器下发真实 IP 候选）');
  });

  return sock;
}

/* ---------------- 分桶 ---------------- */

// 桶 = 房间。这里没有公网穿透，所以默认按来源 IP 分组：
// 同一局域网/同一出口的客户端落到同一个桶，打开网页即可互相看见。
// 回环地址也归入 internal，避免"本机用 localhost 打开却看不见同事"的坑。
const buckets = new Map(); // key -> Map<id, {id, ws}>

function isPrivateIp(ip) {
  if (!ip) return true;
  const s = ip.replace(/^::ffff:/, '');
  if (s === '::1' || s === '127.0.0.1') return true;
  return /^(10\.|192\.168\.|172\.(1[6-9]|2\d|3[01])\.)/.test(s);
}

function clientIp(req) {
  const xf = req.headers['x-forwarded-for'];
  const raw = xf ? String(xf).split(',')[0].trim() : (req.socket.remoteAddress || '');
  return raw.replace(/^::ffff:/, '');
}

function bucketKey(ip) {
  return isPrivateIp(ip) ? 'internal' : ip;
}

function getBucket(key) {
  let b = buckets.get(key);
  if (!b) {
    b = new Map();
    buckets.set(key, b);
  }
  return b;
}

function makeId(bucket) {
  for (;;) {
    const id = crypto.randomBytes(3).toString('hex');
    if (!bucket.has(id)) return id;
  }
}

/* ---------------- 信令 ---------------- */

const wss = new WebSocketServer({ noServer: true });

function handleUpgrade(req, socket, head) {
  if (!String(req.url || '').startsWith('/ws')) {
    socket.destroy();
    return;
  }
  wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, req));
}

server.on('upgrade', handleUpgrade);
httpsServer.on('upgrade', handleUpgrade);

function send(ws, type, data) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify({ type, data }));
}

function broadcastList(bucket, ws) {
  const now = Date.now();
  const list = [...bucket.values()].map((u) => ({
    id: u.id,
    ip: u.ip,
    // 在线时长能帮用户认出"刚打开的那个标签页"和"幽灵连接"
    onlineSec: Math.round((now - u.since) / 1000),
  }));
  send(ws, '1002', list);
}

function broadcastAll(bucket) {
  for (const u of bucket.values()) broadcastList(bucket, u.ws);
}

wss.on('connection', (ws, req) => {
  const ip = clientIp(req);
  const key = bucketKey(ip);
  const bucket = getBucket(key);
  const id = makeId(bucket);

  const user = { id, ws, ip, since: Date.now() };
  bucket.set(id, user);

  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  // 1001 注册成功（这里不下发 turns，纯局域网不需要中继）
  send(ws, '1001', { id, ip, roomId: key === 'internal' ? null : key, relay: null, stunPort: stunOn ? STUN_PORT : null });

  // 先给新人一份名单，再通知所有人刷新，最后让新人去发起连接
  broadcastList(bucket, ws);
  broadcastAll(bucket);
  send(ws, '1003', { id });

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }
    const { uid, targetId, type, data } = msg || {};
    if (!type) return;

    // 群聊广播：同一房间（局域网）内所有打开页面的设备都能收到，无需先直连。
    // 会回显给发送者自己，前端据此渲染自己的气泡。
    if (type === '9007') {
      const text = String((data && data.text) || '').slice(0, 2000).trim();
      if (!text) return;
      for (const u of bucket.values()) send(u.ws, '1007', { id: uid || id, text, ts: Date.now() });
      return;
    }

    if (!targetId) return;

    const target = bucket.get(targetId);
    if (!target) return; // 对方已离线，静默丢弃

    switch (type) {
      case '9002': // offer
        send(target.ws, '1005', { targetId: uid || id, offer: data && data.sdp });
        break;
      case '9003': // answer
        send(target.ws, '1006', { targetId: uid || id, answer: data && data.sdp });
        break;
      case '9001': // trickle candidate（本 demo 用非 trickle，保留兼容）
        send(target.ws, '1004', { targetId: uid || id, candidate: data && data.candidate });
        break;
      case '9999': // 心跳，忽略
        break;
      default:
        break;
    }
  });

  ws.on('close', () => {
    bucket.delete(id);
    broadcastAll(bucket);
    if (bucket.size === 0) buckets.delete(key);
  });

  ws.on('error', () => {
    try { ws.terminate(); } catch {}
  });
});

// 清理半死连接，避免幽灵成员出现在名单里
const sweep = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) {
      try { ws.terminate(); } catch {}
      continue;
    }
    ws.isAlive = false;
    try { ws.ping(); } catch {}
  }
}, 20000);
wss.on('close', () => clearInterval(sweep));

/* ---------------- 启动 ---------------- */

function onServerError(err) {
  if (err.code === 'EADDRINUSE') {
    const m = err.message.match(/:::?(\d+)/);
    const busyPort = m ? m[1] : PORT;
    console.error('');
    console.error(`  端口 ${busyPort} 已被占用。`);
    console.error('  可能已经有一个实例在跑了，直接打开网页试试；');
    console.error('  想换端口： npm start -- 9000');
    console.error('  想先关掉旧的：双击 stop.bat');
    console.error('');
    process.exit(1);
  }
  throw err;
}

server.on('error', onServerError);
httpsServer.on('error', onServerError);

let readyCount = 0;
function onReady() {
  if (++readyCount < 2) return;

  try {
    fs.writeFileSync(PID_FILE, String(process.pid), 'utf8');
  } catch {}

  stunSock = startStun(STUN_PORT);

  console.log('');
  console.log('  P2P 局域网传输 demo 已启动');
  console.log('  ─────────────────────────────────────────');
  for (const u of localUrls()) console.log('  局域网访问:  ' + u.url + '   [' + u.name + ']');
  console.log('  手机访问(https，支持大文件落盘):');
  for (const u of localUrls()) console.log('      ' + u.httpsUrl + '   [' + u.name + ']');
  console.log('  本机访问:    http://127.0.0.1:' + PORT + ' （localhost 也可）');
  console.log('  证书:        ' + (certFresh ? '已生成自签证书（手机首次访问点"继续访问"）' : '复用已有证书'));
  console.log('');
  console.log('  PID ' + process.pid + '，停止方式任选一种：');
  console.log('    · 本机网页里点「关闭服务器」');
  console.log('    · 双击 stop.bat');
  console.log('    · 这个窗口按 Ctrl+C');
  console.log('');

  // 网络类别要调 PowerShell 查，异步做，别拖慢启动
  refreshNetCategory();
  setTimeout(printNetworkWarnings, 2200);
  setInterval(refreshNetCategory, 20000).unref();

  // 由 start.bat 设置 OPEN_BROWSER 时才自动开浏览器，服务此时已就绪，不会出现「拒绝连接」
  if (process.env.OPEN_BROWSER === '1') {
    openBrowser(`http://localhost:${PORT}`);
    console.log('  已在浏览器打开本机页面: http://localhost:' + PORT);
    console.log('');
  }
}

server.listen(PORT, onReady);
httpsServer.listen(HTTPS_PORT, onReady);

function printNetworkWarnings() {
  const risky = localUrls().filter((u) => u.risky);
  if (!risky.length) return;
  console.log('  ⚠ 下面这些地址所在的网络被 Windows 判为「公用」：');
  for (const u of risky) {
    console.log('      ' + u.url + '   [' + u.name + ' · ' + u.categoryName + ']');
  }
  console.log('    公用网络的入站连接默认被防火墙拦掉，别的设备会一直转圈连不上。');
  console.log('    修复：双击 fix-firewall.bat（会弹管理员授权），把端口对所有网络类型放行。');
  console.log('    需要放行的端口：TCP ' + PORT + '（网页）、TCP ' + HTTPS_PORT + '（手机 https）、UDP ' + STUN_PORT + '（打洞）。');
  console.log('');
}

// Ctrl+C / 被任务管理器结束 / 关机，都走同一套清理，别留 PID 残留
for (const sig of ['SIGINT', 'SIGTERM', 'SIGHUP']) {
  process.on(sig, () => shutdown('收到信号 ' + sig));
}
