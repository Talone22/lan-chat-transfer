'use strict';

/* =========================================================================
 *  P2P 局域网传输 demo —— 稳定优先 + 吞吐优先
 *
 *  与「发个东西」原版的关键差异：
 *   1. DataChannel 去掉 maxRetransmits，改为完全可靠（原版 10 次重传后通道自杀）
 *   2. 分片 64KB 起（原版 8KB），消息数降低 8 倍
 *   3. 背压用 bufferedamountlow 事件等待（原版 setTimeout(0) 递归被 4ms 钳制）
 *   4. 帧头内联二进制（原版每片先发一条 JSON），消息数再降一半
 *   5. 接收端支持落盘（File System Access API），内存从 O(文件) 降到 O(分片)
 *   6. 幂等写入：重复分片不重复计数
 *   7. 停顿不销毁状态，可请求补洞续传
 * ========================================================================= */

const $ = (s) => document.querySelector(s);

const HDR = 5;                       // [0x01][u32 index BE]
const HIGH_WATER = 8 * 1024 * 1024;  // 发送缓冲高水位
const LOW_WATER = 1 * 1024 * 1024;   // 低水位（触发 bufferedamountlow）
const STALL_MS = 45000;              // 停顿判定：只告警，不销毁
const DEFAULT_CHUNK = 65536;         // 传输块大小，64KB 是实测最佳值

// 只用自己的内置 STUN，不依赖任何公网服务。
// 没有它的时候，浏览器会把局域网候选换成 mDNS 名称（xxx.local），
// 跨设备解析经常失败 → 表现就是两台真机永远"连接中"。
const CONF = { iceServers: [] };

const ICE_TIMEOUT = 15000;           // 建连看门狗：15 秒还没通就判定失败并给出排查提示

const state = {
  ws: null,
  me: null,
  peers: new Map(),                  // id -> Peer
};

let stunApplied = false;

/* ------------------------------ 工具 ------------------------------ */

const rid = () =>
  (crypto.randomUUID && crypto.randomUUID()) ||
  Date.now().toString(36) + Math.random().toString(36).slice(2, 8);

function fmtBytes(n) {
  if (n < 1024) return n + ' B';
  if (n < 1048576) return (n / 1024).toFixed(1) + ' KB';
  if (n < 1073741824) return (n / 1048576).toFixed(1) + ' MB';
  return (n / 1073741824).toFixed(2) + ' GB';
}

function fmtRate(bytesPerSec) {
  if (bytesPerSec < 1048576) return (bytesPerSec / 1024).toFixed(0) + ' KB/s';
  return (bytesPerSec / 1048576).toFixed(1) + ' MB/s';
}

function log(msg, kind) {
  const el = document.createElement('div');
  el.className = 'log-line' + (kind ? ' ' + kind : '');
  const t = new Date().toLocaleTimeString('zh-CN', { hour12: false });
  el.textContent = `[${t}] ${msg}`;
  const box = $('#log');
  box.appendChild(el);
  box.scrollTop = box.scrollHeight;
  while (box.children.length > 300) box.removeChild(box.firstChild);
}

function sendSignal(type, targetId, data) {
  if (!state.ws || state.ws.readyState !== WebSocket.OPEN) return;
  state.ws.send(JSON.stringify({ uid: state.me, targetId, type, data }));
}

/* 简单的等待器：等某条控制消息到达 */
class Waiter {
  constructor() { this.list = []; }
  push(msg) {
    for (const w of [...this.list]) {
      if (w.pred(msg)) {
        this.list.splice(this.list.indexOf(w), 1);
        clearTimeout(w.timer);
        w.resolve(msg);
      }
    }
  }
  wait(pred, ms, label) {
    return new Promise((resolve, reject) => {
      const w = { pred, resolve };
      w.timer = setTimeout(() => {
        const i = this.list.indexOf(w);
        if (i >= 0) this.list.splice(i, 1);
        reject(new Error(`${label || '等待对端响应'}超时`));
      }, ms);
      this.list.push(w);
    });
  }
}

/* ------------------------------ 速率计 ------------------------------ */

class Rate {
  constructor() { this.reset(); }
  reset() {
    this.t0 = performance.now();
    this.lastT = this.t0;
    this.lastB = 0;
    this.bytes = 0;
    this.avg = 0;
    this.inst = 0;
  }
  add(n) {
    this.bytes += n;
    const now = performance.now();
    if (now - this.lastT > 250) {
      this.inst = ((this.bytes - this.lastB) * 1000) / (now - this.lastT);
      this.lastT = now;
      this.lastB = this.bytes;
    }
    const dt = (now - this.t0) / 1000;
    if (dt > 0.2) this.avg = this.bytes / dt;
  }
}

/* ------------------------------ Peer ------------------------------ */

class Peer {
  constructor(id, meta) {
    this.id = id;
    this.ip = (meta && meta.ip) || '';
    this.onlineSec = meta && meta.onlineSec != null ? meta.onlineSec : null;
    this.pc = null;
    this.dc = null;
    // idle 未连 / connecting 握手中 / ready 已直连 / failed 失败待重试
    this.status = 'idle';
    this.reason = '';
    this.waiter = new Waiter();
    this.chain = Promise.resolve();
    this.tx = null;
    this.rx = null;
    this.stallTimer = null;
    this.iceTimer = null;
    this.rxRate = new Rate();
    this.txRate = new Rate();
  }

  get ready() {
    return this.status === 'ready';
  }

  setStatus(s, why) {
    this.status = s;
    this.reason = why || '';
    renderPeers();
  }

  /* ---- 建连：由点「连接」的一方发起 offer，对端自动应答 ---- */
  async connect() {
    if (this.status === 'connecting') return;
    this.teardown();
    this.setStatus('connecting', '握手中');
    log(`正在连接 ${this.id}…`);

    const pc = new RTCPeerConnection(CONF);
    this.pc = pc;
    this.watch(pc);

    this.dc = pc.createDataChannel('data', { ordered: true });
    this.bindChannel();

    try {
      const off = await pc.createOffer();
      await pc.setLocalDescription(off);
      this.logCandidates(await gather(pc));
      sendSignal('9002', this.id, { sdp: pc.localDescription.sdp });
      this.armIce();
    } catch (err) {
      this.fail('发起连接失败：' + err.message);
    }
  }

  /* ---- 收到 offer：自动应答 ---- */
  async acceptOffer(sdp) {
    if (this.pc && this.pc.signalingState === 'have-local-offer') {
      // glare：两边同时点了「连接」。约定 id 大的一方让步，改成应答，否则互相等死
      if (state.me < this.id) return;
      this.teardown();
      this.setStatus('connecting', '握手中');
    } else if (this.pc && this.pc.signalingState !== 'stable') {
      return;
    } else if (this.status === 'ready') {
      return; // 已经连上，重复 offer 直接忽略
    } else {
      this.teardown();
      this.setStatus('connecting', '握手中');
    }

    const pc = new RTCPeerConnection(CONF);
    this.pc = pc;
    this.watch(pc);
    pc.ondatachannel = (e) => {
      this.dc = e.channel;
      this.bindChannel();
    };

    try {
      await pc.setRemoteDescription({ type: 'offer', sdp });
      const ans = await pc.createAnswer();
      await pc.setLocalDescription(ans);
      this.logCandidates(await gather(pc));
      sendSignal('9003', this.id, { sdp: pc.localDescription.sdp });
      this.armIce();
    } catch (err) {
      this.fail('应答失败：' + err.message);
    }
  }

  async acceptAnswer(sdp) {
    if (!this.pc || this.pc.remoteDescription) return;
    try {
      await this.pc.setRemoteDescription({ type: 'answer', sdp });
    } catch (err) {
      this.fail('设置应答失败：' + err.message);
    }
  }

  watch(pc) {
    pc.onconnectionstatechange = () => {
      if (this.pc !== pc) return;
      const s = pc.connectionState;
      if (s === 'connected') {
        this.clearIce();
        this.setStatus('ready', '已直连');
      } else if (s === 'failed') {
        this.fail('ICE 打洞失败，两边没找到可用路径');
      } else if (s === 'disconnected') {
        this.setStatus('connecting', '链路中断，等待恢复');
      } else {
        renderPeers();
      }
    };
  }

  armIce() {
    this.clearIce();
    this.iceTimer = setTimeout(() => {
      if (this.status === 'ready') return;
      this.fail(`${ICE_TIMEOUT / 1000} 秒内没打通`);
    }, ICE_TIMEOUT);
  }

  clearIce() {
    clearTimeout(this.iceTimer);
    this.iceTimer = null;
  }

  fail(msg) {
    this.clearIce();
    try { this.dc && this.dc.close(); } catch {}
    try { this.pc && this.pc.close(); } catch {}
    this.dc = null;
    this.pc = null;
    this.setStatus('failed', msg);
    log(`和 ${this.id} 连接失败：${msg}`, 'bad');
    log('  可以试试：① 确认两台设备在同一个网络 ② 运行一次 fix-firewall.bat' +
        ' ③ 如果是公共 WiFi，可能开了设备隔离');
  }

  /* 彻底销毁连接对象，但不改状态（供 connect/应答内部复用） */
  teardown() {
    this.clearIce();
    clearTimeout(this.stallTimer);
    try { this.dc && this.dc.close(); } catch {}
    try { this.pc && this.pc.close(); } catch {}
    this.dc = null;
    this.pc = null;
  }

  disconnect() {
    this.teardown();
    this.setStatus('idle', '');
    log(`已和 ${this.id} 断开`);
  }

  close() {
    this.teardown();
    this.status = 'idle';
  }

  logCandidates(seen) {
    const parts = [];
    if (seen.srflx) parts.push('srflx×' + seen.srflx);
    if (seen.relay) parts.push('relay×' + seen.relay);
    if (seen.host) parts.push('host×' + seen.host + (seen.mdns ? `（${seen.mdns} 个是 mDNS 名）` : ''));
    /* 连接细节仅用于排查，不展示 */
    if (!seen.srflx && !seen.relay && seen.host && seen.mdns >= seen.host) {
      log('  ⚠ 网络连接方式受限，可能无法和对方直连。' +
          '请确认防火墙已放行，或换个网络试试。', 'bad');
    }
  }

  bindChannel() {
    this.dc.binaryType = 'arraybuffer';
    this.dc.bufferedAmountLowThreshold = LOW_WATER;

    this.dc.onopen = () => {
      this.clearIce();
      this.setStatus('ready', '已直连');
      log(`已和 ${this.id} 连上`, 'ok');
    };
    this.dc.onclose = () => {
      if (this.status === 'ready') {
        this.setStatus('connecting', '通道已关闭');
        log(`和 ${this.id} 的连接断开了`, 'bad');
      }
    };
    this.dc.onerror = () => log('传输通道出错，请重试', 'bad');

    // 串行处理：保证落盘顺序，同时不让控制消息与数据帧交错
    this.dc.onmessage = (e) => {
      this.chain = this.chain
        .then(() => this.handle(e.data))
        .catch((err) => log('数据处理出错：' + err.message, 'bad'));
    };
  }

  async handle(data) {
    if (typeof data === 'string') {
      let msg;
      try { msg = JSON.parse(data); } catch { return; }
      this.waiter.push(msg);
      if (msg.t === 'offer') return this.onOffer(msg);
      if (msg.t === 'end') return this.onEnd(msg);
      if (msg.t === 'miss') return this.onMiss(msg);
      if (msg.t === 'done') return this.onDone(msg);
      if (msg.t === 'fail') return this.onFail(msg);
      if (msg.t === 'reject') return this.onReject(msg);
      if (msg.t === 'direct-link') return this.onDirectLink(msg);
      if (msg.t === 'direct-done') return this.onDirectDone(msg);
      if (msg.t === 'direct-fail') return this.onDirectFail(msg);
      if (msg.t === 'direct-cancel') return this.onDirectCancel(msg);
      return;
    }
    return this.onFrame(data);
  }

  /* ------------------------------ 发送端 ------------------------------ */

  async sendFile(file, chunkWanted) {
    if (!this.ready) throw new Error(`与 ${this.id} 尚未建立连接`);

    // SCTP 单条消息有上限（Chrome 通常 256KB），必须刨掉帧头，否则 send 会抛错
    const maxMsg = (this.pc && this.pc.sctp && this.pc.sctp.maxMessageSize) || 65536;
    const chunk = Math.max(8192, Math.min(chunkWanted, maxMsg - HDR));
    if (chunk !== chunkWanted) {
      log(`已自动调整传输块大小（${fmtBytes(chunk)}）`);
    }

    const fileId = rid();
    const total = Math.ceil(file.size / chunk) || 1;

    // 保留调用方已创建的进度行，避免被覆盖
    const rowEl = this.tx && this.tx.el ? this.tx.el : null;
    this.tx = {
      fileId, chunk, total, file,
      sentSet: new Set(),   // 已发送的分片索引（进度去重，永不超 100%）
      sent: 0,              // 净发送字节（去重后）
      resent: 0,            // 补发重复字节（单列，与接收端速率可比）
      rawBytes: 0,          // 实际发送总字节（含重复，仅日志用）
      done: false,          // 收到对端 done 后置真，停止一切补发
      rate: new Rate(),
      el: rowEl,
    };
    this._missSet = new Set();   // 待补发的分片集合（合并去重）
    this._resending = false;     // 是否已有补发循环在跑
    this._directToken = null;    // 直链模式的中转 token（完成后清理用）
    this._directTimer = null;    // 直链下载的兜底超时
    this._directPoll = null;     // 直链下载的状态轮询
    this.txProgress();

    // direct=本机服务器可当中转（页面就是它托管的，始终可达）。
    // 手机↔手机也走中转：发送方手机先上传到服务器，接收方手机再从服务器原生下载。
    const direct = !!srvInfo;
    this.dc.send(JSON.stringify({
      t: 'offer', fileId, name: file.name, size: file.size, chunk, total, direct,
    }));

    const resp = await this.waiter.wait(
      (m) => m.fileId === fileId && (m.t === 'accept' || m.t === 'want-direct'),
      120000, '等待接收方确认');

    if (resp.t === 'want-direct') {
      // 手机选择了直链下载：走服务器中转，不再用 WebRTC 传数据
      try {
        await this.sendDirect(file, fileId, file.name, file.size);
      } catch (err) {
        try { this.dc.send(JSON.stringify({ t: 'direct-fail', fileId, reason: err.message })); } catch {}
        this.failSend('直链上传失败: ' + err.message);
        throw err;
      }
      return;
    }

    log('对方已接受，开始发送…');

    try {
      await this.pump();
      if (!this.tx || this.tx.fileId !== fileId || this.tx.done) return;
      this.dc.send(JSON.stringify({ t: 'end', fileId, total }));
      log('发送完毕，等待对方确认…');
      this.armEndWatch(fileId);
    } catch (err) {
      this.failSend(err.message);
      throw err;
    }
  }

  // 发完 end 后的看门狗：接收方崩溃/掉线（手机上很常见）时，
  // 它的 miss/done 永远不会来，发送端不能永远停在 99%。
  // 每次补发循环结束后会重新布置。
  armEndWatch(fileId) {
    clearTimeout(this._endWatch);
    this._endWatch = setTimeout(() => {
      if (this.tx && this.tx.fileId === fileId && !this.tx.done) {
        this.failSend('等待接收方校验超时（3 分钟无回音，对方可能已掉线或退出）');
      }
    }, 180000);
  }

  // 直链模式：把文件流式上传到本机服务器，给接收方一个普通 http 下载链接。
  // 完成判定以「发送端轮询服务器下载状态」为主——不依赖 DataChannel，
  // 手机切后台把连接断了也能正常收尾。
  async sendDirect(file, fileId, name, size) {
    const tx = this.tx;
    const setMeta = (s) => { if (tx && tx.el) tx.el.querySelector('.meta').textContent = s; };
    const setBar = (pct) => { if (tx && tx.el) tx.el.querySelector('.bar > i').style.width = pct + '%'; };

    setMeta('上传中转…');
    log('正在准备发送，请稍候…');
    if (isMobile()) {
      log('  发送期间请保持页面在前台、屏幕亮着', 'bad');
    }

    const res = await fetch('/api/upload', {
      method: 'POST',
      headers: { 'x-file-name': encodeURIComponent(name), 'x-file-size': String(size) },
      body: file,
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || !data.ok) throw new Error(data.error || '上传失败');

    // 竞态防护：上传期间对方可能已取消（onDirectCancel 清了 this.tx）。
    // 此时绝不能再挂 token / 改行文字 / 发链接，把刚传上去的文件删掉收尾。
    if (!this.tx || this.tx.fileId !== fileId) {
      fetch('/api/delete?token=' + encodeURIComponent(data.token), { method: 'POST' }).catch(() => {});
      log('这次传输已取消');
      return;
    }

    this._directToken = data.token;
    setBar('100%');
    setMeta('已上传，等待对方下载（进度看手机通知栏）');
    this.dc.send(JSON.stringify({ t: 'direct-link', fileId, path: data.path, name, size }));
    log('文件已就绪，等待对方下载…');

    // 主完成信号：轮询服务器（3 秒一次）
    const tick = async () => {
      const cur = this.tx;
      if (!cur || cur.fileId !== fileId) return;   // 已完结或被新传输顶掉
      try {
        const d = await (await fetch('/api/dl-status?token=' + data.token, { cache: 'no-store' })).json();
        if (d && d.downloaded) { this.onDirectDone({ fileId }); return; }
      } catch {}
      this._directPoll = setTimeout(tick, 3000);
    };
    this._directPoll = setTimeout(tick, 3000);

    // 兜底：30 分钟还没下完（对方一直没点/取消了但通知丢了），清理中转别占磁盘
    this._directTimer = setTimeout(() => {
      this._cleanupDirect();
      if (this.tx && this.tx.fileId === fileId) {
        if (this.tx.el) {
          this.tx.el.classList.add('fail');
          this.tx.el.querySelector('.meta').textContent = '对方长时间未完成下载';
        }
        log('对方长时间未完成下载，已取消这次传输', 'bad');
        this.tx = null;
      }
    }, 30 * 60 * 1000);
  }

  async pump(indices) {
    const tx = this.tx;
    if (!tx) return;
    tx.rate = tx.rate || new Rate();

    const list = indices || Array.from({ length: tx.total }, (_, i) => i);

    for (const i of list) {
      // 已确认完成 / 被新传输顶掉，立即停，别再发多余的分片
      if (!this.tx || this.tx.fileId !== tx.fileId || tx.done) break;
      if (i < 0 || i >= tx.total) continue;

      const start = i * tx.chunk;
      const end = Math.min(start + tx.chunk, tx.file.size);
      if (start >= end) continue;

      const buf = await tx.file.slice(start, end).arrayBuffer();

      // 背压等待，带超时：链路断了/对端锁屏长时间不消费时明确报错，而不是无限卡死
      await this.drain(60000);

      const frame = new Uint8Array(HDR + buf.byteLength);
      frame[0] = 1;
      new DataView(frame.buffer).setUint32(1, i, false);
      frame.set(new Uint8Array(buf), HDR);

      this.dc.send(frame);

      const isNew = !tx.sentSet.has(i);
      if (isNew) tx.sentSet.add(i);
      tx.rawBytes += buf.byteLength;
      if (isNew) tx.sent += buf.byteLength;
      else tx.resent += buf.byteLength;
      tx.rate.add(buf.byteLength);
      this.txProgress();
    }
  }

  drain(timeoutMs = 60000) {
    const dc = this.dc;
    if (!dc || dc.readyState !== 'open') {
      return Promise.reject(new Error('连接已关闭，发送中断'));
    }
    if (dc.bufferedAmount < HIGH_WATER) return Promise.resolve();

    return new Promise((resolve, reject) => {
      let settled = false;
      const done = () => {
        if (settled) return;
        settled = true;
        cleanup();
        resolve();
      };
      const fail = () => {
        if (settled) return;
        settled = true;
        cleanup();
        const dead = !this.dc || this.dc.readyState !== 'open' ||
          (this.pc && ['failed', 'closed'].includes(this.pc.connectionState));
        reject(new Error(dead
          ? '连接已断开，发送中断'
          : '对端长时间未接收（可能已锁屏/切后台），发送已暂停'));
      };
      const timer = setTimeout(fail, timeoutMs);
      const cleanup = () => {
        clearTimeout(timer);
        dc.removeEventListener('bufferedamountlow', done);
      };
      dc.addEventListener('bufferedamountlow', done);
    });
  }

  txProgress() {
    const tx = this.tx;
    if (!tx || !tx.el) return;
    // 进度按「唯一分片数」算，补发不重复计数，永不超 100%
    const pct = Math.min(100, (tx.sentSet.size / tx.total) * 100);
    tx.el.querySelector('.bar > i').style.width = pct.toFixed(1) + '%';
    let meta = `${pct.toFixed(1)}%  ${fmtBytes(tx.sent)} / ${fmtBytes(tx.file.size)}  ${fmtRate(tx.rate.inst)}`;
    if (tx.resent > 0) meta += `  · 补发 ${fmtBytes(tx.resent)}`;
    tx.el.querySelector('.meta').textContent = meta;
  }

  onMiss(msg) {
    const tx = this.tx;
    if (!tx || msg.fileId !== tx.fileId || tx.done) return;

    // 合并缺口到待补发集合，天然去重
    for (const i of msg.idx) {
      if (i >= 0 && i < tx.total) this._missSet.add(i);
    }

    // 已有补发循环在跑：它会循环取走 _missSet，不用再开一路（消灭并发洪泛）
    if (this._resending) return;

    this._resending = true;
    this.resendLoop(tx).finally(() => {
      this._resending = false;
    });
  }

  async resendLoop(tx) {
    while (this._missSet.size && !tx.done && this.tx === tx) {
      const batch = [...this._missSet];
      this._missSet.clear();
      log(`对方缺少 ${batch.length} 块，正在补发…`);
      try {
        await this.pump(batch);
      } catch (err) {
        this.failSend(err.message);
        return;
      }
      // 补发完重新发结束标记，让对端再校验一次（还有缺口会再发 miss）
      if (!tx.done && this.dc && this.dc.readyState === 'open') {
        this.dc.send(JSON.stringify({ t: 'end', fileId: tx.fileId, total: tx.total }));
        this.armEndWatch(tx.fileId);
      }
    }
  }

  failSend(reason) {
    const tx = this.tx;
    if (tx && tx.el) {
      tx.el.classList.add('fail');
      tx.el.querySelector('.meta').textContent = '发送失败：' + reason;
    }
    clearTimeout(this._endWatch);
    this.tx = null;
    this._missSet = new Set();
    log('发送失败：' + reason, 'bad');
  }

  onDone(msg) {
    const tx = this.tx;
    if (!tx || msg.fileId !== tx.fileId) return;
    tx.done = true;   // 立即停止在途补发
    this._missSet.clear();
    clearTimeout(this._endWatch);
    const secs = (performance.now() - tx.rate.t0) / 1000;
    const resentInfo = tx.resent > 0 ? `  · 补发 ${fmtBytes(tx.resent)}` : '';
    log(`发送完成：${fmtBytes(tx.file.size)} 用时 ${secs.toFixed(1)}s，平均 ${fmtRate(tx.file.size / secs)}${resentInfo}`, 'ok');
    if (tx.el) {
      tx.el.classList.add('done');
      tx.el.querySelector('.meta').textContent =
        `完成  ${fmtBytes(tx.file.size)}  ${secs.toFixed(1)}s  平均 ${fmtRate(tx.file.size / secs)}${resentInfo}`;
    }
    this.tx = null;
  }

  onFail(msg) {
    const tx = this.tx;
    if (!tx || msg.fileId !== tx.fileId) return;
    tx.done = true;
    this._missSet.clear();
    const reason = msg.reason || '未知原因';
    if (tx.el) {
      tx.el.classList.add('fail');
      tx.el.querySelector('.meta').textContent = '接收端失败: ' + reason;
    }
    log('保存失败，传输已停止：' + reason, 'bad');
    this.tx = null;
  }

  onReject(msg) {
    log('对方拒绝了这次传输', 'bad');
    if (this.tx && this.tx.el) this.tx.el.remove();
    this.tx = null;
  }

  /* ---- 直链下载：发送端收到的完成回执（DataChannel 通知或轮询到下载完成）---- */
  onDirectDone(msg) {
    const tx = this.tx;
    if (!tx || msg.fileId !== tx.fileId) return;
    log(`对方已下载完成：${tx.file.name}`, 'ok');
    if (tx.el) {
      tx.el.classList.add('done');
      tx.el.querySelector('.meta').textContent = `完成  ${fmtBytes(tx.file.size)}（直链下载）`;
    }
    this._cleanupDirect();
    this.tx = null;
  }

  /* ---- 直链下载：接收端主动取消 ---- */
  onDirectCancel(msg) {
    const tx = this.tx;
    if (!tx || msg.fileId !== tx.fileId) return;
    log('对方取消了接收', 'bad');
    if (tx.el) {
      tx.el.classList.add('fail');
      tx.el.querySelector('.meta').textContent = '对方取消接收';
    }
    this._cleanupDirect();
    this.tx = null;
  }

  /* ---- 直链下载：接收端收到的失败通知（发送端上传失败）---- */
  onDirectFail(msg) {
    const rx = this.rx;
    if (!rx || msg.fileId !== rx.fileId) return;
    log('下载失败：' + (msg.reason || '未知原因'), 'bad');
    if (rx.el) {
      rx.el.classList.add('fail');
      rx.el.querySelector('.meta').textContent = '下载失败：' + (msg.reason || '未知原因');
    }
    removeRowCancel(rx.el);
    this.rx = null;
  }

  _cleanupDirect() {
    clearTimeout(this._directTimer);
    this._directTimer = null;
    clearTimeout(this._directPoll);
    this._directPoll = null;
    if (this._directToken) {
      fetch('/api/delete?token=' + encodeURIComponent(this._directToken), { method: 'POST' }).catch(() => {});
      this._directToken = null;
    }
  }

  /* ------------------------------ 接收端 ------------------------------ */

  async onOffer(msg) {
    // 上一次直链任务还没开始下载就被新 offer 顶掉：自动取消旧的（并通知发送端），
    // 不再无脑回 busy —— 否则一次没下完，之后所有传输都会被"拒绝"卡死。
    if (this.rx && this.rx.kind === 'direct' && !this.rx.started) {
      this.cancelDirect(this.rx, true);
    }
    if (this.rx) {
      this.dc.send(JSON.stringify({ t: 'reject', fileId: msg.fileId, reason: 'busy' }));
      return;
    }

    // 手机收大文件：优先走直链下载（原生下载管理器，可续传、不占内存、不崩）
    const preferDirect = isMobile() && msg.direct && msg.size >= DIRECT_THRESHOLD;
    if (preferDirect) {
      const useDirect = await askDirect(this.id, msg);
      if (useDirect) {
        this.dc.send(JSON.stringify({ t: 'want-direct', fileId: msg.fileId }));
        this.rx = {
          fileId: msg.fileId, name: msg.name, size: msg.size,
          kind: 'direct', got: null, bytes: 0, sink: null,
          rate: new Rate(), el: createRxRow(this.id, msg),
          lastAt: performance.now(),
          started: false,      // 服务器是否已收到下载请求
          downloadUrl: null,
          token: null,
          tapAt: 0,            // 上次点击「开始下载」的时间（0=未点过）
        };
        this.rx.el.querySelector('.meta').textContent = '正在准备…';
        addRowCancel(this.rx.el, () => this.cancelDirect(this.rx));
        log(`正在准备 ${msg.name}，请稍候…`);
        return;
      }
      // 用户选了「点对点」，落到下面的 WebRTC 流程
    }

    const sink = await askReceive(this.id, msg);
    if (!sink) {
      this.dc.send(JSON.stringify({ t: 'reject', fileId: msg.fileId, reason: 'declined' }));
      return;
    }

    this.rx = {
      fileId: msg.fileId,
      name: msg.name,
      size: msg.size,
      chunk: msg.chunk,
      total: msg.total,
      got: new Set(),
      bytes: 0,
      sink,
      rate: new Rate(),
      el: createRxRow(this.id, msg),
      lastAt: performance.now(),
    };

    this.armStall();
    this.dc.send(JSON.stringify({ t: 'accept', fileId: msg.fileId }));
    const kindLabel = sink.kind === 'disk' ? '直接保存到磁盘'
      : sink.kind === 'opfs' ? '边接收边保存'
      : '先接收完再保存';
    log(`开始接收 ${msg.name}（${fmtBytes(msg.size)}，${kindLabel}）`);
    if (msg.size > 50 * 1048576) {
      log('  文件较大，接收期间请保持页面在前台、屏幕常亮', 'bad');
    }
  }

  onFrame(buf) {
    const rx = this.rx;
    if (!rx) return;                       // 未协商的数据帧直接丢弃
    if (buf.byteLength <= HDR) return;

    const idx = new DataView(buf).getUint32(1, false);
    if (idx >= rx.total) return;

    // 幂等：重复分片不重复落盘、不重复计数
    if (rx.got.has(idx)) return;
    rx.got.add(idx);

    const payload = new Uint8Array(buf, HDR);
    // 落盘是异步的：这里必须 await，handle() 已经被串行链保护
    return rx.sink.write(idx, payload).then(() => {
      rx.bytes += payload.byteLength;
      rx.rate.add(payload.byteLength);
      rx.lastAt = performance.now();
      this.rxProgress();
      this.armStall();   // 收到数据即重置停顿定时器，避免每 45s 误报一次
    }).catch((err) => {
      // 写盘失败（磁盘满/权限等）要如实暴露，不能当作成功吞掉
      rx.got.delete(idx);
      this.failRx('写入失败: ' + err.message);
    });
  }

  failRx(reason) {
    const rx = this.rx;
    if (!rx) return;
    clearTimeout(this.stallTimer);
    log('接收失败：' + reason, 'bad');
    if (rx.el) {
      rx.el.classList.add('fail');
      rx.el.querySelector('.meta').textContent = '接收失败：' + reason;
    }
    try { rx.sink.abort && rx.sink.abort(); } catch {}
    releaseWakeLock();
    // 通知发送端，让它停止补发、别再空转
    try {
      if (this.dc && this.dc.readyState === 'open') {
        this.dc.send(JSON.stringify({ t: 'fail', fileId: rx.fileId, reason }));
      }
    } catch {}
    this.rx = null;
  }

  rxProgress() {
    const rx = this.rx;
    if (!rx || !rx.el) return;
    const pct = (rx.bytes / rx.size) * 100;
    rx.el.querySelector('.bar > i').style.width = pct.toFixed(1) + '%';
    rx.el.querySelector('.meta').textContent =
      `${pct.toFixed(1)}%  ${fmtBytes(rx.bytes)} / ${fmtBytes(rx.size)}  ${fmtRate(rx.rate.inst)}`;
  }

  armStall() {
    clearTimeout(this.stallTimer);
    this.stallTimer = setTimeout(() => {
      const rx = this.rx;
      if (!rx) return;
      if (rx.got.size >= rx.total) return;
      // 只有真的超过 45s 没收到数据才告警。正常传输时每片都会重置本定时器，不会再误报。
      const idle = performance.now() - rx.lastAt;
      if (idle >= STALL_MS) {
        log('传输暂时停滞，正在等待恢复…（已收到的部分不会丢）', 'bad');
        // 链路没死才请求补洞；通道都关了就没必要发了
        if (this.dc && this.dc.readyState === 'open') {
          const holes = this.holes();
          if (holes.length) this.dc.send(JSON.stringify({ t: 'miss', fileId: rx.fileId, idx: holes.slice(0, 20000) }));
        }
      }
      this.armStall();
    }, STALL_MS);
  }

  holes() {
    const rx = this.rx;
    if (!rx) return [];
    const out = [];
    for (let i = 0; i < rx.total; i++) if (!rx.got.has(i)) out.push(i);
    return out;
  }

  async onEnd(msg) {
    const rx = this.rx;
    if (!rx || msg.fileId !== rx.fileId) return;

    const holes = this.holes();
    if (holes.length) {
      log(`缺少 ${holes.length} 块，正在补发…`);
      await this.dc.send(JSON.stringify({ t: 'miss', fileId: rx.fileId, idx: holes.slice(0, 20000) }));
      return;
    }

    clearTimeout(this.stallTimer);
    const secs = (performance.now() - rx.rate.t0) / 1000;

    // 保存是最后一步，也是最容易在手机上失败的一步（内存/存储不足）。
    // 失败必须如实标红 + 通知发送端，不能再假报"接收完成"。
    try {
      await rx.sink.close();
    } catch (err) {
      this.failRx('保存失败: ' + err.message);
      return;
    }

    if (rx.el) {
      rx.el.classList.add('done');
      rx.el.querySelector('.meta').textContent =
        `完成  ${fmtBytes(rx.size)}  ${secs.toFixed(1)}s  平均 ${fmtRate(rx.size / secs)}`;
    }

    this.dc.send(JSON.stringify({ t: 'done', fileId: rx.fileId }));
    log(`接收完成：${rx.name}  ${fmtBytes(rx.size)}  ${secs.toFixed(1)}s  平均 ${fmtRate(rx.size / secs)}`, 'ok');
    releaseWakeLock();
    this.rx = null;
  }

  /* ---- 直链下载：接收端收到下载链接 ----
   * 不自动触发下载：等用户点「接收」行（真实手势，100% 能弹下载）。
   * 之后的状态机：
   *   未点   → 点行 = 开始下载
   *   点了但浏览器没发起下载（对话框被关掉/被拦）→ 15s 后回到可重试
   *   下载中 → 点行 = 确认后取消；右侧「取消」按钮随时可取消
   */
  onDirectLink(msg) {
    const rx = this.rx;
    if (!rx || msg.fileId !== rx.fileId) return;
    rx.downloadUrl = location.origin + msg.path;
    rx.token = msg.path.split('/').pop();
    log(`${msg.name} 已准备好，点「开始下载」即可（可在通知栏查看进度）`);
    if (rx.el) {
      rx.el.classList.add('manual');
      rx.el.querySelector('.meta').textContent = '点这里开始下载';
      rx.el.onclick = () => {
        if (this.rx !== rx) return;
        if (!rx.tapAt) { this.startDirectDownload(rx); return; }  // 未点过 → 开始
        if (!rx.started) return;                                  // 下载对话框还开着，忽略
        if (confirm('下载进行中。确定取消接收？已下载的部分会丢弃。')) this.cancelDirect(rx);
      };
    }
    this.pollDirect(rx);
  }

  startDirectDownload(rx) {
    if (this.rx !== rx || !rx.downloadUrl) return;
    triggerDownload(rx.downloadUrl, rx.name);
    rx.tapAt = performance.now();
    rx.el.classList.remove('manual');
    rx.el.querySelector('.meta').textContent = '下载中…（通知栏看进度，点这里可取消）';
    log('已触发下载，请到浏览器下载列表/通知栏查看进度');
  }

  cancelDirect(rx, superseded) {
    if (this.rx !== rx) return;
    try { this.dc.send(JSON.stringify({ t: 'direct-cancel', fileId: rx.fileId })); } catch {}
    if (rx.el) {
      removeRowCancel(rx.el);
      rx.el.classList.remove('manual');
      rx.el.onclick = null;
      if (!superseded) rx.el.classList.add('fail');
      rx.el.querySelector('.meta').textContent = superseded ? '已取消（有新任务）' : '已取消接收';
    }
    if (superseded) {
      log('上一个任务还没开始就来了新文件，已自动取消');
    } else {
      log('已取消接收', 'bad');
    }
    this.rx = null;
  }

  pollDirect(rx) {
    const token = rx.token;
    const tick = async () => {
      if (this.rx !== rx) return;   // 已完结/取消/被顶掉，停止轮询
      if (rx._busy) return;
      rx._busy = true;
      let d = null;
      try {
        d = await (await fetch('/api/dl-status?token=' + encodeURIComponent(token), { cache: 'no-store' })).json();
      } catch {}
      rx._busy = false;

      // 服务器上已经没有这个文件了：要么对方已下载完成并清理（服务器留了完成记录），
      // 要么任务被取消/过期。两种情况都必须结束轮询，绝不能提示“点这里重试”，
      // 否则会去下载一个 404 文本。
      if (!d || !d.ok) {
        this.finishDirectRx(rx, !!(d && d.downloaded));
        return;
      }

      if (d.started) rx.started = true;
      if (d.downloaded) {
        this.finishDirectRx(rx, true);
        return;
      }

      // 点了「开始下载」但浏览器一直没真正发起请求 → 对话框被关掉或下载被拦，
      // 此时文件还在服务器上，回到可重试状态（点击是真实手势，必能触发）
      if (rx.tapAt && !d.started && performance.now() - rx.tapAt > 15000) {
        rx.tapAt = 0;
        rx.el.classList.add('manual');
        rx.el.querySelector('.meta').textContent = '未检测到下载，点这里重试';
      }
      rx._pollTimer = setTimeout(tick, 3000);
    };

    // 手机下载时页面在后台，轮询会被节流；一回到页面就立刻查一次，不用干等
    rx.pollNow = () => {
      clearTimeout(rx._pollTimer);
      if (!rx._busy) tick();
    };
    tick();
  }

  // 统一收尾：ok=true 表示下载完成；false 表示任务已结束（取消/过期/被清理）
  finishDirectRx(rx, ok) {
    if (this.rx !== rx) return;
    removeRowCancel(rx.el);
    rx.el.classList.remove('manual');
    rx.el.onclick = null;
    if (ok) {
      rx.el.classList.add('done');
      rx.el.querySelector('.meta').textContent = `下载完成  ${fmtBytes(rx.size)}`;
      log(`下载完成：${rx.name}  ${fmtBytes(rx.size)}`, 'ok');
      try { this.dc.send(JSON.stringify({ t: 'direct-done', fileId: rx.fileId, name: rx.name })); } catch {}
    } else {
      rx.el.classList.add('fail');
      rx.el.querySelector('.meta').textContent = '传输已结束';
      log('这次传输已结束（文件已被清理）', 'bad');
    }
    this.rx = null;
  }
}

/* ------------------------------ 接收端选择 ------------------------------ */

// 内存兜底超过这个体积就警告：手机浏览器整块塞内存 + 最后再复制成 Blob，
// 峰值接近 2 倍文件大小，大文件几乎必挂，OPFS 才是正路。
const MEM_WARN_BYTES = 200 * 1024 * 1024;

// 手机接收超过这个体积，优先走「直链下载」（浏览器原生下载管理器，可续传、不占内存、不崩）
const DIRECT_THRESHOLD = 200 * 1024 * 1024;

let screenWakeLock = null;
async function acquireWakeLock() {
  try {
    if ('wakeLock' in navigator && navigator.wakeLock) {
      screenWakeLock = await navigator.wakeLock.request('screen');
    }
  } catch {}
}
function releaseWakeLock() {
  try { screenWakeLock && screenWakeLock.release(); } catch {}
  screenWakeLock = null;
}

// 触发原生下载（同源 + Content-Disposition: attachment 会走浏览器下载管理器）
function triggerDownload(url, name) {
  const a = document.createElement('a');
  a.href = url;
  a.download = name;
  document.body.appendChild(a);
  a.click();
  a.remove();
}

// 传输行右侧的小「取消」按钮
function addRowCancel(row, fn) {
  const line = row.querySelector('.line');
  const btn = document.createElement('button');
  btn.className = 'row-cancel';
  btn.textContent = '取消';
  btn.addEventListener('click', (e) => { e.stopPropagation(); fn(); });
  line.appendChild(btn);
  return btn;
}

function removeRowCancel(row) {
  if (!row) return;
  const btn = row.querySelector('.row-cancel');
  if (btn) btn.remove();
}

function isMobile() {
  return /Android|iPhone|iPad|iPod|Mobile/i.test(navigator.userAgent) || !!window.__P2P_FORCE_MOBILE__;
}

function sinkCaps() {
  const secure = window.isSecureContext === true;
  return {
    secure,
    disk: secure && typeof window.showSaveFilePicker === 'function' && !window.__P2P_FORCE_MEM__,
    opfs: secure && navigator.storage && typeof navigator.storage.getDirectory === 'function',
  };
}

function askDirect(fromId, meta) {
  return new Promise((resolve) => {
    const panel = $('#incoming');
    panel.classList.add('show');
    panel.querySelector('.who').textContent = `来自 ${fromId}`;
    panel.querySelector('.fname').textContent = meta.name;
    panel.querySelector('.fsize').textContent = `${fmtBytes(meta.size)} · 大文件`;
    panel.querySelector('.hint').textContent =
      '用浏览器直接下载：不占内存、可暂停续传，锁屏也不影响';
    const accept = panel.querySelector('.accept');
    const reject = panel.querySelector('.reject');
    accept.textContent = '直接下载';
    reject.textContent = '点对点';
    const finish = (v) => {
      panel.classList.remove('show');
      accept.onclick = reject.onclick = null;
      accept.textContent = '接收';
      reject.textContent = '拒绝';
      resolve(v);
    };
    accept.onclick = () => finish(true);
    reject.onclick = () => finish(false);
  });
}

function askReceive(fromId, meta) {
  return new Promise((resolve) => {
    const panel = $('#incoming');
    panel.classList.add('show');
    panel.querySelector('.who').textContent = `来自 ${fromId}`;
    panel.querySelector('.fname').textContent = meta.name;
    panel.querySelector('.fsize').textContent =
      `${fmtBytes(meta.size)}  ·  ${meta.total} 片 × ${fmtBytes(meta.chunk)}`;

    const caps = sinkCaps();

    let hint;
    if (caps.disk && !isMobile()) {
      hint = '选择保存位置，边接收边写入（推荐大文件）';
    } else if (caps.opfs) {
      hint = '接收完成后自动弹出保存（推荐大文件）';
    } else if (meta.size > MEM_WARN_BYTES) {
      hint = `⚠ 文件较大（${fmtBytes(meta.size)}），当前方式可能保存失败`;
    } else {
      hint = '接收完成后需要手动保存';
    }
    panel.querySelector('.hint').textContent = hint;

    const accept = panel.querySelector('.accept');
    const reject = panel.querySelector('.reject');

    const finish = (v) => {
      panel.classList.remove('show');
      accept.onclick = null;
      reject.onclick = null;
      resolve(v);
    };

    accept.onclick = async () => {
      // 用户手势：请求屏幕常亮（手机锁屏是大文件中断的主因之一）
      acquireWakeLock();

      // 1) 桌面：让用户选保存位置（最可控，直接落盘）
      if (caps.disk && !isMobile()) {
        try {
          const handle = await window.showSaveFilePicker({ suggestedName: meta.name });
          const writable = await handle.createWritable();
          finish({
            kind: 'disk',
            write: (idx, bytes) => writable.write({ type: 'write', position: idx * meta.chunk, data: bytes }),
            close: () => writable.close(),
            abort: () => writable.abort(),
          });
          return;
        } catch (err) {
          if (err && err.name === 'AbortError') { releaseWakeLock(); finish(null); return; }
          // 其它异常降级到 OPFS / 内存
        }
      }

      // 2) 手机 / 没有选位置能力：OPFS 边收边写，最后导出（内存占用 O(分片)）
      if (caps.opfs) {
        try {
          const dir = await navigator.storage.getDirectory();
          const safeName = (meta.name || 'download').replace(/[\\/:*?"<>|]/g, '_');
          const fh = await dir.getFileHandle(safeName, { create: true });
          const writable = await fh.createWritable();
          finish({
            kind: 'opfs',
            write: (idx, bytes) => writable.write({ type: 'write', position: idx * meta.chunk, data: bytes }),
            close: async () => {
              await writable.close();
              // File 指向磁盘，下载时流式读取，不占双倍内存。
              // 不主动 revokeObjectURL：大文件下载很慢，过早吊销会掐断下载（之前 60s 就是坑）。
              const file = await fh.getFile();
              const a = document.createElement('a');
              a.href = URL.createObjectURL(file);
              a.download = meta.name;
              a.click();
            },
            abort: () => writable.abort(),
          });
          return;
        } catch (err) {
          // OPFS 不可用则降级内存
        }
      }

      // 3) 内存兜底
      if (meta.size > MEM_WARN_BYTES) {
        log(`⚠ 文件较大（${fmtBytes(meta.size)}），保存可能失败`, 'bad');
      }
      const parts = new Array(meta.total).fill(null);
      finish({
        kind: 'mem',
        write: async (idx, bytes) => { parts[idx] = bytes; },
        close: async () => {
          const blob = new Blob(parts);
          const a = document.createElement('a');
          a.href = URL.createObjectURL(blob);
          a.download = meta.name;
          a.click();
          // 内存模式的 blob 占内存，但不能过早吊销（会掐断下载）。留 5 分钟余量。
          setTimeout(() => URL.revokeObjectURL(a.href), 5 * 60 * 1000);
          return blob;
        },
        abort: () => {},
      });
    };

    reject.onclick = () => finish(null);
  });
}

/* ------------------------------ 群聊 ------------------------------ */

function appendChat(id, text, ts) {
  const body = $('#chatBody');
  if (!body) return;
  const empty = $('#chatEmpty');
  if (empty) empty.remove();

  const me = id === state.me;
  const wrap = document.createElement('div');
  wrap.className = 'msg' + (me ? ' me' : '');

  const head = document.createElement('div');
  head.className = 'msg-head';
  const nm = document.createElement('b');
  nm.textContent = me ? '我' : (id || '?');
  const tm = document.createElement('span');
  tm.textContent = new Date(ts || Date.now()).toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' });
  head.appendChild(nm);
  head.appendChild(tm);

  const tx = document.createElement('div');
  tx.className = 'msg-text';
  tx.textContent = text;   // textContent 防注入

  wrap.appendChild(head);
  wrap.appendChild(tx);
  body.appendChild(wrap);
  body.scrollTop = body.scrollHeight;
}

function appendChatSys(text) {
  const body = $('#chatBody');
  if (!body) return;
  const empty = $('#chatEmpty');
  if (empty) empty.remove();
  const wrap = document.createElement('div');
  wrap.className = 'msg sys';
  const tx = document.createElement('div');
  tx.className = 'msg-text';
  tx.textContent = text;
  wrap.appendChild(tx);
  body.appendChild(wrap);
  body.scrollTop = body.scrollHeight;
}

function sendChat() {
  const input = $('#chatText');
  const text = input.value.trim();
  if (!text) return;
  if (!state.ws || state.ws.readyState !== 1) {
    log('还没连上服务器，消息发不出去', 'bad');
    return;
  }
  state.ws.send(JSON.stringify({ type: '9007', data: { text } }));
  input.value = '';
  input.focus();
}

/* ------------------------------ UI ------------------------------ */

function createRxRow(peerId, meta) {
  const ph = document.getElementById('filesEmpty');
  if (ph) ph.remove();
  const row = document.createElement('div');
  row.className = 'row';
  row.innerHTML = `
    <div class="line">
      <span class="tag rx">接收</span>
      <span class="name"></span>
      <span class="meta">等待数据…</span>
    </div>
    <div class="bar"><i></i></div>`;
  row.querySelector('.name').textContent = `${meta.name}  ·  来自 ${peerId}`;
  $('#files').prepend(row);
  return row;
}

function createTxRow(peerId, file, chunk) {
  const ph = document.getElementById('filesEmpty');
  if (ph) ph.remove();
  const row = document.createElement('div');
  row.className = 'row';
  row.innerHTML = `
    <div class="line">
      <span class="tag tx">发送</span>
      <span class="name"></span>
      <span class="meta">握手…</span>
    </div>
    <div class="bar"><i></i></div>`;
  row.querySelector('.name').textContent = `${file.name}  ·  发给 ${peerId}`;
  $('#files').prepend(row);
  return row;
}

function renderPeers() {
  const box = $('#peers');
  if (!box) return;
  box.innerHTML = '';
  if (!state.peers.size) {
    box.innerHTML = '<div class="empty">还没有其他设备<br><span>让对方的手机或电脑打开同一个网址</span></div>';
    renderTargets();
    return;
  }
  for (const p of state.peers.values()) {
    const el = document.createElement('div');
    el.className = 'peer' + (p.status === 'ready' ? ' on' : '') + (p.status === 'failed' ? ' bad' : '');
    el.innerHTML = `
      <span class="dot ${p.status}"></span>
      <span class="pid"></span>
      <span class="pmeta"></span>
      <button class="pbtn"></button>`;

    el.querySelector('.pid').textContent = p.id;

    const bits = [];
    if (p.ip) bits.push(p.ip);
    if (p.onlineSec != null) bits.push('在线 ' + fmtUptime(p.onlineSec));
    if (p.status === 'failed' && p.reason) bits.push(p.reason);
    if (p.status === 'connecting' && p.reason) bits.push(p.reason);
    el.querySelector('.pmeta').textContent = bits.join(' · ');
    el.querySelector('.pmeta').title = bits.join(' · ');

    const btn = el.querySelector('.pbtn');
    if (p.status === 'ready') {
      btn.textContent = '断开';
      btn.className = 'pbtn';
    } else if (p.status === 'connecting') {
      btn.textContent = '取消';
      btn.className = 'pbtn';
    } else {
      btn.textContent = p.status === 'failed' ? '重试' : '连接';
      btn.className = 'pbtn primary';
    }
    btn.addEventListener('click', () => {
      if (p.status === 'ready' || p.status === 'connecting') p.disconnect();
      else p.connect();
    });

    box.appendChild(el);
  }
  renderTargets();
}

/* 发送目标：只有已直连的设备能选，避免"拖进去才发现发不出去" */
function renderTargets() {
  const sel = $('#sendTo');
  const hint = $('#sendHint');
  if (!sel) return;

  const ready = [...state.peers.values()].filter((p) => p.ready);
  const prev = sel.value;
  sel.textContent = '';
  if (!ready.length) {
    const ph = document.createElement('option');
    ph.value = '';
    ph.textContent = '暂无已连接设备';
    sel.appendChild(ph);
  }
  for (const p of ready) {
    const o = document.createElement('option');
    o.value = p.id;
    o.textContent = p.ip ? `${p.id} · ${p.ip}` : p.id;
    sel.appendChild(o);
  }
  if (ready.some((p) => p.id === prev)) sel.value = prev;
  sel.disabled = !ready.length;

  if (hint) {
    hint.textContent = ready.length
      ? ready.length > 1
        ? `已连接 ${ready.length} 台，发给下拉里选中的那台`
        : '发给已连接的设备'
      : '还没有连接设备，先在左侧点「连接」';
  }
  const fileBtn = $('#chatFile');
  if (fileBtn) fileBtn.disabled = !ready.length;
}

function pickFileAndSend(file) {
  const ready = [...state.peers.values()].filter((p) => p.ready);
  if (!ready.length) {
    log('还没有连接设备，先在左侧点「连接」', 'bad');
    return;
  }
  const want = $('#sendTo').value;
  const chunk = DEFAULT_CHUNK;
  const target = ready.find((p) => p.id === want) || ready[0];
  const el = createTxRow(target.id, file, chunk);
  target.tx = { el };
  target
    .sendFile(file, chunk)
    .catch((err) => {
      log('发送失败：' + err.message, 'bad');
      if (el) el.querySelector('.meta').textContent = '失败: ' + err.message;
    });
}

/* ------------------------------ 信令 ------------------------------ */

function connectSignal() {
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  const ws = new WebSocket(`${proto}://${location.host}/ws/`);
  state.ws = ws;

  ws.onopen = () => log('已连接到服务器');

  ws.onmessage = (e) => {
    const { type, data } = JSON.parse(e.data);

    if (type === '1001') {
      state.me = data.id;
      $('#selfId').textContent = data.id;
      setStun(data.stunPort);
      appendChatSys('已加入群聊');
      return;
    }

    if (type === '1007') {
      appendChat(data.id, data.text, data.ts);
      return;
    }

    if (type === '1002') {
      const ids = new Set(data.map((u) => u.id));
      for (const u of data) {
        if (u.id === state.me) continue;
        const p = state.peers.get(u.id);
        if (!p) {
          state.peers.set(u.id, new Peer(u.id, u));
          log(`发现新设备 ${u.id}`);
        } else {
          // 名字不变，IP 和在线时长可能会变
          p.ip = u.ip || p.ip;
          p.onlineSec = u.onlineSec;
        }
      }
      for (const [id, p] of state.peers) {
        if (!ids.has(id)) {
          p.close();
          state.peers.delete(id);
          log(`设备 ${id} 已离开`);
        }
      }
      renderPeers();
      return;
    }

    if (type === '1003') {
      // 不自动连。要不要连、连谁，交给用户点「连接」决定
      if (state.peers.size) {
        log('可以聊天了。要传文件，先在左侧点「连接」');
      }
      return;
    }

    if (type === '1005') {
      const p = state.peers.get(data.targetId);
      if (p) p.acceptOffer(data.offer);
      return;
    }

    if (type === '1006') {
      const p = state.peers.get(data.targetId);
      if (p) p.acceptAnswer(data.answer);
      return;
    }

    if (type === '1009') {
      // 服务端即将关闭，先把界面切成"已停止"，别让它看起来像网络抖动
      serverGone = true;
      document.body.classList.add('dead');
      log('共享已停止', 'bad');
      const note = document.querySelector('#srvNote');
      if (note) {
        note.textContent = '共享已停止。要重新开启，双击 start.bat。';
        note.className = 'srv-note warn';
      }
      const st = document.querySelector('#srvState');
      if (st) {
        st.textContent = '已停止';
        st.style.color = 'var(--red)';
      }
      const btn = document.querySelector('#srvShutdown');
      if (btn) {
        btn.disabled = true;
        btn.textContent = '已停止';
      }
      return;
    }
  };

  ws.onclose = () => {
    if (serverGone) {
      log('连接已断开（服务已停止）');
      return;
    }
    log('连接断开，正在重连…', 'bad');
    setTimeout(connectSignal, 2000);
  };
}

/* 等 ICE 收集完成再发 SDP：局域网内通常 <200ms，换取"零竞态"。
   顺便统计候选类型，用来判断 mDNS 有没有生效、STUN 有没有起作用。 */
function gather(pc, capMs = 1500) {
  const seen = { host: 0, srflx: 0, prflx: 0, relay: 0, mdns: 0 };

  const onCand = (e) => {
    const c = e.candidate;
    if (!c) return;
    const type = c.type || 'host';
    seen[type] = (seen[type] || 0) + 1;
    if (/\.local(\s|$|:)/i.test(c.candidate || '')) seen.mdns++;
  };
  pc.addEventListener('icecandidate', onCand);

  const finish = () => {
    pc.removeEventListener('icecandidate', onCand);
    return seen;
  };

  if (pc.iceGatheringState === 'complete') return Promise.resolve(finish());
  return new Promise((resolve) => {
    const done = () => {
      pc.removeEventListener('icegatheringstatechange', check);
      resolve(finish());
    };
    const check = () => {
      if (pc.iceGatheringState === 'complete') done();
    };
    pc.addEventListener('icegatheringstatechange', check);
    setTimeout(done, capMs);
  });
}

/* 只用自己的 STUN（服务端内置），所以完全不依赖公网 */
function setStun(port) {
  if (stunApplied) return;
  stunApplied = true;
  if (!port) {
    log('网络通道不可用，可能无法和其他设备直连', 'bad');
    return;
  }
  const host = location.hostname;
  CONF.iceServers = [{ urls: `stun:${host}:${port}` }];
}

/* ------------------------------ 服务器面板 ------------------------------ */

let serverGone = false;   // 服务被关掉后，别再无脑重连
let srvInfo = null;
let noteHoldUntil = 0;    // 刚复制完地址时，别让自动刷新立刻覆盖提示

// http 页面不是安全上下文，navigator.clipboard 通常不可用，必须留降级方案
async function copyText(text) {
  try {
    if (navigator.clipboard && window.isSecureContext) {
      await navigator.clipboard.writeText(text);
      return true;
    }
  } catch {}
  try {
    const ta = document.createElement('textarea');
    ta.value = text;
    ta.style.position = 'fixed';
    ta.style.opacity = '0';
    document.body.appendChild(ta);
    ta.select();
    const ok = document.execCommand('copy');
    ta.remove();
    return ok;
  } catch {
    return false;
  }
}

function fmtUptime(sec) {
  if (sec < 60) return `${sec} 秒`;
  if (sec < 3600) return `${Math.floor(sec / 60)} 分钟`;
  const h = Math.floor(sec / 3600);
  const m = Math.floor((sec % 3600) / 60);
  return `${h} 小时 ${m} 分`;
}

// 把地址渲染成二维码，手机扫一下直接打开传输页，免去手动输入
function renderQr(url) {
  const c = $('#qrCanvas');
  const label = $('#qrUrl');
  if (!window.qrcode || !url) {
    c.style.display = 'none';
    label.textContent = '';
    return;
  }
  const qr = qrcode(0, 'M'); // typeNumber 0 = 自动选尺寸
  try {
    qr.addData(url);
    qr.make();
  } catch (err) {
    c.style.display = 'none';
    label.textContent = '二维码生成失败';
    return;
  }
  c.style.display = '';
  const n = qr.getModuleCount();
  const cell = 6;
  const margin = 2;
  const size = (n + margin * 2) * cell;
  c.width = c.height = size;
  const ctx = c.getContext('2d');
  // 白底黑码，任何主题下手机都能稳定扫到
  ctx.fillStyle = '#ffffff';
  ctx.fillRect(0, 0, size, size);
  ctx.fillStyle = '#000000';
  for (let r = 0; r < n; r++) {
    for (let cc = 0; cc < n; cc++) {
      if (qr.isDark(r, cc)) ctx.fillRect((cc + margin) * cell, (r + margin) * cell, cell, cell);
    }
  }
  label.textContent = url.replace(/^https?:\/\//, '');
}

// 挑一个手机最可能连得上的地址作为默认二维码。
// 用 http：手机收大文件走「直链下载」交给原生下载管理器，它不继承页面上的
// 自签证书例外 —— https 链接会被下载器判成证书错误直接失败（实测踩过这个坑）。
// 大文件不再依赖 OPFS（直链下载不需要安全上下文），http 完全够用。
function bestQrUrl(urls) {
  const host = location.hostname;
  const isLoop = host === 'localhost' || host === '::1' || host.startsWith('127.');
  if (!isLoop) {
    const cur = urls.find((u) => u.ip === host);
    return (cur && cur.url) || (urls[0] && urls[0].url) || null;
  }
  const good = urls.find((u) => !u.risky) || urls[0];
  return (good && good.url) || null;
}

async function refreshServer() {
  try {
    const r = await fetch('/api/info', { cache: 'no-store' });
    if (!r.ok) throw new Error('HTTP ' + r.status);
    srvInfo = await r.json();
  } catch (err) {
    $('#srvState').textContent = '连不上服务器';
    $('#srvState').style.color = 'var(--red)';
    $('#srvNote').textContent = '无法获取服务状态：' + err.message;
    $('#srvNote').className = 'srv-note bad';
    return;
  }

  const info = srvInfo;
  const st = $('#srvState');
  st.style.color = 'var(--green)';
  st.textContent =
    `已运行 ${fmtUptime(info.uptimeSec)} · ${info.clients} 人在线`;

  // 地址列表：显示 http 地址（手机扫码用；大文件直链下载走原生下载器，http 无证书问题）
  const curHost = location.hostname;
  const box = $('#srvUrls');
  box.textContent = '';
  if (!info.urls.length) {
    box.textContent = '未找到可用的网络地址';
  } else {
    for (const u of info.urls) {
      const row = document.createElement('div');
      row.className = 'addr-row';

      const a = document.createElement('span');
      a.className = 'addr';
      a.title = u.risky
        ? '当前网络可能阻止其他设备访问'
        : '点击复制';
      a.textContent = u.url.replace(/^https?:\/\//, '');
      const em = document.createElement('em');
      const tags = [];
      if (u.ip === curHost) tags.push('当前');
      tags.push(u.name);
      if (u.risky) tags.push('公用 · 需放行');
      em.textContent = tags.join(' · ');
      if (u.risky) em.style.color = 'var(--amber)';
      a.appendChild(em);
      a.addEventListener('click', async () => {
        const ok = await copyText(u.url);
        const note = $('#srvNote');
        note.textContent = ok ? `已复制 ${u.url}` : `复制失败，请手动输入 ${u.url}`;
        note.className = 'srv-note ' + (ok ? 'ok' : 'warn');
        noteHoldUntil = Date.now() + 4000;
      });

      const qbtn = document.createElement('button');
      qbtn.className = 'qr-pick';
      qbtn.textContent = '码';
      qbtn.title = '显示这个地址的二维码';
      qbtn.addEventListener('click', (e) => {
        e.stopPropagation();
        renderQr(u.url);
      });

      row.appendChild(a);
      row.appendChild(qbtn);
      box.appendChild(row);
    }
  }

  // 默认二维码：优先当前页面地址，其次第一个非公用网卡的地址
  renderQr(bestQrUrl(info.urls));

  const btn = $('#srvShutdown');
  const note = $('#srvNote');
  const risky = info.urls.filter((u) => u.risky);

  if (Date.now() > noteHoldUntil) {
    if (risky.length) {
      note.textContent =
        `⚠ 当前网络可能阻止其他设备访问。` +
        '对方打不开页面时，运行一次 fix-firewall.bat。';
      note.className = 'srv-note warn';
    } else if (info.localOnly) {
      note.textContent = '在这台电脑上可以停止共享，其他设备不行。';
      note.className = 'srv-note';
    } else {
      note.textContent =
        '为安全起见，只有运行服务的那台电脑可以停止共享。';
      note.className = 'srv-note';
    }
  }

  btn.disabled = !info.localOnly;
}

async function shutdownServer() {
  const btn = $('#srvShutdown');
  const note = $('#srvNote');

  if (btn.disabled) return;   // 防重复点击：正在关闭/已停止时再点直接忽略

  if (!confirm('确定停止共享？\n\n所有设备都会断开，正在传输的文件会失败。')) {
    return;
  }

  btn.disabled = true;
  btn.textContent = '正在停止…';

  try {
    const r = await fetch('/api/shutdown', {
      method: 'POST',
      headers: { 'x-shutdown': '1' },
      cache: 'no-store',
    });
    const data = await r.json().catch(() => ({}));
    if (!r.ok || !data.ok) throw new Error(data.error || 'HTTP ' + r.status);

    serverGone = true;
    document.body.classList.add('dead');
    note.textContent = '共享已停止。要重新开启，双击 start.bat。';
    note.className = 'srv-note ok';
    $('#srvState').textContent = '已停止';
    $('#srvState').style.color = 'var(--red)';
    btn.disabled = true;
    btn.textContent = '已停止';   // 之前漏了这句，按钮会永远卡在「正在关闭…」
    log('服务器已关闭', 'bad');

    // 断开信令连接，避免一直重连
    try { state.ws && state.ws.close(); } catch {}
  } catch (err) {
    btn.disabled = false;
    btn.textContent = '停止共享';
    note.textContent = '停止失败：' + err.message;
    note.className = 'srv-note bad';
  }
}

/* ------------------------------ 绑定 UI ------------------------------ */

function boot() {
  connectSignal();
  renderPeers();

  const input = $('#fileInput');
  const chatBody = $('#chatBody');

  // 聊天栏「＋」打开文件管理器，选择后发给上面选中的已直连设备
  $('#chatFile').addEventListener('click', () => input.click());
  input.addEventListener('change', () => {
    if (input.files[0]) pickFileAndSend(input.files[0]);
    input.value = '';
  });

  // 拖文件到聊天栏 → 传输（保留电脑端拖拽习惯）
  chatBody.addEventListener('dragover', (e) => {
    e.preventDefault();
    chatBody.classList.add('hot');
  });
  chatBody.addEventListener('dragleave', () => chatBody.classList.remove('hot'));
  chatBody.addEventListener('drop', (e) => {
    e.preventDefault();
    chatBody.classList.remove('hot');
    const f = e.dataTransfer.files[0];
    if (f) pickFileAndSend(f);
  });

  // 群聊：回车或点「发送」
  $('#chatSend').addEventListener('click', sendChat);
  $('#chatText').addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      sendChat();
    }
  });

  // 回到页面时立刻刷新一次直链下载状态（手机下载期间页面在后台，轮询会被节流）
  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState !== 'visible') return;
    for (const p of state.peers.values()) {
      if (p.rx && p.rx.pollNow) p.rx.pollNow();
    }
  });

  $('#bench100') && $('#bench100').addEventListener('click', () => bench(100));
  $('#bench500') && $('#bench500').addEventListener('click', () => bench(500));

  $('#srvShutdown').addEventListener('click', shutdownServer);
  $('#srvRefresh').addEventListener('click', () => {
    $('#srvNote').textContent = '';
    refreshServer();
  });

  refreshServer();
  // 让"运行时长/在线连接数"自己走，免得每次都要手点刷新
  setInterval(() => {
    if (!serverGone && document.visibilityState === 'visible') refreshServer();
  }, 15000);
}

function bench(mb) {
  const size = mb * 1024 * 1024;
  log(`正在生成 ${mb}MB 测试文件…`);
  const buf = new Uint8Array(size);
  // 填充一点非零数据，避免某些系统上的稀疏文件优化影响结果
  for (let i = 0; i < size; i += 4096) buf[i] = i & 0xff;
  const file = new File([buf], `bench-${mb}mb.bin`, { type: 'application/octet-stream' });
  pickFileAndSend(file);
}

document.addEventListener('DOMContentLoaded', boot);
