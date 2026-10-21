'use strict';

/**
 * 停止服务。
 *
 * 顺序：
 *   1. 先走 HTTP /api/shutdown 优雅关闭（会清理 PID 文件、通知已连接的浏览器）
 *   2. 不行再按 PID 文件强杀
 *   3. 还不行就按端口找进程强杀
 *
 * 用法：
 *   node stop.js          停默认端口 8081
 *   node stop.js 9000     停指定端口
 */

const fs = require('node:fs');
const http = require('node:http');
const path = require('node:path');
const { execFileSync } = require('node:child_process');

const PID_FILE = path.join(__dirname, '.server.pid');
const PORT = Number(process.argv[2]) || 8081;

const c = {
  ok: (s) => `\x1b[32m${s}\x1b[0m`,
  bad: (s) => `\x1b[31m${s}\x1b[0m`,
  dim: (s) => `\x1b[90m${s}\x1b[0m`,
};

function readPid() {
  try {
    const raw = fs.readFileSync(PID_FILE, 'utf8').trim();
    const pid = Number(raw);
    return Number.isInteger(pid) && pid > 0 ? pid : null;
  } catch {
    return null;
  }
}

function isAlive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch (e) {
    return e.code === 'EPERM'; // 存在但没权限，也算活着
  }
}

function tryHttpShutdown() {
  return new Promise((resolve) => {
    const req = http.request(
      {
        host: '127.0.0.1',
        port: PORT,
        path: '/api/shutdown',
        method: 'POST',
        headers: { 'x-shutdown': '1', 'Content-Length': 0 },
        timeout: 3000,
      },
      (res) => {
        let body = '';
        res.on('data', (d) => (body += d));
        res.on('end', () => resolve(res.statusCode === 200));
      }
    );
    req.on('timeout', () => { req.destroy(); resolve(false); });
    req.on('error', () => resolve(false));
    req.end();
  });
}

/** 按监听端口反查 PID，用于 PID 文件丢失的情况 */
function findPidByPort(port) {
  try {
    const out = execFileSync('netstat', ['-ano'], { encoding: 'utf8', windowsHide: true });
    for (const line of out.split(/\r?\n/)) {
      // 只认 LISTENING 行，且本地地址端口精确匹配
      if (!/LISTENING/i.test(line)) continue;
      const cols = line.trim().split(/\s+/);
      if (cols.length < 5) continue;
      const local = cols[1];
      const m = local.match(/:(\d+)$/);
      if (m && Number(m[1]) === port) return Number(cols[cols.length - 1]);
    }
  } catch {}
  return null;
}

function forceKill(pid) {
  try {
    execFileSync('taskkill', ['/PID', String(pid), '/F'], {
      stdio: 'ignore',
      windowsHide: true,
    });
    return true;
  } catch {
    return false;
  }
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

(async () => {
  const pid = readPid();

  console.log('');
  console.log('  正在停止 P2P 传输服务…');
  console.log(`  ${c.dim('端口 ' + PORT + (pid ? '，PID ' + pid : ''))}`);

  // 1) 优雅关闭
  const graceful = await tryHttpShutdown();
  if (graceful) {
    // 服务端是异步退出的，等它收尾
    for (let i = 0; i < 10; i++) {
      await sleep(200);
      if (!pid || !isAlive(pid)) break;
    }
    if (!pid || !isAlive(pid)) {
      cleanupPidFile();
      console.log('  ' + c.ok('已停止') + c.dim('（优雅关闭，已通知所有浏览器）'));
      console.log('');
      return;
    }
  }

  // 2) 按 PID 文件强杀
  let killed = false;
  if (pid && isAlive(pid)) {
    killed = forceKill(pid);
    if (killed) {
      await sleep(400);
      cleanupPidFile();
      console.log('  ' + c.ok('已停止') + c.dim('（强杀 PID ' + pid + '）'));
      console.log('');
      return;
    }
  }

  // 3) 按端口反查
  const byPort = findPidByPort(PORT);
  if (byPort) {
    if (byPort === process.pid) {
      console.log('  ' + c.bad('端口 ' + PORT + ' 被当前进程占用，跳过'));
      process.exit(1);
    }
    if (forceKill(byPort)) {
      cleanupPidFile();
      console.log('  ' + c.ok('已停止') + c.dim('（强杀占用端口的 PID ' + byPort + '）'));
      console.log('');
      return;
    }
  }

  cleanupPidFile();
  console.log('  ' + c.dim('没有发现正在运行的服务，端口 ' + PORT + ' 是空闲的。'));
  console.log('');
})();

function cleanupPidFile() {
  try {
    if (fs.existsSync(PID_FILE)) fs.unlinkSync(PID_FILE);
  } catch {}
}
