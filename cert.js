'use strict';

/* =========================================================================
 * 自签 X.509 证书生成器 —— 零依赖，只用 Node 内置 crypto
 *
 * 为什么需要它：
 *   手机通过 http://192.168.x.x 访问页面时不是"安全上下文"，浏览器的
 *   OPFS（Origin Private File System）和 File System Access API 都不可用，
 *   大文件只能整块塞进内存，670MB 直接撑爆保存环节。
 *   拿到安全上下文（https 或 localhost），手机才能边收边写盘。
 *
 * 实现：crypto 生成 RSA 密钥 + 手动 ASN.1 DER 编码证书（约 120 行），
 *   避免引入 openssl / node-forge 等外部依赖。
 * ========================================================================= */

const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');

/* ---------------- ASN.1 DER 编码辅助 ---------------- */

function lenBytes(n) {
  if (n < 0x80) return Buffer.from([n]);
  const tmp = [];
  while (n > 0) { tmp.unshift(n & 0xff); n = Math.floor(n / 256); }
  return Buffer.from([0x80 | tmp.length, ...tmp]);
}

function tlv(tag, content) {
  return Buffer.concat([Buffer.from([tag]), lenBytes(content.length), content]);
}

const SEQ = (c) => tlv(0x30, c);        // SEQUENCE
const SET = (c) => tlv(0x31, c);        // SET
const INTEGER = (n) => {                // 非负 INTEGER
  let hex = n.toString(16);
  if (hex.length % 2) hex = '0' + hex;
  const buf = Buffer.from(hex, 'hex');
  return tlv(0x02, buf[0] & 0x80 ? Buffer.concat([Buffer.from([0]), buf]) : buf);
};
const OID = (str) => {                  // 点分 OID 编码
  const parts = str.split('.').map(Number);
  const bytes = [parts[0] * 40 + parts[1]];
  for (let i = 2; i < parts.length; i++) {
    let v = parts[i];
    const stack = [v & 0x7f];
    v = Math.floor(v / 128);
    while (v > 0) { stack.unshift((v & 0x7f) | 0x80); v = Math.floor(v / 128); }
    bytes.push(...stack);
  }
  return tlv(0x06, Buffer.from(bytes));
};
const BITSTRING = (bits) => tlv(0x03, Buffer.concat([Buffer.from([0]), bits]));
const OCTET = (b) => tlv(0x04, b);
const NULL_VAL = Buffer.from([0x05, 0x00]);
const UTF8 = (s) => tlv(0x0c, Buffer.from(s, 'utf8'));
const IA5 = (s) => tlv(0x16, Buffer.from(s, 'ascii'));

/* ---------------- 证书字段 ---------------- */

// Name：单个 RDN，只含 CN
function name(cn) {
  const attr = SEQ(Buffer.concat([OID('2.5.4.3'), UTF8(cn)]));
  return SEQ(SET(attr));
}

// sha256WithRSAEncryption
function sigAlg() {
  return SEQ(Buffer.concat([OID('1.2.840.113549.1.1.11'), NULL_VAL]));
}

// UTCTime：YYMMDDHHMMSSZ（有效期 10 年，两位年足够）
function utcTime(d) {
  const yy = String(d.getUTCFullYear()).slice(-2);
  const mm = String(d.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(d.getUTCDate()).padStart(2, '0');
  const hh = String(d.getUTCHours()).padStart(2, '0');
  const mi = String(d.getUTCMinutes()).padStart(2, '0');
  const ss = String(d.getUTCSeconds()).padStart(2, '0');
  return tlv(0x17, Buffer.from(`${yy}${mm}${dd}${hh}${mi}${ss}Z`, 'ascii'));
}

// subjectAltName：dNSName [2] IA5String，iPAddress [7] OCTET STRING
function generalNames(hosts) {
  const items = [];
  for (const h of hosts) {
    if (/^\d+\.\d+\.\d+\.\d+$/.test(h)) {
      items.push(tlv(0x87, Buffer.from(h.split('.').map(Number))));
    } else {
      items.push(tlv(0x82, Buffer.from(h, 'ascii')));
    }
  }
  return SEQ(Buffer.concat(items));
}

function extension(oidStr, value) {
  return SEQ(Buffer.concat([OID(oidStr), OCTET(value)]));
}

/* ---------------- 主流程 ---------------- */

function serialize(bigint) {
  // serialNumber 需要正整数，且最高位不能是 1（否则被当成负数）
  const bytes = crypto.randomBytes(16);
  let i = 0;
  while (i < bytes.length - 1 && bytes[i] === 0) i++;
  const b = bytes.subarray(i);
  return tlv(0x02, b[0] & 0x80 ? Buffer.concat([Buffer.from([0]), b]) : b);
}

function generate(hosts, validityDays) {
  const { privateKey, publicKey } = crypto.generateKeyPairSync('rsa', {
    modulusLength: 2048,
    publicExponent: 0x10001,
  });
  const spki = publicKey.export({ type: 'spki', format: 'der' });

  const now = new Date();
  const notAfter = new Date(now.getTime() + validityDays * 86400000);

  const version = tlv(0xa0, INTEGER(2));        // [0] EXPLICIT v3
  const validity = SEQ(Buffer.concat([utcTime(now), utcTime(notAfter)]));

  const san = extension('2.5.29.17', generalNames(hosts));
  const basicConstraints = extension('2.5.29.19', SEQ(tlv(0x01, Buffer.from([0xff])))); // CA:TRUE
  const keyUsage = extension('2.5.29.15', BITSTRING(Buffer.from([0xa0]))); // digitalSignature | keyEncipherment

  const extensions = tlv(0xa3, SEQ(Buffer.concat([san, basicConstraints, keyUsage]))); // [3] EXPLICIT

  const tbs = SEQ(Buffer.concat([
    version,
    serialize(),
    sigAlg(),
    name('P2P LAN Transfer'),
    validity,
    name('P2P LAN Transfer'),
    spki,
    extensions,
  ]));

  const signature = crypto.sign('sha256', tbs, privateKey);
  const cert = SEQ(Buffer.concat([tbs, sigAlg(), BITSTRING(signature)]));

  const b64 = cert.toString('base64');
  const pem = '-----BEGIN CERTIFICATE-----\n' +
    b64.match(/.{1,64}/g).join('\n') +
    '\n-----END CERTIFICATE-----\n';

  return {
    key: privateKey.export({ type: 'pkcs8', format: 'pem' }),
    cert: pem,
  };
}

/* ---------------- 缓存与复用 ---------------- */

// 证书按"当前局域网 IP 集合"缓存。换网络 IP 变了就重新生成，避免 SAN 过期。
function ensureCert(certDir, hosts) {
  const keyPath = path.join(certDir, 'key.pem');
  const certPath = path.join(certDir, 'cert.pem');
  const metaPath = path.join(certDir, 'hosts.json');

  try {
    fs.mkdirSync(certDir, { recursive: true });
  } catch {}

  const want = [...hosts].sort();
  let reusable = false;
  try {
    const have = JSON.parse(fs.readFileSync(metaPath, 'utf8'));
    reusable = Array.isArray(have) &&
      want.every((h) => have.includes(h)) &&
      fs.existsSync(keyPath) &&
      fs.existsSync(certPath);
  } catch {}

  if (reusable) {
    return {
      key: fs.readFileSync(keyPath, 'utf8'),
      cert: fs.readFileSync(certPath, 'utf8'),
      fresh: false,
    };
  }

  const gen = generate(want, 3650);
  try {
    fs.writeFileSync(keyPath, gen.key, { mode: 0o600 });
    fs.writeFileSync(certPath, gen.cert);
    fs.writeFileSync(metaPath, JSON.stringify(want));
  } catch {}
  return { ...gen, fresh: true };
}

module.exports = { ensureCert, generate };
