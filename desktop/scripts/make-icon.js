"use strict";

// 程序化生成应用图标：青绿主色圆角方块 + 白色道路/车流几何图形
// 输出 build/icon.ico（多尺寸）与 build/icon.png（512）
// 用法：node scripts/make-icon.js

const fs = require("fs");
const path = require("path");
const zlib = require("zlib");

const OUT_DIR = path.join(__dirname, "..", "build");

// ---- 极简 PNG 编码器（无依赖） ----

function crc32(buf) {
  let table = crc32.table;
  if (!table) {
    table = new Int32Array(256);
    for (let n = 0; n < 256; n++) {
      let c = n;
      for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
      table[n] = c;
    }
    crc32.table = table;
  }
  let crc = -1;
  for (let i = 0; i < buf.length; i++) crc = (crc >>> 8) ^ table[(crc ^ buf[i]) & 0xff];
  return (crc ^ -1) >>> 0;
}

function chunk(type, data) {
  const len = Buffer.alloc(4);
  len.writeUInt32BE(data.length);
  const typeBuf = Buffer.from(type, "ascii");
  const crcBuf = Buffer.alloc(4);
  crcBuf.writeUInt32BE(crc32(Buffer.concat([typeBuf, data])));
  return Buffer.concat([len, typeBuf, data, crcBuf]);
}

function encodePng(width, height, rgba) {
  const signature = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]);
  const ihdr = Buffer.alloc(13);
  ihdr.writeUInt32BE(width, 0);
  ihdr.writeUInt32BE(height, 4);
  ihdr[8] = 8; // bit depth
  ihdr[9] = 6; // color type RGBA
  const raw = Buffer.alloc((width * 4 + 1) * height);
  for (let y = 0; y < height; y++) {
    raw[y * (width * 4 + 1)] = 0; // filter none
    rgba.copy(raw, y * (width * 4 + 1) + 1, y * width * 4, (y + 1) * width * 4);
  }
  const idat = zlib.deflateSync(raw, { level: 9 });
  return Buffer.concat([signature, chunk("IHDR", ihdr), chunk("IDAT", idat), chunk("IEND", Buffer.alloc(0))]);
}

// ---- 图标绘制 ----

const BG = [14, 124, 102, 255]; // #0E7C66 青绿
const BG_DARK = [11, 100, 83, 255];
const WHITE = [255, 255, 255, 255];
const AMBER = [251, 191, 36, 255];

function drawIcon(size) {
  const rgba = Buffer.alloc(size * size * 4);
  const radius = Math.round(size * 0.22);
  const set = (x, y, color) => {
    if (x < 0 || y < 0 || x >= size || y >= size) return;
    const idx = (y * size + x) * 4;
    rgba[idx] = color[0];
    rgba[idx + 1] = color[1];
    rgba[idx + 2] = color[2];
    rgba[idx + 3] = color[3];
  };

  // 圆角方块背景（带轻微纵向渐变）
  for (let y = 0; y < size; y++) {
    for (let x = 0; x < size; x++) {
      const inCorner =
        (x < radius && y < radius && (x - radius) * (x - radius) + (y - radius) * (y - radius) > radius * radius) ||
        (x >= size - radius && y < radius && (x - (size - radius - 1)) * (x - (size - radius - 1)) + (y - radius) * (y - radius) > radius * radius) ||
        (x < radius && y >= size - radius && (x - radius) * (x - radius) + (y - (size - radius - 1)) * (y - (size - radius - 1)) > radius * radius) ||
        (x >= size - radius && y >= size - radius && (x - (size - radius - 1)) * (x - (size - radius - 1)) + (y - (size - radius - 1)) * (y - (size - radius - 1)) > radius * radius);
      if (inCorner) continue;
      const t = y / size;
      const color = t < 0.5 ? BG : BG_DARK;
      set(x, y, color);
    }
  }

  // 中央道路：梯形（上窄下宽）
  const roadTopW = size * 0.10;
  const roadBottomW = size * 0.30;
  const roadTopY = size * 0.18;
  const roadBottomY = size * 0.86;
  for (let y = Math.floor(roadTopY); y <= Math.floor(roadBottomY); y++) {
    const t = (y - roadTopY) / (roadBottomY - roadTopY);
    const halfW = (roadTopW + (roadBottomW - roadTopW) * t) / 2;
    const cx = size / 2;
    for (let x = Math.floor(cx - halfW); x <= Math.ceil(cx + halfW); x++) {
      set(x, y, WHITE);
    }
  }

  // 道路中线：虚线
  const dashH = size * 0.07;
  const gapH = size * 0.05;
  for (let y = Math.floor(roadTopY + size * 0.04); y < Math.floor(roadBottomY - size * 0.02); y += Math.floor(dashH + gapH)) {
    const t = (y - roadTopY) / (roadBottomY - roadTopY);
    const halfW = (roadTopW + (roadBottomW - roadTopW) * t) / 2;
    const dashW = Math.max(1, Math.round(halfW * 0.28));
    const cx = size / 2;
    for (let dy = 0; dy < dashH && y + dy < size; dy++) {
      for (let x = Math.floor(cx - dashW / 2); x <= Math.ceil(cx + dashW / 2); x++) {
        set(x, y + dy, BG);
      }
    }
  }

  // 左上角琥珀色定位点（卡口语义）
  const dotCx = size * 0.26;
  const dotCy = size * 0.26;
  const dotR = size * 0.085;
  for (let y = Math.floor(dotCy - dotR); y <= Math.ceil(dotCy + dotR); y++) {
    for (let x = Math.floor(dotCx - dotR); x <= Math.ceil(dotCx + dotR); x++) {
      const d = (x - dotCx) * (x - dotCx) + (y - dotCy) * (y - dotCy);
      if (d <= dotR * dotR) set(x, y, AMBER);
    }
  }

  return rgba;
}

// ---- ICO 容器 ----

function buildIco(sizes) {
  const images = sizes.map((s) => ({ size: s, png: encodePng(s, s, drawIcon(s)) }));
  const header = Buffer.alloc(6);
  header.writeUInt16LE(0, 0); // reserved
  header.writeUInt16LE(1, 2); // type icon
  header.writeUInt16LE(images.length, 4);

  const entries = [];
  let offset = 6 + images.length * 16;
  for (const img of images) {
    const entry = Buffer.alloc(16);
    entry[0] = img.size >= 256 ? 0 : img.size;
    entry[1] = img.size >= 256 ? 0 : img.size;
    entry[2] = 0; // palette
    entry[3] = 0; // reserved
    entry.writeUInt16LE(1, 4); // planes
    entry.writeUInt16LE(32, 6); // bpp
    entry.writeUInt32LE(img.png.length, 8);
    entry.writeUInt32LE(offset, 12);
    offset += img.png.length;
    entries.push(entry);
  }

  return Buffer.concat([header, ...entries, ...images.map((i) => i.png)]);
}

fs.mkdirSync(OUT_DIR, { recursive: true });
const ico = buildIco([16, 24, 32, 48, 64, 128, 256]);
fs.writeFileSync(path.join(OUT_DIR, "icon.ico"), ico);
const png = encodePng(512, 512, drawIcon(512));
fs.writeFileSync(path.join(OUT_DIR, "icon.png"), png);
console.log(`icon.ico (${ico.length} bytes) and icon.png written to ${OUT_DIR}`);
