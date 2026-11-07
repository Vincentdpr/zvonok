/* Общее для мессенджера и звонков: хранилище, профиль, аватарки, MQTT */
(function () {
  'use strict';
  const ZV = window.ZV = {};
  const enc = new TextEncoder(), dec = new TextDecoder();
  ZV.enc = enc; ZV.dec = dec;
  ZV.$ = (id) => document.getElementById(id);

  /* Публичные MQTT-серверы: через них браузеры находят друг друга и
     передают зашифрованные сообщения. Используются все сразу. */
  ZV.BROKERS = [
    'wss://broker-cn.emqx.io:8084/mqtt',
    'wss://broker.emqx.io:8084/mqtt',
    'wss://broker.hivemq.com:8884/mqtt',
  ];

  /* ---------- Хранилище ---------- */
  ZV.lsGet = (k, def) => { try { const v = JSON.parse(localStorage.getItem(k)); return v == null ? def : v; } catch (e) { return def; } };
  ZV.lsSet = (k, v) => { try { localStorage.setItem(k, JSON.stringify(v)); return true; } catch (e) { return false; } };
  ZV.loadDev = () => ZV.lsGet('zvonok_dev', {}) || {};
  ZV.saveDev = (p) => ZV.lsSet('zvonok_dev', Object.assign(ZV.loadDev(), p));

  /* ---------- Мелочи ---------- */
  ZV.rid = () => Array.from(crypto.getRandomValues(new Uint8Array(8)), (b) => b.toString(16).padStart(2, '0')).join('');
  ZV.hex = (buf) => Array.from(new Uint8Array(buf), (b) => b.toString(16).padStart(2, '0')).join('');
  ZV.b64u = (u8) => { let s = ''; for (const b of u8) s += String.fromCharCode(b); return btoa(s).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, ''); };
  ZV.b64d = (s) => { s = String(s).replace(/-/g, '+').replace(/_/g, '/'); while (s.length % 4) s += '='; const b = atob(s); const u = new Uint8Array(b.length); for (let i = 0; i < b.length; i++) u[i] = b.charCodeAt(i); return u; };
  ZV.sha = async (str) => ZV.hex(await crypto.subtle.digest('SHA-256', typeof str === 'string' ? enc.encode(str) : str));
  ZV.fmtSize = (n) => n < 1024 ? n + ' Б' : n < 1048576 ? (n / 1024).toFixed(0) + ' КБ' : (n / 1048576).toFixed(1) + ' МБ';
  ZV.fmtTime = (ts) => {
    const d = new Date(ts), now = new Date();
    const t = d.toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' });
    return d.toDateString() === now.toDateString() ? t : d.toLocaleDateString('ru-RU', { day: 'numeric', month: 'short' }) + ', ' + t;
  };
  ZV.randomCode = () => { const a = 'abcdefghjkmnpqrstuvwxyz23456789'; let s = ''; for (const b of crypto.getRandomValues(new Uint8Array(10))) s += a[b % a.length]; return s; };
  ZV.linkify = (el, text) => {
    const re = /(https?:\/\/[^\s<]+)/g; let last = 0, m;
    while ((m = re.exec(text))) {
      el.appendChild(document.createTextNode(text.slice(last, m.index)));
      const a = document.createElement('a'); a.href = m[1]; a.textContent = m[1]; a.target = '_blank'; a.rel = 'noopener noreferrer'; el.appendChild(a);
      last = m.index + m[1].length;
    }
    el.appendChild(document.createTextNode(text.slice(last)));
  };
  ZV.toast = (text) => {
    let t = ZV.$('toast');
    if (!t) { t = document.createElement('div'); t.id = 'toast'; document.body.appendChild(t); }
    t.textContent = text; t.hidden = false; clearTimeout(t._tm);
    t._tm = setTimeout(() => { t.hidden = true; }, 3500);
  };

  /* ---------- Реакции и просмотр картинок ---------- */
  ZV.EMOJI = ['👍', '❤️', '😂', '😮', '😢', '🔥', '👎', '🙏'];
  ZV.showPicker = (el, cb) => {
    document.querySelectorAll('.picker').forEach((x) => x.remove());
    const pk = document.createElement('div'); pk.className = 'picker';
    ZV.EMOJI.forEach((e) => { const b = document.createElement('button'); b.textContent = e; b.onclick = (ev) => { ev.stopPropagation(); pk.remove(); cb(e); }; pk.appendChild(b); });
    el.appendChild(pk);
    setTimeout(() => document.addEventListener('click', () => pk.remove(), { once: true }), 0);
  };
  ZV.lightbox = (url) => {
    let lb = ZV.$('lightbox');
    if (!lb) { lb = document.createElement('div'); lb.id = 'lightbox'; lb.className = 'modal'; lb.appendChild(document.createElement('img')); lb.onclick = () => { lb.hidden = true; }; document.body.appendChild(lb); }
    lb.querySelector('img').src = url; lb.hidden = false;
  };

  /* ---------- Звук ---------- */
  let actx = null;
  ZV.actx = () => {
    if (!actx) actx = new (window.AudioContext || window.webkitAudioContext)();
    if (actx.state === 'suspended') actx.resume().catch(() => {});
    return actx;
  };
  ZV.beep = (f, dur) => {
    try {
      const c = ZV.actx(); const o = c.createOscillator(), g = c.createGain();
      o.frequency.value = f || 880; g.gain.value = 0.06; o.connect(g).connect(c.destination);
      o.start(); o.stop(c.currentTime + (dur || 0.2));
    } catch (e) {}
  };

  /* ---------- Профиль ---------- */
  ZV.STATUS = { online: 'В сети', idle: 'Отошёл', dnd: 'Не беспокоить', invisible: 'Невидимка', off: 'Не в сети' };
  const isImg = (s) => typeof s === 'string' && /^data:image\/(png|jpeg|webp|gif);base64,/.test(s) && s.length < 200000;
  ZV.cleanProfile = (p) => {
    p = p || {};
    return {
      name: String(p.name || 'Гость').slice(0, 32),
      username: String(p.username || '').toLowerCase().replace(/[^a-z0-9_.]/g, '').slice(0, 32),
      pronouns: String(p.pronouns || '').slice(0, 40),
      avatar: isImg(p.avatar) ? p.avatar : '',
      banner: isImg(p.banner) ? p.banner : '',
      bannerColor: /^#[0-9a-f]{6}$/i.test(p.bannerColor || '') ? p.bannerColor : '',
      about: String(p.about || '').slice(0, 190),
      status: ZV.STATUS[p.status] && p.status !== 'off' ? p.status : 'online',
      custom: String(p.custom || '').slice(0, 128),
    };
  };
  /* Профиль — один объект на всю страницу; меняется на месте, не пересоздаётся */
  ZV.profile = ZV.cleanProfile(ZV.lsGet('zvonok_profile', {}) || {});
  if (!ZV.profile.name || ZV.profile.name === 'Гость') { try { const n = localStorage.getItem('zvonok_name'); if (n) ZV.profile.name = n; } catch (e) {} }
  ZV.saveProfile = () => ZV.lsSet('zvonok_profile', ZV.profile);
  /* То, что видят другие: «невидимку» в звонке показываем как «в сети» */
  ZV.publicProfile = () => { const p = ZV.cleanProfile(ZV.profile); if (p.status === 'invisible') p.status = 'online'; return p; };

  ZV.colorOf = (s) => { let h = 0; for (const c of String(s)) h = (h * 31 + c.charCodeAt(0)) >>> 0; return 'hsl(' + (h % 360) + ',45%,42%)'; };
  ZV.avatar = (p, size, opts) => {
    opts = opts || {};
    const w = document.createElement('span'); w.className = 'ava'; w.style.width = w.style.height = size + 'px';
    if (p && p.avatar) { const i = document.createElement('img'); i.src = p.avatar; i.alt = ''; w.appendChild(i); }
    else { const l = document.createElement('span'); l.className = 'letter'; l.style.background = ZV.colorOf((p && p.name) || '?'); l.style.fontSize = Math.round(size * 0.42) + 'px'; l.textContent = ((p && p.name) || '?').trim().charAt(0).toUpperCase() || '?'; w.appendChild(l); }
    if (opts.dot) { const d = document.createElement('span'); d.className = 'dot ' + (opts.status || (p && p.status) || 'online'); w.appendChild(d); }
    return w;
  };
  /* Карточка профиля: баннер, аватарка, имя, ник, статус, «Обо мне» */
  ZV.profileCard = (p, opts) => {
    opts = opts || {};
    const c = document.createElement('div'); c.className = 'profcard';
    const b = document.createElement('div'); b.className = 'banner';
    if (p.banner) b.style.backgroundImage = 'url("' + p.banner + '")';
    b.style.backgroundColor = p.bannerColor || ZV.colorOf(p.name);
    const av = ZV.avatar(p, 88, { dot: true, status: opts.status }); av.classList.add('big');
    const body = document.createElement('div'); body.className = 'pbody';
    const h = document.createElement('h3'); h.textContent = p.name;
    const sub = document.createElement('div'); sub.className = 'sub';
    sub.textContent = [p.username ? '@' + p.username : '', p.pronouns].filter(Boolean).join(' · ');
    body.append(h, sub);
    if (p.custom) { const cs = document.createElement('div'); cs.className = 'custom'; cs.textContent = p.custom; body.appendChild(cs); }
    if (opts.extra) { const ex = document.createElement('div'); ex.className = 'sub'; ex.textContent = opts.extra; body.appendChild(ex); }
    if (p.about) {
      const t = document.createElement('div'); t.className = 'sect'; t.textContent = 'Обо мне';
      const a = document.createElement('div'); a.className = 'about'; ZV.linkify(a, p.about);
      body.append(t, a);
    }
    c.append(b, av, body);
    return c;
  };

  /* Сжать картинку: square — обрезать до квадрата (аватарка) */
  ZV.compressImage = async (file, maxW, maxH, quality, maxLen, square) => {
    const bmp = await createImageBitmap(file);
    let sx = 0, sy = 0, sw = bmp.width, sh = bmp.height;
    if (square) { const s = Math.min(sw, sh); sx = (sw - s) / 2; sy = (sh - s) / 2; sw = sh = s; }
    else if (maxH && maxW / maxH !== sw / sh && maxH < 400) {
      // баннер: обрезаем под пропорции
      const r = maxW / maxH;
      if (sw / sh > r) { const nw = sh * r; sx = (sw - nw) / 2; sw = nw; } else { const nh = sw / r; sy = (sh - nh) / 2; sh = nh; }
    }
    let scale = Math.min(1, maxW / sw, maxH / sh);
    let q = quality, url = '';
    for (let i = 0; i < 6; i++) {
      const c = document.createElement('canvas'); c.width = Math.round(sw * scale); c.height = Math.round(sh * scale);
      c.getContext('2d').drawImage(bmp, sx, sy, sw, sh, 0, 0, c.width, c.height);
      url = c.toDataURL('image/jpeg', q);
      if (url.length <= maxLen) break;
      q = Math.max(0.4, q - 0.1); scale *= 0.8;
    }
    bmp.close && bmp.close();
    return url.length <= maxLen ? url : '';
  };

  /* ============================================================
     Мини-клиент MQTT 3.1.1 поверх WebSocket (без библиотек)
     ============================================================ */
  ZV.Mqtt = function (url, onMsg, onState) {
    let ws = null, buf = new Uint8Array(0), ping = 0, closed = false, delay = 1000, pid = 1;
    const topics = new Set(); const self = this; this.ok = false;
    const str = (s) => { const b = enc.encode(s); return [b.length >> 8, b.length & 255, ...b]; };
    function packet(type, body) {
      const hdr = [type]; let x = body.length;
      do { let d = x % 128; x = Math.floor(x / 128); if (x > 0) d |= 128; hdr.push(d); } while (x > 0);
      const out = new Uint8Array(hdr.length + body.length); out.set(hdr); out.set(body, hdr.length); return out;
    }
    const send = (p) => { if (ws && ws.readyState === 1) ws.send(p); };
    function sub(t) { const id = (pid++ & 0xffff) || 1; send(packet(0x82, [id >> 8, id & 255, ...str(t), 0])); }
    function connect() {
      if (closed) return;
      try { ws = new WebSocket(url, 'mqtt'); } catch (e) { retry(); return; }
      ws.binaryType = 'arraybuffer'; buf = new Uint8Array(0);
      ws.onopen = () => send(packet(0x10, [...str('MQTT'), 4, 0x02, 0, 30, ...str('zv' + Math.random().toString(36).slice(2, 14))]));
      ws.onmessage = (e) => { const d = new Uint8Array(e.data); const n = new Uint8Array(buf.length + d.length); n.set(buf); n.set(d, buf.length); buf = n; parse(); };
      ws.onclose = () => { if (self.ok) { self.ok = false; onState(); } clearInterval(ping); retry(); };
      ws.onerror = () => {};
    }
    function retry() { if (!closed) { setTimeout(connect, delay); delay = Math.min(delay * 2, 15000); } }
    function parse() {
      while (buf.length >= 2) {
        let mult = 1, len = 0, i = 1, b;
        do { if (i >= buf.length) return; b = buf[i++]; len += (b & 127) * mult; mult *= 128; } while (b & 128);
        if (buf.length < i + len) return;
        const type = buf[0] >> 4, flags = buf[0] & 15, body = buf.slice(i, i + len);
        buf = buf.slice(i + len);
        if (type === 2) {
          if (body[1] === 0) { self.ok = true; delay = 1000; topics.forEach(sub); clearInterval(ping); ping = setInterval(() => send(new Uint8Array([0xC0, 0])), 20000); onState(); }
          else ws.close();
        } else if (type === 3) {
          const tl = (body[0] << 8) | body[1];
          const topic = dec.decode(body.subarray(2, 2 + tl));
          let p = 2 + tl; if ((flags >> 1) & 3) p += 2;
          onMsg(topic, body.slice(p));
        }
      }
    }
    this.subscribe = (t) => { topics.add(t); if (self.ok) sub(t); };
    this.publish = (t, payload) => {
      if (!self.ok) return false;
      const tb = str(t); const body = new Uint8Array(tb.length + payload.length);
      body.set(tb); body.set(payload, tb.length); send(packet(0x30, body)); return true;
    };
    this.close = () => { closed = true; clearInterval(ping); try { ws && ws.close(); } catch (e) {} };
    connect();
  };
})();
