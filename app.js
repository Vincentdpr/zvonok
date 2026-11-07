/* Мессенджер: аккаунт, друзья, личные сообщения, беседы, звонки друзьям.
   Без сервера: аккаунт — ключ ECDH в браузере, сообщения шифруются для
   каждого получателя (AES-256) и идут через публичные MQTT-серверы.
   Если получатель не в сети — сообщение ждёт и уходит само, когда вы оба онлайн. */
(function () {
  'use strict';
  const ZV = window.ZV, $ = ZV.$, enc = ZV.enc, dec = ZV.dec, profile = ZV.profile;
  const MAX_GROUP = 10, MAX_IMG = 200000, CALL_FRESH = 3 * 3600000;

  let myPriv = null, myPub = '', myUid = '', inbox = '', myHash = '';
  let brokers = [], rx = Promise.resolve(), netOk = false;
  const keyCache = new Map(), uidCache = new Map();
  let friends = ZV.lsGet('zv_friends', {}) || {};   // uid → { pub, state: friend|incoming|outgoing, ts }
  let groups = ZV.lsGet('zv_groups', {}) || {};     // gid → { gid, name, owner, v, members: { uid: pub } }
  let profiles = ZV.lsGet('zv_profiles', {}) || {}; // uid → { p, ph }
  let outbox = ZV.lsGet('zv_outbox', []) || [];     // [{ uid, pub, o, ts, last }]
  let meta = ZV.lsGet('zv_meta', {}) || {};         // cid → { last, unread, call }
  const online = new Map(), typingMap = new Map();
  let view = { kind: 'friends', tab: 'online' };
  const saveFriends = () => ZV.lsSet('zv_friends', friends);
  const saveGroups = () => ZV.lsSet('zv_groups', groups);
  const saveProfiles = () => ZV.lsSet('zv_profiles', profiles);
  const saveOutbox = () => { if (!ZV.lsSet('zv_outbox', outbox)) { outbox = outbox.filter((it) => !it.o.img); ZV.lsSet('zv_outbox', outbox); } };
  const saveMeta = () => ZV.lsSet('zv_meta', meta);

  /* ============================================================
     Аккаунт (ключи)
     ============================================================ */
  async function uidOf(pub) {
    if (uidCache.has(pub)) return uidCache.get(pub);
    const u = (await ZV.sha(ZV.b64d(pub))).slice(0, 24); uidCache.set(pub, u); return u;
  }
  async function initIdentity() {
    let idn = ZV.lsGet('zv_identity', null);
    if (!idn || !idn.pub || !idn.priv) {
      const kp = await crypto.subtle.generateKey({ name: 'ECDH', namedCurve: 'P-256' }, true, ['deriveBits']);
      idn = { pub: ZV.b64u(new Uint8Array(await crypto.subtle.exportKey('raw', kp.publicKey))), priv: await crypto.subtle.exportKey('jwk', kp.privateKey) };
      ZV.lsSet('zv_identity', idn);
    }
    myPriv = await crypto.subtle.importKey('jwk', idn.priv, { name: 'ECDH', namedCurve: 'P-256' }, false, ['deriveBits']);
    myPub = idn.pub; myUid = await uidOf(myPub); inbox = 'zvonok/v2/u/' + myUid;
  }
  async function pairKey(pub) {
    let k = keyCache.get(pub); if (k) return k;
    const pk = await crypto.subtle.importKey('raw', ZV.b64d(pub), { name: 'ECDH', namedCurve: 'P-256' }, false, []);
    const bits = new Uint8Array(await crypto.subtle.deriveBits({ name: 'ECDH', public: pk }, myPriv, 256));
    const salt = enc.encode('|zvonok-dm-v1');
    const mat = new Uint8Array(bits.length + salt.length); mat.set(bits); mat.set(salt, bits.length);
    k = await crypto.subtle.importKey('raw', await crypto.subtle.digest('SHA-256', mat), 'AES-GCM', false, ['encrypt', 'decrypt']);
    keyCache.set(pub, k); return k;
  }
  const profileHash = async (p) => (await ZV.sha(JSON.stringify(ZV.cleanProfile(p)))).slice(0, 16);

  /* ============================================================
     Сеть
     ============================================================ */
  function initNet() {
    brokers = ZV.BROKERS.map((u) => new ZV.Mqtt(u, (t, p) => { rx = rx.then(() => onInbox(t, p)).catch(() => {}); }, netChanged));
    brokers.forEach((b) => b.subscribe(inbox));
  }
  function netChanged() {
    const n = brokers.filter((b) => b.ok).length;
    $('netInfo').textContent = n ? 'Сеть: ' + n + '/' + brokers.length : 'Нет сети — сообщения ждут отправки';
    $('netInfo').className = n ? '' : 'bad';
    if (n && !netOk) { netOk = true; announce(); for (const u of new Set(outbox.map((it) => it.uid))) flush(u, true); }
    if (!n) netOk = false;
  }
  async function sendRaw(uid, pub, o) {
    if (!pub) return false;
    try {
      const k = await pairKey(pub);
      const iv = crypto.getRandomValues(new Uint8Array(12));
      const ct = new Uint8Array(await crypto.subtle.encrypt({ name: 'AES-GCM', iv }, k, enc.encode(JSON.stringify(o))));
      const pr = ZV.b64d(myPub);
      const out = new Uint8Array(1 + pr.length + 12 + ct.length);
      out[0] = 1; out.set(pr, 1); out.set(iv, 1 + pr.length); out.set(ct, 1 + pr.length + 12);
      let ok = false; brokers.forEach((b) => { if (b.publish('zvonok/v2/u/' + uid, out)) ok = true; });
      return ok;
    } catch (e) { console.warn(e); return false; }
  }
  async function onInbox(t, p) {
    if (t !== inbox || p.length < 96 || p[0] !== 1) return;
    const pub = ZV.b64u(p.slice(1, 66));
    if (pub === myPub) return;
    let o;
    try { o = JSON.parse(dec.decode(await crypto.subtle.decrypt({ name: 'AES-GCM', iv: p.slice(66, 78) }, await pairKey(pub), p.slice(78)))); }
    catch (e) { return; }
    if (!o || typeof o.t !== 'string') return;
    await handle(await uidOf(pub), pub, o);
  }

  /* Надёжная отправка: лежит в очереди, пока получатель не подтвердит */
  function send(uid, pub, o) {
    o.mid = o.mid || ZV.rid();
    outbox.push({ uid, pub, o, ts: Date.now(), last: Date.now() });
    saveOutbox();
    sendRaw(uid, pub, o);
  }
  function flush(uid, force) {
    const now = Date.now();
    for (const it of outbox) if (it.uid === uid && (force || now - it.last > 3000)) { it.last = now; sendRaw(it.uid, it.pub, it.o); }
    saveOutbox();
  }
  setInterval(() => {
    const now = Date.now();
    for (const it of outbox) if (online.has(it.uid) && now - it.last > 15000) { it.last = now; sendRaw(it.uid, it.pub, it.o); }
    const n = outbox.length; outbox = outbox.filter((it) => now - it.ts < 30 * 86400000); if (n !== outbox.length) saveOutbox();
  }, 5000);
  function acked(uid, mid) {
    const it = outbox.find((x) => x.uid === uid && x.o.mid === mid);
    if (!it) return;
    outbox = outbox.filter((x) => x !== it); saveOutbox();
    const o = it.o, cid = o.t === 'dm' ? 'dm:' + uid : o.t === 'gmsg' ? 'g:' + o.gid : '';
    if (cid) { const m = loadConv(cid).find((x) => x.mid === mid); if (m && m.st !== 'ok') { m.st = 'ok'; saveConv(cid); repaintMsg(cid, m); } }
  }

  /* --- Присутствие --- */
  function contacts() {
    const m = new Map();
    for (const [u, f] of Object.entries(friends)) if (f.state === 'friend') m.set(u, f.pub);
    for (const g of Object.values(groups)) for (const [u, pub] of Object.entries(g.members)) if (u !== myUid) m.set(u, pub);
    return m;
  }
  function announce() { if (profile.status === 'invisible' || !netOk) return; for (const [u, pub] of contacts()) sendRaw(u, pub, { t: 'here', ph: myHash, ack: 0 }); }
  setInterval(() => { if (netOk && profile.status !== 'invisible') for (const [u, pub] of contacts()) sendRaw(u, pub, { t: 'here', ph: myHash, ack: 1 }); }, 45000);
  setInterval(() => { const now = Date.now(); let ch = false; for (const [u, ts] of online) if (now - ts > 110000) { online.delete(u); ch = true; } if (ch) renderAll(); }, 10000);
  window.addEventListener('beforeunload', () => { if (netOk) for (const [u, pub] of contacts()) sendRaw(u, pub, { t: 'off' }); });

  const isFriend = (uid) => !!friends[uid] && friends[uid].state === 'friend';
  const inGroupWith = (uid) => Object.values(groups).some((g) => g.members[uid]);
  const str = (s, n) => String(s == null ? '' : s).slice(0, n);
  const validImg = (s) => typeof s === 'string' && /^data:image\/(jpeg|png|webp|gif);base64,/.test(s) && s.length <= MAX_IMG * 1.1 ? s : '';
  function nameOf(uid) { if (uid === myUid) return profile.name; return (profiles[uid] && profiles[uid].p.name) || 'Пользователь ' + uid.slice(0, 4); }
  function profOf(uid) { if (uid === myUid) return ZV.publicProfile(); return (profiles[uid] && profiles[uid].p) || ZV.cleanProfile({ name: nameOf(uid) }); }
  function presence(uid) { if (uid === myUid) return profile.status === 'invisible' ? 'off' : profile.status; return online.has(uid) ? profOf(uid).status : 'off'; }
  async function storeProfile(uid, p, ph) {
    if (!p) return;
    const c = ZV.cleanProfile(p);
    profiles[uid] = { p: c, ph: ph || await profileHash(c) }; saveProfiles();
  }

  /* ============================================================
     Входящие
     ============================================================ */
  async function handle(uid, pub, o) {
    const ack = () => { if (o.mid) sendRaw(uid, pub, { t: 'ack', mid: o.mid }); };
    switch (o.t) {
      case 'ack': acked(uid, str(o.mid, 40)); return;
      case 'freq': {
        ack();
        const f = friends[uid];
        await storeProfile(uid, o.profile);
        if (f && f.state === 'friend') { send(uid, pub, { t: 'facc', profile: ZV.publicProfile() }); return; }
        if (f && f.state === 'outgoing') { friends[uid] = { pub, state: 'friend', ts: Date.now() }; saveFriends(); send(uid, pub, { t: 'facc', profile: ZV.publicProfile() }); ZV.toast(nameOf(uid) + ' теперь ваш друг'); renderAll(); return; }
        if (f) return;
        friends[uid] = { pub, state: 'incoming', ts: Date.now() }; saveFriends();
        ZV.beep(700); ZV.toast(nameOf(uid) + ' хочет добавить вас в друзья'); notifyDesk('Заявка в друзья', nameOf(uid));
        renderAll(); return;
      }
      case 'facc': {
        ack();
        const f = friends[uid];
        if (f && (f.state === 'outgoing' || f.state === 'friend')) {
          await storeProfile(uid, o.profile);
          const was = f.state; friends[uid] = { pub, state: 'friend', ts: Date.now() }; saveFriends();
          if (was !== 'friend') { ZV.toast(nameOf(uid) + ' принял(а) заявку'); ZV.beep(700); }
          sendRaw(uid, pub, { t: 'here', ph: myHash, ack: 0 });
          renderAll();
        }
        return;
      }
      case 'fdel': ack(); if (friends[uid]) { delete friends[uid]; saveFriends(); if (!inGroupWith(uid)) online.delete(uid); renderAll(); } return;
      case 'ginfo': ack(); await applyGinfo(uid, o); return;
    }
    const fr = isFriend(uid), grp = inGroupWith(uid);
    if (!fr && !grp) return; // остальное — только от друзей и участников общих бесед
    switch (o.t) {
      case 'here': {
        const wasOn = online.has(uid); online.set(uid, Date.now());
        if (!o.ack && profile.status !== 'invisible') sendRaw(uid, pub, { t: 'here', ph: myHash, ack: 1 });
        if (o.ph && (!profiles[uid] || profiles[uid].ph !== o.ph)) sendRaw(uid, pub, { t: 'getprof' });
        flush(uid);
        if (!wasOn) renderAll();
        return;
      }
      case 'off': online.delete(uid); renderAll(); return;
      case 'getprof': sendRaw(uid, pub, { t: 'prof', profile: ZV.publicProfile(), ph: myHash }); return;
      case 'prof': await storeProfile(uid, o.profile, str(o.ph, 16)); renderAll(); if (view.kind === 'conv') renderConv(); return;
      case 'dm':
        ack(); if (!fr) return;
        addMsg('dm:' + uid, { mid: str(o.mid, 40), from: uid, text: str(o.text, 4000), img: validImg(o.img), ts: Number(o.ts) || Date.now(), call: !!o.call });
        return;
      case 'gmsg': {
        ack(); const g = groups[o.gid]; if (!g || !g.members[uid]) return;
        addMsg('g:' + g.gid, { mid: str(o.mid, 40), from: uid, text: str(o.text, 4000), img: validImg(o.img), ts: Number(o.ts) || Date.now(), call: !!o.call });
        return;
      }
      case 'gleave': {
        ack(); const g = groups[o.gid];
        if (g && g.members[uid]) { delete g.members[uid]; saveGroups(); addSys('g:' + g.gid, nameOf(uid) + ' покинул(а) беседу'); renderAll(); }
        return;
      }
      case 'edit': case 'del': case 'react': {
        ack();
        const cid = o.gid ? 'g:' + o.gid : 'dm:' + uid;
        if (o.gid ? !(groups[o.gid] && groups[o.gid].members[uid]) : !fr) return;
        const c = loadConv(cid), m = c.find((x) => x.mid === o.target);
        if (!m) return;
        if (o.t === 'react') { setReact(m, str(o.emoji, 8), uid, !!o.on); }
        else if (m.from !== uid) return;
        else if (o.t === 'edit') { m.text = str(o.text, 4000); m.edited = true; }
        else { c.splice(c.indexOf(m), 1); }
        saveConv(cid);
        if (view.cid === cid) { if (o.t === 'del') renderConv(); else repaintMsg(cid, m); }
        return;
      }
      case 'typing': {
        const cid = o.gid ? 'g:' + o.gid : 'dm:' + uid;
        typingMap.set(cid + '|' + uid, Date.now()); paintTyping();
        return;
      }
      case 'call': ring(uid, o); return;
    }
  }

  /* ============================================================
     Беседы
     ============================================================ */
  async function applyGinfo(from, o) {
    const gid = str(o.gid, 16); if (!/^[a-f0-9]{16}$/.test(gid) || !Array.isArray(o.members)) return;
    const mem = {};
    for (const m of o.members.slice(0, MAX_GROUP)) { if (!m || typeof m.pub !== 'string') continue; try { mem[await uidOf(m.pub)] = m.pub; } catch (e) {} }
    const g = groups[gid];
    if (!mem[from] && !(g && g.members[from])) return;
    if (!g && !isFriend(from)) return; // в новую беседу добавить может только друг
    if (g && !g.members[from]) return;
    if (g && (o.v | 0) <= (g.v | 0)) return;
    if (!mem[myUid]) {
      if (g) { delete groups[gid]; saveGroups(); ZV.toast('Вас удалили из беседы «' + g.name + '»'); if (view.cid === 'g:' + gid) showFriends(); renderAll(); }
      return;
    }
    const isNew = !g;
    groups[gid] = { gid, name: str(o.name, 60) || 'Беседа', owner: str(o.owner, 24) || from, v: o.v | 0, members: mem, created: g ? g.created : Date.now() };
    saveGroups();
    if (isNew) { meta['g:' + gid] = Object.assign(meta['g:' + gid] || {}, { last: Date.now() }); saveMeta(); addSys('g:' + gid, nameOf(from) + ' добавил(а) вас в беседу'); ZV.beep(700); }
    for (const [u, pub] of Object.entries(mem)) if (u !== myUid && !online.has(u)) sendRaw(u, pub, { t: 'here', ph: myHash, ack: 0 });
    renderAll(); if (view.cid === 'g:' + gid) renderConv();
  }
  function pushGinfo(g, extraTo) {
    g.v = (g.v | 0) + 1; saveGroups();
    const o = { t: 'ginfo', gid: g.gid, name: g.name, owner: g.owner, v: g.v, members: Object.values(g.members).map((pub) => ({ pub })) };
    for (const [u, pub] of Object.entries(g.members)) if (u !== myUid) send(u, pub, Object.assign({}, o));
    if (extraTo) send(extraTo[0], extraTo[1], Object.assign({}, o));
  }

  /* ============================================================
     Переписка (хранится в браузере)
     ============================================================ */
  const convCache = new Map();
  function loadConv(cid) { if (!convCache.has(cid)) convCache.set(cid, ZV.lsGet('zv_c_' + cid, []) || []); return convCache.get(cid); }
  function saveConv(cid) {
    const c = loadConv(cid); while (c.length > 400) c.shift();
    if (ZV.lsSet('zv_c_' + cid, c)) return;
    for (const m of c) if (m.img) { m.img = ''; m.imgGone = true; if (ZV.lsSet('zv_c_' + cid, c)) { ZV.toast('Мало места: старые картинки удалены из истории'); return; } }
  }
  function addMsg(cid, m) {
    const c = loadConv(cid);
    if (c.some((x) => x.mid === m.mid)) return;
    c.push(m); saveConv(cid);
    meta[cid] = meta[cid] || {}; meta[cid].last = Date.now();
    const looking = view.kind === 'conv' && view.cid === cid && !document.hidden;
    if (m.from !== myUid && !m.sys) {
      typingMap.delete(cid + '|' + m.from);
      if (!looking) {
        meta[cid].unread = (meta[cid].unread | 0) + 1;
        if (profile.status !== 'dnd') { ZV.beep(1000, 0.12); notifyDesk(cid.startsWith('g:') ? convTitle(cid) + ' · ' + nameOf(m.from) : nameOf(m.from), m.text || (m.img ? '🖼 Картинка' : '')); }
      }
    }
    saveMeta();
    if (view.kind === 'conv' && view.cid === cid) { appendMsgEl(cid, m, c[c.length - 2]); paintTyping(); }
    renderSidebar(); updateTitle();
  }
  function addSys(cid, text) { addMsg(cid, { mid: ZV.rid(), from: '', sys: true, text, ts: Date.now() }); }
  function recipients(cid) {
    if (cid.startsWith('dm:')) { const u = cid.slice(3); return isFriend(u) ? [[u, friends[u].pub]] : []; }
    const g = groups[cid.slice(2)]; return g ? Object.entries(g.members).filter(([u]) => u !== myUid) : [];
  }
  function convTitle(cid) { if (cid.startsWith('dm:')) return nameOf(cid.slice(3)); const g = groups[cid.slice(2)]; return g ? g.name : 'Беседа'; }
  function wire(cid, o) { if (cid.startsWith('g:')) o.gid = cid.slice(2); return o; }
  function postMsg(cid, extra) {
    const targets = recipients(cid);
    if (!targets.length) { ZV.toast(cid.startsWith('dm:') ? 'Этот человек больше не у вас в друзьях' : 'В беседе никого нет'); return null; }
    const m = Object.assign({ mid: ZV.rid(), from: myUid, ts: Date.now(), st: 'wait' }, extra);
    addMsg(cid, m);
    for (const [u, pub] of targets) send(u, pub, wire(cid, { t: cid.startsWith('dm:') ? 'dm' : 'gmsg', mid: m.mid, text: m.text || '', img: m.img || '', ts: m.ts, call: !!m.call }));
    return m;
  }
  function sendCtl(cid, o) { for (const [u, pub] of recipients(cid)) send(u, pub, wire(cid, Object.assign({}, o, { mid: ZV.rid() }))); }
  function setReact(m, emo, who, on) {
    if (!ZV.EMOJI.includes(emo)) return;
    m.reacts = m.reacts || {};
    const set = new Set(m.reacts[emo] || []); if (on) set.add(who); else set.delete(who);
    if (set.size) m.reacts[emo] = [...set]; else delete m.reacts[emo];
  }

  /* ============================================================
     Звонки друзьям и в беседах
     ============================================================ */
  function startCall(cid) {
    if (window.Call.active()) return;
    const targets = recipients(cid);
    if (!targets.length) { ZV.toast('Некому звонить'); return; }
    const code = ZV.randomCode(), pass = ZV.rid(), title = convTitle(cid);
    for (const [u, pub] of targets) sendRaw(u, pub, wire(cid, { t: 'call', code, pass, title: cid.startsWith('g:') ? title : '' }));
    postMsg(cid, { text: '📞 Звонок', call: true });
    meta[cid].call = { code, pass, ts: Date.now(), from: myUid }; saveMeta();
    window.Call.start(true, code, pass, { uid: myUid, autoAdmit: targets.map((t) => t[0]), title });
  }
  let ringTimer = 0, ringTone = 0, ringing = null;
  function ring(uid, o) {
    const cid = o.gid ? 'g:' + o.gid : 'dm:' + uid;
    if (o.gid ? !groups[o.gid] : !isFriend(uid)) return;
    meta[cid] = meta[cid] || {}; meta[cid].call = { code: str(o.code, 20), pass: str(o.pass, 40), ts: Date.now(), from: uid }; saveMeta();
    if (view.cid === cid) renderConv();
    if (window.Call.active()) { ZV.toast(nameOf(uid) + ' звонит'); return; }
    if (profile.status === 'dnd') { renderAll(); return; }
    ringing = cid;
    $('ringAva').innerHTML = ''; $('ringAva').appendChild(ZV.avatar(profOf(uid), 96));
    $('ringName').textContent = nameOf(uid);
    $('ringText').textContent = o.gid ? 'звонит в беседе «' + convTitle(cid) + '»' : 'звонит вам';
    $('ring').hidden = false;
    notifyDesk('📞 ' + nameOf(uid), $('ringText').textContent);
    clearInterval(ringTone); clearTimeout(ringTimer);
    const tone = () => { ZV.beep(660, 0.25); setTimeout(() => ZV.beep(880, 0.25), 300); };
    tone(); ringTone = setInterval(tone, 1800);
    ringTimer = setTimeout(stopRing, 40000);
  }
  function stopRing() { clearInterval(ringTone); clearTimeout(ringTimer); $('ring').hidden = true; ringing = null; }
  function joinCall(cid) {
    const c = meta[cid] && meta[cid].call; if (!c) return;
    stopRing();
    window.Call.start(c.from === myUid, c.code, c.pass, { uid: myUid, title: convTitle(cid), autoAdmit: recipients(cid).map((t) => t[0]) });
  }
  $('ringYes').onclick = () => { const cid = ringing; if (cid) joinCall(cid); };
  $('ringNo').onclick = stopRing;

  /* ============================================================
     Уведомления на рабочий стол
     ============================================================ */
  function notifyDesk(title, body) {
    if (!document.hidden || !('Notification' in window) || Notification.permission !== 'granted') return;
    try { const n = new Notification(title, { body: String(body || '').slice(0, 120), silent: true }); n.onclick = () => { window.focus(); n.close(); }; } catch (e) {}
  }
  function updateTitle() {
    const n = Object.values(meta).reduce((s, m) => s + (m.unread | 0), 0) + Object.values(friends).filter((f) => f.state === 'incoming').length;
    document.title = (n ? '(' + n + ') ' : '') + 'Звонок';
  }

  /* ============================================================
     Отрисовка: боковая панель
     ============================================================ */
  function renderAll() { renderSidebar(); renderMain(); paintMyPanel(); updateTitle(); }
  function groupIcon(g, size) {
    const p = { name: g.name }; const a = ZV.avatar(p, size); a.classList.add('grp'); return a;
  }
  function renderSidebar() {
    const box = $('convList'); box.innerHTML = '';
    const cids = new Set(Object.keys(groups).map((g) => 'g:' + g));
    for (const cid of Object.keys(meta)) if (cid.startsWith('dm:') && friends[cid.slice(3)]) cids.add(cid);
    const list = [...cids].sort((a, b) => ((meta[b] || {}).last | 0) - ((meta[a] || {}).last | 0));
    if (!list.length) { const e = document.createElement('div'); e.className = 'hint pad'; e.textContent = 'Здесь появятся личные сообщения и беседы.'; box.appendChild(e); }
    for (const cid of list) {
      const row = document.createElement('div'); row.className = 'conv' + (view.cid === cid ? ' active' : '');
      let ava, sub = '';
      if (cid.startsWith('dm:')) { const u = cid.slice(3); ava = ZV.avatar(profOf(u), 34, { dot: true, status: presence(u) }); sub = profOf(u).custom; }
      else { const g = groups[cid.slice(2)]; ava = groupIcon(g, 34); sub = Object.keys(g.members).length + ' участн.'; }
      const nm = document.createElement('div'); nm.className = 'nm';
      const t = document.createElement('div'); t.textContent = convTitle(cid);
      const s = document.createElement('small'); s.textContent = sub || '';
      nm.append(t, s);
      row.append(ava, nm);
      const c = meta[cid] && meta[cid].call;
      if (c && Date.now() - c.ts < CALL_FRESH) { const ci = document.createElement('span'); ci.textContent = '📞'; ci.title = 'Был звонок'; ci.className = 'muted'; row.appendChild(ci); }
      const un = (meta[cid] || {}).unread | 0;
      if (un) { const b = document.createElement('span'); b.className = 'badge'; b.textContent = un > 99 ? '99+' : un; row.appendChild(b); }
      row.onclick = () => openConv(cid);
      box.appendChild(row);
    }
    const inc = Object.values(friends).filter((f) => f.state === 'incoming').length;
    $('friendsBadge').hidden = !inc; $('friendsBadge').textContent = inc;
    $('navFriends').classList.toggle('active', view.kind === 'friends');
    $('navRooms').classList.toggle('active', view.kind === 'rooms');
  }
  function paintMyPanel() {
    $('myAva').innerHTML = ''; $('myAva').appendChild(ZV.avatar(profile, 34, { dot: true, status: presence(myUid) }));
    $('myName').textContent = profile.name;
    $('myStatus').textContent = profile.custom || ZV.STATUS[profile.status];
  }

  /* ============================================================
     Главная область
     ============================================================ */
  function renderMain() {
    $('friendsView').hidden = view.kind !== 'friends';
    $('convView').hidden = view.kind !== 'conv';
    $('roomsView').hidden = view.kind !== 'rooms';
    if (view.kind === 'friends') renderFriends();
    else if (view.kind === 'conv') renderConvHead();
  }
  function closeNavOnMobile() { if (matchMedia('(max-width:800px)').matches) $('nav').classList.remove('open'); }
  function showFriends(tab) { view = { kind: 'friends', tab: tab || view.tab || 'online' }; renderAll(); closeNavOnMobile(); }
  function showRooms() { view = { kind: 'rooms' }; renderAll(); closeNavOnMobile(); }
  $('navFriends').onclick = () => showFriends();
  $('navRooms').onclick = showRooms;
  document.querySelectorAll('.burger').forEach((b) => { b.onclick = () => $('nav').classList.toggle('open'); });

  /* --- Друзья --- */
  document.querySelectorAll('#friendTabs [data-ft]').forEach((b) => { b.onclick = () => showFriends(b.dataset.ft); });
  function friendRow(uid, f) {
    const row = document.createElement('div'); row.className = 'frow';
    const p = profOf(uid);
    const ava = ZV.avatar(p, 40, { dot: f.state === 'friend', status: presence(uid) });
    const nm = document.createElement('div'); nm.className = 'nm';
    const t = document.createElement('div'); t.textContent = p.name + (p.username ? '  @' + p.username : '');
    const s = document.createElement('small');
    s.textContent = f.state === 'incoming' ? 'Входящая заявка' : f.state === 'outgoing' ? 'Заявка отправлена — ждём, когда он(а) будет в сети' : (presence(uid) === 'off' ? 'Не в сети' : (p.custom || ZV.STATUS[presence(uid)]));
    nm.append(t, s);
    row.append(ava, nm);
    const btn = (txt, title, fn, cls) => { const b = document.createElement('button'); b.textContent = txt; b.title = title; if (cls) b.className = cls; b.onclick = (e) => { e.stopPropagation(); fn(); }; row.appendChild(b); };
    if (f.state === 'friend') {
      btn('💬', 'Написать', () => openConv('dm:' + uid));
      btn('📞', 'Позвонить', () => { openConv('dm:' + uid); startCall('dm:' + uid); });
    } else if (f.state === 'incoming') {
      btn('✓ Принять', 'Принять заявку', () => acceptFriend(uid), 'primary');
      btn('✕', 'Отклонить', () => { delete friends[uid]; saveFriends(); renderAll(); });
    } else {
      btn('Отменить', 'Отменить заявку', () => { send(uid, f.pub, { t: 'fdel' }); delete friends[uid]; saveFriends(); renderAll(); });
    }
    row.onclick = () => openUserCard(uid);
    return row;
  }
  function renderFriends() {
    const tab = view.tab;
    document.querySelectorAll('#friendTabs [data-ft]').forEach((b) => b.classList.toggle('active', b.dataset.ft === tab));
    const inc = Object.values(friends).filter((f) => f.state === 'incoming').length;
    $('pendBadge').hidden = !inc; $('pendBadge').textContent = inc;
    $('friendAdd').hidden = tab !== 'add';
    const box = $('friendList'); box.innerHTML = ''; box.hidden = tab === 'add';
    if (tab === 'add') { paintMyCode(); return; }
    let items = Object.entries(friends);
    if (tab === 'online') items = items.filter(([u, f]) => f.state === 'friend' && online.has(u));
    else if (tab === 'all') items = items.filter(([, f]) => f.state === 'friend');
    else items = items.filter(([, f]) => f.state !== 'friend');
    items.sort((a, b) => nameOf(a[0]).localeCompare(nameOf(b[0])));
    const h = document.createElement('div'); h.className = 'sect';
    h.textContent = (tab === 'online' ? 'В сети' : tab === 'all' ? 'Все друзья' : 'Ожидание') + ' — ' + items.length;
    box.appendChild(h);
    if (!items.length) {
      const e = document.createElement('div'); e.className = 'empty';
      e.textContent = tab === 'pending' ? 'Заявок нет.' : Object.values(friends).some((f) => f.state === 'friend') ? 'Сейчас никого нет в сети.' : 'Друзей пока нет. Откройте «Добавить в друзья» и отправьте свой код другу.';
      box.appendChild(e);
    }
    for (const [u, f] of items) box.appendChild(friendRow(u, f));
  }
  function acceptFriend(uid) {
    const f = friends[uid]; if (!f) return;
    f.state = 'friend'; f.ts = Date.now(); saveFriends();
    send(uid, f.pub, { t: 'facc', profile: ZV.publicProfile() });
    sendRaw(uid, f.pub, { t: 'here', ph: myHash, ack: 0 });
    renderAll();
  }
  function friendLink() { return (/^https?:$/.test(location.protocol) ? location.origin + location.pathname : '') + '?add=' + myPub; }
  function paintMyCode() { $('myCode').value = myPub; }
  function copy(text, btn) {
    const done = () => { if (btn) { const o = btn.textContent; btn.textContent = '✓ Скопировано'; setTimeout(() => { btn.textContent = o; }, 1500); } };
    if (navigator.clipboard) navigator.clipboard.writeText(text).then(done, () => ZV.toast(text)); else ZV.toast(text);
  }
  $('btnCopyCode').onclick = (e) => copy(myPub, e.target);
  $('btnCopyLink').onclick = (e) => copy(friendLink(), e.target);
  $('addForm').onsubmit = async (e) => {
    e.preventDefault();
    let code = $('addCode').value.trim();
    const m = code.match(/[?&]add=([A-Za-z0-9_-]+)/); if (m) code = m[1];
    let raw; try { raw = ZV.b64d(code); } catch (err) { raw = null; }
    if (!raw || raw.length !== 65 || raw[0] !== 4) { $('addMsg').textContent = 'Это не похоже на код друга. Попросите скопировать его целиком.'; $('addMsg').className = 'err'; return; }
    try { await crypto.subtle.importKey('raw', raw, { name: 'ECDH', namedCurve: 'P-256' }, false, []); } catch (err) { $('addMsg').textContent = 'Код повреждён.'; $('addMsg').className = 'err'; return; }
    const pub = ZV.b64u(raw);
    if (pub === myPub) { $('addMsg').textContent = 'Это ваш собственный код 🙂'; $('addMsg').className = 'err'; return; }
    const uid = await uidOf(pub);
    const f = friends[uid];
    if (f && f.state === 'friend') { $('addMsg').textContent = 'Вы уже друзья.'; $('addMsg').className = ''; return; }
    if (f && f.state === 'incoming') { acceptFriend(uid); $('addMsg').textContent = 'Заявка принята!'; $('addMsg').className = 'ok'; return; }
    friends[uid] = { pub, state: 'outgoing', ts: Date.now() }; saveFriends();
    send(uid, pub, { t: 'freq', profile: ZV.publicProfile() });
    $('addCode').value = '';
    $('addMsg').textContent = 'Заявка отправлена. Она дойдёт, когда друг будет в сети.'; $('addMsg').className = 'ok';
    renderSidebar();
  };

  /* --- Переписка --- */
  let editing = null;
  function openConv(cid) {
    view = { kind: 'conv', cid };
    meta[cid] = meta[cid] || { last: Date.now() };
    meta[cid].unread = 0; saveMeta();
    editing = null; $('editBar').hidden = true;
    renderAll(); renderConv(); closeNavOnMobile();
    setTimeout(() => $('msgInput').focus(), 0);
  }
  function renderConvHead() {
    const cid = view.cid; const head = $('convHead'); head.innerHTML = '';
    let ava, sub = '';
    if (cid.startsWith('dm:')) {
      const u = cid.slice(3); ava = ZV.avatar(profOf(u), 32, { dot: true, status: presence(u) });
      sub = presence(u) === 'off' ? 'не в сети — сообщения дойдут, когда появится' : (profOf(u).custom || ZV.STATUS[presence(u)]);
      ava.onclick = () => openUserCard(u); ava.style.cursor = 'pointer';
    } else {
      const g = groups[cid.slice(2)]; if (!g) { showFriends(); return; }
      ava = groupIcon(g, 32);
      const on = Object.keys(g.members).filter((u) => u === myUid || online.has(u)).length;
      sub = Object.keys(g.members).length + ' участников, в сети ' + on;
    }
    const nm = document.createElement('div'); nm.className = 'nm';
    const t = document.createElement('b'); t.textContent = convTitle(cid);
    const s = document.createElement('small'); s.textContent = sub;
    nm.append(t, s);
    head.append(ava, nm);
    const b = (txt, title, fn) => { const x = document.createElement('button'); x.textContent = txt; x.title = title; x.onclick = fn; head.appendChild(x); };
    b('📞', 'Позвонить', () => startCall(cid));
    if (cid.startsWith('g:')) b('👥', 'Участники и настройки беседы', () => openGroupModal(cid.slice(2)));
    // Идущий звонок
    const c = meta[cid] && meta[cid].call;
    const fresh = c && Date.now() - c.ts < CALL_FRESH;
    $('callBanner').hidden = !fresh;
    if (fresh) $('callBannerText').textContent = (c.from === myUid ? 'Вы начали звонок' : nameOf(c.from) + ' начал(а) звонок') + ' в ' + ZV.fmtTime(c.ts);
  }
  $('callJoin').onclick = () => { if (view.cid) joinCall(view.cid); };
  function renderConv() {
    if (view.kind !== 'conv') return;
    renderConvHead();
    const box = $('cmsgs'); box.innerHTML = '';
    const c = loadConv(view.cid);
    if (!c.length) { const e = document.createElement('div'); e.className = 'empty'; e.textContent = 'Начало переписки с «' + convTitle(view.cid) + '». Сообщения хранятся только на ваших устройствах.'; box.appendChild(e); }
    let prev = null;
    for (const m of c) { box.appendChild(msgEl(view.cid, m, prev)); prev = m; }
    box.scrollTop = 1e9;
    paintTyping();
  }
  function appendMsgEl(cid, m, prev) {
    const box = $('cmsgs'); const e = box.querySelector('.empty'); if (e) e.remove();
    const near = box.scrollHeight - box.scrollTop - box.clientHeight < 150;
    box.appendChild(msgEl(cid, m, prev));
    if (near || m.from === myUid) box.scrollTop = 1e9;
  }
  function repaintMsg(cid, m) { if (view.cid !== cid) return; const el = document.querySelector('#cmsgs [data-mid="' + m.mid + '"]'); if (!el) return; const c = loadConv(cid); el.replaceWith(msgEl(cid, m, c[c.indexOf(m) - 1])); }
  function msgEl(cid, m, prev) {
    const el = document.createElement('div'); el.dataset.mid = m.mid;
    if (m.sys) { el.className = 'sys'; el.textContent = m.text + ' · ' + ZV.fmtTime(m.ts); return el; }
    const cont = prev && !prev.sys && prev.from === m.from && m.ts - prev.ts < 5 * 60000;
    el.className = 'cm' + (cont ? ' cont' : '');
    const body = document.createElement('div'); body.className = 'body';
    if (!cont) {
      const who = document.createElement('div'); who.className = 'who';
      const n = document.createElement('span'); n.textContent = nameOf(m.from); n.className = 'link'; n.onclick = () => openUserCard(m.from);
      const tm = document.createElement('time'); tm.textContent = ZV.fmtTime(m.ts);
      who.append(n, tm); body.appendChild(who);
    }
    if (m.text) {
      const tx = document.createElement('div'); tx.className = 'txt' + (m.call ? ' callmsg' : '');
      ZV.linkify(tx, m.text);
      if (m.edited) { const ed = document.createElement('span'); ed.className = 'muted small'; ed.textContent = ' (изменено)'; tx.appendChild(ed); }
      body.appendChild(tx);
    }
    if (m.img) { const img = document.createElement('img'); img.className = 'att'; img.src = m.img; img.alt = ''; img.onclick = () => ZV.lightbox(m.img); body.appendChild(img); }
    if (m.imgGone) { const g = document.createElement('div'); g.className = 'muted small'; g.textContent = '🖼 картинка удалена из истории (мало места)'; body.appendChild(g); }
    if (m.reacts && Object.keys(m.reacts).length) {
      const rr = document.createElement('div'); rr.className = 'reacts';
      for (const [emo, list] of Object.entries(m.reacts)) {
        const b = document.createElement('button'); b.textContent = emo + ' ' + list.length; b.title = list.map(nameOf).join(', ');
        if (list.includes(myUid)) b.className = 'mine';
        b.onclick = () => reactTo(cid, m, emo); rr.appendChild(b);
      }
      body.appendChild(rr);
    }
    if (m.from === myUid && m.st === 'wait') { const w = document.createElement('div'); w.className = 'muted small'; w.textContent = '⏳ ждёт, когда получатель будет в сети'; body.appendChild(w); }
    const tools = document.createElement('div'); tools.className = 'tools';
    const tb = (txt, title, fn) => { const b = document.createElement('button'); b.textContent = txt; b.title = title; b.onclick = (e) => { e.stopPropagation(); fn(e); }; tools.appendChild(b); };
    tb('☺+', 'Реакция', () => ZV.showPicker(el, (emo) => reactTo(cid, m, emo)));
    if (m.from === myUid && !m.call) {
      if (m.text) tb('✎', 'Изменить', () => { editing = m; $('msgInput').value = m.text; $('editBar').hidden = false; $('msgInput').focus(); });
      tb('🗑', 'Удалить у всех', () => {
        if (!confirm('Удалить сообщение у всех?')) return;
        const c = loadConv(cid); c.splice(c.indexOf(m), 1); saveConv(cid);
        sendCtl(cid, { t: 'del', target: m.mid }); renderConv();
      });
    }
    el.append(cont ? document.createElement('span') : ZV.avatar(profOf(m.from), 40), body, tools);
    if (cont) el.firstChild.className = 'gutter';
    else { el.firstChild.style.cursor = 'pointer'; el.firstChild.onclick = () => openUserCard(m.from); }
    return el;
  }
  function reactTo(cid, m, emo) {
    const on = !(m.reacts && m.reacts[emo] && m.reacts[emo].includes(myUid));
    setReact(m, emo, myUid, on); saveConv(cid); repaintMsg(cid, m);
    sendCtl(cid, { t: 'react', target: m.mid, emoji: emo, on });
  }
  $('editCancel').onclick = () => { editing = null; $('editBar').hidden = true; $('msgInput').value = ''; };
  $('msgForm').onsubmit = (e) => {
    e.preventDefault();
    const cid = view.cid; const text = $('msgInput').value.trim();
    if (!cid || !text) return;
    if (editing) {
      const m = editing; editing = null; $('editBar').hidden = true;
      if (text !== m.text) { m.text = text.slice(0, 4000); m.edited = true; saveConv(cid); repaintMsg(cid, m); sendCtl(cid, { t: 'edit', target: m.mid, text: m.text }); }
    } else postMsg(cid, { text: text.slice(0, 4000) });
    $('msgInput').value = '';
  };
  $('msgInput').addEventListener('keydown', (e) => { if (e.key === 'Escape' && editing) $('editCancel').click(); });
  let lastTyping = 0;
  $('msgInput').addEventListener('input', () => {
    const now = Date.now(); if (now - lastTyping < 3000 || !view.cid || editing) return; lastTyping = now;
    for (const [u, pub] of recipients(view.cid)) if (online.has(u)) sendRaw(u, pub, wire(view.cid, { t: 'typing' }));
  });
  function paintTyping() {
    if (view.kind !== 'conv') return;
    const now = Date.now(), names = [];
    for (const [k, ts] of typingMap) { if (now - ts > 6000) { typingMap.delete(k); continue; } const [cid, u] = k.split('|'); if (cid === view.cid) names.push(nameOf(u)); }
    $('typing').textContent = names.length ? names.join(', ') + (names.length > 1 ? ' печатают…' : ' печатает…') : '';
  }
  setInterval(paintTyping, 2000);
  async function sendImages(files) {
    const cid = view.cid; if (!cid) return;
    for (const f of files) {
      if (!/^image\//.test(f.type)) { ZV.toast('В личке можно отправлять картинки. Файлы — в звонке.'); continue; }
      try { const url = await ZV.compressImage(f, 1280, 1280, 0.8, MAX_IMG); if (!url) { ZV.toast('Картинка слишком большая'); continue; } postMsg(cid, { img: url, text: '' }); }
      catch (err) { ZV.toast('Не удалось открыть картинку'); }
    }
  }
  $('btnImg').onclick = () => $('imgInput').click();
  $('imgInput').onchange = (e) => { sendImages([...e.target.files]); e.target.value = ''; };
  $('msgInput').addEventListener('paste', (e) => { const fs = [...((e.clipboardData && e.clipboardData.files) || [])]; if (fs.length) { e.preventDefault(); sendImages(fs); } });
  $('convView').addEventListener('dragover', (e) => e.preventDefault());
  $('convView').addEventListener('drop', (e) => { e.preventDefault(); sendImages([...e.dataTransfer.files]); });
  document.addEventListener('visibilitychange', () => { if (!document.hidden && view.kind === 'conv' && meta[view.cid] && meta[view.cid].unread) { meta[view.cid].unread = 0; saveMeta(); renderSidebar(); updateTitle(); } });

  /* ============================================================
     Карточка пользователя
     ============================================================ */
  function openUserCard(uid) {
    if (!uid) return;
    const box = $('ucBody'); box.innerHTML = '';
    const st = presence(uid);
    box.appendChild(ZV.profileCard(profOf(uid), { status: st, extra: uid === myUid ? 'Это вы' : ZV.STATUS[st] }));
    const acts = $('ucActs'); acts.innerHTML = '';
    const b = (txt, fn, cls) => { const x = document.createElement('button'); x.textContent = txt; if (cls) x.className = cls; x.onclick = () => { $('ucard').hidden = true; fn(); }; acts.appendChild(x); };
    if (uid === myUid) b('Редактировать профиль', () => ZV.openSettings('profile'), 'primary');
    else if (isFriend(uid)) {
      b('💬 Написать', () => openConv('dm:' + uid), 'primary');
      b('📞 Позвонить', () => { openConv('dm:' + uid); startCall('dm:' + uid); });
      b('Удалить из друзей', () => { if (!confirm('Удалить ' + nameOf(uid) + ' из друзей?')) return; send(uid, friends[uid].pub, { t: 'fdel' }); delete friends[uid]; saveFriends(); if (view.cid === 'dm:' + uid) showFriends(); renderAll(); }, 'off');
    } else if (friends[uid] && friends[uid].state === 'incoming') b('✓ Принять заявку', () => acceptFriend(uid), 'primary');
    else if (!friends[uid]) {
      const pub = contacts().get(uid);
      if (pub) b('Добавить в друзья', () => { friends[uid] = { pub, state: 'outgoing', ts: Date.now() }; saveFriends(); send(uid, pub, { t: 'freq', profile: ZV.publicProfile() }); ZV.toast('Заявка отправлена'); renderAll(); }, 'primary');
    }
    $('ucard').hidden = false;
  }
  $('ucard').onclick = (e) => { if (e.target === $('ucard')) $('ucard').hidden = true; };
  $('ucClose').onclick = () => { $('ucard').hidden = true; };
  $('myPanelUser').onclick = () => openUserCard(myUid);

  /* ============================================================
     Создание беседы и её настройки
     ============================================================ */
  let pickMode = null;
  function openPicker(mode, gid) {
    pickMode = { mode, gid };
    $('pickTitle').textContent = mode === 'new' ? 'Создать беседу' : 'Добавить друзей в беседу';
    $('pickName').hidden = mode !== 'new'; $('pickName').value = '';
    $('pickOk').textContent = mode === 'new' ? 'Создать' : 'Добавить';
    const box = $('pickList'); box.innerHTML = '';
    const g = gid && groups[gid];
    const list = Object.entries(friends).filter(([u, f]) => f.state === 'friend' && !(g && g.members[u]));
    if (!list.length) { const e = document.createElement('div'); e.className = 'empty'; e.textContent = mode === 'new' ? 'Сначала добавьте друзей.' : 'Все друзья уже в беседе.'; box.appendChild(e); }
    for (const [u] of list.sort((a, b) => nameOf(a[0]).localeCompare(nameOf(b[0])))) {
      const l = document.createElement('label'); l.className = 'pickrow';
      const cb = document.createElement('input'); cb.type = 'checkbox'; cb.value = u;
      const nm = document.createElement('span'); nm.textContent = nameOf(u);
      l.append(cb, ZV.avatar(profOf(u), 30, { dot: true, status: presence(u) }), nm); box.appendChild(l);
    }
    $('pick').hidden = false;
  }
  $('btnNewGroup').onclick = () => openPicker('new');
  $('pickCancel').onclick = () => { $('pick').hidden = true; };
  $('pick').onclick = (e) => { if (e.target === $('pick')) $('pick').hidden = true; };
  $('pickOk').onclick = () => {
    const sel = [...$('pickList').querySelectorAll('input:checked')].map((c) => c.value);
    const { mode, gid } = pickMode || {};
    if (mode === 'new') {
      if (!sel.length) { ZV.toast('Выберите хотя бы одного друга'); return; }
      if (sel.length + 1 > MAX_GROUP) { ZV.toast('В беседе до ' + MAX_GROUP + ' человек'); return; }
      const id = ZV.rid();
      const members = { [myUid]: myPub }; sel.forEach((u) => { members[u] = friends[u].pub; });
      const name = $('pickName').value.trim().slice(0, 60) || [profile.name, ...sel.map(nameOf)].join(', ').slice(0, 60);
      groups[id] = { gid: id, name, owner: myUid, v: 0, members, created: Date.now() };
      pushGinfo(groups[id]);
      $('pick').hidden = true;
      meta['g:' + id] = { last: Date.now() }; saveMeta();
      addSys('g:' + id, 'Вы создали беседу');
      openConv('g:' + id);
    } else if (gid && groups[gid]) {
      const g = groups[gid];
      if (Object.keys(g.members).length + sel.length > MAX_GROUP) { ZV.toast('В беседе до ' + MAX_GROUP + ' человек'); return; }
      sel.forEach((u) => { g.members[u] = friends[u].pub; });
      pushGinfo(g);
      addSys('g:' + gid, 'Вы добавили: ' + sel.map(nameOf).join(', '));
      $('pick').hidden = true; openGroupModal(gid);
    }
  };
  function openGroupModal(gid) {
    const g = groups[gid]; if (!g) return;
    $('gName').value = g.name;
    const box = $('gMembers'); box.innerHTML = '';
    for (const u of Object.keys(g.members).sort((a, b) => nameOf(a).localeCompare(nameOf(b)))) {
      const row = document.createElement('div'); row.className = 'frow';
      const nm = document.createElement('div'); nm.className = 'nm';
      const t = document.createElement('div'); t.textContent = nameOf(u) + (u === myUid ? ' (вы)' : '') + (u === g.owner ? ' 👑' : '');
      const s = document.createElement('small'); s.textContent = ZV.STATUS[presence(u)];
      nm.append(t, s);
      row.append(ZV.avatar(profOf(u), 34, { dot: true, status: presence(u) }), nm);
      row.onclick = () => openUserCard(u);
      if (g.owner === myUid && u !== myUid) {
        const b = document.createElement('button'); b.textContent = 'Убрать'; b.className = 'off';
        b.onclick = (e) => { e.stopPropagation(); if (!confirm('Убрать ' + nameOf(u) + ' из беседы?')) return; const pub = g.members[u]; delete g.members[u]; pushGinfo(g, [u, pub]); addSys('g:' + gid, 'Вы убрали ' + nameOf(u)); openGroupModal(gid); renderAll(); };
        row.appendChild(b);
      }
      box.appendChild(row);
    }
    $('gAdd').onclick = () => { $('gmodal').hidden = true; openPicker('add', gid); };
    $('gName').onchange = () => { const n = $('gName').value.trim().slice(0, 60); if (!n || n === g.name) return; g.name = n; pushGinfo(g); addSys('g:' + gid, 'Вы переименовали беседу в «' + n + '»'); renderAll(); };
    $('gLeave').onclick = () => {
      if (!confirm('Покинуть беседу «' + g.name + '»?')) return;
      for (const [u, pub] of Object.entries(g.members)) if (u !== myUid) send(u, pub, { t: 'gleave', gid });
      delete groups[gid]; saveGroups(); delete meta['g:' + gid]; saveMeta();
      $('gmodal').hidden = true; showFriends();
    };
    $('gmodal').hidden = false;
  }
  $('gClose').onclick = () => { $('gmodal').hidden = true; };
  $('gmodal').onclick = (e) => { if (e.target === $('gmodal')) $('gmodal').hidden = true; };

  /* ============================================================
     Настройки: «Профиль» (как в Дискорде) и «Аккаунт»
     ============================================================ */
  function paintProfilePane() {
    $('pName').value = profile.name; $('pUser').value = profile.username; $('pPron').value = profile.pronouns;
    $('pAbout').value = profile.about; $('pStatus').value = profile.status; $('pCustom').value = profile.custom;
    $('pBannerColor').value = profile.bannerColor || '#5a4a2a';
    paintPreview();
  }
  function paintPreview() { const b = $('profPreview'); b.innerHTML = ''; b.appendChild(ZV.profileCard(ZV.cleanProfile(profile), { status: presence(myUid) })); }
  let profTimer = 0;
  async function profileChanged() {
    Object.assign(profile, ZV.cleanProfile(profile));
    ZV.saveProfile(); paintPreview(); paintMyPanel();
    if (window.Call && window.Call.active()) window.Call.profileChanged();
    clearTimeout(profTimer);
    profTimer = setTimeout(async () => {
      myHash = await profileHash(ZV.publicProfile());
      if (profile.status === 'invisible') { for (const [u, pub] of contacts()) sendRaw(u, pub, { t: 'off' }); }
      else for (const [u, pub] of contacts()) sendRaw(u, pub, { t: 'here', ph: myHash, ack: 1 });
    }, 800);
  }
  const bind = (id, field) => { $(id).oninput = () => { profile[field] = $(id).value; profileChanged(); }; };
  bind('pName', 'name'); bind('pUser', 'username'); bind('pPron', 'pronouns'); bind('pAbout', 'about'); bind('pCustom', 'custom');
  $('pStatus').onchange = () => { const was = profile.status; profile.status = $('pStatus').value; profileChanged(); if (was === 'invisible' && profile.status !== 'invisible') setTimeout(announce, 900); };
  $('pBannerColor').oninput = () => { profile.bannerColor = $('pBannerColor').value; profileChanged(); };
  $('btnAvaUp').onclick = () => $('avaInput').click();
  $('btnAvaDel').onclick = () => { profile.avatar = ''; profileChanged(); };
  $('avaInput').onchange = async (e) => {
    const f = e.target.files[0]; e.target.value = ''; if (!f) return;
    try { profile.avatar = await ZV.compressImage(f, 256, 256, 0.85, 60000, true); profileChanged(); } catch (err) { ZV.toast('Не удалось открыть картинку'); }
  };
  $('btnBanUp').onclick = () => $('banInput').click();
  $('btnBanDel').onclick = () => { profile.banner = ''; profileChanged(); };
  $('banInput').onchange = async (e) => {
    const f = e.target.files[0]; e.target.value = ''; if (!f) return;
    try { profile.banner = await ZV.compressImage(f, 680, 240, 0.8, 90000, false); profileChanged(); } catch (err) { ZV.toast('Не удалось открыть картинку'); }
  };

  function paintAccountPane() {
    $('accCode').value = myPub;
    $('accId').textContent = myUid;
    $('btnNotif').hidden = !('Notification' in window) || Notification.permission === 'granted';
  }
  $('accCopy').onclick = (e) => copy(myPub, e.target);
  $('accLink').onclick = (e) => copy(friendLink(), e.target);
  $('btnNotif').onclick = () => { Notification.requestPermission().then(paintAccountPane); };
  $('btnExport').onclick = () => {
    const data = { app: 'zvonok', v: 1, ts: Date.now(), ls: {} };
    for (let i = 0; i < localStorage.length; i++) { const k = localStorage.key(i); if (/^(zv_|zvonok_)/.test(k)) data.ls[k] = localStorage.getItem(k); }
    const url = URL.createObjectURL(new Blob([JSON.stringify(data)], { type: 'application/json' }));
    const a = document.createElement('a'); a.href = url; a.download = 'zvonok-akkaunt-' + profile.name.replace(/[^\wа-яё-]/gi, '') + '.json';
    document.body.appendChild(a); a.click(); a.remove(); setTimeout(() => URL.revokeObjectURL(url), 5000);
  };
  $('btnImport').onclick = () => $('importInput').click();
  $('importInput').onchange = async (e) => {
    const f = e.target.files[0]; e.target.value = ''; if (!f) return;
    try {
      const data = JSON.parse(await f.text());
      if (data.app !== 'zvonok' || !data.ls || !data.ls.zv_identity) throw new Error('bad');
      if (!confirm('Заменить текущий аккаунт на аккаунт из файла? Текущий пропадёт, если вы его не сохранили.')) return;
      Object.keys(localStorage).filter((k) => /^(zv_|zvonok_)/.test(k)).forEach((k) => localStorage.removeItem(k));
      for (const [k, v] of Object.entries(data.ls)) if (/^(zv_|zvonok_)/.test(k)) localStorage.setItem(k, v);
      location.href = location.pathname;
    } catch (err) { ZV.toast('Это не файл аккаунта'); }
  };
  $('btnWipe').onclick = () => {
    if (!confirm('Удалить аккаунт, друзей и всю переписку с этого устройства? Это нельзя отменить (если не сохранили файл аккаунта).')) return;
    Object.keys(localStorage).filter((k) => /^(zv_|zvonok_)/.test(k)).forEach((k) => localStorage.removeItem(k));
    location.href = location.pathname;
  };
  document.addEventListener('zv:tab', (e) => { if (e.detail === 'profile') paintProfilePane(); if (e.detail === 'account') paintAccountPane(); });
  $('myPanelSettings').onclick = () => ZV.openSettings('profile');

  /* ============================================================
     Комнаты по ссылке
     ============================================================ */
  const cleanCode = (s) => String(s || '').trim().toLowerCase().replace(/^.*[?&]room=/, '').replace(/[^a-z0-9]/g, '');
  $('btnCreate').onclick = () => window.Call.start(true, ZV.randomCode(), $('createPass').value, { uid: myUid, title: 'Комната' });
  $('btnJoin').onclick = () => { const c = cleanCode($('joinCode').value); if (!c) { ZV.toast('Введите код комнаты'); return; } window.Call.start(false, c, $('joinPass').value, { uid: myUid, title: 'Комната' }); };
  ['joinCode', 'joinPass'].forEach((id) => $(id).addEventListener('keydown', (e) => { if (e.key === 'Enter') $('btnJoin').click(); }));
  $('btnCheck').onclick = () => ZV.openSettings('voice');

  /* ============================================================
     Запуск
     ============================================================ */
  (async () => {
    if (!window.crypto || !crypto.subtle) { document.body.innerHTML = '<p style="padding:20px">Откройте страницу по https://</p>'; return; }
    await initIdentity();
    myHash = await profileHash(ZV.publicProfile());
    initNet();
    const q = new URLSearchParams(location.search);
    const room = cleanCode(q.get('room')), add = q.get('add');
    if (room) {
      $('joinCode').value = room; $('roomCreateCard').hidden = true;
      $('roomsLead').textContent = 'Вас пригласили в звонок. Ведущий увидит ваше имя и впустит вас.';
      showRooms();
    } else if (add) {
      history.replaceState(null, '', location.pathname);
      showFriends('add'); $('addCode').value = add;
      if (add === myPub) { $('addMsg').textContent = 'Это ссылка на вас самих — отправьте её другу.'; $('addCode').value = ''; }
    } else showFriends(Object.values(friends).some((f) => f.state === 'friend') ? 'online' : 'add');
    if (!profile.name || profile.name === 'Гость') setTimeout(() => { ZV.openSettings('profile'); ZV.toast('Как вас зовут? Заполните профиль'); }, 300);
  })();
})();
