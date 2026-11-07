/* Групповой звонок (до 6 человек): видео, голос, экран, чат, файлы.
   Соединение каждый с каждым (WebRTC). Служебные сообщения — через
   MQTT, зашифрованы AES-256 ключом из кода комнаты и пароля. */
(function () {
  'use strict';
  const ZV = window.ZV, $ = ZV.$, enc = ZV.enc, dec = ZV.dec;
  const MAX_PEOPLE = 6;
  const MAX_FILE = 25 * 1024 * 1024;
  const profile = ZV.profile;
  const loadDev = ZV.loadDev, saveDev = ZV.saveDev, lsGet = ZV.lsGet, lsSet = ZV.lsSet;
  const avatar = ZV.avatar, cleanProfile = ZV.cleanProfile, rid = ZV.rid, STATUS = ZV.STATUS;

  /* Если у кого-то не соединяется или нужно надёжно скрывать IP —
     добавьте свой TURN (бесплатно на metered.ca или Cloudflare). */
  const ICE_SERVERS = [
    { urls: ['stun:stun.l.google.com:19302', 'stun:stun1.l.google.com:19302'] },
    { urls: 'stun:stun.cloudflare.com:3478' },
    { urls: 'turn:eu-0.turn.peerjs.com:3478', username: 'peerjs', credential: 'peerjsp' },
    { urls: 'turn:us-0.turn.peerjs.com:3478', username: 'peerjs', credential: 'peerjsp' },
    // { urls: ['turn:АДРЕС:80', 'turn:АДРЕС:443?transport=tcp'], username: 'ЛОГИН', credential: 'ПАРОЛЬ' },
  ];

  /* ============================================================
     Зашифрованный канал «знакомства» в комнате
     ============================================================ */
  const myId = rid();
  let key = null, topic = '', brokers = [], seq = 0, rxChain = Promise.resolve();
  const lastSeq = {}, pendingRx = {};

  async function initSignal(code, pass) {
    const base = await crypto.subtle.importKey('raw', enc.encode('zvonok|' + code + '|' + pass), 'PBKDF2', false, ['deriveKey']);
    key = await crypto.subtle.deriveKey({ name: 'PBKDF2', salt: enc.encode('zvonok-salt-v1'), iterations: 150000, hash: 'SHA-256' },
      base, { name: 'AES-GCM', length: 256 }, false, ['encrypt', 'decrypt']);
    const h = new Uint8Array(await crypto.subtle.digest('SHA-256', enc.encode('zvonok-topic|' + code)));
    topic = 'zvonok/v2/' + ZV.hex(h.slice(0, 20));
    brokers = ZV.BROKERS.map((u) => new ZV.Mqtt(u, (t, p) => { rxChain = rxChain.then(() => onRaw(t, p)); }, netState));
    brokers.forEach((b) => b.subscribe(topic));
  }
  async function signal(obj) {
    obj.from = myId; obj.seq = ++seq;
    const iv = crypto.getRandomValues(new Uint8Array(12));
    const ct = new Uint8Array(await crypto.subtle.encrypt({ name: 'AES-GCM', iv }, key, enc.encode(JSON.stringify(obj))));
    const out = new Uint8Array(12 + ct.length); out.set(iv); out.set(ct, 12);
    let sent = false; brokers.forEach((b) => { if (b.publish(topic, out)) sent = true; });
    return sent;
  }
  async function onRaw(t, payload) {
    if (t !== topic || payload.length < 29) return;
    let obj;
    try { obj = JSON.parse(dec.decode(await crypto.subtle.decrypt({ name: 'AES-GCM', iv: payload.slice(0, 12) }, key, payload.slice(12)))); }
    catch (e) { return; }
    if (!obj || typeof obj.from !== 'string' || obj.from === myId || typeof obj.seq !== 'number') return;
    if (obj.to && obj.to !== myId) return;
    const f = obj.from;
    if (lastSeq[f] === undefined) lastSeq[f] = obj.seq - 1;
    if (obj.seq <= lastSeq[f]) return;
    (pendingRx[f] = pendingRx[f] || {})[obj.seq] = obj;
    drain(f);
    if (Object.keys(pendingRx[f]).length) setTimeout(() => {
      const ks = Object.keys(pendingRx[f] || {}).map(Number);
      if (ks.length) { lastSeq[f] = Math.min(...ks) - 1; drain(f); }
    }, 3000);
  }
  function drain(f) {
    const q = pendingRx[f];
    while (q && q[lastSeq[f] + 1]) { const m = q[lastSeq[f] + 1]; delete q[lastSeq[f] + 1]; lastSeq[f]++; try { onSignal(m); } catch (e) { console.warn(e); } }
  }
  let netTimer = 0;
  function netState() {
    const n = brokers.filter((b) => b.ok).length;
    $('net').textContent = 'серверы связи: ' + n + '/' + brokers.length;
    clearTimeout(netTimer);
    if (n === 0) netTimer = setTimeout(() => { if (!brokers.some((b) => b.ok) && !S.peers.size) setState('Нет связи с серверами — проверьте интернет'); }, 10000);
    if (n > 0 && S.role === 'guest' && !S.joined) knock();
  }

  /* ============================================================
     Камера и микрофон
     ============================================================ */
  let localStream = null, screenStream = null, voiceOut = null, camOut = null;
  function blankVideoTrack() {
    const c = Object.assign(document.createElement('canvas'), { width: 64, height: 36 });
    const g = c.getContext('2d'); g.fillStyle = '#000'; g.fillRect(0, 0, 64, 36);
    const t = c.captureStream(1).getVideoTracks()[0]; t._blank = true; return t;
  }
  function silentAudioTrack() { const t = ZV.actx().createMediaStreamDestination().stream.getAudioTracks()[0]; t._blank = true; return t; }
  function audioC(id) {
    const ns = loadDev().noise !== false;
    const c = { echoCancellation: ns, noiseSuppression: ns, autoGainControl: true };
    if (id) c.deviceId = { exact: id }; return c;
  }
  function videoC(id) { const c = { width: { ideal: 1280 }, height: { ideal: 720 }, frameRate: { ideal: 30 } }; if (id) c.deviceId = { exact: id }; return c; }
  async function getMic() {
    const dev = loadDev();
    try { return (await navigator.mediaDevices.getUserMedia({ audio: audioC(dev.mic) })).getAudioTracks()[0]; }
    catch (e) {
      if (dev.mic) { saveDev({ mic: '' }); try { return (await navigator.mediaDevices.getUserMedia({ audio: audioC() })).getAudioTracks()[0]; } catch (e2) {} }
      return null;
    }
  }
  async function getCam() {
    const dev = loadDev();
    try { return (await navigator.mediaDevices.getUserMedia({ video: videoC(dev.cam) })).getVideoTracks()[0]; }
    catch (e) {
      if (dev.cam) { saveDev({ cam: '' }); try { return (await navigator.mediaDevices.getUserMedia({ video: videoC() })).getVideoTracks()[0]; } catch (e2) {} }
      return null;
    }
  }
  let micMissing = false;
  async function ensureLocal() {
    if (localStream) return;
    const a = await getMic();
    micMissing = !a;
    localStream = new MediaStream([a || silentAudioTrack(), blankVideoTrack()]);
    voiceOut = new MediaStream([localStream.getAudioTracks()[0]]);
    camOut = new MediaStream([localStream.getVideoTracks()[0]]);
  }
  const micTrack = () => localStream && localStream.getAudioTracks()[0];
  const camTrack = () => localStream && localStream.getVideoTracks()[0];
  const camOn = () => { const t = camTrack(); return !!t && !t._blank; };

  function replaceLocal(kind, track) {
    const old = kind === 'audio' ? micTrack() : camTrack();
    if (old === track) return;
    if (old) { localStream.removeTrack(old); old.stop(); }
    localStream.addTrack(track);
    for (const p of S.peers.values()) {
      const s = kind === 'audio' ? p.senders.voice : p.senders.cam;
      if (s) s.replaceTrack(track).catch((e) => console.warn(e));
    }
    if (kind === 'audio') { applyMic(); attachMeter(me, new MediaStream([track])); }
    else { const v = tileVideo('me:cam'); if (v) { v.srcObject = null; v.srcObject = new MediaStream([track]); } }
    sendAll({ t: 'state', st: myState() });
    render();
  }

  /* ============================================================
     Состояние звонка
     ============================================================ */
  const S = {
    role: '', code: '', pass: '', hostId: '', joined: false,
    roster: new Map(), allowed: new Set(), peers: new Map(), early: {}, focused: null,
    autoAdmit: new Set(), uid: '', title: '',
  };
  const me = { id: myId };
  let muted = false, deaf = false, pttDown = false;
  let knockTimer = 0, knockStarted = 0, answered = false, knockQueue = [];

  function myState() { return { mic: !muted && !deaf && !(micTrack() || {})._blank, deaf, cam: camOn(), screen: !!screenStream }; }
  function setState(t) { $('state').textContent = t; }

  /* Вход в звонок. opts: { uid — мой id аккаунта, autoAdmit — кого
     впускать без вопроса (друзья, участники беседы), title — название } */
  async function start(asHost, code, pass, opts) {
    opts = opts || {};
    if (S.role) { ZV.toast('Вы уже в звонке'); return; }
    if (!navigator.mediaDevices || !window.RTCPeerConnection) { ZV.toast('Браузер не поддерживает звонки. Откройте в Chrome, Edge, Яндекс или Firefox.'); return; }
    if (!window.crypto || !crypto.subtle) { ZV.toast('Откройте страницу по https://'); return; }
    S.role = asHost ? 'host' : 'guest'; S.code = code; S.pass = pass || '';
    S.uid = opts.uid || ''; S.title = opts.title || 'Комната';
    (opts.autoAdmit || []).forEach((u) => S.autoAdmit.add(u));
    ZV.actx();
    ZV.toast('Включаем микрофон…');
    await ensureLocal();
    attachMeter(me, new MediaStream([micTrack()]));
    await initSignal(code, S.pass);
    showCall();
    if (micMissing) addSys('Микрофон не найден или запрещён — вас не будет слышно. Проверьте настройки.');
    if (asHost) {
      S.hostId = myId; S.joined = true; S.roster.set(myId, profile.name);
      setState('Ждём участников…');
      setInterval(() => { if (S.role === 'host') broadcastRoster(); }, 8000);
    } else { knockStarted = Date.now(); knock(); }
    applyMic();
    render();
  }

  /* --- Стук в дверь --- */
  function knock() {
    clearTimeout(knockTimer);
    if (S.role !== 'guest' || S.joined) return;
    signal({ t: 'knock', name: profile.name, uid: S.uid });
    const waited = (Date.now() - knockStarted) / 1000;
    setState(answered ? 'Ведущий получил запрос, ждём ответа…' : waited > 15 ? 'Нет ответа. Возможно, звонок уже закончился или пароль неверный. Ждём…' : 'Подключаемся к звонку…');
    knockTimer = setTimeout(knock, 3000);
  }
  function showKnock() {
    const k = knockQueue[0];
    if (!k) { $('knock').hidden = true; return; }
    $('knockText').textContent = '«' + k.name + '» хочет войти' + (knockQueue.length > 1 ? ' (ещё ' + (knockQueue.length - 1) + ' в очереди)' : '');
    if ($('knock').hidden) ZV.beep(880);
    $('knock').hidden = false;
  }
  function admit(k) {
    if (S.role !== 'host') return;
    if (S.roster.size >= MAX_PEOPLE) { signal({ t: 'full', to: k.id }); addSys('Комната заполнена (максимум ' + MAX_PEOPLE + ')'); return; }
    S.roster.set(k.id, k.name);
    signal({ t: 'accept', to: k.id, members: [...S.roster.keys()], auto: [...S.autoAdmit] });
    broadcastRoster();
  }
  $('knockYes').onclick = () => { const k = knockQueue.shift(); showKnock(); if (k) admit(k); };
  $('knockNo').onclick = () => { const k = knockQueue.shift(); if (k) signal({ t: 'reject', to: k.id }); showKnock(); };
  function broadcastRoster() {
    if (S.role !== 'host') return;
    signal({ t: 'roster', members: [...S.roster.keys()], auto: [...S.autoAdmit] });
    setAllowed([...S.roster.keys()]);
  }

  function onSignal(m) {
    const f = m.from;
    switch (m.t) {
      case 'knock':
        if (S.role !== 'host') return;
        if (S.roster.has(f)) { signal({ t: 'accept', to: f, members: [...S.roster.keys()] }); return; }
        if (m.uid && S.autoAdmit.has(m.uid)) { admit({ id: f, name: String(m.name || 'Гость').slice(0, 32) }); return; }
        signal({ t: 'wait', to: f });
        if (!knockQueue.some((k) => k.id === f)) { knockQueue.push({ id: f, name: String(m.name || 'Гость').slice(0, 32) }); showKnock(); }
        return;
      case 'wait': answered = true; return;
      case 'reject': if (!S.joined) { clearTimeout(knockTimer); S.role = 'rejected'; setState('Ведущий не впустил вас.'); } return;
      case 'full': if (!S.joined) { clearTimeout(knockTimer); setState('Комната заполнена.'); } return;
      case 'accept':
        if (S.role !== 'guest' || S.joined) return;
        clearTimeout(knockTimer); S.joined = true; S.hostId = f;
        if (Array.isArray(m.auto)) m.auto.forEach((u) => S.autoAdmit.add(String(u)));
        setState('Соединяемся…'); setAllowed(m.members || []);
        return;
      case 'roster':
        if (f === S.hostId && Array.isArray(m.members)) {
          if (Array.isArray(m.auto)) m.auto.forEach((u) => S.autoAdmit.add(String(u)));
          if (S.joined && !m.members.includes(myId)) { kicked(); return; }
          if (S.joined) setAllowed(m.members);
        }
        return;
      case 'kick': if (f === S.hostId && m.to === myId) kicked(); return;
      case 'newhost': if (S.allowed.has(f) || f === S.hostId) { S.hostId = f; render(); } return;
      case 'bye': leftPeer(f); return;
    }
    if (!S.allowed.has(f)) { (S.early[f] = S.early[f] || []).push(m); setTimeout(() => { delete S.early[f]; }, 15000); return; }
    const p = S.peers.get(f); if (!p) return;
    if (m.t === 'sdp') onSdp(p, m); else if (m.t === 'ice') onIce(p, m.c);
  }

  function setAllowed(members) {
    S.allowed = new Set(members.filter((x) => typeof x === 'string' && x !== myId).slice(0, MAX_PEOPLE));
    for (const id of S.allowed) if (!S.peers.has(id)) {
      connectPeer(id);
      const q = S.early[id]; delete S.early[id];
      (q || []).forEach(onSignal);
    }
    for (const id of [...S.peers.keys()]) if (!S.allowed.has(id)) dropPeer(id);
    retuneAll();
    if (S.joined && !S.peers.size) setState(S.role === 'host' ? 'Ждём участников…' : 'Соединяемся…');
  }
  function leftPeer(id) {
    const p = S.peers.get(id);
    if (S.role === 'host' && S.roster.has(id)) { S.roster.delete(id); broadcastRoster(); }
    else if (p) dropPeer(id);
    if (id === S.hostId) {
      const ids = [myId, ...S.peers.keys()].sort();
      if (ids[0] === myId && S.joined) {
        S.role = 'host'; S.hostId = myId;
        S.roster = new Map(ids.map((x) => [x, x === myId ? profile.name : (S.peers.get(x) ? S.peers.get(x).profile.name : '')]));
        addSys('Вы теперь ведущий: впускаете новых участников');
        signal({ t: 'newhost' }); broadcastRoster();
        setInterval(() => { if (S.role === 'host') broadcastRoster(); }, 8000);
      } else S.hostId = ids[0];
      render();
    }
  }
  function kicked() {
    for (const id of [...S.peers.keys()]) dropPeer(id, true);
    S.joined = false; S.role = 'kicked';
    setState('Ведущий удалил вас из комнаты.'); addSys('Вас удалили из комнаты');
  }

  /* ============================================================
     Соединение с одним участником.
     Голос, камера и экран — отдельные потоки: звук не ждёт видео.
     ============================================================ */
  function connectPeer(id) {
    const hideIp = loadDev().hideIp;
    const p = {
      id, polite: myId > id, makingOffer: false, ignoreOffer: false, cands: [],
      streams: {}, map: {}, profile: cleanProfile({ name: S.roster.get(id) || '…' }), st: {}, greeted: false,
      voice: null, cam: null, screen: null, senders: { voice: null, cam: null, screen: [] },
      audio: makeAudio(), saudio: makeAudio(), vol: 1, svol: 1, localMute: false, speaking: false,
      inFile: null, fileChain: Promise.resolve(),
    };
    S.peers.set(id, p);
    const pc = p.pc = new RTCPeerConnection({ iceServers: ICE_SERVERS, iceTransportPolicy: hideIp ? 'relay' : 'all' });
    p.dc = pc.createDataChannel('chat', { negotiated: true, id: 0 });
    p.fc = pc.createDataChannel('file', { negotiated: true, id: 1 }); p.fc.binaryType = 'arraybuffer';
    p.fc.bufferedAmountLowThreshold = 256 * 1024;
    p.dc.onopen = () => sendTo(p, { t: 'hello', profile: ZV.publicProfile(), st: myState() });
    p.dc.onmessage = (e) => onPeerMsg(p, e.data);
    p.fc.onmessage = (e) => onFileData(p, e.data);
    p.senders.voice = pc.addTrack(micTrack(), voiceOut);
    p.senders.cam = pc.addTrack(camTrack(), camOut);
    if (screenStream) addScreenTo(p);
    pc.onnegotiationneeded = async () => {
      try { p.makingOffer = true; await pc.setLocalDescription(); signal({ t: 'sdp', to: id, d: pc.localDescription, streams: myStreams(p) }); }
      catch (e) { console.warn(e); } finally { p.makingOffer = false; }
    };
    pc.onicecandidate = (e) => { if (e.candidate) signal({ t: 'ice', to: id, c: e.candidate }); };
    pc.ontrack = (e) => { const s = e.streams[0] || new MediaStream([e.track]); p.streams[s.id] = s; applyRemote(p); };
    let failTimer = 0;
    pc.onconnectionstatechange = () => {
      const st = pc.connectionState;
      clearTimeout(failTimer);
      if (st === 'connected') { setState(hideIp ? 'На связи (IP скрыт)' : 'На связи'); retunePeer(p); }
      else if (st === 'disconnected') failTimer = setTimeout(() => { if (pc.connectionState !== 'connected' && !p.polite) pc.restartIce(); }, 4000);
      else if (st === 'failed') {
        if (hideIp) setState('Не удалось соединиться через сервер-посредник. Отключите «Скрывать IP».');
        if (!p.polite) pc.restartIce();
        failTimer = setTimeout(() => { if (S.peers.get(id) === p && pc.connectionState === 'failed') { addSys('Связь с ' + p.profile.name + ' потеряна'); leftPeer(id); } }, 25000);
      }
      render();
    };
    retunePeer(p);
    render();
  }
  function dropPeer(id, silent) {
    const p = S.peers.get(id); if (!p) return;
    S.peers.delete(id);
    try { p.pc.close(); } catch (e) {}
    detachMeter(p);
    p.audio.srcObject = null; p.saudio.srcObject = null; p.audio.remove(); p.saudio.remove();
    if (!silent && p.greeted) { addSys(p.profile.name + ' вышел(а)'); ZV.beep(440); }
    if (S.focused && S.focused.startsWith(id + ':')) S.focused = null;
    retuneAll();
    render();
  }
  function myStreams(p) { return { voice: voiceOut.id, cam: camOut.id, screen: screenStream && p.senders.screen.length ? screenStream.id : null }; }
  async function onSdp(p, m) {
    const d = m.d; if (!d) return;
    if (m.streams) { p.map = m.streams; applyRemote(p); }
    const collision = d.type === 'offer' && (p.makingOffer || p.pc.signalingState !== 'stable');
    p.ignoreOffer = !p.polite && collision;
    if (p.ignoreOffer) return;
    try {
      await p.pc.setRemoteDescription(d);
      for (const c of p.cands.splice(0)) await p.pc.addIceCandidate(c).catch(() => {});
      if (d.type === 'offer') { await p.pc.setLocalDescription(); signal({ t: 'sdp', to: p.id, d: p.pc.localDescription, streams: myStreams(p) }); }
      retunePeer(p);
    } catch (e) { console.warn(e); }
  }
  async function onIce(p, c) {
    if (!c) return;
    if (!p.pc.remoteDescription) { p.cands.push(c); return; }
    try { await p.pc.addIceCandidate(c); } catch (e) { if (!p.ignoreOffer) console.warn(e); }
  }
  function applyRemote(p) {
    const voice = p.streams[p.map.voice] || null;
    if (voice && voice !== p.voice) { p.voice = voice; attachMeter(p, voice); }
    playAudio(p.audio, p.voice);
    p.cam = p.streams[p.map.cam] || p.cam;
    const scr = p.map.screen ? p.streams[p.map.screen] || null : null;
    if (scr && !p.screen) { addSys(p.profile.name + ' показывает экран'); if (!S.focused) S.focused = p.id + ':screen'; }
    if (!scr && p.screen && S.focused === p.id + ':screen') S.focused = null;
    p.screen = scr;
    playAudio(p.saudio, scr);
    applyVolumes(p);
    render();
  }

  /* --- Качество под число участников --- */
  const SCREEN_Q = {
    smooth: { fps: 60, w: 1920, h: 1080, bitrate: 8000000, hint: 'motion', pref: 'maintain-framerate' },
    balance: { fps: 30, w: 1920, h: 1080, bitrate: 5000000, hint: 'motion', pref: 'balanced' },
    text: { fps: 15, w: 2560, h: 1440, bitrate: 4000000, hint: 'detail', pref: 'maintain-resolution' },
  };
  const screenQ = () => SCREEN_Q[loadDev().screenQ] || SCREEN_Q.smooth;
  function setEnc(sender, o, pref) {
    try {
      const prm = sender.getParameters();
      if (!prm.encodings || !prm.encodings.length) return;
      Object.assign(prm.encodings[0], o);
      if (pref) prm.degradationPreference = pref;
      sender.setParameters(prm).catch(() => {});
    } catch (e) {}
  }
  function retunePeer(p) {
    const n = Math.max(1, S.peers.size);
    if (p.senders.cam) setEnc(p.senders.cam, { maxBitrate: n <= 2 ? 1500000 : n <= 3 ? 900000 : 600000, scaleResolutionDownBy: n <= 2 ? 1 : 2, maxFramerate: 30 });
    if (p.senders.voice) setEnc(p.senders.voice, { maxBitrate: 64000, priority: 'high', networkPriority: 'high' });
    const q = screenQ();
    p.senders.screen.forEach((s) => {
      if (s.track && s.track.kind === 'video') setEnc(s, { maxBitrate: Math.max(1500000, q.bitrate / Math.max(1, n * 0.75)), maxFramerate: n >= 3 ? Math.min(30, q.fps) : q.fps }, q.pref);
    });
  }
  function retuneAll() { for (const p of S.peers.values()) retunePeer(p); }

  /* ============================================================
     Канал данных
     ============================================================ */
  function sendTo(p, obj) { try { if (p.dc && p.dc.readyState === 'open') p.dc.send(JSON.stringify(obj)); } catch (e) {} }
  function sendAll(obj) { for (const p of S.peers.values()) sendTo(p, obj); }
  function onPeerMsg(p, raw) {
    let d; try { d = JSON.parse(raw); } catch (e) { return; }
    switch (d.t) {
      case 'hello':
        p.profile = cleanProfile(d.profile); p.st = d.st || {};
        if (S.role === 'host' && S.roster.has(p.id)) S.roster.set(p.id, p.profile.name);
        loadPeerVolume(p);
        if (!p.greeted) { p.greeted = true; addSys(p.profile.name + ' присоединился(ась)'); ZV.beep(660); }
        render(); break;
      case 'profile': p.profile = cleanProfile(d.profile); loadPeerVolume(p); render(); renderChat(); break;
      case 'state': p.st = d.st || {}; render(); break;
      case 'msg': addChat({ mid: String(d.mid).slice(0, 40), from: p.id, text: String(d.text || '').slice(0, 2000), ts: Date.now() }); break;
      case 'react': toggleReact(String(d.mid), String(d.emoji).slice(0, 8), p.id, !!d.on); break;
      case 'bye': leftPeer(p.id); break;
    }
  }

  /* ============================================================
     Файлы
     ============================================================ */
  function onFileData(p, data) {
    if (typeof data === 'string') {
      let d; try { d = JSON.parse(data); } catch (e) { return; }
      if (d.t === 'fstart' && d.size <= MAX_FILE) p.inFile = { mid: String(d.mid).slice(0, 40), name: String(d.name).slice(0, 120), size: d.size, mime: String(d.mime || '').slice(0, 80), parts: [], got: 0 };
      else if (d.t === 'fend' && p.inFile) {
        const f = p.inFile; p.inFile = null;
        if (f.got !== f.size) return;
        const blob = new Blob(f.parts, { type: f.mime || 'application/octet-stream' });
        addChat({ mid: f.mid, from: p.id, text: '', ts: Date.now(), file: { name: f.name, size: f.size, mime: f.mime, url: URL.createObjectURL(blob) } });
      }
      return;
    }
    if (p.inFile) { p.inFile.got += data.byteLength; if (p.inFile.got > MAX_FILE) { p.inFile = null; return; } p.inFile.parts.push(data); }
  }
  async function sendFileTo(p, file, mid) {
    const ch = p.fc; if (!ch || ch.readyState !== 'open') return;
    ch.send(JSON.stringify({ t: 'fstart', mid, name: file.name, size: file.size, mime: file.type }));
    const CH = 16 * 1024;
    for (let off = 0; off < file.size; off += CH) {
      if (ch.readyState !== 'open') return;
      if (ch.bufferedAmount > 1024 * 1024) await new Promise((r) => { ch.onbufferedamountlow = () => { ch.onbufferedamountlow = null; r(); }; });
      ch.send(await file.slice(off, off + CH).arrayBuffer());
    }
    ch.send(JSON.stringify({ t: 'fend', mid }));
  }
  function sendFiles(files) {
    for (const file of files) {
      if (file.size > MAX_FILE) { addSys('«' + file.name + '» больше 25 МБ — не отправлено'); continue; }
      if (!S.peers.size) { addSys('Некому отправить: в комнате никого нет'); return; }
      const mid = rid();
      addChat({ mid, from: myId, text: '', ts: Date.now(), file: { name: file.name, size: file.size, mime: file.type, url: URL.createObjectURL(file) } });
      for (const p of S.peers.values()) p.fileChain = p.fileChain.then(() => sendFileTo(p, file, mid)).catch(() => {});
    }
  }

  /* ============================================================
     Звук
     ============================================================ */
  function makeAudio() { const a = document.createElement('audio'); a.autoplay = true; a.hidden = true; document.body.appendChild(a); return a; }
  let audioHint = false;
  function playAudio(el, stream) {
    const s = stream && stream.getAudioTracks().length ? stream : null;
    if (el.srcObject !== s) el.srcObject = s;
    if (!s) return;
    const id = loadDev().spk; if (id && el.setSinkId) el.setSinkId(id).catch(() => {});
    if (el.paused) el.play().catch(() => {
      if (!audioHint) { audioHint = true; addSys('Нажмите в любом месте страницы, чтобы включить звук'); }
      document.addEventListener('click', () => { document.querySelectorAll('audio').forEach((a) => { if (a.srcObject) a.play().catch(() => {}); }); }, { once: true });
    });
  }
  function applyVolumes(p) {
    const mute = deaf || p.localMute;
    [[p.audio, p.vol, 'g'], [p.saudio, p.svol, 'sg']].forEach(([el, v, gk]) => {
      el.muted = mute;
      if (v <= 1 || !el.srcObject) { el.volume = Math.max(0, Math.min(1, v)); if (p[gk]) p[gk].gain.value = 0; return; }
      el.volume = 1;
      const actx = ZV.actx();
      if (!p[gk] || p[gk]._src !== el.srcObject) {
        try { if (p[gk]) p[gk].disconnect(); } catch (e) {}
        const src = actx.createMediaStreamSource(el.srcObject);
        const g = actx.createGain(); src.connect(g).connect(actx.destination); g._src = el.srcObject; p[gk] = g;
      }
      p[gk].gain.value = mute ? 0 : v - 1;
    });
  }
  function volKey(p) { return 'vol:' + p.profile.name; }
  function loadPeerVolume(p) { const v = lsGet('zvonok_vol', {})[volKey(p)]; if (v) { p.vol = v.v; p.svol = v.s; p.localMute = !!v.m; } applyVolumes(p); }
  function savePeerVolume(p) { const all = lsGet('zvonok_vol', {}); all[volKey(p)] = { v: p.vol, s: p.svol, m: p.localMute }; lsSet('zvonok_vol', all); }

  function attachMeter(obj, stream) {
    detachMeter(obj);
    if (!stream || !stream.getAudioTracks().length) return;
    try {
      const actx = ZV.actx();
      obj.msrc = actx.createMediaStreamSource(stream);
      obj.an = actx.createAnalyser(); obj.an.fftSize = 256; obj.mbuf = new Uint8Array(256);
      obj.msrc.connect(obj.an);
    } catch (e) {}
  }
  function detachMeter(obj) { try { obj.msrc && obj.msrc.disconnect(); } catch (e) {} obj.msrc = obj.an = null; }
  function level(obj) {
    if (!obj.an) return 0;
    obj.an.getByteTimeDomainData(obj.mbuf);
    let s = 0; for (const b of obj.mbuf) { const x = b - 128; s += x * x; }
    return Math.sqrt(s / obj.mbuf.length);
  }
  setInterval(() => {
    if (!localStream || !S.role) return;
    const mt = micTrack();
    const meSpeak = !!mt && mt.enabled && level(me) > 4;
    let changed = meSpeak !== me.speaking; me.speaking = meSpeak;
    for (const p of S.peers.values()) {
      const sp = !p.localMute && !!p.st.mic && level(p) > 4;
      if (sp !== p.speaking) { p.speaking = sp; changed = true; }
    }
    if (changed) paintSpeaking();
  }, 150);

  function applyMic() {
    const t = micTrack(); if (!t) return;
    const ptt = loadDev().inMode === 'ptt';
    t.enabled = !t._blank && !muted && !deaf && (!ptt || pttDown);
    sendAll({ t: 'state', st: myState() });
    paintMe();
  }
  function toggleMute() { if (deaf) { deaf = false; muted = false; } else muted = !muted; ZV.beep(muted ? 300 : 600); applyMic(); for (const p of S.peers.values()) applyVolumes(p); }
  function toggleDeaf() { deaf = !deaf; ZV.beep(deaf ? 300 : 600); applyMic(); for (const p of S.peers.values()) applyVolumes(p); }
  function keyName(k) { return !k ? 'не назначена' : k === ' ' ? 'Пробел' : k.length === 1 ? k.toUpperCase() : k; }
  let capturingKey = false;
  document.addEventListener('keydown', (e) => {
    if (capturingKey) { e.preventDefault(); capturingKey = false; saveDev({ pttKey: e.key }); $('pttKey').textContent = keyName(e.key); $('btnPttKey').textContent = 'Назначить'; return; }
    if (S.joined && e.ctrlKey && (e.key === 'm' || e.key === 'ь')) { e.preventDefault(); toggleMute(); return; }
    if (S.joined && e.ctrlKey && (e.key === 'd' || e.key === 'в')) { e.preventDefault(); toggleDeaf(); return; }
    const dev = loadDev();
    if (S.joined && dev.inMode === 'ptt' && dev.pttKey && e.key === dev.pttKey && !e.repeat && !/INPUT|TEXTAREA|SELECT/.test(document.activeElement.tagName)) { e.preventDefault(); pttDown = true; applyMic(); }
  });
  document.addEventListener('keyup', (e) => { const dev = loadDev(); if (dev.inMode === 'ptt' && e.key === dev.pttKey && pttDown) { pttDown = false; applyMic(); } });
  window.addEventListener('blur', () => { if (pttDown) { pttDown = false; applyMic(); } });

  /* ============================================================
     Камера и экран
     ============================================================ */
  async function toggleCam() {
    if (camOn()) { replaceLocal('video', blankVideoTrack()); return; }
    $('btnCam').disabled = true;
    const t = await getCam();
    $('btnCam').disabled = false;
    if (!t) { addSys('Камера не найдена или доступ запрещён'); return; }
    replaceLocal('video', t);
  }
  function addScreenTo(p) { p.senders.screen = screenStream.getTracks().map((t) => p.pc.addTrack(t, screenStream)); retunePeer(p); }
  async function toggleScreen() {
    if (screenStream) { stopScreen(); return; }
    if (!navigator.mediaDevices.getDisplayMedia) { addSys('На этом устройстве показ экрана не поддерживается (обычно — телефон).'); return; }
    const q = screenQ();
    try {
      screenStream = await navigator.mediaDevices.getDisplayMedia({
        video: { frameRate: { ideal: q.fps, max: q.fps }, width: { max: q.w }, height: { max: q.h } },
        audio: { echoCancellation: false, noiseSuppression: false, autoGainControl: false, restrictOwnAudio: true, suppressLocalAudioPlayback: false },
        systemAudio: 'include', selfBrowserSurface: 'exclude', surfaceSwitching: 'include',
      });
    } catch (e) { screenStream = null; return; }
    const v = screenStream.getVideoTracks()[0];
    if (v) { v.contentHint = q.hint; v.onended = stopScreen; }
    for (const p of S.peers.values()) addScreenTo(p);
    sendAll({ t: 'state', st: myState() });
    render();
  }
  function stopScreen() {
    if (!screenStream) return;
    screenStream.getTracks().forEach((t) => t.stop());
    screenStream = null;
    for (const p of S.peers.values()) { p.senders.screen.forEach((s) => { try { p.pc.removeTrack(s); } catch (e) {} }); p.senders.screen = []; }
    if (S.focused === 'me:screen') S.focused = null;
    sendAll({ t: 'state', st: myState() });
    render();
  }
  function hangUp() {
    sendAll({ t: 'bye' });
    const done = () => {
      stopScreen();
      if (localStream) localStream.getTracks().forEach((t) => t.stop());
      for (const p of S.peers.values()) { try { p.pc.close(); } catch (e) {} }
      brokers.forEach((b) => b.close());
      location.href = location.pathname;
    };
    if (key) signal({ t: 'bye' }).finally(() => setTimeout(done, 300)); else done();
  }

  /* ============================================================
     Отрисовка
     ============================================================ */
  const tiles = new Map();
  function tileVideo(k) { const t = tiles.get(k); return t && t.video; }
  function wantedTiles() {
    const list = [];
    list.push({ k: 'me:cam', who: me, prof: profile, stream: camOn() ? new MediaStream([camTrack()]) : null, label: profile.name + ' (вы)', mirror: true });
    if (screenStream) list.push({ k: 'me:screen', who: me, prof: profile, stream: screenStream, label: 'Ваш экран', screen: true });
    for (const p of S.peers.values()) {
      list.push({ k: p.id + ':cam', who: p, prof: p.profile, stream: p.st.cam ? p.cam : null, label: p.profile.name });
      if (p.screen) list.push({ k: p.id + ':screen', who: p, prof: p.profile, stream: p.screen, label: 'Экран: ' + p.profile.name, screen: true });
    }
    return list;
  }
  function render() {
    if (!S.role) return;
    const want = wantedTiles();
    const keys = new Set(want.map((w) => w.k));
    for (const [k, t] of tiles) if (!keys.has(k)) { t.video.srcObject = null; t.el.remove(); tiles.delete(k); }
    if (S.focused && !keys.has(S.focused)) S.focused = null;
    const grid = $('grid'), fa = $('focusArea');
    for (const w of want) {
      let t = tiles.get(w.k);
      if (!t) {
        const el = document.createElement('div'); el.className = 'tile';
        const video = document.createElement('video'); video.autoplay = true; video.playsInline = true; video.muted = true;
        const avaWrap = document.createElement('div'); avaWrap.className = 'avaWrap';
        const label = document.createElement('div'); label.className = 'label';
        el.append(video, avaWrap, label);
        el.onclick = () => { S.focused = S.focused === w.k ? null : w.k; render(); };
        el.ondblclick = () => { if (document.fullscreenElement) document.exitFullscreen(); else if (el.requestFullscreen) el.requestFullscreen().catch(() => {}); };
        t = { el, video, avaWrap, label, avaKey: '' };
        tiles.set(w.k, t);
      }
      t.who = w.who;
      const hasVideo = !!w.stream && w.stream.getVideoTracks().length > 0;
      if (hasVideo) {
        const cur = t.video.srcObject;
        if (!(cur && cur.getVideoTracks()[0] === w.stream.getVideoTracks()[0])) t.video.srcObject = w.stream;
      } else if (t.video.srcObject) t.video.srcObject = null;
      t.video.hidden = !hasVideo;
      t.video.style.transform = w.mirror ? 'scaleX(-1)' : '';
      const ak = (w.prof.avatar || '') + '|' + w.prof.name;
      if (!hasVideo && t.avaKey !== ak) { t.avaWrap.innerHTML = ''; t.avaWrap.appendChild(avatar(w.prof, 84)); t.avaKey = ak; }
      t.avaWrap.hidden = hasVideo;
      const st = w.who === me ? myState() : w.who.st;
      t.label.textContent = (w.screen ? '' : (st.deaf ? '🎧̸ ' : !st.mic ? '🔇 ' : '')) + w.label;
      const parent = S.focused === w.k ? fa : grid;
      if (t.el.parentNode !== parent) parent.appendChild(t.el);
    }
    for (const w of want) { const t = tiles.get(w.k); if (t.el.parentNode === grid) grid.appendChild(t.el); }
    for (const t of tiles.values()) if (!t.video.hidden && t.video.paused) t.video.play().catch(() => {});
    fa.hidden = !S.focused; $('stage').classList.toggle('focus', !!S.focused);
    $('empty').hidden = S.peers.size > 0;
    renderMembers(); paintMe(); paintSpeaking();
  }
  function paintSpeaking() {
    for (const [k, t] of tiles) t.el.classList.toggle('speaking', !k.endsWith(':screen') && !!(t.who && t.who.speaking));
    document.querySelectorAll('#members .member').forEach((m) => { m.classList.toggle('speaking', !!(m._who && m._who.speaking)); });
  }
  function memberRow(who, prof, st, isMe) {
    const row = document.createElement('div'); row.className = 'member'; row._who = who;
    const nm = document.createElement('span'); nm.className = 'nm'; nm.textContent = prof.name + (isMe ? ' (вы)' : '');
    const isHost = (isMe ? myId : who.id) === S.hostId;
    const ic = document.createElement('span'); ic.className = 'ic';
    ic.textContent = (st.screen ? '🖥 ' : '') + (st.cam ? '📷 ' : '') + (st.deaf ? '🎧̸' : !st.mic ? '🔇' : '') + (!isMe && who.localMute ? ' 🔕' : '');
    row.append(avatar(prof, 32, { dot: true }), nm);
    if (isHost) { const h = document.createElement('span'); h.className = 'host'; h.textContent = '👑'; h.title = 'Ведущий'; row.appendChild(h); }
    row.appendChild(ic);
    if (!isMe && who.pc && who.pc.connectionState !== 'connected') { const c = document.createElement('span'); c.className = 'ic'; c.textContent = '⏳'; c.title = 'Соединяемся'; row.appendChild(c); }
    row.onclick = () => openCard(isMe ? null : who);
    return row;
  }
  function renderMembers() {
    const box = $('members'); box.innerHTML = '';
    box.appendChild(memberRow(me, ZV.publicProfile(), myState(), true));
    for (const p of S.peers.values()) box.appendChild(memberRow(p, p.profile, p.st, false));
    paintSpeaking();
  }
  function paintMe() {
    if (!S.role) return;
    const st = myState();
    $('meAva').innerHTML = ''; $('meAva').appendChild(avatar(profile, 34, { dot: true }));
    $('meName').textContent = profile.name; $('meStatus').textContent = profile.custom || STATUS[profile.status];
    $('btnMute').className = muted || deaf ? 'off' : ''; $('btnMute').textContent = muted || deaf ? '🔇' : '🎙';
    $('btnDeaf').className = deaf ? 'off' : ''; $('btnDeaf').textContent = deaf ? '🎧̸' : '🎧';
    $('btnCam').className = st.cam ? 'on' : ''; $('btnCam').textContent = st.cam ? '📷 Выкл. камеру' : '📷 Камера';
    $('btnScreen').className = st.screen ? 'on' : ''; $('btnScreen').textContent = st.screen ? '🖥 Остановить' : '🖥 Экран';
  }

  /* ============================================================
     Чат звонка
     ============================================================ */
  const chat = []; const chatById = new Map();
  function whoProfile(id) { if (id === myId) return profile; const p = S.peers.get(id); return p ? p.profile : { name: 'Ушёл', avatar: '' }; }
  function addChat(m) {
    if (chatById.has(m.mid)) return;
    m.reacts = new Map(); const pr = whoProfile(m.from); m.name = pr.name; m.avatar = pr.avatar;
    chat.push(m); chatById.set(m.mid, m);
    if (chat.length > 500) { const old = chat.shift(); chatById.delete(old.mid); if (old.el) old.el.remove(); }
    const prev = chat[chat.length - 2];
    m.cont = !!prev && prev.from === m.from && m.ts - prev.ts < 5 * 60000;
    $('msgs').appendChild(renderMsg(m));
    $('msgs').scrollTop = 1e9;
    if (m.from !== myId) { if ($('chat').hidden) $('btnChat').className = 'on'; if (document.hidden && profile.status !== 'dnd') ZV.beep(1000); }
  }
  function renderMsg(m) {
    const el = m.el || document.createElement('div');
    el.className = 'cm' + (m.cont ? ' cont' : ''); el.innerHTML = '';
    const body = document.createElement('div'); body.className = 'body';
    if (!m.cont) {
      const who = document.createElement('div'); who.className = 'who'; who.textContent = m.name;
      const tm = document.createElement('time'); tm.textContent = ZV.fmtTime(m.ts);
      who.appendChild(tm); body.appendChild(who);
    }
    if (m.text) { const tx = document.createElement('div'); tx.className = 'txt'; ZV.linkify(tx, m.text); body.appendChild(tx); }
    if (m.file) {
      if (/^image\/(png|jpeg|gif|webp|bmp)$/.test(m.file.mime)) {
        const img = document.createElement('img'); img.className = 'att'; img.src = m.file.url; img.alt = m.file.name;
        img.onclick = () => ZV.lightbox(m.file.url);
        body.appendChild(img);
      } else {
        const a = document.createElement('a'); a.className = 'file'; a.href = m.file.url; a.download = m.file.name;
        a.textContent = '📄 ' + m.file.name + ' · ' + ZV.fmtSize(m.file.size); body.appendChild(a);
      }
    }
    if (m.reacts.size) {
      const rr = document.createElement('div'); rr.className = 'reacts';
      for (const [emo, set] of m.reacts) {
        if (!set.size) continue;
        const b = document.createElement('button'); b.textContent = emo + ' ' + set.size;
        if (set.has(myId)) b.className = 'mine';
        b.onclick = () => myReact(m.mid, emo); rr.appendChild(b);
      }
      body.appendChild(rr);
    }
    const add = document.createElement('button'); add.className = 'addReact'; add.textContent = '☺+'; add.title = 'Реакция';
    add.onclick = (e) => { e.stopPropagation(); ZV.showPicker(el, (emo) => myReact(m.mid, emo)); };
    el.append(avatar({ name: m.name, avatar: m.avatar }, 36), body, add);
    m.el = el; return el;
  }
  function renderChat() { for (const m of chat) { const pr = whoProfile(m.from); if (pr.name !== 'Ушёл') { m.name = pr.name; m.avatar = pr.avatar; } renderMsg(m); } }
  function myReact(mid, emo) { const m = chatById.get(mid); if (!m) return; const on = !(m.reacts.get(emo) || new Set()).has(myId); toggleReact(mid, emo, myId, on); sendAll({ t: 'react', mid, emoji: emo, on }); }
  function toggleReact(mid, emo, who, on) {
    const m = chatById.get(mid); if (!m || !ZV.EMOJI.includes(emo)) return;
    const set = m.reacts.get(emo) || new Set(); if (on) set.add(who); else set.delete(who); m.reacts.set(emo, set);
    renderMsg(m);
  }
  function addSys(text) { const d = document.createElement('div'); d.className = 'sys'; d.textContent = text; $('msgs').appendChild(d); $('msgs').scrollTop = 1e9; }

  $('chatForm').onsubmit = (e) => {
    e.preventDefault();
    const text = $('chatInput').value.trim(); if (!text) return;
    if (!S.peers.size) { addSys('В комнате пока никого нет'); return; }
    const mid = rid(); sendAll({ t: 'msg', mid, text }); addChat({ mid, from: myId, text, ts: Date.now() }); $('chatInput').value = '';
  };
  $('btnAttach').onclick = () => $('fileInput').click();
  $('fileInput').onchange = (e) => { sendFiles([...e.target.files]); e.target.value = ''; };
  $('chatInput').addEventListener('paste', (e) => { const fs = [...((e.clipboardData && e.clipboardData.files) || [])]; if (fs.length) { e.preventDefault(); sendFiles(fs); } });
  $('chat').addEventListener('dragover', (e) => { e.preventDefault(); $('chat').classList.add('drag'); });
  $('chat').addEventListener('dragleave', () => $('chat').classList.remove('drag'));
  $('chat').addEventListener('drop', (e) => { e.preventDefault(); $('chat').classList.remove('drag'); sendFiles([...e.dataTransfer.files]); });

  /* ============================================================
     Карточка участника в звонке
     ============================================================ */
  let cardPeer = null;
  function openCard(p) {
    cardPeer = p;
    const pr = p ? p.profile : ZV.publicProfile();
    $('pcHead').innerHTML = '';
    $('pcHead').appendChild(ZV.profileCard(pr, { extra: (p ? p.id : myId) === S.hostId ? '👑 ведущий звонка' : '' }));
    $('pcVolRow').hidden = !p;
    $('pcKick').hidden = !p || S.role !== 'host';
    if (p) {
      $('pcVol').value = Math.round(p.vol * 100); $('pcVolVal').textContent = $('pcVol').value + '%';
      $('pcSVol').value = Math.round(p.svol * 100); $('pcSVolVal').textContent = $('pcSVol').value + '%';
      $('pcMute').checked = p.localMute;
    }
    $('pcard').hidden = false;
  }
  $('pcVol').oninput = () => { if (!cardPeer) return; cardPeer.vol = $('pcVol').value / 100; $('pcVolVal').textContent = $('pcVol').value + '%'; applyVolumes(cardPeer); savePeerVolume(cardPeer); };
  $('pcSVol').oninput = () => { if (!cardPeer) return; cardPeer.svol = $('pcSVol').value / 100; $('pcSVolVal').textContent = $('pcSVol').value + '%'; applyVolumes(cardPeer); savePeerVolume(cardPeer); };
  $('pcMute').onchange = () => { if (!cardPeer) return; cardPeer.localMute = $('pcMute').checked; applyVolumes(cardPeer); savePeerVolume(cardPeer); render(); };
  $('pcKick').onclick = () => {
    if (!cardPeer || S.role !== 'host') return;
    const id = cardPeer.id; $('pcard').hidden = true;
    signal({ t: 'kick', to: id }); S.roster.delete(id); broadcastRoster(); addSys(cardPeer.profile.name + ' удалён(а) из комнаты');
  };
  $('pcClose').onclick = () => { $('pcard').hidden = true; };
  $('pcard').onclick = (e) => { if (e.target === $('pcard')) $('pcard').hidden = true; };

  /* ============================================================
     Настройки: голос, видео, демонстрация, приватность
     (вкладки «Профиль» и «Аккаунт» — в app.js)
     ============================================================ */
  function showTab(name) {
    document.querySelectorAll('#settings [data-tab]').forEach((b) => b.classList.toggle('active', b.dataset.tab === name));
    document.querySelectorAll('#settings [data-pane]').forEach((s) => { s.hidden = s.dataset.pane !== name; });
    if (name === 'voice') openVoiceTab(); else stopVoiceTab();
    document.dispatchEvent(new CustomEvent('zv:tab', { detail: name }));
  }
  document.querySelectorAll('#settings [data-tab]').forEach((b) => { b.onclick = () => showTab(b.dataset.tab); });
  function openSettings(tab) {
    const dev = loadDev();
    $('noiseSup').checked = dev.noise !== false;
    $('inMode').value = dev.inMode === 'ptt' ? 'ptt' : 'voice';
    $('pttRow').hidden = $('pttHint').hidden = $('inMode').value !== 'ptt';
    $('pttKey').textContent = keyName(dev.pttKey);
    $('screenQ').value = SCREEN_Q[dev.screenQ] ? dev.screenQ : 'smooth';
    $('hideIp').checked = !!dev.hideIp;
    $('settings').hidden = false;
    showTab(tab || 'profile');
  }
  ZV.openSettings = openSettings;

  let meterRaf = 0, previewTrack = null;
  async function openVoiceTab() {
    if (!localStream) { await ensureLocal(); attachMeter(me, new MediaStream([micTrack()])); }
    await fillDevices();
    cancelAnimationFrame(meterRaf);
    const bar = $('micLevel').firstElementChild;
    (function tick() { bar.style.width = Math.min(100, level(me) / 30 * 100) + '%'; meterRaf = requestAnimationFrame(tick); })();
    if (camOn()) { $('preview').srcObject = new MediaStream([camTrack()]); $('btnPreview').hidden = true; }
    else { $('preview').srcObject = null; $('btnPreview').hidden = false; $('btnPreview').textContent = 'Проверить камеру'; }
  }
  function stopVoiceTab() {
    cancelAnimationFrame(meterRaf);
    if (previewTrack) { previewTrack.stop(); previewTrack = null; }
    $('preview').srcObject = null;
    // Не в звонке — микрофон держать незачем
    if (!S.role && localStream) { localStream.getTracks().forEach((t) => t.stop()); localStream = null; detachMeter(me); }
  }
  $('btnPreview').onclick = async () => {
    if (previewTrack) { previewTrack.stop(); previewTrack = null; $('preview').srcObject = null; $('btnPreview').textContent = 'Проверить камеру'; return; }
    previewTrack = await getCam();
    if (!previewTrack) { $('btnPreview').textContent = 'Камера не найдена'; return; }
    $('preview').srcObject = new MediaStream([previewTrack]); $('btnPreview').textContent = 'Выключить предпросмотр';
    fillDevices();
  };
  async function fillDevices() {
    let list = [];
    try { list = await navigator.mediaDevices.enumerateDevices(); } catch (e) {}
    const dev = loadDev();
    const cur = (t) => (t && !t._blank && t.getSettings().deviceId) || '';
    const fill = (sel, kind, chosen, none, word) => {
      const items = list.filter((d) => d.kind === kind);
      sel.innerHTML = '';
      if (!items.length) { sel.add(new Option(none, '')); sel.disabled = true; return; }
      sel.disabled = false;
      items.forEach((d, i) => sel.add(new Option(d.label || (word + ' ' + (i + 1)), d.deviceId)));
      if (chosen && items.some((d) => d.deviceId === chosen)) sel.value = chosen;
    };
    fill($('micSel'), 'audioinput', cur(micTrack()) || dev.mic, 'Микрофонов нет', 'Микрофон');
    fill($('camSel'), 'videoinput', cur(camTrack()) || cur(previewTrack) || dev.cam, 'Камер нет', 'Камера');
    const canSink = 'setSinkId' in HTMLMediaElement.prototype;
    fill($('spkSel'), 'audiooutput', dev.spk, canSink ? 'Устройство по умолчанию' : 'Выбор недоступен в этом браузере', 'Динамики');
    if (!canSink) $('spkSel').disabled = true;
  }
  async function reMic() { const t = await getMic(); if (!t) { ZV.toast('Не удалось включить микрофон'); return; } replaceLocal('audio', t); }
  $('micSel').onchange = (e) => { saveDev({ mic: e.target.value }); reMic(); };
  $('noiseSup').onchange = (e) => { saveDev({ noise: e.target.checked }); reMic(); };
  $('camSel').onchange = async (e) => {
    saveDev({ cam: e.target.value });
    if (camOn()) { const t = await getCam(); if (t) replaceLocal('video', t); }
    else if (previewTrack) { previewTrack.stop(); previewTrack = await getCam(); $('preview').srcObject = previewTrack ? new MediaStream([previewTrack]) : null; }
  };
  $('spkSel').onchange = (e) => { saveDev({ spk: e.target.value }); document.querySelectorAll('audio').forEach((a) => { if (a.setSinkId) a.setSinkId(e.target.value).catch(() => {}); }); };
  $('inMode').onchange = (e) => { saveDev({ inMode: e.target.value }); $('pttRow').hidden = $('pttHint').hidden = e.target.value !== 'ptt'; if (e.target.value === 'ptt' && !loadDev().pttKey) saveDev({ pttKey: 'v' }); $('pttKey').textContent = keyName(loadDev().pttKey); applyMic(); };
  $('btnPttKey').onclick = () => { capturingKey = true; $('btnPttKey').textContent = 'Нажмите клавишу…'; };
  $('screenQ').onchange = (e) => { saveDev({ screenQ: e.target.value }); retuneAll(); };
  $('hideIp').onchange = (e) => saveDev({ hideIp: e.target.checked });
  $('btnSpkTest').onclick = () => {
    const ctx = ZV.actx(); const dst = ctx.createMediaStreamDestination(); const o = ctx.createOscillator(); const g = ctx.createGain();
    o.frequency.value = 660; g.gain.value = 0.15; o.connect(g).connect(dst);
    const a = new Audio(); a.srcObject = dst.stream;
    const id = loadDev().spk; if (id && a.setSinkId) a.setSinkId(id).catch(() => {});
    a.play().catch(() => {}); o.start(); o.stop(ctx.currentTime + 0.7);
    setTimeout(() => { a.srcObject = null; }, 800);
  };
  $('btnMicTest').onclick = () => {
    const t = micTrack(), btn = $('btnMicTest');
    if (!t || t._blank || !window.MediaRecorder) { btn.textContent = 'Микрофон не найден'; return; }
    const wasEnabled = t.enabled; t.enabled = true;
    const rec = new MediaRecorder(new MediaStream([t])); const parts = [];
    rec.ondataavailable = (e) => { if (e.data.size) parts.push(e.data); };
    rec.onstop = () => {
      t.enabled = wasEnabled; applyMic();
      const url = URL.createObjectURL(new Blob(parts, { type: rec.mimeType }));
      const a = new Audio(url); const id = loadDev().spk; if (id && a.setSinkId) a.setSinkId(id).catch(() => {});
      btn.textContent = '🔊 Воспроизведение…';
      a.onended = a.onerror = () => { URL.revokeObjectURL(url); btn.disabled = false; btn.textContent = '⏺ Записать 3 сек и прослушать'; };
      a.play().catch(() => a.onended());
    };
    btn.disabled = true; btn.textContent = '🔴 Говорите… 3'; rec.start();
    let n = 3; const iv = setInterval(() => { n--; if (n > 0) btn.textContent = '🔴 Говорите… ' + n; else { clearInterval(iv); rec.stop(); } }, 1000);
  };
  function closeSettings() { stopVoiceTab(); $('settings').hidden = true; document.dispatchEvent(new CustomEvent('zv:tab', { detail: '' })); }
  $('btnSettingsClose').onclick = closeSettings;
  $('settings').onclick = (e) => { if (e.target === $('settings')) closeSettings(); };
  if (navigator.mediaDevices && navigator.mediaDevices.addEventListener) navigator.mediaDevices.addEventListener('devicechange', () => { if (!$('settings').hidden) fillDevices(); });

  /* ============================================================
     Экран звонка
     ============================================================ */
  function showCall() {
    $('app').hidden = true; $('call').hidden = false;
    $('roomTitle').textContent = S.title;
    $('roomCode').textContent = 'код: ' + S.code + (S.pass ? ' · с паролем' : '');
    const link = /^https?:$/.test(location.protocol) ? location.origin + location.pathname + '?room=' + S.code : S.code;
    $('btnInvite').onclick = () => {
      const ok = () => { $('btnInvite').textContent = '✓ Скопировано'; setTimeout(() => { $('btnInvite').textContent = '🔗 Пригласить'; }, 1500); };
      if (navigator.clipboard) navigator.clipboard.writeText(link).then(ok, () => addSys('Ссылка: ' + link)); else addSys('Ссылка: ' + link);
      if (S.pass) addSys('Не забудьте отдельно сообщить пароль: ' + S.pass);
    };
    $('btnScreen').hidden = !navigator.mediaDevices.getDisplayMedia;
    if (matchMedia('(max-width:900px)').matches) { $('chat').hidden = true; $('side').hidden = true; }
    netState();
  }
  $('btnMute').onclick = toggleMute;
  $('btnDeaf').onclick = toggleDeaf;
  $('btnSettings').onclick = () => openSettings('voice');
  $('btnCam').onclick = toggleCam;
  $('btnScreen').onclick = toggleScreen;
  $('btnHang').onclick = hangUp;
  $('btnChat').onclick = () => { $('chat').hidden = !$('chat').hidden; $('btnChat').className = ''; };
  $('btnSide').onclick = () => { $('side').hidden = !$('side').hidden; };
  window.addEventListener('beforeunload', () => sendAll({ t: 'bye' }));

  /* Что нужно мессенджеру */
  window.Call = {
    start,
    active: () => !!S.role,
    profileChanged: () => { sendAll({ t: 'profile', profile: ZV.publicProfile() }); if (S.role === 'host') S.roster.set(myId, profile.name); render(); renderChat(); },
  };
})();
