import express from 'express';
import { WebSocketServer } from 'ws';
import http from 'http';
import fetch from 'node-fetch';

const PORT = process.env.PORT || 8080;
const DEEPGRAM_API_KEY = process.env.DEEPGRAM_API_KEY;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const ELEVENLABS_API_KEY = process.env.ELEVENLABS_API_KEY;
const PUBLIC_HOST = process.env.PUBLIC_HOST; // e.g. jointspeech-twilio.up.railway.app
const APP_CALLBACK_URL = process.env.APP_CALLBACK_URL; // e.g. https://jointspeech.com/api/twilio/save-call

const LANGUAGE_NAMES = {
  en: 'English', es: 'Spanish', fr: 'French', pt: 'Portuguese', zh: 'Chinese',
  hi: 'Hindi', ar: 'Arabic', ru: 'Russian', ja: 'Japanese', ko: 'Korean',
  de: 'German', it: 'Italian', pl: 'Polish', vi: 'Vietnamese', tr: 'Turkish',
  ur: 'Urdu', fa: 'Farsi', ro: 'Romanian', nl: 'Dutch', el: 'Greek', uk: 'Ukrainian',
};

// Matches whatever Twilio's speech recognition heard (e.g. "Spanish", "I think French")
// against our supported language names. Returns null if nothing recognizable was said.
function matchLanguageName(spoken) {
  if (!spoken) return null;
  const lower = spoken.toLowerCase();
  for (const name of Object.values(LANGUAGE_NAMES)) {
    if (lower.includes(name.toLowerCase())) return name;
  }
  return null;
}

// So the customer (not just the rep) understands what's happening — Twilio's own
// <Say> voices handle this natively, no extra translation API call/latency needed.
const CUSTOMER_INSTRUCTIONS = {
  Spanish: { sayLang: 'es-MX', text: 'Por favor hable en oraciones completas. Puede haber un breve retraso mientras traducimos.' },
  French: { sayLang: 'fr-FR', text: 'Veuillez parler en phrases complètes. Il pourrait y avoir un léger délai pendant la traduction.' },
  Portuguese: { sayLang: 'pt-BR', text: 'Por favor, fale em frases completas. Pode haver um pequeno atraso durante a tradução.' },
  Chinese: { sayLang: 'zh-CN', text: '请说完整的句子。翻译时可能会有短暂的延迟。' },
  Hindi: { sayLang: 'hi-IN', text: 'कृपया पूरे वाक्यों में बोलें। अनुवाद करते समय थोड़ी देरी हो सकती है।' },
  Arabic: { sayLang: 'ar-XA', text: 'يرجى التحدث بجمل كاملة. قد يكون هناك تأخير قصير أثناء الترجمة.' },
  Russian: { sayLang: 'ru-RU', text: 'Пожалуйста, говорите полными предложениями. Во время перевода может быть небольшая задержка.' },
  Japanese: { sayLang: 'ja-JP', text: '完全な文でお話しください。翻訳中に少し遅れが生じる場合があります。' },
  Korean: { sayLang: 'ko-KR', text: '완전한 문장으로 말씀해 주세요. 번역 중에 약간의 지연이 있을 수 있습니다.' },
  German: { sayLang: 'de-DE', text: 'Bitte sprechen Sie in vollständigen Sätzen. Es kann zu einer kurzen Verzögerung bei der Übersetzung kommen.' },
  Italian: { sayLang: 'it-IT', text: 'Si prega di parlare con frasi complete. Potrebbe esserci un breve ritardo durante la traduzione.' },
  Polish: { sayLang: 'pl-PL', text: 'Proszę mówić pełnymi zdaniami. Podczas tłumaczenia może wystąpić niewielkie opóźnienie.' },
  Vietnamese: { sayLang: 'vi-VN', text: 'Vui lòng nói thành câu hoàn chỉnh. Có thể có một chút chậm trễ trong khi chúng tôi dịch.' },
  Turkish: { sayLang: 'tr-TR', text: 'Lütfen tam cümlelerle konuşun. Çeviri sırasında kısa bir gecikme olabilir.' },
  Romanian: { sayLang: 'ro-RO', text: 'Vă rugăm să vorbiți în propoziții complete. Poate exista o scurtă întârziere în timpul traducerii.' },
  Dutch: { sayLang: 'nl-NL', text: 'Spreek in volledige zinnen. Er kan een korte vertraging zijn tijdens het vertalen.' },
  Greek: { sayLang: 'el-GR', text: 'Παρακαλώ μιλήστε με ολόκληρες προτάσεις. Μπορεί να υπάρξει μικρή καθυστέρηση κατά τη μετάφραση.' },
  Ukrainian: { sayLang: 'uk-UA', text: 'Будь ласка, говоріть повними реченнями. Під час перекладу може бути невелика затримка.' },
};

const app = express();

// Allow the JointSpeech web app (running on a different domain) to poll these endpoints
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});
app.use(express.urlencoded({ extended: false }));
app.use(express.json());

// callSid -> { twilioWs, streamSid, startedAt, languageCode, languageName, fromNumber, transcriptLog: [] }
const activeCalls = new Map();

// callSid -> { fromNumber } — captured from the /voice webhook before the media stream's
// "start" event arrives, then merged into activeCalls once we have a WS connection for it.
const pendingCallMeta = new Map();

// ── Twilio webhook: answers the call, asks whoever connected to say the customer's
// language (so we don't have to guess), then opens the bidirectional media stream ──
app.post('/voice', (req, res) => {
  console.log(`[${req.body?.CallSid}] /voice hit, From=${req.body?.From}`);
  if (req.body?.CallSid) {
    pendingCallMeta.set(req.body.CallSid, { fromNumber: req.body.From || null });
  }
  res.type('text/xml');
  res.send(`<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Gather input="speech" speechTimeout="auto" action="/voice/language" method="POST">
    <Say>JointSpeech connected. Please say the customer's language now.</Say>
  </Gather>
  <Redirect method="POST">/voice/language</Redirect>
</Response>`);
});

app.post('/voice/language', (req, res) => {
  const callSid = req.body?.CallSid;
  console.log(`[${callSid}] /voice/language hit, SpeechResult="${req.body?.SpeechResult}"`);
  const matchedLang = matchLanguageName(req.body?.SpeechResult);

  if (callSid) {
    const meta = pendingCallMeta.get(callSid) || {};
    pendingCallMeta.set(callSid, { ...meta, presetLanguageName: matchedLang });
  }

  const wsUrl = `wss://${PUBLIC_HOST}/media`;
  const confirmMsg = matchedLang
    ? `Connecting you now for ${matchedLang}. Please speak in complete sentences — there may be a short delay while we translate.`
    : `Connecting you now. Please speak in complete sentences — there may be a short delay while we translate.`;
  const customerInstructions = matchedLang ? CUSTOMER_INSTRUCTIONS[matchedLang] : null;
  const customerSay = customerInstructions
    ? `\n  <Say language="${customerInstructions.sayLang}">${customerInstructions.text}</Say>`
    : '';

  res.type('text/xml');
  res.send(`<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Say>${confirmMsg}</Say>${customerSay}
  <Connect>
    <Stream url="${wsUrl}" />
  </Connect>
</Response>`);
});

// ── Agent dashboard: list calls currently in progress ──
app.get('/calls', (req, res) => {
  const calls = Array.from(activeCalls.entries()).map(([callSid, call]) => ({
    callSid,
    startedAt: call.startedAt,
    languageName: call.languageName || 'Detecting…',
    lastTranscript: call.transcriptLog.length ? call.transcriptLog[call.transcriptLog.length - 1] : null,
  }));
  res.json({ calls });
});

// ── Agent dashboard: full live transcript for one call ──
app.get('/calls/:callSid', (req, res) => {
  const call = activeCalls.get(req.params.callSid);
  if (!call) return res.status(404).json({ error: 'Call not found or already ended' });
  res.json({
    callSid: req.params.callSid,
    startedAt: call.startedAt,
    languageName: call.languageName || 'Detecting…',
    transcriptLog: call.transcriptLog,
  });
});

// ── Agent's app calls this to speak a reply into the live call ──
// Agent always types in English; we translate to the customer's detected language before TTS.
app.post('/speak/:callSid', async (req, res) => {
  const { callSid } = req.params;
  const { text, voiceId } = req.body;
  const call = activeCalls.get(callSid);
  if (!call) return res.status(404).json({ error: 'Call not found or already ended' });
  if (!text || !text.trim()) return res.status(400).json({ error: 'No text provided' });

  try {
    let spokenText = text;
    const targetLang = call.languageName;
    if (targetLang && targetLang !== 'English') {
      const translateRes = await fetch('https://api.openai.com/v1/responses', {
        method: 'POST',
        headers: { Authorization: `Bearer ${OPENAI_API_KEY}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({
          model: 'gpt-4.1',
          input: `Translate this into natural, polite ${targetLang} for a customer service phone call. Only return the ${targetLang} translation.\n\n${text}`,
        }),
      });
      const translateData = await translateRes.json();
      spokenText = translateData.output?.[0]?.content?.[0]?.text || text;
    }

    const elRes = await fetch(`https://api.elevenlabs.io/v1/text-to-speech/${voiceId || '21m00Tcm4TlvDq8ikWAM'}?output_format=ulaw_8000`, {
      method: 'POST',
      headers: { 'xi-api-key': ELEVENLABS_API_KEY, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        text: spokenText,
        model_id: 'eleven_multilingual_v2',
        voice_settings: { stability: 0.45, similarity_boost: 0.75, style: 0.3, use_speaker_boost: true },
      }),
    });
    if (!elRes.ok) {
      const err = await elRes.text();
      return res.status(500).json({ error: `TTS failed: ${err}` });
    }
    const audioBuffer = Buffer.from(await elRes.arrayBuffer());
    const base64Audio = audioBuffer.toString('base64');

    call.twilioWs.send(JSON.stringify({
      event: 'media',
      streamSid: call.streamSid,
      media: { payload: base64Audio },
    }));

    call.transcriptLog.push({ speaker: 'agent', transcript: text, translation: spokenText, time: Date.now() });

    res.json({ ok: true, spokenText });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

const server = http.createServer(app);

// Two WS paths share this HTTP server (Twilio's media stream + the agent dashboard's
// listen-live stream). Attaching multiple `{ server, path }` WebSocketServer instances
// directly can corrupt frames on the other path, so both run in noServer mode and we
// route the single 'upgrade' event manually — the pattern the ws library itself recommends.
const wss = new WebSocketServer({ noServer: true, perMessageDeflate: false });
const listenWss = new WebSocketServer({ noServer: true, perMessageDeflate: false });

server.on('upgrade', (req, socket, head) => {
  const { pathname } = new URL(req.url, 'http://internal');
  if (pathname === '/media') {
    wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, req));
  } else if (pathname === '/listen') {
    listenWss.handleUpgrade(req, socket, head, (ws) => listenWss.emit('connection', ws, req));
  } else {
    socket.destroy();
  }
});

// ── Agent dashboard: listen live to a call's raw audio (one-way, no agent mic) ──
listenWss.on('connection', (listenerWs, req) => {
  const callSid = new URL(req.url, 'http://internal').searchParams.get('callSid');
  const call = callSid ? activeCalls.get(callSid) : null;
  if (!call) {
    listenerWs.close(4404, 'Call not found or already ended');
    return;
  }
  call.listeners.add(listenerWs);
  listenerWs.on('close', () => call.listeners.delete(listenerWs));
});

wss.on('connection', (twilioWs) => {
  let callSid = null;
  let streamSid = null;
  let deepgramWs = null;
  let mediaCount = 0;

  twilioWs.on('message', async (raw) => {
    const msg = JSON.parse(raw.toString());

    if (msg.event === 'start') {
      callSid = msg.start.callSid;
      streamSid = msg.start.streamSid;

      // Open Deepgram streaming connection matched to Twilio's raw mulaw 8kHz audio format
      const dgUrl = 'wss://api.deepgram.com/v1/listen?model=nova-2&encoding=mulaw&sample_rate=8000&channels=1&multichannel=false&language=multi&interim_results=true&punctuate=true&endpointing=800';
      deepgramWs = new (await import('ws')).WebSocket(dgUrl, ['token', DEEPGRAM_API_KEY]);

      deepgramWs.on('open', () => {
        console.log(`[${callSid}] Deepgram connected`);
      });

      deepgramWs.on('message', async (dgRaw) => {
        try {
          const dgMsg = JSON.parse(dgRaw.toString());
          if (dgMsg.type !== 'Results') {
            console.log(`[${callSid}] Deepgram non-Results message: ${dgRaw.toString().slice(0, 200)}`);
            return;
          }
          const alt = dgMsg.channel?.alternatives?.[0];
          const transcript = alt?.transcript || '';
          const isFinal = dgMsg.is_final;
          if (transcript) {
            console.log(`[${callSid}] Deepgram Results (isFinal=${isFinal}): "${transcript}"`);
          }
          if (!transcript || !isFinal) return;

          const langCode = alt?.languages?.[0];
          const detectedLangName = LANGUAGE_NAMES[langCode] || null;
          const call = activeCalls.get(callSid);
          // Once a call locks onto a real foreign language (whether from the rep saying
          // it up front, or from an earlier detection), don't let a single stray English
          // misdetection (numbers, names, short phrases often get heard as English) flip
          // it back — only a genuinely different foreign language should.
          const lockedToForeign = call?.languageName && call.languageName !== 'English';
          if (call && detectedLangName && !(lockedToForeign && detectedLangName === 'English')) {
            call.languageCode = langCode;
            call.languageName = detectedLangName;
          }

          // Short utterances often don't carry their own language tag — fall back to
          // the call's known language (preset or previously locked) instead of skipping
          // translation entirely.
          const langName = detectedLangName || call?.languageName || null;

          // Translate to English via GPT (skip if already English)
          let translation = transcript;
          if (langName && langName !== 'English') {
            const translateRes = await fetch('https://api.openai.com/v1/responses', {
              method: 'POST',
              headers: { Authorization: `Bearer ${OPENAI_API_KEY}`, 'Content-Type': 'application/json' },
              body: JSON.stringify({
                model: 'gpt-4.1',
                input: `Translate this from ${langName} into natural, clear English. Only return the translation.\n\n${transcript}`,
              }),
            });
            const translateData = await translateRes.json();
            translation = translateData.output?.[0]?.content?.[0]?.text || transcript;
          }

          console.log(`[${callSid}] Customer (${langName || 'unknown'}): ${transcript} -> ${translation}`);

          if (call) {
            call.transcriptLog.push({ speaker: 'customer', transcript, translation, time: Date.now() });
          }
        } catch (err) {
          console.error('Deepgram message handling error', err);
        }
      });

      deepgramWs.on('error', (err) => console.error(`[${callSid}] Deepgram error`, err));
      deepgramWs.on('close', (code, reason) => console.log(`[${callSid}] Deepgram closed: code=${code} reason=${reason}`));

      // Preserve transcript history if this callSid somehow gets a second "start"
      // event (e.g. a media stream reconnect) instead of wiping it clean.
      const existingCall = activeCalls.get(callSid);
      if (existingCall) {
        existingCall.twilioWs = twilioWs;
        existingCall.streamSid = streamSid;
        existingCall.deepgramWs = deepgramWs;
      } else {
        const meta = pendingCallMeta.get(callSid);
        pendingCallMeta.delete(callSid);
        activeCalls.set(callSid, {
          twilioWs,
          streamSid,
          deepgramWs,
          startedAt: Date.now(),
          languageCode: null,
          languageName: meta?.presetLanguageName || null,
          fromNumber: meta?.fromNumber || null,
          transcriptLog: [],
          listeners: new Set(),
        });
      }
      console.log(`[${callSid}] Call started, streamSid=${streamSid}`);
    }

    if (msg.event === 'media') {
      mediaCount++;
      if (mediaCount % 100 === 0) {
        console.log(`[${callSid}] Received ${mediaCount} audio chunks so far, deepgramWs.readyState=${deepgramWs?.readyState}`);
      }
      if (deepgramWs && deepgramWs.readyState === 1) {
        deepgramWs.send(Buffer.from(msg.media.payload, 'base64'));
      }
      // Relay the same raw call audio to any agents listening live in the dashboard
      const call = activeCalls.get(callSid);
      if (call && call.listeners.size) {
        const payload = JSON.stringify({ event: 'media', payload: msg.media.payload });
        for (const listenerWs of call.listeners) {
          if (listenerWs.readyState === 1) listenerWs.send(payload);
        }
      }
    }

    if (msg.event === 'stop') {
      console.log(`[${callSid}] Call ended`);
      if (deepgramWs) deepgramWs.close();
      finalizeCall(callSid);
    }
  });

  twilioWs.on('close', () => {
    if (deepgramWs) deepgramWs.close();
    finalizeCall(callSid);
  });
});

// Saves the finished call to JointSpeech (Supabase) so it shows up in Saved Calls /
// Memory Library, then removes it from in-memory active-call tracking. Guarded so the
// "stop" event and the WS "close" event (both of which fire per call) only save once.
async function finalizeCall(callSid) {
  if (!callSid) return;
  const call = activeCalls.get(callSid);
  if (!call) return;
  activeCalls.delete(callSid);

  for (const listenerWs of call.listeners) {
    try { listenerWs.close(4410, 'Call ended'); } catch {}
  }

  if (!APP_CALLBACK_URL || call.transcriptLog.length === 0) return;

  try {
    await fetch(APP_CALLBACK_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        callSid,
        fromNumber: call.fromNumber,
        languageName: call.languageName,
        startedAt: call.startedAt,
        endedAt: Date.now(),
        transcriptLog: call.transcriptLog,
      }),
    });
  } catch (err) {
    console.error(`[${callSid}] Failed to save call`, err);
  }
}

server.listen(PORT, () => {
  console.log(`JointSpeech Twilio server listening on port ${PORT}`);
});

// Railway sends SIGTERM on every redeploy. Without this, in-progress Deepgram
// connections get abandoned mid-stream instead of closed, which can leave them
// counted as active on Deepgram's side until they time out on their own.
function shutdownGracefully(signal) {
  console.log(`Received ${signal}, closing ${activeCalls.size} active call(s) cleanly...`);
  for (const call of activeCalls.values()) {
    try { call.deepgramWs?.close(); } catch {}
    try { call.twilioWs?.close(); } catch {}
    for (const listenerWs of call.listeners) {
      try { listenerWs.close(); } catch {}
    }
  }
  server.close(() => process.exit(0));
  // Failsafe in case something hangs and close() never fires
  setTimeout(() => process.exit(0), 2000);
}

process.on('SIGTERM', () => shutdownGracefully('SIGTERM'));
process.on('SIGINT', () => shutdownGracefully('SIGINT'));
