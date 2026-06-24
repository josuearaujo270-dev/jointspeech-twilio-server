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

// ── Twilio webhook: answers the call, opens a bidirectional media stream ──
app.post('/voice', (req, res) => {
  if (req.body?.CallSid) {
    pendingCallMeta.set(req.body.CallSid, { fromNumber: req.body.From || null });
  }
  const wsUrl = `wss://${PUBLIC_HOST}/media`;
  res.type('text/xml');
  res.send(`<?xml version="1.0" encoding="UTF-8"?>
<Response>
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
const wss = new WebSocketServer({ server, path: '/media' });

wss.on('connection', (twilioWs) => {
  let callSid = null;
  let streamSid = null;
  let deepgramWs = null;

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
          if (dgMsg.type !== 'Results') return;
          const alt = dgMsg.channel?.alternatives?.[0];
          const transcript = alt?.transcript || '';
          const isFinal = dgMsg.is_final;
          if (!transcript || !isFinal) return;

          const langCode = alt?.languages?.[0];
          const langName = LANGUAGE_NAMES[langCode] || null;
          const call = activeCalls.get(callSid);
          if (call && langName) {
            call.languageCode = langCode;
            call.languageName = langName;
          }

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

      // Preserve transcript history if this callSid somehow gets a second "start"
      // event (e.g. a media stream reconnect) instead of wiping it clean.
      const existingCall = activeCalls.get(callSid);
      if (existingCall) {
        existingCall.twilioWs = twilioWs;
        existingCall.streamSid = streamSid;
      } else {
        const meta = pendingCallMeta.get(callSid);
        pendingCallMeta.delete(callSid);
        activeCalls.set(callSid, {
          twilioWs,
          streamSid,
          startedAt: Date.now(),
          languageCode: null,
          languageName: null,
          fromNumber: meta?.fromNumber || null,
          transcriptLog: [],
        });
      }
      console.log(`[${callSid}] Call started, streamSid=${streamSid}`);
    }

    if (msg.event === 'media') {
      if (deepgramWs && deepgramWs.readyState === 1) {
        deepgramWs.send(Buffer.from(msg.media.payload, 'base64'));
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
