import express from 'express';
import { WebSocketServer } from 'ws';
import http from 'http';
import fetch from 'node-fetch';

const PORT = process.env.PORT || 8080;
const DEEPGRAM_API_KEY = process.env.DEEPGRAM_API_KEY;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const ELEVENLABS_API_KEY = process.env.ELEVENLABS_API_KEY;
const PUBLIC_HOST = process.env.PUBLIC_HOST;
const APP_CALLBACK_URL = process.env.APP_CALLBACK_URL;

const app = express();
app.use(express.urlencoded({ extended: false }));

const activeCalls = new Map();

app.post('/voice', (req, res) => {
  const wsUrl = `wss://${PUBLIC_HOST}/media`;
  res.type('text/xml');
  res.send(`<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <Stream url="${wsUrl}" />
  </Connect>
</Response>`);
});

app.use(express.json());
app.post('/speak/:callSid', async (req, res) => {
  const { callSid } = req.params;
  const { text, voiceId } = req.body;
  const call = activeCalls.get(callSid);
  if (!call) return res.status(404).json({ error: 'Call not found or already ended' });

  try {
    const elRes = await fetch(`https://api.elevenlabs.io/v1/text-to-speech/${voiceId || '21m00Tcm4TlvDq8ikWAM'}?output_format=ulaw_8000`, {
      method: 'POST',
      headers: { 'xi-api-key': ELEVENLABS_API_KEY, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        text,
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

    res.json({ ok: true });
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
  let mediaCount = 0;

  twilioWs.on('message', async (raw) => {
    const msg = JSON.parse(raw.toString());

    if (msg.event === 'start') {
      callSid = msg.start.callSid;
      streamSid = msg.start.streamSid;

      const dgUrl = 'wss://api.deepgram.com/v1/listen?model=nova-2&encoding=mulaw&sample_rate=8000&language=multi&interim_results=true&punctuate=true&endpointing=800';
      deepgramWs = new (await import('ws')).WebSocket(dgUrl, ['token', DEEPGRAM_API_KEY]);

      deepgramWs.on('open', () => {
        console.log(`[${callSid}] Deepgram connected`);
      });

      deepgramWs.on('message', async (dgRaw) => {
        try {
          const dgMsg = JSON.parse(dgRaw.toString());
          console.log(`[${callSid}] Deepgram raw: ${dgRaw.toString().slice(0, 300)}`);
          if (dgMsg.type !== 'Results') return;
          const transcript = dgMsg.channel?.alternatives?.[0]?.transcript || '';
          const isFinal = dgMsg.is_final;
          if (!transcript || !isFinal) return;

          const translateRes = await fetch('https://api.openai.com/v1/responses', {
            method: 'POST',
            headers: { Authorization: `Bearer ${OPENAI_API_KEY}`, 'Content-Type': 'application/json' },
            body: JSON.stringify({
              model: 'gpt-4.1',
              input: `Translate this to natural, clear English. Only return the translation.\n\n${transcript}`,
            }),
          });
          const translateData = await translateRes.json();
          const translation = translateData.output?.[0]?.content?.[0]?.text || transcript;

          console.log(`[${callSid}] Customer: ${transcript} -> ${translation}`);

          if (APP_CALLBACK_URL) {
            fetch(APP_CALLBACK_URL, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ callSid, transcript, translation }),
            }).catch(() => {});
          }
        } catch (err) {
          console.error('Deepgram message handling error', err);
        }
      });

      deepgramWs.on('error', (err) => console.error(`[${callSid}] Deepgram error`, err));

      activeCalls.set(callSid, { twilioWs, streamSid });
      console.log(`[${callSid}] Call started, streamSid=${streamSid}`);
    }

    if (msg.event === 'media') {
      if (!mediaCount) mediaCount = 0;
      mediaCount++;
      if (mediaCount % 50 === 0) console.log(`[${callSid}] Received ${mediaCount} media chunks, deepgram readyState=${deepgramWs?.readyState}`);
      if (deepgramWs && deepgramWs.readyState === 1) {
        const audioChunk = Buffer.from(msg.media.payload, 'base64');
        deepgramWs.send(audioChunk);
      }
    }

    if (msg.event === 'stop') {
      console.log(`[${callSid}] Call ended`);
      if (deepgramWs) deepgramWs.close();
      if (callSid) activeCalls.delete(callSid);
    }
  });

  twilioWs.on('close', () => {
    if (deepgramWs) deepgramWs.close();
    if (callSid) activeCalls.delete(callSid);
  });
});

server.listen(PORT, () => {
  console.log(`JointSpeech Twilio server listening on port ${PORT}`);
});
