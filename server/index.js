import 'dotenv/config';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import express from 'express';
import cors from 'cors';
import { iterateOpenAIChatStream } from './openaiSse.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const rootDir = path.join(__dirname, '..');

const app = express();
app.use(cors());
app.use(express.json({ limit: '1mb' }));
app.use(express.static(path.join(rootDir, 'public')));

function joinChatCompletionsUrl(baseUrl) {
  const trimmed = String(baseUrl || '').replace(/\/+$/, '');
  if (!trimmed) {
    throw new Error('Missing base URL');
  }
  if (trimmed.endsWith('/chat/completions')) {
    return trimmed;
  }
  return `${trimmed}/chat/completions`;
}

function createWriteQueue(res) {
  let chain = Promise.resolve();

  const writeSse = (eventName, dataObj) => {
    const data = JSON.stringify(dataObj);
    const frame = `event: ${eventName}\ndata: ${data}\n\n`;

    chain = chain.then(
      () =>
        new Promise((resolve, reject) => {
          if (res.writableEnded) {
            resolve();
            return;
          }
          const ok = res.write(frame, (err) => (err ? reject(err) : resolve()));
          if (!ok) {
            res.once('drain', resolve);
          }
        }),
    );

    return chain;
  };

  return { writeSse, wait: () => chain };
}

async function streamProvider({
  label,
  baseUrl,
  apiKey,
  model,
  messages,
  signal,
  writeSse,
}) {
  const trimmedBase = String(baseUrl || '').trim();
  if (!trimmedBase) {
    await writeSse(label, {
      error: `Missing base URL for ${label}. Set the matching *_BASE_URL value in your .env file.`,
    });
    await writeSse(label, { done: true });
    return;
  }

  const url = joinChatCompletionsUrl(trimmedBase);

  if (!apiKey) {
    await writeSse(label, {
      error: `Missing API key for ${label}. Check your .env file.`,
    });
    await writeSse(label, { done: true });
    return;
  }

  let response;
  try {
    response = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify({
        model,
        messages,
        stream: true,
      }),
      signal,
    });
  } catch (err) {
    await writeSse(label, {
      error: err instanceof Error ? err.message : String(err),
    });
    await writeSse(label, { done: true });
    return;
  }

  if (!response.ok || !response.body) {
    const text = await response.text().catch(() => '');
    await writeSse(label, {
      error: `HTTP ${response.status}: ${text || response.statusText}`,
    });
    await writeSse(label, { done: true });
    return;
  }

  try {
    for await (const part of iterateOpenAIChatStream(response.body, signal)) {
      if (part.type === 'content' || part.type === 'reasoning') {
        await writeSse(label, { delta: part.text, kind: part.type });
      }
    }
  } catch (err) {
    if (signal?.aborted) {
      await writeSse(label, { done: true });
      return;
    }
    await writeSse(label, {
      error: err instanceof Error ? err.message : String(err),
    });
  }

  await writeSse(label, { done: true });
}

app.post('/api/chat/stream', async (req, res) => {
  const message = typeof req.body?.message === 'string' ? req.body.message.trim() : '';
  if (!message) {
    res.status(400).json({ error: 'message is required' });
    return;
  }

  res.status(200);
  res.setHeader('Content-Type', 'text/event-stream; charset=utf-8');
  res.setHeader('Cache-Control', 'no-cache, no-transform');
  res.setHeader('Connection', 'keep-alive');
  if (typeof res.flushHeaders === 'function') {
    res.flushHeaders();
  }

  const abortController = new AbortController();
  const { signal } = abortController;

  // Do NOT use req.on('close') here: for POST requests, IncomingMessage often
  // emits 'close' as soon as the body has been fully read, which would abort
  // upstream fetches immediately ("This operation was aborted").
  const onClientDisconnect = () => {
    abortController.abort();
  };
  req.on('aborted', onClientDisconnect);
  res.on('close', onClientDisconnect);

  const { writeSse, wait } = createWriteQueue(res);

  const messages = [{ role: 'user', content: message }];

  const deepseek = {
    label: 'deepseek',
    baseUrl: process.env.DEEPSEEK_BASE_URL,
    apiKey: process.env.DEEPSEEK_API_KEY,
    model: process.env.DEEPSEEK_MODEL || 'deepseek-chat',
    messages,
    signal,
    writeSse,
  };

  const mimo = {
    label: 'mimo',
    baseUrl: process.env.MIMO_BASE_URL,
    apiKey: process.env.MIMO_API_KEY,
    model: process.env.MIMO_MODEL || 'mimo-v2-flash',
    messages,
    signal,
    writeSse,
  };

  const glm = {
    label: 'glm',
    baseUrl: process.env.GLM_BASE_URL,
    apiKey: process.env.GLM_API_KEY,
    model: process.env.GLM_MODEL || 'glm-4-flash',
    messages,
    signal,
    writeSse,
  };

  try {
    await Promise.all([streamProvider(deepseek), streamProvider(mimo), streamProvider(glm)]);
    await writeSse('all', { done: true });
  } catch (err) {
    await writeSse('all', {
      error: err instanceof Error ? err.message : String(err),
    });
  } finally {
    req.removeListener('aborted', onClientDisconnect);
    res.removeListener('close', onClientDisconnect);
    await wait();
    if (!res.writableEnded) {
      res.end();
    }
  }
});

app.get('/api/health', (_req, res) => {
  res.json({ ok: true });
});

export default app;

// Vercel 注入 VERCEL=1；在平台上由 server.js 导入本模块，不能 listen 占用端口。
if (!process.env.VERCEL) {
  const port = Number(process.env.PORT) || 3000;
  app.listen(port, () => {
    console.log(`Server listening on http://localhost:${port}`);
  });
}
