import 'dotenv/config';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import express from 'express';
import cors from 'cors';
import { iterateOpenAIChatStream } from './openaiSse.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const rootDir = path.join(__dirname, '..');

const PROVIDERS = ['deepseek', 'mimo', 'glm'];

const app = express();
app.use(cors());
app.use(express.json({ limit: '1mb' }));
app.use(express.static(path.join(rootDir, 'public')));

// ── Rate Limiting (in-memory, per IP) ──────────────────────────────────
const RATE_LIMIT_WINDOW_MS = 60_000;
const RATE_LIMIT_MAX = 20;
const rateLimitStore = new Map();

// Periodic cleanup to avoid memory leak
setInterval(() => {
  const now = Date.now();
  for (const [key, entry] of rateLimitStore) {
    if (now > entry.resetTime) rateLimitStore.delete(key);
  }
}, RATE_LIMIT_WINDOW_MS);

function rateLimitMiddleware(req, res, next) {
  const ip = req.ip || req.socket.remoteAddress || 'unknown';
  const now = Date.now();
  let entry = rateLimitStore.get(ip);

  if (!entry || now > entry.resetTime) {
    entry = { count: 0, resetTime: now + RATE_LIMIT_WINDOW_MS };
    rateLimitStore.set(ip, entry);
  }

  entry.count++;
  const remaining = Math.max(0, RATE_LIMIT_MAX - entry.count);
  res.setHeader('X-RateLimit-Limit', RATE_LIMIT_MAX);
  res.setHeader('X-RateLimit-Remaining', remaining);
  res.setHeader('X-RateLimit-Reset', Math.ceil(entry.resetTime / 1000));

  if (entry.count > RATE_LIMIT_MAX) {
    const retryAfter = Math.ceil((entry.resetTime - now) / 1000);
    res.setHeader('Retry-After', retryAfter);
    res.status(429).json({ error: '请求过于频繁，请稍后再试' });
    return;
  }

  next();
}

// ── Upstream timeout ───────────────────────────────────────────────────
const UPSTREAM_TIMEOUT_MS = Number(process.env.UPSTREAM_TIMEOUT_MS) || 60_000;

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

function baseUrlEnvKey(label) {
  if (label === 'deepseek') return 'DEEPSEEK_BASE_URL';
  if (label === 'mimo') return 'MIMO_BASE_URL';
  if (label === 'glm') return 'GLM_BASE_URL';
  return `${label.toUpperCase()}_BASE_URL`;
}

/** @returns {string | null} */
function describeMisconfiguredBaseUrl(label, baseTrimmed) {
  let parsed;
  try {
    const withProto = /^https?:\/\//i.test(baseTrimmed) ? baseTrimmed : `https://${baseTrimmed}`;
    parsed = new URL(withProto);
  } catch {
    return `${label}: ${baseUrlEnvKey(label)} is not a valid URL (${baseTrimmed}).`;
  }

  const host = parsed.hostname.toLowerCase();
  if (host.endsWith('vercel.app') || host.endsWith('now.sh')) {
    const key = baseUrlEnvKey(label);
    return `${label}: ${key} points to ${parsed.host}. That is your deployment host, not the model API. Set ${key} to the provider's real API base (e.g. DeepSeek: https://api.deepseek.com/v1).`;
  }

  return null;
}

/** @returns {string | null} */
function describeDeepSeekPathIfWrong(fullUrl) {
  try {
    const u = new URL(fullUrl);
    if (u.hostname.toLowerCase() === 'api.deepseek.com' && !u.pathname.startsWith('/v1/')) {
      return 'DeepSeek: DEEPSEEK_BASE_URL must include /v1 (example: https://api.deepseek.com/v1). Otherwise requests hit the wrong path and can 404.';
    }
  } catch {
    return null;
  }
  return null;
}

function createWriteQueue(res) {
  let chain = Promise.resolve();

  const writeSse = (eventName, dataObj) => {
    const data = JSON.stringify(dataObj);
    // SSE: CRLF line endings + blank line terminator (better proxy / browser compatibility).
    const frame = [`event: ${eventName}`, `data: ${data}`, '', ''].join('\r\n');

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
  timeoutMs,
}) {
  const trimmedBase = String(baseUrl || '').trim();
  if (!trimmedBase) {
    await writeSse(label, {
      error: `Missing base URL for ${label}. Set the matching *_BASE_URL value in your .env file.`,
    });
    await writeSse(label, { done: true });
    return;
  }

  const misconfigured = describeMisconfiguredBaseUrl(label, trimmedBase);
  if (misconfigured) {
    await writeSse(label, { error: misconfigured });
    await writeSse(label, { done: true });
    return;
  }

  const url = joinChatCompletionsUrl(trimmedBase);

  if (label === 'deepseek') {
    const deepseekHint = describeDeepSeekPathIfWrong(url);
    if (deepseekHint) {
      await writeSse(label, { error: deepseekHint });
      await writeSse(label, { done: true });
      return;
    }
  }

  if (!apiKey) {
    await writeSse(label, {
      error: `Missing API key for ${label}. Check your .env file.`,
    });
    await writeSse(label, { done: true });
    return;
  }

  // Merge client abort + upstream timeout into one signal
  const timeoutController = new AbortController();
  const timeoutId = setTimeout(() => timeoutController.abort(), timeoutMs);
  const mergedSignal = signal
    ? AbortSignal.any([signal, timeoutController.signal])
    : timeoutController.signal;

  const isTimeout = () => timeoutController.signal.aborted && !signal?.aborted;

  try {
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
          max_tokens: 4096,
        }),
        signal: mergedSignal,
      });
    } catch (err) {
      await writeSse(label, {
        error: isTimeout()
          ? `请求超时（${Math.round(timeoutMs / 1000)}秒）`
          : err instanceof Error ? err.message : String(err),
      });
      await writeSse(label, { done: true });
      return;
    }

    if (!response.ok || !response.body) {
      const text = await response.text().catch(() => '');
      let detail = text || response.statusText;
      if (
        response.status === 404 &&
        /NOT_FOUND/i.test(text) &&
        (/sin1::/i.test(text) || /vercel/i.test(text))
      ) {
        detail = `${detail.trim()} (${baseUrlEnvKey(label)} likely points to the wrong host — e.g. your *.vercel.app URL instead of the provider API.)`;
      }
      await writeSse(label, {
        error: `HTTP ${response.status}: ${detail}`,
      });
      await writeSse(label, { done: true });
      return;
    }

    try {
      for await (const part of iterateOpenAIChatStream(response.body, mergedSignal)) {
        if (part.type === 'content') {
          await writeSse(label, { delta: part.text, kind: part.type });
        }
      }
    } catch (err) {
      if (signal?.aborted) {
        await writeSse(label, { done: true });
        return;
      }
      await writeSse(label, {
        error: isTimeout()
          ? `请求超时（${Math.round(timeoutMs / 1000)}秒）`
          : err instanceof Error ? err.message : String(err),
      });
    }

    await writeSse(label, { done: true });
  } finally {
    clearTimeout(timeoutId);
  }
}

app.post('/api/chat/stream', rateLimitMiddleware, async (req, res) => {
  // Accept: `providerMessages` (per-provider), `messages` (shared), or legacy `message` string
  let providerMessages;
  if (req.body?.providerMessages && typeof req.body.providerMessages === 'object') {
    providerMessages = req.body.providerMessages;
  } else if (Array.isArray(req.body?.messages) && req.body.messages.length > 0) {
    providerMessages = Object.fromEntries(PROVIDERS.map(p => [p, req.body.messages]));
  } else {
    const message = typeof req.body?.message === 'string' ? req.body.message.trim() : '';
    if (!message) {
      res.status(400).json({ error: 'message, messages, or providerMessages is required' });
      return;
    }
    const single = [{ role: 'user', content: message }];
    providerMessages = Object.fromEntries(PROVIDERS.map(p => [p, single]));
  }

  res.status(200);
  res.setHeader('Content-Type', 'text/event-stream; charset=utf-8');
  res.setHeader('Cache-Control', 'no-cache, no-transform');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');
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

  const makeProvider = (label, envSuffix, defaultModel) => ({
    label,
    baseUrl: process.env[`${envSuffix}_BASE_URL`],
    apiKey: process.env[`${envSuffix}_API_KEY`],
    model: process.env[`${envSuffix}_MODEL`] || defaultModel,
    messages: providerMessages[label] || providerMessages.deepseek || [{ role: 'user', content: '' }],
    signal,
    writeSse,
    timeoutMs: UPSTREAM_TIMEOUT_MS,
  });

  const deepseek = makeProvider('deepseek', 'DEEPSEEK', 'deepseek-chat');
  const mimo = makeProvider('mimo', 'MIMO', 'mimo-v2-flash');
  const glm = makeProvider('glm', 'GLM', 'glm-4-flash');

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
