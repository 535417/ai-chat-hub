const PROVIDERS = ['deepseek', 'mimo', 'glm'];

// ── Conversation state ─────────────────────────────────────────────────
// userMessages: [{ content, timestamp }]
// providerResponses: { deepseek: [{ content, timestamp }], ... }
const conversation = {
  userMessages: [],
  providerResponses: Object.fromEntries(PROVIDERS.map(p => [p, []])),
};

function $(selector, root = document) {
  return root.querySelector(selector);
}

function setStatus(text) {
  const el = $('#status');
  if (el) el.textContent = text;
}

function getPanel(provider) {
  const panel = document.querySelector(`.panel[data-provider="${provider}"]`);
  if (!panel) return null;
  return {
    panel,
    messages: panel.querySelector('[data-role="messages"]'),
    output: panel.querySelector('[data-role="output"]'),
    state: panel.querySelector('[data-role="state"]'),
  };
}

// ── Build messages array for API request ───────────────────────────────
function buildMessagesForProvider(provider) {
  const msgs = [];
  const responses = conversation.providerResponses[provider];
  for (let i = 0; i < conversation.userMessages.length; i++) {
    msgs.push({ role: 'user', content: conversation.userMessages[i].content });
    if (responses[i]) {
      msgs.push({ role: 'assistant', content: responses[i].content });
    }
  }
  return msgs;
}

// ── Render conversation history in a panel ─────────────────────────────
function renderMessages(provider) {
  const ui = getPanel(provider);
  if (!ui) return;
  const container = ui.messages;
  container.innerHTML = '';

  const responses = conversation.providerResponses[provider];

  for (let i = 0; i < conversation.userMessages.length; i++) {
    // User message
    const userDiv = document.createElement('div');
    userDiv.className = 'msg msg--user';
    userDiv.innerHTML = `<span class="msg__role">你</span><div class="msg__text">${escapeHtml(conversation.userMessages[i].content)}</div>`;
    container.appendChild(userDiv);

    // Assistant response (may not exist yet for the latest turn)
    if (responses[i]) {
      const asstDiv = document.createElement('div');
      asstDiv.className = 'msg msg--assistant';
      asstDiv.innerHTML = `<span class="msg__role">${providerLabel(provider)}</span><div class="msg__text">${escapeHtml(responses[i].content)}</div>`;
      container.appendChild(asstDiv);
    }
  }

  // Scroll to bottom
  container.scrollTop = container.scrollHeight;
}

function escapeHtml(text) {
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}

function providerLabel(provider) {
  if (provider === 'deepseek') return 'DeepSeek';
  if (provider === 'mimo') return 'MiMo';
  if (provider === 'glm') return 'GLM';
  return provider;
}

// ── Panel state management ─────────────────────────────────────────────
function setPanelState(provider, mode) {
  const ui = getPanel(provider);
  if (!ui) return;
  ui.state.classList.remove('is-streaming', 'is-done', 'is-error');
  if (mode === 'streaming') {
    ui.state.textContent = '输出中…';
    ui.state.classList.add('is-streaming');
  } else if (mode === 'done') {
    ui.state.textContent = '完成';
    ui.state.classList.add('is-done');
  } else if (mode === 'error') {
    ui.state.textContent = '出错';
    ui.state.classList.add('is-error');
  } else {
    ui.state.textContent = '未发送';
  }
}

function resetPanels() {
  for (const p of PROVIDERS) {
    const ui = getPanel(p);
    if (!ui) continue;
    ui.messages.innerHTML = '';
    ui.output.textContent = '';
    ui.output.hidden = true;
    ui.messages.hidden = false;
    ui.state.textContent = '未发送';
    ui.state.classList.remove('is-streaming', 'is-done', 'is-error');
  }
}

// ── Streaming output (for current turn) ───────────────────────────────
// During streaming, we show the live output in the output element,
// then move it to the messages container when done.
const liveBuffers = Object.fromEntries(PROVIDERS.map(p => [p, '']));

function startLiveOutput(provider) {
  liveBuffers[provider] = '';
  const ui = getPanel(provider);
  if (!ui) return;
  ui.output.textContent = '';
  ui.output.hidden = false;
}

function appendDelta(provider, text) {
  liveBuffers[provider] += text;
  const ui = getPanel(provider);
  if (!ui) return;
  ui.output.textContent += text;
  // Auto-scroll
  ui.output.scrollTop = ui.output.scrollHeight;
}

function finalizeLiveOutput(provider) {
  const content = liveBuffers[provider];
  const ui = getPanel(provider);
  if (ui) {
    ui.output.hidden = true;
  }

  if (content) {
    conversation.providerResponses[provider].push({ content, timestamp: Date.now() });
  }
  liveBuffers[provider] = '';
  renderMessages(provider);
}

function showError(provider, message) {
  const ui = getPanel(provider);
  if (!ui) return;
  // Show error in the output area, then finalize
  const prefix = ui.output.textContent ? `${ui.output.textContent}\n\n` : '';
  ui.output.textContent = `${prefix}[错误] ${message}`;
  ui.output.hidden = false;
  setPanelState(provider, 'error');
}

// ── SSE parsing ────────────────────────────────────────────────────────
function takeNextSseFrame(buffer) {
  const crlf = buffer.indexOf('\r\n\r\n');
  const lf = buffer.indexOf('\n\n');
  let sep = -1;
  let sepLen = 0;
  if (crlf !== -1 && (lf === -1 || crlf <= lf)) {
    sep = crlf;
    sepLen = 4;
  } else if (lf !== -1) {
    sep = lf;
    sepLen = 2;
  }
  if (sep === -1) return null;
  return {
    frame: buffer.slice(0, sep),
    rest: buffer.slice(sep + sepLen),
  };
}

async function readSseStream(response, onEvent, signal) {
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';

  while (true) {
    if (signal.aborted) {
      await reader.cancel().catch(() => {});
      return;
    }

    const { done, value } = await reader.read();
    if (done) break;

    buffer += decoder.decode(value, { stream: true });

    while (true) {
      const next = takeNextSseFrame(buffer);
      if (!next) break;
      const rawFrame = next.frame;
      buffer = next.rest;

      let eventName = 'message';
      const dataLines = [];

      for (const line of rawFrame.split(/\r?\n/)) {
        const trimmed = line.replace(/\r$/, '').trimEnd();
        if (!trimmed || trimmed.startsWith(':')) continue;
        if (trimmed.startsWith('event:')) {
          eventName = trimmed.slice('event:'.length).trim();
        } else if (trimmed.startsWith('data:')) {
          dataLines.push(trimmed.slice('data:'.length).trimStart());
        }
      }

      const dataText = dataLines.join('\n');
      if (!dataText) continue;

      let data;
      try {
        data = JSON.parse(dataText);
      } catch {
        data = { raw: dataText };
      }

      onEvent({ event: eventName, data });
    }
  }
}

// ── Main streaming request ─────────────────────────────────────────────
async function streamChat(providerMessages, signal) {
  const response = await fetch('/api/chat/stream', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ providerMessages }),
    signal,
  });

  if (!response.ok) {
    const text = await response.text().catch(() => '');
    throw new Error(text || `HTTP ${response.status}`);
  }

  const finished = new Set();

  await readSseStream(
    response,
    ({ event, data }) => {
      if (event === 'all') return;
      if (!PROVIDERS.includes(event)) return;

      if (data?.error) {
        showError(event, String(data.error));
        finished.add(event);
        return;
      }

      if (typeof data?.delta === 'string' && data.delta) {
        setPanelState(event, 'streaming');
        appendDelta(event, data.delta);
      }

      if (data?.done) {
        finished.add(event);
        const ui = getPanel(event);
        const hasErrorClass = ui?.state.classList.contains('is-error');
        if (!hasErrorClass) {
          setPanelState(event, 'done');
        }
        finalizeLiveOutput(event);
      }
    },
    signal,
  );

  for (const p of PROVIDERS) {
    if (!finished.has(p)) {
      const ui = getPanel(p);
      const hasErrorClass = ui?.state.classList.contains('is-error');
      if (ui && !hasErrorClass) {
        setPanelState(p, 'done');
      }
      finalizeLiveOutput(p);
    }
  }
}

// ── UI wiring ──────────────────────────────────────────────────────────
function wireUi() {
  const form = $('#chat-form');
  const input = $('#message-input');
  const sendBtn = $('#send-btn');
  const clearBtn = $('#clear-btn');

  let activeController = null;

  form.addEventListener('submit', async (e) => {
    e.preventDefault();
    const text = input.value.trim();
    if (!text) return;

    if (activeController) {
      activeController.abort();
    }

    activeController = new AbortController();
    const { signal } = activeController;

    // Add user message to history
    conversation.userMessages.push({ content: text, timestamp: Date.now() });
    input.value = '';

    sendBtn.disabled = true;
    setStatus('请求中…（三个模型并行流式输出）');

    // Reset panels and render existing history
    resetPanels();
    for (const p of PROVIDERS) {
      renderMessages(p);
      startLiveOutput(p);
      setPanelState(p, 'streaming');
    }

    // Build per-provider messages arrays
    const providerMessages = {};
    for (const p of PROVIDERS) {
      providerMessages[p] = buildMessagesForProvider(p);
    }

    try {
      await streamChat(providerMessages, signal);
      setStatus('完成');
    } catch (err) {
      if (signal.aborted) {
        setStatus('已取消');
      } else {
        const msg = err instanceof Error ? err.message : String(err);
        setStatus('请求失败');
        for (const p of PROVIDERS) {
          if (!getPanel(p)?.output.textContent) {
            showError(p, msg);
          }
        }
      }
    } finally {
      sendBtn.disabled = false;
      activeController = null;
    }
  });

  clearBtn.addEventListener('click', () => {
    if (activeController) {
      activeController.abort();
      activeController = null;
    }
    conversation.userMessages = [];
    for (const p of PROVIDERS) {
      conversation.providerResponses[p] = [];
    }
    resetPanels();
    setStatus('就绪');
  });

  input.addEventListener('keydown', (e) => {
    if (e.key !== 'Enter') return;
    if (e.shiftKey) return;
    e.preventDefault();
    form.requestSubmit();
  });
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', wireUi, { once: true });
} else {
  wireUi();
}
