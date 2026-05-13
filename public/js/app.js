const PROVIDERS = ['deepseek', 'mimo', 'glm'];

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
    output: panel.querySelector('[data-role="output"]'),
    state: panel.querySelector('[data-role="state"]'),
  };
}

function resetPanels() {
  for (const p of PROVIDERS) {
    const ui = getPanel(p);
    if (!ui) continue;
    ui.output.textContent = '';
    ui.state.textContent = '未发送';
    ui.state.classList.remove('is-streaming', 'is-done', 'is-error');
  }
}

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

function appendDelta(provider, text) {
  const ui = getPanel(provider);
  if (!ui) return;
  ui.output.textContent += text;
}

function showError(provider, message) {
  const ui = getPanel(provider);
  if (!ui) return;
  const prefix = ui.output.textContent ? `${ui.output.textContent}\n\n` : '';
  ui.output.textContent = `${prefix}[错误] ${message}`;
  setPanelState(provider, 'error');
}

/**
 * Parse SSE frames from a fetch streaming body.
 * Calls onEvent({ event, data }) for each completed frame.
 */
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

async function streamChat(message, signal) {
  const response = await fetch('/api/chat/stream', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ message }),
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
      if (event === 'all') {
        return;
      }

      if (!PROVIDERS.includes(event)) {
        return;
      }

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
    }
  }
}

function wireUi() {
  const form = $('#chat-form');
  const input = $('#message-input');
  const sendBtn = $('#send-btn');
  const clearBtn = $('#clear-btn');

  let activeController = null;

  form.addEventListener('submit', async (e) => {
    e.preventDefault();
    const message = input.value.trim();
    if (!message) return;

    if (activeController) {
      activeController.abort();
    }

    activeController = new AbortController();
    const { signal } = activeController;

    sendBtn.disabled = true;
    setStatus('请求中…（三个模型并行流式输出）');
    resetPanels();
    for (const p of PROVIDERS) setPanelState(p, 'streaming');

    try {
      await streamChat(message, signal);
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
