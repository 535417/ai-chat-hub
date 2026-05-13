/**
 * Incrementally parse an OpenAI-style chat completion SSE stream.
 * Yields text deltas from choices[0].delta.content (and reasoning_content if present).
 */

export async function* iterateOpenAIChatStream(body, signal) {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';

  while (true) {
    if (signal?.aborted) {
      await reader.cancel().catch(() => {});
      return;
    }

    const { done, value } = await reader.read();
    if (done) break;

    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split('\n');
    buffer = lines.pop() ?? '';

    for (const rawLine of lines) {
      const line = rawLine.replace(/\r$/, '').trimEnd();
      if (!line || line.startsWith(':')) continue;

      if (line === 'data: [DONE]') {
        return;
      }

      if (!line.startsWith('data: ')) {
        continue;
      }

      const payload = line.slice('data: '.length).trim();
      if (payload === '[DONE]') {
        return;
      }

      let json;
      try {
        json = JSON.parse(payload);
      } catch {
        continue;
      }

      const choice = json.choices?.[0];
      if (!choice) continue;

      const delta = choice.delta ?? {};
      const content = normalizeDeltaText(delta.content);
      const reasoning = normalizeDeltaText(delta.reasoning_content);

      if (content) {
        yield { type: 'content', text: content };
      }
      if (reasoning) {
        yield { type: 'reasoning', text: reasoning };
      }

      if (choice.finish_reason && !content && !reasoning) {
        yield { type: 'finish', finish_reason: choice.finish_reason };
      }
    }
  }
}

function normalizeDeltaText(value) {
  if (value == null) return '';
  if (typeof value === 'string') return value;
  if (Array.isArray(value)) {
    return value
      .map((part) => {
        if (typeof part === 'string') return part;
        if (part && typeof part === 'object' && part.type === 'text' && typeof part.text === 'string') {
          return part.text;
        }
        return '';
      })
      .join('');
  }
  return String(value);
}
