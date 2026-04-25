export async function generateDm({ config, promptText, leadRecord }) {
  const controller = config.requestTimeoutMs ? new AbortController() : null;
  const timeout = config.requestTimeoutMs
    ? setTimeout(() => controller.abort(), config.requestTimeoutMs)
    : null;

  try {
    const response = await fetch(config.ollamaUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        model: config.model,
        stream: false,
        messages: [
          {
            role: "system",
            content: buildSystemPrompt()
          },
          {
            role: "user",
            content: buildUserPrompt(promptText, leadRecord)
          }
        ]
      }),
      signal: controller?.signal
    });

    if (!response.ok) {
      const body = await response.text();
      throw new Error(`Ollama request failed with ${response.status}: ${body}`);
    }

    const payload = await response.json();
    const content = getAssistantContent(payload);
    const dmText = normalizeDmText(content);

    if (!dmText) {
      throw new Error("Model returned an empty DM.");
    }

    return dmText;
  } catch (error) {
    if (error?.name === "AbortError") {
      throw new Error(`Ollama request timed out after ${config.requestTimeoutMs}ms.`);
    }

    throw error;
  } finally {
    if (timeout) {
      clearTimeout(timeout);
    }
  }
}

function buildSystemPrompt() {
  return [
    "You write one personalized outbound DM from structured lead data.",
    "Follow the provided prompt exactly.",
    "Return only the final DM text.",
    "Do not return JSON.",
    "Do not wrap the answer in markdown fences.",
    "Do not explain your reasoning."
  ].join("\n");
}

function buildUserPrompt(promptText, leadRecord) {
  return [
    "Follow this DM writing prompt exactly:",
    promptText,
    "",
    "Lead record:",
    JSON.stringify(leadRecord, null, 2),
    "",
    "Return only the final DM text."
  ].join("\n");
}

function getAssistantContent(payload) {
  if (payload?.message?.content) {
    return payload.message.content;
  }

  if (payload?.choices?.[0]?.message?.content) {
    return payload.choices[0].message.content;
  }

  throw new Error("Ollama response did not include assistant content.");
}

function normalizeDmText(content) {
  const raw = stripCodeFences(String(content || "").trim());

  if (!raw) {
    return "";
  }

  const jsonValue = parseJsonValue(raw);

  if (jsonValue && typeof jsonValue === "object") {
    const nested = firstNonEmpty([
      jsonValue.dm,
      jsonValue.message,
      jsonValue.text,
      jsonValue.personalized_dm,
      jsonValue.personalized_message
    ]);

    if (nested) {
      return stripWrappingQuotes(normalizeWhitespacePreservingParagraphs(nested));
    }
  }

  return stripWrappingQuotes(normalizeWhitespacePreservingParagraphs(raw));
}

function parseJsonValue(value) {
  const trimmed = String(value || "").trim();

  if (!trimmed || (!trimmed.startsWith("{") && !trimmed.startsWith("["))) {
    return null;
  }

  try {
    return JSON.parse(trimmed);
  } catch {
    return null;
  }
}

function stripCodeFences(value) {
  return String(value || "")
    .replace(/^```(?:text|json)?\s*/i, "")
    .replace(/\s*```$/i, "")
    .trim();
}

function stripWrappingQuotes(value) {
  const text = String(value || "").trim();

  if (
    (text.startsWith("\"") && text.endsWith("\"")) ||
    (text.startsWith("'") && text.endsWith("'"))
  ) {
    return text.slice(1, -1).trim();
  }

  return text;
}

function normalizeWhitespacePreservingParagraphs(value) {
  return String(value || "")
    .replace(/\r\n/g, "\n")
    .split(/\n{2,}/)
    .map((paragraph) => paragraph.replace(/[ \t]+/g, " ").trim())
    .filter(Boolean)
    .join("\n\n")
    .trim();
}

function firstNonEmpty(values) {
  for (const value of values) {
    const text = String(value || "").trim();

    if (text) {
      return text;
    }
  }

  return "";
}
