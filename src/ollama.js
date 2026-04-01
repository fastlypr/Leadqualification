const RESPONSE_SCHEMA = {
  lead_category: "Hospitality | F&B | Real Estate | Lifestyle & Service | Outside ICP | Unclear",
  qualification_status: "Qualified | Disqualified",
  qualification_note: "short note explaining why"
};

export async function qualifyLead({ config, promptText, leadRecord }) {
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
    const parsed = parseModelJson(content);

    return normalizeQualificationResult(parsed, config);
  } catch (error) {
    if (error.name === "AbortError") {
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
    "You qualify sales leads from CSV row data.",
    "Qualify the current business, not the audience they serve.",
    "If the current business is outside ICP, return qualification_status as Disqualified.",
    "Only use the exact statuses Qualified or Disqualified.",
    "Only use the exact categories from the schema.",
    "Return only valid JSON.",
    "Do not wrap the JSON in markdown.",
    "Use this exact schema:",
    JSON.stringify(RESPONSE_SCHEMA, null, 2)
  ].join("\n");
}

function buildUserPrompt(promptText, leadRecord) {
  return [
    "Follow these lead qualification rules exactly:",
    promptText,
    "",
    "Lead record:",
    JSON.stringify(leadRecord, null, 2),
    "",
    "Return only JSON with the required keys. Keep `qualification_note` concise."
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

function parseModelJson(content) {
  const text = String(content || "").trim();
  const extracted = extractJsonObject(text);

  if (!extracted) {
    throw new Error(`Model did not return valid JSON. Raw response: ${truncate(text, 500)}`);
  }

  try {
    return JSON.parse(extracted);
  } catch (error) {
    throw new Error(`Could not parse model JSON: ${truncate(extracted, 500)}`);
  }
}

function extractJsonObject(text) {
  const cleaned = text
    .replace(/^```json\s*/i, "")
    .replace(/^```\s*/i, "")
    .replace(/\s*```$/i, "")
    .trim();

  const start = cleaned.indexOf("{");
  if (start === -1) {
    return null;
  }

  let depth = 0;
  let inString = false;
  let escaped = false;

  for (let index = start; index < cleaned.length; index += 1) {
    const char = cleaned[index];

    if (inString) {
      if (escaped) {
        escaped = false;
        continue;
      }

      if (char === "\\") {
        escaped = true;
        continue;
      }

      if (char === "\"") {
        inString = false;
      }

      continue;
    }

    if (char === "\"") {
      inString = true;
      continue;
    }

    if (char === "{") {
      depth += 1;
      continue;
    }

    if (char === "}") {
      depth -= 1;

      if (depth === 0) {
        return cleaned.slice(start, index + 1);
      }
    }
  }

  return null;
}

function normalizeQualificationResult(result, config) {
  const status = normalizeStatus(
    result.qualification_status ?? result.status ?? result.lead_status
  );
  const category = normalizeCategory(
    result.lead_category ?? result.category ?? result.business_category ?? result.industry_category
  );

  const note = truncate(
    squashWhitespace(
      result.qualification_note ??
        result.note ??
        result.short_note ??
        result.summary ??
        ""
    ),
    config.maxNoteLength
  );

  return {
    lead_category: category || "Unclear",
    qualification_status: status || "",
    qualification_note: note
  };
}

function normalizeStatus(value) {
  const normalized = squashWhitespace(String(value || "")).toLowerCase();

  if (!normalized) {
    return "";
  }

  if (normalized === "qualified") {
    return "Qualified";
  }

  if (
    normalized === "disqualified" ||
    normalized === "unqualified" ||
    normalized === "not qualified"
  ) {
    return "Disqualified";
  }

  return "";
}

function normalizeCategory(value) {
  const normalized = squashWhitespace(String(value || "")).toLowerCase();

  if (!normalized) {
    return "";
  }

  if (normalized === "hospitality") {
    return "Hospitality";
  }

  if (normalized === "f&b" || normalized === "food & beverage" || normalized === "food and beverage") {
    return "F&B";
  }

  if (normalized === "real estate") {
    return "Real Estate";
  }

  if (
    normalized === "lifestyle & service" ||
    normalized === "lifestyle and service" ||
    normalized === "lifestyle/service"
  ) {
    return "Lifestyle & Service";
  }

  if (normalized === "outside icp" || normalized === "outside" || normalized === "other") {
    return "Outside ICP";
  }

  if (normalized === "unclear" || normalized === "unknown") {
    return "Unclear";
  }

  return "";
}

function squashWhitespace(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function truncate(value, maxLength) {
  const stringValue = String(value || "");

  if (!maxLength || stringValue.length <= maxLength) {
    return stringValue;
  }

  return `${stringValue.slice(0, Math.max(0, maxLength - 3)).trimEnd()}...`;
}
