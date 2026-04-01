const RESPONSE_SCHEMA = {
  lead_category:
    "Hotel | Resort | Serviced Apartment | Villa | Hospitality Brand | Restaurant | Cafe | Bar | Cloud Kitchen | Dining Group | F&B Brand | Real Estate Developer | Property Group | Real Estate Agency | Project Launch | Salon | Gym | Spa | Studio | Wellness Brand | Lifestyle Brand | Outside ICP | Unclear",
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

  const aliases = new Map([
    ["hotel", "Hotel"],
    ["hotels", "Hotel"],
    ["hotel brand", "Hotel"],
    ["resort", "Resort"],
    ["resorts", "Resort"],
    ["serviced apartment", "Serviced Apartment"],
    ["serviced apartments", "Serviced Apartment"],
    ["service apartment", "Serviced Apartment"],
    ["service apartments", "Serviced Apartment"],
    ["villa", "Villa"],
    ["villas", "Villa"],
    ["hospitality brand", "Hospitality Brand"],
    ["hospitality", "Hospitality Brand"],
    ["restaurant", "Restaurant"],
    ["restaurants", "Restaurant"],
    ["restaurant brand", "Restaurant"],
    ["cafe", "Cafe"],
    ["cafe brand", "Cafe"],
    ["cafes", "Cafe"],
    ["café", "Cafe"],
    ["cafés", "Cafe"],
    ["bar", "Bar"],
    ["bars", "Bar"],
    ["cloud kitchen", "Cloud Kitchen"],
    ["cloud kitchens", "Cloud Kitchen"],
    ["dining group", "Dining Group"],
    ["restaurant group", "Dining Group"],
    ["food group", "Dining Group"],
    ["f&b brand", "F&B Brand"],
    ["food & beverage brand", "F&B Brand"],
    ["food and beverage brand", "F&B Brand"],
    ["f&b", "F&B Brand"],
    ["food & beverage", "F&B Brand"],
    ["food and beverage", "F&B Brand"],
    ["real estate developer", "Real Estate Developer"],
    ["developer", "Real Estate Developer"],
    ["property developer", "Real Estate Developer"],
    ["property group", "Property Group"],
    ["real estate group", "Property Group"],
    ["real estate agency", "Real Estate Agency"],
    ["property agency", "Real Estate Agency"],
    ["brokerage", "Real Estate Agency"],
    ["project launch", "Project Launch"],
    ["project marketing", "Project Launch"],
    ["clinic", "Outside ICP"],
    ["clinics", "Outside ICP"],
    ["salon", "Salon"],
    ["salons", "Salon"],
    ["gym", "Gym"],
    ["gyms", "Gym"],
    ["fitness center", "Gym"],
    ["fitness centre", "Gym"],
    ["spa", "Spa"],
    ["spa brand", "Spa"],
    ["studio", "Studio"],
    ["studios", "Studio"],
    ["wellness brand", "Wellness Brand"],
    ["wellness", "Wellness Brand"],
    ["lifestyle brand", "Lifestyle Brand"],
    ["lifestyle", "Lifestyle Brand"],
    ["outside icp", "Outside ICP"],
    ["outside", "Outside ICP"],
    ["other", "Outside ICP"],
    ["disqualified", "Outside ICP"],
    ["unclear", "Unclear"],
    ["unknown", "Unclear"]
  ]);

  if (aliases.has(normalized)) {
    return aliases.get(normalized);
  }

  if (
    normalized.includes("hotel") ||
    normalized.includes("motel")
  ) {
    return "Hotel";
  }

  if (normalized.includes("resort")) {
    return "Resort";
  }

  if (normalized.includes("serviced apartment") || normalized.includes("service apartment")) {
    return "Serviced Apartment";
  }

  if (normalized.includes("villa")) {
    return "Villa";
  }

  if (normalized.includes("restaurant")) {
    return "Restaurant";
  }

  if (normalized.includes("cafe") || normalized.includes("café")) {
    return "Cafe";
  }

  if (normalized.includes("bar")) {
    return "Bar";
  }

  if (normalized.includes("cloud kitchen")) {
    return "Cloud Kitchen";
  }

  if (normalized.includes("dining group") || normalized.includes("restaurant group")) {
    return "Dining Group";
  }

  if (normalized.includes("f&b") || normalized.includes("food & beverage") || normalized.includes("food and beverage")) {
    return "F&B Brand";
  }

  if (normalized.includes("developer")) {
    return "Real Estate Developer";
  }

  if (normalized.includes("property group") || normalized.includes("real estate group")) {
    return "Property Group";
  }

  if (
    normalized.includes("real estate agency") ||
    normalized.includes("property agency") ||
    normalized.includes("brokerage")
  ) {
    return "Real Estate Agency";
  }

  if (normalized.includes("project launch")) {
    return "Project Launch";
  }

  if (normalized.includes("clinic")) {
    return "Outside ICP";
  }

  if (normalized.includes("salon")) {
    return "Salon";
  }

  if (normalized.includes("gym") || normalized.includes("fitness center") || normalized.includes("fitness centre")) {
    return "Gym";
  }

  if (normalized.includes("spa")) {
    return "Spa";
  }

  if (normalized.includes("studio")) {
    return "Studio";
  }

  if (normalized.includes("wellness")) {
    return "Wellness Brand";
  }

  if (normalized.includes("lifestyle")) {
    return "Lifestyle Brand";
  }

  if (normalized.includes("hospitality")) {
    return "Hospitality Brand";
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
