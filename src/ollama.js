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

    return normalizeQualificationResult(parsed, config, leadRecord);
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
    "qualification_note must cite a concrete row signal or a clearly missing signal.",
    "Never return a vague note like unclear, outside ICP, good fit, bad fit, or not a fit by itself.",
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
    "Return only JSON with the required keys. Keep `qualification_note` concise but specific."
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

function normalizeQualificationResult(result, config, leadRecord) {
  const status = normalizeStatus(
    result.qualification_status ?? result.status ?? result.lead_status
  );
  const category = normalizeCategory(
    result.lead_category ?? result.category ?? result.business_category ?? result.industry_category
  );

  const rawNote = squashWhitespace(
    result.qualification_note ??
      result.note ??
      result.short_note ??
      result.summary ??
      ""
  );
  const note = buildQualificationNote({
    rawNote,
    status: status || "",
    category: category || "Unclear",
    leadRecord,
    maxLength: config.maxNoteLength
  });

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

function buildQualificationNote({ rawNote, status, category, leadRecord, maxLength }) {
  const trimmed = truncate(rawNote, maxLength);

  if (trimmed && !isVagueQualificationNote(trimmed, leadRecord)) {
    return trimmed;
  }

  return truncate(buildFallbackQualificationNote({ status, category, leadRecord }), maxLength);
}

function isVagueQualificationNote(note, leadRecord) {
  const normalized = squashWhitespace(note).toLowerCase();

  if (!normalized) {
    return true;
  }

  if (normalized.length < 24) {
    return true;
  }

  const genericPhrases = [
    "unclear",
    "outside icp",
    "good fit",
    "bad fit",
    "not a fit",
    "not fit",
    "qualified",
    "disqualified",
    "strong fit",
    "weak fit"
  ];

  const hasGenericPhrase = genericPhrases.some((phrase) => normalized === phrase || normalized.startsWith(`${phrase}.`) || normalized.startsWith(`${phrase},`) || normalized.includes(` is ${phrase}`));
  const hasRowEvidence = hasConcreteRowEvidence(note, leadRecord);

  if (hasGenericPhrase && !hasRowEvidence) {
    return true;
  }

  if (normalized.includes("unclear") && !hasRowEvidence) {
    return true;
  }

  return false;
}

function hasConcreteRowEvidence(note, leadRecord) {
  const haystack = squashWhitespace(note).toLowerCase();
  const candidates = [
    leadRecord.title,
    leadRecord.companyName,
    leadRecord.industry,
    leadRecord.firstName,
    leadRecord.lastName,
    leadRecord.fullName
  ]
    .map((value) => squashWhitespace(value).toLowerCase())
    .filter(Boolean);

  return candidates.some((value) => value.length >= 4 && haystack.includes(value));
}

function buildFallbackQualificationNote({ status, category, leadRecord }) {
  const title = squashWhitespace(leadRecord.title);
  const companyName = squashWhitespace(leadRecord.companyName);
  const industry = squashWhitespace(leadRecord.industry);
  const combinedText = [
    leadRecord.title,
    leadRecord.companyName,
    leadRecord.industry,
    leadRecord.summary,
    leadRecord.titleDescription
  ]
    .map((value) => squashWhitespace(value).toLowerCase())
    .filter(Boolean)
    .join(" ");

  const authorityLabel = getAuthorityLabel(title);
  const businessEvidence = buildBusinessEvidence(category, companyName, industry, leadRecord);

  if (status === "Qualified") {
    const parts = [businessEvidence];

    if (authorityLabel) {
      parts.push(`title "${title}" shows decision-making authority`);
    } else if (title) {
      parts.push(`title "${title}" still appears senior enough for outreach`);
    }

    return joinReasonParts(parts);
  }

  if (matchesAny(combinedText, [
    "coach",
    "coaching",
    "consultant",
    "consulting",
    "advisor",
    "advisory",
    "mentor",
    "speaker",
    "trainer",
    "practitioner"
  ])) {
    return joinReasonParts([
      companyName ? `company "${companyName}" looks like a coaching or consulting business` : "current business looks like coaching or consulting",
      "that sits outside Unsocials' ICP"
    ]);
  }

  if (matchesAny(combinedText, [
    "clinic",
    "medical",
    "healthcare",
    "hospital",
    "dentist",
    "dental"
  ])) {
    return joinReasonParts([
      industry ? `industry "${industry}" points to healthcare` : "current business looks like healthcare",
      "which is outside Unsocials' ICP"
    ]);
  }

  if (matchesAny(combinedText, [
    "software",
    "saas",
    "technology",
    "tech",
    "it services",
    "information technology"
  ])) {
    return joinReasonParts([
      industry ? `industry "${industry}" points to software or technology` : "current business looks like software or technology",
      "which is outside Unsocials' ICP"
    ]);
  }

  if (matchesAny(combinedText, [
    "agency",
    "marketing",
    "advertising",
    "recruiter",
    "recruitment",
    "staffing"
  ]) && category === "Outside ICP") {
    return joinReasonParts([
      industry ? `industry "${industry}" does not match a consumer-facing ICP brand` : businessEvidence,
      "and the business looks like a service provider rather than an operator brand"
    ]);
  }

  if (!authorityLabel && looksLikeLowAuthority(title)) {
    return joinReasonParts([
      businessEvidence,
      title ? `title "${title}" does not show buying authority` : "the row does not show buying authority"
    ]);
  }

  if (category === "Outside ICP") {
    return joinReasonParts([
      businessEvidence,
      "it does not match Unsocials' hospitality, F&B, real estate, or approved lifestyle ICP"
    ]);
  }

  if (category === "Unclear") {
    const missingBusinessReason = industry
      ? `industry "${industry}" does not clearly prove a current ICP business`
      : "the row does not clearly show a current hospitality, F&B, real estate, or approved lifestyle business";

    if (!authorityLabel && title) {
      return joinReasonParts([missingBusinessReason, `title "${title}" also does not clearly show buying authority`]);
    }

    return joinReasonParts([missingBusinessReason, businessEvidence]);
  }

  if (!authorityLabel && title) {
    return joinReasonParts([businessEvidence, `title "${title}" does not clearly show final buying authority`]);
  }

  return joinReasonParts([businessEvidence, "the row still lacks enough evidence to qualify this lead"]);
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

function buildBusinessEvidence(category, companyName, industry, leadRecord) {
  const description = squashWhitespace(leadRecord.titleDescription);
  const categoryLabel = formatCategoryLabel(category);

  if (category === "Outside ICP") {
    if (companyName && industry) {
      return `company "${companyName}" with industry "${industry}" does not look like an Unsocials ICP brand`;
    }

    if (industry) {
      return `industry "${industry}" does not look like an Unsocials ICP brand`;
    }

    if (companyName) {
      return `company "${companyName}" does not clearly look like an Unsocials ICP brand`;
    }

    return "current business does not clearly look like an Unsocials ICP brand";
  }

  if (category === "Unclear") {
    if (companyName && industry) {
      return `company "${companyName}" and industry "${industry}" do not clearly confirm the current business type`;
    }

    if (industry) {
      return `industry "${industry}" alone does not clearly confirm the current business type`;
    }

    if (companyName) {
      return `company "${companyName}" alone does not clearly confirm the current business type`;
    }

    if (description) {
      return "the company description does not clearly confirm the current business type";
    }

    return "the row does not clearly confirm the current business type";
  }

  if (companyName && industry) {
    return `company "${companyName}" and industry "${industry}" point to a ${categoryLabel} business`;
  }

  if (companyName) {
    return `company "${companyName}" points to a ${categoryLabel} business`;
  }

  if (industry) {
    return `industry "${industry}" points to a ${categoryLabel} business`;
  }

  if (description) {
    return `the company description points to a ${categoryLabel} business`;
  }

  return `current business appears to be a ${categoryLabel} business`;
}

function formatCategoryLabel(category) {
  return String(category || "")
    .replace(/&/g, "and")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
}

function getAuthorityLabel(title) {
  const normalized = squashWhitespace(title).toLowerCase();

  if (!normalized) {
    return "";
  }

  if (
    /(owner|founder|co-founder|cofounder|ceo|chief executive|managing director|director|general manager|gm|president|principal|head of marketing|marketing head|brand director|partner)/.test(
      normalized
    )
  ) {
    return "decision-maker";
  }

  return "";
}

function looksLikeLowAuthority(title) {
  const normalized = squashWhitespace(title).toLowerCase();

  if (!normalized) {
    return false;
  }

  return /(intern|assistant|coordinator|analyst|executive|associate|specialist|trainee|representative)/.test(
    normalized
  );
}

function matchesAny(text, fragments) {
  return fragments.some((fragment) => text.includes(fragment));
}

function joinReasonParts(parts) {
  const cleaned = parts.map((part) => squashWhitespace(part)).filter(Boolean);

  if (cleaned.length === 0) {
    return "";
  }

  const sentence = cleaned.join(", ");

  return sentence.endsWith(".") ? sentence : `${sentence}.`;
}

function truncate(value, maxLength) {
  const stringValue = String(value || "");

  if (!maxLength || stringValue.length <= maxLength) {
    return stringValue;
  }

  return `${stringValue.slice(0, Math.max(0, maxLength - 3)).trimEnd()}...`;
}
