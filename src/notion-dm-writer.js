const NOTION_API_BASE_URL = "https://api.notion.com/v1";
const NOTION_VERSION = "2022-06-28";

const PROPERTY_DEFINITIONS = {
  leadUrl: {
    preferredName: "linkedinProfileUrl",
    fallbackName: "Lead URL",
    type: "url"
  },
  dmText: {
    preferredName: "DM",
    fallbackName: "Personalized DM",
    type: "rich_text"
  },
  dmStatus: {
    preferredName: "DM status",
    fallbackName: "DM Status",
    type: "rich_text"
  },
  leadCategory: {
    preferredName: "Lead category",
    fallbackName: "Lead Category",
    type: "rich_text"
  },
  qualification: {
    preferredName: "Qualification",
    fallbackName: "AI Qualification",
    type: "rich_text"
  },
  qualificationNote: {
    preferredName: "Qualification note",
    fallbackName: "AI Qualification note",
    type: "rich_text"
  }
};

const EXCLUDED_RAW_RECORD_KEYS = new Set(["dm_text", "dm_status"]);

const RESERVED_NOTION_SCHEMA_NAMES = new Set([
  "title",
  "rich_text",
  "number",
  "select",
  "multi_select",
  "status",
  "date",
  "people",
  "files",
  "checkbox",
  "url",
  "email",
  "phone_number",
  "formula",
  "relation",
  "rollup",
  "created_time",
  "created_by",
  "last_edited_time",
  "last_edited_by",
  "button",
  "verification"
]);

export async function createDmWriterNotionSync({ config, logger }) {
  if (!config.notionToken || !config.notionDatabaseId) {
    return null;
  }

  const client = createNotionClient(config.notionToken);
  let schema = await loadDatabaseSchema(client, config.notionDatabaseId);

  await logger.info(
    `DM Notion sync enabled for database ${config.notionDatabaseId}. Title property: ${schema.titlePropertyName}.`
  );

  return {
    async prepareSchema(record) {
      schema = await ensureDatabaseSchema(client, config.notionDatabaseId, schema, record);
      return schema;
    },

    async findExistingLead(record) {
      schema = await ensureDatabaseSchema(client, config.notionDatabaseId, schema, record);
      const page = await findExistingPage(client, config.notionDatabaseId, schema, record);

      if (!page) {
        return null;
      }

      return buildExistingLeadSnapshot(page, schema);
    },

    async upsertDm(record, existingPageId = "") {
      schema = await ensureDatabaseSchema(client, config.notionDatabaseId, schema, record);
      const pageId =
        existingPageId || (await findExistingPageId(client, config.notionDatabaseId, schema, record));
      const properties = buildPageProperties(schema.titlePropertyName, schema.propertyNames, record);

      if (pageId) {
        await client.request("PATCH", `/pages/${pageId}`, { properties });
        return { action: "updated", pageId };
      }

      const created = await client.request("POST", "/pages", {
        parent: { database_id: config.notionDatabaseId },
        properties
      });

      return { action: "created", pageId: created.id };
    }
  };
}

function createNotionClient(token) {
  return {
    async request(method, requestPath, body, attempt = 0) {
      const url = `${NOTION_API_BASE_URL}${requestPath}`;
      let response;

      try {
        response = await fetch(url, {
          method,
          headers: {
            Authorization: `Bearer ${token}`,
            "Notion-Version": NOTION_VERSION,
            "Content-Type": "application/json"
          },
          body: body ? JSON.stringify(body) : undefined
        });
      } catch (error) {
        const reason =
          error instanceof Error && error.cause instanceof Error
            ? error.cause.message
            : error instanceof Error
              ? error.message
              : String(error);

        throw new Error(
          `Notion network request failed for ${method} ${url}. ${reason}. If this is Ubuntu, try IPv4-first networking and confirm the server can reach api.notion.com.`
        );
      }

      if (response.ok) {
        if (response.status === 204) {
          return null;
        }

        return response.json();
      }

      const errorText = await response.text();

      if ((response.status === 429 || response.status >= 500) && attempt < 3) {
        const retryAfter = Number(response.headers.get("retry-after") || 0);
        const delayMs =
          Number.isFinite(retryAfter) && retryAfter > 0 ? retryAfter * 1000 : 1000 * (attempt + 1);
        await sleep(delayMs);
        return this.request(method, requestPath, body, attempt + 1);
      }

      if (response.status === 401 || response.status === 403 || response.status === 404) {
        throw new Error(
          `Notion request failed with ${response.status}. Check that the integration token is valid and the database is shared with the integration. ${truncate(errorText, 250)}`
        );
      }

      throw new Error(`Notion request failed with ${response.status}: ${truncate(errorText, 400)}`);
    }
  };
}

async function loadDatabaseSchema(client, databaseId) {
  const database = await client.request("GET", `/databases/${databaseId}`);
  const titlePropertyName = findTitlePropertyName(database.properties);

  if (!titlePropertyName) {
    throw new Error("Notion database does not have a title property.");
  }

  return {
    titlePropertyName,
    properties: database.properties || {},
    propertyNames: resolveCorePropertyNames(database.properties || {}, titlePropertyName)
  };
}

async function ensureDatabaseSchema(client, databaseId, schema, record) {
  const properties = schema?.properties || {};
  const titlePropertyName = schema?.titlePropertyName || findTitlePropertyName(properties);
  const propertyNames = resolveCorePropertyNames(properties, titlePropertyName);

  const updates = {};
  for (const [key, definition] of Object.entries(PROPERTY_DEFINITIONS)) {
    ensurePropertyType(properties, propertyNames[key], definition.type, updates);
  }

  for (const field of getRawRecordFieldMappings(record, titlePropertyName, Object.values(propertyNames))) {
    ensurePropertyType(properties, field.propertyName, field.propertyType, updates);
  }

  if (Object.keys(updates).length > 0) {
    await client.request("PATCH", `/databases/${databaseId}`, { properties: updates });
  }

  return {
    titlePropertyName,
    propertyNames,
    properties: {
      ...properties,
      ...Object.fromEntries(
        Object.entries(updates).map(([name, definition]) => [name, inferPropertyFromDefinition(definition)])
      )
    }
  };
}

function ensurePropertyType(properties, propertyName, expectedType, updates) {
  const existing = properties[propertyName];

  if (!existing) {
    updates[propertyName] = propertyDefinitionForType(expectedType);
    return;
  }

  if (existing.type !== expectedType) {
    throw new Error(
      `Notion property "${propertyName}" exists but is type "${existing.type}". Expected "${expectedType}".`
    );
  }
}

function propertyDefinitionForType(type) {
  if (type === "url") {
    return { url: {} };
  }

  if (type === "rich_text") {
    return { rich_text: {} };
  }

  throw new Error(`Unsupported Notion property type: ${type}`);
}

function inferPropertyFromDefinition(definition) {
  if (definition?.url) {
    return { type: "url" };
  }

  if (definition?.rich_text) {
    return { type: "rich_text" };
  }

  return { type: "" };
}

function findTitlePropertyName(properties) {
  for (const [name, property] of Object.entries(properties || {})) {
    if (property?.type === "title") {
      return name;
    }
  }

  return "";
}

async function findExistingPageId(client, databaseId, schema, record) {
  const page = await findExistingPage(client, databaseId, schema, record);
  return page?.id || null;
}

async function findExistingPage(client, databaseId, schema, record) {
  const leadUrl = getLeadUrl(record);

  if (!leadUrl) {
    return null;
  }

  const query = await client.request("POST", `/databases/${databaseId}/query`, {
    filter: {
      property: schema.propertyNames.leadUrl,
      url: {
        equals: leadUrl
      }
    },
    page_size: 1
  });

  return query?.results?.[0] || null;
}

function buildExistingLeadSnapshot(page, schema) {
  const flattened = flattenPageProperties(page);

  return {
    pageId: page.id,
    dm_text: firstNonEmpty([
      flattened[schema.propertyNames.dmText],
      flattened.DM,
      flattened["Personalized DM"]
    ]),
    dm_status: firstNonEmpty([
      flattened[schema.propertyNames.dmStatus],
      flattened["DM status"],
      flattened["DM Status"]
    ]),
    lead_category: firstNonEmpty([
      flattened[schema.propertyNames.leadCategory],
      flattened["Lead category"]
    ]),
    qualification: firstNonEmpty([
      flattened[schema.propertyNames.qualification],
      flattened.Qualification
    ]),
    qualification_note: firstNonEmpty([
      flattened[schema.propertyNames.qualificationNote],
      flattened["Qualification note"]
    ])
  };
}

function buildPageProperties(titlePropertyName, propertyNames, record) {
  const properties = {
    [titlePropertyName]: {
      title: buildRichTextArray(getLeadName(record) || "Unnamed Lead")
    },
    [propertyNames.leadUrl]: {
      url: getLeadUrl(record) || null
    },
    [propertyNames.dmText]: {
      rich_text: buildRichTextArray(record.dm_text)
    },
    [propertyNames.dmStatus]: {
      rich_text: buildRichTextArray(record.dm_status)
    },
    [propertyNames.leadCategory]: {
      rich_text: buildRichTextArray(record["Lead category"] || record.lead_category)
    },
    [propertyNames.qualification]: {
      rich_text: buildRichTextArray(record.Qualification || record.qualification_status)
    },
    [propertyNames.qualificationNote]: {
      rich_text: buildRichTextArray(record["Qualification note"] || record.qualification_note)
    }
  };

  for (const field of getRawRecordFieldMappings(record, titlePropertyName, Object.values(propertyNames))) {
    properties[field.propertyName] = buildPropertyValue(field.propertyType, record[field.recordKey]);
  }

  return properties;
}

function buildPropertyValue(propertyType, value) {
  if (propertyType === "url") {
    return {
      url: normalizeUrl(value)
    };
  }

  return {
    rich_text: buildRichTextArray(value)
  };
}

function getRawRecordFieldMappings(record, titlePropertyName, reservedPropertyNames = []) {
  const keys = Object.keys(record || {}).sort((left, right) => left.localeCompare(right));
  const reservedNames = new Set([
    String(titlePropertyName || "").trim().toLowerCase(),
    ...RESERVED_NOTION_SCHEMA_NAMES,
    ...reservedPropertyNames.map((name) => String(name || "").trim().toLowerCase())
  ]);
  const usedNames = new Set(reservedNames);
  const mappings = [];

  for (const recordKey of keys) {
    const trimmedKey = String(recordKey || "").trim();
    const normalizedKey = trimmedKey.toLowerCase();

    if (!trimmedKey || EXCLUDED_RAW_RECORD_KEYS.has(trimmedKey) || reservedNames.has(normalizedKey)) {
      continue;
    }

    const propertyType = inferRecordPropertyType(trimmedKey, record[recordKey]);
    const propertyName = buildDynamicPropertyName(trimmedKey, usedNames);
    mappings.push({ recordKey, propertyName, propertyType });
    usedNames.add(propertyName.toLowerCase());
  }

  return mappings;
}

function inferRecordPropertyType(recordKey, value) {
  const normalizedKey = String(recordKey || "").trim().toLowerCase();
  const text = String(value || "").trim();

  if (
    (normalizedKey.includes("url") || normalizedKey.includes("website")) &&
    (!text || /^https?:\/\//i.test(text))
  ) {
    return "url";
  }

  return "rich_text";
}

function buildDynamicPropertyName(recordKey, usedNames) {
  const baseName = String(recordKey || "").trim();
  const normalized = baseName.toLowerCase();

  if (!usedNames.has(normalized)) {
    return baseName;
  }

  const prefixedBase = `CSV ${baseName}`;
  return buildUniquePropertyName(prefixedBase, usedNames);
}

function resolveCorePropertyNames(properties, titlePropertyName) {
  const blockedNames = new Set(
    Object.keys(properties || {}).map((name) => String(name || "").trim().toLowerCase())
  );
  const usedNames = new Set([String(titlePropertyName || "").trim().toLowerCase()]);
  const resolved = {};

  for (const [key, definition] of Object.entries(PROPERTY_DEFINITIONS)) {
    const propertyName = resolveConfiguredPropertyName({
      properties,
      preferredName: definition.preferredName,
      fallbackName: definition.fallbackName,
      expectedType: definition.type,
      blockedNames,
      usedNames
    });

    resolved[key] = propertyName;
    usedNames.add(propertyName.toLowerCase());
    blockedNames.add(propertyName.toLowerCase());
  }

  return resolved;
}

function resolveConfiguredPropertyName({
  properties,
  preferredName,
  fallbackName,
  expectedType,
  blockedNames,
  usedNames
}) {
  const existingPreferred = findPropertyNameCaseInsensitive(properties, preferredName);

  if (existingPreferred && properties[existingPreferred]?.type === expectedType) {
    return existingPreferred;
  }

  const existingFallback = fallbackName
    ? findPropertyNameCaseInsensitive(properties, fallbackName)
    : "";

  if (
    existingFallback &&
    properties[existingFallback]?.type === expectedType &&
    !usedNames.has(existingFallback.toLowerCase())
  ) {
    return existingFallback;
  }

  const normalizedPreferred = String(preferredName || "").trim().toLowerCase();

  if (!usedNames.has(normalizedPreferred) && !blockedNames.has(normalizedPreferred)) {
    return preferredName;
  }

  return buildUniquePropertyName(fallbackName || preferredName, blockedNames);
}

function findPropertyNameCaseInsensitive(properties, targetName) {
  const normalizedTarget = String(targetName || "").trim().toLowerCase();

  for (const name of Object.keys(properties || {})) {
    if (String(name || "").trim().toLowerCase() === normalizedTarget) {
      return name;
    }
  }

  return "";
}

function buildUniquePropertyName(baseName, blockedNames) {
  const trimmedBase = String(baseName || "").trim();
  const normalizedBase = trimmedBase.toLowerCase();

  if (!blockedNames.has(normalizedBase)) {
    return trimmedBase;
  }

  let suffix = 2;

  while (true) {
    const candidate = `${trimmedBase} ${suffix}`;
    const normalizedCandidate = candidate.toLowerCase();

    if (!blockedNames.has(normalizedCandidate)) {
      return candidate;
    }

    suffix += 1;
  }
}

function getLeadName(record) {
  return firstNonEmpty([
    record.Name,
    record.name,
    [record.firstName, record.lastName].filter(Boolean).join(" ").trim(),
    record.companyName,
    record.linkedinProfileUrl
  ]);
}

function getLeadUrl(record) {
  return firstNonEmpty([
    normalizeUrl(record.linkedinProfileUrl),
    normalizeUrl(record["Lead URL"]),
    normalizeUrl(record.profileUrl)
  ]);
}

function normalizeUrl(value) {
  const text = String(value || "").trim();

  if (!text) {
    return null;
  }

  return /^https?:\/\//i.test(text) ? text : null;
}

function buildRichTextArray(value) {
  const text = truncate(String(value || "").trim(), 1900);

  if (!text) {
    return [];
  }

  return [
    {
      type: "text",
      text: {
        content: text
      }
    }
  ];
}

function flattenPageProperties(page) {
  const record = {
    notion_page_id: page.id,
    notion_url: page.url
  };

  for (const [name, property] of Object.entries(page.properties || {})) {
    record[name] = extractPropertyValue(property);
  }

  return record;
}

function extractPropertyValue(property) {
  if (!property || !property.type) {
    return "";
  }

  switch (property.type) {
    case "title":
      return getPlainText(property.title);
    case "rich_text":
      return getPlainText(property.rich_text);
    case "url":
      return property.url || "";
    case "status":
      return property.status?.name || "";
    case "select":
      return property.select?.name || "";
    default:
      return "";
  }
}

function getPlainText(items) {
  return (items || [])
    .map((item) => item.plain_text || item.text?.content || "")
    .filter(Boolean)
    .join("");
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

function truncate(value, maxLength) {
  const text = String(value || "");

  if (text.length <= maxLength) {
    return text;
  }

  return `${text.slice(0, Math.max(0, maxLength - 3)).trimEnd()}...`;
}

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}
