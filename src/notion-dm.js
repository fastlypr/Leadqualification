const NOTION_API_BASE_URL = "https://api.notion.com/v1";
const NOTION_VERSION = "2022-06-28";

const PROPERTY_NAMES = {
  qualification: "Qualification",
  painHook: "Pain hook",
  personalizedLine: "Personalized line"
};

export async function createDmNotionSync({ notionToken, databaseId, logger }) {
  if (!notionToken) {
    throw new Error("Missing NOTION_TOKEN in environment.");
  }

  if (!databaseId) {
    throw new Error("Missing DM Notion database id.");
  }

  const client = createNotionClient(notionToken);
  const schema = await ensureDatabaseSchema(client, databaseId);

  await logger.info(
    `DM Notion sync enabled for database ${databaseId}. Title property: ${schema.titlePropertyName}.`
  );

  return {
    async listTargetPages({ maxLeads = null }) {
      const pages = await listDatabasePages(client, databaseId);
      const eligible = pages.filter((page) => {
        const record = flattenPageProperties(page);
        const qualification = String(record[PROPERTY_NAMES.qualification] || "").trim();
        const painHook = String(record[PROPERTY_NAMES.painHook] || "").trim();
        const personalizedLine = String(record[PROPERTY_NAMES.personalizedLine] || "").trim();

        if (!(qualification === "Qualified" || qualification === "Needs Review")) {
          return false;
        }

        return !(painHook && personalizedLine);
      });

      if (!Number.isInteger(maxLeads)) {
        return eligible;
      }

      return eligible.slice(0, maxLeads);
    },

    flattenPage(page) {
      return flattenPageProperties(page);
    },

    async updateDmFields(pageId, fields) {
      await client.request("PATCH", `/pages/${pageId}`, {
        properties: {
          [PROPERTY_NAMES.painHook]: {
            rich_text: buildRichTextArray(fields.pain_hook)
          },
          [PROPERTY_NAMES.personalizedLine]: {
            rich_text: buildRichTextArray(fields.personalized_line)
          }
        }
      });
    }
  };
}

function createNotionClient(token) {
  return {
    async request(method, path, body, attempt = 0) {
      const url = `${NOTION_API_BASE_URL}${path}`;
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
          `Notion network request failed for ${method} ${url}. ${reason}.`
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
        const delayMs = Number.isFinite(retryAfter) && retryAfter > 0 ? retryAfter * 1000 : 1000 * (attempt + 1);
        await sleep(delayMs);
        return this.request(method, path, body, attempt + 1);
      }

      if (response.status === 401 || response.status === 403 || response.status === 404) {
        throw new Error(
          `Notion request failed with ${response.status}. Check token access and database sharing. ${truncate(errorText, 250)}`
        );
      }

      throw new Error(`Notion request failed with ${response.status}: ${truncate(errorText, 400)}`);
    }
  };
}

async function ensureDatabaseSchema(client, databaseId) {
  const database = await client.request("GET", `/databases/${databaseId}`);
  const titlePropertyName = findTitlePropertyName(database.properties);

  if (!titlePropertyName) {
    throw new Error("Notion database does not have a title property.");
  }

  const updates = {};
  ensurePropertyType(database.properties, PROPERTY_NAMES.painHook, "rich_text", updates);
  ensurePropertyType(database.properties, PROPERTY_NAMES.personalizedLine, "rich_text", updates);

  if (Object.keys(updates).length > 0) {
    await client.request("PATCH", `/databases/${databaseId}`, { properties: updates });
  }

  return { titlePropertyName };
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
  if (type === "rich_text") {
    return { rich_text: {} };
  }

  throw new Error(`Unsupported Notion property type: ${type}`);
}

function findTitlePropertyName(properties) {
  for (const [name, property] of Object.entries(properties || {})) {
    if (property?.type === "title") {
      return name;
    }
  }

  return "";
}

async function listDatabasePages(client, databaseId) {
  const pages = [];
  let cursor = null;

  while (true) {
    const payload = await client.request("POST", `/databases/${databaseId}/query`, {
      page_size: 100,
      start_cursor: cursor || undefined
    });

    pages.push(...(payload.results || []));

    if (!payload.has_more || !payload.next_cursor) {
      break;
    }

    cursor = payload.next_cursor;
  }

  return pages;
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
    case "email":
      return property.email || "";
    case "phone_number":
      return property.phone_number || "";
    case "number":
      return property.number ?? "";
    case "select":
      return property.select?.name || "";
    case "multi_select":
      return (property.multi_select || []).map((item) => item.name).join(", ");
    case "status":
      return property.status?.name || "";
    case "checkbox":
      return property.checkbox ? "true" : "false";
    case "date":
      return property.date?.start || "";
    case "people":
      return (property.people || []).map((item) => item.name || item.id).join(", ");
    case "relation":
      return (property.relation || []).map((item) => item.id).join(", ");
    case "files":
      return (property.files || [])
        .map((item) => item.name || item.external?.url || item.file?.url || "")
        .filter(Boolean)
        .join(", ");
    case "formula":
      return extractFormulaValue(property.formula);
    case "rollup":
      return extractRollupValue(property.rollup);
    case "created_time":
      return property.created_time || "";
    case "last_edited_time":
      return property.last_edited_time || "";
    default:
      return "";
  }
}

function extractFormulaValue(formula) {
  if (!formula) {
    return "";
  }

  if (formula.type === "string") {
    return formula.string || "";
  }

  if (formula.type === "number") {
    return formula.number ?? "";
  }

  if (formula.type === "boolean") {
    return formula.boolean ? "true" : "false";
  }

  if (formula.type === "date") {
    return formula.date?.start || "";
  }

  return "";
}

function extractRollupValue(rollup) {
  if (!rollup) {
    return "";
  }

  if (rollup.type === "number") {
    return rollup.number ?? "";
  }

  if (rollup.type === "date") {
    return rollup.date?.start || "";
  }

  if (rollup.type === "array") {
    return (rollup.array || [])
      .map((item) => extractPropertyValue(item))
      .filter(Boolean)
      .join(", ");
  }

  return "";
}

function getPlainText(items) {
  return (items || [])
    .map((item) => item.plain_text || item.text?.content || "")
    .filter(Boolean)
    .join("");
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
