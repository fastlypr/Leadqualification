const NOTION_API_BASE_URL = "https://api.notion.com/v1";
const NOTION_VERSION = "2022-06-28";

const PROPERTY_NAMES = {
  leadUrl: "Lead URL",
  leadCategory: "Lead category",
  industry: "Industry",
  qualification: "Qualification",
  qualificationNote: "Qualification note",
  painHook: "Pain hook",
  personalizedLine: "Personalized line"
};

export async function createNotionSync({ config, logger }) {
  if (!config.notionToken || !config.notionDatabaseId) {
    return null;
  }

  const client = createNotionClient(config.notionToken);
  const schema = await ensureDatabaseSchema(client, config.notionDatabaseId);

  await logger.info(
    `Notion sync enabled for database ${config.notionDatabaseId}. Title property: ${schema.titlePropertyName}.`
  );

  return {
    async upsertLead(record) {
      const pageId = await findExistingPageId(client, config.notionDatabaseId, record);
      const properties = buildPageProperties(schema.titlePropertyName, record);

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
        const delayMs = Number.isFinite(retryAfter) && retryAfter > 0 ? retryAfter * 1000 : 1000 * (attempt + 1);
        await sleep(delayMs);
        return this.request(method, path, body, attempt + 1);
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

async function ensureDatabaseSchema(client, databaseId) {
  const database = await client.request("GET", `/databases/${databaseId}`);
  const titlePropertyName = findTitlePropertyName(database.properties);

  if (!titlePropertyName) {
    throw new Error("Notion database does not have a title property.");
  }

  const updates = {};
  ensurePropertyType(database.properties, PROPERTY_NAMES.leadUrl, "url", updates);
  ensurePropertyType(database.properties, PROPERTY_NAMES.leadCategory, "rich_text", updates);
  ensurePropertyType(database.properties, PROPERTY_NAMES.industry, "rich_text", updates);
  ensurePropertyType(database.properties, PROPERTY_NAMES.qualification, "rich_text", updates);
  ensurePropertyType(database.properties, PROPERTY_NAMES.qualificationNote, "rich_text", updates);
  ensurePropertyType(database.properties, PROPERTY_NAMES.painHook, "rich_text", updates);
  ensurePropertyType(database.properties, PROPERTY_NAMES.personalizedLine, "rich_text", updates);

  if (Object.keys(updates).length > 0) {
    await client.request("PATCH", `/databases/${databaseId}`, { properties: updates });
  }

  return {
    titlePropertyName
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

function findTitlePropertyName(properties) {
  for (const [name, property] of Object.entries(properties || {})) {
    if (property?.type === "title") {
      return name;
    }
  }

  return "";
}

async function findExistingPageId(client, databaseId, record) {
  const leadUrl = getLeadUrl(record);

  if (!leadUrl) {
    return null;
  }

  const query = await client.request("POST", `/databases/${databaseId}/query`, {
    filter: {
      property: PROPERTY_NAMES.leadUrl,
      url: {
        equals: leadUrl
      }
    },
    page_size: 1
  });

  return query?.results?.[0]?.id || null;
}

function buildPageProperties(titlePropertyName, record) {
  return {
    [titlePropertyName]: {
      title: buildRichTextArray(getFullName(record) || "Unnamed Lead")
    },
    [PROPERTY_NAMES.leadUrl]: {
      url: getLeadUrl(record) || null
    },
    [PROPERTY_NAMES.leadCategory]: {
      rich_text: buildRichTextArray(record.lead_category)
    },
    [PROPERTY_NAMES.industry]: {
      rich_text: buildRichTextArray(record.industry)
    },
    [PROPERTY_NAMES.qualification]: {
      rich_text: buildRichTextArray(record.qualification_status)
    },
    [PROPERTY_NAMES.qualificationNote]: {
      rich_text: buildRichTextArray(record.qualification_note)
    },
    [PROPERTY_NAMES.painHook]: {
      rich_text: buildRichTextArray(record.pain_hook)
    },
    [PROPERTY_NAMES.personalizedLine]: {
      rich_text: buildRichTextArray(record.personalized_line)
    }
  };
}

function getFullName(record) {
  return firstNonEmpty([
    record.fullName,
    record.name,
    [record.firstName, record.lastName].filter(Boolean).join(" "),
    record.companyName
  ]);
}

function getLeadUrl(record) {
  return firstNonEmpty([
    record.defaultProfileUrl,
    record.linkedInProfileUrl,
    record.profileUrl
  ]);
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
