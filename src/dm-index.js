import path from "node:path";
import { readFile } from "node:fs/promises";
import { setDefaultResultOrder } from "node:dns";
import { ensureRuntimeDirectories, loadConfig, PROMPT_PLACEHOLDER } from "./config.js";
import { createLogger } from "./logger.js";
import { createDmNotionSync } from "./notion-dm.js";

const DEFAULT_DM_DATABASE_ID = "3359a2df-3f5c-8033-83e2-e639ed9dd635";
const DM_RESPONSE_SCHEMA = {
  pain_hook: "short pain statement",
  personalized_line: "short personalized phrase"
};

try {
  setDefaultResultOrder("ipv4first");
  const maxLeads = parsePositiveInteger(process.argv[2]);
  const baseConfig = await loadConfig(process.cwd());
  await ensureRuntimeDirectories(baseConfig);
  const config = await buildDmConfig(baseConfig);
  const logger = createLogger(baseConfig.logsDir);

  await logger.info(
    maxLeads
      ? `Starting DM personalization run with a limit of ${maxLeads} lead(s).`
      : "Starting DM personalization run."
  );

  const promptText = await loadDmPersonalizerPrompt(config.promptFile);
  const notionSync = await createDmNotionSync({
    notionToken: baseConfig.notionToken,
    databaseId: config.databaseId,
    logger
  });
  const pages = await notionSync.listTargetPages({ maxLeads });

  if (pages.length === 0) {
    await logger.info("No eligible Notion leads found for DM personalization.");
    process.exit(0);
  }

  await logger.info(`Queued ${pages.length} Notion lead(s) for DM personalization.`);

  for (let index = 0; index < pages.length; index += 1) {
    const page = pages[index];
    const record = notionSync.flattenPage(page);
    const label = describeNotionLead(record);

    await logger.info(
      `Processing DM row ${index + 1}/${pages.length}${label ? ` (${label})` : ""}.`
    );

    try {
      const fields = await generateDmFields({
        config: baseConfig,
        promptText,
        leadRecord: record
      });

      await notionSync.updateDmFields(page.id, fields);
      await logger.info(
        `Updated DM fields for row ${index + 1}/${pages.length}.`
      );
    } catch (error) {
      await logger.error(
        `DM row ${index + 1}/${pages.length} failed${label ? ` (${label})` : ""}: ${formatError(error)}`
      );
    }
  }

  await logger.info("DM personalization run finished.");
  process.exit(0);
} catch (error) {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
}

async function buildDmConfig(baseConfig) {
  const promptFile = path.resolve(
    baseConfig.cwd,
    process.env.DM_PERSONALIZER_PROMPT_FILE || "./config/dm_personalizer_prompt.txt"
  );
  const databaseId = normalizeNotionDatabaseId(
    process.env.DM_NOTION_DATABASE_ID || DEFAULT_DM_DATABASE_ID
  );

  return {
    promptFile,
    databaseId
  };
}

async function loadDmPersonalizerPrompt(promptFile) {
  const prompt = (await readFile(promptFile, "utf8")).trim();

  if (!prompt || prompt.includes(PROMPT_PLACEHOLDER) || /TODO_ADD_YOUR/i.test(prompt)) {
    throw new Error(`Update ${promptFile} with your real DM personalization prompt before running.`);
  }

  return prompt;
}

async function generateDmFields({ config, promptText, leadRecord }) {
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

    return {
      pain_hook: normalizeShortField(parsed.pain_hook ?? parsed.painHook ?? "", 30),
      personalized_line: normalizeShortField(
        parsed.personalized_line ?? parsed.personalizedLine ?? "",
        20
      )
    };
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
    "You write DM personalization fields from Notion lead records.",
    "Return only valid JSON.",
    "Do not wrap the JSON in markdown.",
    "Use this exact schema:",
    JSON.stringify(DM_RESPONSE_SCHEMA, null, 2)
  ].join("\n");
}

function buildUserPrompt(promptText, leadRecord) {
  return [
    "Follow these DM personalization instructions exactly:",
    promptText,
    "",
    "Lead record:",
    JSON.stringify(leadRecord, null, 2),
    "",
    "Return only JSON with pain_hook and personalized_line."
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
  } catch {
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

function normalizeShortField(value, maxWords) {
  const cleaned = String(value || "").replace(/\s+/g, " ").trim().replace(/^["']|["']$/g, "");

  if (!cleaned) {
    return "";
  }

  const words = cleaned.split(" ").filter(Boolean);

  if (words.length <= maxWords) {
    return cleaned;
  }

  return words.slice(0, maxWords).join(" ");
}

function describeNotionLead(record) {
  const candidates = [
    record.Name,
    record["Full name"],
    record["Company Name"],
    record["Lead URL"]
  ];

  return candidates.find((value) => String(value || "").trim()) || "";
}

function formatError(error, maxLength = 300) {
  const message =
    error instanceof Error ? error.message : typeof error === "string" ? error : JSON.stringify(error);

  if (message.length <= maxLength) {
    return message;
  }

  return `${message.slice(0, Math.max(0, maxLength - 3)).trimEnd()}...`;
}

function parsePositiveInteger(value) {
  if (value === undefined) {
    return null;
  }

  const parsed = Number(value);

  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error("Usage: node src/dm-index.js [max-leads]");
  }

  return parsed;
}

function normalizeNotionDatabaseId(value) {
  const raw = String(value || "").trim();

  if (!raw) {
    return "";
  }

  const match = raw.match(/[0-9a-fA-F]{32}|[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}/);

  if (!match) {
    return raw;
  }

  const compact = match[0].replace(/-/g, "").toLowerCase();

  return [
    compact.slice(0, 8),
    compact.slice(8, 12),
    compact.slice(12, 16),
    compact.slice(16, 20),
    compact.slice(20)
  ].join("-");
}

function truncate(value, maxLength) {
  const text = String(value || "");

  if (text.length <= maxLength) {
    return text;
  }

  return `${text.slice(0, Math.max(0, maxLength - 3)).trimEnd()}...`;
}
