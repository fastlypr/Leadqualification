import path from "node:path";
import { mkdir, readFile } from "node:fs/promises";

export const PROMPT_PLACEHOLDER = "TODO_ADD_YOUR_PROMPT_HERE";

export async function loadConfig(cwd = process.cwd()) {
  const envPath = path.resolve(cwd, ".env");
  await loadDotEnv(envPath);

  const settingsPath = path.resolve(cwd, "config/settings.json");
  const raw = await readFile(settingsPath, "utf8");
  const parsed = JSON.parse(raw);

  const config = {
    cwd,
    envPath,
    settingsPath,
    ollamaUrl: getEnvValue("OLLAMA_URL") || parsed.ollamaUrl,
    model: getEnvValue("OLLAMA_MODEL") || getEnvValue("MODEL") || parsed.model || "mistral",
    requestTimeoutMs: parseRequestTimeout(
      getEnvValue("REQUEST_TIMEOUT_MS") ?? parsed.requestTimeoutMs
    ),
    maxNoteLength: Number(parsed.maxNoteLength || 220),
    maxOutreachLength: Number(parsed.maxOutreachLength || 500),
    notionToken: getEnvValue("NOTION_TOKEN") || null,
    notionDatabaseId: normalizeNotionDatabaseId(getEnvValue("NOTION_DATABASE_ID") || null),
    promptFile: path.resolve(cwd, parsed.promptFile || "./config/prompt.txt"),
    inputDir: path.resolve(cwd, parsed.inputDir || "./input"),
    processingDir: path.resolve(cwd, parsed.processingDir || "./processing"),
    outputDir: path.resolve(cwd, parsed.outputDir || "./output"),
    doneDir: path.resolve(cwd, parsed.doneDir || "./done"),
    failedDir: path.resolve(cwd, parsed.failedDir || "./failed"),
    logsDir: path.resolve(cwd, parsed.logsDir || "./logs")
  };

  if (!config.ollamaUrl) {
    throw new Error("Missing `ollamaUrl` in config/settings.json.");
  }

  return config;
}

function parseRequestTimeout(value) {
  if (value === null || value === undefined || value === "" || value === 0 || value === "0") {
    return null;
  }

  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();

    if (
      normalized === "null" ||
      normalized === "none" ||
      normalized === "infinite" ||
      normalized === "infinity"
    ) {
      return null;
    }
  }

  const parsed = Number(value);

  if (!Number.isFinite(parsed) || parsed < 0) {
    throw new Error(
      "Invalid `requestTimeoutMs` in config/settings.json. Use a positive number of milliseconds or null for no timeout."
    );
  }

  return parsed;
}

export async function ensureRuntimeDirectories(config) {
  const dirs = [
    config.inputDir,
    config.processingDir,
    config.outputDir,
    config.doneDir,
    config.failedDir,
    config.logsDir
  ];

  for (const dir of dirs) {
    await mkdir(dir, { recursive: true });
  }
}

export async function loadPrompt(config) {
  const prompt = (await readFile(config.promptFile, "utf8")).trim();

  if (!prompt || prompt.includes(PROMPT_PLACEHOLDER)) {
    throw new Error(
      `Update ${config.promptFile} with your real qualification prompt before processing leads.`
    );
  }

  return prompt;
}

async function loadDotEnv(envPath) {
  let raw;

  try {
    raw = await readFile(envPath, "utf8");
  } catch (error) {
    if (error && error.code === "ENOENT") {
      return;
    }

    throw error;
  }

  for (const line of raw.split(/\r?\n/)) {
    const trimmed = line.trim();

    if (!trimmed || trimmed.startsWith("#")) {
      continue;
    }

    const normalized = trimmed.startsWith("export ") ? trimmed.slice(7).trim() : trimmed;
    const separatorIndex = normalized.indexOf("=");

    if (separatorIndex <= 0) {
      continue;
    }

    const key = normalized.slice(0, separatorIndex).trim();
    const value = stripWrappingQuotes(normalized.slice(separatorIndex + 1).trim());

    if (!key || process.env[key] !== undefined) {
      continue;
    }

    process.env[key] = value;
  }
}

function getEnvValue(name) {
  const value = process.env[name];

  return value === undefined || value === "" ? null : value;
}

function stripWrappingQuotes(value) {
  if (
    (value.startsWith('"') && value.endsWith('"')) ||
    (value.startsWith("'") && value.endsWith("'"))
  ) {
    return value.slice(1, -1);
  }

  return value;
}

function normalizeNotionDatabaseId(value) {
  const raw = String(value || "").trim();

  if (!raw) {
    return null;
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
