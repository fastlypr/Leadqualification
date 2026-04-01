import path from "node:path";
import { mkdir, readFile } from "node:fs/promises";

export const PROMPT_PLACEHOLDER = "TODO_ADD_YOUR_PROMPT_HERE";

export async function loadConfig(cwd = process.cwd()) {
  const settingsPath = path.resolve(cwd, "config/settings.json");
  const raw = await readFile(settingsPath, "utf8");
  const parsed = JSON.parse(raw);

  const config = {
    cwd,
    settingsPath,
    ollamaUrl: parsed.ollamaUrl,
    model: parsed.model || "mistral",
    requestTimeoutMs: parseRequestTimeout(parsed.requestTimeoutMs),
    maxNoteLength: Number(parsed.maxNoteLength || 220),
    maxOutreachLength: Number(parsed.maxOutreachLength || 500),
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

    if (normalized === "none" || normalized === "infinite" || normalized === "infinity") {
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
