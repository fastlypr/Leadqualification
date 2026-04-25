import { spawn } from "node:child_process";
import path from "node:path";
import {
  access,
  copyFile,
  readFile,
  rename,
  readdir,
  unlink,
  writeFile
} from "node:fs/promises";
import { PROMPT_PLACEHOLDER } from "./config.js";
import { mergeHeaders, parseCsv, stringifyCsv } from "./csv.js";
import { importGoogleSheetIntoInput } from "./google-sheet.js";
import { generateDm } from "./ollama-dm.js";
import { createDmWriterNotionSync } from "./notion-dm-writer.js";
import { createTaskQueue } from "./queue.js";

const OUTPUT_COLUMNS = ["dm_text", "dm_status", "processed_at", "processing_error"];

const DM_INPUT_FIELD_ORDER = [
  "Name",
  "Lead category",
  "Qualification",
  "Qualification note",
  "companyIndustry",
  "companyName",
  "companyWebsite",
  "firstName",
  "lastName",
  "linkedinCompanyDescription",
  "linkedinCompanySpecialities",
  "linkedinCompanyTagline",
  "linkedinCompanyWebsite",
  "linkedinDescription",
  "linkedinHeadline",
  "linkedinJobDescription",
  "linkedinJobLocation",
  "linkedinJobTitle",
  "linkedinProfileUrl",
  "location"
];

export async function processPendingDmFiles({ config, logger, maxLeads = null }) {
  const resumableStatePaths = await listStateFiles(config.processingDir);
  let inputCsvPaths = await listCsvFiles(config.inputDir);
  const runState = {
    maxLeads,
    processedLeadCount: 0,
    remainingLeadCount: Number.isInteger(maxLeads) ? maxLeads : Number.POSITIVE_INFINITY
  };

  if (resumableStatePaths.length === 0 && inputCsvPaths.length === 0 && config.googleSheetUrl) {
    await importGoogleSheetIntoInput({ config, logger });
    inputCsvPaths = await listCsvFiles(config.inputDir);
  }

  if (resumableStatePaths.length === 0 && inputCsvPaths.length === 0) {
    await logger.info("No DM source CSV files waiting in the DM input or processing folders.");
    return;
  }

  const promptText = await loadDmPrompt(config.promptFile);
  const notionSync = await createDmWriterNotionSync({ config, logger });

  for (const statePath of resumableStatePaths) {
    try {
      const result = await processExistingJob(statePath, promptText, config, logger, runState, notionSync);

      if (result.limitReached) {
        await logger.info(`Stopped after ${runState.processedLeadCount} processed DM row(s).`);
        return;
      }
    } catch (error) {
      await logger.error(`Failed to resume DM job ${path.basename(statePath)}: ${formatError(error)}`);
    }
  }

  const freshInputCsvPaths = await listCsvFiles(config.inputDir);

  for (const inputPath of freshInputCsvPaths) {
    try {
      const result = await processNewInput(inputPath, promptText, config, logger, runState, notionSync);

      if (result.limitReached) {
        await logger.info(`Stopped after ${runState.processedLeadCount} processed DM row(s).`);
        return;
      }
    } catch (error) {
      await logger.error(`Failed to process DM file ${path.basename(inputPath)}: ${formatError(error)}`);
    }
  }
}

async function loadDmPrompt(promptFile) {
  const prompt = (await readFile(promptFile, "utf8")).trim();

  if (!prompt || prompt.includes(PROMPT_PLACEHOLDER) || /TODO_ADD_YOUR/i.test(prompt)) {
    throw new Error(`Update ${promptFile} with your real DM writer prompt before running.`);
  }

  return prompt;
}

async function processExistingJob(statePath, promptText, config, logger, runState, notionSync) {
  const state = reviveState(JSON.parse(await readFile(statePath, "utf8")), config);

  await logger.info(`Resuming DM file ${state.sourceName} from row ${state.nextRowIndex + 1}.`);
  return processJob(state, promptText, config, logger, runState, notionSync);
}

async function processNewInput(inputPath, promptText, config, logger, runState, notionSync) {
  const sourceName = path.basename(inputPath);
  const baseName = path.basename(inputPath, path.extname(inputPath));
  const stamp = fileStamp();
  const jobId = `${slugify(baseName)}-${stamp}`;
  const state = {
    jobId,
    sourceName,
    originalPath: path.join(config.processingDir, `${jobId}.original.csv`),
    workingPath: path.join(config.processingDir, `${jobId}.working.csv`),
    statePath: path.join(config.processingDir, `${jobId}.state.json`),
    outputPath: await buildAvailablePath(config.outputDir, `${baseName}.dms.csv`),
    donePath: await buildAvailablePath(config.doneDir, sourceName),
    failedOriginalPath: await buildAvailablePath(config.failedDir, `${baseName}.${stamp}.original.csv`),
    failedWorkingPath: await buildAvailablePath(config.failedDir, `${baseName}.${stamp}.working.csv`),
    failedStatePath: await buildAvailablePath(config.failedDir, `${baseName}.${stamp}.state.json`),
    nextRowIndex: 0,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString()
  };

  await rename(inputPath, state.originalPath);

  try {
    const { headers, records } = parseCsv(await readFile(state.originalPath, "utf8"));

    if (headers.length === 0) {
      throw new Error("CSV is empty or missing a header row.");
    }

    const workingHeaders = mergeHeaders(headers, OUTPUT_COLUMNS);
    const workingRecords = records.map((record) => withOutputColumns(record));

    await writeCsv(state.workingPath, workingHeaders, workingRecords);
    await writeState(state);
    await logger.info(`Started DM file ${sourceName} with ${workingRecords.length} lead(s).`);
    return processJob(state, promptText, config, logger, runState, notionSync);
  } catch (error) {
    await logger.error(`Could not initialize DM file ${sourceName}: ${formatError(error)}`);
    await moveFileIfPresent(state.originalPath, state.failedOriginalPath);
    await moveFileIfPresent(state.workingPath, state.failedWorkingPath);
    await moveFileIfPresent(state.statePath, state.failedStatePath);
    throw error;
  }
}

async function processJob(state, promptText, config, logger, runState, notionSync) {
  const { headers, records } = parseCsv(await readFile(state.workingPath, "utf8"));
  const workingHeaders = mergeHeaders(headers, OUTPUT_COLUMNS);
  const total = records.length;
  const startIndex = Math.min(state.nextRowIndex || 0, total);
  const queue = createTaskQueue({ concurrency: 1 });
  const queuedItems = [];
  let limitReached = false;

  for (let index = startIndex; index < total; index += 1) {
    const row = withOutputColumns(records[index]);

    if (isAlreadyProcessed(row)) {
      records[index] = row;
      state.nextRowIndex = index + 1;
      state.updatedAt = new Date().toISOString();
      await writeState(state);
      continue;
    }

    if (runState.remainingLeadCount <= 0) {
      limitReached = true;
      break;
    }

    const queuePosition = queuedItems.length + 1;
    queuedItems.push({
      index,
      queuePosition,
      row,
      label: describeLead(row)
    });
    runState.remainingLeadCount -= 1;
  }

  if (queuedItems.length > 0) {
    if (notionSync) {
      await notionSync.prepareSchema(queuedItems[0].row);
      await logger.info(`Prepared DM Notion fields for ${state.sourceName} before writing starts.`);
    }

    await logger.info(
      `Queued ${queuedItems.length} DM lead(s) from ${state.sourceName} with request concurrency 1.`
    );
  }

  const queueTasks = queuedItems.map((item) =>
    queue.add(async () => {
      await logger.info(
        `Processing DM row ${item.index + 1}/${total}${item.label ? ` (${item.label})` : ""}. Queue ${item.queuePosition}/${queuedItems.length}.`
      );

      const outcome = await processLeadWithRetries({
        item,
        total,
        config,
        promptText,
        notionSync,
        logger
      });

      if (outcome.syncResult) {
        await logger.info(
          `Synced DM row ${item.index + 1}/${total} to Notion as ${outcome.syncResult.action}.`
        );
      }

      if (outcome.failed) {
        await logger.error(
          `DM row ${item.index + 1}/${total} failed${item.label ? ` (${item.label})` : ""}: ${item.row.processing_error}`
        );
      } else if (outcome.skippedExisting) {
        await logger.info(
          `Skipped DM row ${item.index + 1}/${total}${item.label ? ` (${item.label})` : ""} because a DM already exists in Notion for this lead.`
        );
      } else {
        await logger.info(`Completed DM row ${item.index + 1}/${total}.`);
      }

      records[item.index] = item.row;
      state.nextRowIndex = item.index + 1;
      state.updatedAt = new Date().toISOString();
      runState.processedLeadCount += 1;
      await writeCsv(state.workingPath, workingHeaders, records);
      await writeState(state);
    })
  );

  await Promise.all(queueTasks);
  await queue.onIdle();

  if (limitReached) {
    await logger.info(
      `Paused ${state.sourceName} at row ${state.nextRowIndex + 1}/${total} after reaching the DM run limit.`
    );
    return { completed: false, limitReached: true };
  }

  await copyFile(state.originalPath, state.donePath);
  await unlink(state.originalPath);
  await copyFile(state.workingPath, state.outputPath);
  await unlink(state.workingPath);
  await unlink(state.statePath);

  if (notionSync) {
    await logger.info(
      `Finished ${state.sourceName}. DM output was saved to ${path.basename(state.outputPath)} and synced to Notion.`
    );
  } else {
    await logger.info(
      `Finished ${state.sourceName}. DM output saved to ${path.basename(state.outputPath)}.`
    );
  }

  return { completed: true, limitReached: false };
}

async function processLeadWithRetries({ item, total, config, promptText, notionSync, logger }) {
  const maxAttempts = Math.max(1, Number(config.leadMaxAttempts) || 1);
  let lastError = null;

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      const outcome = await processLeadAttempt({
        item,
        total,
        config,
        promptText,
        notionSync
      });

      return {
        failed: false,
        skippedExisting: Boolean(outcome.skippedExisting),
        syncResult: outcome.syncResult || null
      };
    } catch (error) {
      lastError = error;
      const formattedError = formatError(error, 500);
      const retryable = shouldRetryLeadError(error);
      const hasAttemptsLeft = attempt < maxAttempts;

      await logger.error(
        `Attempt ${attempt}/${maxAttempts} failed for DM row ${item.index + 1}/${total}${item.label ? ` (${item.label})` : ""}: ${formattedError}`
      );

      if (!(retryable && hasAttemptsLeft)) {
        break;
      }

      if (shouldRestartOllamaForError(error)) {
        await restartOllama({ config, logger, rowNumber: item.index + 1, total, attempt });
      }

      await logger.info(
        `Retrying DM row ${item.index + 1}/${total}${item.label ? ` (${item.label})` : ""} with attempt ${attempt + 1}/${maxAttempts}.`
      );
    }
  }

  markLeadAsFailed(item.row, lastError);
  const syncResult = await syncFailedLeadToNotion({ item, total, notionSync, logger });

  return {
    failed: true,
    syncResult
  };
}

async function processLeadAttempt({ item, total, config, promptText, notionSync }) {
  let existingLead = null;

  if (notionSync) {
    existingLead = await notionSync.findExistingLead(item.row);

    if (existingLead && hasExistingDm(existingLead)) {
      applyExistingDmResult(item.row, existingLead);
      return { skippedExisting: true, syncResult: null };
    }
  }

  const dmConfig = {
    ...config,
    requestTimeoutMs: getLeadAttemptTimeoutMs(config)
  };
  const dmText = await generateDm({
    config: dmConfig,
    promptText,
    leadRecord: buildDmInput(item.row)
  });

  applyDmResult(item.row, dmText);

  if (!notionSync) {
    return { syncResult: null };
  }

  const syncResult = await notionSync.upsertDm(item.row, existingLead?.pageId || "");
  return { syncResult };
}

function hasExistingDm(existingLead) {
  const dmText = String(existingLead?.dm_text || "").trim();
  const dmStatus = String(existingLead?.dm_status || "").trim().toLowerCase();

  return Boolean(dmText) && dmStatus !== "failed";
}

function applyDmResult(row, dmText) {
  row.dm_text = String(dmText || "").trim();
  row.dm_status = row.dm_text ? "Generated" : "Failed";
  row.processing_error = "";
  row.processed_at = new Date().toISOString();
}

function applyExistingDmResult(row, existingLead) {
  row.dm_text = existingLead.dm_text || row.dm_text || "";
  row.dm_status = existingLead.dm_status || (row.dm_text ? "Generated" : row.dm_status || "");
  row.processing_error = "";
  row.processed_at = new Date().toISOString();
}

function markLeadAsFailed(row, error) {
  row.dm_text = row.dm_text || "";
  row.dm_status = "Failed";
  row.processing_error = formatError(error, 500);
  row.processed_at = new Date().toISOString();
}

async function syncFailedLeadToNotion({ item, total, notionSync, logger }) {
  if (!notionSync) {
    return null;
  }

  try {
    return await notionSync.upsertDm(item.row);
  } catch (error) {
    await logger.error(
      `Could not sync Failed DM status for row ${item.index + 1}/${total}${item.label ? ` (${item.label})` : ""}: ${formatError(error, 400)}`
    );
    return null;
  }
}

function buildDmInput(record) {
  const source = stripOutputColumns(record);
  const selected = {
    Name: firstNonEmpty([
      source.Name,
      source.name,
      [source.firstName, source.lastName].filter(Boolean).join(" ").trim()
    ]),
    "Lead category": firstNonEmpty([source["Lead category"], source.lead_category]),
    Qualification: firstNonEmpty([source.Qualification, source.qualification_status]),
    "Qualification note": firstNonEmpty([source["Qualification note"], source.qualification_note]),
    companyIndustry: source.companyIndustry,
    companyName: source.companyName,
    companyWebsite: source.companyWebsite,
    firstName: source.firstName,
    lastName: source.lastName,
    linkedinCompanyDescription: source.linkedinCompanyDescription,
    linkedinCompanySpecialities: source.linkedinCompanySpecialities,
    linkedinCompanyTagline: source.linkedinCompanyTagline,
    linkedinCompanyWebsite: source.linkedinCompanyWebsite,
    linkedinDescription: source.linkedinDescription,
    linkedinHeadline: source.linkedinHeadline,
    linkedinJobDescription: source.linkedinJobDescription,
    linkedinJobLocation: source.linkedinJobLocation,
    linkedinJobTitle: source.linkedinJobTitle,
    linkedinProfileUrl: source.linkedinProfileUrl,
    location: source.location
  };

  for (const field of Object.keys(selected)) {
    if (!String(selected[field] ?? "").trim()) {
      delete selected[field];
    }
  }

  return orderFields(selected, DM_INPUT_FIELD_ORDER);
}

function withOutputColumns(record) {
  const nextRecord = { ...record };

  for (const column of OUTPUT_COLUMNS) {
    nextRecord[column] = nextRecord[column] ?? "";
  }

  return nextRecord;
}

function stripOutputColumns(record) {
  const nextRecord = { ...record };

  for (const column of OUTPUT_COLUMNS) {
    delete nextRecord[column];
  }

  return nextRecord;
}

function orderFields(record, fieldOrder) {
  const ordered = {};

  for (const field of fieldOrder) {
    if (Object.hasOwn(record, field)) {
      ordered[field] = record[field];
    }
  }

  for (const [field, value] of Object.entries(record)) {
    if (!Object.hasOwn(ordered, field)) {
      ordered[field] = value;
    }
  }

  return ordered;
}

function isAlreadyProcessed(record) {
  const processedAt = String(record.processed_at || "").trim();
  const dmText = String(record.dm_text || "").trim();
  const dmStatus = String(record.dm_status || "").trim();
  const error = String(record.processing_error || "").trim();

  return Boolean(processedAt && (dmText || dmStatus || error));
}

function describeLead(record) {
  return firstNonEmpty([
    record.Name,
    [record.firstName, record.lastName].filter(Boolean).join(" ").trim(),
    record.companyName,
    record.linkedinProfileUrl
  ]);
}

function reviveState(rawState, config) {
  const baseName = path.basename(rawState.sourceName || "dm-file", ".csv");
  const stamp = fileStamp();

  return {
    ...rawState,
    failedOriginalPath:
      rawState.failedOriginalPath ||
      path.join(config.failedDir, `${baseName}.${stamp}.original.csv`),
    failedWorkingPath:
      rawState.failedWorkingPath ||
      path.join(config.failedDir, `${baseName}.${stamp}.working.csv`),
    failedStatePath:
      rawState.failedStatePath ||
      path.join(config.failedDir, `${baseName}.${stamp}.state.json`)
  };
}

async function writeCsv(filePath, headers, records) {
  const normalizedRecords = records.map((record) => {
    const nextRecord = {};

    for (const header of headers) {
      nextRecord[header] = record[header] ?? "";
    }

    return nextRecord;
  });

  await writeFile(filePath, stringifyCsv(headers, normalizedRecords), "utf8");
}

async function writeState(state) {
  await writeFile(state.statePath, `${JSON.stringify(state, null, 2)}\n`, "utf8");
}

async function listCsvFiles(dirPath) {
  const entries = await readdir(dirPath, { withFileTypes: true });

  return entries
    .filter((entry) => entry.isFile() && entry.name.toLowerCase().endsWith(".csv"))
    .map((entry) => path.join(dirPath, entry.name))
    .sort((left, right) => left.localeCompare(right));
}

async function listStateFiles(dirPath) {
  const entries = await readdir(dirPath, { withFileTypes: true });

  return entries
    .filter((entry) => entry.isFile() && entry.name.endsWith(".state.json"))
    .map((entry) => path.join(dirPath, entry.name))
    .sort((left, right) => left.localeCompare(right));
}

async function buildAvailablePath(dirPath, preferredName) {
  const parsed = path.parse(preferredName);
  let candidate = path.join(dirPath, preferredName);

  if (!(await exists(candidate))) {
    return candidate;
  }

  const stamp = fileStamp();
  let counter = 1;

  while (true) {
    const name = `${parsed.name}.${stamp}.${counter}${parsed.ext}`;
    candidate = path.join(dirPath, name);

    if (!(await exists(candidate))) {
      return candidate;
    }

    counter += 1;
  }
}

async function exists(filePath) {
  try {
    await access(filePath);
    return true;
  } catch {
    return false;
  }
}

async function moveFileIfPresent(fromPath, toPath) {
  if (await exists(fromPath)) {
    await rename(fromPath, toPath);
  }
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

function slugify(value) {
  return String(value || "dm-file")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "") || "dm-file";
}

function fileStamp() {
  return new Date().toISOString().replace(/[:.]/g, "-");
}

function getLeadAttemptTimeoutMs(config) {
  const stallTimeout = Number(config.leadStallTimeoutMs) || 10 * 60 * 1000;
  const requestTimeout = Number(config.requestTimeoutMs) || 0;

  if (requestTimeout > 0) {
    return Math.min(stallTimeout, requestTimeout);
  }

  return stallTimeout;
}

function shouldRetryLeadError(error) {
  const message = formatError(error, 1000).toLowerCase();

  if (
    message.includes("validation_error") ||
    message.includes("exists but is type") ||
    message.includes("check that the integration token is valid") ||
    message.includes("notion request failed with 400")
  ) {
    return false;
  }

  return true;
}

function shouldRestartOllamaForError(error) {
  const message = formatError(error, 1000).toLowerCase();

  return (
    message.includes("ollama request timed out") ||
    message.includes("ollama request failed") ||
    message.includes("ollama response did not include assistant content")
  );
}

async function restartOllama({ config, logger, rowNumber, total, attempt }) {
  const command = String(config.ollamaRestartCommand || "").trim();

  if (!command) {
    await logger.info(
      `Skipping Ollama restart for DM row ${rowNumber}/${total} attempt ${attempt} because no restart command is configured.`
    );
    return;
  }

  await logger.info(
    `Restarting Ollama before retrying DM row ${rowNumber}/${total} using: ${command}`
  );

  try {
    await runShellCommand(command);
    await logger.info(`Ollama restart command finished before retrying DM row ${rowNumber}/${total}.`);
  } catch (error) {
    await logger.error(
      `Ollama restart command failed before retrying DM row ${rowNumber}/${total}: ${formatError(error, 400)}`
    );
  }
}

async function runShellCommand(command) {
  await new Promise((resolve, reject) => {
    const child = spawn("/bin/sh", ["-lc", command], {
      stdio: ["ignore", "pipe", "pipe"]
    });
    let stdout = "";
    let stderr = "";

    child.stdout.on("data", (chunk) => {
      stdout += String(chunk);
    });

    child.stderr.on("data", (chunk) => {
      stderr += String(chunk);
    });

    child.on("error", reject);
    child.on("close", (code) => {
      if (code === 0) {
        resolve();
        return;
      }

      const detail = [stderr.trim(), stdout.trim()].filter(Boolean).join(" ");
      reject(new Error(detail || `Command exited with status ${code}.`));
    });
  });
}

function formatError(error, maxLength = 300) {
  const message =
    error instanceof Error ? error.message : typeof error === "string" ? error : JSON.stringify(error);

  if (message.length <= maxLength) {
    return message;
  }

  return `${message.slice(0, Math.max(0, maxLength - 3)).trimEnd()}...`;
}
