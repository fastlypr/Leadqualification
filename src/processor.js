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
import { loadPrompt } from "./config.js";
import { mergeHeaders, parseCsv, stringifyCsv } from "./csv.js";
import { importGoogleSheetIntoInput } from "./google-sheet.js";
import { createNotionSync } from "./notion.js";
import { qualifyLead } from "./ollama.js";
import { createTaskQueue } from "./queue.js";

const OUTPUT_COLUMNS = [
  "lead_category",
  "qualification_status",
  "qualification_note",
  "processed_at",
  "processing_error"
];

const QUALIFICATION_INPUT_FIELD_ORDER = [
  "firstName",
  "companyName",
  "linkedinHeadline",
  "linkedinJobTitle",
  "linkedinJobDescription",
  "linkedinDescription",
  "companyIndustry",
  "linkedinCompanyDescription",
  "linkedinCompanyTagline",
  "linkedinCompanySpecialities",
  "linkedinJobLocation",
  "linkedinIsOpenToWorkBadge"
];

export async function processPendingFiles({ config, logger, maxLeads = null }) {
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
    await logger.info("No CSV files waiting in input/ or processing/.");
    return;
  }

  const promptText = await loadPrompt(config);
  const notionSync = await createNotionSync({ config, logger });

  for (const statePath of resumableStatePaths) {
    try {
      const result = await processExistingJob(statePath, promptText, config, logger, runState, notionSync);

      if (result.limitReached) {
        await logger.info(`Stopped after ${runState.processedLeadCount} processed lead(s).`);
        return;
      }
    } catch (error) {
      await logger.error(`Failed to resume job ${path.basename(statePath)}: ${formatError(error)}`);
    }
  }

  const freshInputCsvPaths = await listCsvFiles(config.inputDir);

  for (const inputPath of freshInputCsvPaths) {
    try {
      const result = await processNewInput(inputPath, promptText, config, logger, runState, notionSync);

      if (result.limitReached) {
        await logger.info(`Stopped after ${runState.processedLeadCount} processed lead(s).`);
        return;
      }
    } catch (error) {
      await logger.error(`Failed to process ${path.basename(inputPath)}: ${formatError(error)}`);
    }
  }
}

async function processExistingJob(statePath, promptText, config, logger, runState, notionSync) {
  const state = reviveState(
    JSON.parse(await readFile(statePath, "utf8")),
    config
  );

  await logger.info(`Resuming ${state.sourceName} from row ${state.nextRowIndex + 1}.`);
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
    outputPath: await buildAvailablePath(config.outputDir, `${baseName}.qualified.csv`),
    donePath: await buildAvailablePath(config.doneDir, `${sourceName}`),
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
    await logger.info(`Started ${sourceName} with ${workingRecords.length} lead(s).`);
    return processJob(state, promptText, config, logger, runState, notionSync);
  } catch (error) {
    await logger.error(`Could not initialize ${sourceName}: ${formatError(error)}`);
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
      await logger.info(`Prepared Notion fields for ${state.sourceName} before qualification starts.`);
    }

    await logger.info(
      `Queued ${queuedItems.length} lead(s) from ${state.sourceName} with request concurrency 1.`
    );
  }

  const queueTasks = queuedItems.map((item) =>
    queue.add(async () => {
      await logger.info(
        `Processing ${state.sourceName} row ${item.index + 1}/${total}${item.label ? ` (${item.label})` : ""}. Queue ${item.queuePosition}/${queuedItems.length}.`
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
          `Synced row ${item.index + 1}/${total} to Notion as ${outcome.syncResult.action}.`
        );
      }

      if (outcome.failed) {
        await logger.error(
          `Row ${item.index + 1}/${total} failed${item.label ? ` (${item.label})` : ""}: ${item.row.processing_error}`
        );
      } else if (outcome.skippedExisting) {
        const skipDetail = item.row.qualification_status
          ? ` as ${item.row.qualification_status}`
          : item.row.processing_error
            ? `. ${item.row.processing_error}`
            : "";

        await logger.info(
          `Skipped row ${item.index + 1}/${total}${item.label ? ` (${item.label})` : ""} because this Lead URL already exists in Notion${skipDetail}.`
        );
      } else {
        await logger.info(
          `Completed row ${item.index + 1}/${total} as ${item.row.qualification_status || "Unknown"}${item.row.lead_category ? ` [${item.row.lead_category}]` : ""}.`
        );
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
      `Paused ${state.sourceName} at row ${state.nextRowIndex + 1}/${total} after reaching the run limit.`
    );
    return { completed: false, limitReached: true };
  }

  await copyFile(state.originalPath, state.donePath);
  await unlink(state.originalPath);

  if (notionSync) {
    await unlink(state.workingPath);
    await unlink(state.statePath);
    await logger.info(
      `Finished ${state.sourceName}. Leads were synced to Notion and the source CSV was moved to done/.`
    );
  } else {
    await copyFile(state.workingPath, state.outputPath);
    await unlink(state.workingPath);
    await unlink(state.statePath);
    await logger.info(
      `Finished ${state.sourceName}. Output saved to ${path.basename(state.outputPath)}.`
    );
  }

  return { completed: true, limitReached: false };
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

function buildQualificationInput(record) {
  const source = stripOutputColumns(record);
  const selected = {
    firstName: source.firstName,
    companyName: source.companyName,
    linkedinHeadline: source.linkedinHeadline,
    linkedinJobTitle: source.linkedinJobTitle,
    linkedinJobDescription: source.linkedinJobDescription,
    linkedinDescription: source.linkedinDescription,
    companyIndustry: source.companyIndustry,
    linkedinCompanyDescription: source.linkedinCompanyDescription,
    linkedinCompanyTagline: source.linkedinCompanyTagline,
    linkedinCompanySpecialities: source.linkedinCompanySpecialities,
    linkedinJobLocation: source.linkedinJobLocation,
    linkedinIsOpenToWorkBadge: source.linkedinIsOpenToWorkBadge
  };

  for (const field of Object.keys(selected)) {
    if (!String(selected[field] ?? "").trim()) {
      delete selected[field];
    }
  }

  return orderFields(selected, QUALIFICATION_INPUT_FIELD_ORDER);
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
  const status = String(record.qualification_status || "").trim();
  const error = String(record.processing_error || "").trim();

  return Boolean(processedAt && (status || error));
}

function describeLead(record) {
  const candidates = [
    record.name,
    record.full_name,
    record.email,
    record.company,
    record.website
  ];

  return candidates.find((value) => String(value || "").trim()) || "";
}

function reviveState(rawState, config) {
  const baseName = path.basename(rawState.sourceName || "lead-file", ".csv");
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

async function moveFileIfPresent(fromPath, toPath) {
  if (await exists(fromPath)) {
    await rename(fromPath, toPath);
  }
}

function slugify(value) {
  return String(value || "lead-file")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "") || "lead-file";
}

function fileStamp() {
  return new Date().toISOString().replace(/[:.]/g, "-");
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
        notionSync,
        logger
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
        `Attempt ${attempt}/${maxAttempts} failed for row ${item.index + 1}/${total}${item.label ? ` (${item.label})` : ""}: ${formattedError}`
      );

      if (!(retryable && hasAttemptsLeft)) {
        break;
      }

      if (shouldRestartOllamaForError(error)) {
        await restartOllama({ config, logger, rowNumber: item.index + 1, total, attempt });
      }

      await logger.info(
        `Retrying row ${item.index + 1}/${total}${item.label ? ` (${item.label})` : ""} with attempt ${attempt + 1}/${maxAttempts}.`
      );
    }
  }

  markLeadAsFailed(item.row, lastError, config);
  const syncResult = await syncFailedLeadToNotion({ item, total, notionSync, logger });

  return {
    failed: true,
    syncResult
  };
}

async function processLeadAttempt({ item, total, config, promptText, notionSync, logger }) {
  let result = null;
  let syncResult = null;

  if (notionSync) {
    const existingLead = await notionSync.findExistingLead(item.row);

    if (existingLead) {
      applyExistingLeadResult(item.row, existingLead);
      return { skippedExisting: true, syncResult: null };
    }
  }

  const qualificationConfig = {
    ...config,
    requestTimeoutMs: getLeadAttemptTimeoutMs(config)
  };

  result = await qualifyLead({
    config: qualificationConfig,
    promptText,
    leadRecord: buildQualificationInput(item.row)
  });

  applyLeadResult(item.row, result);

  if (notionSync) {
    syncResult = await notionSync.upsertLead(item.row);
  }

  return { syncResult };
}

function applyLeadResult(row, result) {
  row.lead_category = result.lead_category;
  row.qualification_status = result.qualification_status;
  row.qualification_note = result.qualification_note;
  row.processing_error = "";
  row.processed_at = new Date().toISOString();
}

function applyExistingLeadResult(row, existingLead) {
  row.lead_category = existingLead.lead_category || row.lead_category || "";
  row.qualification_status = existingLead.qualification_status || row.qualification_status || "";
  row.qualification_note = existingLead.qualification_note || row.qualification_note || "";
  row.processing_error =
    row.qualification_status || row.qualification_note || row.lead_category
      ? ""
      : "Skipped because this Lead URL already exists in Notion without saved qualification fields.";
  row.processed_at = new Date().toISOString();
}

function markLeadAsFailed(row, error, config) {
  row.lead_category = row.lead_category || "Unclear";
  row.qualification_status = "Failed";
  row.qualification_note = buildFailedQualificationNote(error, config.maxNoteLength);
  row.processing_error = formatError(error, 500);
  row.processed_at = new Date().toISOString();
}

function buildFailedQualificationNote(error, maxLength) {
  const base = "Lead processing failed after repeated attempts.";
  const detail = formatError(error, Math.max(60, maxLength - base.length - 1));
  return truncate(`${base} ${detail}`.trim(), maxLength);
}

async function syncFailedLeadToNotion({ item, total, notionSync, logger }) {
  if (!notionSync) {
    return null;
  }

  try {
    return await notionSync.upsertLead(item.row);
  } catch (error) {
    await logger.error(
      `Could not sync Failed status for row ${item.index + 1}/${total}${item.label ? ` (${item.label})` : ""}: ${formatError(error, 400)}`
    );
    return null;
  }
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
      `Skipping Ollama restart for row ${rowNumber}/${total} attempt ${attempt} because no restart command is configured.`
    );
    return;
  }

  await logger.info(
    `Restarting Ollama before retrying row ${rowNumber}/${total} using: ${command}`
  );

  try {
    await runShellCommand(command);
    await logger.info(`Ollama restart command finished before retrying row ${rowNumber}/${total}.`);
  } catch (error) {
    await logger.error(
      `Ollama restart command failed before retrying row ${rowNumber}/${total}: ${formatError(error, 400)}`
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

function truncate(value, maxLength) {
  const text = String(value || "");

  if (text.length <= maxLength) {
    return text;
  }

  return `${text.slice(0, Math.max(0, maxLength - 3)).trimEnd()}...`;
}
