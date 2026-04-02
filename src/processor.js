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
import { loadDmPrompt, loadPrompt } from "./config.js";
import { mergeHeaders, parseCsv, stringifyCsv } from "./csv.js";
import { createNotionSync } from "./notion.js";
import { generateDmFields, qualifyLead } from "./ollama.js";
import { createTaskQueue } from "./queue.js";

const OUTPUT_COLUMNS = [
  "lead_category",
  "qualification_status",
  "qualification_note",
  "pain_hook",
  "personalized_line",
  "processed_at",
  "processing_error"
];

const ICP_INPUT_FIELDS = [
  "fullName",
  "firstName",
  "lastName",
  "companyName",
  "title",
  "industry",
  "companyLocation",
  "location",
  "summary",
  "titleDescription",
  "durationInRole",
  "durationInCompany"
];

export async function processPendingFiles({ config, logger, maxLeads = null }) {
  const resumableStatePaths = await listStateFiles(config.processingDir);
  const inputCsvPaths = await listCsvFiles(config.inputDir);
  const runState = {
    maxLeads,
    processedLeadCount: 0,
    remainingLeadCount: Number.isInteger(maxLeads) ? maxLeads : Number.POSITIVE_INFINITY
  };

  if (resumableStatePaths.length === 0 && inputCsvPaths.length === 0) {
    await logger.info("No CSV files waiting in input/ or processing/.");
    return;
  }

  const prompts = {
    qualificationPromptText: await loadPrompt(config),
    dmPromptText: await loadDmPrompt(config)
  };
  const notionSync = await createNotionSync({ config, logger });

  for (const statePath of resumableStatePaths) {
    try {
      const result = await processExistingJob(statePath, prompts, config, logger, runState, notionSync);

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
      const result = await processNewInput(inputPath, prompts, config, logger, runState, notionSync);

      if (result.limitReached) {
        await logger.info(`Stopped after ${runState.processedLeadCount} processed lead(s).`);
        return;
      }
    } catch (error) {
      await logger.error(`Failed to process ${path.basename(inputPath)}: ${formatError(error)}`);
    }
  }
}

async function processExistingJob(statePath, prompts, config, logger, runState, notionSync) {
  const state = reviveState(
    JSON.parse(await readFile(statePath, "utf8")),
    config
  );

  await logger.info(`Resuming ${state.sourceName} from row ${state.nextRowIndex + 1}.`);
  return processJob(state, prompts, config, logger, runState, notionSync);
}

async function processNewInput(inputPath, prompts, config, logger, runState, notionSync) {
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
    return processJob(state, prompts, config, logger, runState, notionSync);
  } catch (error) {
    await logger.error(`Could not initialize ${sourceName}: ${formatError(error)}`);
    await moveFileIfPresent(state.originalPath, state.failedOriginalPath);
    await moveFileIfPresent(state.workingPath, state.failedWorkingPath);
    await moveFileIfPresent(state.statePath, state.failedStatePath);
    throw error;
  }
}

async function processJob(state, prompts, config, logger, runState, notionSync) {
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
    await logger.info(
      `Queued ${queuedItems.length} lead(s) from ${state.sourceName} with request concurrency 1.`
    );
  }

  const queueTasks = queuedItems.map((item) =>
    queue.add(async () => {
      await logger.info(
        `Processing ${state.sourceName} row ${item.index + 1}/${total}${item.label ? ` (${item.label})` : ""}. Queue ${item.queuePosition}/${queuedItems.length}.`
      );

      try {
        const result = await qualifyLead({
          config,
          promptText: prompts.qualificationPromptText,
          leadRecord: buildQualificationInput(item.row)
        });

        item.row.lead_category = result.lead_category;
        item.row.qualification_status = result.qualification_status;
        item.row.qualification_note = result.qualification_note;
        item.row.pain_hook = "";
        item.row.personalized_line = "";

        if (shouldGenerateDm(item.row)) {
          const dmResult = await generateDmFields({
            config,
            promptText: prompts.dmPromptText,
            leadRecord: buildDmInput(item.row)
          });

          item.row.pain_hook = dmResult.pain_hook;
          item.row.personalized_line = dmResult.personalized_line;
        }

        item.row.processing_error = "";
        item.row.processed_at = new Date().toISOString();

        if (notionSync) {
          const syncResult = await notionSync.upsertLead(item.row);
          await logger.info(
            `Synced row ${item.index + 1}/${total} to Notion as ${syncResult.action}.`
          );
        }

        await logger.info(
          `Completed row ${item.index + 1}/${total} as ${item.row.qualification_status || "Unknown"}${item.row.lead_category ? ` [${item.row.lead_category}]` : ""}.`
        );
      } catch (error) {
        item.row.processing_error = formatError(error, 500);
        item.row.processed_at = new Date().toISOString();

        await logger.error(
          `Row ${item.index + 1}/${total} failed${item.label ? ` (${item.label})` : ""}: ${item.row.processing_error}`
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
  const selected = {};

  for (const field of ICP_INPUT_FIELDS) {
    const value = source[field];

    if (String(value ?? "").trim()) {
      selected[field] = value;
    }
  }

  return selected;
}

function buildDmInput(record) {
  return compactRecord({
    ...buildQualificationInput(record),
    lead_category: record.lead_category,
    qualification_status: record.qualification_status,
    qualification_note: record.qualification_note
  });
}

function isAlreadyProcessed(record) {
  const processedAt = String(record.processed_at || "").trim();
  const status = String(record.qualification_status || "").trim();
  const painHook = String(record.pain_hook || "").trim();
  const personalizedLine = String(record.personalized_line || "").trim();
  const error = String(record.processing_error || "").trim();

  if (!processedAt) {
    return false;
  }

  if (error) {
    return true;
  }

  if (!status) {
    return false;
  }

  if (!shouldGenerateDm(record)) {
    return true;
  }

  return Boolean(painHook && personalizedLine);
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

function formatError(error, maxLength = 300) {
  const message =
    error instanceof Error ? error.message : typeof error === "string" ? error : JSON.stringify(error);

  if (message.length <= maxLength) {
    return message;
  }

  return `${message.slice(0, Math.max(0, maxLength - 3)).trimEnd()}...`;
}

function shouldGenerateDm(record) {
  const status = String(record.qualification_status || "").trim();

  return status === "Qualified" || status === "Needs Review";
}

function compactRecord(record) {
  const nextRecord = {};

  for (const [key, value] of Object.entries(record)) {
    if (String(value ?? "").trim()) {
      nextRecord[key] = value;
    }
  }

  return nextRecord;
}
