import path from "node:path";
import { copyFile } from "node:fs/promises";
import { setDefaultResultOrder } from "node:dns";
import { ensureRuntimeDirectories, loadConfig } from "./config.js";
import { createLogger } from "./logger.js";
import { processPendingDmFiles } from "./dm-writer.js";

try {
  setDefaultResultOrder("ipv4first");
  const cli = parseCliArgs(process.argv.slice(2));

  if (cli.showHelp) {
    console.log(buildUsageText());
    process.exit(0);
  }

  const baseConfig = await loadConfig(process.cwd(), {
    ...cli.configOverrides,
    promptFile: cli.configOverrides.promptFile || "./config/DM.txt",
    googleSheetFileName: cli.configOverrides.googleSheetFileName || "google-sheet-dm-leads.csv"
  });
  const config = buildDmRuntimeConfig(baseConfig);
  await ensureRuntimeDirectories(config);

  if (cli.inputCsvPath) {
    await stageInputCsv(config, cli.inputCsvPath);
  }

  const logger = createLogger(config.logsDir);
  await logger.info(
    cli.maxLeads
      ? `Starting DM writer run with a limit of ${cli.maxLeads} lead(s).`
      : "Starting DM writer run."
  );
  await processPendingDmFiles({ config, logger, maxLeads: cli.maxLeads });
  await logger.info("DM writer run finished.");
  process.exit(0);
} catch (error) {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
}

function buildDmRuntimeConfig(baseConfig) {
  return {
    ...baseConfig,
    inputDir: path.join(baseConfig.inputDir, "dm-writer"),
    processingDir: path.join(baseConfig.processingDir, "dm-writer"),
    outputDir: path.join(baseConfig.outputDir, "dm-writer"),
    doneDir: path.join(baseConfig.doneDir, "dm-writer"),
    failedDir: path.join(baseConfig.failedDir, "dm-writer"),
    logsDir: path.join(baseConfig.logsDir, "dm-writer")
  };
}

async function stageInputCsv(config, rawInputPath) {
  const inputPath = path.resolve(config.cwd, rawInputPath);
  const fileName = path.basename(inputPath);

  if (!fileName.toLowerCase().endsWith(".csv")) {
    throw new Error(`Input CSV must end with .csv: ${rawInputPath}`);
  }

  await copyFile(inputPath, path.join(config.inputDir, fileName));
}

function parseCliArgs(argv) {
  const configOverrides = {};
  let maxLeads = null;
  let inputCsvPath = "";

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];

    if (arg === "--help" || arg === "-h") {
      return { maxLeads: null, configOverrides: {}, inputCsvPath: "", showHelp: true };
    }

    if (arg === "--max-leads") {
      maxLeads = parsePositiveInteger(argv[index + 1], "--max-leads");
      index += 1;
      continue;
    }

    if (arg === "--input-csv") {
      inputCsvPath = requireValue(argv[index + 1], "--input-csv");
      index += 1;
      continue;
    }

    if (arg === "--google-sheet-url") {
      configOverrides.googleSheetUrl = requireValue(argv[index + 1], "--google-sheet-url");
      index += 1;
      continue;
    }

    if (arg === "--google-sheet-file") {
      configOverrides.googleSheetFileName = requireValue(argv[index + 1], "--google-sheet-file");
      index += 1;
      continue;
    }

    if (arg === "--notion-database" || arg === "--notion-db") {
      configOverrides.notionDatabaseId = requireValue(argv[index + 1], arg);
      index += 1;
      continue;
    }

    if (arg === "--notion-token") {
      configOverrides.notionToken = requireValue(argv[index + 1], "--notion-token");
      index += 1;
      continue;
    }

    if (arg === "--prompt-file") {
      configOverrides.promptFile = requireValue(argv[index + 1], "--prompt-file");
      index += 1;
      continue;
    }

    if (arg.startsWith("--")) {
      throw new Error(`Unknown option: ${arg}\n\n${buildUsageText()}`);
    }

    if (maxLeads !== null) {
      throw new Error(`Unexpected argument: ${arg}\n\n${buildUsageText()}`);
    }

    maxLeads = parsePositiveInteger(arg, "max-leads");
  }

  return { maxLeads, configOverrides, inputCsvPath, showHelp: false };
}

function parsePositiveInteger(value, label = "max-leads") {
  if (value === undefined) {
    throw new Error(`Missing value for ${label}\n\n${buildUsageText()}`);
  }

  const parsed = Number(value);

  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`Invalid value for ${label}: ${value}\n\n${buildUsageText()}`);
  }

  return parsed;
}

function requireValue(value, optionName) {
  if (value === undefined || String(value).startsWith("--")) {
    throw new Error(`Missing value for ${optionName}\n\n${buildUsageText()}`);
  }

  return value;
}

function buildUsageText() {
  return [
    "Usage: node src/dm-writer-index.js [max-leads] [options]",
    "",
    "Options:",
    "  --max-leads <number>           Limit the number of DM leads processed in this run",
    "  --input-csv <path>             Stage a local CSV into the DM writer input folder for this run",
    "  --google-sheet-url <url>       Override the Google Sheet source URL for this run",
    "  --google-sheet-file <name>     Override the downloaded CSV filename for this run",
    "  --notion-database <url-or-id>  Override the Notion database URL/ID for this run",
    "  --notion-db <url-or-id>        Alias for --notion-database",
    "  --notion-token <token>         Override the Notion token for this run",
    "  --prompt-file <path>           Use a different DM prompt file for this run",
    "  --help                         Show this help text"
  ].join("\n");
}
