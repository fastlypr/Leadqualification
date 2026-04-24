import { setDefaultResultOrder } from "node:dns";
import { ensureRuntimeDirectories, loadConfig } from "./config.js";
import { createLogger } from "./logger.js";
import { processPendingFiles } from "./processor.js";

try {
  setDefaultResultOrder("ipv4first");
  const cli = parseCliArgs(process.argv.slice(2));
  if (cli.showHelp) {
    console.log(buildUsageText());
    process.exit(0);
  }
  const config = await loadConfig(process.cwd(), cli.configOverrides);
  await ensureRuntimeDirectories(config);
  const logger = createLogger(config.logsDir);
  await logger.info(
    cli.maxLeads
      ? `Starting lead qualification run with a limit of ${cli.maxLeads} lead(s).`
      : "Starting lead qualification run."
  );
  await processPendingFiles({ config, logger, maxLeads: cli.maxLeads });
  await logger.info("Lead qualification run finished.");
  process.exit(0);
} catch (error) {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
}

function parseCliArgs(argv) {
  const configOverrides = {};
  let maxLeads = null;

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];

    if (arg === "--help" || arg === "-h") {
      return { maxLeads: null, configOverrides: {}, showHelp: true };
    }

    if (arg === "--max-leads") {
      maxLeads = parsePositiveInteger(argv[index + 1], "--max-leads");
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

  return { maxLeads, configOverrides, showHelp: false };
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
    "Usage: node src/index.js [max-leads] [options]",
    "",
    "Options:",
    "  --max-leads <number>           Limit the number of leads processed in this run",
    "  --google-sheet-url <url>       Override the Google Sheet source URL for this run",
    "  --google-sheet-file <name>     Override the downloaded CSV filename for this run",
    "  --notion-database <url-or-id>  Override the Notion database URL/ID for this run",
    "  --notion-db <url-or-id>        Alias for --notion-database",
    "  --notion-token <token>         Override the Notion token for this run",
    "  --prompt-file <path>           Use a different prompt file for this run",
    "  --help                         Show this help text"
  ].join("\n");
}
