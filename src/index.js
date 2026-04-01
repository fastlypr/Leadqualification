import { setDefaultResultOrder } from "node:dns";
import { ensureRuntimeDirectories, loadConfig } from "./config.js";
import { createLogger } from "./logger.js";
import { processPendingFiles } from "./processor.js";

try {
  setDefaultResultOrder("ipv4first");
  const maxLeads = parsePositiveInteger(process.argv[2]);
  const config = await loadConfig(process.cwd());
  await ensureRuntimeDirectories(config);
  const logger = createLogger(config.logsDir);
  await logger.info(
    maxLeads
      ? `Starting lead qualification run with a limit of ${maxLeads} lead(s).`
      : "Starting lead qualification run."
  );
  await processPendingFiles({ config, logger, maxLeads });
  await logger.info("Lead qualification run finished.");
  process.exit(0);
} catch (error) {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
}

function parsePositiveInteger(value) {
  if (value === undefined) {
    return null;
  }

  const parsed = Number(value);

  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error("Usage: node src/index.js [max-leads]");
  }

  return parsed;
}
