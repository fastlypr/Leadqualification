import path from "node:path";
import { appendFile, mkdir } from "node:fs/promises";

export function createLogger(logsDir) {
  const logFile = path.join(logsDir, "processor.log");

  async function write(level, message) {
    await mkdir(logsDir, { recursive: true });
    const line = `[${new Date().toISOString()}] [${level}] ${message}\n`;
    await appendFile(logFile, line, "utf8");
    const printer = level === "ERROR" ? console.error : console.log;
    printer(line.trim());
  }

  return {
    info(message) {
      return write("INFO", message);
    },
    error(message) {
      return write("ERROR", message);
    }
  };
}
