import path from "node:path";
import { writeFile } from "node:fs/promises";

export async function importGoogleSheetIntoInput({ config, logger }) {
  if (!config.googleSheetUrl) {
    return false;
  }

  const source = buildGoogleSheetSource(config.googleSheetUrl, config.googleSheetFileName);
  const csvText = await downloadGoogleSheetCsv(source.urls);

  const targetPath = path.join(config.inputDir, source.fileName);
  await writeFile(targetPath, csvText, "utf8");

  if (logger) {
    await logger.info(
      `Imported Google Sheet into input/${source.fileName} from ${source.urls[0]}.`
    );
  }

  return true;
}

export function buildGoogleSheetSource(rawUrl, preferredFileName = "") {
  const fileName = sanitizeCsvFileName(preferredFileName || "google-sheet-leads.csv");
  const parsed = new URL(String(rawUrl || "").trim());

  if (!parsed.protocol.startsWith("http")) {
    throw new Error("Google Sheet URL must start with http:// or https://.");
  }

  if (parsed.hostname !== "docs.google.com") {
    return {
      urls: [parsed.toString()],
      fileName
    };
  }

  const publishedMatch = parsed.pathname.match(/^\/spreadsheets\/d\/e\/([^/]+)/);

  if (publishedMatch) {
    const gid = extractGid(parsed);
    const csvUrl = new URL(`/spreadsheets/d/e/${publishedMatch[1]}/pub`, parsed.origin);
    csvUrl.searchParams.set("output", "csv");

    if (gid) {
      csvUrl.searchParams.set("gid", gid);
      csvUrl.searchParams.set("single", "true");
    }

    return {
      urls: [csvUrl.toString()],
      fileName
    };
  }

  const standardMatch = parsed.pathname.match(/^\/spreadsheets\/d\/([^/]+)/);

  if (standardMatch) {
    const gid = extractGid(parsed) || "0";
    const exportUrl = new URL(`/spreadsheets/d/${standardMatch[1]}/export`, parsed.origin);
    exportUrl.searchParams.set("format", "csv");
    exportUrl.searchParams.set("gid", gid);
    const gvizUrl = new URL(`/spreadsheets/d/${standardMatch[1]}/gviz/tq`, parsed.origin);
    gvizUrl.searchParams.set("tqx", "out:csv");
    gvizUrl.searchParams.set("gid", gid);

    return {
      urls: [exportUrl.toString(), gvizUrl.toString()],
      fileName
    };
  }

  return {
    urls: [parsed.toString()],
    fileName
  };
}

async function downloadGoogleSheetCsv(urls) {
  const errors = [];

  for (const url of urls) {
    try {
      const response = await fetch(url);

      if (!response.ok) {
        errors.push(`Download failed from ${url} with ${response.status} ${response.statusText}`);
        continue;
      }

      const csvText = (await response.text()).replace(/^\uFEFF/, "");

      if (!csvText.trim()) {
        errors.push(`Google Sheet returned an empty response from ${url}`);
        continue;
      }

      if (looksLikeHtml(csvText)) {
        errors.push(`Google Sheet returned HTML instead of CSV from ${url}`);
        continue;
      }

      return csvText;
    } catch (error) {
      errors.push(
        error instanceof Error ? `${url}: ${error.message}` : `${url}: ${String(error)}`
      );
    }
  }

  throw new Error(
    `Google Sheet could not be downloaded as CSV. ${errors.join(" | ")}`
  );
}

function extractGid(url) {
  const searchGid = url.searchParams.get("gid");

  if (searchGid) {
    return searchGid;
  }

  const hashMatch = url.hash.match(/gid=([0-9]+)/);

  if (hashMatch) {
    return hashMatch[1];
  }

  return "";
}

function sanitizeCsvFileName(value) {
  const normalized = String(value || "google-sheet-leads.csv")
    .trim()
    .replace(/[<>:"/\\|?*\u0000-\u001F]/g, "-")
    .replace(/\s+/g, "-");

  if (!normalized) {
    return "google-sheet-leads.csv";
  }

  return normalized.toLowerCase().endsWith(".csv") ? normalized : `${normalized}.csv`;
}

function looksLikeHtml(value) {
  const sample = String(value || "").trimStart().slice(0, 500).toLowerCase();

  return (
    sample.startsWith("<!doctype html") ||
    sample.startsWith("<html") ||
    sample.includes("<head") ||
    sample.includes("<body")
  );
}
