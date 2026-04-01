export function parseCsv(text) {
  const cleaned = String(text || "").replace(/^\uFEFF/, "");
  const rows = [];
  let row = [];
  let field = "";
  let inQuotes = false;

  for (let index = 0; index < cleaned.length; index += 1) {
    const char = cleaned[index];

    if (inQuotes) {
      if (char === "\"") {
        if (cleaned[index + 1] === "\"") {
          field += "\"";
          index += 1;
        } else {
          inQuotes = false;
        }
      } else {
        field += char;
      }

      continue;
    }

    if (char === "\"") {
      inQuotes = true;
      continue;
    }

    if (char === ",") {
      row.push(field);
      field = "";
      continue;
    }

    if (char === "\n") {
      row.push(field);
      rows.push(row);
      row = [];
      field = "";
      continue;
    }

    if (char === "\r") {
      if (cleaned[index + 1] === "\n") {
        index += 1;
      }

      row.push(field);
      rows.push(row);
      row = [];
      field = "";
      continue;
    }

    field += char;
  }

  if (inQuotes) {
    throw new Error("Invalid CSV: unterminated quoted field.");
  }

  if (field.length > 0 || row.length > 0) {
    row.push(field);
    rows.push(row);
  }

  const nonBlankRows = rows.filter((cells) => cells.some((cell) => String(cell || "").trim() !== ""));

  if (!nonBlankRows.length) {
    return { headers: [], records: [] };
  }

  const headerRow = nonBlankRows[0];
  const maxColumns = Math.max(...nonBlankRows.map((cells) => cells.length));
  const baseHeaders = [];

  for (let index = 0; index < maxColumns; index += 1) {
    const rawHeader = headerRow[index] ?? "";
    const trimmed = rawHeader.trim();
    baseHeaders.push(trimmed || `column_${index + 1}`);
  }

  const headers = dedupeHeaders(baseHeaders);
  const records = nonBlankRows.slice(1).map((cells) => {
    const record = {};

    for (let index = 0; index < headers.length; index += 1) {
      record[headers[index]] = cells[index] ?? "";
    }

    return record;
  });

  return { headers, records };
}

export function stringifyCsv(headers, records) {
  const lines = [];
  lines.push(headers.map(escapeCell).join(","));

  for (const record of records) {
    const line = headers.map((header) => escapeCell(record[header] ?? "")).join(",");
    lines.push(line);
  }

  return `${lines.join("\n")}\n`;
}

export function mergeHeaders(headers, extraHeaders) {
  const merged = [...headers];

  for (const header of extraHeaders) {
    if (!merged.includes(header)) {
      merged.push(header);
    }
  }

  return merged;
}

function dedupeHeaders(headers) {
  const counts = new Map();

  return headers.map((header) => {
    const seen = counts.get(header) || 0;
    counts.set(header, seen + 1);

    if (seen === 0) {
      return header;
    }

    return `${header}_${seen + 1}`;
  });
}

function escapeCell(value) {
  const stringValue = String(value ?? "");
  const needsQuotes =
    stringValue.includes(",") ||
    stringValue.includes("\"") ||
    stringValue.includes("\n") ||
    stringValue.includes("\r") ||
    /^\s|\s$/.test(stringValue);

  if (!needsQuotes) {
    return stringValue;
  }

  return `"${stringValue.replaceAll("\"", "\"\"")}"`;
}
