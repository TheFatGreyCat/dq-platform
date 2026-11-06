export interface ExportColumn {
  key: string;
  header: string;
}

export type ExportRow = Record<string, unknown>;

const FORMULA_PREFIX = /^[=+\-@\t\r]/;

function formatNumber(value: number): string {
  const text = String(value);
  if (!/e/i.test(text)) return text;
  return value.toLocaleString("fullwide", { useGrouping: false, maximumFractionDigits: 20 });
}

function toCell(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (typeof value === "string") return FORMULA_PREFIX.test(value) ? `'${value}` : value;
  if (typeof value === "number") return formatNumber(value);
  if (typeof value === "bigint" || typeof value === "boolean") return String(value);
  if (value instanceof Date) return value.toISOString();
  return JSON.stringify(value, (_key, value) => (typeof value === "bigint" ? value.toString() : value));
}

export function toCsv(columns: ExportColumn[], rows: ExportRow[]): string {
  const table = [columns.map((c) => c.header), ...rows.map((row) => columns.map((c) => row[c.key]))];
  return table
    .map((cells) =>
      cells
        .map((value) => {
          const cell = toCell(value);
          return /[",\r\n]/.test(cell) ? `"${cell.replace(/"/g, '""')}"` : cell;
        })
        .join(","),
    )
    .join("\r\n");
}

export function toJson(columns: ExportColumn[], rows: ExportRow[]): string {
  const visible = rows.map((row) => Object.fromEntries(columns.map((c) => [c.key, row[c.key] ?? null])));
  return JSON.stringify(visible, (_key, value) => (typeof value === "bigint" ? value.toString() : value), 2);
}

export function downloadExport(content: string, filename: string, mimeType: string): void {
  const url = URL.createObjectURL(new Blob([content], { type: `${mimeType};charset=utf-8` }));
  try {
    const link = document.createElement("a");
    link.href = url;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    link.remove();
  } finally {
    URL.revokeObjectURL(url);
  }
}
