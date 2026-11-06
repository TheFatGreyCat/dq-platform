import { describe, expect, it, vi } from "vitest";
import { downloadExport, toCsv, toJson } from "./exportData";

const columns = [
  { key: "name", header: "Name" },
  { key: "value", header: "Value" },
];

describe("toCsv", () => {
  it("prefixes formula-leading strings with an apostrophe", () => {
    const values = ["=1+1", "+1", "-1", "@sum", "\tx", "\rx"];
    const csv = toCsv(columns, values.map((value) => ({ name: value, value: "ok" })));
    const lines = csv.split("\r\n");
    expect(lines[1]).toBe("'=1+1,ok");
    expect(lines[2]).toBe("'+1,ok");
    expect(lines[3]).toBe("'-1,ok");
    expect(lines[4]).toBe("'@sum,ok");
    expect(lines[5]).toBe("'\tx,ok");
    expect(csv).toContain("\"'\rx\",ok");
  });

  it("quotes commas, quotes and newlines, uses CRLF rows, keeps Unicode", () => {
    const csv = toCsv(columns, [{ name: 'a,"b"', value: "line1\nline2" }, { name: "สวัสดี ✓", value: "x" }]);
    expect(csv).toBe('Name,Value\r\n"a,""b""","line1\nline2"\r\nสวัสดี ✓,x');
  });

  it("renders null/undefined as empty and keeps bigint and numbers exact", () => {
    const csv = toCsv(columns, [
      { name: null, value: 12345678901234567890n },
      { name: undefined, value: 1e21 },
      { name: 0.5, value: -3 },
    ]);
    expect(csv.split("\r\n").slice(1)).toEqual([
      ",12345678901234567890",
      ",1000000000000000000000",
      "0.5,-3",
    ]);
  });

  it("exports only the visible columns", () => {
  const nameColumn = columns[0];
  if (!nameColumn) throw new Error('name column missing');
  const csv = toCsv([nameColumn], [{ name: "a", value: "hidden", secret: "token" }]);
    expect(csv).toBe("Name\r\na");
  });
});

describe("toJson", () => {
  it("contains only visible columns and serializes bigint/null", () => {
  const valueColumn = columns[1];
  if (!valueColumn) throw new Error('value column missing');
  const json = toJson([valueColumn], [{ name: "a", value: 10n }, { name: "b" }]);
    expect(JSON.parse(json)).toEqual([{ value: "10" }, { value: null }]);
  });
});

describe("downloadExport", () => {
  it("revokes the object URL after clicking", () => {
    const create = vi.fn(() => "blob:test");
    const revoke = vi.fn();
    vi.stubGlobal("URL", { createObjectURL: create, revokeObjectURL: revoke });
    const click = vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(() => {});
    downloadExport("a", "x.csv", "text/csv");
    expect(click).toHaveBeenCalled();
    expect(revoke).toHaveBeenCalledWith("blob:test");
    click.mockRestore();
    vi.unstubAllGlobals();
  });
});
