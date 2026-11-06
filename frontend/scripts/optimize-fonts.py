#!/usr/bin/env python3
"""Manual font optimizer (FontTools). Never run from the build.

Modes are cumulative, each builds on the previous one:
  copy        exact source bytes (baseline)
  glyphs      keep (used source chars + U+0020-017F + dashes/quotes/ellipsis) & cmap & original unicode-range
  features    + drop only the `frac` feature
  no-hinting  + drop hinting
  split       + one file per regional face: the original latin face is "core" (shaping boundary kept
                whole), every other region is "ext"; regional files are never merged

Usage:
  PYTHONPATH=/tmp/dq-fonttools python frontend/scripts/optimize-fonts.py --mode split --output DIR
  python frontend/scripts/optimize-fonts.py --compare DIR_A DIR_B ...
"""
import argparse
import hashlib
import json
import re
import shutil
import sys
from pathlib import Path

from fontTools import subset
from fontTools.ttLib import TTFont

MODES = ["copy", "glyphs", "features", "no-hinting", "split"]
FRONTEND = Path(__file__).resolve().parent.parent
FONTSOURCE = FRONTEND / "node_modules" / "@fontsource"
# Pinned original imports (normal style), independent of what tokens.css imports now.
SOURCE_FACES = [("sarabun", "400"), ("sarabun", "500"), ("sarabun", "600"), ("sarabun", "700"), ("roboto-mono", "400")]
DEFAULT_OUTPUT = FRONTEND / "public" / "fonts"
SOURCE_ROOTS = [FRONTEND / "src", FRONTEND / "index.html"]
SOURCE_SUFFIXES = {".ts", ".tsx", ".js", ".jsx", ".css", ".html", ".json"}
SKIP_DIRS = {"node_modules", "dist", "__tests__", "test", "tests", "__mocks__"}
SKIP_NAME = re.compile(r"\.(test|spec)\.|^setup", re.I)

PUNCTUATION = set(range(0x2010, 0x2016)) | {0x2018, 0x2019, 0x201A, 0x201B, 0x201C, 0x201D, 0x201E, 0x2026}
BASE_KEEP = set(range(0x20, 0x180)) | PUNCTUATION
SPACING = {0x20, 0xA0, 0x202F, 0x205F, 0xFEFF} | set(range(0x2000, 0x200C))
DROPPED_FEATURE = "frac"


def parse_range(text):
    cps = set()
    for part in text.split(","):
        part = part.strip()
        if not part:
            continue
        assert "?" not in part, f"wildcard unicode-range unsupported: {part}"
        lo, _, hi = part.removeprefix("U+").partition("-")
        cps.update(range(int(lo, 16), int(hi or lo, 16) + 1))
    return cps


def format_range(cps):
    parts = []
    runs, start, prev = [], None, None
    for cp in sorted(cps):
        if start is None:
            start = prev = cp
        elif cp == prev + 1:
            prev = cp
        else:
            runs.append((start, prev))
            start = prev = cp
    if start is not None:
        runs.append((start, prev))
    for lo, hi in runs:
        parts.append(f"U+{lo:04X}" if lo == hi else f"U+{lo:04X}-{hi:04X}")
    return ",".join(parts)


def read_faces():
    """Original @font-face blocks from the pinned @fontsource CSS, in source order."""
    faces = []
    for pkg, weight in SOURCE_FACES:
        css = (FONTSOURCE / pkg / f"{weight}.css").read_text()
        for block in re.findall(r"@font-face\s*\{(.*?)\}", css, re.S):
            def prop(name):
                return re.search(rf"{name}:\s*([^;]+);", block).group(1).strip()
            src = re.search(r"url\(\./files/([^)]+)\.woff2\)", block).group(1)
            faces.append({
                "package": pkg,
                "family": prop("font-family").strip("'\""),
                "style": prop("font-style"),
                "display": prop("font-display"),
                "weight": prop("font-weight"),
                "name": src,
                "path": FONTSOURCE / pkg / "files" / f"{src}.woff2",
                "range": parse_range(prop("unicode-range")),
            })
    return faces


def scan_used_codepoints(excluded):
    cps, files = set(), []
    for root in SOURCE_ROOTS:
        paths = [root] if root.is_file() else sorted(root.rglob("*"))
        for p in paths:
            rel_parts = set(p.relative_to(FRONTEND).parts[:-1])
            if (not p.is_file() or p.suffix not in SOURCE_SUFFIXES or rel_parts & SKIP_DIRS
                    or SKIP_NAME.search(p.name) or any(p.resolve().is_relative_to(e) for e in excluded)):
                continue
            files.append(str(p.relative_to(FRONTEND)))
            cps.update(ord(c) for c in p.read_text(encoding="utf-8") if ord(c) >= 0x20 and ord(c) != 0x7F)
    return cps, files


def features_of(font):
    return sorted({fr.FeatureTag for t in ("GSUB", "GPOS") if t in font
                   for fr in font[t].table.FeatureList.FeatureRecord} if any(t in font for t in ("GSUB", "GPOS")) else [])


def has_hinting(font):
    if any(t in font for t in ("fpgm", "prep", "cvt ")):
        return True
    if "glyf" in font:
        glyf = font["glyf"]
        return any(hasattr(g, "program") and g.program.getBytecode() for g in (glyf[n] for n in font.getGlyphOrder())
                   if g.numberOfContours != 0)
    return False


def describe(path):
    data = path.read_bytes()
    font = TTFont(path, recalcTimestamp=False)
    return {
        "bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
        "glyphs": font["maxp"].numGlyphs,
        "axes": [a.axisTag for a in font["fvar"].axes] if "fvar" in font else [],
        "features": features_of(font),
        "hinting": has_hinting(font),
        "codepoints": len(font.getBestCmap()),
    }


def rename_internal(font):
    """DQ-prefixed internal names; subfamily (2/17) and other IDs preserved."""
    name = font["name"]
    for rec in list(name.names):
        if rec.nameID in (1, 4, 16):
            new = "DQ " + rec.toUnicode().removeprefix("DQ ")
        elif rec.nameID == 6:
            new = "DQ-" + rec.toUnicode().removeprefix("DQ-")
        elif rec.nameID == 3:
            new = rec.toUnicode() + ";DQ" if not rec.toUnicode().endswith(";DQ") else rec.toUnicode()
        else:
            continue
        name.setName(new, rec.nameID, rec.platformID, rec.platEncID, rec.langID)


def build(src, dest, cps, mode):
    level = MODES.index(mode)
    # recalcBBoxes=False keeps original head/hhea/OS2 bounds and advanceWidthMax (native controls depend on them)
    font = TTFont(src, recalcBBoxes=False, recalcTimestamp=False)
    opts = subset.Options()
    opts.name_IDs = ["*"]
    opts.name_languages = ["*"]
    opts.notdef_outline = True
    opts.layout_features = ["*"] if level < 2 else [f for f in features_of(font) if f != DROPPED_FEATURE]
    opts.hinting = level < 3
    sub = subset.Subsetter(opts)
    sub.populate(unicodes=cps)
    sub.subset(font)
    rename_internal(font)
    font.flavor = "woff2"
    font.save(dest)


def generate(mode, out):
    level = MODES.index(mode)
    out.mkdir(parents=True, exist_ok=True)
    previous = out / "audit.json"
    if previous.exists():
        for f in json.loads(previous.read_text()).get("files", []):
            (out / f).unlink(missing_ok=True)

    faces = read_faces()
    used, source_files = scan_used_codepoints({out.resolve(), DEFAULT_OUTPUT.resolve()})
    # all Vietnamese source chars incl. combining marks, from the Vietnamese face(s)
    vietnamese = set().union(*(f["range"] for f in faces if "vietnamese" in f["name"]))
    wanted = used | vietnamese | BASE_KEEP

    css, audit_faces, files = [], [], []
    for face in faces:
        cmap = set(TTFont(face["path"], recalcTimestamp=False).getBestCmap())
        available = cmap & face["range"]
        keep = available if level == 0 else available & wanted
        is_core = re.search(r"-latin-\d+-normal$", face["name"]) is not None
        parts = [("-core" if is_core else "-ext", keep)] if mode == "split" else [("", keep)]
        entry = {"family": face["family"], "weight": face["weight"], "style": face["style"],
                 "source": face["name"], "original": describe(face["path"]), "outputs": [], "skipped": []}
        for suffix, cps in parts:
            tag = f"{face['name']}{suffix}"
            if not cps:
                entry["skipped"].append({"name": tag, "reason": "empty"})
                continue
            if level > 0 and cps <= SPACING:
                entry["skipped"].append({"name": tag, "reason": "spacing-only"})
                continue
            dest = out / f"{tag}.woff2"
            if level == 0:
                shutil.copyfile(face["path"], dest)
            else:
                build(face["path"], dest, cps, mode)
            files.append(dest.name)
            rng = format_range(cps)
            entry["outputs"].append({"file": dest.name, "expected": sorted(cps), "unicode_range": rng, **describe(dest)})
            css.append(f"/* {tag} */\n@font-face {{\n  font-family: '{face['family']}';\n  font-style: {face['style']};\n"
                       f"  font-display: {face['display']};\n  font-weight: {face['weight']};\n"
                       f"  src: url(./{dest.name}) format('woff2');\n  unicode-range: {rng};\n}}\n")
        audit_faces.append((face, entry))

    (out / "fonts.css").write_text("\n".join(css), encoding="utf-8")
    licenses = out / "licenses"
    licenses.mkdir(exist_ok=True)
    for pkg in sorted({f["package"] for f in faces}):
        shutil.copyfile(FONTSOURCE / pkg / "LICENSE", licenses / f"{pkg}-LICENSE")
        files.append(f"licenses/{pkg}-LICENSE")

    verify(out, audit_faces, level)

    orig = sum(e["original"]["bytes"] for _, e in audit_faces)
    gen = sum(o["bytes"] for _, e in audit_faces for o in e["outputs"])
    for _, e in audit_faces:
        for o in e["outputs"]:
            del o["expected"]
    audit = {
        "mode": mode,
        "files": files + ["fonts.css", "audit.json"],
        "source_files_scanned": source_files,
        "used_codepoints": sorted(used),
        "faces": [e for _, e in audit_faces],
        "totals": {"original_bytes": orig, "generated_bytes": gen, "reduction_bytes": orig - gen,
                   "reduction_pct": round(100 * (orig - gen) / orig, 2),
                   "files": sum(len(e["outputs"]) for _, e in audit_faces)},
    }
    (out / "audit.json").write_text(json.dumps(audit, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"{mode}: {audit['totals']['files']} fonts, {orig} -> {gen} bytes "
          f"(-{audit['totals']['reduction_pct']}%) in {out}")


def verify(out, audit_faces, level):
    """Re-read written fonts and fonts.css; actual cmap must match CSS ranges."""
    css_ranges = {}
    for tag, rng in re.findall(r"/\* (\S+) \*/.*?unicode-range: ([^;]+);", (out / "fonts.css").read_text(), re.S):
        css_ranges[tag] = parse_range(rng)
    for face, entry in audit_faces:
        seen = set()
        for o in entry["outputs"]:
            tag = o["file"].removesuffix(".woff2")
            actual = set(TTFont(out / o["file"], recalcTimestamp=False).getBestCmap())
            in_range = actual & face["range"]
            assert css_ranges[tag] == in_range, f"{tag}: CSS range != actual cmap within original range"
            assert css_ranges[tag] <= face["range"], f"{tag}: CSS range exceeds original face range"
            if level > 0:
                assert actual == set(o["expected"]), f"{tag}: cmap != requested codepoints"
            assert not (seen & css_ranges[tag]), f"{tag}: overlaps sibling split file"
            seen |= css_ranges[tag]


def compare(dirs):
    rows = []
    for d in dirs:
        a = json.loads((Path(d) / "audit.json").read_text())
        rows.append((a["mode"], d, a["totals"]))
    base = rows[0][2]["original_bytes"]
    print(f"{'mode':<12}{'files':>6}{'bytes':>10}{'vs original':>13}  dir")
    for mode, d, t in rows:
        print(f"{mode:<12}{t['files']:>6}{t['generated_bytes']:>10}{100 * (base - t['generated_bytes']) / base:>12.2f}%  {d}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mode", choices=MODES, default="split")
    ap.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, metavar="DIR")
    ap.add_argument("--compare", nargs="+", metavar="DIR", help="print size comparison of generated audit.json dirs")
    args = ap.parse_args()
    if args.compare:
        compare(args.compare)
    else:
        generate(args.mode, args.output.resolve())


if __name__ == "__main__":
    sys.exit(main())
