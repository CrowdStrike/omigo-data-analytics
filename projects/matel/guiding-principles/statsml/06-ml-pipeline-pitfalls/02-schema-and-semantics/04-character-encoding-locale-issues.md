# Pitfall: Character Encoding / Locale Issues

**Page type:** detail page (three `.card-section` blocks, each a two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Character Encoding / Locale Issues

**Subtitle:** Text data corrupted by encoding mismatches, causing silent data loss and incorrect feature extraction

## The Problem

**Tags:** `the trap` (red pill), `encoding` (blue pill)

- **Mojibake** — UTF-8 read as Latin-1 turns "José" into "JosÃ©" for every non-ASCII char
- **Silent corruption** — pipelines keep running while names and text features turn to garbage
- **Locale drift** — case folding and collation follow the host locale, varying by machine
- **BOM bytes** — a byte order mark at file start breaks CSV parsers and JSON decoders
- **Smart quotes** — Windows-1252 quotes and em-dashes become multi-character junk in UTF-8

*Example:* A table with 10% non-ASCII names is exported as UTF-8 but imported as Latin-1, so "François" becomes "FranÃ§ois" and the model sees two different customers.

**Impact:** Text features are silently corrupted and string matching fails, and detection is hard because corrupted text looks "almost right."

### Visualization (canvas `c1`, 720×300)

Comparison table of correct UTF-8 vs mojibake Latin-1 readings of four names.

- **Title (bold 14px, `#1a5276`, top center):** "Encoding Mismatch — UTF-8 Data Read as Latin-1 (Mojibake)".
- **Column headers** (bold 11px `#1a5276`, centered over columns at x=120/260/400/540, y=50): "Original", "UTF-8 (correct)", "Read as Latin-1", "Corruption".
- **Four rows** (start y=80, row height 50):
  - José → boxed "José" (green) → boxed "JosÃ©" (red) → "é → Ã©"
  - François → "François" → "FranÃ§ois" → "ç → Ã§"
  - Müller → "Müller" → "MÃ¼ller" → "ü → Ã¼"
  - Café → "Café" → "CafÃ©" → "é → Ã©"
- **Row styling:** Original in `#2c3e50` 14px; UTF-8 value in a 120×30 white box with 2px `#27ae60` stroke, text `#27ae60`; Latin-1 value in a 120×30 white box with 2px `#e74c3c` stroke, text `#e74c3c`; corruption description in gray `#666` 11px.
- **Caption (gray `#666`, 11px, bottom center, y=270):** "Problem: UTF-8 bytes interpreted as Latin-1 produce mojibake. Names become unmatchable."

## Why It Happens

**Tags:** `root cause` (orange pill), `byte defaults` (blue pill)

- **System defaults** — Windows leans on Windows-1252, Linux on UTF-8, old databases on Latin-1
- **Excel BOM** — CSV exports add a UTF-8 BOM that parsers read into the first column name
- **Client defaults** — database drivers fall back to Latin-1 unless the charset is set explicitly
- **Language history** — Python 2 treated strings as bytes, and ported code still omits encodings
- **Locale case rules** — in a Turkish locale, "i".upper() yields "İ" instead of "I"

*Example:* A pipeline reads a CSV exported from Excel and treats the BOM bytes (EF BB BF) as text, so the first column name becomes "﻿user_id" and joins fail silently.

**Root Cause:** Bytes carry no label saying how they were encoded, so every system guesses with its own default and a wrong guess yields plausible-looking garbage instead of an error.

### Visualization (canvas `c2`, 720×300)

2×2 grid of common encoding issue boxes, each with source, symptom, and fix.

- **Title (bold 14px, `#1a5276`, top center):** "Common Encoding Issues — Sources and Symptoms".
- **Four boxes** (140×70, white fill, 2px `#e74c3c` stroke, centered at (100,80), (360,80), (100,180), (360,180); source in bold red 10px, symptom in gray 9px, fix in bold green 8px, each possibly multi-line):
  - Source "Excel CSV / with BOM"; symptom "\uFEFF in / first column"; fix 'encoding= / "utf-8-sig"'.
  - Source "Windows-1252 / smart quotes"; symptom '" " → Ã¢â‚¬Å"'; fix "Use UTF-8 / everywhere".
  - Source "Locale-dependent / case folding"; symptom "Turkish i.upper() / → İ not I"; fix "Use casefold(), / not lower()".
  - Source "MySQL default / Latin-1"; symptom "Non-ASCII chars / corrupted"; fix "Use utf8mb4 / charset".
- **Detection divider:** vertical dashed orange line (`#e67e22`, dash 4/4, width 2) down the center from y=50 to y=270.
- **Detection label (bold orange 11px, centered, y=265):** "Detection: Scan for U+FFFD (�), mojibake patterns, BOM bytes".

## The Correct Approach

**Tags:** `the fix` (green pill), `UTF-8` (blue pill)

- **UTF-8 connections** — set database connection encoding to UTF-8, utf8mb4 in MySQL
- **Explicit I/O** — pass encoding='utf-8' at every file boundary instead of relying on defaults
- **BOM-safe reads** — read CSV with encoding='utf-8-sig' so a leading BOM is stripped
- **Locale-free ops** — compare strings with casefold() rather than locale-dependent lower()
- **Ingestion checks** — scan text for U+FFFD (�), mojibake like "Ã©", and stray BOM bytes
- **CI coverage** — keep test names like "José" and "北京" so regressions surface early

*Example:* A pipeline sets UTF-8 explicitly on the database connection, file reads, and HTTP headers, and a mojibake validator on ingestion keeps production clean.

**Fix:** UTF-8 everywhere — validate text on ingestion with try/except UnicodeDecodeError and log warnings whenever non-UTF-8 data is detected.

### Visualization (canvas `c3`, 720×300)

Four-step prevention pipeline flow plus test-suite and best-practice boxes.

- **Title (bold 14px, `#1a5276`, top center):** "Prevention Pipeline — Enforce UTF-8 at Every Boundary".
- **Four step boxes** (100×50, white fill, 2px `#1a5276` stroke, centered at x=80/240/400/560, y=70; label in bold 11px `#2c3e50`, detail in 9px `#1a5276` below the box; gray `#666` arrows with filled arrowheads between consecutive boxes):
  - "Database / Connection" — detail "charset=utf8mb4"
  - "File I/O" — detail 'encoding="utf-8"'
  - "HTTP / Headers" — detail "Content-Type: / UTF-8"
  - "Validation" — detail "Check for / mojibake"
- **Test cases box:** 600×50 at (60,160), 2px `#27ae60` stroke; bold green 11px "Test Suite — International Characters"; 11px `#2c3e50` text 'Add test cases: "José", "François", "Müller", "北京", "Москва", "مصر"'.
- **Best practice box:** 600×50 at (60,225), 2px `#1a5276` stroke; bold blue 11px "Best Practice"; 11px `#2c3e50` text "UTF-8 everywhere. Explicit encoding at all I/O. Validate on ingestion. Test with non-ASCII."

## Regeneration instructions

- **Layout:** three `.card-section` blocks, each with an `<h2>` (1.3rem, `#1a5276`, 2px `#2980b9` bottom border) and a `table.layout` (full width, border-collapse) with one row: `.text-col` `<td>` (45%) and `.viz-col` `<td>` (55%). Text cell holds a `.tags` div of pill spans, a `<ul>` of `<li><b>Label</b> — sentence</li>` bullets, an italic `.example` paragraph, and a `.key-point` callout (`#f8f9fa` background, 3px `#e74c3c` left border, `<strong>` lead word).
- **Tag pills:** inline-block, 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width: 100%` with 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** each canvas declares `width="720" height="300"`; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#444`.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
