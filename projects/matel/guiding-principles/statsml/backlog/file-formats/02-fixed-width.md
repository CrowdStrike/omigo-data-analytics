# Fixed-Width Records

**Page type:** detail page (kusto-style 2-col text/viz layout: intro callout, one h2 section with text left 45% / canvas right 55%)
**HTML title tag:** Fixed-Width Records

**Subtitle:** Column positions defined in a separate document.

**Intro callout:** **Core trade-off:** Fast positional parsing with zero ambiguity — but the schema lives outside the file, in a document you hope someone maintained.

## How It Works

Every field occupies a predefined number of characters in every row. No delimiters — fields are extracted by byte position. The layout specification lives in a separate document.

- **Schema:** External — a "record layout" doc defining start position, length, type, padding
- **Layout:** Identical-length rows, padded fields
- **Access:** O(1) — jump to byte offset, random access to any row
- **Best for:** Mainframe batch, banking file transfers

**How it's parsed:** The reader is told: last name is columns 1-8, DOB is columns 17-24 as YYYYMMDD, salary is columns 29-36 zero-padded cents. It slices each row at those exact positions — no delimiters, no quoting, no ambiguity. Text fields are right-padded with spaces, numbers left-padded with zeros. Because every record is the same length, row N starts at byte N × record_length — random access without an index, and extremely fast reads in C or COBOL.

**Failure mode:** Schema drift. The layout document and the file are maintained separately — when they disagree, fields shift and every value is silently wrong. Adding a field means changing every record ever written.

*Example: NACHA/ACH bank transfers, IRS filings, census reporting, insurance claims — the COBOL systems that still run most financial infrastructure.*

### Visualization (canvas `c1`, 720×300)

Character grid of three fixed-width records with color-shaded field spans, a column ruler, and an external schema-doc callout.

- **Title (bold 14px, top center, `#1a5276`):** "Fixed-Width — Fields Defined by Byte Position".
- **Data rows (13px monospace, 16px per character cell, 30px row height, grid horizontally centered for 36 columns, first baseline y=68, text `#333`):**
  - `SMITH   JOHN    19850315NYC 00045000`
  - `JONES   ALICE   19901122CHI 00072500`
  - `CHEN    BOB     20010830LAX 00031000`
- **Field span shading (translucent rectangles behind characters in every row) with labels centered below the grid (10px system-ui):**
  - cols 1-8 "last name 1-8" — fill `rgba(26,82,118,0.15)`, label `#1a5276`
  - cols 9-16 "first name 9-16" — fill `rgba(39,174,96,0.15)`, label `#27ae60`
  - cols 17-24 "DOB 17-24" — fill `rgba(230,126,34,0.18)`, label `#e67e22`
  - cols 25-27 "city 25-27" — fill `rgba(142,68,173,0.15)`, label `#8e44ad`
  - cols 29-36 "salary 29-36" — fill `rgba(231,76,60,0.15)`, label `#e74c3c`
- **Column ruler (above the grid):** 9px monospace `#999` column numbers every 4 columns: 1, 5, 9, 13, 17, 21, 25, 29, 33.
- **External schema doc callout (below the field labels):** dashed (dash 5/4) red `#e74c3c` rectangle spanning the full 36-column grid width, 46px tall, width 1.5; centered 11px red text: "record layout document — lives OUTSIDE the file"; below it, centered 10px `#666` text: "if it drifts from reality, every field shifts and every value is silently wrong".

## Regeneration instructions

- **Layout:** backlog detail page. h1 with 2px `#2980b9` bottom border, `.subtitle`, then an `.intro-callout` (background `#f8f9fa`, left border 3px solid `#2980b9`, padding 10px 14px, 0.93rem). One `.card-section` (margin-bottom 40px) with an h2 (1.3rem `#1a5276`, 2px `#2980b9` bottom border) and a `table.layout` (width 100%, td padding 12px): left `td.text-col` 45% holds the paragraph, bullets, "How it's parsed" paragraph (0.9rem), `.key-point`, and `.example`; right `td.viz-col` 55% holds the canvas. No index number in the h1 or title.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `.key-point` background `#f8f9fa` with left border 3px solid `#e74c3c`, 0.9rem; `.example` italic `#555` 0.9rem; ul 0.92rem; `code` background `#f0f4f8`; `pre` background `#f0f4f8`, padding 12px, radius 4px, 0.9em. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300, CSS `width: 100%` with 1px `#e0e0e0` border and 4px radius; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, bar fill `rgba(26,82,118,0.35)`.
- Any card links in regenerated HTML use `.html` extensions.
