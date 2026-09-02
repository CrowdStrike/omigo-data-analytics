# CSV / TSV

**Page type:** detail page (kusto-style 2-col text/viz layout: intro callout, one h2 section with text left 45% / canvas right 55%)
**HTML title tag:** CSV / TSV

**Subtitle:** Delimited plain text, universal, no schema.

**Intro callout:** **Core trade-off:** Needs nothing installed and works everywhere — but breaks the moment your data contains the delimiter itself.

## How It Works

Each line is a record; fields are separated by a comma (CSV) or tab (TSV). No metadata, no types, no schema — the file is the data and nothing else.

- **Schema:** None — everything is a string until parsed
- **Layout:** Row-oriented text, one record per line
- **Spec:** RFC 4180 — widely ignored by producers
- **Best for:** Lowest-common-denominator interchange

**How it's parsed:** Split on newlines, then split each line on the delimiter. Except that fails the moment a field contains the delimiter — so RFC 4180 adds quoting: wrap the field in double-quotes, escape embedded quotes by doubling them. Now the parser needs a state machine, not a split. And because many producers ignore the spec, real-world CSV parsing is a collection of heuristics: sniff the delimiter, guess the encoding, hope the quoting is consistent.

**Failure mode:** The data collides with the format. A comma inside a value, a newline inside a quoted field, a BOM at the start — each breaks naive parsers silently. No standard for null vs empty string.

*Example: Database exports, spreadsheet interchange, feed files for ad platforms, bulk uploads to SaaS tools — anywhere two systems share no protocol.*

### Visualization (canvas `c1`, 720×300)

Color-coded rendering of raw CSV text showing delimiter collisions.

- **Title (bold 14px, top center, `#1a5276`):** "CSV — When the Data Contains the Delimiter".
- **Text rows (14px monospace, left-aligned from x=60, first baseline y=60, line height 34px), tokens colored individually:**
  - Row 1 (header): `name` (#333), `,` (`#e67e22`), `city` (#333), `,` (`#e67e22`), `bio` (#333)
  - Row 2: `Alice` (#333), `,` (`#e67e22`), `New York` (#333), `,` (`#e67e22`), `"Likes commas, really"` (`#e74c3c`)
  - Row 3: `Bob` (#333), `,` (`#e67e22`), `"Portland, OR"` (`#e74c3c`), `,` (`#e67e22`), `"Said ""hello"" once"` (`#8e44ad`)
  - Row 4: `Charlie` (#333), `,` (`#e67e22`), `London` (#333), `,` (`#e67e22`) — trailing field left empty
- **Trailing empty field marker:** dashed (dash 3/3) gray `#999` rectangle (60×20) just after the trailing comma of the Charlie row, with 11px `#999` label inside: "null? empty?".
- **Legend (12px system-ui, colored "■" swatch at x=60 followed by `#555` text, 22px apart):**
  - `#e67e22` — "delimiter — the only structure there is"
  - `#e74c3c` — "delimiter inside data — quoting required, parser needs a state machine"
  - `#8e44ad` — "quote inside quoted field — escaped by doubling"

## Regeneration instructions

- **Layout:** backlog detail page. h1 with 2px `#2980b9` bottom border, `.subtitle`, then an `.intro-callout` (background `#f8f9fa`, left border 3px solid `#2980b9`, padding 10px 14px, 0.93rem). One `.card-section` (margin-bottom 40px) with an h2 (1.3rem `#1a5276`, 2px `#2980b9` bottom border) and a `table.layout` (width 100%, td padding 12px): left `td.text-col` 45% holds the paragraph, bullets, "How it's parsed" paragraph (0.9rem), `.key-point`, and `.example`; right `td.viz-col` 55% holds the canvas. No index number in the h1 or title.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `.key-point` background `#f8f9fa` with left border 3px solid `#e74c3c`, 0.9rem; `.example` italic `#555` 0.9rem; ul 0.92rem; `code` background `#f0f4f8`; `pre` background `#f0f4f8`, padding 12px, radius 4px, 0.9em. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300, CSS `width: 100%` with 1px `#e0e0e0` border and 4px radius; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, bar fill `rgba(26,82,118,0.35)`.
- Any card links in regenerated HTML use `.html` extensions.
