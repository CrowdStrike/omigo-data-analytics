# Datetime-Formatted Strings for Filenames

**Page type:** detail page (backlog-style two-column layout table: text left 45%, canvas right 55%, one `.lang-section` per numbered h2; sections 1-2 hold small `ex-table` tables in the text column)
**HTML title tag:** Datetime-Formatted Strings for Filenames

**Subtitle:** Timestamp conventions that sort correctly, parse unambiguously, and work across OS/tools

**Intro callout (blue accent):** **Core tension:** ISO 8601 (`2026-08-11T14:30:22Z`) is the standard but contains colons (illegal in Windows filenames) and `T` separators that hurt readability. Need a filename-safe variant that preserves sort order and parseability.

## 1. Questions to Resolve

(Text column is an `ex-table` with dark-blue header row.)

| Question | Options |
|---|---|
| Format | `20260811-143022` vs `2026-08-11_143022` vs `20260811T143022Z` |
| Timezone handling | Always UTC? Local + offset suffix? Or omit (assume UTC)? |
| Precision | Seconds sufficient? Milliseconds for rapid batch runs? |
| Date-only vs datetime | When is date sufficient (`20260811`) vs full timestamp needed? |
| Separator between date/time | Hyphen, underscore, or concatenated? |

### Visualization (canvas `c1`, 720×320)

Check/cross matrix: candidate timestamp formats vs three requirements.

- **Title (bold 14px `#1a5276`, centered):** "Candidate formats vs the three requirements"
- **Column headers (bold 12px `#555`, centered at x = 330, 445, 560):** "sorts correctly", "Windows/URL safe", "readable"
- **Rows (format in 13px Menlo monospace `#2c3e50` at x=20; marks bold 15px — ✓ green `#27ae60`, ✗ red `#e74c3c`; note 11px `#999` beneath the format; thin `#eee` divider line under each row; rows start y=90, spacing 48):**
  | Format | sorts | safe | readable | Note |
  |---|---|---|---|---|
  | `2026-08-11T14:30:22Z` | ✓ | ✗ | ✓ | ISO 8601 — colons break Windows/URLs |
  | `20260811T143022Z` | ✓ | ✓ | ✗ | ISO basic — safe but hard to scan |
  | `2026-08-11_143022` | ✓ | ✓ | ✓ | expanded — human-facing |
  | `20260811-143022` | ✓ | ✓ | ✓ | compact — embedded in filenames |
- **Caption (13px `#444`, bottom center):** "Fixed-width digits in big-endian order (year → second) make lexicographic sort = chronological sort"

## 2. Proposed Convention

(Text column is an `ex-table` with dark-blue header row.)

| Use Case | Format | Example |
|---|---|---|
| Daily artifacts | `YYYYMMDD` | `profile-ames-20260811.json` |
| Pipeline runs | `YYYYMMDD-HHmmss` | `run-20260811-143022.log` |
| Sub-second batches | `YYYYMMDD-HHmmss-SSS` | `batch-20260811-143022-817.parquet` |

### Visualization (canvas `c2`, 720×320)

Filename anatomy diagram plus a precision ladder.

- **Title (bold 14px `#1a5276`, centered):** "Anatomy: name-timestamp.ext"
- **Filename (bold 26px Menlo monospace, centered as one line at y=110), colored by part:** `run` (primary `#1a5276`), `-` (`#999`), `20260811` (green `#27ae60`), `-` (`#999`), `143022` (orange `#e67e22`), `.log` (`#888`).
- **Label brackets:** colored leader lines from each labeled part alternating up/down to bold 12px labels in the part color plus 11px `#999` sub-labels:
  - `run` → "semantic name" / "what it is"
  - `20260811` → "date (YYYYMMDD)" / "compact, big-endian"
  - `143022` → "time (HHmmss)" / "UTC, no colons"
  - `.log` → "extension" (no sub)
- **Precision ladder (heading bold 12px `#444` at (60, 215)):** "Precision grows only as needed:" — three rows (y = 240, 264, 288), each with a small 8×14 bar in `rgba(26,82,118,0.35)`, a 13px monospace value at x=78 and an 11px `#999` label at x=260:
  - `20260811` — "daily artifact"
  - `20260811-143022` — "pipeline run"
  - `20260811-143022-817` — "sub-second batch"

## 3. Rules

- No colons, no `T`, no `+` in filenames — these break on Windows/URLs.
- All timestamps UTC unless explicitly local (suffix `-local` if needed).
- Compact form (no hyphens in date) for embedded timestamps; expanded for human-facing.
- Timestamp goes after the semantic name, before extension: `name-timestamp.ext`.

### Visualization (canvas `c3`, 720×320)

Split panel: a chronologically sorted file listing (left) and forbidden characters (right).

- **Title (bold 14px `#1a5276`, centered):** "Lexicographic order = chronological order"
- **Left — file listing (13px Menlo monospace `#2c3e50` on `rgba(26,82,118,0.08)` row highlights, 260px wide starting at x=50, rows at y = 70, 100, 130, 160):**
  - `run-20260810-235959.log`
  - `run-20260811-081500.log`
  - `run-20260811-143022.log`
  - `run-20260812-020000.log`
  - A green `#27ae60` vertical arrow alongside (x=330, from y=58 down to y=180 with a filled triangle head), with bold 12px green text: "ls, sort, S3 listing —" / "all agree with time".
- **Right — forbidden characters (heading bold 13px red `#e74c3c` at (480, 65)):** "Never in filenames:" — three 26×26 red-outlined boxes with the character in bold 16px monospace and a red diagonal cross-out stroke, plus a 12px `#555` reason:
  - `:` — "illegal on Windows"
  - `T` — "hurts readability"
  - `+` — "mangled in URLs"
- **Bottom rule strip:** 13px `#444` centered: "UTC by default; suffix -local only when a local time is genuinely meant"; then 12px `#999` centered: "name-timestamp.ext keeps the semantic prefix groupable and the timestamp sortable within the group"

**Status footer (12px `#999`):** Status: stub. Needs brainstorming session.

## Regeneration instructions

- **Template:** backlog detail page (kusto-style 2-col text/viz layout). Structure: h1, `.subtitle` paragraph, `.intro` callout, then one `.lang-section` per section — each with an h2 (numbered "N. Title") and a `table.layout` with `td.text-col` (45%) and `td.viz-col` (55%) holding the canvas. Sections 1 and 2 put an `.ex-table` in the text column; section 3 puts a bullet list. Ends with a small gray status paragraph. No index number in the page h1/title.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; section h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border. `.intro` — background `#f0f4f8`, left border `3px solid #2980b9`, padding 8px 12px, 0.9rem. `.ex-table` — full width, 0.88em; th background `#1a5276` white text, padding 6px 8px, left-aligned; td 1px `#ddd` border, padding 6px 8px; even rows `#f8f9fa`. `code` background `#f4f4f4`, padding 2px 6px, radius 3px, 0.9em. `ul` 0.92rem, margin 8px 0 8px 20px. Canvas `width: 100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic width/height attributes per chart (all 720×320); scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id, h)` helper; JS declares palette constants primary `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; monospace `Menlo, monospace` for filename/timestamp text inside charts.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; bar/highlight fills `rgba(26,82,118,0.35)` and `rgba(26,82,118,0.08)`; gray text `#444`/`#555`/`#999`.
- **Links:** none on this page; if any card links exist in regenerated HTML, use `.html` extensions.
