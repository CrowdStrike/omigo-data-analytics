# Filename Naming Conventions

**Page type:** detail page (backlog-style two-column layout table: text left 45%, canvas right 55%, one `.card-section` per numbered h2; section 6 also has a full-width comparison table)
**HTML title tag:** Filename Naming Conventions

**Subtitle:** Consistent, sortable, glob-friendly naming for pipeline artifacts, configs, models, and docs.

**Philosophy callout (blue accent):** **Core tension:** Human readability vs machine sortability. Filenames are the first metadata layer — they should encode enough to find, sort, and deduplicate without opening the file.

## 1. Anatomy of a Well-Formed Name

A filename is a record with positional fields. Fix the field order once and every consumer can parse it without a catalog lookup.

- **Dataset** — which pipeline produced it.
- **Entity / grain** — what one row means.
- **Date** — ISO-8601 only, never local formats.
- **Version** — schema or model revision (`v3`), or a directory level (`v3/model.h5`).
- **Partition** — zero-padded shard index (`part-0007`).
- **Extension** — real format, not a guess. Compound suffixes (`.train.json`) only when the second-to-last token is a genuine role, not a comment.

**Key point (red-accent callout):** **Field order rule:** most-stable field first, most-volatile last. That makes the prefix encode sequence/priority and the suffix encode type/version — and a filename alone, stripped of its directory path, stays locally unambiguous.

**Example (italic):** Example: `clickstream_web-sessions_2026-03-14_v3_part-0007.parquet` tells you dataset, grain, day, schema version, and shard without a single read.

### Visualization (canvas `c1`, 720×300)

Annotated filename anatomy diagram plus a delimiter legend.

- **Title (bold 14px `#1a5276`, centered):** "Anatomy of a Filename: Positional Fields"
- **Filename segments:** rendered as a centered row of boxes (bold 13px monospace text, box height 30 at y=96, box width = text width + 12, tinted fill = segment color + `18` alpha suffix, 1.5px stroke in segment color), joined by red `#e74c3c` underscore separator glyphs. Each box has a thin gray `#ccc` connector to a 10px label in the segment color, alternating above/below:
  - `clickstream` — label "dataset" (above) — `#1a5276`
  - `web-sessions` — label "entity / grain" (below) — `#27ae60`
  - `2026-03-14` — label "date (ISO-8601)" (above) — `#e67e22`
  - `v3` — label "version" (below) — `#e74c3c`
  - `part-0007` — label "partition" (above) — `#1a5276`
  - `.parquet` — label "extension" (below) — `#27ae60`
- **Delimiter legend (three rows starting y≈196, 18×18 tinted swatch boxes with the glyph, 11px `#2c3e50` text):**
  - `_` (red `#e74c3c`): "field separator — splits the name into positional fields"
  - `-` (green `#27ae60`): "word separator — only inside a single field"
  - `.` (orange `#e67e22`): "extension only — never used to separate fields"
- **Caption (11px `#888`, bottom center):** "Stable fields first, volatile fields last — split by \"_\" gives fields at fixed positions"

## 2. Lexicographic Sort Must Equal Reading Order

**Principle 1:** filenames sort lexicographically in the order you would read them. Every tool that lists files — `ls`, S3 `ListObjects`, git, a file picker — sorts by bytes, not by meaning.

- **Dates:** `2026-03-14` sorts chronologically for free. `03-14-2026` and `14-Mar-2026` do not.
- **Numbers:** zero-pad to a fixed width or `10` sorts before `2`.
- **Prefix width:** 2-digit (`01-`) is enough only if the set is bounded well under a hundred; 3-digit (`001-`) costs one character and never needs a rename sweep. Renumbering later rewrites every link that pointed at the old name.
- **Time:** if sub-day ordering matters, use a compact UTC stamp (`20260314T021055Z`) — still byte-sortable.

**Key point (red-accent callout):** **Impact:** when sort order is correct by construction, "the latest file" is just the last line of a listing — no metadata service, no timestamp parsing, no tie-break logic.

### Visualization (canvas `c2`, 720×300)

Two side-by-side panels comparing lexicographic listings of ISO vs local date formats.

- **Title (bold 14px `#1a5276`, centered):** "Byte Sort Order: Chronological or Scrambled?"
- **Subtitle (10px `#888`):** "both lists as returned by a plain lexicographic listing"
- **Panels:** each 324×190 at y=56, tinted fill (panel color + `10`), 2px stroke; bold 11px centered title; small "true order" header at top right; four rows of 11px monospace filenames with a circular rank badge (radius 10, tinted fill, 1px stroke, bold 10px number = true chronological rank); 10px panel note at bottom center in panel color.
  - Left panel (x=24, green `#27ae60`), title "ISO-8601 date field":
    - `sales_2025-11-02.csv` rank 1
    - `sales_2025-12-24.csv` rank 2
    - `sales_2026-01-08.csv` rank 3
    - `sales_2026-03-14.csv` rank 4
    - Note: "sorted = chronological"
  - Right panel (x=372, red `#e74c3c`), title "Local MM-DD-YYYY date field":
    - `sales_01-08-2026.csv` rank 3
    - `sales_03-14-2026.csv` rank 4
    - `sales_11-02-2025.csv` rank 1
    - `sales_12-24-2025.csv` rank 2
    - Note: "sorted by month, years interleaved"
- **Caption (11px `#888`, bottom center):** "\"the latest file\" is the last line of the listing — only if the date field is ISO-8601"

## 3. Delimiter Grammar: One Separator per Level

The separator debate (`kebab-case` vs `snake_case`) is not about taste. If one character does both jobs, splitting the name becomes ambiguous.

- **Reserve `_` for fields** — `split('_')` returns fields at fixed positions.
- **Reserve `-` for words inside a field** — `web-sessions` stays one field.
- **Reserve `.` for extensions** — never as a field separator.
- **Never mix conventions** within one directory level; a mixed directory has no parser at all.

**Key point (red-accent callout):** **The failure mode:** `clickstream_web_sessions_2026_03_14_v3.parquet` shatters the date into three fields and shifts every position downstream. Any two-word value silently breaks the parser.

**Example (italic):** Example: adding one entity named `ad_clicks` instead of `ad-clicks` moves the date field by one index for that file only — the worst kind of bug, since most files still parse.

### Visualization (canvas `c3`, 720×300)

Two annotated split() rows — one parsing correctly, one breaking — separated by a gray divider.

- **Title (bold 14px `#1a5276`, centered):** "split(\"_\") — One Separator per Level"
- **Row 1 (y=60, green `#27ae60`, tag "PARSES"):** filename `clickstream_web-sessions_2026-03-14_v3.parquet` in 11px monospace, then its split fields drawn as a full-width row of equal-width tinted boxes (fill = color + `14`, 1.2px stroke, height 26) each with the field text (9-10px monospace) and a small gray `[i]` index label below: `clickstream`, `web-sessions`, `2026-03-14`, `v3.parquet`. Note below (10px, green): "hyphens stay inside a field, so every index means the same thing in every file"
- **Divider:** 1px `#ccc` horizontal line at y=168.
- **Row 2 (y=192, red `#e74c3c`, tag "BREAKS"):** filename `clickstream_web_sessions_2026_03_14_v3.parquet`; split fields: `clickstream`, `web`, `sessions`, `2026`, `03`, `14`, `v3.parquet`. Note: "the date is shattered and every field after the entity has shifted position"
- **Caption (11px `#888`, bottom center):** "A parser that depends on field count fails the first time a value contains two words"

## 4. Pattern Matchability

A naming scheme is only as good as the queries it supports. The test: can you select an arbitrary date range with a single wildcard, or must you list everything and filter in application code?

- **ISO date in the name** — one glob per month or day; prefix pushdown works on object stores.
- **Directory partitions** (`dt=2026-03-14/`) — same benefit, plus engines prune partitions without opening files.
- **Month words** — matchable per month, but no range: `march` and `april` share no usable prefix.
- **Epoch seconds or random ids** — no prefix structure, so every query degrades to a full listing plus a conversion step.

**Key point (red-accent callout):** **Cost of getting it wrong:** on a bucket with millions of keys, a scheme without a sortable prefix turns a one-month read into a full-bucket scan. The naming choice, not the compute, becomes the bottleneck.

### Visualization (canvas `c4`, 720×300)

Table-style chart of naming schemes vs glob patterns with verdict badges.

- **Title (bold 14px `#1a5276`, centered):** "Selecting One Month: One Glob or a Full Listing?"
- **Column headers (bold 10px `#1a5276`):** "SCHEME" (x=24), "EXAMPLE KEY" (x=132), "PATTERN FOR 2026-03" (x=356), "SELECTION" (centered at x≈660); 1px `#ccc` rule below headers; even rows striped `rgba(26,82,118,0.04)`.
- **Rows (scheme 11px system-ui `#2c3e50`; example key 10px monospace `#555`; pattern 10px monospace in verdict color; verdict badge 124×22 tinted box (color + `1e`) with 1px stroke and bold 10px centered text):**
  | Scheme | Example key | Pattern for 2026-03 | Verdict | Color |
  |---|---|---|---|---|
  | ISO date field | `sales_2026-03-14.csv` | `sales_2026-03-*.csv` | one glob | `#27ae60` |
  | Dir partition | `dt=2026-03-14/sales.csv` | `dt=2026-03-*/*.csv` | one glob | `#27ae60` |
  | Month word | `sales_march-2026.csv` | `sales_march-2026.csv` | no ranges | `#e67e22` |
  | Epoch seconds | `sales_1772064000.csv` | `(none expressible)` | list + convert | `#e74c3c` |
  | Random run id | `sales_9f3c1a7b42.csv` | `(none expressible)` | full listing | `#e74c3c` |
- **Caption (11px `#888`, bottom center):** "A sortable prefix turns a month query into a prefix scan instead of a bucket scan"

## 5. Character Hazards and Length Limits

**Principle 2:** no spaces, no uppercase, no special characters beyond hyphen, underscore, and dot. Each banned character breaks a different layer, and the layer that breaks is rarely the one you test on.

- **Space** — word-splits in shells and unquoted scripts; needs `%20` in URLs.
- **Colon** — illegal on Windows, so a checkout of the repo fails there even if CI is green on Linux.
- **Uppercase** — case-insensitive filesystems collide `Sales.csv` with `sales.csv`; object stores treat them as distinct keys, so the two layers disagree.
- **Unicode / emoji** — macOS and Linux normalize differently (NFD vs NFC), producing two names that look identical and hash differently.
- **Query characters** (`#`, `?`, `&`) — truncated or reinterpreted the moment the path becomes a URL.
- **Leading hyphen** — parsed as a command-line flag.
- **Length** — object keys allow long paths, but Windows `MAX_PATH` and nested build directories cut in far earlier. Budget the full path, not just the filename.

**Key point (red-accent callout):** **Practical rule:** restrict names to `[a-z0-9._-]` and enforce it with a lint check at write time. A name that never enters the system cannot break a downstream consumer.

### Visualization (canvas `c5`, 720×300)

Hazard-by-layer matrix: which layer breaks first for each risky character.

- **Title (bold 14px `#1a5276`, centered):** "Character Hazards: Which Layer Breaks First"
- **Legend line (10px `#888`, centered):** "x  breaks        ~  needs escaping / risky        .  safe"
- **Columns (bold 10px `#1a5276`, centered at x = 330, 425, 520, 620):** "Shell", "S3 / URL", "Windows", "Git / CI"; left header "HAZARD" at x=24; 1px `#ccc` rule below; even rows striped `rgba(26,82,118,0.04)`.
- **Rows (character glyph 10px monospace `#1a5276`, label 11px `#2c3e50`; cell marks: `x` bold red `#e74c3c`, `~` bold orange `#e67e22`, safe = small green `#27ae60` dot radius 4):**
  | Glyph | Hazard | Shell | S3/URL | Windows | Git/CI |
  |---|---|---|---|---|---|
  | " " | space | x | ~ | ~ | ~ |
  | : | colon | ~ | ~ | x | x |
  | A-Z | uppercase | . | . | x | x |
  | utf8 | unicode / emoji | ~ | ~ | ~ | x |
  | #?& | query characters | ~ | x | . | . |
  | -x | leading hyphen | x | . | . | ~ |
  | >255 | long full path | . | ~ | x | ~ |
- **Caption (11px `#888`, bottom center):** "Restrict names to [a-z0-9._-] and every column above turns safe"

## 6. Collisions, Anti-Patterns, and Open Questions

**Principle 4:** a filename alone should be locally unambiguous. Two different runs that produce the same name means one write silently destroys the other.

- **Add the dimension that differs** — region, tenant, source system — as its own field.
- **Add a run identity** — a UTC run stamp or run id distinguishes a backfill from the nightly job for the same logical day.
- **Keep the logical date separate from the run time** — event date answers "what period", run stamp answers "which attempt".

**Anti-patterns:**

- `final_v2_FINAL_new.csv` — no versioning discipline.
- `data (1).json` — spaces, parens, no semantics.
- `a.py`, `b.py`, `c.py` — zero information content.
- Mixing conventions within one directory level.

**Key point (red-accent callout):** **Why it goes unnoticed:** overwrite-on-collision is not an error. The pipeline reports success, row counts look plausible, and the loss only surfaces when someone reconciles totals weeks later.

### Visualization (canvas `c6`, 720×300)

Two-panel flow diagram: collision (top) vs disambiguated names (bottom).

- **Title (bold 14px `#1a5276`, centered):** "Two Runs, One Name: Collision vs Disambiguation"
- **Top panel — tag "COLLISION" (bold 10px red `#e74c3c`, at 24,52):**
  - Two source boxes (168×34, fill `rgba(26,82,118,0.06)`, 1.2px `#1a5276` stroke; bold 10px title + 9px `#666` subtitle): "nightly run" / "event date 2026-03-14, region eu" (at 24,60) and "backfill run" / "event date 2026-03-14, region us" (at 24,104).
  - Red arrows from both source boxes converging into ONE file box (292×30, fill red + `14`, 1.5px red stroke, 9px monospace centered): `sales_2026-03-14.csv`.
  - Red annotation text (10px, at x=560): "second write overwrites the first —" / "no error, no warning"
- **Divider:** 1px `#ccc` horizontal line at y=152.
- **Bottom panel — tag "DISAMBIGUATED" (bold 10px green `#27ae60`, at 24,174):**
  - Same two source boxes (at 24,182 and 24,234), each with a green arrow to its OWN green file box: `sales_eu_2026-03-14_run-20260315T0210Z.csv` and `sales_us_2026-03-14_run-20260316T0155Z.csv`.
  - Green annotation text (10px, at x=560): "region field + run stamp" / "make each write" / "independently addressable"
- **Caption (11px `#888`, bottom center):** "Event date answers \"which period\"; run stamp answers \"which attempt\""

### Comparison table (full-width `table.compare`, below the section)

| Question to resolve | Options |
|---|---|
| Separator convention | `kebab-case` vs `snake_case` vs mixed (one per level) |
| Numeric prefix width | 2-digit (`01-`) vs 3-digit (`001-`) — when do you outgrow 99? |
| Version embedding | `-v3` suffix vs directory-based (`v3/model.h5`) |
| Extension conventions | Double extensions (`.train.json`)? Compound names instead? |
| Max filename length | Practical limits across OS, git, URLs |

**Status footer (12px `#999`):** Status: stub. Needs brainstorming session.

## Regeneration instructions

- **Template:** backlog detail page (kusto-style 2-col text/viz layout). Structure: h1, `.subtitle` paragraph, `.philosophy` callout, then one `.card-section` per section — each with an h2 (numbered "N. Title") and a `table.layout` with `td.text-col` (45%) and `td.viz-col` (55%) holding the canvas. Section 6 appends a full-width `table.compare` after the layout table. Ends with a small gray status paragraph. No index number in the page h1/title.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; section h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border. `.philosophy` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.9rem. `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem. `code` ui-monospace, background `#f4f6f8`, color `#1a5276`, padding 1px 5px, radius 3px, 0.86rem. `table.compare` full width, 1px `#e0e0e0` cell borders, padding 8px 12px, 0.88rem; th background `#f8f9fa` color `#1a5276` weight 600. `ul` 0.92rem, margin 8px 0 8px 20px. Canvas `width: 100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic width/height attributes per chart (all 720×300); scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; monospace font stack `Menlo, Consolas, monospace` for filename text inside charts. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; stripe fill `rgba(26,82,118,0.04)`; gray text `#555`/`#666`/`#888`.
- **Links:** none on this page; if any card links exist in regenerated HTML, use `.html` extensions.
