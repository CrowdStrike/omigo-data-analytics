# Pitfall: NULL Semantics Mismatch

**Page type:** detail page (three `.card-section` blocks, each a two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** NULL Semantics Mismatch

**Subtitle:** When NULL means different things in different columns or contexts, creating silent misinterpretation of missing data.

## The Problem

**Tags:** `the trap` (red pill), `nulls` (blue pill)

- **No inherent meaning** — one NULL symbol covers not applicable, not collected, and declined
- **Column-specific** — NULL phone means no phone; NULL last_purchase means never purchased
- **Uniform treatment** — undocumented NULLs force models to treat all these signals identically
- **Wrong imputation** — mean imputation suits MCAR but fails badly on MNAR missingness
- **Join loss** — NULL never equals NULL in SQL, so joins on nullable keys drop rows silently

*Example:* NULL last_login_date means never logged in, a strong churn signal, while NULL preferred_language just means not asked yet.

**Impact:** Models misinterpret signal as noise, imputation destroys information, and joins silently drop NULL-keyed rows.

### Visualization (canvas `c1`, 720×300)

Three-column diagram: the same NULL symbol carrying three different meanings and imputations.

- **Title (bold 14px, `#1a5276`, top center):** "NULL Semantics: Same Symbol, Different Meanings".
- **Three column boxes** (170×200 each, 2px `#1a5276` stroke, starting x=50 with 180px spacing, y=60). Each box contains: a bold blue 11px column header; a bold orange (`#e67e22`) 16px "NULL"; 10px `#444` "Meaning:" plus two meaning lines; two bold green (`#27ae60`) 10px impute/feature lines.
  - **Column A: phone_number** — Meaning: '"Not Applicable"' / "User has no phone"; Impute: '"no_phone"'; Feature: "has_phone=0".
  - **Column B: last_login** — Meaning: '"Never Occurred"' / "Strong churn signal"; Impute: "days=-999"; Feature: "never_login=1".
  - **Column C: survey_q5** — Meaning: '"Not Yet Asked"' / "MCAR, neutral"; Impute: "median/mode"; Or: "drop column".
- **Bottom warning (bold red `#e74c3c` 12px, centered, y=280):** "Treating all three identically destroys information!"

## Why It Happens

**Tags:** `root cause` (orange pill), `lost context` (blue pill)

- **Context decay** — NULL survives every pipeline hop, but the meaning behind it does not
- **Thin dictionaries** — data dictionaries record nullable yes/no, never what NULL means
- **Default mindset** — treating every NULL as unknown conflates absent data with absent signal
- **Mixed encodings** — one source sends NULL, another an empty string, another -1
- **Schema evolution** — new nullable columns backfill history with NULLs and no stated reason

*Example:* A CRM migration maps both "field not on old form" and "customer declined" to NULL, and a year later no one can tell who refused.

**Root Cause:** NULL is one database concept forced to carry four or more real-world meanings, and without metadata the context is lost.

### Visualization (canvas `c2`, 720×300)

Diagram of three source systems with different NULL meanings merging into one context-free column.

- **Title (bold 14px, `#1a5276`, top center):** "Multiple Sources, One NULL — Context Lost".
- **Three source boxes** (210×50 each at x=30, stacked from y=55 with 65px spacing, 2px stroke in the system's color; bold 11px system label in the same color, 10px `#444` meaning line; gray 1.5px arrows converging on the merged box):
  - "System A" (`#1a5276`): 'NULL = "not applicable"'
  - "System B" (`#e67e22`): 'NULL = "not collected yet"'
  - "System C" (`#e74c3c`): 'NULL = "declined to answer"'
- **Merged table box:** 200×100 at (370,100), 3px `#e74c3c` stroke; bold blue 12px "Merged Table" and 'Column: "last_login"'; two stacked bold orange 14px "NULL" values; red 10px "(no context preserved!)".
- **Bottom annotations (centered):** bold red 11px "All 3 meanings collapsed into identical NULL — information destroyed" at y=250; 10px `#555` 'Model sees one "missing" pattern where there are actually three distinct signals' at y=272.

## The Correct Approach

**Tags:** `the fix` (green pill), `missingness` (blue pill)

- **Missingness is data** — preserve the reason each NULL exists instead of eliminating it
- **Document per column** — the dictionary must state what NULL means, not just nullable yes/no
- **Indicator features** — add boolean is_missing columns that turn absence into usable signal
- **Match imputation to type** — mean for MCAR, conditional for MAR, indicators for MNAR
- **Reason codes** — a documented convention like -1 not applicable, -2 not collected, -3 declined

*Example:* A churn model replaces NULL last_login with a has_logged_in flag plus a missing_reason enum, and the never-logged-in reason becomes a top predictor.

**Fix:** Every NULL column should expand into the value when present plus the reason for absence — the reason IS the feature.

### Visualization (canvas `c3`, 720×300)

Diagram expanding one ambiguous NULL column into three explicit columns.

- **Title (bold 14px, `#1a5276`, top center):** "Correct: Expand NULL Into Explicit Columns".
- **Original column box:** 140×50 at (30,55), 2px `#e74c3c` stroke; bold blue 11px "last_login", red 12px "NULL".
- **Three green (`#27ae60`, 2px) expansion arrows** fan out from the original box to three stacked boxes on the right (each 230px wide, 2px `#27ae60` stroke, bold blue 11px name, 10px `#444` description, green ✓ at the right edge):
  - **last_login_value** (230×45 at (225,40)) — "The date (or NULL if truly absent)".
  - **last_login_missing_reason** (230×60 at (225,100)) — "enum: never_logged_in" / "not_tracked | system_error".
  - **has_logged_in** (230×45 at (225,175)) — "boolean feature (direct model input)".
- **Right-side summary (centered at x=590):** bold green 12px "Context preserved!" at y=100; 10px `#555` "Each NULL type becomes" / "a distinct, usable signal".
- **Bottom note (bold green 11px, centered, y=260):** "1 ambiguous NULL → 3 explicit, informative columns".

## Regeneration instructions

- **Layout:** three `.card-section` blocks, each with an `<h2>` (1.3rem, `#1a5276`, 2px `#2980b9` bottom border) and a `table.layout` (full width, border-collapse) with one row: `.text-col` `<td>` (45%) and `.viz-col` `<td>` (55%). Text cell holds a `.tags` div of pill spans, a `<ul>` of `<li><b>Label</b> — sentence</li>` bullets, an italic `.example` paragraph, and a `.key-point` callout (`#f8f9fa` background, 3px `#e74c3c` left border, `<strong>` lead word).
- **Tag pills:** inline-block, 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width: 100%` with 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** each canvas declares `width="720" height="300"`; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#444`/`#555`.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
