# ETL Structure

**Page type:** detail page (backlog-style 2-col text/viz layout: numbered h2 sections, text left ~45%, canvas right ~55%)
**HTML title tag:** ETL Structure

**Subtitle:** Principled organization of Extract-Transform-Load pipelines — separation of concerns, testability, and lineage preservation

## Intro callout

**Core tension:** ETL code tends toward monolithic scripts where extraction, business logic, and storage are tangled. Debugging requires reproducing the full pipeline. The goal: each stage independently testable, replayable from intermediate artifacts, with clear contracts between stages.

## 1. Questions to Resolve

Table (`.ex-table`, columns Question / Options):

| Question | Options |
|----------|---------|
| Stage boundaries | E\|T\|L (classic) vs EL\|T (ELT) vs Extract\|Validate\|Transform\|Validate\|Load |
| Intermediate persistence | Always materialize between stages? Or streaming with checkpoints? |
| Schema enforcement | Schema-on-read vs schema-on-write vs contract tests at boundaries |
| Error handling | Fail-fast vs dead-letter queue vs quarantine-and-continue |
| Idempotency | Full replay from source vs incremental with watermarks |
| Directory layout | `raw/staged/curated` vs `bronze/silver/gold` vs domain-based |

### Visualization (canvas `c1`, 720×320)

Diagram: three horizontal pipeline variants showing different stage-boundary choices.

- **Title (bold 14px, `#1a5276`, top center):** "Where do the stage boundaries go?"
- **Three rows** at y = 70, 150, 230; each has a bold 12px `#444` left-aligned name label above the boxes at x=20, then a row of stage boxes connected by gray arrows, and an 11px `#999` note to the right of the last box:
  1. Name "E | T | L (classic)", boxes: Extract (blue `#1a5276`), Transform (orange `#e67e22`), Load (green `#27ae60`); note "transform before the warehouse".
  2. Name "EL | T (ELT)", boxes: Extract (blue), Load raw (green), Transform (orange); note "land raw first, transform in-warehouse".
  3. Name "E | V | T | V | L (gated)", boxes: Extract (blue), Validate (red `#e74c3c`), Transform (orange), Validate (red), Load (green); note "validation gates at every boundary".
- **Box style:** 3-box rows use width 130 gap 50; 5-box row uses width 100 gap 28; height 34; fill `rgba(26,82,118,0.08)` (red boxes use `rgba(231,76,60,0.10)`), 1.5px stroke and bold 12px label in the box's color; arrows between boxes in `#888` with filled triangular heads.
- **Caption (13px `#444`, bottom center):** "Same work, different boundaries — the boundary decides what is independently testable and replayable"

## 2. Structural Principles (Draft)

- Extract produces raw, unmodified source data — never business logic in extraction.
- Transform is pure: same input → same output. No network calls, no clock reads.
- Load is the only stage that mutates external state — isolated for retry/rollback.
- Validation gates between stages catch schema drift before it propagates.
- Every intermediate artifact is addressable (named, timestamped, versioned).

### Visualization (canvas `c2`, 720×320)

Diagram: gated pipeline with addressable artifacts hanging below each stage.

- **Title (bold 14px `#1a5276`, top center):** "Each stage has one job; gates and artifacts sit between"
- **Three stage boxes** (150×62 at y=80): EXTRACT at x=25 (blue `#1a5276`, fill `rgba(26,82,118,0.08)`, lines "EXTRACT" / "raw, unmodified" / "no business logic"); TRANSFORM at x=285 (orange `#e67e22`, fill `rgba(230,126,34,0.10)`, lines "TRANSFORM" / "pure function" / "no network, no clock"); LOAD at x=545 (green `#27ae60`, fill `rgba(39,174,96,0.10)`, lines "LOAD" / "only stage mutating" / "external state").
- **Two gate boxes** (46×46 at x=217 and x=477, y+8): red `#e74c3c` stroke, fill `rgba(231,76,60,0.08)`, labeled "gate" (bold 11px) over "schema" (10px). Gray arrows connect stage → gate → stage.
- **Artifacts:** dashed gray drop-lines from below each stage down to bordered `#f4f4f4` monospace-label boxes: "raw/orders-20260811.json" (x=100), "staged/orders-20260811.parquet" (x=360), "curated (target table)" (x=620).
- **Note (11px `#999`, centered):** "every intermediate artifact: named, timestamped, versioned — replay starts here, not at the source"
- **Replay annotation:** green `#27ae60` dashed elbow line from the staged artifact (x=360, y=250) right to x=620, with bold green 12px text near the bottom: "failure in Load? re-run from the staged artifact — Extract and Transform never repeat"

## 3. Anti-Patterns

- Transform that reads from DB mid-flight (non-deterministic joins)
- Extract that also filters/transforms ("smart extraction")
- Load that silently coerces types to fit the target schema
- No intermediate artifacts — failure at step 47 means replaying from step 1
- Shared mutable state between pipeline branches

### Visualization (canvas `c3`, 720×320)

Diagram: the same three-stage pipeline drawn with anti-pattern annotations and red X marks.

- **Title (bold 14px `#1a5276`, top center):** "The tangled version: same three boxes, wrong arrows"
- **Three stage boxes** (150×50 at y=95): EXTRACT at x=40 (blue, lines "EXTRACT" / "+ hidden filtering"); TRANSFORM at x=285 (orange, lines "TRANSFORM" / "+ live DB reads"); LOAD at x=530 (green, lines "LOAD" / "+ silent type coercion"). Gray arrows Extract→Transform→Load.
- **DB back-channel:** red dashed vertical line from a red-bordered box (100×34 at x=310, y=220, fill `rgba(231,76,60,0.08)`) labeled "live DB" (bold red 12px) up into TRANSFORM; a red X mark at (360,185) with bold red text "mid-flight read →" at x=275; 11px `#999` caption below the DB box: "non-deterministic joins:" / "same input ≠ same output".
- **Smart extraction annotation:** red X mark above EXTRACT at (115, y−22), bold red 12px "\"smart extraction\"" above it, 11px `#999` "raw data is no longer raw" below the box.
- **Coercion annotation:** red X mark above LOAD at (605, y−22), bold red 12px "coerces to fit target" above it, 11px `#999` "drift enters the warehouse silently" below the box.
- **Caption (13px `#444`, bottom center):** "And with no intermediate artifacts, a failure at step 47 replays from step 1"

## Status footer

Status: stub. Needs brainstorming session. (small gray `#999` 12px text at page bottom)

## Regeneration instructions

- **Template:** backlog detail-page layout — h1 with 2px `#2980b9` bottom border, `.subtitle`, `.intro` callout (background `#f0f4f8`, left border `3px solid #2980b9`, 8px 12px padding, 0.9rem), then one `.lang-section` per numbered section: h2 (1.3rem `#1a5276`, 2px `#2980b9` bottom border) followed by a `table.layout` row with `td.text-col` (45%) holding the table/bullets and `td.viz-col` (55%) holding the canvas.
- **Table style:** `.ex-table` — full width, collapsed borders, 0.88em; `th` background `#1a5276` white text; `td` 6px 8px padding, `1px solid #ddd` border; even rows `#f8f9fa`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `ul` 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius; `code` on `#f4f4f4` background.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; grays `#444`/`#555`/`#888`/`#999`.
- **Canvas:** intrinsic width 720, heights as given; a shared `setup(id, h)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
