# Data Collection as a Side Effect

**Page type:** detail page (backlog-style two-column layout: text left 45%, canvas right 55%, one `.lang-section` per topic; h1 carries a BACKLOG status pill)
**HTML title tag:** Data Collection as a Side Effect

**Subtitle:** Normal use of a product leaves behind a dataset worth more than the service delivered.

**Intro callout:** Normal product use leaves behind a dataset that was never sampled by design. Where the byproduct record flows — back into the same service or into a different product — determines both the incentives and the statistical traps.

## 1. Closed Loop vs Open Loop

One interaction, two outputs: the user gets a service, the operator keeps a record. The distinction that matters is where that record goes.

- **Closed** — feeds the same service. Traffic traces improve the next route.
- **Open** — feeds a different product. CAPTCHA answers trained OCR; the user got no OCR.

**Key point:** In a closed loop the model trained on the data also decides who generates the next batch — so the sample is never independent of the model fitted to it.

### Visualization (canvas `c1`, 720×300)

Flow diagram: one interaction splitting into two outputs, each output feeding a loop type.

- **Title (bold 16px, `#1a5276`, top center):** "One Interaction, Two Outputs".
- **Boxes:** 150×46px, fill is box color + `33` alpha suffix, 2px stroke in box color, 13px label centered (with optional 12px `#5a6875` sub-label below):
  - "Normal use" (`#1a5276`) centered at top, y=46.
  - "Service to user" / sub "stated purpose" (`#27ae60`) at left x=56, y=138.
  - "Record kept" / sub "byproduct" (`#e67e22`) at right, y=138.
  - "Same service" / sub "closed loop" (`#2980b9`) at left x=56, y=232.
  - "Other product" / sub "open loop" (`#8e44ad`) at right, y=232.
- **Arrows** (1.8px lines with filled triangular heads): "Normal use" → "Service to user" in `#27ae60`; "Normal use" → "Record kept" in `#e67e22`; "Record kept" → "Other product" in `#8e44ad` (vertical); "Record kept" → "Same service" in `#2980b9` (diagonal); dashed arrow from "Same service" back up to "Service to user" in `#2980b9`.
- **Annotation:** 12px `#2980b9` text "steers the next sample" next to the dashed arrow (left-aligned, x ≈ 141, y=208).

### Comparison table (full width, below the section row)

Styled `table.compare` — header row `#1a5276` background, white text; even rows `#f8fafb`; first column bold.

| Application | Byproduct | Loop |
|---|---|---|
| **reCAPTCHA** | Transcriptions of words OCR failed on; later, labels on street imagery | Open |
| **Duolingo** | Learner translations, once resold as a translation service (retired 2017) | Both |
| **Pokémon Go** | Pedestrian-scale GPS traces and opt-in camera scans of locations | Open |
| **Maps / Waze** | Speed and position traces — what live traffic estimates are computed from | Closed |
| **Phone keyboards** | Next-word and correction signal fitting the language model | Closed |

## 2. Why the Sample Is Not the Population

Byproduct data arrives with a frame nobody designed:

- **Frame is the user base** — "people who kept using this," not the population. Coverage error is structural.
- **Records aren't users** — contribution is heavy-tailed, so an unweighted mean is an average over user-*time*.
- **Absence is ambiguous** — an unmapped street may not exist, or nobody walked it.

**Key point:** Aggregate to one row per user before averaging, or state plainly that the estimate is record-weighted.

### Visualization (canvas `c2`, 720×300)

Lorenz-curve chart of per-user record contribution vs the equality diagonal.

- **Title (bold 16px, `#1a5276`, top center):** "Records per User Are Heavy-Tailed".
- **Plot area:** x=80, y=44, width = canvas−190, height = canvas−100; L-shaped axes in `#95a5a6` (1.4px).
- **Axis labels (13px `#4a5866`):** x-axis "Users, least → most active" centered below; y-axis "Share of records" rotated −90° at x=20.
- **Equality diagonal:** dashed (6/5) green `#27ae60` line, 2.2px, from bottom-left to top-right.
- **Observed curve:** y = x^4.2 (power curve, k=4.2), drawn 0→1 in 100 steps; stroke `#e74c3c` 3px; area between curve and top-right corner filled `rgba(231,76,60,0.18)`.
- **Labels (13px):** "equal contribution" in `#27ae60` at ~30% width, 40% height (left-aligned); "observed" in `#e74c3c` right-aligned near bottom-right of plot.
- **Caption (13px `#e67e22`, bottom center):** "gap = record-weighted vs user-weighted".

## Regeneration instructions

- **Layout:** backlog detail page. `h1` (2rem `#1a5276`, bottom border `2px solid #2980b9`) with inline `.status` pill "BACKLOG" (background `#fef9e7`, border `1px solid #f39c12`, text `#b7950b`, 4px radius, 0.8rem); `.subtitle` (`#666`, 0.95rem); `.intro` callout (background `#f0f4f8`, left border `3px solid #2980b9`, 8px 12px padding, 0.9rem). One `.lang-section` per numbered h2 (1.3rem `#1a5276`, bottom border `2px solid #2980b9`); inside each, `table.layout` with `td.text-col` 45% and `td.viz-col` 55%, both `vertical-align: top`, 12px padding. No index number in the h1.
- **Table:** section 1 additionally has a full-width `table.compare` after the layout table — th background `#1a5276` white text 8px 12px padding, td 8px 12px with `1px solid #eee` bottom border, even rows `#f8fafb`, font 0.9em.
- **Text blocks:** intro `<p>`, `<ul>` bullets (0.92rem) with `<strong>` lead-ins, `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, 0.9rem).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. Canvases have `width: 100%`, `1px solid #e0e0e0` border, 4px radius.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange; secondary `#2980b9` blue and `#8e44ad` purple; gray labels `#5a6875`/`#4a5866`.
- **Canvas:** intrinsic 720×300; sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), `ctx.scale` back to logical coordinates via a shared `setupCanvas(id)` helper.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
