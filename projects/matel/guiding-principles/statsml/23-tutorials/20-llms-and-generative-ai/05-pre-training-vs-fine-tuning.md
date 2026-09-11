# Pre-training vs Fine-tuning

**Page type:** detail page (tutorial layout: `.card-section` blocks, each a two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Pre-training vs Fine-tuning

**Subtitle:** One long general education, then a short specialization — med school teaches medicine, residency teaches your hospital's way of practicing it

## Med School First, Residency After

**Tags:** `core idea` (blue), `running example` (green)

- **Pre-training** — the model reads a huge slice of the internet: grammar, facts, reasoning
- **That's med school** — years of general study; the graduate can discuss any disease
- **Fine-tuning** — a short second round of training on your own examples
- **That's residency** — the same doctor learns how your hospital handles its patients
- **Order is fixed** — residency only works on someone who already finished med school

*Example:* A fresh graduate knows medicine but not your intake forms; after residency they write notes exactly the way your hospital wants.

**Key point:** Pre-training builds general ability; fine-tuning shapes behavior on top of it. Both are called "training", but they differ enormously in size and solve different problems.

### Visualization (canvas `c1`, 720×300)

Two parallel horizontal timelines (doctor and model), each split into a long general stage and a short special stage.

- **Title (bold 15px `#1a5276`, top center):** "The Same Shape: Long General Stage, Short Special Stage".
- **Timeline geometry:** bars run x=70 to x=650, 44px tall; split at 86% of the width (long stage left, short stage right, continuing past x=650 for the short-stage labels).
- **Timeline 1 (y=72), label "THE DOCTOR":** long block "med school — general medicine" / sub "years: anatomy, diseases, judgment" in blue `#2a78d6` (fill alpha 0.30); short block labeled "residency" / "months" in orange `#d95926` (fill alpha 0.45).
- **Timeline 2 (y=168), label "THE MODEL":** long block "pre-training — reads the internet" / sub "months: language, facts, reasoning" in violet `#4a3aa7`; short block "fine-tune" / "hours" in green `#008300`.
- **Mapping arrows:** dashed mute `#6b7280` vertical lines (dash 4/3) connecting the centers of the long stages and of the short stages between the two timelines.
- **Captions (bottom center):** bold 13px orange "the short stage cannot replace the long one — it only specializes it" at y=254; 12px mute "block widths schematic — the real size gap is far larger (500,000x in the next section)" at y=278.

## A Support-Ticket Bot by the Numbers

**Tags:** `worked example` (green)

- **The base model** — pre-trained on about 1,000,000,000,000 words of general text
- **Your data** — 5,000 past tickets, each paired with the reply your best agent wrote
- **The size gap** — 5,000 tickets × 400 words = 2,000,000 words: 500,000x smaller
- **Before** — the base model answers in your required format 20 times out of 100
- **After** — the fine-tuned model hits the format 85 times out of 100

*Example:* Nothing about the product changed — the model already spoke English; the 5,000 examples taught it to sound like your support desk.

**Key point:** Fine-tuning is cheap because it starts from a finished model — a few thousand good examples move style from 20% to 85%, while those same examples alone could never train a model from scratch.

### Visualization (canvas `c2`, 720×300)

Split panel (dashed divider at x=370): data-size comparison on the left, before/after format-match bars on the right.

- **Title (bold 15px `#1a5276`, top center):** "Tiny Data, Big Style Change".
- **Left panel:** a 170px square at (60, 70) filled `rgba(74,58,167,0.18)` with violet `#4a3aa7` 2px outline, labeled inside "pre-training text" (bold 13px violet) and "~1,000,000,000,000 words" (12px). Beside it a tiny green `#008300` sliver (26×3 px) with a green leader line and bold green 12px labels "your 5,000 tickets" / "= 2,000,000 words". Below: 12px `#444` "5,000 × 400 words per ticket"; bold 13px orange `#d95926` "500,000x smaller"; 11px mute "(sliver not to scale — true scale is invisible)".
- **Right panel bars:** baseline y=240, chart height 160, y max 100; bars 90px wide — "base model / before fine-tuning" 20% in blue `#2a78d6` at x=430; "fine-tuned / after 5,000 tickets" 85% in green at x=580; alpha 0.65 fills, bold 15px colored "%": labels above, 12px two-line labels below; thin gray baseline.
- **Panel heading (bold 13px ink at (550, 62)):** "replies matching house format"; 11px mute "illustrative rates" beneath.

## Which Problem Needs Which Training

**Tags:** `rule of thumb` (blue), `where it's used` (blue)

- **Tone and format** — fine-tune: reply length, structure, house style, escalation rules
- **Fresh facts** — don't fine-tune: put the documents in the context window instead
- **General weakness** — a model bad at reasoning needs a better base, not a residency
- **Try prompting first** — ten good instructions often buy much of what 5,000 examples do
- **Cost ladder** — prompt (minutes) < fine-tune (hours) < pre-train (months)

*Example:* Asked about a product launched last week, the fine-tuned bot still guesses — that fact was in neither round of training.

**Key point:** Style problems are fine-tuning problems; knowledge problems are context problems; capability problems are base-model problems. Match the tool to the problem.

### Visualization (canvas `c3`, 720×300)

Two three-bar panels (dashed divider at x=360) showing which fix moves which dial.

- **Title (bold 15px `#1a5276`, top center):** "Each Fix Moves a Different Dial"; 11px mute "illustrative rates" beneath.
- **Panel geometry (each):** baseline y=228, chart height 138, y max 100, bar width 74, alpha 0.65 fills, bold 13px colored "%" labels above bars, 11px two-line bar labels below, thin gray baseline; bold 13px ink panel title at y=58; bold 12px colored note at y=282.
- **Left panel (x=60, width 270), title "House-format match":** bars "base model" 20% blue `#2a78d6`, "base + prompting" 60% aqua `#199e70`, "fine-tuned" 85% green `#008300`; note (green): "style: fine-tuning wins".
- **Right panel (x=400, width 270), title "Accuracy, last week’s product":** bars "base model" 10% blue, "fine-tuned" 12% green, "docs in window" 90% orange `#d95926`; note (orange): "facts: the window wins".

## The Classic Mix-Up: "Fine-Tune It on Our Docs"

**Tags:** `common mistake` (red)

- **The hope** — fine-tune on the company wiki so the model "knows" the wiki
- **What happens** — it learns to sound like the wiki, not to recall it reliably
- **Why** — fine-tuning nudges habits; it is an unreliable way to store new facts
- **Residency test** — residency never re-teaches anatomy; it teaches procedures
- **The fix** — keep facts in the window (retrieval); keep style in the fine-tune

*Example:* Fine-tuned on 300 wiki pages, the bot quotes prices in perfect house style — and still gets nearly 4 in 10 of them wrong.

**Key point (labeled "Common mistake:"):** using fine-tuning as a knowledge upload. It changes how the model answers far more reliably than what it knows.

### Visualization (canvas `c4`, 720×300)

Three-bar chart comparing wiki-question accuracy across approaches, with delta brackets.

- **Title (bold 15px `#1a5276`, top center):** "Answering Questions About the Company Wiki".
- **Data:** "base model / never saw the wiki" 55% blue `#2a78d6`; "fine-tuned / on 300 wiki pages" 62% magenta `#d55181`; "wiki pages / in the window" 92% green `#008300`.
- **Bars:** plot x=110 width 500, baseline y=226, chart height 150, y max 100, bar width 110, alpha 0.65 fills; bold 15px colored "N% correct" labels above; 12px two-line labels below; thin gray baseline.
- **Delta brackets:** magenta line between bars 1→2 labeled bold "+7 points"; green line between bars 2→3 labeled bold "+30 points".
- **Callout (bold 13px red `#e74c3c`, centered at y=62):** "fine-tuning barely moved the facts — putting pages in the window did".
- **Note (11px mute, right-aligned):** "illustrative rates".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`, social-graph reference skeleton). `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks; each has an `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) and a `table.layout` with `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` row of colored pill spans first, then a `<ul>` of one-line bullets each opening with `<b>` (bold terms colored `#1a5276`), one italic `.example` paragraph, one `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, `<strong>` label).
- **Tag pill colors:** blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`. Pills: 0.72rem, weight 600, padding 2px 10px, radius 10px.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem; canvases `width:100%` with `1px solid #e0e0e0` border, radius 4px. No nav bar, no back/home links, no cross-page links.
- **Canvas:** all charts 720×300 logical, scaled by `window.devicePixelRatio` via a shared `setup(id)` helper (fixed 720×300 rect, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), `ctx.scale` back to logical coordinates). Hardcoded data arrays only — no `Math.random()`.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
