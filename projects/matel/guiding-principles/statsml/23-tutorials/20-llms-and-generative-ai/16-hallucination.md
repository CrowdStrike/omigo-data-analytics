# Hallucination

**Page type:** detail page (tutorial layout: h1 + subtitle, then one `.card-section` per concept, each with h2 and a two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Hallucination

**Subtitle:** The model predicts what a good answer looks like, not what is true — so it can produce a perfect-looking citation for a paper that doesn't exist

## A Perfect-Looking Citation for a Paper That Doesn't Exist

**Tags:** `core idea` (blue), `failure mode` (red)

- **The ask** — "give me a source showing coffee improves memory"
- **The answer** — authors, year, title, journal, volume, pages: formatted perfectly
- **The catch** — no such paper exists; the journal name only sounds real
- **Why** — the model learned what citations look like, not which ones exist
- **The name** — this is a hallucination: fluent output with no fact behind it

*Example (italic):* Two plausible surnames, a typical year, a real-sounding journal — assembled into a reference to nothing.

**Key point:** The model predicts the shape of a good answer; whether the answer is true is not part of that prediction.

### Visualization (canvas `c1`, 720×300)

Annotated citation diagram: anatomy of the invented citation.

- **Title (bold 15px, `#1a5276`, top center):** "Anatomy of the Invented Citation"
- **Citation box:** rectangle at x=80, y=96, 560×66; fill `#f8f9fa`, border 1.5px `#1a5276`; inside, two lines of 14px Georgia serif in `#222`:
  - "Rivera, M. & Chen, L. (2019). Caffeine and short-term recall."
  - "Journal of Cognitive Enhancement, 14(2), 88–102."
- **Callouts (1.5px colored leader line to bold 12px colored label; three above the box, two below):**
  - above at box x+90, blue `#2a78d6`: "plausible surnames"
  - above at box x+250, yellow `#c98500`: "typical year"
  - above at box x+440, aqua `#199e70`: "believable title"
  - below at box x+150, violet `#4a3aa7`: "real-sounding journal" / "(does not exist)"
  - below at box x+420, orange `#d95926`: "believable volume + pages" / "(point at nothing)"
- **Warning line (bold 13px red `#e74c3c`, centered near bottom):** "every part is pattern — no part was checked against any database"
- **Note (muted `#6b7280`, 12px, centered, bottom):** "a made-up example citation, shown for illustration"

## Why It Fills the Gap Instead of Stopping

**Tags:** `worked example` (green), `mechanism` (blue)

- **One word at a time** — after "Journal of Cognitive", the model ranks next words
- **The ranking** — Enhancement 24%, Psychology 21%, Science 17%, Neuroscience 12%
- **Every path is fluent** — each option completes a nice-looking citation
- **No stop option** — training text rarely says "no such paper exists" mid-citation
- **A 10-citation test** — niche topic: 4 real, 3 mangled, 3 fully invented

*Example (italic):* The mangled ones bite hardest — real authors and a real journal, but the wrong year and page numbers.

**Key point:** Each word is locally plausible; nothing in the chain ever asks "does this paper actually exist?"

### Visualization (canvas `c2`, 720×300)

Two-panel chart: next-word probability bars (left) and a 10-citation audit grid (right), split by a vertical dashed divider (`#bdc3c7`, dash 4/3) at x=390.

- **Title (bold 15px, `#1a5276`, top center):** "Every Next Word Is Fluent — and 6 of 10 Citations Fail the Lookup"
- **Left panel (subtitle bold 13px `#1a5276`, centered at x=205):** 'next word after "Journal of Cognitive …"'
  - Horizontal bars (20px tall, rows 32px apart, starting x=130, max width 200px, scale max 30%): `Enhancement` 24% blue `#2a78d6`; `Psychology` 21% aqua `#199e70`; `Science` 17% violet `#4a3aa7`; `Neuroscience` 12% yellow `#c98500`; `(all others)` 26% muted `#6b7280`. Bars at 0.75 alpha; word labels 12px `#222` right-aligned; bold 12px percentage labels in bar color to the right of each bar.
  - Annotation (orange `#d95926`, bold 12px, centered below): '"no such paper exists" ranks nowhere'
- **Right panel (subtitle bold 13px `#1a5276`, centered at x=555):** "10 requested citations, checked by hand"
  - 5×2 grid of 44px squares (10px gaps, starting x=435, y=80), each labeled `#1`–`#10` in bold 12px white; status order: real, real, mangled, real, invented, mangled, real, invented, mangled, invented. Colors at 0.8 alpha: real green `#008300`, mangled orange `#d95926`, invented red `#e74c3c`.
  - Legend (12×12 swatches, 12px `#222`): "4 real" green, "3 mangled" orange, "3 invented" red.
  - Verdict (bold 13px red `#e74c3c`, centered): "6 of 10 would fail a library lookup"
  - Note (muted `#6b7280`, 12px): "illustrative audit of a niche-topic request"

## Where It Bites Hardest

**Tags:** `where it's used` (blue), `failure mode` (red)

- **Citations & case law** — briefs citing invented court cases have reached judges
- **Numbers** — "23% of users..." arrives sourceless, with perfect confidence
- **Code & APIs** — it suggests a library function that has never existed
- **Medical & legal** — fluent, specific, unverified is the dangerous combination
- **Niche topics** — thinner training data means more gaps filled with pattern

*Example (italic):* The invented API call compiles in your head but not in the terminal.

**Key point:** Risk peaks where the answer is specific, checking is hard, and you want it to be true — verify before reuse.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: risk of an invented detail, by task type (illustrative).

- **Title (bold 15px, `#1a5276`, top center):** "Chance of at Least One Invented Detail, by Task"
- **Layout:** padding top 54 / bottom 44 / left 235 / right 90; vertical `#999` axis line on the left of the bars; 5 rows, bar height 60% of row
- **Data (task → risk %, bar color at 0.75 alpha):**
  - "rewrite text you pasted" → 2%, green `#008300`
  - "summarize a pasted doc" → 5%, aqua `#199e70`
  - "widely-known facts" → 8%, blue `#2a78d6`
  - "niche facts" → 25%, orange `#d95926`
  - "exact citations" → 45%, red `#e74c3c`
- **Scale:** x max 50%; task labels 12px `#222` right-aligned; bold 13px value labels ("2%" … "45%") in bar color to the right of each bar
- **Annotation (bold 13px red `#e74c3c`, two lines inside plot at ~42% width):** "risk rises as answers get" / "specific and hard to check"
- **Caption (muted `#6b7280`, 12px, bottom):** "illustrative rates — the ordering is the point, not the exact numbers"

## The Confusion: Fluency Is Not Accuracy

**Tags:** `common mistake` (red), `bias` (orange)

- **Not lying** — lying needs knowing the truth; the model has no truth flag to hide
- **Confidence is a style** — the assured tone is copied from textbooks, not evidence
- **Smoothness misleads** — readers trust fluent wrong answers over clumsy right ones
- **One voice** — the same confident tone carries its best facts and purest inventions

*Example (italic):* "The study was published in 2019" reads identically whether the study exists or not — the tone carries no signal.

**Key point:** Judge the claim, not the prose — fluent and confident is the model's default voice, right or wrong.

### Visualization (canvas `c4`, 720×300)

Quadrant scatter plot: fluency vs accuracy as independent axes.

- **Title (bold 15px, `#1a5276`, top center):** "Fluency and Accuracy Are Different Axes"
- **Plot area:** padding top 52 / bottom 52 / left 120 / right 40; outer `#999` border; dashed `#ccc` (4/4) midlines splitting into quadrants
- **Danger quadrant shading:** top-left quadrant (low accuracy, high fluency) filled `rgba(231,76,60,0.10)`
- **Axis labels (12px `#444`):** x axis — "wrong" at 25% width, "right" at 75% width, caption "how true the answer is →" centered at bottom; y axis (rotated) — "how smooth it reads →" plus "fluent" (upper) and "clumsy" (lower) tick labels
- **Points (7px radius filled dots; x = accuracy fraction, y = fluency fraction from top):**
  - fluent + right, green `#008300`: (0.82, 0.15), (0.90, 0.28), (0.70, 0.20)
  - fluent + wrong (hallucinations), red `#e74c3c`: (0.15, 0.12), (0.25, 0.22), (0.10, 0.30)
  - clumsy + right, blue `#2a78d6`: (0.80, 0.72)
  - clumsy + wrong, muted `#6b7280`: (0.20, 0.80)
- **Annotations:** bold 13px red `#e74c3c`, two lines in the top-left quadrant: "hallucinations live here:" / "fluent, specific, wrong"; bold 12px green `#008300`, right-aligned near top-right: "what readers assume: fluent = right"

## Regeneration instructions

- **Template:** tutorials topic-page skeleton (social-graph reference style). Structure: `<h1>` (no index number), `.subtitle` paragraph, then four `.card-section` blocks; each has an `<h2>` and a `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) holding `.tags` pills, a `<ul>` of one-line bullets with `<b>` lead terms, an italic `.example` paragraph, and a `.key-point` callout; right `<td class="viz-col">` (50%) holding one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border. Section h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border. `.subtitle` `#666` 0.95rem. Canvas `width:100%`, 1px `#e0e0e0` border, 4px radius. `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem. `ul` 0.92rem; `li b` colored `#1a5276`.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors — blue: bg `rgba(26,82,118,0.12)` text `#1a5276`; green: bg `rgba(39,174,96,0.15)` text `#27ae60`; red: bg `rgba(231,76,60,0.12)` text `#e74c3c`; orange: bg `rgba(230,126,34,0.15)` text `#e67e22`.
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** all four canvases 720×300 logical; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), and calls `ctx.scale` so drawing stays in logical coordinates, and clears. All data arrays hardcoded (no randomness).
- No nav bar, no back/home links. In regenerated HTML any card links would use `.html` extensions (this page has none).
