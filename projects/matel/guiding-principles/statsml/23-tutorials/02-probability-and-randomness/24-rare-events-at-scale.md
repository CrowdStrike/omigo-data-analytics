# Rare Events at Scale

**Page type:** detail page (tutorial card-sections: h2 with blue underline per section; two-column layout table — text left 50%, canvas right 50%; section 2 stacks two canvases in its viz cell)
**HTML title tag:** Rare Events at Scale

**Subtitle:** A one-in-a-million event stops being rare once you try a hundred million times — "practically impossible" times a huge N equals routine

## One in a Million, a Hundred Times a Day

**Tags:** `core idea` (blue), `counter-intuitive` (orange)

- **The setup** — a bug fires on 1 in every 1,000,000 requests, completely at random
- **The scale** — the service handles 100,000,000 requests every day
- **The count** — expected hits per day = 0.000001 × 100,000,000 = 100
- **Two true statements** — "almost never" per request, "several times an hour" per system
- **The lens** — rarity is a rate; what you actually see is rate × volume

*Example:* The on-call engineer meets the "one-in-a-million" bug about four times an hour.

**Key point:** Never judge how rare something is until you multiply by how many times it gets to try.

### Visualization (canvas `c1`, 720×300)

Split panel: left a schematic dot grid with one hit dot; right a week of daily bug-hit counts as bars.

- **Title (bold 15px, `#1a5276`, top center):** "Rare per Request — Routine per Day".
- **Divider:** dashed (4/3) vertical line `#bdc3c7` at x=320.
- **Left panel (dot grid):** 18 columns × 9 rows of dots starting at (42, 58), step 13px; all dots 2.5px radius `rgba(26,82,118,0.25)` except one hit dot (row 4, col 9) at 4.5px radius red `#e74c3c`. Labels centered at x=165: bold 12px red "the bug: 1 request in 1,000,000" (y=208); 12px mute `#6b7280` "(schematic — every dot stands for" / "thousands of clean requests)" (y=232/248); bold 12px ink `#1a5276` "per request: 0.0001% chance" (y=274).
- **Right panel (bar chart):** sub-title bold 13px ink "Bug hits per day at 100M requests/day"; data days `['Mon','Tue','Wed','Thu','Fri','Sat','Sun']`, hits `[92, 105, 98, 111, 87, 103, 104]`; bars 38px wide filled `rgba(42,120,214,0.55)`, baseline at y=240 (line `#999`), y scale max 130 over 155px height; value labels 12px `#444` above bars, day labels 12px `#222` below.
- **Expected line:** dashed (5/4) orange `#d95926`, width 2, at 100 hits, with bold 13px orange label "expected: 100 hits every day" above it.
- **Caption (12px mute, below day labels):** "illustrative week".

## The Whole Calculation Is One Multiplication

**Tags:** `worked example` (green), `rule of thumb` (blue)

- **Rate × volume** — 0.000001 × 1,000,000 requests = 1 expected hit
- **Scale it** — 10M requests → 10 hits; 100M → 100 hits; 1B → 1,000
- **Hand check** — at 1,000 requests: 0.000001 × 1,000 = 0.001 hits
- **Quiet days** — chance of zero hits in 1M requests is about 37%
- **No quiet days** — at 100M requests, a zero-hit day is about 1 in 10⁴³

*Example:* Multiply 0.999999 by itself a million times: 0.368 — a 37% chance of a quiet day at 1M requests.

**Key point:** Expected count = probability × tries — the same tiny p gives 0.001, 1, or 100 hits depending only on the tries.

### Visualization (canvas `c2a`, 420×300)

Bar chart (log-scale heights): expected hits ladder as traffic scales.

- **Title (bold 15px, `#1a5276`, top center):** "Expected Hits = Rate × Volume"; sub-title 12px mute `#6b7280`: "bug rate fixed at 1 in 1,000,000".
- **Data:** labels `['1M/day', '10M/day', '100M/day', '1B/day']`, hit-count labels `['1', '10', '100', '1,000']`, bar heights proportional to log units `[1, 2, 3, 4]` on a scale max of 4.4.
- **Layout:** padding top 58, bottom 66, left 50, right 20; baseline axis `#999`; bars 64px wide, evenly gapped; the 100M/day bar filled orange `#d95926`, others `rgba(26,82,118,0.35)`; hit-count value labels bold 13px `#222` above bars, traffic labels 12px below.
- **Captions (bottom center):** 12px `#444` "daily traffic (bar height on log scale)"; bold 12px orange "every 10× traffic → 10× hits".

### Visualization (canvas `c2b`, 420×300)

Bar chart: chance of a zero-hit day at each traffic level.

- **Title (bold 15px, `#1a5276`, top center):** "Chance of a Zero-Hit Day"; sub-title 12px mute: "same 1-in-a-million bug".
- **Data:** labels `['100k/day', '1M/day', '10M/day', '100M/day']`, values % `[90.5, 36.8, 0.005, 0]`, displayed value text `['90.5%', '36.8%', '0.005%', '~0%']`; y scale max 100.
- **Layout:** padding top 58, bottom 66, left 50, right 18; baseline axis `#999`; bars 60px wide, evenly gapped (minimum 2px height when value > 0); first two bars filled `rgba(25,158,112,0.55)` with `#444` value labels, last two red `#e74c3c` with red value labels (bold 12px); traffic labels 12px `#222` below.
- **Captions (bottom center):** 12px `#444` "daily traffic"; bold 12px red `#e74c3c` "at 100M: about 1 in 10⁴³ — never".

## Testing Missed It — Production Won't

**Tags:** `where it's used` (blue), `common mistake` (red)

- **The test run** — QA fires 10,000 requests: only a 1% chance the bug shows up at all
- **The launch** — production fires 100M a day: the bug is effectively guaranteed by breakfast
- **Bad rows** — a 0.01% corruption rate in a 1-billion-row table means ~100,000 bad rows
- **Fraud volume** — a 1-in-100,000 fraud rate over 10M daily transactions = 100 cases a day
- **Design shift** — at scale, plan for "when", not "if": monitors, retries, dedup, alerts

*Example:* "We never saw it in testing" and "100 users hit it today" describe the same bug.

**Key point:** At scale, rare failures and outliers are guaranteed — absence in a small sample is silence, not proof.

### Visualization (canvas `c3`, 720×300)

Line chart: P(bug seen at least once) vs total requests on log-spaced x categories, with test-scale and production-scale zones shaded.

- **Title (bold 15px, `#1a5276`, top center):** "Chance the 1-in-a-Million Bug Shows Up at Least Once".
- **Data:** x labels `['10k', '100k', '1M', '10M', '100M']` (evenly spaced = log spacing), probability % `[1.0, 9.5, 63.2, 99.995, 100]`, displayed as `['1%', '9.5%', '63.2%', '99.995%', '~100%']`; y from 0 to 100.
- **Axes:** padding top 52, bottom 56, left 62, right 30; axis lines `#999`; x-axis label "total requests made (log spacing)" (12px `#444`); rotated y-axis label "P(seen at least once), %".
- **Shaded zones:** left zone (up to x-position 0.5) filled `rgba(25,158,112,0.10)` with bold 12px aqua `#199e70` label "test scale"; right zone (from x-position 3.5) filled `rgba(217,89,38,0.10)` with bold 12px orange `#d95926` label "production scale".
- **Series:** connected line in violet `#4a3aa7`, width 3; 4.5px violet dots; probability text labels bold 12px violet above each point (the "1%" label offset right).
- **Annotation (bold 13px red `#e74c3c`, centered at the middle point, y≈38%):** "invisible in a 10k-request test, certain in production".

## "Practically Impossible" Needs an N Attached

**Tags:** `common mistake` (red), `rule of thumb` (blue)

- **Personal odds** — one user making 1,000 requests has a 0.1% chance of ever seeing the bug
- **System odds** — across all users' 100M daily requests, ~100 people hit it every day
- **Both are right** — same probability, different exposure, opposite conclusions
- **Lottery logic** — each ticket is hopeless, yet with millions sold, someone usually wins
- **The flip side** — 100 hits a day is not suspicious here; at this N it is the expected number

*Example:* Support says "no single user will ever see this" while the error log collects 100 reports a day.

**Key point:** Attach the exposure N to every rarity claim — "1 in a million" by itself is unfinished math.

### Visualization (canvas `c4`, 720×300)

Two side-by-side framed panels contrasting one user's exposure with the system's exposure.

- **Title (bold 15px, `#1a5276`, top center):** "Same 1-in-a-Million Bug, Two Exposures, Opposite Conclusions".
- **Left panel (blue):** rectangle at (45, 50), 290×190, border blue `#2a78d6` width 2, fill `rgba(42,120,214,0.07)`. Contents centered at x=190: bold 13px blue "ONE USER"; 12px `#444` "1,000 requests in a lifetime"; bold 26px blue "0.1%"; 12px `#444` "chance of EVER seeing the bug"; a 200×14 outlined meter (blue border) at (90, 180) with only a 2px blue sliver filled; 12px mute `#6b7280` "0.000001 × 1,000 = 0.001 expected hits".
- **Right panel (orange):** rectangle at (385, 50), 290×190, border orange `#d95926` width 2, fill `rgba(217,89,38,0.07)`. Contents centered at x=530: bold 13px orange "THE SYSTEM"; 12px `#444` "100,000,000 requests every day"; bold 26px orange "~100 / day"; 12px `#444` "users hit the bug daily"; a 200×14 meter at (430, 180) fully filled `rgba(217,89,38,0.55)` with orange border; 12px mute "0.000001 × 100,000,000 = 100 expected hits".
- **Caption (bold 13px red `#e74c3c`, bottom center, y=272):** "both statements are true — the probability is the same, only the exposure N changed".

## Regeneration instructions

- **Layout:** tutorial detail page. `<h1>` + `.subtitle`, then 4 `.card-section` blocks, each with an `<h2>` (1.3rem, `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` (width 100%, border-collapse). Every section uses one `<tr>` with left `td.text-col` (50%) and right `td.viz-col` (50%). Sections 1, 3, 4 hold one 720×300 canvas; section 2's viz cell stacks two canvases, `c2a` (420×300) and `c2b` (420×300, `margin-top:12px`).
- **Text cells:** `.tags` pills, then a `<ul>` of bullets, an italic `.example` line, and a `.key-point` callout.
- **Page CSS:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `<ul>` 0.92rem; `li b` colored `#1a5276`; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Tag pills:** `.tag` inline-block 0.72rem weight 600, padding 2px 10px, radius 10px; blue = `rgba(26,82,118,0.12)` bg / `#1a5276` text; green = `rgba(39,174,96,0.15)` / `#27ae60`; red = `rgba(231,76,60,0.12)` / `#e74c3c`; orange = `rgba(230,126,34,0.15)` / `#e67e22`.
- **Callout style:** `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.example` — italic, `#555`, 0.9rem.
- **Canvas:** the shared `setup(id, lw, lh)` helper defaults to 720×300 and accepts overrides (used for c2a/c2b); scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Hardcoded data arrays, no `Math.random()`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
