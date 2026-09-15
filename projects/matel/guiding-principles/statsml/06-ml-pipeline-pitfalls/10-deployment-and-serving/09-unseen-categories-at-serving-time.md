# Pitfall: Unseen Categories at Serving Time

**Page type:** detail page (three card-sections, each a two-column layout table: text left 50%, canvas right 50%)
**HTML title tag:** Unseen Categories at Serving Time

**Subtitle:** A category the encoder never saw in training arrives in production — and the worse outcome is not a crash but a confident score.

## The Problem

**Tags:** `the trap` (red), `encoders` (blue)

- **Vocabulary gap** — a category reaches serving that the fitted encoder vocabulary never contained
- **Loud failure** — the encoder raises, the request errors, someone is paged, and the row is not scored
- **Quiet failure** — the unknown label is mapped to index 0, a real category, and scoring continues
- **Plausible output** — the score looks ordinary, so nothing downstream marks the row as corrupted
- **Hash collision** — a hashing encoder drops the unseen value into an occupied bucket with no error
- **All-zero one-hot** — every indicator column is 0, which the model reads as a valid "none of the above"
- **Compounding** — the bad row feeds dashboards and retraining labels, so one gap becomes many

*Example:* Illustrative Example — of 1,000 serving rows, the rows whose tier is absent from the training vocabulary are all scored as if they were the slot-0 category.

**Impact:** A crash is recoverable because it is visible; a silent misencode ships a confident prediction computed from the wrong category.

### Visualization (canvas `c1`, 720×300)

Lookup-table fall-through diagram: an unseen category misses the vocabulary and lands on slot 0, which is visibly a real category.

- **Title (bold 14px, `#1a5276`, centered):** "Unknown Label Falls Through to Slot 0 — a Real Category".
- **Data (hardcoded literals, shared across `c1` and `c2`):**
  - `VOCAB = ['Tier A', 'Tier B', 'Tier C']` — the encoder vocabulary fitted at training time, indices 0/1/2.
  - `BATCH = [['Tier A',452], ['Tier B',310], ['Tier C',163], ['Tier D',60], ['Tier E',15]]` — one serving batch, counts per category.
- **Computed at render time (never asserted):** `total = 1000`; `unknownRows = 75` (categories not in `VOCAB`); `unknownRate = 75/1000 = 7.5%`; `unseenDistinct = 2` of `5`.
- **Left panel — "Serving batch" (11px `#444` heading at x=30):** the five `BATCH` rows in 11px monospace, count right-aligned; in-vocabulary rows in `#1a5276`, unseen rows (`Tier D`, `Tier E`) in bold `#e74c3c` with a trailing "← unseen" tag. A 10px `#666` total line prints the computed `total`, and a second line prints "unseen categories: `unseenDistinct` of `distinct`".
- **Middle — vocabulary table (150×110 box at (250, 90), white fill, 2px `#1a5276` border):** header bold 11px `#1a5276` "encoder vocabulary"; three 11px monospace rows `0 → Tier A`, `1 → Tier B`, `2 → Tier C`. The slot-0 row is highlighted with a `rgba(230,126,34,0.18)` fill band and its label drawn bold `#e67e22`.
- **Fall-through arrow:** red `#e74c3c` 2px elbow from the `Tier D`/`Tier E` rows, around the bottom of the vocabulary box, into the slot-0 row, filled triangular head; 10px bold red label "not found → .get(x, 0)".
- **Right panel — scored row (170×90 box at (500, 100), white fill, 2px `#e74c3c` border):** bold 11px red "scored as Tier A"; 10px `#2c3e50` lines "index 0 is a real category", "model returns a normal score", "no error, no flag". A red 2px arrow with a filled head runs from the slot-0 row of the vocabulary box into this panel.
- **Bottom annotation (bold 12px red, centered):** computed string "`unknownRate`% of rows (`unknownRows`/`total`) scored as the wrong category — silently." with the values interpolated from the literals.

## Why It Happens

**Tags:** `root cause` (orange), `vocabulary drift` (blue)

- **New enum values** — a new product tier, country, payment method, or vendor code appears upstream
- **Version strings** — app version and build labels gain a fresh distinct value with every release
- **Renames** — a renamed vendor code is a brand-new token even though the entity did not change
- **Casing and whitespace** — "US", "us", "U.S.", or a trailing space each mint their own category
- **Spelling variants** — a typo in a free-text field produces yet another label the encoder never saw
- **Test split shares vocabulary** — a split of the same history cannot hold a genuinely new category
- **Offline metrics stay clean** — validation therefore never exercises the unseen-category path
- **Target encoding has no statistic** — an unseen level has no target mean, so the library's fallback value silently becomes a modelling decision nobody made

*Example:* Illustrative Example — 2 of the 5 tier codes in the batch are unseen, so a rare-looking 40% of distinct categories corresponds to only 7.5% of rows.

**Root Cause:** The encoder vocabulary is frozen at fit time while the upstream category space keeps growing, and the default unknown behaviour is substitution rather than refusal.

### Visualization (canvas `c2`, 720×300)

Side-by-side panels: a loud crash versus a quiet misencode, with the quiet one marked worse. Uses the same `VOCAB`/`BATCH` literals as `c1`.

- **Title (bold 14px, `#1a5276`, centered):** "Two Failure Modes for the Same Unknown Category".
- **Left panel (300×195 at (35, 55), white fill, 3px `#e74c3c` border):** bold 13px red heading "CRASH — loud"; 11px `#2c3e50` lines "encoder raises on unknown label", "request fails, error surfaces", "row is never scored", "on-call is paged the same minute". A bold 11px green verdict line "recoverable: the failure is visible". A 10px `#666` line prints the computed rejected-row count and rate from the literals.
- **Right panel (300×195 at (385, 55), white fill, 3px `#e67e22` border):** bold 13px orange heading "SILENT MISENCODE — quiet"; 11px `#2c3e50` lines "unknown label → index 0", "row scored as a real category", "output is a plausible number", "no error, no alert, no page". A bold 11px red verdict line "worse: the failure is invisible". A 10px `#666` line prints the same computed count and rate as silently mis-scored rows.
- **Divider:** dashed (4,3) 1px `#ccc` vertical line at x=360 from y=55 to y=250.
- **Bottom annotation (bold 12px `#e67e22`, centered):** "A plausible score is more expensive than an exception.".

## The Correct Approach

**Tags:** `the fix` (green), `unknown bucket` (blue)

- **Reserve UNKNOWN** — add an explicit UNKNOWN slot to the vocabulary at training time, not later
- **Fund the bucket** — fold rare training categories into it so the model learns a real response
- **Pin the vocabulary** — version the fitted encoder alongside the model artifact, never refit at serving
- **Assert membership** — check every incoming category against the pinned vocabulary before encoding
- **Count, don't guess** — emit an unknown-rate counter instead of substituting a neighbour silently
- **Normalize first** — lowercase and trim before lookup so casing variants stop minting new labels
- **Alert on the rate** — treat unknown rate as a first-class data-quality metric with a threshold
- **Principled fallback** — prefer encoders whose documented unseen behaviour is a deliberate choice

*Example:* Illustrative Example — rare training tiers are folded into UNKNOWN so the bucket carries genuine training mass, and later unseen tiers route there instead of onto slot 0.

**Fix:** Train a funded UNKNOWN bucket, pin the vocabulary with the model artifact, and alert on the unknown rate instead of substituting silently.

### Visualization (canvas `c3`, 720×300)

Bar chart of the training vocabulary after rare categories are folded into a funded UNKNOWN bucket. This is the page's one conventional chart.

- **Title (bold 14px, `#1a5276`, centered):** "Training Vocabulary With a Funded UNKNOWN Bucket (Illustrative Example)".
- **Data (hardcoded literals):** `TRAIN = [['Tier A',4200], ['Tier B',3100], ['Tier C',1500], ['UNKNOWN',1200]]`; total computed = 10,000.
- **Computed at render time:** each share = count/total × 100 → 42.0%, 31.0%, 15.0%, 12.0% (sums to 100.0%); the printed total is summed from the literals, not typed.
- **Bars:** width 90, baseline y=235, height = count / maxCount × 155 where `maxCount` is computed from `TRAIN`; centred at x = 130, 290, 450, 610. The three real tiers fill `rgba(26,82,118,0.35)` with 2px `#1a5276` border; UNKNOWN fills `rgba(39,174,96,0.25)` with a 2px `#27ae60` border.
- **Labels:** bold 12px value above each bar showing the computed share (e.g. "42.0%") in the bar's stroke colour; 11px `#333` category name below the baseline; 10px `#666` raw count below that, formatted with a thousands separator.
- **Baseline:** 1px `#999` line from x=60 to x=670 at y=235.
- **UNKNOWN annotation (10px `#27ae60`, centred under its bar):** "rare tiers folded in".
- **Bottom annotation (bold 11px `#27ae60`, centered):** computed string "UNKNOWN holds `share`% of `total` training rows — the model learns a real response for it." with the values interpolated.

## Regeneration instructions

- **Layout:** one `.card-section` per h2 ("The Problem", "Why It Happens", "The Correct Approach"); each h2 1.3rem `#1a5276` with a 2px `#2980b9` bottom border, followed by `table.layout` (border-collapse, full width) with a single `<tr>`: `td.text-col` (**50%**) holding `.tags` pills + `<ul>` bullets + `.example` italic paragraph + `.key-point` callout, and `td.viz-col` (**50%**) holding one canvas. Shrink a visual via canvas `max-width`/`max-height`, never by narrowing the column.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `ul` 0.92rem; `li b` in `#1a5276`. No nav bar, no back/home/see-also links — leaf page.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Callouts:** `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem, bold lead-in (`Impact:` / `Root Cause:` / `Fix:`). `.example` italic `#555` 0.9rem.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, 1px `#e0e0e0` border, 4px radius; scaled by `window.devicePixelRatio` through the shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Draw functions register in a `__charts` array and re-run on window resize, debounced 150ms.
- **Data rule:** all chart data is **hardcoded literal arrays** — the vocabulary and the counts are the lesson. No `Math.random()` anywhere. Every printed statistic (unknown rate, unseen-distinct count, shares, totals) is computed in JS from those literals at render time; nothing is typed as a string constant.
- **Palette:** primary blue `#1a5276`, accent `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, grays `#999`/`#666`/`#444`/`#333`.
- In regenerated HTML, any card links use `.html` extensions.
