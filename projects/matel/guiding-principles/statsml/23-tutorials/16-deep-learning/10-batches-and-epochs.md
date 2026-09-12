# Batches & Epochs

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Batches & Epochs

**Subtitle:** Training data is served in small plates: 10,000 photos in batches of 32 means 313 nudge steps, and one full pass through everything is an epoch

## Serving 10,000 Photos in Plates of 32

**Tags:** `core idea` (blue), `running example` (green)

- **The setup** — a photo classifier is learning from a folder of 10,000 labelled photos
- **A batch** — instead of all 10,000 at once, the model looks at just 32 photos at a time
- **A step** — after each plate of 32, it measures its misses and nudges its weights once
- **An epoch** — once every photo has been seen exactly once, one epoch is complete
- **Then repeat** — shuffle the 10,000 photos and go around again for epoch two, three, ...

*Example:* Like reading a 10,000-page book in 32-page sittings: each sitting is a step, finishing the book is an epoch.

**Key point:** Batch = one small plate of examples, step = one nudge after a plate, epoch = one full trip through the whole dataset.

### Visualization (canvas `c1`, 720×300)

Flow diagram: dataset box → batch boxes → nudge arrows, with an epoch bracket.

- **Title (bold 15px `#1a5276`, top center):** "One Epoch = the Whole Folder, Served 32 Photos at a Time".
- **Dataset box:** rectangle at (40,90) 150×110, fill `rgba(42,120,214,0.08)`, stroke blue `#2a78d6` 2px; bold blue centered text "10,000 photos" and smaller "(shuffled)".
- **Arrow:** gray (`#6b7280`) arrow from dataset box to the batch row.
- **Batch boxes (five slots, 74px wide, 12px gaps, starting x=255, y=110, 70px tall):** "batch 1" / "32 photos", "batch 2" / "32 photos", "batch 3" / "32 photos", a ". . ." gap in gray, "batch 313" / "16 photos". First three: fill `rgba(25,158,112,0.10)`, stroke aqua `#199e70`; last: fill `rgba(217,89,38,0.10)`, stroke orange `#d95926`.
- **Nudge arrows:** a magenta (`#d55181`) downward arrow under each batch box; bold magenta caption centered below: "after every batch: one nudge of the weights = one step".
- **Epoch bracket:** navy (`#1a5276`) bracket spanning all five batch slots above them, labeled bold "all 313 batches together = 1 epoch".
- **Bottom annotation (bold 13px orange, centered):** "last plate is smaller (16 photos) — it still counts as a step".

## Counting the Steps: 313 Plates Make One Epoch

**Tags:** `worked example` (green), `rule of thumb` (blue)

- **The division** — 10,000 photos ÷ 32 per batch = 312.5, and half a batch still counts
- **Full plates** — 312 full batches use 312 × 32 = 9,984 photos
- **The leftover plate** — the last 16 photos form a smaller batch number 313
- **So one epoch** — 313 steps, meaning the weights get nudged 313 times per epoch
- **Ten epochs** — 10 × 313 = 3,130 steps, and every photo was seen exactly 10 times

*Example:* If you doubled the batch to 64, one epoch would be 157 steps: fewer but bigger plates, same 10,000 photos.

**Key point:** Steps per epoch = dataset size ÷ batch size, rounded up — here ceil(10,000 / 32) = 313.

### Visualization (canvas `c2`, 720×300)

Segmented horizontal bar of the 10,000 photos plus an arithmetic block.

- **Title (bold 15px `#1a5276`, top center):** "10,000 Photos ÷ 32 per Batch → 313 Steps".
- **Bar:** at (60,70), 600×46; left portion 9,984/10,000 of the width filled `rgba(25,158,112,0.25)` (full batches), remainder filled `rgba(217,89,38,0.45)` (leftover); whole bar outlined navy `#1a5276` 1.5px; 39 faint vertical tick lines (`rgba(26,82,118,0.35)`) across the green portion suggesting 32-photo slices.
- **Bar labels:** bold aqua `#199e70` centered under the green portion: "312 full batches × 32 photos = 9,984 photos"; bold orange right-aligned: "+ 1 batch of 16".
- **Arithmetic block:** box at (140,170) 440×100, fill `#f8f9fa`, stroke navy 1.5px; text lines in `#2c3e50` 14px: "steps per epoch  =  ceil( 10,000 / 32 )  =  313" and "10 epochs  =  10 × 313  =  3,130 steps"; bold magenta `#d55181` line: "every photo seen exactly 10 times".

## Why 32 — Not All 10,000, and Not One at a Time

**Tags:** `where it's used` (blue), `trade-off` (orange)

- **All 10,000 at once** — may not fit in memory, and gives only 1 nudge per epoch: slow learning
- **One at a time** — 10,000 nudges per epoch, but each is based on a single photo: very noisy
- **Batch of 32** — fits in memory, and averaging 32 misses makes each nudge steadier
- **The sweet spot** — common batch sizes are 32 to 256; smaller = noisier, bigger = more memory
- **Every framework asks** — batch_size and epochs are the first two knobs in any training call

*Example:* Asking one customer vs averaging 32: the single opinion swings wildly, the average of 32 points the right way.

**Key point:** Batching is a compromise — big enough to average out noise, small enough to fit in memory and nudge often.

### Visualization (canvas `c3`, 720×300)

Line chart: loss vs training time for three batch sizes.

- **Title (bold 15px `#1a5276`, top center):** "Same Training Time, Three Batch Sizes".
- **Axes:** L-shaped gray `#999`; padding top 56, bottom 56, left 62, right 190; 25 x-samples; y scale 0 to 2.6 (loss).
- **Batch = 1 curve (orange `#d95926`, width 2):** noisy exponential decay — base `2.3·exp(−i/6) + 0.45` plus deterministic jitter `0.38·sin(i·2.7)·exp(−i/30)`, clamped at 2.6.
- **Batch = 32 curve (green `#008300`, width 3):** smooth decay `2.3·exp(−i/7) + 0.35`.
- **Batch = 10,000 curve (violet `#4a3aa7`, width 2.5):** staircase values `[2.3×4, 2.05×4, 1.82×4, 1.62×4, 1.45×4, 1.30×4, 1.18]` (each level held 4 samples, 25 points total).
- **In-chart labels (bold 13px):** green "batch 32: steady and fast"; orange "batch 1: jumpy"; violet "batch 10,000: rare nudges".
- **Legend (right side, 12px swatches):** orange "batch = 1 (noisy)", green "batch = 32 (sweet spot)", violet "batch = 10,000 (slow)".
- **Axis captions (`#444` 12px):** x "training time →  (curves illustrative)"; rotated y "loss (how badly it misses)".
- **Takeaway (bold 13px magenta `#d55181`, bottom center):** "32 averages away the noise and still nudges 313 times per epoch".

## The Loss Curve: One Dot per Epoch, 313 Nudges Behind Each Dot

**Tags:** `common mistake` (red), `core idea` (blue)

- **The loss** — one number for "how badly the model missed"; smaller is better
- **Our curve** — loss drops from 2.30 after epoch 1 to 0.41 after epoch 10
- **Each dot hides work** — between two dots the model took 313 steps and nudges
- **Steps vs epochs mixup** — "trained for 3,130 steps" and "10 epochs" describe the same run here
- **Flattening is normal** — big drops early, tiny gains later; more epochs is not always better

*Example:* A colleague brags "50 epochs!" on 100 photos — that is only 200 steps at batch 32, far less work than it sounds.

**Key point:** When reading a loss curve, always check what the x-axis counts — epochs and steps differ by a factor of 313 here.

### Visualization (canvas `c4`, 720×300)

Line chart: loss per epoch with dot value labels and a bracket annotation.

- **Title (bold 15px `#1a5276`, top center):** "Loss per Epoch: 2.30 Down to 0.41 in Ten Epochs".
- **Data:** loss by epoch 1–10: `[2.30, 1.45, 0.98, 0.74, 0.60, 0.52, 0.47, 0.44, 0.42, 0.41]`; y scale 0–2.6; padding top 56, bottom 62, left 62, right 40; points centered per slot; gray L-shaped axes.
- **Series:** blue `#2a78d6` line width 3 with radius-5 blue dots; bold `#222` value labels above each dot; gray epoch numbers 1–10 below the axis.
- **Bracket annotation:** orange (`#d95926`, 2px) bracket between the epoch-1 and epoch-2 dots, with bold orange 13px text "313 steps of batch-32 nudging between these two dots".
- **Annotation (bold 13px green `#008300`, right-aligned near the tail):** "big gains early, tiny gains late".
- **Axis captions (`#444` 12px):** x "epoch (full passes through all 10,000 photos)"; rotated y "loss after the epoch".
- **Takeaway (bold 13px magenta `#d55181`, bottom center):** "10 epochs = 3,130 steps — one dot per epoch hides 313 nudges each".

## Regeneration instructions

- **Layout:** tutorial detail page. `<h1>` (no index number), `.subtitle` paragraph, then four `.card-section` blocks; each has an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` (full width, border-collapse) with one `<tr>`: left `td.text-col` (50%) and right `td.viz-col` (50%), both 12px padding, top-aligned.
- **Left column structure:** `.tags` row of colored pill spans (0.72rem, 600 weight, 2px 10px padding, 10px radius — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); then a `<ul>` (0.92rem) of bullets each opening with `<b>` term in `#1a5276`; one italic `.example` paragraph (`#555`, 0.9rem); one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, 8px 12px padding, 0.9rem) starting with `<strong>Key point:</strong>`.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 0.95rem. No nav bar, no back/home links.
- **Canvas:** each canvas declared `width="720" height="300"`, CSS `width:100%`, 1px `#e0e0e0` border, 4px radius; a shared `setup(id)` helper scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Shared `arrow()` helper with 9px arrowheads. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** JS object `P = { blue:#2a78d6, green:#008300, magenta:#d55181, yellow:#c98500, aqua:#199e70, orange:#d95926, violet:#4a3aa7, ink:#1a5276, text:#2c3e50, mute:#6b7280, grid:#e5e9ef }`. Doc palette anchors: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. All chart data hardcoded (no `Math.random()`; jitter is deterministic `Math.sin`).
- In regenerated HTML, any card links use `.html` extensions (this page has no outgoing links).
