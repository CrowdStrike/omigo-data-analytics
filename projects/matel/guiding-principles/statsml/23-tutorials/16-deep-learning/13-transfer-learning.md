# Transfer Learning

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Transfer Learning

**Subtitle:** Start from a network that already learned to see on millions of photos, then retune just its last piece for your own small problem

## Borrowing Eyes Trained on a Million Photos

**Tags:** `core idea` (blue), `running example` (green), `small data` (orange)

- **The problem** — an animal shelter wants to sort photos into dog, cat, or rabbit
- **The catch** — the shelter has only 900 labeled photos; deep nets want millions
- **The shortcut** — download a network already trained on millions of everyday photos
- **What it knows** — its early layers already detect edges, fur texture, ears, eyes
- **The swap** — keep those layers frozen, replace only the final layer with your own

*Example:* Hiring a trained photographer and teaching them your three animals beats raising one from birth.

**Key point:** Transfer learning reuses what a big pretrained network already learned, so your small dataset only has to teach the last step.

### Visualization (canvas `c1`, 720×300)

Layer-stack diagram: frozen pretrained layers plus a new head.

- **Title (bold 15px `#1a5276`, top center):** "Keep the Trained Layers Frozen, Swap Only the Last One".
- **Frozen stack:** five boxes (220×28, 8px gaps) starting at (250,52), fill `rgba(42,120,214,0.18)`, stroke blue `#2a78d6` 2px, bold navy centered labels top-to-bottom: "edges", "textures", "fur / ears", "eyes / faces", "photo summary".
- **New head:** sixth box below, fill `rgba(217,89,38,0.15)`, stroke orange `#d95926` 2.5px, bold orange label "NEW: dog / cat / rabbit".
- **Frozen bracket (left of stack, blue):** spanning the five frozen boxes, with right-aligned blue text "FROZEN" (bold) and "learned from" / "millions of photos" (12px).
- **Trained label (right of head box, bold orange 12px, two lines):** "TRAINED on your" / "900 shelter photos".
- **Input arrow:** short gray (`#6b7280`) line entering the top of the stack.
- **Annotation (bold magenta `#d55181` 13px, right of stack, two lines):** "the hard part — learning to see —" / "is already done for you".

## Counting the Weights: 1,539 Instead of 11 Million

**Tags:** `worked example` (green), `by hand` (blue)

- **The setup** — the pretrained net ends in 512 numbers describing each photo
- **New head** — one fresh layer maps those 512 numbers to 3 animal scores
- **Weight count** — 512 × 3 = 1,536 weights, plus 3 biases = 1,539 to train
- **Frozen part** — the other ~11,000,000 pretrained weights are left untouched
- **Data split** — 900 shelter photos become 720 for training, 180 for testing
- **The ratio** — 720 photos teaching 1,539 weights is fine; teaching 11M is hopeless

*Example:* Check it yourself: 512 × 3 + 3 = 1,539 — about two weights per training photo, still tiny next to 11 million.

**Key point:** Freezing shrinks the learning job from 11 million weights to 1,539 — small enough for 720 photos to pin down.

### Visualization (canvas `c2`, 720×300)

Two-panel chart split by a dashed divider at x=430: weight-count bars left, arithmetic table right.

- **Title (bold 15px `#1a5276`, top center):** "Weights That 720 Training Photos Must Teach".
- **Divider:** vertical dashed `#bdc3c7` line (dash 4/3) at x=430.
- **Left panel (bars 40px tall from x=70):** "from scratch: ~11,000,000 weights" bar 310px wide, fill `rgba(42,120,214,0.35)`, stroke blue `#2a78d6`, bold navy label inside; "new head only: 1,539 weights" bar just 6px wide, fill `rgba(0,131,0,0.35)`, stroke green `#008300`, bold green label beside it. Bold magenta 13px centered caption "~7,000x less to learn"; gray 12px "512 features × 3 classes + 3 biases = 1,539".
- **Right panel ("the head, by hand" table, header bold navy 13px, centered at x=575):** rows label/value — "photo summary in" / "512 numbers"; "classes out" / "3 scores"; "weights" / "512 × 3 = 1,536"; "biases" / "+ 3"; "total trained" / "1,539" (final value bold green); labels gray right-aligned, values `#2c3e50` left-aligned; light gray (`#e5e9ef`) rule under the table.
- **Bottom annotation (bold orange 12px, centered):** "900 photos = 720 train / 180 test".

## Why 900 Photos Is Never Enough on Its Own

**Tags:** `where it's used` (blue), `best practice` (green)

- **From scratch** — trained on 900 photos alone, the net memorizes quirks: 62% on new photos
- **With transfer** — same 900 photos on the pretrained base reach 91% (illustrative)
- **The gap shrinks** — only past ~10,000 photos does from-scratch start catching up
- **Everyday reality** — most real projects have hundreds of labels, not millions
- **Everywhere now** — medical scans, defect photos, chatbots all start from pretrained models

*Example:* A radiology team with 2,000 X-rays fine-tunes a photo model — nobody trains from zero anymore.

**Key point:** A data scientist rarely gets big data — transfer learning is how deep learning works at real-world dataset sizes.

### Visualization (canvas `c3`, 720×300)

Two-line chart: accuracy vs training-set size, transfer vs from scratch.

- **Title (bold 15px `#1a5276`, top center):** "Accuracy on New Photos vs Training-Set Size (illustrative)".
- **Data:** x categories `100, 300, 900, 3,000, 10,000` labeled photos; from-scratch accuracies `[35, 48, 62, 78, 88]` (blue `#2a78d6`); transfer accuracies `[76, 85, 91, 93, 94]` (green `#008300`); both lines width 3 with radius-4 dots.
- **Axes:** y range 20–100 with light gray (`#e5e9ef`) gridlines and gray `#6b7280` labels at 40/60/80/100%; padding top 56, bottom 60, left 62, right 175; gray L-shaped axes; `#2c3e50` size labels below.
- **Annotation at 900:** vertical dashed magenta (`#d55181`, dash 4/3, 1.5px) connector between the two curves at x=900, with bold magenta 13px label "at 900 photos: 62% vs 91%".
- **Legend (right side, 12px swatches):** green "transfer learning", blue "from scratch".
- **X-axis caption (`#444` 12px, centered):** "labeled training photos".

## The Confusion: Fine-Tuning Is Not Retraining Everything

**Tags:** `common mistake` (red), `trade-off` (orange)

- **The mistake** — unfreezing all layers and training hard, as if starting fresh
- **What breaks** — big updates scramble the pretrained layers; the borrowed skill is erased
- **Our numbers** — head-only 91%; gentle full fine-tune 93%; aggressive full retrain 58%
- **Safe recipe** — train the new head first, then optionally unfreeze with a tiny learning rate
- **Related domains** — photo models transfer to photos; they lend little to audio or tables

*Example:* Retraining everything on 900 photos is like repainting a finished portrait with a house brush.

**Key point:** The pretrained weights are the asset — touch them gently or not at all, or you erase exactly what you borrowed.

### Visualization (canvas `c4`, 720×300)

Three-bar column chart: fine-tuning strategies compared.

- **Title (bold 15px `#1a5276`, top center):** "Same 900 Photos, Three Fine-Tuning Strategies (illustrative)".
- **Bars (110px wide, 0.65 alpha fill plus 2px stroke in the same color; two-line labels below):** "head only," / "base frozen" 91% green `#008300`; "full net," / "tiny learning rate" 93% aqua `#199e70`; "full net," / "big learning rate" 58% orange `#d95926`. Y range 40–100; padding top 58, bottom 74, left 62, right 40; gray L-shaped axes; bold colored 14px percentage labels above bars.
- **Annotation over the third bar (bold red `#e74c3c` 13px, two lines):** "pretrained skill erased" / "— worse than head-only".
- **Caption (gray `#6b7280` 12px, bottom center):** "accuracy on 180 held-out shelter photos".

## Regeneration instructions

- **Layout:** tutorial detail page. `<h1>` (no index number), `.subtitle` paragraph, then four `.card-section` blocks; each has an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` (full width, border-collapse) with one `<tr>`: left `td.text-col` (50%) and right `td.viz-col` (50%), both 12px padding, top-aligned.
- **Left column structure:** `.tags` row of colored pill spans (0.72rem, 600 weight, 2px 10px padding, 10px radius — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); then a `<ul>` (0.92rem) of bullets each opening with `<b>` term in `#1a5276`; one italic `.example` paragraph (`#555`, 0.9rem); one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, 8px 12px padding, 0.9rem) starting with `<strong>Key point:</strong>`.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 0.95rem. No nav bar, no back/home links.
- **Canvas:** each canvas declared `width="720" height="300"`, CSS `width:100%`, 1px `#e0e0e0` border, 4px radius; a shared `setup(id)` helper scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** JS object `P = { blue:#2a78d6, green:#008300, magenta:#d55181, yellow:#c98500, aqua:#199e70, orange:#d95926, violet:#4a3aa7, ink:#1a5276, text:#2c3e50, mute:#6b7280, grid:#e5e9ef }`. Doc palette anchors: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. All chart data hardcoded (no `Math.random()`).
- In regenerated HTML, any card links use `.html` extensions (this page has no outgoing links).
