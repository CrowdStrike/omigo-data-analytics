# Data Augmentation

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Data Augmentation

**Subtitle:** When you can't collect more data, make more: flip, tilt, and brighten the examples you already have, and every transformed copy teaches the model the same truth from a new angle

## Forty Photos of One Dog

**Tags:** `core idea` (blue), `free data` (green), `transformations` (orange)

- **The app** — you want your phone to recognize your dog Max in photos, but you only have 40 pictures
- **Too few** — with 40 shots the model memorizes those exact photos instead of learning what Max looks like
- **The trick** — flip each photo left–right: Max facing the other way is a brand-new picture to the model
- **More knobs** — tilt a little, brighten, darken: each small change makes another photo that is still Max
- **The name** — creating extra training data by transforming what you already have is data augmentation

*Example (italic):* One photo of Max on the couch becomes six: the original, a mirror image, two slight tilts, a brighter one, and a darker one.

**Key point:** Data augmentation means new training examples made by label-preserving transforms of the ones you have — the photo changes, the answer "that's Max" does not.

### Visualization (canvas `c1`, 720×300)

Flow diagram: one original photo box on the left fanning out via arrows to five transform boxes on the right, showing one picture becoming five extra ones.

- **Title (bold 15px, `#1a5276`, top center):** "One Photo In, Five Free Photos Out".
- **Original box:** rounded rectangle x=45, y=115, width 150, height 80; 2px blue `#2a78d6` border, fill `rgba(42,120,214,0.10)`; bold 13px blue text centered inside, two lines: "original photo" / "Max facing left".
- **Five transform boxes (x=440, width 240, height 36, at y = 42, 92, 142, 192, 242):** rounded rectangles, 2px borders in order blue `#2a78d6`, green `#008300`, aqua `#199e70`, yellow `#c98500`, violet `#4a3aa7`, matching fills at 0.10 alpha; 12px `#2c3e50` text centered: "flip left–right (Max faces right)", "tilt 10° clockwise", "tilt 10° counter-clockwise", "10% brighter", "10% darker".
- **Arrows:** five 2px `#6b7280` lines from the original box's right edge (195, 155) to each transform box's left edge midpoint, small solid arrowheads at the ends.
- **Annotation (bold 13px orange `#d95926`, two lines centered near x=300, y=272):** "same dog, same label —" / "five new pictures for free".
- **Caption (11px `#444`, bottom right):** "illustrative — one of the 40 photos".

## From 40 Photos to 240

**Tags:** `worked example` (blue), `count it by hand` (green)

- **Start** — 40 original photos of Max, taken over one weekend
- **Five transforms** — flip, tilt +10°, tilt −10°, 10% brighter, 10% darker, applied to every photo
- **The count** — each original yields itself plus 5 variants: 40 × 6 = 240 training photos
- **Check by hand** — 40 + 40 + 40 + 40 + 40 + 40 = 240; no new photo shoot needed
- **Still one dog** — all 240 carry the same label "Max"; the transforms never change the answer

*Example (italic):* Photo #7 of Max in the yard becomes six rows in the training set: the original plus its mirrored, two tilted, brighter, and darker copies.

**Key point:** 40 originals × (1 original + 5 transforms) = 240 examples — a 6× dataset made with arithmetic, not with more photo shoots.

### Visualization (canvas `c2`, 720×300)

Before/after bar chart: a single short bar for the 40 originals next to a tall stacked bar of six 40-photo slices reaching 240, so the 6× growth is countable by eye.

- **Title (bold 15px, `#1a5276`, top center):** "40 Originals Become 240 Training Photos".
- **Axes:** origin x=70, baseline y=250, plot height 195; y = photo count 0 to 240 with 12px `#444` tick labels "0", "40", "80", "120", "160", "200", "240" and light `#e5e9ef` gridlines at each tick.
- **Before bar:** x=180, width 100, height = 40 photos (≈32px), fill `rgba(42,120,214,0.35)`, 2px blue `#2a78d6` border; bold 13px blue label "40" above the bar, 12px `#444` label "before" below the baseline.
- **After bar (stacked):** x=460, width 100, six 40-photo slices from the baseline up in order original, flipped, tilt +10°, tilt −10°, brighter, darker; fills at 0.30 alpha of blue `#2a78d6`, green `#008300`, aqua `#199e70`, yellow `#c98500`, violet `#4a3aa7`, orange `#d95926`, each slice with a 1px white divider; 11px `#444` slice labels to the right of the bar at each slice's mid-height: "original 40", "flipped 40", "tilt +10° 40", "tilt −10° 40", "brighter 40", "darker 40"; bold 13px green label "240" above the bar, 12px `#444` label "after" below the baseline.
- **Annotation (bold 13px green `#008300`, two lines near x=250, y=95):** "6× the data," / "zero new photos taken".
- **Caption (11px `#444`, bottom right):** "counts from the worked example".

## Why the Model Stops Memorizing

**Tags:** `where it's used` (blue), `overfitting` (red), `generalization` (green)

- **Without it** — trained on the 40 originals, the model scores 99% on those but only 71% on new photos
- **Memorizing** — it learned "couch + this exact lighting + this pose", not what Max actually looks like
- **With it** — trained on the 240 augmented photos, it reaches 86% on new photos it has never seen
- **Why it works** — lighting, tilt, and direction now vary in training, so they stop being reliable shortcuts
- **Everywhere** — audio (speed, pitch shifts) and text (synonym swaps) use the same more-from-less trick

*Example (italic):* Same model, same 40 real photos — augmentation alone lifted new-photo accuracy from 71% to 86%.

**Key point:** Augmentation fights overfitting: by varying what doesn't matter (angle, brightness), it forces the model to learn what does (the dog).

### Visualization (canvas `c3`, 720×300)

Line chart over 10 training epochs: training accuracy without augmentation racing to 99% while new-photo accuracy stalls at 71%, versus the augmented model's new-photo accuracy climbing to 86%.

- **Title (bold 15px, `#1a5276`, top center):** "Training vs New-Photo Accuracy, With and Without Augmentation".
- **Axes:** origin x=65, baseline y=245, plot width 590, plot height 185; x = epoch 1 to 10 with 12px `#444` tick labels "1"–"10"; y = accuracy 50% to 100% with 12px `#444` tick labels "50%", "60%", "70%", "80%", "90%", "100%" and light `#e5e9ef` gridlines.
- **Shared x grid for all three lines:** epochs `[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]`.
- **Training, no augmentation:** mute `#6b7280` 2px dashed (dash 6/4) line, values `[58, 70, 79, 86, 91, 94, 96, 98, 99, 99]`; 12px `#6b7280` label "training (40 originals)" near epoch 4 above the line.
- **New photos, no augmentation:** orange `#d95926` 3px line, values `[56, 63, 67, 69, 70, 71, 71, 70, 71, 71]`; bold 12px orange label "new photos, no aug: 71%" near epoch 8 below the line.
- **New photos, with augmentation:** green `#008300` 3px line, values `[55, 62, 68, 73, 77, 80, 82, 84, 85, 86]`; bold 12px green label "new photos, with aug: 86%" near epoch 8 above the line.
- **Gap marker:** vertical dashed `#6b7280` (dash 4/3) segment at epoch 10 between 71% and 99%, 11px `#6b7280` label to its left: "memorization gap".
- **Annotation (bold 13px green `#008300`, two lines near x=200, y=75):** "same 40 real photos —" / "augmentation adds 15 points".
- **Caption (11px `#444`, bottom right):** "illustrative accuracies".

## Flips That Change the Answer

**Tags:** `common mistake` (red), `label safety` (orange)

- **The rule** — a transform is safe only if a person would still give the changed photo the same label
- **Unsafe** — rotate a house-number photo 180° and every 6 becomes a 9; the label is now a lie
- **Safe** — flip a photo of Max left–right and it is still Max; dogs have no reading direction
- **Task decides** — the flip that is safe for dogs is unsafe for street signs and handwritten digits
- **Training only** — augment the training set alone; the test set stays untouched real photos

*Example (italic):* A team augmented digit photos with 180° rotations and accuracy fell — every rotated 6 was teaching the model that 9s look like 6s.

**Common mistake:** Applying transforms blindly. Ask of every transform: would a person still give the result the same label? If not, you are manufacturing wrong answers.

### Visualization (canvas `c4`, 720×300)

Two-column comparison diagram: the same kind of transform applied to two tasks — rotating a house-number photo destroys the label, flipping a dog photo keeps it — with a verdict under each column.

- **Title (bold 15px, `#1a5276`, top center):** "The Same Trick Can Be Safe or Poisonous".
- **Divider:** vertical 1px `#e5e9ef` line at x=360 from y=40 to y=265.
- **Left column (center x=180), header bold 13px red `#e74c3c` at y=52:** "house number, rotated 180°".
  - Top box: rounded rectangle x=105, y=65, width 150, height 62, 2px `#6b7280` border; bold 30px `#2c3e50` "6" centered with 11px `#444` caption "label: six" below the digit inside the box.
  - Arrow: 2px `#6b7280` vertical arrow from (180, 132) to (180, 162), 11px `#444` label "rotate 180°" to its right.
  - Bottom box: rounded rectangle x=105, y=167, width 150, height 62, 2px red `#e74c3c` border, fill `rgba(231,76,60,0.08)`; bold 30px red "9" centered with 11px red caption "still labeled six" below the digit inside the box.
  - Verdict (bold 13px red, centered at y=258): "unsafe — the label is now wrong".
- **Right column (center x=540), header bold 13px green `#008300` at y=52:** "dog photo, flipped left–right".
  - Top box: rounded rectangle x=465, y=65, width 150, height 62, 2px `#6b7280` border; 13px `#2c3e50` text centered, two lines: "Max facing left" / "label: Max".
  - Arrow: 2px `#6b7280` vertical arrow from (540, 132) to (540, 162), 11px `#444` label "flip" to its right.
  - Bottom box: rounded rectangle x=465, y=167, width 150, height 62, 2px green `#008300` border, fill `rgba(0,131,0,0.08)`; 13px green text centered, two lines: "Max facing right" / "still Max".
  - Verdict (bold 13px green, centered at y=258): "safe — the label survives".
- **Annotation (bold 13px magenta `#d55181`, centered at y=288):** "augment only with changes that keep the label true".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all counts, accuracy series, and box/arrow coordinates are the hardcoded values above (no randomness); the accuracy curves are invented and labeled illustrative; text numbers (40, 240, 6×, 71%, 86%, 99%) must match the chart numbers exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
