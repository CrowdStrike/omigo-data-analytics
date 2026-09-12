# Knowledge Distillation & Quantization

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Knowledge Distillation & Quantization

**Subtitle:** Two tricks that shrink a huge model into one that fits on a phone — a small student model learns from the big model's soft answers, then its stored numbers are rounded onto a coarser grid

## The Pet-Photo App and Its Giant Brain

**Tags:** `core idea` (blue), `teacher & student` (green), `soft answers` (orange)

- **The app** — a photo app sorts your camera roll into dog, wolf, and cat, using a 500 MB model in the cloud
- **The wish** — the app should work offline on a phone, so the 500 MB brain must shrink to a few MB
- **The teacher** — for a blurry husky photo the big model answers softly: 90% dog, 8% wolf, 2% cat
- **The label** — the training file only says "dog"; it never mentions that huskies half-resemble wolves
- **Distillation** — train the small student to copy the teacher's soft answers, not just the bare labels
- **Extra lessons** — "8% wolf" quietly teaches the student which animals look alike and by how much

*Example (italic):* On the same husky photo, the label says "dog, and nothing else", while the teacher says "90% dog, 8% wolf, 2% cat" — the second answer contains a free lesson about lookalikes.

**Key point:** Distillation trains a small model to imitate a big model's soft answers, which carry more information per photo than the plain label ever did.

### Visualization (canvas `c1`, 720×300)

Two side-by-side bar panels for the same husky photo: the bare label on the left (all-or-nothing bars) and the teacher's soft answer on the right (graded bars), sharing one probability scale.

- **Title (bold 15px, `#1a5276`, top center):** "One Blurry Husky Photo: Bare Label vs Teacher's Soft Answer".
- **Left panel:** origin x=70, baseline y=245, panel width 270, plot height 180; y = probability 0 to 1, tick labels "0", "0.5", "1.0" (12px `#444`), light `#e5e9ef` gridlines at 0.25, 0.5, 0.75; bold 13px `#6b7280` panel label "bare label" centered above at y=70.
- **Left bars (width 56, gap 28):** categories dog, wolf, cat at heights `[1.00, 0.00, 0.00]`; dog bar fill mute `#6b7280`, the two zero bars drawn as 2px flat stubs; 12px `#444` category labels below baseline; bold 12px value labels "1.00", "0", "0" above each bar.
- **Right panel:** origin x=400, baseline y=245, panel width 270, same y scale and gridlines; bold 13px blue `#2a78d6` panel label "teacher's soft answer" centered above at y=70.
- **Right bars (width 56, gap 28):** heights `[0.90, 0.08, 0.02]`; dog bar blue `#2a78d6`, wolf bar orange `#d95926`, cat bar aqua `#199e70`; bold 12px value labels "0.90", "0.08", "0.02" above each bar.
- **Annotation (bold 12px orange `#d95926`, near x=470, y=150, two lines):** "the 8% wolf is a lesson" / "the label never gives".
- **Caption (12px `#444`, bottom right):** "illustrative — one training photo seen two ways".

## Rounding the Weights: 0.42 Becomes 53

**Tags:** `worked example` (blue), `quantization` (green), `8-bit grid` (orange)

- **The weights** — inside the student every learned number is a 32-bit decimal, like 0.42 or -0.87
- **The grid** — 8-bit quantization keeps only 255 allowed steps between -1 and 1: store round(w × 127)
- **By hand** — 0.42 × 127 = 53.34, rounds to the integer 53; reading it back gives 53 / 127 = 0.417
- **Tiny error** — the round trip moved 0.42 to 0.417, an error of 0.003; most weights lose about that much
- **The payoff** — each weight drops from 32 bits to 8, so the whole file instantly becomes 4× smaller
- **Cheap math** — phones add and multiply small integers faster than decimals, so answers speed up too

*Example (italic):* The five weights 0.42, -0.87, 0.05, 0.91, -0.33 become the integers 53, -110, 6, 116, -42, and read back as 0.417, -0.866, 0.047, 0.913, -0.331 — every error is 0.004 or less.

**Key point:** Quantization stores each weight as round(w × 127), an integer between -127 and 127 — a 4× smaller file for round-off errors of only a few thousandths.

### Visualization (canvas `c2`, 720×300)

Snap-to-grid chart: one horizontal weight axis, five float weights drawn as dots on an upper track, arrows dropping each onto its nearest 8-bit grid value on a lower track, with the round-trip error labeled.

- **Title (bold 15px, `#1a5276`, top center):** "Five Weights Snap onto the 8-Bit Grid (× 127, round, ÷ 127)".
- **Axis:** horizontal 2px `#999` line at y=250 from x=60 to x=660 (width 600), weight value -1 to 1; tick labels "-1.0", "-0.5", "0", "0.5", "1.0" (12px `#444`) below; x pixel = 60 + (v + 1) / 2 × 600.
- **Upper track (y=115):** 11px `#6b7280` left label "32-bit float" at x=60, y=95; blue `#2a78d6` 7px dots at weights `[0.42, -0.87, 0.05, 0.91, -0.33]` (x pixels 486, 99, 375, 633, 261); bold 12px blue value label above each dot ("0.42", "-0.87", "0.05", "0.91", "-0.33").
- **Lower track (y=205):** 11px `#6b7280` left label "8-bit integer" at x=60, y=185; green `#008300` 7px dots at read-back values `[0.417, -0.866, 0.047, 0.913, -0.331]`; bold 12px green integer label below each dot ("53", "-110", "6", "116", "-42").
- **Arrows:** 2px `#6b7280` vertical arrow with small arrowhead from each upper dot down to its lower dot.
- **Grid hint:** 12 faint 1px `#e5e9ef` vertical ticks between y=200 and y=210 spaced every 50px along the axis, 11px `#6b7280` note "255 allowed steps (shown coarsely)" at x=60, y=272.
- **Annotation (bold 12px orange `#d95926`, near x=470, y=160, two lines):** "worst round-trip error here: 0.004" / "file shrinks 4×".
- **Caption (12px `#444`, bottom right):** "illustrative — five weights from one layer".

## From 500 MB in the Cloud to 5 MB in a Pocket

**Tags:** `where it's used` (blue), `size vs accuracy` (green)

- **The teacher** — the cloud model: 500 MB, 92% accurate, and a round trip to the server takes about 2 s
- **After distillation** — the student copies the soft answers: 20 MB and 90% accurate, runs on the phone
- **After quantization** — rounding the student's weights to 8 bits: 5 MB, 89.5% accurate, answers in 30 ms
- **The trade** — 100× smaller and dramatically faster, for a total accuracy cost of 2.5 points
- **The pattern** — assistants, keyboards, and camera apps ship distilled-then-quantized copies of big models
- **Order matters** — distill first, then quantize; rounding a well-taught student loses almost nothing

*Example (italic):* The pipeline reads 500 MB at 92% → 20 MB at 90% → 5 MB at 89.5% — the last step costs half a point of accuracy and buys a 4× smaller download.

**Key point:** Distillation then quantization turned a 500 MB, 2-second cloud model into a 5 MB, 30 ms on-phone model while keeping 89.5 of the original 92 accuracy points.

### Visualization (canvas `c3`, 720×300)

Two side-by-side bar panels for the three model stages: file size on the left (log-scale bars) and accuracy on the right (zoomed 85–95% scale), same three stages in both panels.

- **Title (bold 15px, `#1a5276`, top center):** "Shrink Pipeline: 500 MB → 20 MB → 5 MB, Accuracy 92% → 90% → 89.5%".
- **Left panel (file size, log scale):** origin x=70, baseline y=245, panel width 280, plot height 175; y = log10(MB), gridlines `#e5e9ef` with 12px `#444` tick labels "1 MB", "10 MB", "100 MB", "1000 MB"; bold 13px ink `#1a5276` panel label "file size (log scale)" above at y=68.
- **Left bars (width 60, gap 26):** stages teacher, distilled, quantized at sizes `[500, 20, 5]` MB; fills blue `#2a78d6`, aqua `#199e70`, green `#008300`; bold 12px value labels "500 MB", "20 MB", "5 MB" above each bar; 12px `#444` stage labels below baseline ("teacher", "distilled", "+ 8-bit").
- **Right panel (accuracy):** origin x=410, baseline y=245, panel width 280, plot height 175; y = accuracy 85 to 95%, tick labels "85%", "90%", "95%" (12px `#444`), `#e5e9ef` gridlines; bold 13px ink panel label "accuracy" above at y=68.
- **Right bars (width 60, gap 26):** heights `[92, 90, 89.5]`, same three fills as the left panel; bold 12px value labels "92%", "90%", "89.5%" above each bar; same stage labels below.
- **Annotation (bold 12px violet `#4a3aa7`, near x=470, y=105, two lines):** "100× smaller," / "2.5 points cheaper".
- **Caption (12px `#444`, bottom right):** "illustrative — one pet-photo model's shrink pipeline; accuracy axis zoomed".

## Why Not Just Train a Small Model From Scratch?

**Tags:** `common mistake` (red), `soft vs hard labels` (orange)

- **The question** — if a 20 MB model can hit 90%, why bother with the teacher at all?
- **From scratch** — the same 20 MB architecture trained on the bare labels alone tops out near 85%
- **Distilled** — the identical architecture trained on the teacher's soft answers reaches 90%
- **The difference** — 5 points came purely from richer answers like "90% dog, 8% wolf" on every photo
- **Not pruning** — quantization rounds every weight to fewer bits; pruning deletes weights — different trick

*Example (italic):* Two 20 MB students saw the same photos: the one taught by bare labels reached 85%, the one taught by the teacher's soft answers reached 90% — the model size never changed.

**Common mistake:** Assuming a small model trained from scratch matches a distilled one. The size is the same but the lessons are not — soft answers carry the teacher's lookalike knowledge, worth 5 points here.

### Visualization (canvas `c4`, 720×300)

Single-axis arrow chart: one horizontal accuracy scale with three marked models — small-from-scratch, distilled student, and teacher — and a bold arrow showing the jump distillation buys at identical model size.

- **Title (bold 15px, `#1a5276`, top center):** "Same 20 MB Model, Two Teachers: Bare Labels 85% vs Soft Answers 90%".
- **Axis:** horizontal 2px `#999` line at y=210 from x=80 to x=660 (width 580), accuracy 82 to 94%; tick labels "82%", "84%", ..., "94%" every 2 points (12px `#444`) below; x pixel = 80 + (a - 82) / 12 × 580.
- **Scratch marker:** orange `#d95926` 8px dot at 85% (x=225); bold 12px orange label below at y=240: "from scratch — 85%"; 11px `#6b7280` second line "20 MB, bare labels".
- **Distilled marker:** green `#008300` 8px dot at 90% (x=467); bold 12px green label above at y=170: "distilled — 90%"; 11px `#6b7280` second line "20 MB, soft answers".
- **Teacher marker:** blue `#2a78d6` 8px dot at 92% (x=563); bold 12px blue label below at y=240: "teacher — 92%"; 11px `#6b7280` second line "500 MB".
- **Jump arrow:** 4px green `#008300` arced arrow from the scratch dot to the distilled dot peaking at y=115, arrowhead at the distilled end.
- **Annotation (bold 13px green `#008300`, centered above the arc near x=346, y=95):** "+5 points from soft answers alone — same size, better lessons".
- **Caption (12px `#444`, bottom right):** "illustrative — identical architecture, different training targets".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar heights, dot positions, and weight values are the hardcoded arrays above (no randomness); the c2 read-back values are true round(w × 127) / 127 results to 3 decimals; the size/accuracy figures are invented and every canvas carries an "illustrative" caption.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
