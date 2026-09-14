# Always-On Microphones

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Always-On Microphones

**Subtitle:** A wake word means the microphone never stops listening — what gets kept, sent, and reviewed is a separate question from what gets answered

## The Speaker on the Kitchen Counter

**Tags:** `core idea` (blue), `wake word buffer` (orange), `on-device` (green)

- **The counter** — Alice's voice-controlled speaker sits by the kettle and replies only after "hey, assistant"
- **Always processing** — to catch those two words it must push audio through a detector every second of the day
- **The rolling buffer** — only a few seconds of audio exist on the device at once, each second overwriting the last
- **The local test** — a small model on the chip scores that buffer for the wake word, with no network involved
- **Then it leaves** — on a match, the clip from just before the phrase through the request is sent onward
- **Wake word, defined** — a fixed phrase a device detects locally to decide when to start sending audio somewhere
- **Two questions** — "is it listening?" and "what is kept?" have different answers and very different risks

*Example (italic):* Bob says "hey, assistant, timer for ten minutes"; the buffer holds a few seconds, the match fires, and only that clip travels.

**Key point:** Continuous listening is a design requirement of any wake word — it is not evidence that everything is recorded, and the buffer is overwritten in place unless a match fires.

### Visualization (canvas `c1`, 720×300)

Flow diagram: microphone → rolling buffer → wake-word test, with a dashed device boundary; a match crosses it into send/store/review, a non-match loops back to be overwritten.

- **Title (bold 15px, `#1a5276`, top center):** "Where Audio Does and Does Not Leave the Kitchen".
- **Box style:** 8px corner radius, 2px border, centered 12px `#2c3e50` text (11px for the second line where noted).
- **Mic box:** x=40, y=110, 100×58, fill `rgba(25,158,112,0.15)`, border `#199e70`; "microphone" at y=136, "always on" (11px) at y=154.
- **Buffer box:** x=156, y=110, 134×58, fill `rgba(42,120,214,0.15)`, border `#2a78d6`; "rolling buffer" at y=136, "a few seconds" (11px) at y=154.
- **Test box:** x=306, y=108, 104×62, fill `rgba(74,58,167,0.15)`, border `#4a3aa7`; "wake word" at y=134, "match?" at y=152.
- **Chain arrows (3px `#6b7280`, 10px heads):** mic→buffer from (140,139) to (152,139); buffer→test from (290,139) to (302,139).
- **Device boundary:** dashed vertical line at x=430 from y=46 to y=250, 1.5px `#6b7280`, dash [6,5]; 11px `#6b7280` centered label "device boundary" at (430, 40).
- **Right-hand boxes (each 226×50, text centered at box mid-height):** x=456, y=56 fill `rgba(217,89,38,0.15)` border `#d95926` "clip sent to the assistant"; x=456, y=124 fill `rgba(213,81,129,0.15)` border `#d55181` "transcript stored"; x=456, y=192 fill `rgba(201,133,0,0.18)` border `#c98500` "sample reviewed by people".
- **Match arrow:** 3px `#008300` diagonal from (410,130) to (452,84) with head; bold 11px `#008300` label "yes" at (432,118), left-aligned.
- **Vertical arrows (2px `#6b7280`):** (569,106)→(569,120) and (569,174)→(569,188), heads pointing down.
- **No-match return path (2px `#008300`):** polyline (358,170) → (358,205) → (223,205) → (223,172) with an upward head into the buffer box; 12px `#008300` centered label "no match → buffer overwritten" at (290,224).
- **Annotation (bold 13px violet `#4a3aa7`, x=40, y=272, left-aligned):** "listening is continuous; crossing the boundary is not".
- **Caption (12px `#444`, bottom right):** "buffer length illustrative".

## Counting a Day of Captures

**Tags:** `worked example` (blue), `base rate` (orange)

- **Setup** — Alice's household says the wake word 20 times a day; the room produces 2,000 other utterances
- **False-accept rate** — assume 0.5% of those non-wake utterances sound close enough to trip the detector
- **Hand-check** — 2,000 × 0.005 = 10 false triggers a day, each capturing whatever was being said next
- **Confusion matrix** — 20 true accepts, 0 missed, 10 false accepts, 1,990 correctly ignored, 2,020 in total
- **Precision** — 20 ÷ (20 + 10) = 66.7%, so roughly 1 in 3 captured clips was never meant for the device
- **Assumption** — every genuine wake word is detected (recall 100%), which flatters the device, not the risk
- **Over a year** — 10 × 365 = 3,650 unintended clips, alongside 20 × 365 = 7,300 intended ones
- **Same shape as a screening test** — 99.5% specificity still yields mostly false positives when hits are rare

*Example (italic):* Only 0.99% of what the room says is the wake word (20 of 2,020), which is exactly why a 0.5% error on the other 2,000 dominates the captures (all figures illustrative).

**Key point:** Treat the wake word as a binary classifier — with a rare positive class, even a very low false-accept rate makes a third of all captures unintended.

### Visualization (canvas `c2`, 720×300)

Confusion matrix: one day of 2,020 utterances split by what was said and what the device captured, with precision computed at render time.

- **Title (bold 15px, `#1a5276`, top center):** "One Day of Utterances: 30 Captures, Only 20 Intended".
- **Hardcoded counts:** `TP = 20`, `FN = 0`, `FP = 10`, `TN = 1990`.
- **Grid:** origin x=180, y=80, two columns 170 wide, two rows 64 tall (spans x 180–520, y 80–208); 2×2 cells drawn as filled rects with 2px borders.
- **Super-header (12px `#6b7280`, centered at (350,50)):** "what the device did"; column headers bold 12px `#2c3e50` at (265,72) "captured" and (435,72) "not captured".
- **Row labels (12px `#2c3e50`, right-aligned ending at x=172):** "wake word said" at y=116, "no wake word" at y=180.
- **Cells** — value bold 17px centered in the cell's upper half, sub-label 11px centered below it:
  - true accept (row 1, col 1): fill `rgba(0,131,0,0.18)`, border `#008300`, "20" / "true accept".
  - missed (row 1, col 2): fill `rgba(107,114,128,0.10)`, border `#6b7280`, "0" / "missed wake word".
  - false accept (row 2, col 1): fill `rgba(217,89,38,0.20)`, border `#d95926`, "10" / "false accept".
  - ignored (row 2, col 2): fill `rgba(42,120,214,0.12)`, border `#2a78d6`, "1,990" / "correctly ignored".
- **Row totals (12px `#444`, left-aligned at x=530):** "= 20 wake" at y=116, "= 2,000 other" at y=180.
- **Column totals (bold 12px `#1a5276`, centered under the grid at y=228):** "30 captured" at x=265, "1,990 not captured" at x=435.
- **Annotation (bold 13px orange `#d95926`, centered at (350,258)), built at render time** from the counts: `'precision = ' + TP + '/' + (TP+FP) + ' = ' + (100*TP/(TP+FP)).toFixed(1) + '% — 1 in ' + ((TP+FP)/FP).toFixed(0) + ' captures unintended'` → "precision = 20/30 = 66.7% — 1 in 3 captures unintended".
- **Caption (12px `#444`, bottom right):** "counts illustrative".

## What Survives After the Clip Is Sent

**Tags:** `where it's used` (blue), `retention` (orange), `hardware mute` (green)

- **The buffer is the good part** — audio that never matches is overwritten in place and never leaves the room
- **Once sent, it persists** — a matched clip can be stored, transcribed, and held under a retention policy
- **Human review** — samples of clips are listened to by people to grade and improve the detector's accuracy
- **Transcripts are data** — stored text answers subpoenas, discovery, and breaches like any other database table
- **False accepts compound** — 3,650 unintended clips a year is 3,650 chances for a private sentence to be kept
- **Software toggle** — a "stop listening" setting asks the software to stand down; a bug or update can undo it
- **Hardware mute** — a switch that physically cuts the microphone circuit is a stronger control than any setting
- **Where you meet it** — reviewing a voice product's data flow, or answering "what audio do we actually hold?"

*Example (italic):* Ask for the deletion path before the capture path — the kitchen's real risk lives in what was retained, not in what the buffer momentarily heard.

**Key point:** The privacy question is retention, not attention — what is kept, transcribed, and reviewed after a match, and for how long.

### Visualization (canvas `c3`, 720×300)

Proportional stacked bar of a year's captures splitting into intended and unintended, feeding three retention destinations.

- **Title (bold 15px, `#1a5276`, top center):** "A Year of Captures: 10,950 Clips, 3,650 Never Meant for the Device".
- **Hardcoded counts:** `intended = 7300`, `unintended = 3650`, `total = 10950`; segment widths computed as `600 * count / total` → 400px and 200px.
- **Sub-caption (12px `#6b7280`, left-aligned at (60,76)):** "20 intended + 10 false accepts per day × 365 days".
- **Stacked bar:** x=60, y=88, total width 600, height 46; intended segment fill `rgba(42,120,214,0.30)` border 2px `#2a78d6` with bold 13px `#1a5276` centered label "7,300 intended"; unintended segment fill `rgba(217,89,38,0.30)` border 2px `#d95926` with bold 13px `#d95926` centered label "3,650 unintended".
- **Destination boxes (each 190×58, 8px radius, 2px border, two centered lines: 12px then 11px):** x=60 y=170 fill `rgba(213,81,129,0.13)` border `#d55181` "stored as transcripts" / "kept per retention policy"; x=265 y=170 fill `rgba(201,133,0,0.16)` border `#c98500` "sampled for human review" / label built at render time as `'≈' + Math.round(total*0.01) + ' clips at a 1% sample'` → "≈110 clips at a 1% sample"; x=470 y=170 fill `rgba(74,58,167,0.13)` border `#4a3aa7` "legal requests & breaches" / "same as any stored data".
- **Arrows (2px `#6b7280`, heads down):** (155,134)→(155,164), (360,134)→(360,164), (565,134)→(565,164).
- **Annotation (bold 13px green `#008300`, x=60, y=262, left-aligned):** "a hardware mute cuts the mic; a software toggle only asks".
- **Caption (12px `#444`, bottom right):** "counts and sample rate illustrative".

## Listening Is Not the Same as Sending

**Tags:** `common mistake` (red), `honest framing` (orange)

- **Two claims** — "it is always listening" is true by design; "it is always recording and sending" is a different claim
- **Why conflating hurts** — the overstated version is easy to refute, and the refutation buries the real exposure
- **Real exposure one** — false-accept captures: 10 clips a day that nobody in the kitchen addressed to the device
- **Real exposure two** — retention of the clips that were legitimately sent, plus every transcript derived from them
- **Not the exposure** — a permanent secret upload of the room; that is not how wake-word detection is built
- **How to check** — read the documented data flow and retention terms, and test what the mute switch physically cuts
- **Ask for numbers** — false-accept rates are rarely published, so treat any figure you compute as illustrative

*Example (italic):* Alice's 24 hours: the detector runs the whole time, 20 clips leave on purpose, and 10 leave by accident (illustrative).

**Common mistake:** Arguing over whether the device "records everything" — measure the false-accept rate and the retention window instead, because those are the quantities that carry the risk.

### Visualization (canvas `c4`, 720×300)

One-day timeline: a continuous local-listening band with 20 intended capture ticks above it and 10 false-accept ticks below it.

- **Title (bold 15px, `#1a5276`, top center):** "One Day on the Counter: Listening All Day, Sending 30 Slivers".
- **Time scale:** hour `t` maps to `x = 60 + (t/24)*620`, so the day spans x 60–680.
- **Listening band:** rect x=60, y=120, 620×34, fill `rgba(107,114,128,0.12)`, 1.5px `#6b7280` border; bold 12px `#6b7280` left-aligned label "detector runs locally — nothing leaves" at (72,142).
- **Intended ticks (20, blue `#2a78d6`, 3px wide, from y=118 up to y=98):** hours `[7.1, 7.4, 7.8, 8.2, 8.6, 12.1, 12.5, 13.0, 17.2, 17.6, 18.0, 18.3, 18.7, 19.1, 19.4, 19.8, 20.2, 20.6, 21.1, 21.5]`.
- **False-accept ticks (10, orange `#d95926`, 3px wide, from y=156 down to y=176):** hours `[6.9, 9.3, 11.2, 14.5, 15.8, 16.4, 19.6, 20.9, 22.3, 23.1]`.
- **Legends (bold 12px, left-aligned at x=60):** blue `#2a78d6` "20 intended wake-word captures" at y=90; orange `#d95926` "10 false accepts — a private sentence gets sent" at y=196.
- **Axis:** 1.5px `#999` line at y=214 from x=60 to x=680; ticks and 12px `#444` centered labels at y=232 for hours 0, 6, 12, 18, 24 → "12am", "6am", "noon", "6pm", "12am".
- **Annotation (bold 13px violet `#4a3aa7`, x=60, y=262, left-aligned):** "always listening ≠ always sending — the risk is the 10".
- **Caption (12px `#444`, bottom right):** "timing illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Helpers: `roundBoxTL` for rounded boxes, `arrowHead` for arrowheads.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all chart data are hardcoded literal arrays — no `Math.random()` anywhere. The household's 20 wake-word uses/day, 2,000 other utterances/day, 0.5% false-accept rate, the resulting 20/0/10/1,990 confusion matrix, the 365-day scale-up (7,300 / 3,650 / 10,950), and the 1% human-review sample are all invented and labeled illustrative. Every arithmetic claim must reconcile: 2,000 × 0.005 = 10; 20 + 0 + 10 + 1,990 = 2,020; 20 + 10 = 30 captures; 20 ÷ 30 = 66.7% precision (1 in 3 unintended); 1,990 ÷ 2,000 = 99.5% specificity; 20 ÷ 2,020 = 0.99% base rate; 10 × 365 = 3,650; 20 × 365 = 7,300; 30 × 365 = 10,950; 10,950 × 0.01 ≈ 110. Precision, the "1 in 3" figure, the bar-segment widths, and the review-sample count are computed in JS from the hardcoded counts at render time, never hardcoded as strings.
- **Framing:** strictly non-conspiratorial. The page describes documented mechanisms only — continuous local detection on a short overwritten buffer, capture on match, cloud storage/transcription/sampled human review of what was sent, and the difference between a software mute setting and a hardware microphone cut. It must never assert that devices secretly record or upload everything. No real vendors, products, or assistant names; people are Alice and Bob.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
