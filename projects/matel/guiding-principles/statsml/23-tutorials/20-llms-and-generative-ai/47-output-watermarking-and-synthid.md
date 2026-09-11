# Output Watermarking & SynthID

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Output Watermarking &amp; SynthID

**Subtitle:** Nudge the model's word choices in a hidden pattern, and a detector holding the key can later tell the text was machine-written

## A Coin That Secretly Prefers Heads

**Tags:** `core idea` (blue), `watermarking` (green)

- **The choice point** — at every word the model has several good options, often near-ties
- **The secret split** — a key divides the vocabulary into "green" and "red" lists, reshuffled at each position
- **The nudge** — the sampler leans slightly toward green words whenever the choice is close
- **No single tell** — any one green word proves nothing; half of anyone's words are green by chance
- **The signature** — hundreds of words running 70%+ green is a statistical fingerprint

*Example (italic):* "The results were great / strong / solid" — all three fit, so picking the green one costs nothing and quietly signs the sentence.

**Key point:** The watermark lives in which near-tied words got picked — invisible to a reader, countable by a detector with the key.

### Visualization (canvas `c1`, 720×300)

A token strip showing a sentence with green/red word choices, and a running tally beneath.

- **Title (bold 15px, `#1a5276`, top center):** "Each Word Choice Leans Slightly Green".
- **Token strip:** 12 rounded rects 50×30 in a row, tops y=76, left edges x = 34 + i×54; each filled `rgba(0,131,0,0.15)` with 1.5px `#008300` border for green picks, or `rgba(213,81,129,0.12)` with 1.5px `#d55181` border for red picks; pattern (G=green, R=red): `[G,G,R,G,G,G,R,G,G,G,R,G]` (9 green, 3 red); bold 11px centered labels in the border color: "The", "test", "was", "a", "clear", "win", "for", "the", "new", "recipe", "in", "March".
- **Legend (12px, y=136):** green swatch "picked from the green list" at (34,130); magenta swatch "picked from the red list" at (300,130).
- **Tally row:** bold 13px `#008300` centered at (360, 172): "green picks: 9 of 12 (75%) — chance alone would give about 6 (50%)".
- **Bracket note (12px `#6b7280`, centered at (360, 196)):** "the lists reshuffle at every position — only the key knows which words were green where".
- **Annotation (bold 12px orange `#d95926`, centered at y=248):** "no single word is suspicious — the lean across many words is the watermark".
- **Caption (11px `#444`, bottom right, y=290):** "12-token toy sentence, illustrative".

## Count the Green Words Yourself

**Tags:** `worked example` (blue), `AI detection` (orange)

- **The passage** — 20 tokens of suspect text; the detector re-derives each position's green list
- **Chance expects** — with a 50/50 split, about 10 of 20 words should be green
- **Observed** — 15 of 20 are green; that's 5 above the chance expectation
- **How surprising?** — the spread for 20 fair coin flips is about ±2.2, so 15 is over 2σ high
- **The verdict** — likely watermarked; a longer passage would make the call near-certain

*Example (italic):* It's the same math as testing a crooked coin: 15 heads in 20 flips makes you suspicious; 150 in 200 removes all doubt.

**Key point:** Detection is just counting green words against the coin-flip expectation — redo the 15-vs-10 comparison and you've run the detector.

### Visualization (canvas `c2`, 720×300)

Two distribution curves of green-word counts for 20 tokens — chance vs watermarked — with the observed count marked.

- **Title (bold 15px, `#1a5276`, top center):** "20 Tokens: Where Does 15 Green Fall?".
- **Axes:** baseline y=230, x-axis = green count 0–20 mapped from x=70 to x=660 (29.5px per count); 12px `#444` tick labels under the baseline at counts 0, 5, 10, 15, 20 (y=248); axis line 1px `#999` along the baseline.
- **Chance curve (gray `#6b7280`, 2.5px, filled `rgba(107,114,128,0.10)`):** bell centered at 10 with sd 2.2, heights h(k) = 150·exp(−(k−10)²/(2·2.2²)) px above the baseline, drawn as a smooth polyline over k = 3..17 (0.25 steps).
- **Watermarked curve (green `#008300`, 2.5px, filled `rgba(0,131,0,0.10)`):** bell centered at 15 with sd 1.9, heights h(k) = 150·exp(−(k−15)²/(2·1.9²)), drawn over k = 8..20.
- **Curve labels (bold 12px, at the peaks):** "chance (no watermark)" in `#6b7280` near (365, 66); "watermarked" in `#008300` near (513, 66) — offset so they don't collide.
- **Observed marker:** dashed 2px `#d95926` vertical line at count 15 from y=230 up to y=78; bold 12px `#d95926` label "observed: 15 of 20" beside it at (513, 92) or nearest clear spot.
- **Annotation (bold 12px orange `#d95926`, centered at y=268):** "15 sits in the chance curve's far tail but dead center for the watermark".
- **Caption (11px `#444`, bottom right, y=292):** "curves illustrative; real detectors use longer texts".

## The Real Thing: SynthID

**Tags:** `where it's used` (blue), `SynthID` (green)

- **In production** — Google DeepMind's SynthID-Text watermarks text this way; published in Nature (2024) and open-sourced
- **Needs the key** — detection requires the same secret split, so each provider detects only its own marks
- **Not universal** — Vendor A's detector reads nothing in Vendor B's text; there is no all-AI detector
- **Vs style guessers** — post-hoc "AI detectors" guess from writing style and misfire on human text
- **Images too** — SynthID also marks images at the pixel level; same brand, different scheme

*Example (italic):* A style-based detector once flagged plainly human-written essays as machine text — a watermark check with the key has no such ambiguity to argue about.

**Key point:** Key-based watermark detection is a count with known statistics; style-based detection is a guess — the two get confused constantly.

### Visualization (canvas `c3`, 720×300)

Two-panel contrast: key-based watermark detection vs style-based guessing.

- **Title (bold 15px, `#1a5276`, top center):** "Two Very Different 'AI Detectors'".
- **Left panel:** rounded rect x=30, y=52, 320×192, fill `rgba(0,131,0,0.05)`, 2px `#008300` border; bold 13px `#008300` header "watermark detector (with key)" centered at (190, 74).
  - 12px `#2c3e50` centered lines at y=100/120/140: "re-derives each green list", "counts green words: 75% vs 50%", "answer comes with odds attached".
  - Bold 12px `#008300` centered at (190, 172): "works only on text from its own model".
  - 11px `#6b7280` centered at (190, 194): "misses text that was heavily rewritten".
- **Right panel:** rounded rect x=380, y=52, 310×192, fill `rgba(213,81,129,0.05)`, 2px `#d55181` border; bold 13px `#d55181` header "style guesser (no key)" centered at (535, 74).
  - 12px `#2c3e50` centered lines at y=100/120/140: "reads tone, word variety, rhythm", "no ground truth to count against", "verdict is a hunch, not a measurement".
  - Bold 12px `#d55181` centered at (535, 172): "flags some human writers as machines".
  - 11px `#6b7280` centered at (535, 194): "documented false-positive problem".
- **Annotation (bold 12px orange `#d95926`, centered at y=270):** "same label, different machines — ask 'is there a key?' before trusting a verdict".
- **Caption (11px `#444`, bottom right, y=292):** "summaries simplified".

## What a Watermark Can't Survive

**Tags:** `common mistake` (red), `limits` (orange)

- **Not a metadata tag** — the mark lives in the word choices themselves, not in a hidden header
- **Paraphrasing washes it** — rewrite the sentences and the green-word lean fades away
- **Translation resets it** — new words in a new language carry none of the original lean
- **Short text is mute** — a one-line answer has too few coin flips to call either way
- **Per-provider only** — no key, no detection; unmarked models produce unmarked text

*Example (italic):* Copy a watermarked paragraph through one round of "rewrite this in your own words" and the detector's count drops back toward chance.

**Common mistake:** Treating watermarking as proof-grade provenance for all AI text — it is strong evidence only for unedited text, from a provider that marks, checked with that provider's key.

### Visualization (canvas `c4`, 720×300)

Bar chart: detector confidence on the same watermarked passage as it gets shorter or more edited.

- **Title (bold 15px, `#1a5276`, top center):** "The Same Watermark After Each Change (illustrative)".
- **Axes:** baseline y=230, plot top y=64; y = green-word rate 40–80% mapped 4.15px per point (label ticks 40/50/60/70/80 with `#e5e9ef` gridlines and 12px `#444` right-aligned labels at x=64); dashed 1.5px `#6b7280` horizontal reference line at 50% labeled "chance" in 11px `#6b7280` at its right end (x=655, above the line).
- **Bars (80px wide, centered x = `[150, 280, 410, 540, 655−40→see below]`):** use centers `[150, 280, 410, 540, 645]` with widths 80 (last bar may be 70 to fit):
  - "original, 300 words" 75% green, fill `#008300`
  - "trimmed to 40 words" 73% (still leaning, too few words), fill `#199e70`
  - "lightly edited" 68%, fill `#c98500`
  - "paraphrased" 55%, fill `#d95926`
  - "translated" 50%, fill `#d55181`
  - bold 12px value labels above each bar in the bar's color: "75%", "73%", "68%", "55%", "50%".
- **X labels (11px `#444`, centered under each bar at y=248, two lines where needed):** "original / 300 words", "trimmed / to 40 words", "lightly / edited", "para- / phrased", "trans- / lated".
- **Verdict row (bold 11px, centered at y=282, same x centers):** "clear call" `#008300`, "weak call" `#199e70`, "weak call" `#c98500`, "no call" `#d95926`, "no call" `#d55181`.
- **Annotation (bold 12px orange `#d95926`, centered at (365, 84)):** "editing pulls the count back toward the 50% chance line".
- **Caption (11px `#444`, bottom right, y=297):** "rates illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** the c1 strip must show exactly 9 green of 12 (75%); the c2 example must read observed 15 vs chance 10 of 20 to match the text; c4 bars read 75/73/68/55/50; all values hardcoded, no randomness; bell curves computed from the fixed formulas above (deterministic).
- **Facts discipline:** SynthID claims limited to documented public facts (Google DeepMind, Nature 2024 publication, open-sourced, also used for images); hypothetical providers named "Vendor A/B".
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
