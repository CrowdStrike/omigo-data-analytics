# Few-Shot Examples

**Page type:** detail page (tutorial layout: `.card-section` blocks, each an h2 + two-column `table.layout`, text left 50%, canvas right 50%)
**HTML title tag:** Few-Shot Examples

**Subtitle:** Paste a few solved examples into the prompt and the model copies the pattern — no training, just showing instead of describing

## Three Labeled Tickets Pasted Into the Prompt

**Tags:** `core idea` (blue), `running example` (green)

- **The task** — label each incoming support ticket as Billing, Bug, or How-to
- **Zero-shot** — the prompt states the three label names and nothing else
- **Few-shot** — the same prompt with three solved tickets pasted above the new one
- **No training** — nothing is learned or saved; the examples ride along in every request
- **Pattern copy** — the model continues the pattern the three examples establish

*Example:* "I was charged twice this month → Billing" is one line, and it teaches label spelling and style at once.

**Key point:** A few-shot prompt is just the zero-shot prompt with worked answers pasted in front — that is the whole trick.

### Visualization (canvas `c1`, 720×300)

Side-by-side prompt comparison diagram, split by a vertical dashed divider at x=360 (`#bdc3c7`, dash 4/3).

- **Title (bold 15px, `#1a5276`, top center):** "The Same Prompt, With and Without Three Worked Examples".
- **Left half — zero-shot:** label "zero-shot" (bold 13px, mute `#6b7280`, centered at x=185, y=48). Box at (45, 58) 280×58, stroke `#6b7280`, fill `#f4f6f8`, containing three 11px monospace lines: "Classify this ticket as" / "Billing, Bug, or How-to:" / "<new ticket>". Below, mute 12px centered two lines: "the model guesses the boundaries" / "and the answer style".
- **Right half — few-shot:** label "few-shot: 3 solved tickets first" (bold 13px, green `#008300`, centered at x=535, y=48). Box at (395, 58) 280×130, stroke blue `#2a78d6`, fill `rgba(42,120,214,0.06)`, containing 11px monospace lines: "Classify as Billing, Bug, How-to." (text color `#2c3e50`); then in magenta `#d55181`: "\"Charged twice this month\"" / "  → Billing"; in orange `#d95926`: "\"Export button does nothing\"" / "  → Bug"; in aqua `#199e70`: "\"How do I add a second user?\"" / "  → How-to"; then in text color: "<new ticket> →". Below the box, green 12px centered: "the model continues the pattern".
- **Bottom annotation (bold 13px, orange `#d95926`, centered):** "three pasted lines — no retraining, no new model".

## Ten Tickets You Can Score by Hand

**Tags:** `worked example` (green)

- **The test set** — 10 held-out tickets: 3 Billing, 4 Bug, 3 How-to, labeled by a human
- **Zero-shot score** — 6 of 10 right: Billing 2/3, Bug 2/4, How-to 2/3
- **Few-shot score** — 9 of 10 right: Billing 3/3, Bug 4/4, How-to 2/3
- **The classic miss** — "My refund never arrived": zero-shot said Bug, few-shot said Billing
- **Same model** — the only change between the two runs was three pasted lines

*Example:* "My refund never arrived" sounds like something broken — the Billing example taught the boundary.

**Key point:** Accuracy went 6/10 to 9/10 without touching the model — three lines of prompt did what retraining would.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: zero-shot vs few-shot correct counts by category.

- **Title (bold 15px, `#1a5276`, centered):** "10 Held-Out Tickets: Correct Labels by Category (illustrative)".
- **Groups (label, zero-shot, few-shot, total):** Billing (3): 2 vs 3; Bug (4): 2 vs 4; How-to (3): 2 vs 2; All (10): 6 vs 9.
- **Geometry:** plot starts x=90, width 540, baseline y=225, chart height 155, y scale max 11; 4 equal group slots, bar width 44, zero-shot bar left of group center, few-shot right.
- **Bars:** zero-shot mute `#6b7280` at alpha 0.6; few-shot green `#008300` at alpha 0.75. Bold 12px value labels above each bar in the bar's color; group label (e.g. "Billing (3)") below baseline in `#2c3e50`.
- **Ceiling ticks:** per group, a dashed (4/3) mute horizontal tick at the group's total count spanning the group width.
- **Legend (top left):** mute swatch + "zero-shot"; green swatch + "few-shot (3 examples)".
- **Annotations (bottom center):** orange `#d95926` bold 13px: "6/10 → 9/10 — the refund ticket moved from Bug to Billing"; mute 12px: "dashes mark each category’s ticket count".

## Examples Teach Format and Judgment

**Tags:** `where it's used` (blue), `rule of thumb` (blue)

- **Format** — zero-shot answered in full sentences 6 of 10 times; few-shot gave a bare label 10 of 10
- **Parser-safe** — a bare label drops straight into a column; a sentence needs cleanup code
- **Judgment** — the refund ticket sits between Billing and Bug; the examples place it
- **Pick edge cases** — examples near the boundaries teach more than easy obvious ones
- **Cover every label** — include at least one example per category or the missing one gets ignored

*Example:* "This sounds like it could be a billing problem" is correct and still breaks the pipeline.

**Key point:** Examples do double duty — they show the exact output shape and where the tricky boundaries lie.

### Visualization (canvas `c3`, 720×300)

Left: two sample replies; right: bar chart of bare-label counts. Vertical dashed divider at x=390 (`#bdc3c7`, dash 4/3).

- **Title (bold 15px, `#1a5276`, centered):** "Same Correct Answer, Two Shapes — and What the Parser Sees".
- **Left — sample outputs:** label "zero-shot reply:" (bold 12px mute); box (45, 64) 310×48, stroke `#6b7280`, fill `#f4f6f8`, italic 11px text: "\"This sounds like it could be a billing" / "problem with the customer’s invoice.\""; below in bold 11px red `#e74c3c`: "✗ correct, but needs cleanup code to parse". Then "few-shot reply:" (bold 12px green `#008300`); box (45, 176) 310×34, stroke `#008300`, fill `rgba(0,131,0,0.05)`, bold 12px monospace: "Billing"; below in bold 11px green: "✓ drops straight into the label column".
- **Right — bar chart:** heading "replies that were a bare label" (bold 12px `#1a5276`, centered over the plot). Two bars: "zero-shot" 4 (mute `#6b7280`) and "few-shot" 10 (green `#008300`), alpha 0.75, bar width 80; baseline y=220, chart height 145, y max 11; bold 14px value labels "4/10" / "10/10" above bars. Dashed mute reference line at 10 labeled "all 10".
- **Bottom annotation (bold 13px orange `#d95926`, centered):** "examples fix the shape and the judgment at once (illustrative counts)".

## Few-Shot Is Not Training

**Tags:** `common mistake` (red), `watch out` (orange)

- **Nothing is saved** — the model forgets the examples the moment the request ends
- **You pay every call** — the examples ride in each request and cost tokens each time
- **Wrong labels teach wrong** — one mislabeled example quietly flips predictions like it
- **Order matters a little** — borderline tickets can drift toward the last example's label
- **When to graduate** — if you want hundreds of examples, that is fine-tuning territory

*Example:* Send the next ticket without the examples and the model is back to zero-shot behavior instantly.

**Common mistake:** Few-shot is teaching per request, not training — the lesson lasts exactly one API call.

### Visualization (canvas `c4`, 720×300)

Flow diagram of two consecutive requests with arrows to result boxes.

- **Title (bold 15px, `#1a5276`, centered):** "Two Consecutive Requests to the Same Model".
- **Request 1:** label "request 1 — examples included" (bold 13px green `#008300`, left at x=60, y=56). Box (60, 64) 250×62, stroke blue `#2a78d6`, fill `rgba(42,120,214,0.06)`, 11px monospace lines: "3 solved tickets" / "+ \"My refund never arrived\"" / "→ ?". Green arrow (310,95)→(400,95) to box (404, 76) 130×38, stroke green, fill `rgba(0,131,0,0.05)`, bold 12px monospace green: "Billing ✓".
- **Request 2:** label "request 2 — examples left out" (bold 13px red `#e74c3c` at x=60, y=162). Box (60, 170) 250×48, stroke `#6b7280`, fill `#f4f6f8`, monospace lines: "\"My refund never arrived\"" / "→ ?". Red arrow (310,194)→(400,194) to box (404, 175) 250×38, stroke red `#e74c3c`, fill `rgba(231,76,60,0.05)`, bold 12px monospace red: "\"Sounds like a bug...\" ✗".
- **Annotations (centered):** mute 12px: "nothing was saved between the calls — the model is unchanged"; orange `#d95926` bold 13px: "the lesson lasts exactly one API call — and you pay its tokens every time".

## Regeneration instructions

- **Layout:** tutorial page — `<h1>` + `.subtitle`, then 4 `.card-section` blocks; each has an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` with one row: left `td.text-col` (50%) holding `.tags` pills, a `<ul>` of bullets each opening with a `<b>` term (`#1a5276`), an italic `.example` paragraph, and a `.key-point` callout; right `td.viz-col` (50%) holding one `<canvas>` 720×300 (CSS `width:100%`, `1px solid #e0e0e0` border, 4px radius).
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; bullets 0.92rem; inline `code` in ui-monospace on `#f4f6f8`. No nav bar, no back/home links.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue = `rgba(26,82,118,0.12)`/`#1a5276`, green = `rgba(39,174,96,0.15)`/`#27ae60`, red = `rgba(231,76,60,0.12)`/`#e74c3c`, orange = `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvas:** each canvas declares intrinsic `width="720" height="300"`; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Shared `box()` (fill + 1.5px stroke rect) and `arrow()` (2px line with filled triangular head) helpers. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange. All numbers are hardcoded literal arrays (no `Math.random()`); invented numbers carry an "illustrative" label.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
