# Argmax vs Softmax

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Argmax vs Softmax

**Subtitle:** The same list of scores can answer two ways — argmax gives one hard choice, softmax gives soft shares that sum to 1, and one-hot is argmax run backwards

## One Blurry Photo, Two Ways to Answer

**Tags:** `core idea` (blue), `hard vs soft` (green), `running example` (orange)

- **The app** — a pet app scores one blurry photo: cat 2.0, dog 1.0, fox 0.5, rabbit −0.5
- **Argmax** — point at the biggest score and say only its name: "cat" — one hard choice
- **Softmax** — reshape the same scores into shares: cat 0.60, dog 0.22, fox 0.13, rabbit 0.05
- **Same winner** — softmax keeps the ranking of the scores, so its top share is argmax's pick
- **The trade** — argmax gives an answer you can act on; softmax keeps how sure the app is

*Example (italic):* Shown the same blurry photo, argmax answers "cat" while softmax answers "0.60 cat, 0.22 dog".

**Key point:** Argmax collapses a score list into one hard label; softmax reshapes it into probabilities that sum to 1. Same input, two kinds of answer.

### Visualization (canvas `c1`, 720×300)

Dual-panel bar chart: the four raw scores with the argmax pick flagged (left) vs the four softmax shares (right), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "One Photo: Raw Scores vs Softmax Shares".
- **Data:** classes `["cat", "dog", "fox", "rabbit"]`; scores `[2.0, 1.0, 0.5, -0.5]`; softmax shares `[0.60, 0.22, 0.13, 0.05]`.
- **Left panel (raw scores):** panel from x=55, width 280; dashed `#999` zero line at y=205 labeled "0" 11px `#6b7280`; scale 50px per score unit (so 2.0 rises to y=105, −0.5 dips to y=230); four bars 44px wide in 70px slots, fill `rgba(42,120,214,0.45)`, 2px `#2a78d6` border; bold 12px `#2a78d6` value labels ("2.0", "1.0", "0.5", "−0.5") just outside each bar end; class names 12px `#444` at y=270; orange `#d95926` bold 13px annotation "argmax picks the tallest: cat" with a 2px orange arrow to the cat bar top; caption 12px `#444` "raw scores (logits) — can be negative".
- **Right panel (softmax):** panel from x=400, width 280, baseline y=245, chart height 160, y scale 0–0.7; same four 44px bars, fill `rgba(0,131,0,0.4)`, 2px `#008300` border; bold 12px `#008300` labels "0.60", "0.22", "0.13", "0.05" above the bars; class names 12px `#444` below baseline; green bold 13px annotation "shares sum to 1.00"; caption 12px `#444` "softmax shares of the same scores".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Turning Scores into Shares, by Hand

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **Step 1** — exponentiate every score: e^2.0 = 7.39, e^1.0 = 2.72, e^0.5 = 1.65, e^−0.5 = 0.61
- **Why e^x** — it makes every value positive and stretches leads: 2.0 beats 1.0 by ×2.7, not +1
- **Step 2** — add them up: 7.39 + 2.72 + 1.65 + 0.61 = 12.37
- **Step 3** — divide each by the sum: 7.39/12.37 = 0.60, then 0.22, 0.13, and 0.05
- **Check** — 0.60 + 0.22 + 0.13 + 0.05 = 1.00, so the four shares behave like probabilities

*Example (italic):* Rabbit's negative score −0.5 still earns a positive share, 0.61/12.37 = 0.05 — softmax never outputs an exact zero.

**Key point:** Softmax is e^score divided by the sum of all the e^scores — three arithmetic steps you can redo on paper. It never changes which score is biggest.

### Visualization (canvas `c2`, 720×300)

Three mini bar panels forming a left-to-right pipeline — raw scores, then e^score, then shares — joined by labeled arrows.

- **Title (bold 15px, `#1a5276`, top center):** "Softmax by Hand: Exponentiate, Sum, Divide".
- **Data:** scores `[2.0, 1.0, 0.5, -0.5]`; e-values `[7.39, 2.72, 1.65, 0.61]`; shares `[0.60, 0.22, 0.13, 0.05]`; class letters `["C", "D", "F", "R"]`.
- **Panel A (scores):** from x=40, width 180; heading bold 12px `#444` "1. scores" at y=55; dashed `#999` zero line at y=210, 35px per unit; four bars 30px wide in 45px slots, fill `rgba(42,120,214,0.45)`, 2px `#2a78d6` border; bold 11px `#2a78d6` value labels at each bar end; class letters 11px `#444` at y=262.
- **Panel B (e^score):** from x=270, width 180; heading "2. e^score"; baseline y=250, chart height 155, y scale 0–8; bars fill `rgba(25,158,112,0.4)`, 2px `#199e70` border; bold 11px `#199e70` labels "7.39", "2.72", "1.65", "0.61" above; class letters below baseline.
- **Panel C (shares):** from x=500, width 180; heading "3. ÷ 12.37"; baseline y=250, chart height 155, y scale 0–0.7; bars fill `rgba(0,131,0,0.4)`, 2px `#008300` border; bold 11px `#008300` labels "0.60", "0.22", "0.13", "0.05" above; class letters below baseline.
- **Arrows:** two 2px `#d95926` arrows at y=150 (x=225→265 and x=455→495) with bold 13px orange labels "e^x" and "÷ sum" above them.
- **Takeaway (bold 13px `#008300`, bottom center at y=292):** "0.60 + 0.22 + 0.13 + 0.05 = 1.00".

## One-Hot: Argmax Run Backwards

**Tags:** `core idea` (blue), `where it's used` (orange)

- **Argmax forward** — turns the score vector into just a name: [2.0, 1.0, 0.5, −0.5] → "cat"
- **One-hot back** — turns the name into a vector again: "cat" → [1, 0, 0, 0], a 1 in cat's slot
- **Inverse pair** — one-hot rebuilds a vector from the label, undoing argmax's vector-to-label step
- **Hardest softmax** — [1, 0, 0, 0] is the shape softmax approaches as cat's lead grows huge
- **In training** — the stored label "cat" becomes [1, 0, 0, 0], the target the model's shares must chase

*Example (italic):* Cross-entropy on this photo compares the one-hot target [1, 0, 0, 0] against the model's [0.60, 0.22, 0.13, 0.05].

**Key point:** Argmax maps a vector to a label; one-hot maps the label back to a vector. The round trip keeps the winner and erases every trace of doubt.

### Visualization (canvas `c3`, 720×300)

Round-trip diagram: softmax shares (left panel) flow through an "argmax → cat → one-hot" center chip into the one-hot vector (right panel), both panels on the same 0–1 scale.

- **Title (bold 15px, `#1a5276`, top center):** "Argmax → Label → One-Hot: the Round Trip".
- **Data:** softmax shares `[0.60, 0.22, 0.13, 0.05]`; one-hot vector `[1, 0, 0, 0]`; classes `["cat", "dog", "fox", "rabbit"]`.
- **Left panel (soft):** from x=50, width 220, baseline y=235, chart height 165, y scale 0–1.1; four bars 38px wide in 55px slots, fill `rgba(0,131,0,0.4)`, 2px `#008300` border; bold 12px `#008300` labels "0.60", "0.22", "0.13", "0.05" above; class names 11px `#444` below baseline; caption 12px `#444` "soft: softmax shares".
- **Center chip:** rounded rect (radius 8) from x=305 to x=415 centered at y=140, fill `rgba(26,82,118,0.08)`, 2px `#1a5276` border, "cat" bold 16px `#1a5276` centered inside; 2px `#1a5276` arrow from the left panel into the chip labeled bold 12px "argmax" above it, and a 2px `#4a3aa7` arrow from the chip into the right panel labeled bold 12px `#4a3aa7` "one-hot" above it.
- **Right panel (hard):** from x=450, width 220, same baseline/height/scale; bars fill `rgba(74,58,167,0.4)`, 2px `#4a3aa7` border, heights `[1, 0, 0, 0]` (zero bars drawn as 2px stubs); bold 12px `#4a3aa7` labels "1", "0", "0", "0" above; class names below; caption 12px `#444` "hard: one-hot target [1, 0, 0, 0]".
- **Annotation:** magenta `#d55181` bold 12px near the right panel's dog slot: "dog's 0.22 of doubt is erased".

## The Coin Flip Argmax Hides

**Tags:** `common mistake` (red), `confidence` (orange)

- **Photo B** — a second photo scores cat 1.1, dog 1.05, fox −0.3, rabbit −1.1
- **Softmax says** — cat 0.43, dog 0.41, fox 0.11, rabbit 0.05 — cat barely edges out dog
- **Argmax says** — "cat", the exact same answer it gave the 0.60 photo, with no hint of doubt
- **No gradient** — nudging 1.05 to 1.06 leaves argmax frozen on "cat", so nets train through softmax
- **Reading rule** — act on argmax if you must pick, but report softmax whenever the margin matters

*Example (italic):* An auto-tagger shipped argmax labels only, so nobody saw that photo B was a 0.43-vs-0.41 coin flip.

**Common mistake:** Treating the argmax label as confidence. "Cat" can mean 0.60 sure or 0.43 sure — only the softmax shares tell you which one you got.

### Visualization (canvas `c4`, 720×300)

Dual-panel bar chart: softmax shares for the confident photo A (left) and the near-tie photo B (right), each topped by an identical "argmax: cat" badge, split by a dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Two Photos, One Argmax Answer".
- **Data:** photo A shares `[0.60, 0.22, 0.13, 0.05]`; photo B shares `[0.43, 0.41, 0.11, 0.05]` (from scores 1.1, 1.05, −0.3, −1.1); classes `["cat", "dog", "fox", "rabbit"]`.
- **Left panel (photo A):** from x=55, width 280, baseline y=240, chart height 150, y scale 0–0.7; four bars 44px wide in 70px slots, fill `rgba(0,131,0,0.4)`, 2px `#008300` border; bold 12px `#008300` value labels above; class names 12px `#444` below baseline; badge at y=52 centered over the panel: rounded chip, fill `#1a5276`, bold 12px white text "argmax: cat"; caption 12px `#444` "photo A: a clear call".
- **Right panel (photo B):** from x=400, width 280, same baseline/height/scale; bars fill `rgba(217,89,38,0.45)`, 2px `#d95926` border; bold 12px `#d95926` value labels above; identical `#1a5276` "argmax: cat" badge at y=52; magenta `#d55181` bold 13px annotation with a bracket over the cat and dog bars: "0.43 vs 0.41 — a coin flip"; caption 12px `#444` "photo B: a near tie".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.
- **Takeaway (bold 13px `#d55181`, bottom center at y=294):** "same label, very different confidence".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
