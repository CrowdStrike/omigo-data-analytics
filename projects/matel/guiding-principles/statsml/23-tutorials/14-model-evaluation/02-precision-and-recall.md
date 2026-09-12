# Precision & Recall

**Page type:** detail page (tutorial card-sections: h2 per section, two-column table.layout with text left 50%, canvas right 50%)
**HTML title tag:** Precision & Recall

**Subtitle:** Two questions about the same fraud flags: how many alarms were real (precision), and how much of the fraud got caught (recall)

## 100 Flags, Two Questions

**Tags:** `core idea` (blue), `running example` (green)

- **The data** — 1,000 transactions, 50 of them fraud (5%)
- **The model flags 100** transactions as suspicious
- **Inside the flags** — 40 are real fraud, 60 are false alarms
- **Left behind** — 10 of the 50 frauds were never flagged
- **Precision** — of the 100 flagged, how many were fraud? 40/100 = 40%
- **Recall** — of all 50 frauds, how many got flagged? 40/50 = 80%

*Example:* Precision is the analyst's day: 6 of every 10 alerts they review turn out to be wasted work.

**Key point:** Precision starts from the flags; recall starts from the fraud. Same 40 catches, two different denominators.

### Visualization (canvas `c1`, 720×300)

Two stacked-column "piles" comparing the 100 flagged transactions vs the 50 actual frauds, separated by a vertical dashed divider.

- **Title (bold 15px, `#1a5276`, top center):** "Same 40 Catches, Two Denominators".
- **Divider:** vertical dashed line (`#bdc3c7`, dash 4/3, width 1) at x=360 from y=40 to y=h-14.
- **Scale:** 1 item = 1.7px of column height; baseline y=232; both columns 120px wide.
- **Left pile (x=110):** stacked column of 100 flagged — bottom segment 40 items green `#008300` labeled "40 real fraud" (white bold 13px), top segment 60 items orange `#d95926` labeled "60 false alarms" (white bold 13px); gray `#6b7280` outline around the whole stack. Below: "100 flagged" (bold 13px, `#2c3e50`), then in green bold 14px: "precision = 40/100 = 40%" (offset 60px right of column center).
- **Right pile (x=470):** stacked column of 50 frauds — bottom segment 40 items green labeled "40 caught" (white bold 13px), top segment 10 items magenta `#d55181` with label "10 missed" in magenta to the right of the column; gray outline. Below: "50 frauds" (bold 13px), then in blue `#2a78d6` bold 14px: "recall = 40/50 = 80%".
- **Column captions (gray 12px, y=52):** "what the model flagged" over the left pile, "what was actually fraud" over the right pile.

## Both Fractions by Hand

**Tags:** `worked example` (green), `core idea` (blue)

- **Caught (TP)** — 40 transactions flagged AND truly fraud
- **False alarms (FP)** — 60 flagged but actually legit
- **Missed (FN)** — 10 frauds the model let through
- **Precision** — 40 / (40 + 60) = 40 / 100 = 40%
- **Recall** — 40 / (40 + 10) = 40 / 50 = 80%
- **Not counted** — the 890 correctly-passed legit rows appear in neither fraction

*Example:* Both fractions share the same top number, 40 — only the bottom changes.

**Key point:** Precision = TP / (TP + FP). Recall = TP / (TP + FN). Learn them as "caught over flagged" and "caught over all fraud".

### Visualization (canvas `c2`, 720×300)

Two horizontal segmented bars drawn on one shared scale (1 transaction = 5px width), showing precision and recall denominators.

- **Title (bold 15px, `#1a5276`, top center):** "Same Top Number (40), Different Bottom Number".
- **Bars:** start x=150, height 44px each, gray `#6b7280` outline.
  - Precision bar (y=70): green `#008300` segment 40 wide labeled "TP = 40" (white bold 13px) + orange `#d95926` segment 60 wide labeled "FP = 60"; total length 100 units. Left-side labels (right-aligned at x=140): "precision" (bold 12px `#2c3e50`) and "40 / 100 = 40%" (gray 12px).
  - Recall bar (y=160): green segment 40 wide labeled "TP = 40" + magenta `#d55181` segment 10 wide with "FN = 10" label in magenta to the right of the bar; total length 50 units. Left-side labels: "recall" and "40 / 50 = 80%".
- **Annotations (centered):** green bold 13px at y=250: "the green 40 is identical in both — only what it is divided by changes"; gray 12px at y=272: "both bars drawn to the same scale: 1 transaction = same width".

## The Tug-of-War: Moving the Flagging Bar

**Tags:** `trade-off` (orange), `where it's used` (blue)

- **Strict bar** — flag only 20: 18 real, 2 false → precision 90%, recall 36%
- **Our bar** — flag 100: 40 real, 60 false → precision 40%, recall 80%
- **Loose bar** — flag 200: 48 real, 152 false → precision 24%, recall 96%
- **The trade** — pushing one number up almost always drags the other down
- **Pick by cost** — missed fraud loses money; false alarms burn analyst time and block customers

*Example:* A cancer screen leans toward recall (miss nothing); a spam filter leans toward precision (never eat a real email).

**Key point:** The threshold is a business choice, not a math choice — decide which mistake hurts more, then set the bar.

### Visualization (canvas `c3`, 720×300)

Two-series line chart of precision and recall across three flagging thresholds.

- **Title (bold 15px, `#1a5276`, top center):** "Loosen the Bar: Recall Rises, Precision Falls".
- **Data:** x categories flags = `[20, 100, 200]`; precision = `[90, 40, 24]` (%); recall = `[36, 80, 96]` (%).
- **Axes:** padding top 56, bottom 54, left 70, right 170; y from 0 to ~105% with labels at 0%, 50%, 100% (gray 12px, right-aligned) and light gridlines `#e5e9ef` at 50 and 100; L-shaped axes in `#999`. X tick labels under each point (gray 12px): "flag 20", "flag 100", "flag 200".
- **Series:** precision line orange `#d95926`, width 3, with 5px dots; recall line blue `#2a78d6`, width 3, with 5px dots. Each point labeled with its % value (bold 12px in the series color), placed above the higher of the two series and below the lower.
- **Legend (top right, x=w-180):** orange swatch + "precision", blue swatch + "recall" (12px `#2c3e50`).
- **Side annotations (bold 12px, `#1a5276`, left-aligned at x=w-180, y=130/150/170):** "flag 20: 18 real, 2 false" / "flag 100: 40 real, 60 false" / "flag 200: 48 real, 152 false".
- **X-axis caption (gray 12px, bottom center):** "transactions flagged (out of 1,000 with 50 frauds)".

## The Confusion: Either Number Alone Can Be Gamed

**Tags:** `common mistake` (red), `trade-off` (orange)

- **Vague claims** — "the model is 80% right" could mean precision, recall, or accuracy
- **Flag everything** — all 1,000: recall hits 100% (50 of 50) while precision drops to 5%
- **Flag one sure thing** — 1 obvious fraud: precision 100%, recall only 2% (1 of 50)
- **Both extremes are useless** — yet each shows a perfect 100% somewhere
- **Report the pair** — a single number without its partner hides the trade made to get it

*Example:* "We catch 96% of fraud" quietly came with 152 false alarms for every 48 real catches.

**Common mistake:** Quoting precision or recall alone — always ask for the other number before believing either.

### Visualization (canvas `c4`, 720×300)

Two side-by-side bar-pair panels showing degenerate models, separated by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Either Number Alone Can Be Made Perfect — and Useless".
- **Divider:** vertical dashed line (`#bdc3c7`, dash 4/3) at x=360 from y=40 to y=h-14.
- **Panels:** each 300px wide (left at x=30, right at x=390), baseline y=224, bar height scale 140px for 105%; two bars per panel (85px wide) — precision in orange `#d95926`, recall in blue `#2a78d6` — with value labels above (bold 13px `#2c3e50`) and "precision"/"recall" labels below (12px); thin `#999` baseline.
  - Left panel: title "flag all 1,000" (bold 13px `#1a5276`), subtitle "catches all 50 — plus 950 false alarms" (gray 12px); precision 5%, recall 100%.
  - Right panel: title "flag 1 sure thing", subtitle "one perfect catch — 49 frauds missed"; precision 100%, recall 2%.
- **Takeaway (red `#e74c3c` bold 13px, centered, y=272):** "a perfect 100% on one side hides a disaster on the other".

## Regeneration instructions

- **Template:** tutorial detail page (tutorials/ style, per social-graph reference skeleton). h1 (no index number) with 2px `#2980b9` bottom border, `.subtitle` paragraph, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) holding `.tags` pills, a `<ul>` of one-line bullets with `<b>` lead terms, an italic `.example` paragraph, and a `.key-point` callout; right `<td class="viz-col">` (50%) holding one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem; `li b` in `#1a5276`; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; canvases `width:100%`, `1px solid #e0e0e0` border, 4px radius.
- **Tag pills:** `.tag` inline-block, 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** intrinsic 720×300 attributes; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; all data hardcoded literal arrays (no Math.random). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- In regenerated HTML, any card links use `.html` extensions. No nav bar, no back/home links.
