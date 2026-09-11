# Cost, Latency, Quality

**Page type:** detail page (tutorial layout: `.card-section` blocks with h2 + layout table; first section is 3-column 38/31/31 text + two canvases, remaining sections two-column text 50% / canvas 50%)
**HTML title tag:** Cost, Latency, Quality

**Subtitle:** Every model choice trades three things — money, speed, and how good the answers are — and no single model wins all three

## Same Task, Two Models, Three Numbers

**Tags:** `core idea` (blue), `running example` (green)

- **The task** — summarize support tickets; both models get identical prompts
- **Small model** — $2 per 1,000 calls, 0.8s median response, 78% quality
- **Large model** — $40 per 1,000 calls, 4.5s median response, 92% quality
- **Quality** — the pass rate on a 20-ticket graded golden set (illustrative numbers)
- **No free lunch** — the better answers cost 20x more and take 5.6x longer

*Example (italic):* The large model writes the better summary of ticket #7 — 3.7 seconds after the small one already answered.

**Key point:** "Which model is best?" is the wrong question. Ask "best at what price, at what speed, for this task?"

### Visualization (canvas `c1a`, 420×340)

Two-bar vertical chart of cost per 1,000 calls (drawn by a shared `twoBars` helper).

- **Title (bold 15px, `#1a5276`, top center):** "Cost per 1,000 calls".
- **Data:** "small model" $2, "large model" $40; value labels "$2" and "$40" bold 13px above the bars.
- **Bars:** 100px wide, small model aqua `#199e70`, large model violet `#4a3aa7`; scale max 44; model names 12px below baseline.
- **Axes:** y ticks at $0, $20, $40 (formatted "$0" etc.), gridlines `#e5e9ef`; padding top 56, bottom 52, left 62, right 20.
- **Note (bold 13px violet, bottom center):** "20x the price".

### Visualization (canvas `c1b`, 400×340)

Two stacked panels of horizontal bars: latency (top) and quality (bottom) for the two models.

- **Title (bold 15px, `#1a5276`, top center):** "Response time & quality".
- **Top panel:** heading bold 12px mute `#6b7280` "median response time"; bars start at x=120, width scaled to a 0–5s range: "small" 0.8s (aqua `#199e70`), "large" 4.5s (violet `#4a3aa7`); 24px tall, rows at y=66 and y=104; names right-aligned left of bars, bold 12px value labels ("0.8s", "4.5s") right of bars.
- **Bottom panel:** heading "quality (golden-set pass rate)"; bars scaled 0–100%: "small" 78% (aqua), "large" 92% (violet); rows at y=190 and y=228; value labels "78%", "92%".
- **Note (bold 13px violet, bottom center):** "+14 quality points, 5.6x slower".

## What the Gap Costs at 10,000 Calls a Day

**Tags:** `worked example` (green), `core idea` (blue)

- **Volume** — 10,000 ticket summaries per day, every day
- **Small model** — 10 × $2 = $20/day → $20 × 365 = $7,300 per year
- **Large model** — 10 × $40 = $400/day → $400 × 365 = $146,000 per year
- **The gap** — $138,700 a year buys 14 quality points (78% → 92%)
- **Frame it** — is fixing 14 extra summaries in 100 worth ~$380 a day here?

*Example (italic):* For internal triage notes, probably not; for summaries sent to paying customers, maybe yes.

**Key point:** Per-call prices look tiny until you multiply by volume. Always cost the yearly bill, then ask what each quality point is worth in your use case.

### Visualization (canvas `c2`, 720×300)

Two-bar vertical chart of yearly cost with a dashed gap bracket between the bar tops.

- **Title (bold 15px, `#1a5276`, top center):** "Yearly Bill at 10,000 Calls per Day".
- **Data:** "small model (78% quality)" $7,300/yr (value label "$7,300 / yr", aqua `#199e70`); "large model (92% quality)" $146,000/yr (label "$146,000 / yr", violet `#4a3aa7`).
- **Axes:** y scaled to max 160 ($k) with ticks "$0k", "$50k", "$100k", "$150k" and gridlines `#e5e9ef`; padding top 56, bottom 50, left 80, right 40.
- **Bars:** 170px wide, evenly spaced; bold 13px value labels above bars, 12px names below baseline.
- **Gap marker:** dashed red line (`#e74c3c`, dash 6/4, 2px) connecting the small bar's top toward the large bar's top.
- **Annotation (bold 14px red `#e74c3c`, centered near top):** "the 14 quality points cost $138,700 a year".

## Routing: Send Easy Cases to the Cheap Model

**Tags:** `where it's used` (blue), `rule of thumb` (green)

- **The insight** — most tickets are easy; the small model handles them well
- **The split** — 70% easy tickets → small model (90% quality on the easy ones)
- **The rest** — 30% hard tickets → large model (92% quality)
- **Blended quality** — 0.7 × 90 + 0.3 × 92 = 90.6%, near the large model's 92%
- **Blended cost** — 0.7 × $2 + 0.3 × $40 = $13.40 per 1,000 vs $40 all-large

*Example (italic):* A one-line router — "long ticket, multiple orders, or angry customer → large model" — is often enough to start.

**Rule of thumb:** Don't pick one model — pick a policy. Routing easy cases cheap kept 98.5% of the quality for a third of the cost here.

### Visualization (canvas `c3`, 720×300)

Scatter plot of three policies: cost per 1,000 calls (x) vs blended quality (y).

- **Title (bold 15px, `#1a5276`, top center):** "Three Policies: Cost per 1,000 Calls vs Quality".
- **Data (name, cost, quality, color):** "all small" ($2, 78%, aqua `#199e70`); "routed 70/30" ($13.40, 90.6%, green `#008300`); "all large" ($40, 92%, violet `#4a3aa7`).
- **Axes:** x $0–$45 with tick labels $0, $10, $20, $30, $40; y quality 70–95% with tick labels 70%, 80%, 90% and gridlines `#e5e9ef`; x-axis title "cost per 1,000 calls" (bottom center), y-axis title "blended quality" (rotated vertical); padding top 56, bottom 52, left 70, right 40.
- **Points:** 9px-radius filled circles; bold 13px name labels beside each point; 12px sub-labels "$2, 78%", "$13.40, 90.6%", "$40, 92%" below the names in `#2c3e50`.
- **Annotation (bold 14px green `#008300`, centered near top):** "routing keeps 98.5% of the quality at 34% of the cost".

## The Confusion: "Always Use the Best Model"

**Tags:** `common mistake` (red), `trade-off` (orange)

- **The mistake** — defaulting to the biggest model for everything, "to be safe"
- **Latency has a budget** — a live-chat user gives up waiting after about 2 seconds
- **Live chat** — only the 0.8s small model fits; the 4.5s large one loses the user
- **Agent assist** — a ~6s budget fits both; now decide on quality per dollar
- **Overnight batch** — nobody is waiting; latency is irrelevant, quality per dollar rules

*Example (italic):* The "worse" small model wins the chat use case outright — the best answer that arrives too late is a non-answer.

**Common mistake:** Comparing models on quality alone. The use case sets the latency budget and the money budget first — quality is chosen inside those limits.

### Visualization (canvas `c4`, 720×300)

Horizontal latency-budget bars per use case with vertical model-speed marker lines crossing them.

- **Title (bold 15px, `#1a5276`, top center):** "Latency Budget by Use Case vs Model Speed".
- **Axes:** x 0–8s with tick labels 0s, 2s, 4s, 6s, 8s; padding top 62, bottom 46, left 150, right 30.
- **Budget bars (32px tall, fill `rgba(42,120,214,0.18)`, 1px blue `#2a78d6` stroke, rows 52px apart):** "live chat" budget 2s, note "only small fits" (aqua bold 12px right of the bar); "agent assist" budget 6s, note "both fit — pick by quality per $"; "overnight batch" budget capped at full axis width (no limit), note "no limit — quality per $ rules" drawn in dark text `#2c3e50` inside the bar. Row names bold 12px right-aligned left of the bars.
- **Model markers:** solid aqua `#199e70` vertical line (3px) at 0.8s labeled "small: 0.8s" (bold 12px aqua above the axis); dashed violet `#4a3aa7` vertical line (3px, dash 7/4) at 4.5s labeled "large: 4.5s" (bold 12px violet).
- **Takeaway (bold 13px red `#e74c3c`, bottom center):** "the large model misses the live-chat budget entirely — quality never got a vote".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout`.
- **Column widths:** section 1 uses the 3-column layout — `td.text-col3` (38%) + two `td.viz-col3` (31% each) holding canvases `c1a` (420×340) and `c1b` (400×340); sections 2–4 use `td.text-col` (50%) + `td.viz-col` (50%) with 720×300 canvases.
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic width/height attributes as given per chart; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. A shared `twoBars(...)` helper draws simple two-bar vertical charts (used by `c1a`).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
