# Kimi & Open-Weight Contenders

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Kimi & Open-Weight Contenders

**Subtitle:** Kimi, DeepSeek, Qwen, and Llama ship as downloadable weights you can run behind your own firewall — trading some frontier capability for the guarantee that code never leaves the building

## The Model You Can Download

**Tags:** `core idea` (blue), `open weights` (green), `self-hosting` (orange)

- **The file** — an open-weight model is a downloadable file you can run on GPUs you control
- **The contenders** — Kimi (Moonshot), DeepSeek, Qwen (Alibaba), Llama (Meta) all publish coding-capable weights
- **The unlock** — prompts and code go to a server you own, so source never crosses the company firewall
- **The contrast** — a hosted frontier API sends every prompt, file, and diff to the vendor's cloud
- **The trade** — hosted buys top capability with zero ops; open weights buy control and pay in hardware

*Example (italic):* A hospital points its coding assistant at a GPU box in its own data center; code touching patient systems never leaves the building.

**Key point:** "Open weight" means the model artifact itself is public — anyone can download it and serve it inside their own network. That is the run-it-inside-the-firewall unlock.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram on a shared firewall boundary: the hosted-API path (code crosses the firewall to the vendor's cloud) vs the self-hosted path (code loops back inside the network).

- **Title (bold 15px, `#1a5276`, top center):** "Same Prompt, Two Destinations: Where Does the Code Go?".
- **Firewall:** vertical dashed `#6b7280` (dash 6/4) line at x=450 from y=55 to y=255; bold 12px `#6b7280` label "company firewall" just right of it at y=68.
- **Row 1 (y=110), label 12px `#444` at x=20:** "hosted API"; blue `#2a78d6` rounded box at x=60, 160px wide, labeled "your code + prompt" (12px), 3px red `#e74c3c` arrow from x=220 across the firewall to a red box at x=500, 180px wide, labeled "vendor cloud GPUs" with bold 12px red "✗ code crosses the line" beneath it.
- **Row 2 (y=210), label:** "self-hosted weights"; blue box at x=60 "your code + prompt", 3px green `#008300` arrow to a green box at x=255, 170px wide, labeled "your own GPU server" with bold 12px green "✓ never leaves" beneath it — everything left of the firewall.
- **Box style:** 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=282):** "the model moves to the data — the data never moves to the model".

## A Bank Picks Its Coding Model

**Tags:** `worked example` (blue), `compliance gate` (green)

- **The rule** — bank policy says source code may never leave the corporate network, no exceptions
- **Option A** — the frontier hosted API is the most capable, but prompts carry code off-network: fails
- **Option B** — self-hosted open weights on an 8-GPU cluster keep every token inside: passes the gate
- **The math** — hosted would run $30/dev/mo × 400 developers = $12,000/mo; the cluster ~$18,000/mo (illustrative)
- **The verdict** — the bank pays ~$6,000/mo more for a somewhat weaker model, because the gate is binary

*Example (italic):* For 400 developers, hosted is $30 × 400 = $12,000/mo and the 8-GPU cluster is $18,000/mo — 1.5× the price, and still the only option that passes the rule.

**Key point:** Capability rankings only matter among options that pass the compliance gate; when code cannot leave the network, open weights are not the cheaper choice — they are the only choice.

### Visualization (canvas `c2`, 720×300)

Vertical bar chart of monthly cost for the two options, with a pass/fail gate verdict stamped on each bar — showing the gate, not the price, deciding.

- **Title (bold 15px, `#1a5276`, top center):** "The Bank's Choice: $12k Hosted Fails the Gate, $18k Self-Hosted Passes".
- **Axes:** origin x=90, baseline y=245, plot width 560, plot height 175; y = $0 to $20,000/mo, gridlines `#e5e9ef` at 5k/10k/15k with 11px `#6b7280` labels "$5k"/"$10k"/"$15k".
- **Bar 1 (hosted):** blue `rgba(42,120,214,0.30)` with 2px `#2a78d6` border, x=180, 100px wide, height 105px ($12,000); 12px `#444` label below baseline "hosted API — 400 devs × $30"; bold 13px red `#e74c3c` stamp centered on the bar: "✗ fails gate: code leaves network".
- **Bar 2 (self-hosted):** green `rgba(0,131,0,0.25)` with 2px `#008300` border, x=440, 100px wide, height 158px ($18,000); 12px `#444` label below "self-hosted — 8-GPU cluster"; bold 13px green `#008300` stamp: "✓ passes gate".
- **Value labels:** bold 12px `#2c3e50` above each bar: "$12,000/mo" and "$18,000/mo".
- **Annotation (bold 13px magenta `#d55181`, near x=300, y=75):** "the gate decides before the price does".
- **Caption (12px `#444`, bottom right):** "costs illustrative".

## The Contenders and the Fine-Tuning Unlock

**Tags:** `where it's used` (blue), `fine-tuning` (green), `mixture of experts` (orange)

- **Kimi K2** — Moonshot's July 2025 release: a mixture-of-experts with ~1T total parameters, 32B active
- **DeepSeek** — V3 (Dec 2024) runs 671B total / 37B active; the R1 reasoning model ships under MIT
- **Qwen** — Alibaba's coder line spans small Apache-2.0 models up to Qwen3-Coder at 480B / 35B active
- **Llama** — Meta's line, topping out at the 405B-parameter Llama 3.1, made big downloadable weights mainstream
- **Fine-tuning** — downloaded weights can be tuned without code leaving your network; hosted tuning cannot

*Example (italic):* A firm fine-tunes an open coder model on ten years of its internal APIs — an option no hosted vendor can match without the code leaving home.

**Key point:** The trade is stable: hosted frontier models lead on raw capability, while open weights lead on control — data residency, fine-tuning rights, and cost predictability at scale.

### Visualization (canvas `c3`, 720×300)

Horizontal release timeline of the open-weight coding contenders, each a labeled dot with its published parameter figures — dates and sizes only, no benchmark scores.

- **Title (bold 15px, `#1a5276`, top center):** "The Open-Weight Lineup: Two Years of Downloadable Contenders".
- **Axis:** horizontal 2px `#999` timeline at y=160 from x=60 to x=680; 12px `#444` tick labels below at x=100 "Jul 2024", x=340 "Jan 2025", x=580 "Jul 2025" (scale: 40px per month).
- **Dots (8px radius), alternating labels above and below with 1px `#6b7280` leader lines:**
  - x=100 blue `#2a78d6`, label above at y=95: bold "Llama 3.1" + 12px "405B dense (Meta)"
  - x=260 aqua `#199e70`, label below at y=210: bold "Qwen2.5-Coder" + 12px "32B (Alibaba)"
  - x=300 violet `#4a3aa7`, label above at y=95: bold "DeepSeek-V3" + 12px "671B / 37B active"
  - x=340 violet `#4a3aa7`, label below at y=245: bold "DeepSeek-R1" + 12px "reasoning, MIT"
  - x=580 aqua `#199e70`, label above at y=95: bold "Qwen3-Coder" + 12px "480B / 35B active"
  - x=600 orange `#d95926`, label below at y=210: bold "Kimi K2" + 12px "~1T / 32B active (Moonshot)"
- **Label fonts:** names bold 13px in the dot's color, detail lines 12px `#444`.
- **Annotation (bold 13px green `#008300`, near x=430, y=270):** "every one of these can run entirely inside your network".
- **Caption (12px `#444`, bottom right):** "dates and parameter counts as published; no benchmark scores shown".

## Open Weight Is Not Free — or Open Source

**Tags:** `common mistake` (red), `total cost` (orange)

- **The $0 illusion** — the license fee is zero, but serving a large MoE needs a GPU cluster and an ops team
- **The real bill** — GPUs, inference engineers, and monitoring-plus-upgrades make up the monthly cost
- **License fine print** — Llama's community license carries usage terms; open-weight licenses vary widely
- **Not open source** — weights are public but training data and code are not; many licenses fail the OSI test
- **Capability gap** — assuming a self-hosted model matches the frontier hosted one sets pilots up to disappoint

*Example (italic):* A team budgets $0 for its "free" model, then discovers GPUs at $11,000, ops at $5,000, and upgrades at $2,000 add up to the bank's $18,000/mo (illustrative).

**Common mistake:** Reading "open weight" as "free and equivalent." The license costs nothing; the hardware, the people, and the capability gap are the price — budget for all three before switching.

### Visualization (canvas `c4`, 720×300)

Two-row cost comparison: the naive budget (license: $0, an empty bar) vs the real monthly bill (a stacked bar of GPUs + ops + upgrades totaling $18,000).

- **Title (bold 15px, `#1a5276`, top center):** "The 'Free' Model's Bill: $0 License, $18,000/mo Everything Else".
- **Layout:** row labels 12px `#444` right-aligned ending at x=195; bars start at x=210, 26px tall; scale $1,000 = 25px (full $18,000 = 450px).
- **Row 1 (y=110), label "what the budget saw":** empty bar outline 1px dashed `#6b7280`, width 450; bold 13px red `#e74c3c` text inside at x=230: "license: $0 — the only line item people plan for".
- **Row 2 (y=190), label "what the invoice says":** stacked segments — blue `#2a78d6` fill `rgba(42,120,214,0.30)` width 275 labeled "GPUs $11k" (bold 12px `#2a78d6`, inside), orange `#d95926` fill `rgba(217,89,38,0.25)` width 125 labeled "ops $5k", aqua `#199e70` fill `rgba(25,158,112,0.25)` width 50 labeled "upgrades $2k" (label above the segment, it is narrow); bold 12px `#2c3e50` total "= $18,000/mo" at the bar's right end.
- **Segment style:** 1px borders in each segment's solid color, labels 12px bold.
- **Annotation (bold 13px orange `#d95926`, centered near y=262):** "open weight moves the cost from the license to the infrastructure".
- **Caption (12px `#444`, bottom right):** "cost breakdown illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded figures above (no randomness); the bank's costs ($30/dev/mo × 400 = $12,000 vs $18,000/mo, split $11k GPUs / $5k ops / $2k upgrades) are invented and labeled illustrative; the c3 timeline entries — Llama 3.1 405B (Jul 2024), Qwen2.5-Coder 32B (Nov 2024), DeepSeek-V3 671B/37B (Dec 2024), DeepSeek-R1 (Jan 2025), Qwen3-Coder 480B/35B (Jul 2025), Kimi K2 ~1T/32B (Jul 2025) — use published release dates and parameter counts; no benchmark scores appear anywhere on the page.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
