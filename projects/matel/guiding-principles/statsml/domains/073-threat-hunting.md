# Domain Pitfalls: Threat Hunting

**Page type:** detail page (one h2 per pitfall, each followed by a two-column obj-table row: text left ~40%, canvas right ~60%)
**HTML title tag:** Domain Pitfalls: Threat Hunting

**Subtitle:** Statistical and analytical traps in proactive threat detection and adversary pursuit

Note: canvas elements carry `width="720" height="300"` attributes, but the `setupCanvas` helper forces the drawing surface and CSS size to 720×200. Effective chart size is 720×200.

## Hypothesis-driven search bias

**Hypothesis-driven search bias**

- **The trap:** Hunter has a theory and searches for confirming evidence — and reliably finds some.
- **Dense graph:** Support always turns up because EVERYTHING connects at 3 hops in a dense graph.
- **Missing baseline:** The connections are real but meaningless without base-rate comparison.
- **Amplifier:** Confirmation bias is amplified by data richness.

### Visualization (canvas `canvas1`, 720×200)

Dense node-link graph with one highlighted "hypothesis path".

- **Background:** `#f0f4f8` fill over the whole canvas.
- **Nodes:** 18 pseudo-randomly placed nodes (LCG with seed 42: seed = (seed × 1664525 + 1013904223) mod 2^32; x in [60, 660], y in [30, 170]).
- **Edges:** every node pair closer than 200px is connected with a `rgba(41,128,185,0.15)` line width 1 — a dense hairball where everything connects.
- **Hypothesis path:** node indices 2 → 5 → 9 → 14 → 17 connected with `#e74c3c` lines width 2.5; those nodes drawn as radius-7 red `#e74c3c` dots with `#c0392b` outline; all other nodes radius-4 blue `#2980b9` dots.
- **Labels (top right):** bold 17px `#e74c3c` "Hypothesis path"; 17px `#2980b9` "(but 47 other paths also exist)".

## Dwell time estimation impossible

**Dwell time estimation impossible**

- **The question:** The attacker has been inside for HOW long — you don't know what you don't know.
- **First evidence ≠ first intrusion:** Actual dwell time is always LONGER than detected dwell time.
- **Consequence:** You are always underestimating exposure.

### Visualization (canvas `canvas2`, 720×200)

Timeline diagram: actual intrusion span (red) vs shorter detected dwell time (green), with an unknown gap.

- **Background:** `#f8f4f0`.
- **Timeline:** horizontal gray `#7f8c8d` line width 2 at y=100 from x=60 to x=680.
- **Actual intrusion span:** rect from x=80 to x=650, 60px tall centered on the timeline, fill `rgba(231,76,60,0.15)`, stroke `#e74c3c` width 2; red triangle marker at its start with bold 17px label "Actual intrusion".
- **Detected dwell span:** rect from x=360 to x=650, 40px tall, fill `rgba(39,174,96,0.2)`, stroke `#27ae60` width 2; green triangle marker below its start with 17px label "First evidence found".
- **Detection point:** amber `#f39c12` dot radius 6 at the right end of the spans, labeled "Detected" (17px amber above).
- **Gap label:** bold 17px `#e74c3c` "UNKNOWN GAP" centered under the region between actual start and detected start.
- **Day labels (13px `#7f8c8d`, below):** "Day 0" at the actual start, "Day 45" at the detected start, "Day 72" at the detection point.

## MITRE ATT&CK coverage gaps

**MITRE ATT&CK coverage gaps**

- **The math:** The framework has 200+ techniques; your detection covers 40.
- **Where attackers live:** The 160 unmonitored techniques are where attackers operate.
- **Streetlight effect:** You hunt in the light while they work in the dark — the coverage map shows your blind spots.

### Visualization (canvas `canvas3`, 720×200)

Cell-grid coverage map of ATT&CK techniques.

- **Background:** `#f4f8f0`.
- **Title (bold 17px `#1a5276` at (40, 22)):** "ATT&CK Technique Coverage Map (210 techniques)".
- **Grid:** 35 columns × 6 rows = 210 cells, 14px squares with 2px gaps, starting at (40, 35).
- **Cell assignment (pseudo-random, Lehmer LCG seed 17, multiplier 48271, mod 2147483647):** 40 random cells green `#27ae60` (monitored); 12 additional random cells among the uncovered are red `#e74c3c` (attacker-used); all remaining cells gray `#bdc3c7` (unmonitored).
- **Legend (14px `#2c3e50`, below the grid):** green swatch "Monitored (40)"; gray swatch "Unmonitored (158)"; red swatch "Attacker techniques (12) - ALL unmonitored".

## Lateral movement invisible in single-source

**Lateral movement invisible in single-source**

- **Fragmented view:** Endpoint log shows "login," network log shows "connection," auth log shows "ticket granted."
- **No single source:** None of them shows the full movement chain.
- **Requirement:** You must correlate 3+ sources in real-time.

### Visualization (canvas `canvas4`, 720×200)

Three log-source timelines across five hosts, with the full attack path only visible as a dashed line crossing all three.

- **Background:** `#f0f0f8`.
- **Host columns (bold 14px `#1a5276` header row at y=18):** Host A-E at x = 180, 280, 380, 480, 580.
- **Source rows (bold 13px name in row color at left, light `#ddd` timeline from x=160 to x=620):**
  - Endpoint Logs — y=40 — `#3498db` — events: "login" at Host A, "process" at Host C
  - Network Logs — y=90 — `#e67e22` — events: "connection" at Host B, "connection" at Host D
  - Auth Logs — y=140 — `#9b59b6` — events: "ticket" at Host C, "ticket" at Host E
- **Event markers:** radius-10 circles at (hostX, rowY), fill in source color at alpha 0.3 with solid outline width 2, event name (11px, source color) below; empty slots get small radius-4 `#ddd` dots.
- **Attack path:** dashed (4/4) red `#e74c3c` line width 2 zig-zagging across sources: Host A (Endpoint) → Host B (Network) → Host C (Auth) → Host D (Network) → Host E (Auth).
- **Caption (bold 17px `#e74c3c`, bottom left at x=160):** "Full path: only visible correlating ALL 3 sources".

## Analyst fatigue false-negative

**Analyst fatigue false-negative**

- **The miss:** After reviewing 500 alerts, the analyst misses alert 501 — the real one.
- **Depletion:** Cognitive capacity depletes, so the most important alert arrives when you're mentally exhausted.
- **Weak link:** Human-in-the-loop becomes human-as-weak-link.

### Visualization (canvas `canvas5`, 720×200)

Declining detection-rate curve over alerts reviewed, with the missed real threat marked.

- **Background:** `#f8f4f8`.
- **Axes:** L-shape `#7f8c8d` width 1.5; plot from x=80 to x=680, y=30 to y=160. Y label (rotated 14px `#2c3e50`): "Detection Rate %", ticks 0-100% every 25% with `#ecf0f1` gridlines; X label: "Alerts Reviewed", ticks 0, 100, 200, 300, 400, 500, 600.
- **Data (alerts → detection rate %):** 0→95, 50→92, 100→88, 150→82, 200→74, 250→68, 300→58, 350→50, 400→42, 450→35, 500→28, 550→22.
- **Series:** red `#e74c3c` line width 2.5 with area fill `rgba(231,76,60,0.1)` under the curve (x scaled to 550 max).
- **Critical marker:** amber `#f39c12` circle radius 8 with `#e67e22` outline at alert 501 (just above the curve at ~25%), labeled bold 13px `#e67e22`: "Alert #501: REAL THREAT (missed)".

## Indicator of Compromise decay

**Indicator of Compromise decay**

- **Staleness:** IOC says malicious IP 1.2.3.4 → attacker moves to 5.6.7.8 next week → the IOC is stale.
- **Half-lives:** IP/domain IOCs decay in 3-7 days, and one recompile gives a hash IOC a new hash.
- **Result:** Hunting with last month's IOCs is hunting ghosts.

### Visualization (canvas `canvas6`, 720×200)

Exponential half-life decay curves for four IOC types over 35 days.

- **Background:** `#f8f8f0`.
- **Axes:** L-shape `#7f8c8d` width 1.5; plot from x=80 to x=680, y=30 to y=165. Y label (rotated 14px `#2c3e50`): "IOC Relevance %", ticks 0-100% every 25% with `#ecf0f1` gridlines; X label: "Days Since IOC Published", ticks 0d, 7d, 14d, 21d, 28d, 35d.
- **Curves (relevance = 100 × 0.5^(days/halfLife), plotted 0-35 days in 0.5-day steps, line width 2.5):**
  - Hash IOCs — half-life 1 day — `#e74c3c`
  - IP IOCs — half-life 5 days — `#e67e22`
  - Domain IOCs — half-life 7 days — `#f1c40f`
  - TTP-based — half-life 60 days — `#27ae60`
- **Legend (13px `#2c3e50`, along the top of the plot):** colored line sample + name for each IOC type.
- **Stale zone:** `rgba(231,76,60,0.08)` band covering days 14-35, labeled bold 14px `rgba(231,76,60,0.5)` centered: "STALE ZONE".

## Regeneration instructions

- **Layout:** detail page in the domains-page style: h1 + `.subtitle`, then one `<h2>` per pitfall (1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px, margin 40px 0 15px), each followed by a full-width `.obj-table` with a single `<tr>`: left `<td>` (40%) holds `.obj-title` div (repeating the h2 text) + `<ul>` bullets, right `<td>` (60%, centered) holds the canvas. No thead, no nav, no badges, no cross-page links.
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; `ul` 0.9em `#333`, margin 8px 0 8px 20px; `strong` in `#1a5276`; `.obj-table td` border `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; even rows background `#fafcfe`; `.obj-title` 1.05em, weight 600, `#1a5276`; `.philosophy` callout style defined (background `#f0f4f8`, left border `4px solid #2980b9`) but unused on this page.
- **Canvas:** canvas tags declare `width="720" height="300"` but a `setupCanvas(id)` helper overrides the drawing surface to 720×200, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; default font set to 17px -apple-system. Each chart paints its own pale tinted full-canvas background (`#f0f4f8`, `#f8f4f0`, `#f4f8f0`, `#f0f0f8`, `#f8f4f8`, `#f8f8f0`).
- **Palette:** primary blue `#1a5276`, accent blues `#2980b9`/`#3498db`, green `#27ae60`, red `#e74c3c` (dark `#c0392b`), orange `#e67e22`, amber `#f39c12`, yellow `#f1c40f`, purple `#9b59b6`, grays `#7f8c8d`/`#2c3e50`/`#bdc3c7`/`#ddd`/`#ecf0f1`.
