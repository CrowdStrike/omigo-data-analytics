# Domain Pitfalls: Employee Surveys

**Page type:** detail page (h2 section headings, each followed by a one-row two-column obj-table: text left 50%, canvas right 50%; no subtitle)
**HTML title tag:** Domain Pitfalls: Employee Surveys

## Social Desirability Bias

**Obj-title:** Social Desirability Bias

- Employees say what they think leadership wants to hear.
- "I'm highly engaged!" = safe answer; "I hate my job" = risky answer.
- Results skew positive; the delta between survey and reality = the fear factor.

### Visualization (canvas `canvas1`, 720×200 — declared 720×300 in markup, resized to 720×200 by the setup helper)

Paired bar chart comparing reported survey scores vs reality on a 1-5 scale.

- **Title (bold 17px `#1a5276`, at 200,20):** "Reported vs Reality (1-5 scale)".
- **Categories and values:** Engagement — reported 4.3, actual 3.1; Satisfaction — 4.1 vs 2.8; Trust in Mgmt — 4.0 vs 2.5; Work-Life Bal — 3.9 vs 2.9.
- **Bars:** per category (140px pitch starting x=80, baseline y=170, scale height 130 over 0-5), reported bar 30px wide in `#2980b9`, actual bar adjacent in `#e74c3c`.
- **Delta labels:** bold 12px `#c0392b` above each pair: "Δ1.2", "Δ1.3", "Δ1.5", "Δ1.0" (reported minus actual, one decimal).
- **Category labels:** 12px `#333` under the baseline.
- **Legend (right, at 580,40):** `#2980b9` square + "Reported"; `#e74c3c` square + "Reality" (13px `#333`).
- **Baseline:** thin `#bbb` line from x=60 to x=640 at y=170.

## Non-Response from Disengaged

**Obj-title:** Non-Response from Disengaged

- 40% response rate; the 60% who didn't respond are disproportionately: unhappy, checked out, looking elsewhere.
- Responding requires CARING enough to bother.
- Your data oversamples the engaged.

### Visualization (canvas `canvas2`, 720×200 — declared 720×300, resized by setup helper)

Pie chart plus horizontal-bar composition breakdown.

- **Title (bold 17px `#1a5276`):** "Who Responds vs Who Doesn't".
- **Pie (center 150,115, radius 70, white 2px slice borders):** 40% respondents in green `#27ae60`, 60% non-respondents in red `#e74c3c`; white bold 15px slice labels "40%" and "60%".
- **Right breakdown (starting x=310):** heading "Non-respondents are:" (14px `#333`), then horizontal bars (18px tall, width = pct × 3, 30px row pitch starting y=72) with 13px `#333` labels:
  - "Unhappy / frustrated (45%)" — `#e74c3c`.
  - "Checked out / apathetic (30%)" — `#e67e22`.
  - "Actively looking to leave (15%)" — `#c0392b`.
  - "Too busy (neutral) (10%)" — `#95a5a6`.
- **Takeaway (bold 13px `#1a5276` at 310,185):** "→ Your survey data only reflects the green slice".

## Likert Scale Compression

**Obj-title:** Likert Scale Compression

- 5-point scale: nobody uses 1 or 2 (too negative, feels risky).
- Actual range used: 3-5.
- Meaningful differences all live between 3.8 and 4.2, a delta of just 0.4 on the 5-point scale.
- That 0.4 delta looks trivial but is actually significant inside such a compressed response scale.

### Visualization (canvas `canvas3`, 720×200 — declared 720×300, resized by setup helper)

Bar chart of the actual response distribution across the 5 Likert points.

- **Title (bold 17px `#1a5276`):** "Actual Response Distribution on 5-Point Scale".
- **Bars (80px wide, 110px pitch starting x=70, baseline y=165, height scaled to max 50% over 110px, all with `#2980b9` 1px outline):** scale point 1 = 2% (fill `#fadbd8`), 2 = 5% (`#fadbd8`), 3 = 25% (`#f9e79f`), 4 = 45% (`#2980b9`), 5 = 23% (`#2980b9`).
- **Labels:** bold 14px `#333` percent above each bar; 13px scale-point number (1-5) below the baseline; thin `#bbb` axis line from x=50 to x=660.
- **Compressed-range bracket:** two vertical red `#e74c3c` dashed lines (dash 4/3, width 2) framing points 3-5 from y=35 down to the baseline; bold 12px `#e74c3c` annotations "Actual usable range" and "(3.8 vs 4.2 = BIG difference here)".
- **Dead zone label (over points 1-2):** 11px `#999` "Dead zone" / "(fear of using)".

## Survey Fatigue Over Time

**Obj-title:** Survey Fatigue Over Time

- Q1 survey: 80% response, thoughtful answers; Q2: 60% response, shorter answers; Q4: 35% response, random clicking.
- Longitudinal trends confounded by declining response quality.
- You can't tell if engagement dropped or just survey participation dropped.

### Visualization (canvas `canvas4`, 720×200 — declared 720×300, resized by setup helper)

Two-line chart of response rate and answer quality declining across quarters.

- **Title (bold 17px `#1a5276`):** "Response Rate & Quality Over Repeated Surveys".
- **Data (x = Q1, Q2, Q3, Q4):** Response Rate [80, 60, 45, 35] — solid `#2980b9` line, width 3, 5px dots. Answer Quality [90, 70, 45, 20] — dashed `#e74c3c` line (dash 6/4), width 3, 5px dots.
- **Axes:** x from 100 to 620, baseline y=170, top y=45; `#bbb` axis lines; y labels 0%, 25%, 50%, 75%, 100% in 12px `#666` with light `#eee` gridlines; quarter labels 14px `#333` under the baseline.
- **Legend (at ~460,50):** blue solid line swatch + "Response Rate"; red dashed line swatch + "Answer Quality" (13px `#333`).

## Anonymity Distrust

**Obj-title:** Anonymity Distrust

- "This survey is anonymous" — but I'm one of 3 people on this team.
- With demographic questions: the unique combination identifies me.
- Distrust of anonymity → dishonest answers → data is fiction; small teams can't have real anonymity.

### Visualization (canvas `canvas5`, 720×200 — declared 720×300, resized by setup helper)

Line chart: team size vs perceived anonymity, with an honesty-threshold danger zone.

- **Title (bold 17px `#1a5276`):** "Team Size vs Perceived Anonymity".
- **Data (label, belief-in-anonymity %):** 3 people 8%; 5 people 20%; 10 people 45%; 25 people 68%; 50 people 82%; 100+ 92%. Evenly spaced points from x=100 to x=640; baseline y=170, top y=50.
- **Curve:** solid `#2980b9` line, width 3; 6px dots with white 2px outline, colored red `#e74c3c` when anonymity < 50%, else green `#27ae60`; each point labeled with team size below the axis and its percent above the point (12px `#333`).
- **Threshold:** horizontal red `#e74c3c` dashed line (dash 5/5, width 1.5) at 50%, labeled "Honesty threshold" (12px `#e74c3c`, right end); the region below it shaded `rgba(231,76,60,0.07)`.
- **Rotated y-axis label:** "Belief in anonymity (%)" (12px `#666`).
- **Zone label:** bold 12px `#c0392b` "FICTION ZONE" near the bottom left of the shaded region.

## Action Gap

**Obj-title:** Action Gap

- Survey → results presented → no visible change → next survey: "why bother answering, nothing happens".
- Each survey without follow-through REDUCES future response quality.
- Surveying without acting = actively harming future data collection.

### Visualization (canvas `canvas6`, 720×200 — declared 720×300, resized by setup helper)

Paired-bar decline chart with "no action" arrows between survey cycles.

- **Title (bold 17px `#1a5276`):** "The Survey-Inaction Death Spiral".
- **Data (per survey: response rate %, trust in process %):** Survey 1 — 82, 85; Survey 2 — 65, 60; Survey 3 — 45, 35; Survey 4 — 28, 15; Survey 5 — 18, 5.
- **Bars:** at each of 5 evenly spaced x positions (x=80 to x=650, baseline y=170, top y=45), a 25px-wide response bar in `#2980b9` (left) and trust bar in `#e67e22` (right); percent value labels above each bar in matching colors (11px); survey labels 12px `#333` below the baseline; `#bbb` axis line.
- **Between consecutive surveys:** a horizontal `#c0392b` arrow (width 1.5) at y=150 with a 9px `#c0392b` label "no action" above it.
- **Legend (at 480,42):** `#2980b9` square + "Response Rate"; `#e67e22` square + "Trust in Process" (13px `#333`).

## Regeneration instructions

- **Layout:** standard domains detail page (139-style): h1 (no subtitle paragraph on this page), then per pitfall an unnumbered `<h2>` followed by a one-row `.obj-table` — left `<td>` (40%) with `.obj-title` + `<ul>` bullets, right `<td>` (60%, centered) with one `<canvas>`. No thead, no nav, no badges, no cross-page links.
- **Page CSS:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px solid `#2980b9` bottom border; `.subtitle` `#666` 1.05em (defined, unused); ul 0.9em `#333`; `strong` `#1a5276`; `.obj-table` cells border `1px solid #e0e0e0`, padding 20px 24px, even rows `#fafcfe`; `.obj-title` 1.05em weight 600 `#1a5276`; `.philosophy` callout style defined but unused.
- **Canvas:** markup declares `width="720" height="300"`; a shared `setupCanvas(id)` helper overrides to 720×200 CSS pixels and scales the backing store by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), default font 17px system sans-serif.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, dark red `#c0392b`, grays `#666`/`#333`/`#999`/`#95a5a6`.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
