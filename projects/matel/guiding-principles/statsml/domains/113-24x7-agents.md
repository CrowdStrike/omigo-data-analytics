# 24/7 AI Agents Destroying Time-Based Detection

**Page type:** detail page (two-column obj-table layout: text left 50%, canvas right 50%, one h2 + one-row table per pitfall)
**HTML title tag:** 113. 24/7 AI Agents Destroying Time-Based Detection

**Subtitle:** Always-on AI agents erase the day/night and weekday/weekend rhythms that anomaly detection was built on.

## Legitimate Agents Login at 2am

- AI agents operate continuously — "off-hours" no longer exist
- 2am login was once a strong anomaly signal; now it's normal agent behavior

**Example:** Before agents: 2am logins had 94% true-positive rate for compromise. After agents: same signal has 3% true-positive rate.

### Visualization (canvas `c1`, declared 720×300, drawn at 720×200)

24-hour login activity lines, before vs after agents, with shaded night hours, on a light blue background.

- **Background:** `#eaf2f8`.
- **Title (17px, `#1a5276`, at (175, 18)):** "Login Activity by Hour (Before vs After Agents)".
- **Before series (24 hourly points, blue `#2980b9`, width 2):** `[2, 1, 1, 1, 1, 3, 15, 45, 80, 90, 88, 85, 70, 82, 87, 85, 78, 50, 20, 10, 5, 3, 2, 2]`.
- **After series (24 points, red `#e74c3c`, width 2):** `[35, 32, 30, 28, 30, 35, 55, 70, 85, 88, 86, 84, 72, 80, 85, 83, 76, 55, 45, 40, 38, 36, 35, 34]`.
- **Geometry:** scale max 95 over 150px height, baseline y=185, points spaced 27px starting x=50.
- **Night shading:** two `rgba(44,62,80,0.08)` rectangles covering hours 0–4 and 20–23 (y=30–185), with 10px `#7f8c8d` label "Night hours" at (60, 42).
- **X labels (10px, `#1a5276`, every 3 hours at y=198):** 0:00, 3:00, 6:00, 9:00, 12:00, 15:00, 18:00, 21:00.
- **Legend (10px):** "Before agents" in `#2980b9` at (560, 45); "After agents" in `#e74c3c` at (560, 60).

## State-Sponsored Attacks Indistinguishable from Agents

- APT groups now operate during victim's night — looks like an agent
- No behavioral difference between legitimate automation and intrusion

**Example:** APT29 shifted operations to target's 1-5am window. Alert was auto-closed as "agent activity" — dwell time: 287 days.

### Visualization (canvas `c2`, declared 720×300, drawn at 720×200)

Horizontal bar chart of APT dwell times, on a light red background.

- **Background:** `#fdedec`.
- **Title (17px, `#1a5276`, at (195, 18)):** "APT Dwell Time: Mistaken for Agent Activity".
- **Data (red `#e74c3c` bars, 22px tall, spaced 32px starting y=45, starting x=80, width = days/300 × 500):** APT29 287 days, APT41 194 days, Lazarus 156 days, Turla 223 days, Fancy Bear 98 days.
- **Labels:** group name in 12px `#1a5276` at x=5 per row; white text inside each bar: "<days> days undetected" (e.g. "287 days undetected").
- **Caption (11px, `#7f8c8d`, at (300, 190)):** 'All initially classified as "normal agent behavior"'.

## "Unusual Hour" Detection Signal = Dead

- Time-of-day was one of the strongest anomaly features
- With 24/7 agents, every hour has legitimate traffic — signal destroyed

**Example:** Feature importance of "hour_of_access" in threat models dropped from 0.31 (top-3 feature) to 0.02 (noise) in 18 months.

### Visualization (canvas `c3`, declared 720×300, drawn at 720×200)

Declining area/line chart of feature importance over ten quarters with a noise-threshold line, on a light yellow background.

- **Background:** `#fef9e7`.
- **Title (17px, `#1a5276`, at (190, 18)):** 'Feature Importance: "hour_of_access" Over Time'.
- **Quarters (10px, `#1a5276`, at y=198):** Q1-23, Q2-23, Q3-23, Q4-23, Q1-24, Q2-24, Q3-24, Q4-24, Q1-25, Q2-25; spaced 68px starting x=60.
- **Importance values:** `[0.31, 0.29, 0.27, 0.22, 0.15, 0.10, 0.06, 0.04, 0.03, 0.02]`; scale max 0.35 over 155px height, baseline y=185.
- **Line:** orange `#f39c12`, width 2.5; area fill `rgba(243,156,18,0.2)`.
- **Threshold:** horizontal dashed red `#e74c3c` line (dash 4/4) at importance 0.05, labeled "Noise threshold" in 11px `#e74c3c` at the right end.

## Behavioral Baselines Assumed Human 9-5

- All anomaly detection models trained on human work patterns
- Models must be completely retrained — historical data is useless

**Example:** UEBA system trained 2018-2023 on human patterns. After agent deployment, false-positive rate went from 2% to 67%.

### Visualization (canvas `c4`, declared 720×300, drawn at 720×200)

Color-coded monthly bar chart of UEBA false-positive rate with a deployment marker, on a light green background.

- **Background:** `#eafaf1`.
- **Title (17px, `#1a5276`, at (180, 18)):** "UEBA False-Positive Rate After Agent Deployment".
- **Months (10px labels):** Jan–Oct, bars 40px wide spaced 65px starting x=60.
- **FP values:** `[2, 2, 3, 8, 22, 38, 52, 60, 65, 67]` percent; scale max 75 over 150px height, baseline y=185; percentage label above each bar.
- **Bar colors:** green `#27ae60` if < 10%, orange `#f39c12` if < 40%, else red `#e74c3c`.
- **Marker:** vertical dashed `#1a5276` line (dash 4/3) at the April bar (x=60+3·65) from y=30 to y=185, labeled "Agents deployed" in 11px `#1a5276`.

## Weekend/Holiday Patterns Meaningless

- Agents don't take weekends or holidays off
- Holiday-based detection rules fire continuously on agents

**Example:** "Christmas Day access" rule generated 14,000 alerts in 2025. All were legitimate agents. The one real breach was buried at alert #8,847.

### Visualization (canvas `c5`, declared 720×300, drawn at 720×200)

Weekly activity lines: human era with weekend dip vs flat agent era, on a light purple background.

- **Background:** `#f4ecf7`.
- **Title (17px, `#1a5276`, at (200, 18)):** "Weekly Activity Pattern: Human vs Agent Era".
- **Days (12px, `#1a5276`, at y=198):** Mon–Sun, spaced 90px starting x=80.
- **Human series (blue `#2980b9`, width 2.5):** `[85, 88, 87, 86, 78, 12, 8]`.
- **Agent-era series (red `#e74c3c`, width 2.5):** `[88, 89, 88, 87, 82, 75, 74]`.
- **Geometry:** scale max 95 over 150px height, baseline y=185.
- **Labels (12px):** "Human era (clear weekend dip)" in `#2980b9` at (350, 170); "Agent era (flat — no signal)" in `#e74c3c` at (350, 50).

## Impossible to Separate Agent from Attacker by TIME

- Temporal dimension is completely compromised as a detection axis
- Must shift to content/intent-based detection — much harder

**Example:** Red team exercise: security analysts could not distinguish agent traffic from simulated APT by timing alone — 0% accuracy (coin flip = 50%).

### Visualization (canvas `c6`, declared 720×300, drawn at 720×200)

ROC curve hugging the random diagonal, on a light blue background.

- **Background:** `#ebf5fb`.
- **Title (17px, `#1a5276`, at (175, 18)):** "ROC Curve: Temporal-Only Detection (Post-Agents)".
- **Random baseline:** dashed gray `#bdc3c7` diagonal (dash 4/4, width 1.5) from (80, 185) to (660, 35), labeled "Random baseline" in 12px `#bdc3c7` at (500, 80).
- **ROC curve:** red `#e74c3c`, width 2.5, through points (FPR, TPR): `[0,0], [0.1,0.12], [0.2,0.22], [0.3,0.33], [0.4,0.43], [0.5,0.52], [0.6,0.61], [0.7,0.72], [0.8,0.81], [0.9,0.91], [1,1]` — barely above the diagonal; plot maps FPR to x=80–660 and TPR to y=185–35.
- **Annotation (12px, `#e74c3c`, at (400, 130)):** "AUC = 0.51 (random!)".
- **Axis hints (11px, `#1a5276`):** "FPR →" at (620, 198), "TPR ↑" at (55, 35).

## Velocity-Based Detection Fails

- Agents legitimately do 1000 API calls/minute
- Rate-based anomaly detection can't distinguish agent from brute-force

**Example:** Legitimate CI/CD agent: 2,400 API calls/min. Credential-stuffing attack: 1,800 calls/min. Agent actually EXCEEDS attack velocity.

### Visualization (canvas `c7`, declared 720×300, drawn at 720×200)

Horizontal bar chart comparing API call velocities of agents vs attacks, on a light yellow background.

- **Background:** `#fef9e7`.
- **Title (17px, `#1a5276`, at (230, 18)):** "API Calls/Minute: Agents vs Attacks".
- **Data (bars 20px tall, spaced 27px starting y=38, starting x=200, width = value/5000 × 420):** Human user 5; Brute force 1,800 (red `#e74c3c`); Credential stuff 1,200 (red `#e74c3c`); CI/CD agent 2,400 (blue `#2980b9`); Data sync agent 3,100 (blue); Monitoring agent 4,500 (blue). Non-attack rows are blue; attacks red.
- **Labels (11px, `#1a5276`):** item name at x=80 per row; formatted value (e.g. "2,400") just right of each bar.
- **Caption (11px, `#7f8c8d`, at (180, 195)):** "Agents exceed attack velocity — rate limiting breaks legitimate use".

## Entire Temporal Dimension of Anomaly Detection is Obsolete

- Time, velocity, frequency, periodicity — all traditional temporal signals are dead
- Security must rebuild on non-temporal features: intent, data sensitivity, lateral movement patterns

**Example:** Audit of Fortune 500 SIEM rules: 73% rely on temporal features. Post-agent deployment, those rules have a combined 89% false-positive rate.

### Visualization (canvas `c8`, declared 720×300, drawn at 720×200)

Paired bar chart: rule-category prevalence vs post-agent false-positive rate, on a light red background.

- **Background:** `#fdedec`.
- **Title (17px, `#1a5276`, at (150, 18)):** "SIEM Rule Categories & Post-Agent False-Positive Rates".
- **Categories (10px labels at y=125, groups spaced 170px starting x=60):** Time-based (73% of rules, 89% FP), Velocity (15%, 72% FP), Geo+Time (8%, 65% FP), Content/Intent (4%, 8% FP).
- **Bars:** per category, a blue `#2980b9` 50px-wide bar for % of rules and a red `#e74c3c` 50px bar (offset 55px) for FP rate; height = percent/100 × 80, baseline y=110; value labels above bars ("73%", "89% FP", etc.).
- **Legend (11px, at x=500):** blue swatch "% of rules" (y≈151), red swatch "False-positive rate" (y≈169).
- **Takeaway (11px, `#27ae60`, at (200, 185)):** "Only content/intent-based rules survive".

## Regeneration instructions

- **Layout:** standard detail-page structure — one `<h2>` per pitfall (1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px), each followed by a one-row `.obj-table`: left `<td>` (40%) holds `.obj-title` (same text as h2) + two-bullet list + bold-labeled Example paragraph; right `<td>` (60%, centered) holds the canvas. Even rows background `#fafcfe`; cell borders `1px solid #e0e0e0`, padding 20px 24px.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; ul 0.9em `#333`; p 0.95em `#333`; `strong` in `#1a5276`; `.obj-title` 1.05em weight 600 `#1a5276`. `.philosophy` callout style defined (background `#f0f4f8`, left border `4px solid #2980b9`) but unused. No nav bar, no back/home links.
- **Canvas:** HTML attributes declare `width="720" height="300"`, but the shared `setupCanvas(id)` helper renders each at 720×200 CSS pixels — it sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Titles 17px system; labels 10–12px. Each chart has a distinct pastel full-canvas background tint as noted.
- **Palette:** primary blue `#1a5276`/`#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#f39c12`, grays `#7f8c8d`/`#bdc3c7`.
- Card links elsewhere point to this page as `domains/113-24x7-agents.html` in regenerated HTML.
