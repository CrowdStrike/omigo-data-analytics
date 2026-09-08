# Spaceship Launch / Rocket Data

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one h2 + one-row table per pitfall)
**HTML title tag:** 104. Spaceship Launch / Rocket Data

**Subtitle:** One-shot systems where data decisions are irreversible, bandwidth is limited, and ground truth only exists after you've already committed.

## One-Shot System: No Second Chance If Data Is Wrong

**GO/NO-GO on Incomplete Data, Where "Wrong" = Catastrophic**

- **Irreversible:** Unlike ML, where you can retrain, a rocket launch is IRREVERSIBLE once engines ignite.
- **Challenger 1986:** Engineers held O-ring cold-temperature data, but it was AMBIGUOUS at the observed 36°F.
- **No precedent:** No prior launch had ever flown that cold, so the data could not settle the question.
- **The cost:** They launched on that ambiguous reading, and the decision cost the crew of 7 their lives.
- **The challenge:** A GO/NO-GO call must be made on INCOMPLETE data, and here "wrong" = catastrophic.

### Visualization (canvas `canvas1`, 720×200)

Timeline diagram with an irreversible decision point.

- **Background:** light gray `#f8f9fa` fill over full 720×200.
- **Title (bold 17px, `#1a5276`, left at x=50, y=30):** "Irreversible Decision Point". Subtitle below (14px, `#666`, y=50): "GO/NO-GO with incomplete data — once past T-0, no undo".
- **Timeline:** horizontal line `#555` width 2 from (50,120) to (670,120) with arrowhead at right end.
- **Tick labels (14px `#333`, centered, y=145) at x positions:** T-60min (100), T-30min (200), T-10min (300), T-0 (420), T+10s (530), T+60s (630); tick marks from y=115 to y=125 at each x.
- **Reversible zone:** rect fill `rgba(46,204,113,0.2)` from (50,70) size 370×40; centered green `#27ae60` label (15px) "REVERSIBLE: Can still abort" at (235,95).
- **Irreversible zone:** rect fill `rgba(231,76,60,0.2)` from (420,70) size 250×40; red `#e74c3c` label "IRREVERSIBLE: Committed" at (545,95).
- **Decision point marker:** filled red `#e74c3c` circle radius 8 at (420,120) with white bold 12px "!" inside.
- **Data confidence line:** dashed orange `#f39c12` (dash 5/3, width 2) through points (50,170)→(200,165)→(300,168)→(420,170); orange 13px label below at (50,185): "Data confidence (never reaches 100%)".

## Telemetry Bandwidth Limits During Ascent

**1000+ Sensors, 1-5 Mbps Downlink — Priorities Frozen Before Launch**

- **The bottleneck:** The rocket carries 1000+ sensors, but the telemetry link moves only ~1-5 Mbps to ground.
- **Pre-committed priorities:** Which sensors get real-time downlink is decided BEFORE launch, never in flight.
- **Guessing ahead:** That priority list encodes what you THINK will matter, not what actually goes wrong.
- **Blind spots:** An anomaly in a low-priority sensor stays invisible for the whole duration of the flight.
- **Late or never:** It surfaces post-flight if the vehicle survives, and not at all if it doesn't.

### Visualization (canvas `canvas2`, 720×200)

Stacked horizontal bar showing bandwidth allocation.

- **Background:** `#f8f9fa`.
- **Title (bold 17px `#1a5276` at 50,25):** "Telemetry Bandwidth Allocation (5 Mbps total)".
- **Stacked bar:** starts at (50,50), total width 500, height 40, white 1px separators between segments. Segments in order: Navigation 30% `#2980b9`, Propulsion 25% `#27ae60`, Structural 20% `#f39c12`, Thermal 12% `#e74c3c`, Other (100+ sensors) 13% `#95a5a6`.
- **Legend:** below bar starting y=110, 14px, two rows of 3 (x = 50 + (i%3)*220), 14×14 color swatch plus label with percentage, e.g. "Navigation (30%)".
- **Annotation (right side, red `#e74c3c`):** 17px "1000+ sensors" at (580,65); 14px "Only ~50 get" (580,85) and "real-time downlink" (580,102); short red arrow (width 1.5) from (575,70) to (545,70) pointing at the "Other" segment.
- **Bottom note (13px `#666` at 50,185):** "Anomaly in low-priority sensor: invisible until post-flight (if vehicle survives)".

## Sensor Saturation at Extremes

**Sensors Are Least Reliable at the Most Critical Moment**

- **Saturated readings:** A vibration sensor rated to 50G still reports "50G" when actual vibration hits 80G.
- **Worst timing:** That clipping happens at max-Q, exactly the moment the reading matters most.
- **Ambiguity:** Ground sees "vibration at limit" and can't tell 50G (okay) from 200G (vehicle breaking apart).
- **Everywhere at once:** The same saturation hits temperature in the thrust chamber and pressure in the tanks.
- **Staging too:** Acceleration sensors clip during staging events, the other high-transient phase of flight.

### Visualization (canvas `canvas3`, 720×200)

Two-curve chart: actual vibration vs saturated sensor reading.

- **Background:** `#f8f9fa`.
- **Title (bold 17px `#1a5276` at 50,25):** "Sensor Saturation: What Ground Sees vs. Reality".
- **Axes:** L-shaped `#555` width 1.5 from (80,45) down to (80,170) across to (650,170). Y-axis labels (13px `#333`, right-aligned at x=75): 200G (y=55), 100G (y=90), 50G (y=120), 0G (y=170).
- **Sensor limit line:** dashed red `#e74c3c` (dash 4/4, width 1) horizontal at y=120 from x=80 to 650, labeled "Sensor max (50G)" in red at (555,115).
- **Actual vibration curve:** blue `#2980b9` width 2.5, quadratic curve from (100,165) rising through (280,130) to a peak near (400,55), falling through (500,110) to (630,155).
- **Sensor reading curve:** orange `#f39c12` width 2.5, same start/end but clipped flat at y=120 (the 50G limit) between roughly x=280 and x=510.
- **Legend (14px, y≈185):** blue swatch + "Actual vibration"; orange swatch + "Sensor reading (saturated)".
- **Peak annotation (bold 13px `#1a5276`, centered at x=400):** "Max-Q" (y=48) / "(blind zone)" (y=63).

## Countdown Abort Decision from Incomplete Data

**30 Seconds to Classify: Sensor Failure, Noise, or Real Problem?**

- **No time to diagnose:** At T-minus 30 seconds an anomalous reading arrives with no time to investigate it.
- **Three readings:** It may be sensor failure (benign), a real problem (abort!), or transient noise (ignore).
- **Cost of aborting:** Aborting on a false alarm, historically a 2% rate, wastes roughly $50M of launch cost.
- **Cost of flying:** Launching with a real problem loses a $2B vehicle and possibly the lives aboard.
- **The discipline:** This is decision theory under extreme time pressure with irreducibly uncertain data.

### Visualization (canvas `canvas4`, 720×200)

Decision tree diagram with outcome costs.

- **Background:** `#f8f9fa`.
- **Title (bold 17px `#1a5276` at 50,25):** "T-30s Abort Decision Tree". Timer note top-right (bold 15px red `#e74c3c`, right-aligned at 670,25): "30 seconds to decide".
- **Root node:** orange `#f39c12` rect (280,42) 160×30 with white bold 14px centered text "Anomalous Reading".
- **Branches:** three `#555` width-1.5 lines from root to child nodes.
- **Child nodes (white bold 13px centered labels):** green `#27ae60` rect (90,110) 140×28 "ABORT"; gray `#95a5a6` rect (295,110) 130×28 "WAIT (no time)"; red `#e74c3c` rect (490,110) 140×28 "CONTINUE".
- **Left outcomes (13px, centered at x=130):** `#333` "If false alarm:" (y=155); orange "$50M wasted (98%)" (y=170); green "If real: lives saved (2%)" (y=185).
- **Right outcomes (centered at x=590):** `#333` "If false alarm:" (y=155); green "$50M saved (98%)" (y=170); red "If real: $2B + lives (0.01%)" (y=185).

## Post-Launch Debris Tracking (Space Situational Awareness)

**Collision Probability Computed From Two Uncertain Positions**

- **Barely detectable:** The radar cross-section of a 1cm bolt sits right at the edge of what radar can see.
- **The catalog:** 30,000+ orbital objects are tracked, each carrying roughly ~5m of position uncertainty.
- **Uncertain probabilities:** A "1-in-10,000" collision estimate inherits uncertainty in BOTH positions.
- **Wide range:** The true risk could really be 1-in-100 (act now) or 1-in-1,000,000 (safely ignorable).
- **No resolution:** You can't tell which — input uncertainty swamps the estimate it was meant to produce.

### Visualization (canvas `canvas5`, 720×200)

Dark space scene with two overlapping uncertainty circles.

- **Background:** dark navy `#0a1628` full canvas; 40 tiny white stars (0.5px radius dots) at fixed scattered positions.
- **Title (bold 17px, light `#ecf0f1`, at 50,25):** "Orbital Debris Tracking — Position Uncertainty".
- **Satellite A:** green `#2ecc71` dashed circle (dash 3/3, width 1.5) radius 35 centered (250,110) with solid green 4px center dot; labels (13px, centered): "Satellite A" (250,155), "(±5m)" (250,170).
- **Debris:** red `#e74c3c` dashed circle radius 40 centered (310,100) with solid red 4px center dot; labels: "Debris" (340,148), "(±15m)" (340,163).
- **Overlap zone:** yellow `rgba(241,196,15,0.15)` filled circle radius 20 at (280,105) with dashed `#f1c40f` outline (dash 2/2).
- **Right-side text (left-aligned at x=430):** light `#ecf0f1` 15px "Computed collision prob: 1/10,000" (y=70); orange `#f39c12` "But with position uncertainty:" (y=95), "  Could be 1/100 (danger!)" (y=115), "  Could be 1/1,000,000 (safe)" (y=135); bold 14px red "30,000+ objects tracked" (y=165); gray `#95a5a6` 13px "1cm bolt = barely detectable by radar" (y=185).

## Re-Entry Communications Blackout

**3-8 Minutes of Zero Telemetry at the Most Dangerous Phase**

- **Plasma gap:** Re-entry plasma blocks radio signals for 3-8 minutes, at the most dangerous phase of all.
- **Zero telemetry:** Ground receives NOTHING for that entire window, no partial or degraded stream.
- **Unknowable state:** The vehicle is either fine (can't confirm) or disintegrating (can't detect in time).
- **Predict, don't observe:** All pre-blackout data must be stretched to predict the post-blackout state.
- **Worst-moment gap:** Anything that goes wrong DURING blackout leaves no data behind at all.

### Visualization (canvas `canvas6`, 720×200)

Timeline with a blackout gap in the telemetry signal.

- **Background:** `#f8f9fa`.
- **Title (bold 17px `#1a5276` at 50,25):** "Re-Entry Communications Blackout".
- **Timeline:** horizontal `#555` width 2 line at y=100 from x=50 to 670.
- **Pre-blackout signal:** blue `#2980b9` width 2 sine wave (amplitude 10, centered y=80) from x=50 to 250; blue centered label "Signal OK" at (150,145).
- **Blackout zone:** dark rect `rgba(44,62,80,0.85)` from (250,55) size 200×90; centered text: bold 16px red `#e74c3c` "BLACKOUT" (350,85); light `#ecf0f1` 13px "3-8 minutes" (350,105), "ZERO telemetry" (350,120), "Plasma barrier" (350,135).
- **Post-blackout signal:** green `#27ae60` dashed sine wave (dash 3/3) from x=450 to 650, centered y=80; green label "Signal recovered?" at (550,145); two bold 18px red question marks at (490,80) and (520,90).
- **Bottom note (13px `#666` at 50,180):** "Most dangerous phase = zero data. If vehicle disintegrates during blackout: no record exists."

## Ground Truth Only Exists After Recovery

**Committing the Mission on Proxy Measurements**

- **Delayed truth:** Whether the payload deployed and the orientation was right is only KNOWN after recovery.
- **Thermal too:** Whether thermal protection actually worked waits on payload activation hours or days later.
- **Proxy measurements:** During flight, telemetry SUGGESTS things are fine, but it is indirect measurement.
- **No mid-flight check:** Direct confirmation needs physical inspection or an end-to-end functional test.
- **Too late to matter:** Neither can happen until the mission is essentially over and already committed.

### Visualization (canvas `canvas7`, 720×200)

Mission-phase timeline with proxy markers and a single ground-truth point.

- **Background:** `#f8f9fa`.
- **Title (bold 17px `#1a5276` at 50,25):** "Ground Truth Only After Recovery".
- **Timeline:** `#555` width 2 line at y=120 from x=50 to 670.
- **Phase blocks (30% alpha fills, y=95, height 50, with centered 13px labels at y=135 in the same color):** Launch (x=50, w=150, `#3498db`), Ascent (200, 130, `#2980b9`), Orbit (330, 120, `#1a5276`), Deploy (450, 100, `#16a085`), Recovery (550, 120, `#27ae60`).
- **Proxy indicators:** orange `#f39c12` 13px "Proxy" labels at x = 125, 265, 390, 500 (y=80), each with a small downward orange arrow (from y=83 to y=93).
- **Ground truth marker:** bold 14px green `#27ae60` "GROUND TRUTH" centered at (610,80), with a short green line down to the timeline and a 3px green dot at (610,75).
- **Certainty bars (height 6, y=55):** orange bars at (50,w=80), (210,60), (350,50), (470,40); green bar at (580,90); tiny 11px `#999` label "Certainty level:" at (50,50).
- **Bottom annotation (13px `#666` at 50,175):** "All decisions during flight based on INDIRECT measurements. Direct confirmation only possible after mission ends."

## Test Data ≠ Flight Data (Every Flight Is Unique)

**50 Ground Tests Can't Validate Flight #1**

- **Controlled vs unique:** Ground testing is repeatable with known inputs; every real flight is one of a kind.
- **What varies:** Weather, vibration profile, thermal environment, and trajectory perturbations differ each time.
- **Design ≠ instance:** Testing validates the DESIGN but can't validate THIS SPECIFIC flight condition.
- **Surprise on flight #1:** Anomalies that never appeared across 50 ground tests can still appear on flight #1.
- **Tiny n:** Flight-data sample size is often n=1-5, far too small to characterize anything statistically.

### Visualization (canvas `canvas8`, 720×200)

Side-by-side comparison boxes: ground testing vs actual flight.

- **Background:** `#f8f9fa`.
- **Title (bold 17px `#1a5276` at 50,25):** "Test Data vs. Flight Data: Sample Size Problem".
- **Left box:** blue `#2980b9` — 15%-alpha fill and 1.5px stroke rect (50,50) 280×120. Centered bold 14px blue header "GROUND TESTING" (190,68); 13px `#333` lines: "n = 50+ repeatable tests" (190,88), "Controlled environment" (190,105), "Known inputs" (190,122), "Validates DESIGN" (190,139). Row of 50 small blue 3px dots along y=155 (10 per group, spaced 26px starting x=70).
- **Right box:** red `#e74c3c` — 15%-alpha fill and 1.5px stroke rect (380,50) 280×120. Centered bold 14px red header "ACTUAL FLIGHT" (520,68); 13px `#333` lines: "n = 1 (this specific flight)" (520,88), "Unique weather + vibration" (520,105), "Novel condition combinations" (520,122), "Can't validate THIS flight" (520,139). Single red 6px dot at (520,155) with white bold "1" inside.
- **Between boxes:** bold 28px `#555` "≠" centered at (345,115).
- **Bottom note (13px `#666` at 50,190):** "Anomaly absent in 50 ground tests can appear on flight #1 — the condition combination is unique and unrepeatable."

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table` layout — one `h2` section heading per pitfall, followed by a full-width single-row table: left `<td>` (40%) holds `.obj-title` div + a `<ul>` of bullets, right `<td>` (60%, centered) holds one `<canvas>`. Even rows have background `#fafcfe`. Cell borders `1px solid #e0e0e0`, padding 20px 24px.
- **Page CSS:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6. h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px `#2980b9` bottom border and 8px padding-bottom; subtitle `#666` 1.05em; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` in `#1a5276`; ul 0.9em `#333`. `.philosophy` callout style defined (background `#f0f4f8`, left border 4px `#2980b9`) but unused. Canvas CSS: display block, width 720px, height 200px, margin 0 auto. No nav bar, no back/home links.
- **Canvas:** all charts drawn at 720×200 logical size via a shared `setupCanvas(id)` helper that sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; default font 17px system sans.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#f39c12`, gray `#95a5a6`/`#555`/`#666`; canvas backgrounds `#f8f9fa` (dark space scene uses `#0a1628`).
- Note: in regenerated HTML, any card/page links use `.html` extensions (this page has none).
