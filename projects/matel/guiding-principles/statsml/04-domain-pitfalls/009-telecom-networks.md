# Telecom Domain: Data Pitfalls

**Page type:** detail page (one h2 per pitfall, each followed by a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** Telecom Domain - Data Pitfalls in Statistical ML

**Subtitle:** Contagious churn, ambiguous labels, shifting network topology, and billion-event scale break standard subscriber models.

## Network Effects in Churn

**Obj-title:** Network Effects in Churn

- Churn is contagious - when one person leaves, their contacts are more likely to follow
- Traditional churn models treat each subscriber independently (IID assumption)
- Social graph structure means one churner can trigger a cascade of 3-5 connected churners
- Ignoring network effects underestimates churn risk by 20-40% in social clusters
- Retention offers to "influencer" nodes have outsized ROI due to cascade prevention

**Example:** A family plan holder (4 lines) churns. Within 60 days, 3 colleagues they frequently called also churn - they were the social anchor keeping those contacts on-network. The churn model scored those colleagues as "low risk" because their individual features looked fine.

### Visualization (canvas `canvas1`, 720×240)

Social graph diagram (left) plus vertical cascade timeline (right).

- **Title (17px, `#1a5276`, centered at x=360, y=18):** "Churn Cascade in Social Graph".
- **Graph nodes** (circles, white 2px stroke, white 13px label centered inside):
  - A at (200,120), churned trigger node — radius 22, fill `#922b21`
  - B (130,60), C (130,180), D (270,60) — churned, radius 16, fill `#c0392b`
  - E (60,120), F (270,180), G (340,120), H (60,50), I (340,50) — active, radius 16, fill `#27ae60`
- **Edges** (index pairs): A–B, A–C, A–D, A–G; B–E, B–H; C–E, C–F; D–F, D–I; E–H; G–I. Edges between two churned nodes drawn `#c0392b` width 2.5; all others `#bdc3c7` width 1.5.
- **Timeline:** vertical `#333` line at x=480 from y=30 to y=220 with arrowhead at bottom. Event dots (radius 5) with 13px text at x=495:
  - y=50: "Day 0: A churns" in `#922b21`
  - y=95: "Day 15: B churns (called A daily)" in `#c0392b`
  - y=135: "Day 28: D churns (A was on same plan)" in `#c0392b`
  - y=175: "Day 42: C churns (lost 2 contacts)" in `#c0392b`
  - y=210: "E, F at risk (weakened network)" in `#e67e22`
- **Legend (12px, y=230):** "● Churned" in `#c0392b` at x=60, "● Active" in `#27ae60` at x=150.

## Churn Definition Ambiguity

**Obj-title:** Churn Definition Ambiguity

- Postpaid churn is clear (contract cancellation) but prepaid churn has no explicit signal
- Is 30 days without recharge "churned"? What about seasonal workers or travelers?
- Different churn definitions produce completely different model performance
- A user inactive for 45 days might recharge on day 46 - false positive if labeled churned at 30
- The definition boundary creates artificial label noise near the threshold

**Example:** A prepaid operator defines churn as "no recharge in 30 days." Analysis shows 15% of "churned" users recharge within the next 30 days. Extending the window to 60 days reduces false churn labels but delays intervention. There is no perfect threshold - it's a business decision masquerading as ground truth.

### Visualization (canvas `canvas2`, 720×240)

Three horizontal recharge timelines (day 0–170 mapped from x=100 to x=680) with threshold markers.

- **Users** (thin `#999` horizontal axis per user; recharge events drawn as filled dots radius 5 plus vertical tick ±8px in the user color; right-aligned 13px name label at x=95):
  - "User 1: True Churn", y=55, color `#c0392b`, recharge days `[5, 20, 38, 55, 72]` (stops at day 72)
  - "User 2: Vacation", y=120, color `#e67e22`, recharge days `[5, 22, 40, 58, 75, 120, 135]` (gap then returns)
  - "User 3: Seasonal Worker", y=185, color `#2980b9`, recharge days `[5, 15, 25, 35, 45, 130, 140, 150, 160]` (pattern breaks)
- **30-day rule marker:** vertical dashed `#c0392b` line (dash 5/3, width 2) at day 102 (30 days after User 1's last recharge), from y=25 to y=215; 12px centered labels "30-day rule" above (y=22) and ""churned"" below (y=228).
- **60-day rule marker:** vertical dashed `#8e44ad` line (dash 3/3, width 2) at day 132, y=25 to y=215; label "60-day rule" at y=22.
- **Annotations (11px, left-aligned just right of the 30-day line):**
  - "✓ Correctly labeled churned" in `#c0392b` at y=55
  - "✗ False churn! Returns day 120" in `#e67e22` at y=120
  - "✗ False churn! Seasonal pattern" in `#2980b9` at y=185
- **Legend (17px, `#1a5276`, left-aligned at x=100, y=228):** "● = Recharge Event".
- **Day scale (11px, `#555`, y=240):** "Day 0", "Day 30", "Day 60", "Day 90", "Day 120", "Day 150" centered at their day positions.

## Topology Changes Invalidate Model

**Obj-title:** Topology Changes Invalidate Model

- Adding a new cell tower redistributes traffic across the entire local area
- All location-based features shift overnight - model inputs become stale
- A "high usage" cell that was congested becomes "normal" after capacity expansion
- Network topology changes happen monthly - models need constant retraining
- Feature distributions shift non-uniformly: nearby cells affected more than distant ones

**Example:** A new tower is activated in a dense urban area. Traffic on 6 neighboring cells drops 30-50% overnight. The congestion-based churn model now scores those cells as "low risk" - but the subscribers haven't changed behavior, only the infrastructure did.

### Visualization (canvas `canvas3`, 720×240)

Before/after tower map comparison with load percentages.

- **Section titles (17px, `#1a5276`, centered):** "Before (congested)" at (200,20); "After (redistributed)" at (540,20).
- **Before towers** (circle radius 15, load% label 11px `#333` below at y+30, white label inside; translucent coverage halo radius 20+(load/100)×15+15 — fill `rgba(192,57,43,0.15)` if load>90 else `rgba(41,128,185,0.1)`; tower fill `#c0392b` if load>90 else `#e67e22`):
  - T1 (100,120) 95%, T2 (200,70) 88%, T3 (200,170) 92%, T4 (300,120) 85%
- **After towers** (halo `rgba(39,174,96,0.2)` for the new tower else `rgba(41,128,185,0.1)`; fill `#27ae60` for new, `#3498db` otherwise):
  - T1 (440,120) 62%, T2 (540,70) 55%, T3 (540,170) 58%, T4 (640,120) 52%, NEW (540,120) 70%
- **Arrow:** `#333` horizontal arrow from (340,120) to (390,120) with 12px `#555` two-line label "New tower" / "added" centered at x=365.
- **Load-change labels (11px, `#27ae60`, left-aligned):** "-33%" at (440,145), "-33%" at (540,95), "-34%" at (540,200), "-33%" at (640,145).
- **Bottom annotation (13px, `#c0392b`, centered at (360,230)):** "All features shift overnight - model trained on "before" is now invalid".

## CDR Does Not Equal Behavior

**Obj-title:** CDR Does Not Equal Behavior

- Call Detail Records show WHAT happened (duration, time, destination) but not WHY
- Same CDR pattern: scammer making 200 short calls vs salesperson vs automated dialer
- Without context, CDR-based fraud/churn models have high false positive rates
- Behavior shifted to OTT apps (WhatsApp, Telegram) makes CDRs increasingly incomplete
- CDR captures voice/SMS but misses the dominant communication channel for most users

**Example:** Three users each make 150+ calls/day, average 45 seconds each, to unique numbers. CDR patterns are identical. One is a telemarketer (legitimate), one is a scammer (fraud), one is an emergency dispatcher (essential service). CDR alone cannot distinguish them.

### Visualization (canvas `canvas4`, 720×240)

Three persona circles connected by dashed lines to one shared CDR metrics box.

- **Title (17px, `#1a5276`, centered at (360,22)):** "Identical CDR Patterns - Different Intent".
- **Profiles** (circle radius 28 at y=70, fill = profile color + "22" alpha suffix, 3px stroke in profile color; 24px emoji icon inside at y=78; 15px name below at y=115):
  - "Scammer" ⚠ `#c0392b` at x=130
  - "Salesperson" 💼 `#27ae60` at x=360
  - "Family Hub" 👨‍👩‍👧 `#2980b9` at x=590
- **CDR box:** rect (180,135) 360×85, stroke `#2980b9` width 2, fill `#eaf2f8`. Header (14px, `#1a5276`, centered at (360,153)): "CDR Record (identical for all three)". Metrics in two columns (13px `#333`, starting x=195 and x=375, rows y=172 and y=192):
  - "Calls/day: 150+", "Avg duration: 45 sec" (left column)
  - "Unique numbers: 140+", "Peak hours: 9am-6pm" (right column)
- **Connectors:** dashed (3/3) 1.5px lines in each profile color from (profile x, 120) to the box top edge (x=270, 450, or 360 respectively, y=135).
- **Bottom annotation (14px, `#c0392b`, centered at (360,235)):** "CDR cannot distinguish intent - context lives outside the data".

## Massive Scale Constraints

**Obj-title:** Massive Scale Constraints

- 100M subscribers x 100 events/day = 10 billion events daily
- Algorithms that work on 1M rows may be infeasible at 10B (O(n²) is dead)
- Even O(n log n) at 10B scale requires careful engineering
- Real-time scoring must happen in <10ms per subscriber for operational use
- Feature engineering on full CDR history is often replaced by approximate summaries

**Example:** A graph-based churn model works beautifully on a 500K subscriber test set (takes 2 hours). Scaling to 100M subscribers would take 40,000 hours (4.5 years). The team must switch to approximate neighborhood sampling, losing the very graph structure that made the model work.

### Visualization (canvas `canvas5`, 720×240)

Horizontal bar chart of compute time vs scale.

- **Title (17px, `#1a5276`, centered at (400,20)):** "Compute Time vs Scale (O(n²) algorithm)".
- **Axes:** `#333` 1px L-shaped axes — horizontal at y=190 from x=100 to x=700, vertical at x=100 from y=190 up to y=20.
- **Bars** (height 30, starting x=110, rows begin y=40 stepping 42; bar width capped at 580px; feasible bars `#27ae60`, infeasible `#c0392b`; row label right-aligned 13px `#333` at x=95; stats 12px, white inside bar when width>150 else `#333` after the bar):
  - "Test Set" — bar 30px, "500K subs | 2 hours" (feasible, green)
  - "Pilot" — bar 80px, "5M subs | 20 hours" (feasible, green)
  - "Region" — bar 200px, "25M subs | 2.5B/day | 100 hours" (infeasible, red)
  - "Full Network" — bar 580px (capped from 600), "100M subs | 10B/day | 40,000 hrs" (infeasible, red)
- **Feasibility boundary:** dashed `#e67e22` horizontal line (dash 5/3, width 2) between the second and third bars (y ≈ 118), right-aligned 13px label "Feasibility boundary" at x=695 just above it.
- **Annotations (centered at x=400):** 14px `#555` at y=218: "100M × 100 events/day = 10,000,000,000 daily events"; 12px `#c0392b` at y=236: "200x subscribers → 40,000x compute (quadratic scaling)".

## Regeneration instructions

- **Layout:** standard detail page. h1 + `.subtitle`, then one `<h2>` per pitfall followed by an `.obj-table` (full-width, border-collapse) with a single `<tr>`: left `<td>` (40%) holds `.obj-title`, bullet `<ul>`, and an `.example` box; right `<td>` (60%, centered) holds the canvas. Even rows background `#fafcfe`.
- **Example box:** `.example` — background `#eaf2f8`, padding 10px, border-radius 5px, italic 0.9em, with a leading `<strong>Example:</strong>` label.
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 1.05em; cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` `#1a5276`; `.philosophy` callout style available (background `#f0f4f8`, left border 4px `#2980b9`) though unused on this page. No nav bar, no back/home links.
- **Canvases:** each 720×240 intrinsic; each chart IIFE sets backing store to rendered width × dpr via `window.devicePixelRatio`, and calls `ctx.scale` so drawing stays in logical coordinates. Base chart font 17px -apple-system.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`/`#3498db`, green `#27ae60`, red `#e74c3c`/dark red `#c0392b`/`#922b21`, orange `#e67e22`, purple `#8e44ad`, gray text `#333`/`#555`/`#666`.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
