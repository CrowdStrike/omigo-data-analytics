# Collection Frequency — When Timing Creates Blind Spots

**Page type:** detail page, metric-testing template (white background; one two-column obj-table per scenario: text left 40%, canvas right 60%; obj-title heading; canvases 720×200 with devicePixelRatio scaling and redraw on resize)
**HTML title tag:** Collection Frequency — When Timing Creates Blind Spots

**Subtitle:** Real-world cases where the collection frequency was wrong for the decision speed, causing preventable damage.

## Fraud detection: Daily batch vs real-time

Fraudster drains $50K in 2 hours. Daily batch at midnight: detects 22 hours too late. The money is gone. Real-time detection would have caught it within seconds of the first anomalous transaction.

### Visualization (canvas `canvas1`, 720×200)

Timeline diagram of a 24-hour day showing a fraud window and a late detection point.

- **Background:** white fill over full 720×200.
- **Timeline:** horizontal line `#333` (width 2) from x=50 to x=680 at y=120.
- **Hour markers:** 13 ticks (5px above/below the line) evenly spaced across 630px, labeled in gray `#666` 12px: `12am, 2am, 4am, 6am, 8am, 10am, 12pm, 2pm, 4pm, 6pm, 8pm, 10pm, 12am` (labels at y=140).
- **Fraud window:** rectangle spanning 2am–4am (x from 50+(2/24)*630, width (2/24)*630, y=50 to 120), fill `rgba(231,76,60,0.3)`, stroke `#e74c3c` width 3.
- **Fraud label:** bold 17px red `#e74c3c`, centered above the window at 3am, y=40: "FRAUD: $50K drained".
- **Detection point:** filled circle radius 10 at (680, 120) in `rgba(39,174,96,0.3)`; below it green `#27ae60` bold 17px "Detection" at y=170 and 13px "(midnight batch)" at y=185.
- **Gap arrow:** dashed orange `#e67e22` line (dash 5/3, width 2) from x=4am-position to x=670 at y=75.
- **Gap label:** bold 17px orange `#e67e22`, centered at (380, 25): "22-hour gap = damage done".

## Power grid: 15-min meter vs sub-minute spike

20kW spike for 60 seconds blows transformer. 15-min average shows 1.8kW. The event that caused the failure is INVISIBLE in the metered data. Engineers stare at "normal" readings while replacing a burned transformer.

### Visualization (canvas `canvas2`, 720×200)

Line chart contrasting the actual bursty power signal with the flat 15-minute averaged reading.

- **Axes:** L-shaped axes `#333` width 1 from (60,20) down to (60,170) across to (690,170).
- **Y-axis labels (right-aligned gray `#666` 12px):** "20kW" at y=35, "10kW" at y=80, "2kW" at y=155, "0" at y=173.
- **Actual signal:** red `#e74c3c` line width 2.5 — a gentle sine baseline around y=155 (amplitude 2, ~1.5kW) from x=60 to x=340, then a sharp spike: up through (342,100), (344,50), (346,30), peak plateau (350,28), back down through (354,30), (356,50), (358,100) to (360,155), then sine baseline again out to x=690.
- **15-min averaged signal:** blue `#2980b9` dashed line (dash 8/4, width 3): flat at y=157 from x=60 to 330, tiny bump to y=150 at x=380, back to y=157 at x=430, flat to x=690.
- **Legend labels (bold 17px, left-aligned at x=400):** red "Actual (60s spike = 20kW)" at y=45; blue "15-min average: 1.8kW (flat)" at y=70.
- **Spike annotation:** red 13px centered at (350, 18): "Transformer blows".
- **X-axis label:** gray `#666` 12px centered at (375, 190): "Time (15 minutes)".

## E-commerce: Monthly conversion vs daily-by-device

Mobile checkout broke 3 weeks ago. Monthly blended metric: -15% (desktop masks it). Daily by device: would have caught it Day 1. Three weeks of lost mobile revenue because nobody segmented the signal.

### Visualization (canvas `canvas3`, 720×200)

Three-series line chart over 28 days: steady desktop line, mobile cliff, and a slowly declining blended line.

- **Axes:** `#333` width 1 from (60,15) down to (60,170) across to (690,170).
- **Y-axis labels (right-aligned gray 12px):** "5%" y=35, "3%" y=90, "1%" y=145, "0%" y=173.
- **X-axis labels (centered gray 12px at y=185):** "Day 1", "Day 7", "Day 14", "Day 21", "Day 28" at x=80+i*150.
- **Desktop line:** green `#27ae60` width 2.5, gentle sine around y=42 (amplitude 3, ~4.5%) from x=80 to x=680.
- **Mobile line:** red `#e74c3c` width 2.5, sine around y=50 from x=80 to x=200 (break point x=200), then vertical cliff to y=140 at x=205, then low sine around y=140 (amplitude 2) out to x=680.
- **Blended line:** purple `#8e44ad` width 2 dashed (dash 6/4): sine around y=46 to x=200, then gradual linear decline (slope 0.12 per px) from (200,46).
- **Break annotation:** vertical dashed red line (dash 3/3, width 1) at x=200 from y=15 to y=170; red 13px centered label "Mobile breaks" at (200, 12).
- **Legend (bold 17px left-aligned at x=500):** green "Desktop (fine)" y=35, red "Mobile (cliff)" y=55, purple "Blended (-15%)" y=75.

## Security logs: Weekly review vs real-time

Attacker exfiltrates data Mon-Thu. Friday log review sees it. Data already gone. Four full days of unchecked data transfer because the review cadence assumed threats move slowly.

### Visualization (canvas `canvas4`, 720×200)

Weekday timeline with growing red exfiltration bars Mon–Thu and a green detection box on Friday.

- **Timeline:** horizontal `#333` line width 2 from (60,130) to (680,130).
- **Day markers:** ticks and gray `#666` 13px labels "Mon", "Tue", "Wed", "Thu", "Fri" at x=120+i*130, labels at y=150.
- **Exfiltration bars:** four bars fill `rgba(231,76,60,0.7)`, each 60px wide starting at x=90+i*130, heights growing 30, 45, 60, 75px, sitting on the timeline (top at 130-height).
- **Title annotation:** bold 17px red `#e74c3c` centered at (310, 25): "Data exfiltrated: Mon → Thu".
- **Cumulative labels (red 12px, centered above each bar):** "2GB" at (120, 95), "5GB" at (250, 80), "12GB" at (380, 70), "25GB" at (510, 60).
- **Friday detection box:** rectangle (610, 60, 60×70), fill `rgba(39,174,96,0.3)`, stroke `#27ae60` width 2; green bold 17px "Detected" centered at (640, 170).
- **Too-late annotation:** dark red `#c0392b` bold 15px centered at (640, 55): "25GB already gone".
- **Gap arrow:** dashed orange `#e67e22` line (dash 4/3, width 2) from (150,45) to (600,45); orange 13px centered label "4 days undetected" at (375, 42).

## Music platform: Per-second skip data but weekly "engagement" report

New album terrible, 80% skip by 10am release day. Weekly report: next Monday. Algorithm already demoted it. The data existed in real-time but the reporting cadence hid it from decision-makers for 6 days.

### Visualization (canvas `canvas5`, 720×200)

Line chart of skip rate over 7 days with a vertical marker where the weekly report finally arrives.

- **Axes:** `#333` width 1 from (60,15) down to (60,170) across to (690,170).
- **Y-axis labels (right-aligned gray 12px):** "80%" y=40, "50%" y=80, "20%" y=130, "0%" y=173.
- **X-axis labels (centered gray 12px at y=188):** "Fri (release)", "Sat", "Sun", "Mon", "Tue", "Wed", "Thu" at x=100+i*88.
- **Skip-rate line:** red `#e74c3c` width 3, starting at (80,155), rapid rise through (90,120), (100,60), (120,40), then stays high — sine around y=38 (amplitude 4) out to x=680.
- **Rise annotation:** red 13px left-aligned at (125, 30): "80% skip by 10am".
- **Weekly report marker:** vertical dashed blue `#2980b9` line (dash 5/3, width 2) at x=364 (Monday) from y=15 to y=170.
- **Report label:** blue bold 17px centered at (364, 100): "Weekly report arrives"; blue 13px centered at (364, 118): "(6 days late)".
- **Gap annotation:** orange `#e67e22` bold 15px centered at (400, 160): "Signal existed instantly — report delayed 6 days".

## Stock trading: End-of-day portfolio risk vs intraday

Portfolio "safe" at close yesterday. Flash crash at 2pm today: 30% drawdown in 45 minutes, recovered by close. End-of-day risk report: "no change." The drama is invisible in daily data.

### Visualization (canvas `canvas6`, 720×200)

Line chart of intraday portfolio value with a dramatic V-shaped flash crash vs a flat daily-close line.

- **Axes:** `#333` width 1 from (60,15) down to (60,175) across to (690,175).
- **Y-axis labels (right-aligned gray 12px):** "$1M" y=40, "$700K" y=130, "$0" y=175.
- **X-axis labels (centered gray 12px at y=192):** "9:30am", "11am", "12pm", "1pm", "2pm", "3pm", "4pm" at x=80+i*95.
- **Intraday line:** red `#e74c3c` width 3 — gentle sine around y=38 from x=80 to ~x=420 (2pm), then sharp V: (425,40), (440,60), (455,90), (465,120), bottom (470,135) = 30% drawdown, recovery through (480,120), (495,90), (510,60), (530,45), (560,40), then sine around y=38 out to x=680.
- **Daily close line:** blue `#2980b9` dashed (dash 8/5, width 3), flat at y=38 from x=80 to x=680.
- **Drawdown annotation:** red bold 17px centered at (470, 155): "-30% drawdown"; red 13px centered at (470, 170): "(45 min)".
- **Legend (bold 17px left-aligned):** blue "Daily close: \"no change\"" at (80, 18); red "Intraday: catastrophe" at (420, 18).

## Monitoring: 5-second SNMP poll vs microsecond bursts

Network link at 100% for 200ms, idle for 800ms. 5-sec poll: "20% utilization." Users experiencing packet drops. The polling interval is 25x longer than the burst, making congestion invisible.

### Visualization (canvas `canvas7`, 720×200)

Square-wave chart of bursty link utilization vs a flat dashed polled-average line with poll-point dots.

- **Axes:** `#333` width 1 from (60,15) down to (60,170) across to (690,170).
- **Y-axis labels (right-aligned gray 12px):** "100%" y=30, "50%" y=90, "20%" y=135, "0%" y=173.
- **Actual traffic:** red `#e74c3c` square wave width 2 starting at (70,170): 5 cycles of a 50px-wide burst up to y=28 followed by an 80px-wide idle stretch at y=170.
- **Burst fills:** each 50px burst rectangle (y=28 to 170) filled `rgba(231,76,60,0.15)`.
- **Polled measurement:** blue `#2980b9` dashed line (dash 8/5, width 3), flat at y=135 (20%) from x=70 to x=680.
- **SNMP poll points:** 4 filled blue circles radius 5 at x=150, 300, 450, 600, y=135.
- **Legend (bold 17px left-aligned at x=380):** red "Actual: 100% bursts (packet drops!)" y=28; blue "SNMP poll: \"20% — all fine\"" y=52.
- **X-axis label:** gray 12px centered at (375, 190): "Time (5-second window)".

## Auto-scaling: 3-minute provision time vs 30-second spike

Traffic spike 30s duration. Auto-scaler: detect + provision + boot + deploy = 3-5 min. By the time it scales: spike is over. You pay for instances that arrive after the battle is lost.

### Visualization (canvas `canvas8`, 720×200)

Line chart of a sharp 30-second traffic spike vs the slow auto-scaler capacity ramp that arrives after the spike ends.

- **Axes:** `#333` width 1 from (60,15) down to (60,170) across to (690,170).
- **Y-axis labels (right-aligned gray 12px):** "10x" y=30, "5x" y=80, "1x" y=155.
- **X-axis labels (centered gray 12px at y=185):** "0s" x=80, "30s" x=140, "1min" x=220, "2min" x=370, "3min" x=510, "4min" x=640.
- **Traffic spike:** sharp triangle-ish peak in red `#e74c3c` width 3 with fill `rgba(231,76,60,0.2)`: from (80,155) up through (85,100), (95,40), peak (110,28), down through (125,40), (135,100) to (140,155); stroke continues flat at y=155 out to x=680.
- **Scale-up line:** blue `#2980b9` width 3 step/ramp: flat at y=155 from (80) to (160) (detection delay), then slow ramp (160,152) → (350,150) → (450,145), then steps up (500,80), (550,60), (600,55), flat to (680,55).
- **Annotations:** red bold 17px centered at (110, 18): "Traffic spike (30s)"; blue bold 17px centered at (560, 45): "Instances ready (3+ min later)"; orange `#e67e22` bold 15px centered at (380, 120): "Spike over. Users already got 503s.".
- **Arrow:** dashed orange line (dash 4/3, width 1.5) from (145,135) to (490,135) with a filled orange arrowhead at the right end.

## Regeneration instructions

- **Layout:** detail-page `.obj-table`: full-width table with `border-collapse: collapse` and a 2px solid `#2980b9` outer border; a `<thead>` row with two `<th>` cells "Scenario" / "Visualization" (background `#1a5276`, white text, padding 12px 16px, left-aligned); one `<tr>` per scenario in `<tbody>`; left `<td>` (40%) holds an `<h3>` title (1.05em `#1a5276`) plus one paragraph (0.95em, line-height 1.6); right `<td>` (60%) holds the canvas. Cell borders `1px solid #2980b9`, padding 16px, `vertical-align: top`; even rows background `#f0f8ff`.
- **Page style:** body system sans-serif (-apple-system stack), background `#fafafa`, text `#2c3e50`, padding 20px 10px; h1 2em `#1a5276`; `h2.subtitle` `#555`, weight 400, 1.1em, line-height 1.5. No nav bar, no back/home links.
- **Canvas:** each canvas styled `display: block; width: 720px; height: 200px; margin-top: 8px`; backing store sized 720×200 multiplied by `window.devicePixelRatio`, with `ctx.scale` back to logical coordinates in each per-canvas IIFE.
- **Fonts inside canvases:** -apple-system stack; 12-13px for axis labels/small annotations, bold 15-17px for headline annotations and legends.
- **Palette:** primary blue `#1a5276`, chart blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, dark red `#c0392b`, gray text `#666`/`#333`.
