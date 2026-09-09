# Satellite Direct-to-Device — Emergency SOS and Off-Grid Messaging

**Page type:** detail page (backlog-style two-column layout: text left 50%, canvas right 50%, one `.lang-section` per topic; h1 carries a BACKLOG status pill)
**HTML title tag:** Satellite Direct-to-Device — Emergency SOS and Off-Grid Messaging

**Subtitle:** An ordinary handset reaching orbit trades speed for reach — and creates records where nothing was ever recorded.

**Intro callout:** A phone with no tower in range can now push a few hundred bits to a satellite passing overhead. Every physical limit of that link — tiny antenna, thin margin, minutes of waiting — shapes the data it leaves behind: sparse, bursty position fixes that exist only because someone chose to send one.

## 1. Why a Bare Handset Can Reach Orbit

The phone has no dish, so the only currency left to spend is speed.

- **Nothing was added** — same flat internal antenna, same fraction of a watt of transmit power.
- **The satellite does the work** — a large steerable antenna array in orbit listens for a very faint signal.
- **Distance is brutal** — signal strength falls with the square of range, and orbit is hundreds of kilometres up.
- **Buying margin with time** — send each bit many times over and add the copies up at the receiver.
- **The exact trade** — every doubling of repeats buys about 3 dB of margin and halves the throughput.
- **Where that lands** — 256 repeats of a 51.2 kbps stream leave 200 bits per second, about 25 characters.
- **Hence text only** — no voice, no photos, no browsing; the payload is a short line of characters.

**Key point:** The link does not close because the radio got better — it closes because the payload got 256 times smaller.

### Visualization (canvas `c1`, 720×340)

Dual-axis chart (bars + line): throughput collapsing as repeats buy link margin. Marked "Illustrative Example".

- **Title (bold 16px, `#1a5276`, top center):** "Trading Speed for Margin".
- **Subtitle (12px `#7f8c8d`, centered under the title):** "Illustrative Example — base stream 51.2 kbps".
- **Plot area:** x=70, y=74, width = canvas−150, height = canvas−146; L-shaped axes `#95a5a6` (1.4px), plus a right-hand vertical axis in `#e67e22`.
- **X categories (9 slots):** repeats `[1, 2, 4, 8, 16, 32, 64, 128, 256]`, labels 12px `#4a5866`; axis label "Times each bit is repeated" (13px `#4a5866`, centered below).
- **Bars (throughput, left axis, log10 scale 100 → 100000 bits/s):** height from `51200 / N` computed per slot; bar width 0.5·slot; fill `rgba(26,82,118,0.35)`, stroke `#1a5276` 1.4px.
- **Left tick labels:** at 100, 1000, 10000, 100000 → printed as `100`, `1k`, `10k`, `100k` (12px `#5a6875`, right-aligned); left axis title "bits per second" (12px `#1a5276`, above the axis).
- **Line (margin gain, right axis 0 → 30 dB):** points `10·log10(N)` computed per slot, stroke `#e67e22` 3px, filled `#e67e22` dots radius 3.5.
- **Right tick labels:** 0, 10, 20, 30 with a `dB` suffix (12px `#e67e22`, left-aligned outside the plot).
- **Computed annotations (13px):** beside the last bar, `#1a5276`, the string `(51200/256) + ' bps'` → "200 bps"; above the last line point, `#e67e22`, `(10·log10(256)).toFixed(1) + ' dB gain'` → "24.1 dB gain". Both computed at render time from the plotted values.

## 2. Point at the Sky and Hold Still

The instruction to stand in the open and stop moving is the user closing the link by hand.

- **No margin to waste** — the budget is a few decibels, so any blockage is the whole reserve.
- **Water absorbs radio** — a hand or torso between phone and sky costs more than the margin holds.
- **Leaves and branches too** — wet foliage scatters the signal well before a canyon wall does.
- **Low angles are worse** — a satellite near the horizon means a longer slant path through more air and clutter.
- **Roofs end it** — indoors the loss is far past anything the link can absorb.
- **Why you hold still** — the receiver is summing repeats, and a moving phone breaks that sum.
- **The on-screen arrow** — the phone is steering the user, not steering an antenna.

**Key point:** A budget with a couple of decibels of slack makes the human body an outage.

### Visualization (canvas `c2`, 720×320)

Bar chart: link margin left after each obstruction, green above the closure line, red below. Marked "Illustrative Example".

- **Title (bold 16px, `#1a5276`, top center):** "What Is Left of the Margin".
- **Subtitle (12px `#7f8c8d`, centered under the title):** "Illustrative Example — 10 dB clear-sky budget".
- **Plot area:** x=72, y=74, width = canvas−140, height = canvas−140; vertical axis `#95a5a6` 1.4px.
- **Data:** blockage loss in dB per case — `open sky` 0, `low angle` 1, `tree cover` 12, `body in path` 15, `indoors` 25. Bar value = `10 − loss`, computed → `[10, 9, −2, −5, −15]`.
- **Scale:** y from −20 to +15 dB; tick labels every 5 (12px `#5a6875`, right-aligned); axis title "margin remaining (dB)" (12px `#4a5866`).
- **Zero line:** solid `#34495e` 1.4px across the plot at 0 dB, with label "link closes above this line" (13px `#34495e`, right-aligned at the plot's right edge just above the line).
- **Bars:** 5 slots, width 0.42·slot, drawn from the zero line — value ≥ 0 fill `rgba(39,174,96,0.50)` stroke `#27ae60`; value < 0 fill `rgba(231,76,60,0.50)` stroke `#e74c3c`, both 1.4px.
- **Value labels (12px, computed):** the bar value with an explicit sign, drawn above positive bars and below negative bars in the bar's own stroke colour.
- **X labels:** case names (12px `#4a5866`) on the baseline row below the plot, one line each.

## 3. Minutes, Not Milliseconds

The delay is almost entirely waiting for a satellite to show up.

- **Flight time is nothing** — 550 km up and back is about 3.7 milliseconds of travel at light speed.
- **The wait dominates** — with a thin constellation, minutes pass before one is overhead.
- **Then acquisition** — locking on and holding steady adds tens of seconds before a bit moves.
- **The send is slow** — 160 characters is 1,280 bits, which at 200 bits per second is 6.4 seconds.
- **No live session** — the phone hands over a message and the satellite carries it (store-and-forward).
- **Delivery is later** — the satellite relays it when a ground station comes into its own view.
- **Illustrative total** — about 460 seconds end to end, roughly 4,600× a tower's tenth-of-a-second path.

**Key point:** Distance costs milliseconds; availability costs minutes — and only one of them is on the bill.

### Visualization (canvas `c3`, 720×340)

Horizontal log-scale bars: the time budget of one short message. Marked "Illustrative Example".

- **Title (bold 16px, `#1a5276`, top center):** "Where the Minutes Go — One Short Message".
- **Subtitle (12px `#7f8c8d`, centered under the title):** "Illustrative Example — 200 bps link, 550 km orbit".
- **Plot area:** x=170, y=74, width = canvas−230, height = canvas−150; L-shaped axes `#95a5a6` 1.4px.
- **Components (seconds; the last two computed, not typed):**
  - `wait for a pass` — 420
  - `lock on, hold still` — 30
  - `send 160 characters` — `1280 / 200` = 6.4
  - `radio flight time` — `2 · 550 / 299792.458` ≈ 0.00367
  - `ground delivery` — 4
- **Scale:** log10 x axis from 0.001 s to 1000 s; gridline ticks at 0.001, 0.01, 0.1, 1, 10, 100, 1000 as dotted `#dfe4e8` verticals with labels (11px `#5a6875`, centered below); axis label "seconds (log scale)" (13px `#4a5866`, centered below).
- **Bars:** 5 rows, height 0.5·row; the two waiting rows (`wait for a pass`, `lock on, hold still`) fill `rgba(231,76,60,0.45)` stroke `#e74c3c`; the three physics/transport rows fill `rgba(26,82,118,0.35)` stroke `#1a5276`; all 1.4px.
- **Row labels:** component names (12px `#4a5866`, right-aligned at x=160).
- **Value labels (12px, computed, drawn just past each bar end in the bar's stroke colour):** values ≥ 1 s printed as `N.N s`, values < 1 s printed as `N.N ms` from `value·1000`.
- **Footer (13px `#1a5276`, centered below the axis label):** total from `sum(components)` printed as `Total: NNN.N s` → "Total: 460.4 s".

## 4. A Pass Window, Not a Connection

Low orbit gives a strong link that is only there sometimes; high orbit gives a weak link that is always there.

- **Low orbit is close** — a few hundred kilometres, so the faint handset signal still arrives usable.
- **But it moves fast** — a satellite crosses the sky in minutes, then the link is simply gone.
- **Geostationary sits still** — one fixed point in the sky, always in view from the same spot.
- **It is 65× further** — about 36,000 km, costing roughly 36 dB more path loss than 550 km.
- **Also low on the horizon** — from high latitudes it sits near the skyline, straight into the clutter.
- **Elevation mask** — below roughly 25° above the horizon the path is too obstructed to count.
- **Illustrative window** — three passes in two hours leave about 18 usable minutes out of 120.

**Key point:** Off-grid messaging is scheduled, not connected — the phone waits for geometry to allow it.

### Visualization (canvas `c4`, 720×340)

Line chart: satellite height above the horizon over two hours, with pass windows above an elevation mask. Marked "Illustrative Example".

- **Title (bold 16px, `#1a5276`, top center):** "Three Passes in Two Hours".
- **Subtitle (12px `#7f8c8d`, centered under the title):** "Illustrative Example — 25° elevation mask".
- **Plot area:** x=70, y=74, width = canvas−140, height = canvas−150; L-shaped axes `#95a5a6` 1.4px.
- **Scales:** y from 0 to 90 degrees, tick labels every 15 with a `°` suffix (12px `#5a6875`, right-aligned); x from 0 to 120 minutes, tick labels every 20 (12px `#5a6875`, centered below); axis labels "Minutes" (13px `#4a5866`, centered below) and "height above horizon" (12px `#4a5866`, above the y axis).
- **Low-orbit curve (stroke `#1a5276` 3px):** sum of three humps, sampled t = 0 → 120 in 0.25-minute steps. Each hump `(centre c, peak p, half-width Hw)` contributes `p · cos((π/2)·(t−c)/Hw)` while `|t−c| < Hw`, else 0:
  - `(c=12, p=78, Hw=5.5)`, `(c=52, p=34, Hw=3.0)`, `(c=96, p=61, Hw=4.5)`.
- **Mask line:** dashed `#e74c3c` (dash 5/4, 1.8px) at 25°, label "25° mask" (12px `#e74c3c`, left-aligned just above the line at the plot's left edge).
- **Usable shading:** for every sample above 25°, fill the column between the curve and the mask line with `rgba(39,174,96,0.30)`.
- **Geostationary line:** solid `#e67e22` 2.5px horizontal at 30°, label "geostationary — always up, 36 dB weaker" (12px `#e67e22`, left-aligned above the line near the plot's left edge).
- **Computed annotation (13px `#27ae60`, centered above the plot's interior):** minutes above the mask, counted from the plotted samples as `(samples above 25°) · 0.25`, printed as `NN.N min usable of 120`. The three humps give ≈18.1 minutes above the mask; the printed figure comes from the sample count, never a literal.

## 5. Records From Places That Produced None

The data-collection consequence: telemetry now appears where the map was blank.

- **The blank was structural** — no tower meant no attach, no handover, no billing row, nothing.
- **Now there is a row** — a satellite message carries a timestamp and a position fix.
- **Very few of them** — a handful per device per day at most, against hundreds on a tower.
- **Bursty, not periodic** — clustered in the minutes around a send, then nothing for hours.
- **High value per point** — a wilderness fix is one of the only observations for that whole area.
- **Every one is deliberate** — the user pressed something; nothing is collected in the background.
- **Illustrative split** — check-ins 44%, location shares 26%, emergency SOS 18%, other text 12%, passive 0%.

**Key point:** These rows are not sparse tower data — they are a different sampling process entirely.

### Visualization (canvas `c5`, 720×320)

Horizontal bar chart: what caused each satellite-path record to exist. Marked "Illustrative Example".

- **Title (bold 16px, `#1a5276`, top center):** "Why the Record Exists At All".
- **Subtitle (12px `#7f8c8d`, centered under the title):** "Illustrative Example — share of satellite-path records".
- **Plot area:** x=190, y=74, width = canvas−250, height = canvas−140; L-shaped axes `#95a5a6` 1.4px.
- **Data (percent, 5 rows):** `manual check-in` 44, `location share` 26, `emergency SOS` 18, `other short text` 12, `passive background` 0.
- **Scale:** x from 0 to 50 percent, dotted `#dfe4e8` gridlines every 10 with labels `N%` (11px `#5a6875`, centered below); axis label "share of records" (13px `#4a5866`, centered below).
- **Bars:** height 0.52·row; `emergency SOS` fill `rgba(231,76,60,0.50)` stroke `#e74c3c`; all others fill `rgba(26,82,118,0.35)` stroke `#1a5276`; 1.4px.
- **Row labels:** cause names (12px `#4a5866`, right-aligned at x=180).
- **Value labels (12px, computed, just past each bar end in the bar's stroke colour):** the row's percent with a `%` suffix.
- **Zero-row annotation (12px `#e67e22`, left-aligned at the axis for the `passive background` row):** "0% — nothing arrives unasked".
- **Footer (12px `#5a6875`, centered below the axis label):** computed sum printed as `Shares sum to N%` → "Shares sum to 100%".

## 6. Why These Fixes Break Ordinary Analysis

A fix exists because of the emergency, so the emergency rate per fix is meaningless.

- **The selection is the trigger** — sending is conditioned on trouble or on intent to report.
- **Tower data is unconditioned** — pings continue whether anything is happening or not.
- **The rate diverges wildly** — the same event looks thousands of times more common in the satellite rows.
- **Illustrative counts** — 12 incident-linked rows in 600,000 tower rows is 0.002%.
- **Against the satellite path** — 162 incident-linked rows in 900 is 18%, a ratio near 9,000×.
- **Pooling is the error** — union the two sources and the incident rate tracks the source mix, not reality.
- **Coverage looks inverted** — a wilderness gap with a few SOS rows outscores a city on risk per record.
- **What is safe** — treat them as a separate stratum with its own denominator, never as extra pings.

**Key point:** Two collection processes, two denominators — a shared rate estimate belongs to neither.

### Visualization (canvas `c6`, 720×320)

Two log-scale bars: incident-linked share of records by collection path. Marked "Illustrative Example".

- **Title (bold 16px, `#1a5276`, top center):** "Same Event, Two Denominators".
- **Subtitle (12px `#7f8c8d`, centered under the title):** "Illustrative Example — incident-linked share of records".
- **Plot area:** x=90, y=76, width = canvas−170, height = canvas−156; L-shaped axes `#95a5a6` 1.4px.
- **Data (counts, with the plotted percent computed as `100·hits/total`):**
  - `tower pings` — 12 of 600,000 → 0.002%
  - `satellite messages` — 162 of 900 → 18%
- **Scale:** log10 y from 0.001% to 100%; tick labels at 0.001, 0.01, 0.1, 1, 10, 100 printed as `N%` (12px `#5a6875`, right-aligned), each with a dotted `#dfe4e8` gridline.
- **Bars:** 2 slots, width 0.30·slot, drawn from the axis floor — tower fill `rgba(26,82,118,0.35)` stroke `#1a5276`; satellite fill `rgba(231,76,60,0.50)` stroke `#e74c3c`; 1.4px.
- **X labels:** path names (13px `#4a5866`) under each slot, with the count fraction beneath in 11px `#7f8c8d` (e.g. "12 of 600,000"), both built from the data.
- **Value labels (12px, computed, above each bar in its stroke colour):** the computed percent, formatted with enough decimals to be exact (`0.002%`, `18%`).
- **Ratio annotation (13px `#e67e22`, centered between the two bar tops):** computed as `satPct / towerPct` and printed as `NNNN× higher per record` → "9000× higher per record".

## Regeneration instructions

- **Layout:** backlog detail page. `h1` (2rem `#1a5276`, bottom border `2px solid #2980b9`) with inline `.status` pill "BACKLOG" (background `#fef9e7`, border `1px solid #f39c12`, text `#b7950b`, 4px radius, 0.8rem); `.subtitle` (`#666`, 0.95rem); `.intro` callout (background `#f0f4f8`, left border `3px solid #2980b9`, 8px 12px padding, 0.9rem). One `.lang-section` per numbered h2 (1.3rem `#1a5276`, bottom border `2px solid #2980b9`); inside each, `table.layout` with `td.text-col` 50% and `td.viz-col` 50%, both `vertical-align: top`, 12px padding. No index number in the h1 or the title tag.
- **Text blocks:** intro `<p>`, `<ul>` bullets (0.92rem) with `<strong>` lead-ins, `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, 0.9rem).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. Canvases `width: 100%`, `height: auto`, `1px solid #e0e0e0` border, 4px radius.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, `rgba(26,82,118,0.35)` bar fill; gray labels `#5a6875`/`#4a5866`, axes `#95a5a6`, gridlines `#dfe4e8`.
- **Canvas:** intrinsic 720 wide; the backing store is sized to the rendered CSS width × `window.devicePixelRatio` via a shared `setupCanvas(id)` helper (display capped at the logical width with `style.maxWidth`), with `ctx.setTransform` back to logical coordinates. All six draw functions are registered and re-run on a debounced `window.resize`.
- **Data:** no `Math.random()` anywhere. Every series is a hardcoded literal array or a closed-form function of the plotted index, because the shape and counts carry the lesson. Every statistic printed beside a chart — throughput, margin gain, total seconds, usable minutes, share sum, percentages, ratio — is computed from the plotted values at render time. The seeded `lcg(seed)` helper is not needed on this page since no draw is stochastic.
- **Physical constants stated as fact:** speed of light 299,792.458 km/s, low-orbit altitude 550 km, geostationary altitude ~35,786 km. Everything constructed for a chart is labelled "Illustrative Example".
- **No cross-page links of any kind.**
