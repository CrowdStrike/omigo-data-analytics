# Network Security Pitfalls

**Page type:** detail page (one h2 per pitfall, each followed by a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** Network Security Pitfalls

**Subtitle:** Why network detection is blind to encrypted payloads, internal lateral movement, and protocol-abusing covert channels.

## Encrypted Traffic Opacity

**85%+ of Traffic Is Encrypted — the IDS Sees Metadata, Never Content**

- **The block:** TLS 1.3 hides the payload unless you insert a MITM proxy in the path.
- **What stays visible:** packet sizes, timing patterns, destinations — the envelope only.
- **What goes dark:** payload content at 0% visibility, signature matches at 5%.
- **Why rules fail:** signature detection matches on bytes the sensor can no longer read.
- **What to change:** lean on traffic-analysis patterns instead of deep packet inspection.

### Visualization (canvas `canvas1`, 720×200)

Stacked bar chart of IDS visibility per traffic attribute: each 100%-tall bar split into a visible (green) bottom portion and blind (red) top portion.

- **Title (bold 17px, `#1a5276`, centered):** "What IDS Can See in Encrypted Traffic".
- **Categories (x-axis):** Payload Content, Packet Sizes, Timing Patterns, Destinations, Signatures — bars 90px wide, starting x=80, spacing 130px, baseline y=175, full height 120px.
- **Visibility data (% visible):** `[0, 85, 70, 90, 5]` — green `#27ae60` fills the visible portion from the baseline up; red `#e74c3c` fills the remainder to 100%; each full bar outlined 1px `#2c3e50`.
- **Percentage label above each bar (bold 14px):** the visibility value, colored green `#27ae60` if >50 else red `#e74c3c`. Category names below baseline in 13px `#333`.
- **Legend (right side):** green swatch "Visible", red swatch "Blind" (13px `#333`).

## East-West Blindspot

**Perimeter Traffic Is 90% Monitored; Internal Traffic Only 20%**

- **Where the sensors sit:** traditional firewalls guard the perimeter, not the interior.
- **The gap:** lateral movement server-to-server is often completely unmonitored.
- **Consequence:** an attacker already inside the network moves freely and unseen.
- **Flat-network math:** one compromised host equals access to everything.
- **Volume mismatch:** internal traffic dwarfs perimeter traffic yet gets far less scrutiny.

### Visualization (canvas `canvas2`, 720×200)

Two side-by-side pie charts comparing monitoring coverage of perimeter vs internal traffic.

- **Title (bold 17px, `#1a5276`, centered):** "Network Traffic Monitoring Coverage".
- **Left pie (center x=180, y=115, radius 65, slices start at top, white 2px slice borders):** Monitored 90% green `#27ae60`, Unmonitored 10% red `#e74c3c`. Two-line label below in bold 14px `#1a5276`: "North-South" / "(Perimeter)".
- **Right pie (center x=420, same geometry):** Monitored 20% green, Unmonitored 80% red. Label: "East-West" / "(Internal)".
- **Legend (far right):** green swatch "Monitored", red swatch "Unmonitored" (13px `#333`).

## Protocol Misuse

**Attacks Ride Inside Protocols That Are 85-99% Legitimate Traffic**

- **DNS abuse:** carries data exfiltration while looking like ordinary name lookups.
- **HTTPS abuse:** carries command-and-control traffic; detection difficulty is "Very Hard".
- **ICMP abuse:** carries covert channels hidden inside diagnostic pings.
- **Why headers don't help:** every one of these is well-formed at the protocol level.
- **What to change:** detect behavior within a legitimate protocol, not header validity.

### Visualization (canvas `canvas3`, 720×200)

Grouped bar chart of legitimate vs malicious use per protocol, annotated with detection difficulty.

- **Title (bold 17px, `#1a5276`, centered):** "Protocol: Legitimate vs Malicious Use (Detection Difficulty)".
- **Protocols (x-axis groups):** DNS, HTTPS, ICMP, SSH — group width 130px starting x=100, bars 50px wide, baseline y=160, max height 100px = 100%.
- **Data:** legitimate `[95, 99, 90, 85]` (%) in blue `#3498db`; malicious `[5, 1, 10, 15]` (%) in red `#e74c3c` (minimum 5px height for visibility).
- **Labels:** protocol name in bold 14px `#333` below the baseline; detection difficulty in 12px orange `#e67e22` on a second line: DNS "Hard", HTTPS "Very Hard", ICMP "Medium", SSH "Medium".
- **Baseline:** thin light-gray line `#bdc3c7` from x=80 to x=650.
- **Legend (right side):** blue swatch "Legitimate", red swatch "Malicious" (13px `#333`).

## DNS Tunneling

**255 Bytes per DNS Query, Times Millions of Queries, Equals Gigabytes Stolen**

- **The mechanism:** stolen bytes are encoded as subdomain labels of an attacker-owned domain.
- **Per-query yield:** each query smuggles out roughly 255 bytes of payload.
- **At scale:** millions of queries accumulate into gigabytes leaving the network.
- **Why it works:** DNS is always allowed outbound — a permanent covert channel.
- **The monitoring gap:** standard tooling rarely checks subdomain entropy or query rate.

### Visualization (canvas `canvas4`, 720×200)

Area chart of cumulative data exfiltrated via DNS over 24 hours.

- **Title (bold 17px, `#1a5276`, centered):** "DNS Tunneling: Cumulative Data Exfiltration".
- **Plot area:** x from 80 to 650, baseline y=170, top y=45; 25 points (hours 0–24).
- **Data:** hourly exfiltration rates (MB/hour) `[25, 42, 38, 55, 30, 48, 60, 35, 44, 52, 28, 46, 57, 33, 41, 50, 62, 37, 45, 53, 29, 47, 58, 40, 43]`, accumulated into a monotonically rising cumulative series scaled so the final total reaches the top of the plot.
- **Style:** area fill `rgba(231,76,60,0.3)` under a red `#e74c3c` line 2.5px wide; solid `#333` L-shaped axes.
- **X-axis:** tick labels "0h", "6h", "12h", "18h", "24h" (13px `#333`); axis title "Hours" centered below. **Y-axis:** rotated label "MB Exfiltrated" on the left.
- **Annotations (upper left of plot):** bold 13px red `#e74c3c` "255 bytes/query x millions = GBs stolen"; below it 12px gray `#666` "DNS always allowed through firewalls".

## Bandwidth-Constrained Inspection

**Full Capture at 10Gbps Costs 4.3TB per Hour — So Nobody Keeps It All**

- **The arithmetic:** 4.3TB/hour becomes 103TB per day and 3,096TB per month.
- **Forced tradeoff:** storage economics require sampling or filtering, never full retention.
- **What it costs you:** uncaptured packets are invisible to retrospective investigation.
- **The real loss:** evidence is destroyed by bandwidth economics, not by the attacker.
- **The exploit:** attackers hide in the gaps between sampled packets.

### Visualization (canvas `canvas5`, 720×200)

Log-scale bar chart of full-packet-capture storage requirements at 10Gbps by retention window.

- **Title (bold 17px, `#1a5276`, centered):** "Full Packet Capture Storage at 10Gbps".
- **Bars (x-axis):** 1 Hour, 8 Hours, 1 Day, 1 Week, 1 Month — values `[4.3, 34.4, 103, 722, 3096]` TB; bar height is log-scaled (log(value)/log(3096) × 105px, minimum 10px), bars 80px wide, starting x=90, gap 115px, baseline y=160.
- **Bar fill:** vertical gradient from red `#e74c3c` (top) to orange `#f39c12` (bottom), 1px border `#c0392b`.
- **Labels:** value + " TB" in bold 13px `#c0392b` above each bar; timeframe in 13px `#333` below baseline; solid `#333` baseline from x=70 to x=680.
- **Note (bottom right, 12px `#666`):** "Log scale — what you can't store is invisible".

## Infrastructure Changes Break Baselines

**One Cloud Migration Drops Anomaly Accuracy from 90% to 55% Overnight**

- **On-prem to cloud:** the migration changes every traffic pattern the model learned.
- **New CDN:** packet timing shifts, so timing-based features stop meaning what they did.
- **The result:** a model trained on the old infrastructure is useless on the new one.
- **The tax:** every infrastructure change forces a full retrain, not a tweak.
- **Steady state:** continuous drift leaves detection unreliable through every transition.

### Visualization (canvas `canvas6`, 720×200)

Line chart of anomaly-model accuracy over 12 months with sharp drops at each infrastructure change, marked by dashed vertical lines and triangle markers.

- **Title (bold 17px, `#1a5276`, centered):** "Model Accuracy vs Infrastructure Changes".
- **Plot area:** x from 80 to 660, baseline y=170, top y=50; y-scale 20%–100% with horizontal gridlines `#ecf0f1` at 20/40/60/80/100% labeled in 11px `#999` on the left.
- **Accuracy series (13 monthly points M0–M12):** `[95, 92, 90, 55, 62, 70, 78, 45, 53, 61, 30, 42, 55]` (%), drawn as a blue `#2980b9` line 2.5px wide.
- **Infrastructure change markers at months 3, 7, 10:** dashed red vertical line (`#e74c3c`, dash 4/4, 1.5px) from top to baseline, downward red triangle at the top, and an 11px red label above: "Cloud Migration" (M3), "New CDN" (M7), "Network Redesign" (M10).
- **Axes:** solid `#333` L-shape; x-tick labels "M0", "M3", "M6", "M9", "M12" in 12px `#333`; axis title "Months" centered below.

## Regeneration instructions

- **Layout:** standard detail-page pattern — h1 + `.subtitle`, then per pitfall an `<h2>` (1.4em, `#1a5276`, bottom border `2px solid #2980b9`) followed by a single-row `.obj-table`: full-width table, left `<td>` (40%) holds `.obj-title` (a one-line punchline, not a repeat of the h2) followed by a `<ul>` of 4-5 `<li>` labeled bullets (`<strong>Label:</strong> phrase`, each fitting one line), right `<td>` (60%, centered) holds the canvas. Even table rows have background `#fafcfe`.
- **Page CSS:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; `.obj-title` 1.05em weight 600 `#1a5276`; `ul` margin `8px 0 8px 20px`, 0.9em `#333`; `li` margin `4px 0`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `strong` `#1a5276`; `.philosophy` callout style available (background `#f0f4f8`, left border `4px solid #2980b9`) but unused on this page. No nav bar, no back/home links.
- **Canvases:** the `<canvas>` elements carry only ids; a shared `setupCanvas(canvas, 720, 200)` helper sets intrinsic size 720×200, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Default chart font 17px system sans-serif; titles bold 17px.
- **Palette:** primary blue `#1a5276`, blue accents `#2980b9`/`#3498db`, green `#27ae60`, red `#e74c3c` (dark red `#c0392b`), orange `#f39c12`/`#e67e22`, gray text `#666`/`#333`.
- Note: in regenerated HTML, any card/page links use `.html` extensions (this page has none).
