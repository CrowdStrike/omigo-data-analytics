# WiFi / Bluetooth / RFID Security & Data

**Page type:** detail page (one h2 per pitfall, each followed by a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** 99. WiFi / Bluetooth / RFID Security & Data

**Subtitle:** Continuous broadcast tracking, RFID skimming, evil twin attacks, device fingerprinting — the invisible wireless attack surface.

## Continuous Probe Request Broadcasting

- Phone constantly broadcasts "looking for [saved network names]" in clear text — unauthenticated, unencrypted.
- Walking a city, your phone screams "Is Starbucks here? HomeWiFi? WorkNetwork?" to every receiver in range.
- Anyone listening can identify you from your unique probe list and read which networks you have visited.
- Movement tracking follows: the same probe list reappearing at several sensors stitches together your path.
- MAC randomization does NOT help — probe request CONTENT (saved network names) is the fingerprint, not the MAC.
- Passive WiFi sensors at airports, malls, and city streets collect these broadcasts continuously.

**Example/Impact:** A person's saved network list (home, work, hotels, airports) creates a unique signature. Even with randomized MACs, the content of probe requests identifies individuals with high confidence across multiple sensor locations.

### Visualization (canvas `c1`, 720×300)

Diagram: a phone broadcasting probe requests, overheard by passive sensors.

- **Phone:** dark blue rectangle (`#1a5276`) at (340,65) 40×60 with light blue screen inset (`#d6eaf8`).
- **Broadcast waves:** concentric circles centered (360,95), radii 30 to 110 step 25, stroke `#2980b9` width 1.5.
- **Probe request labels** (red `#e74c3c`, 13px, left-aligned at x=490, y=45/68/91/114): `"Is Starbucks here?"`, `"Is HomeWiFi here?"`, `"Is WorkNetwork here?"`, `"Is HotelGuest here?"`.
- **Passive sensors:** three red dots (`#e74c3c`, radius 7) at x=60, y=48/98/148, labeled in bold 14px `#1a5276`: "Sensor A", "Sensor B", "Sensor C".
- **Dashed gray lines** (`#bdc3c7`, dash 3/3) from each sensor toward the phone at (330,95).
- **Annotations (bottom center):** gray 12px `#7f8c8d` "MAC randomization defeated by content analysis" at y=170; bold 14px `#c0392b` "Unique probe list = persistent device fingerprint" at y=185.

## Bluetooth MAC Address Tracking Across Stores

- BT beacons at every store entrance count foot traffic — sensors detect your Bluetooth MAC as you pass.
- Newer OS "randomized" addresses rotate on predictable intervals — correlatable within a single visit.
- Mall-wide tracking: path through stores, dwell time per location, visit frequency, day-of-week patterns.
- Visit patterns alone identify regulars — the same de-anonymization as home+work = identity.
- Sold to brands as "anonymized foot traffic analytics" — consistent shoppers are trivially re-identified.

**Example/Impact:** A shopper who visits Store A (2min), Store B (8min), Store D (15min) every Tuesday afternoon is uniquely identifiable without any name or account. Visit pattern IS identity.

### Visualization (canvas `c2`, 720×300)

Diagram: five store boxes with a tracking path and dwell-time bar chart.

- **Stores:** boxes 70×45 at y=25, centered at x = 80, 200, 320, 440, 580, fill `#eaf2f8`, stroke `#2980b9` width 2, labels "Store A"–"Store E" in `#1a5276` 13px.
- **Tracking path:** dashed red line (`#e74c3c`, width 2, dash 5/5) through points (80,90)→(200,95)→(320,88)→(440,97)→(580,90); red person dot (radius 5) at (320,88).
- **Dwell-time bars:** values `[2, 8, 1, 15, 4]` minutes, bar height = value×3.5px, width 24, baseline y=175, fill `rgba(41,128,185,0.6)`, stroke `#2980b9`; value labels "2m", "8m", "1m", "15m", "4m" below at y=190 in `#1a5276` 11px.
- **Label (bold 13px `#1a5276`, center, y=118):** "Dwell Time (minutes) — Visit pattern = unique identity".

## RFID Credit Card Skimming

- Contactless cards (NFC/RFID) respond to ANY reader within ~4cm — the protocol has no reader authentication.
- The card authenticates ITSELF to the reader, but never verifies the reader is legitimate.
- Attacker with concealed reader bumps against you in a crowd — reads card number + expiry instantly.
- Transaction limits ($100 contactless) mean the attacker can make purchases immediately without PIN.
- The card has NO way to know it's talking to a thief vs. a legitimate payment terminal.

**Example/Impact:** Crowded subway or bus — attacker with reader in backpack presses against victims. Card data harvested from dozens of people per hour. The fundamental protocol flaw: reader authentication is simply not part of the spec.

### Visualization (canvas `c3`, 720×300)

Diagram: contactless card being read by a concealed attacker reader.

- **Card:** yellow rectangle (fill `#f4d03f`, stroke `#d4ac0d`) at (80,55) 130×85, with gold chip (`#d4ac0d`, stroke `#b7950b`) at (100,70) 30×22; card text in `#1a5276` 12px: "RFID/NFC Card" and "**** **** **** 4532".
- **Range circle:** dashed red circle (`#e74c3c`, dash 4/4, width 1.5) radius 90 centered (145,97), labeled above in red 11px: "~4cm effective range".
- **Arrow:** thick dark red (`#c0392b`, width 2.5) from (240,97) to (400,97) with filled arrowhead.
- **Attacker reader:** dark box `#2c3e50` at (410,60) 110×75 with light inset `#ecf0f1`; bold red 12px text "Concealed" / "Reader".
- **Callout (left-aligned at x=545):** bold 13px `#c0392b` "No reader authentication!"; then 12px `#1a5276` "Card authenticates itself" and "Reader: no auth required".
- **Bottom text (bold 14px `#e74c3c`, center, y=175):** "Card -> Number + Expiry -> Thief (protocol flaw)".

## Evil Twin AP Attacks

- Attacker creates WiFi network with same name as legitimate AP (airport, coffee shop, hotel).
- Phone auto-connects to saved network names — no way to verify the AP is the REAL "Airport WiFi."
- All traffic routed through attacker: credentials captured, sessions hijacked, DNS poisoned.
- Same problem as Stingray for cellular — device cannot authenticate the access point, only the reverse.
- Higher signal strength wins — a better antenna overrides the legitimate AP from the victim's view.

**Example/Impact:** "Free Airport WiFi" hotspot in terminal — actually an attacker's laptop with a WiFi card. Every device that connects leaks credentials. HTTPS helps but DNS hijacking + certificate warnings get clicked through by most users.

### Visualization (canvas `c4`, 720×300)

Diagram: phone choosing between a legitimate AP and an evil twin.

- **Legitimate AP:** green circle (`#27ae60`, radius 22) at (140,45), white "AP" text; labels below in green 13px: `"Airport WiFi"` and "(Legitimate)".
- **Evil twin AP:** red circle (`#e74c3c`, radius 22) at (560,45), white "AP" text; labels below in red: `"Airport WiFi"` and bold "(EVIL TWIN)".
- **Victim phone:** dark blue rect (`#1a5276`) at (335,105) 40×60 with light blue screen (`#d6eaf8`).
- **Connections:** solid red line (width 2.5) from phone to evil twin; dashed gray line (`#bdc3c7`, dash 4/4) from phone to legitimate AP with a red X mark at ~(240,93).
- **Label (red 11px near evil-twin link, at (470,105)):** "Stronger signal wins".
- **Bottom text:** bold 14px `#c0392b` centered at y=180 "All traffic routed through attacker"; gray 12px `#7f8c8d` at y=195 "Passwords, sessions, cookies — captured in plaintext".

## Device Fingerprinting from Signal Characteristics

- Every WiFi/BT chip has manufacturing variations that give it a unique radio-frequency signature.
- The tells: clock drift, power curve shape, and modulation imperfections in the transmitted signal.
- These physical-layer characteristics are inherent to the hardware and cannot be changed by software.
- Research demonstrates 99%+ accuracy in identifying individual devices from radio fingerprints alone.
- MAC randomization = defeated by physics: the address changes but the RF signature stays constant.
- Passive collection: the receiver only observes the signal, never communicates with the device.

**Example/Impact:** Two devices with randomized MACs transmitting — their waveforms differ due to oscillator imprecision, amplifier nonlinearity, and antenna coupling. A trained classifier identifies each device with >99% accuracy regardless of what MAC they advertise.

### Visualization (canvas `c5`, 720×300)

Two distinct RF waveforms illustrating unique per-chip signatures.

- **Title (bold 14px `#1a5276`, top center):** "RF Signal Fingerprint — Unique Per Physical Chip".
- **Device A waveform:** blue (`#2980b9`, width 2) from x=40 to 360 around y=65: sum of sines `sin(0.08x)*18 + sin(0.15x)*8 + cos(0.03x)*5`; label right in blue 13px "Device A (MAC: aa:bb:cc:random)".
- **Device B waveform:** red (`#e74c3c`, width 2) from x=40 to 360 around y=125: `sin(0.09x)*16 + cos(0.12x)*11 + sin(0.04x)*7`; label "Device B (MAC: dd:ee:ff:random)" in red.
- **Callout box:** outlined rect (`#1a5276`, width 2) at (375,145) 300×35 containing bold 15px "99%+ identification accuracy".
- **Footer (gray 12px `#7f8c8d`, center, y=195):** "Clock drift + power curves + modulation imperfections = permanent hardware ID".

## Beacon Stuffing / BLE Spam

- Attackers flood an area with fake Bluetooth beacons — AirTag-like alerts, pairing requests, proximity pings.
- Proximity-based services overwhelmed: legitimate beacons drowned in noise from hundreds of fake ones.
- "Item Found Moving With You" alerts fire from malicious beacons planted on buses and trains — alert fatigue.
- Noise masks REAL tracking — 50 fake alerts a day and you ignore the one real stalking alert.
- Social engineering vector: "Connect to [Fake Device] for free WiFi" or "Tap to pair [Fake Speaker]."

**Example/Impact:** Flipper Zero or custom BLE transmitter broadcasting hundreds of fake device advertisements per second. Every iPhone/Android in range flooded with pairing popups. Legitimate anti-stalking alerts become useless in the noise.

### Visualization (canvas `c6`, 720×300)

Diagram: victim phone flooded by fake BLE beacons from both sides.

- **Victim phone:** dark blue rect (`#1a5276`) at (335,55) 40×65 with light blue screen; stack of 4 red notification bars on screen with fading alpha (0.7 down to 0.4).
- **Fake beacons:** red dots (`#e74c3c`, radius 8) with dark-red 11px labels below — left column at x=60, y=30/75/120/165: "Fake AirTag", "Free WiFi!", "Pair Device", "BT Speaker"; right column at x=590, same y values: "Item Found", "Connect Me", "Fake Tag", "AirPods".
- **Lines:** faint dashed red lines (`rgba(231,76,60,0.4)`, dash 2/4) from each beacon toward the phone at (355,87).
- **Labels (center):** bold 14px `#1a5276` at y=155 "Notification Flood — Alert Fatigue"; gray 12px `#7f8c8d` at y=185 "Real stalking alert lost in noise of 50 fake alerts/day".

## WiFi Positioning Database Staleness

- WiFi-based positioning relies on databases mapping AP MAC addresses (BSSIDs) to physical locations.
- APs move (office renovation), are replaced (new router = different MAC), or disappear (business closes).
- A 6-month-old database places you in a building that no longer has that AP — error grows with time.
- Nobody updates in real time: war-driving surveys are periodic, crowd-sourced updates lag months.
- Indoor accuracy decays exponentially as the AP landscape changes — 3 months acceptable, 12 months unusable.

**Example/Impact:** Employee relocates with their home router — the positioning database still maps that BSSID to the old city. Any device seeing that AP gets geolocated to the WRONG city entirely. Error: hundreds of kilometers from a single stale entry.

### Visualization (canvas `c7`, 720×300)

Line chart: position error growing with database age.

- **Title (bold 14px `#1a5276`, top center):** "Position Error vs. Database Age".
- **Axes:** dark gray (`#2c3e50`, width 1.5); x-axis from (70,170) to (680,170), y-axis from (70,170) to (70,35). X tick labels at x = 70, 192, 314, 436, 558, 680: "0mo", "1mo", "3mo", "6mo", "9mo", "12mo" (gray `#7f8c8d` 12px). Rotated y-axis label: "Error (meters)".
- **Error curve:** red (`#e74c3c`, width 3) power-law growth from (70,167): y = 167 − t^1.5 × 130 where t = (x−70)/610; area above curve to top filled `rgba(231,76,60,0.1)`.
- **Threshold line:** dashed green (`#27ae60`, dash 5/5, width 1.5) horizontal at y=125, labeled in green 12px: "Acceptable accuracy threshold (30m)".
- **Annotation (bold 14px `#e74c3c`, center, y=55):** "Unusable after ~6 months — errors exceed 100m+".

## Cross-Protocol Correlation (WiFi + BT + Cellular = Full Track)

- Each protocol alone is partial: WiFi says "device is in this area," BT says "device is in THIS store."
- Cellular adds "device traveled from A to B" — fuse all three for second-by-second indoor tracking, no gaps.
- Each sensor owner sees only their slice — the data broker who buys all three sees everything.
- Temporal correlation: WiFi, BT, and Cell hits all at 10:01 = one person, despite different random MACs.
- Defense against one protocol (e.g., WiFi off) doesn't help if the other two still triangulate you.

**Example/Impact:** Data broker purchases WiFi probe logs from malls, BT beacon data from retailers, and cell tower records from carriers. Fused: complete movement history at room-level precision, 24/7, for millions of people. Each source alone was "just analytics."

### Visualization (canvas `c8`, 720×300)

Fusion diagram: three protocol sources converging into a data-fusion box.

- **Protocol circles (radius 24, y=38):** "WiFi" at x=130 in `#2980b9` with sub-label "Area presence"; "Bluetooth" at x=350 in `#8e44ad` with "Store-level"; "Cellular" at x=570 in `#27ae60` with "City travel". White protocol names inside circles; colored info labels below; "(partial)" in gray `#7f8c8d` 11px under each at y=88.
- **Fusion arrows:** lines in `#1a5276` (width 2) from each circle at y=92 converging to (350,115).
- **Fusion box:** filled `#1a5276` rect at (250,110) 200×32 with white bold 13px text "DATA FUSION".
- **Down arrow:** dark red (`#c0392b`, width 2.5) from (350,142) to (350,158) with filled arrowhead.
- **Result text (center):** bold 15px `#c0392b` at y=182 "Complete second-by-second tracking with indoor precision"; gray 12px `#7f8c8d` at y=197 "Each source alone = \"just analytics.\" Combined = total surveillance."

## Regeneration instructions

- **Layout:** detail page — h1 + `.subtitle`, then one `h2` per pitfall (1.4em `#1a5276`, bottom border `2px solid #2980b9`), each followed by a single-row `.obj-table`: left `<td>` (40%) holds `.obj-title` (same text as the h2), a `<ul>` of bullets, and an **Example/Impact:** paragraph; right `<td>` (60%, centered) holds the canvas. Even table rows background `#fafcfe`.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`; `.philosophy` callout style defined (background `#f0f4f8`, left border `4px solid #2980b9`) though unused on this page. No nav bar, no back/home links.
- **Canvas:** each declares intrinsic `width="720" height="300"`; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, dark red `#c0392b`, purple `#8e44ad`, gray text `#666`/`#7f8c8d`.
