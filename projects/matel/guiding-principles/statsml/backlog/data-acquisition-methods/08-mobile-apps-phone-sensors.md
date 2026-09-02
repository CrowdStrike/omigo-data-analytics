# Mobile Apps & Phone Sensors — A Sensor Platform in Every Pocket

**Page type:** detail page (four titled sections, each a two-column row: text left 45%, canvas right 55%)
**HTML title tag:** Mobile Apps & Phone Sensors — A Sensor Platform in Every Pocket

**Subtitle:** Ship an app and, with user permission, you can collect telemetry and sensor readings from a fleet of phones — location, motion, pressure, and in-app behavior from an instrument that is already in everyone's pocket.

**Intro callout (blue-left-border box):** Building a dedicated sensor network means buying hardware, deploying it, and maintaining it. A mobile app skips all three steps: the hardware is already purchased, charged, and carried everywhere by its owner. The trade is that every reading is gated by a permission prompt the user must accept — the fleet is rented from its users one Allow tap at a time, and it shrinks whenever they change their minds.

## 1. What a phone can measure

A modern smartphone carries more instruments than a 1990s field laboratory, and an app can read them all — but only after the operating system asks the user first.

- **GPS location:** a position fix every few seconds, assisted by Wi-Fi and cell.
- **Accelerometer + gyro:** continuous motion and orientation sampling.
- **Motion inference:** steps, falls, and gait from that same stream.
- **Barometer:** pressure precise enough to detect a floor change.
- **Screen & in-app events:** taps, sessions, and feature usage.
- **Permission gate:** every sensor waits on an OS prompt and an Allow tap.

Key point: The instrument is already deployed: shipping an app turns thousands of privately owned phones into a data-collection fleet, but the operating system — not the app developer — decides what a prompt must ask, and the user decides what to answer.

### Visualization (canvas `c1`, 720×420)

Phone diagram with four labeled sensor callouts, each connector interrupted by an orange permission-gate badge.

- **Title (bold 14px `#1a5276`, centered, y=22):** "One device, many instruments — each behind a permission gate"
- **Phone body:** rectangle 120×250 at (300, 70), white fill, 2px `#1a5276` border; inner screen rectangle 100×193 at (310, 92) filled `rgba(26,82,118,0.12)`; speaker slot (30×4 rounded bar, `#bbb`) centered at y=81; home dot (radius 5, 1.5px `#bbb` circle) centered at y=302. Inside the screen, centered at x=360: bold 11px `#1a5276` "YOUR APP" (y=180) and 10px `#666` "telemetry SDK" (y=196).
- **Sensor callouts (each 190×58, white fill, 2px border in item color; bold 12px label in color at y+20, two 11px `#666` sublines at y+36 and y+50, all left-aligned at x+10):**
  - left top (30, 85) "GPS location" `#1a5276` — "position fix every few" / "seconds when allowed"
  - left bottom (30, 235) "Barometer" `#e67e22` — "air pressure — altitude," / "floor-level changes"
  - right top (500, 85) "Accelerometer + gyro" `#27ae60` — "motion and orientation —" / "steps, falls, gait"
  - right bottom (500, 235) "Screen & in-app events" `#8e44ad` — "taps, sessions, feature" / "usage inside the app"
- **Connectors:** 1.5px `#bbb` lines from each callout's inner edge to the nearest phone edge (left callouts to x=300, right callouts to x=420), drawn at the callout's vertical center; a filled dot (radius 3.5, item color) where each line meets the phone.
- **Permission gates:** on each connector midpoint, a 52×18 badge, fill `rgba(230,126,34,0.12)`, 1.5px `#e67e22` border, bold 9px `#e67e22` centered "ALLOW?".
- **Note (11px `#999`, centered, y=352):** "each sensor flows only after the user accepts an OS permission prompt"
- **Caption (12px `#999`, centered, y = h−14):** "The hardware is deployed and charged by its owner — the app only asks to listen"

## 2. Crowdsensing at scale

Once many phones run the same app, individually noisy readings aggregate into measurements no dedicated sensor network can match, because the phones are already everywhere the phenomenon happens.

- **Passive pings:** Google Maps traffic comes from phones on the road.
- **The signal:** anonymized position and speed, aggregated per road segment.
- **Active reports:** Waze adds user-reported crashes, hazards, and closures.
- **Dedicated sensors:** loop detectors cover only the wired corridors.
- **Phone fleet:** every road with drivers on it is instrumented.
- **Feedback loop:** rerouting drivers changes the traffic being measured.

Key point: Millions of small, cheap, poorly calibrated sensors beat a handful of precise dedicated ones on coverage: aggregation averages away individual noise, and the fleet is maintained, powered, and transported by its users at no cost to the collector.

### Visualization (canvas `c2`, 720×380)

Road with phone-ping dots (sparse and fast on the left, dense and slow on the right) aggregating into a colored traffic-segment bar below.

- **Title (bold 14px `#1a5276`, centered, y=22):** "From individual pings to a live traffic map"
- **Road:** two 1.5px `#999` horizontal lines at y=70 and y=126 from x=40 to x=680; dashed (10/8) 1px `#ccc` center line at y=98.
- **Phone pings:** filled `#1a5276` dots (radius 5), each with a rightward speed arrow (1.5px `#1a5276` line with small arrowhead). Free-flow group, long 20px arrows, alternating lanes: (70, 84), (135, 112), (200, 84), (262, 112). Congested group, short 7px arrows: (430, 84), (462, 112), (492, 84), (520, 112), (548, 84), (576, 112), (606, 84), (634, 112).
- **Waze report flag:** at x=480 a 1.5px `#e67e22` vertical line from y=70 up to y=48 with a small filled `#e67e22` triangle pennant; bold 10px `#e67e22` centered above (y=40): "user report: crash ahead".
- **Ping label (11px `#666`, left-aligned at x=40, y=150):** "each phone reports anonymized position + speed every few seconds"
- **Aggregation arrow:** 2px `#999` vertical line at x=360 from y=168 to y=205 with a filled downward arrowhead; 11px `#999` label left-aligned at (378, 190): "aggregate by road segment".
- **Segment bar (y=230, height 26):** three abutting rectangles — x=60–300 fill `#27ae60`, x=300–460 fill `#e67e22`, x=460–660 fill `#e74c3c`; below each, centered bold 11px label in the segment color (y=278): "free flow", "slowing", "congested"; under those, 10px `#666` (y=294): "avg 60 mph", "avg 35 mph", "avg 12 mph"; then 10px `#999` centered at y=316: "(illustrative speeds)".
- **Caption (12px `#999`, centered, y = h−14):** "Millions of small sensors beat any dedicated sensor network on coverage"

## 3. Research through apps

In 2015 Apple released ResearchKit, an open-source framework that turned the phone from a survey device into a clinical study instrument, with consent, tasks, and measurement all delivered in the app.

- **ResearchKit (2015):** Apple's open-source framework for studies as apps.
- **Active tasks:** short guided exercises that read sensors while performed.
- **mPower:** Parkinson's symptoms via tap, gait, and voice tasks.
- **At-home frequency:** measurements far more often than clinic visits allow.
- **Apple Heart Study:** Stanford enrolled hundreds of thousands of watch wearers.
- **Validation:** irregular-rhythm alerts checked against ECG patches.
- **The trade:** huge cohorts fast, but noisier data and faster dropout.

Key point: The phone became the study site: recruitment that once required years of clinic visits collapsed into weeks of app installs, at the cost of accepting consumer-grade sensors and self-selected, fast-attriting participants.

### Visualization (canvas `c3`, 720×340)

Study pipeline of four boxes with arrows, plus two named-study example rows below.

- **Title (bold 14px `#1a5276`, centered, y=22):** "A clinical study delivered as an app"
- **Pipeline boxes (each 150×72 at y=70, white fill, 2px border in box color; bold 12px label in color at y+22, two 11px `#666` sublines at y+42 and y+58, all centered on the box):**
  - x=40 "ENROLLMENT" `#1a5276` — "in-app e-consent," / "eligibility screen"
  - x=205 "PHONE TASK" `#27ae60` — "tap test, gait walk," / "voice sample"
  - x=370 "MEASUREMENTS" `#e67e22` — "sensor readings per task," / "repeated for months"
  - x=535 "DATASET" `#8e44ad` — "de-identified, shared" / "with researchers"
- **Arrows:** 2px `#999` horizontal lines between consecutive boxes at y=106 with filled right-arrowheads.
- **Example rows (each 640×46 at x=40, white fill, 2px border in row color; bold 12px name in color at (x+14, y+19); 11px `#666` description at (x+14, y+36)):**
  - y=190 "mPower (2015)" `#27ae60` — "Parkinson's symptoms measured through finger-tapping, gait, and voice tasks on participants' own iPhones"
  - y=248 "Apple Heart Study (2017–2019)" `#1a5276` — "Stanford enrolled hundreds of thousands of watch wearers to validate irregular-rhythm notifications"
- **Caption (12px `#999`, centered, y = h−14):** "Recruitment that once took years of clinic visits now takes weeks of app installs"

## 4. What limits collection

The fleet is rented, not owned: every stage between install and month six is a point where the user, the OS vendor, or the battery can end the data stream — and each exit reshapes who remains in the sample.

- **Permission opt-in:** the fleet is the opt-in rate times the install base.
- **ATT (2021):** cross-app tracking became an explicit opt-in on iOS.
- **Store disclosures:** privacy labels declare collection before install.
- **Battery cost:** background GPS drains visibly, so users switch it off.
- **Attrition:** month-six actives are a small fraction of installs.
- **Selection bias:** every exit has a reason, so survivors are not typical.

Key point: A consent-gated fleet measures the people who keep saying yes: the funnel from install to long-term participant shrinks at every stage, and because each exit has a reason, the remaining sample is biased in ways the raw data never shows.

### Visualization (canvas `c4`, 720×400)

Shrinking-cohort funnel: four horizontal bars from installs down to month-six actives, each with a reason line beneath.

- **Title (bold 14px `#1a5276`, centered, y=22):** "The consent funnel — who is left in the sample (illustrative shares)"
- **Bars (height 30, left edge x=190, max width 470; row label 11px `#555` right-aligned at x=180 on the bar's vertical center; percentage bold 12px in bar color at bar end + 10, left-aligned; reason line 10px `#999` left-aligned at x=190, 14px below the bar):**
  - y=70 "Installs the app" — width 470, `#1a5276`, "100%" — reason: "the only stage the developer fully controls"
  - y=140 "Grants sensor permission" — width 291, `#27ae60`, "62%" — reason: "prompt declined means no data at all — since ATT (2021), cross-app tracking needs its own opt-in"
  - y=210 "Keeps background collection on" — width 160, `#e67e22`, "34%" — reason: "battery drain is visible — users switch background access off"
  - y=280 "Still active at month 6" — width 70, `#e74c3c`, "15%" — reason: "attrition — the users who remain are not typical of those who installed"
- **Note (11px `#666`, centered, y=352):** "each stage drops non-randomly, so the surviving sample differs from the install base"
- **Caption (12px `#999`, centered, y = h−14):** "The dataset describes the people who kept saying yes — not the people who installed"

## Regeneration instructions

- **Template:** backlog multi-section detail page. After h1, `.subtitle`, and `.intro` callout, one `.lang-section` per numbered section: `h2` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px padding-bottom), then a `table.layout` with one `<tr>`: left `<td class="text-col">` (45%) with lead paragraph, bullets, `.key-point`; right `<td class="viz-col">` (55%) with the canvas. Section h2 headings carry the "1.–4." numbers shown above.
- **Bullet style:** each bullet is a bold label plus a short one-line phrase ("**Label:** short phrase."); labels are colored via `li strong { color: #1a5276; }` in the page CSS; split content into more bullets rather than let a line wrap.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`; `.subtitle` `#666` 0.95rem; `.intro` — background `#f0f4f8`, `border-left: 3px solid #2980b9`, padding 8px 12px, 0.9rem, 32px margin-bottom; `.key-point` — background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem; `ul` 0.92rem; `li strong` `#1a5276`; canvases `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px; `.lang-section` 40px margin-bottom.
- **Canvases:** intrinsic width 720, heights 420/380/340/400 as specified; shared `setupCanvas(id, h)` helper scales by `window.devicePixelRatio` (multiply backing store, `ctx.scale(dpr,dpr)`). Canvas fonts use `-apple-system, sans-serif`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`; grays `#555`/`#666`/`#888`/`#999`/`#bbb`/`#ccc`; translucent fills `rgba(26,82,118,0.12)`, `rgba(230,126,34,0.12)`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
