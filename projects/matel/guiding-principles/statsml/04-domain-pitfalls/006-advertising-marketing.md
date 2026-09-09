# Advertising Data Pitfalls

**Page type:** detail page (one h2 per pitfall, each with a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** Advertising Data Pitfalls

**Subtitle:** Advertising measurement is fundamentally compromised by attribution ambiguity, self-reporting platforms, cross-device fragmentation, and a rapidly shrinking data landscape due to privacy regulation.

## Attribution is Broken

**Obj-title:** Platform Conversion Claims Sum to 2-3× the Actual Total

- Customer journeys span 5-20 touchpoints across weeks before the purchase decision finally closes
- Last-click attribution ignores all of them and assigns 100% credit to the final interaction
- Upper-funnel awareness channels (display, video) are systematically undervalued by click-based models
- Those same channels drive the initial consideration that makes every later click possible
- Multi-touch attribution models require heroic assumptions about interaction effects that cannot be validated
- Each ad platform reports independently, and their sum regularly exceeds total actual conversions by 2-3x
- The "true" attribution is fundamentally unknowable without randomized holdout experiments

**Example:** A D2C brand tracked a customer who saw a video platform ad, clicked a social network retargeting ad, searched the brand on search engine, received an email, then purchased via a direct visit. Last-click gave 100% credit to "direct" -- the least informative touchpoint. Each platform separately claimed the conversion.

### Visualization (canvas `canvas1`, 720×240)

Customer journey diagram: five touchpoint circles along a dashed timeline, with last-click credit labels and a "fair credit" bar row below.

- **Title (top center, `#1a5276`, 17px):** "Customer Journey: 5 Touchpoints, Last-Click Gets All Credit".
- **Journey line:** dashed blue `#2980b9` (dash 5/5, width 2) from x=60 to x=660 at y=100; touchpoints evenly spaced along it as 20px-radius circles.
- **Touchpoints (name above, day label below in `#666`, credit label in bold 13px below that):**
  - "video platform Ad" — Day 1, credit "0%", gray fill `#bdc3c7`, gray outline `#95a5a6`.
  - "social network Retarget" — Day 5, credit "0%", gray.
  - "search engine Search" — Day 8, credit "0%", gray.
  - "Email Campaign" — Day 12, credit "0%", gray.
  - "Direct Visit" — Day 14, credit "100%", red fill `#e74c3c`, dark red outline `#c0392b`; below it bold green `#27ae60` 13px label "PURCHASE".
- **Fair-credit bars (y=170):** translucent blue `rgba(41,128,185,0.4)` horizontal bars under each touchpoint sized by a fair model's credit `[25, 20, 30, 15, 10]`, each captioned in blue 10px "Fair: N%" (Fair: 25%, Fair: 20%, Fair: 30%, Fair: 15%, Fair: 10%).
- **Commentary (bold red 12px, bottom center):** "Last-click attribution ignores all awareness-building touchpoints".

## Incrementality is Unknown

**Obj-title:** Brand Search Ads Switched Off — Revenue Loss Near Zero

- Most "conversions" attributed to ads would have happened anyway -- the ad merely intercepted an existing intent
- Brand search ads often claim credit for users already navigating to the site
- Without randomized holdout tests, incremental lift is indistinguishable from correlation
- Platforms have no incentive to measure incrementality because it would reveal lower true ROAS

**Example:** An e-commerce company ran a landmark study turning off search engine brand search ads. Expected revenue loss: significant. Actual loss: near zero. Users who would have clicked the paid link simply clicked the organic result instead. The entire brand search budget was capturing existing intent, not creating new demand.

### Visualization (canvas `canvas2`, 720×240)

Venn diagram of organic buyers vs ad-attributed buyers with a large overlap.

- **Title (top center, `#1a5276`, 17px):** "Incrementality: Who Actually Needed the Ad?".
- **Circles:** two 85px-radius circles at (260, 130) and (400, 130); left fill red `#e74c3c` at 30% alpha with outline `#c0392b`; right fill blue `#2980b9` at 30% alpha with outline `#2471a3`.
- **Labels:** left circle (three lines, `#c0392b` 12px): "Would Buy" / "Anyway" / "(Organic)"; right circle (`#2471a3`): "Saw Ad &" / "Bought" / "(Attributed)"; overlap region center: bold `#1a5276` "OVERLAP" over 11px "~60-80%".
- **Annotation (bold green `#27ae60` 13px, right side):** "True Incremental" / "(only 20-40%)" with a green arrow pointing to the right-only region of the blue circle.
- **Bottom annotation (`#333` 12px, center):** "Platforms report the entire blue circle as "ad-driven conversions"".

## Cross-Device Double-Counting

**Obj-title:** 3.6 Devices Per Person Turn 1 Sale Into 3 Conversions

- The average consumer uses 3.6 devices, each appearing as a unique user to ad platforms
- A single conversion generates multiple attribution claims -- one per device that served an impression
- Deterministic cross-device matching requires logged-in users; probabilistic matching has 30-60% error rates
- Frequency caps fail across devices, leading to overexposure and wasted spend
- Reported reach and unique user counts are systematically inflated by device fragmentation

**Example:** A user researches a product on their phone during lunch, sees a retargeting ad on their work laptop, and purchases on their home tablet. Three platforms each report one unique conversion. The brand's dashboard shows 3 conversions when only 1 sale occurred.

### Visualization (canvas `canvas3`, 720×240)

Hub diagram: one central user icon connected by dashed lines to three device boxes, each claiming a conversion.

- **Title (top center, `#1a5276`, 17px):** "One User, Three Devices, Three Claimed Conversions".
- **Center:** simple person icon in `#2c3e50` (head circle radius 15 + shoulder half-circle radius 25) at (360, 100), captioned in bold 11px: "1 Real User" / "1 Real Purchase".
- **Device boxes (90×50 rectangles with blue `#2980b9` 2px outline; bold device name in `#1a5276`, platform in `#666` 11px, claim in bold red `#e74c3c` 11px; dashed gray `#bdc3c7` connector lines, dash 3/3, to the center):**
  - "Phone" / "photo-sharing platform" / "Claims: 1 conversion" at (120, 80).
  - "Laptop" / "search engine Ads" / "Claims: 1 conversion" at (360, 195).
  - "Tablet" / "social network" / "Claims: 1 conversion" at (600, 80).
- **Bottom summary (bold red 13px, center):** "Dashboard reports: 3 conversions | Reality: 1 conversion | Inflation: 3x".

## Creative Fatigue vs Targeting Decay

**Obj-title:** CTR 2.1% → 0.8% Fits Stale Creative and Tired Audience Alike

- CTR declines over the campaign lifetime, but the cause of that decline is genuinely ambiguous
- Two readings fit the same curve: the creative has gone stale, or the audience is exhausted
- Frequency-driven fatigue and audience saturation produce identical performance curves
- Refreshing creative when the problem is targeting wastes production budget without fixing decay
- Expanding audience when the problem is creative fatigue dilutes targeting quality unnecessarily
- Without controlled experiments isolating each factor, optimization decisions are essentially guesses

**Example:** A campaign's CTR dropped from 2.1% to 0.8% over 6 weeks. The team produced 5 new creative variants at significant cost. CTR briefly recovered to 1.1% then resumed decline -- the real issue was audience exhaustion in a narrow targeting segment, not creative fatigue.

### Visualization (canvas `canvas4`, 720×240)

Three-line chart: observed CTR with two indistinguishable hypothesis curves.

- **Title (top center, `#1a5276`, 17px):** "CTR Decline: Creative Fatigue or Audience Exhaustion?".
- **Data (weeks 1–8, x labels "Wk 1" … "Wk 8"):**
  - Observed CTR `[2.1, 1.9, 1.6, 1.3, 1.1, 0.9, 0.85, 0.8]` — solid dark `#2c3e50`, width 3.
  - If Creative Fatigue `[2.1, 1.85, 1.5, 1.2, 1.0, 0.85, 0.75, 0.7]` — dashed red `#e74c3c`, dash 8/4, width 2.
  - If Audience Exhaustion `[2.1, 1.95, 1.7, 1.4, 1.15, 0.95, 0.88, 0.82]` — dashed blue `#2980b9`, dash 4/4, width 2.
- **Axes:** margins top 30 / right 20 / bottom 40 / left 55; y-axis 0.0%–2.4% with labels every 0.6% and `#eee` gridlines, rotated y label "CTR".
- **Legend (top right, line swatches):** "Observed CTR" (dark solid), "If Creative Fatigue" (red dashed), "If Audience Exhaustion" (blue dashed).
- **Annotation (bottom center):** bold red 18px "?" above red 11px "Cannot distinguish without controlled experiment".

## Privacy Erosion Shrinking Features

**Obj-title:** Identifiers Fell From ~70% to ~25% of Users After ATT

- Third-party cookies deprecated across all major browsers by 2024, eliminating cross-site tracking
- iOS App Tracking Transparency (ATT) reduced identifier availability from ~70% to ~25% of users
- GDPR/CCPA consent rates average 40-60%, creating systematic non-response bias in available data
- Lookalike audiences degrade as seed audience data becomes incomplete and stale
- Attribution windows shortened from 28 days to 7 days, missing long-consideration purchases

**Example:** After iOS 14.5 ATT enforcement, a mobile gaming advertiser saw their measurable install attribution drop by 55%. Their CPA "increased" 3x not because performance worsened, but because conversions became invisible. Media mix models estimated true performance decline was only 15%.

### Visualization (canvas `canvas5`, 720×240)

Declining area/line chart of available targeting data indexed to 100, with regulatory event markers.

- **Title (top center, `#1a5276`, 17px):** "Available Targeting Data Per User Over Time".
- **Data:** years `['2018','2019','2020','2021','2022','2023','2024','2025']`; data available (indexed to 100) `[100, 95, 82, 68, 52, 38, 28, 20]`.
- **Series:** red `#e74c3c` line, width 2.5, with shaded area under the curve in `rgba(231, 76, 60, 0.1)`.
- **Axes:** margins top 30 / right 20 / bottom 45 / left 55; y-axis 0%–100% with labels every 25% and `#eee` gridlines, rotated y label "Data Available"; x-axis one label per year.
- **Event annotations (`#1a5276` 10px above short dashed gray `#7f8c8d` leader lines, dash 2/2, attached to the curve):** "CCPA" at 2020 (y=82); "iOS 14 ATT" at 2021 (y=68); "GDPR Enforcement" at 2022 (y=52); "Cookie Deprecation" at 2024 (y=28).
- **Bottom annotation (bold red 11px, center):** "80% of targeting signals lost in 7 years".

## Regeneration instructions

- **Layout:** standard detail-page structure: h1, `.subtitle` paragraph, then per pitfall an `<h2>` (1.4em `#1a5276`, 2px solid `#2980b9` bottom border) followed by a `.obj-table` with a single `<tr>`: left `<td>` (40%) contains `.obj-title` + `<ul>` bullets + `.example` callout, right `<td>` (60%, centered) contains the canvas. Even table rows have background `#fafcfe`.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; bullets 0.9em `#333`; `strong` in `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`. No nav bar, no back/home links.
- **Example callout:** `.example` — background `#f0f4f8`, left border `3px solid #2980b9`, padding 10px 14px, 0.9em. (A `.philosophy` class with 4px border also exists in the stylesheet but is unused on this page.)
- **Canvas:** all canvases 720×240, declared with intrinsic `width`/`height` attributes and scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), one IIFE per chart.
- **Palette:** primary blue `#1a5276`, chart blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22` (unused here), dark `#2c3e50`, dark red `#c0392b`, grays `#bdc3c7`/`#95a5a6`/`#7f8c8d`, text `#333`/`#666`.
