# Cross-Domain / Cross-Platform Transfer

**Page type:** detail page (two-column obj-table layout: text left 50%, canvas right 50%, one h2 + one-row table per pitfall)
**HTML title tag:** 110. Cross-Domain / Cross-Platform Transfer

**Subtitle:** When you train on one domain and deploy on another — the distributions, behaviors, and feature spaces are different even when they seem 'similar.'

## Mobile Data → Web Deployment

- 80% of training data from mobile app (short sessions, swipe-based, impulse behavior)
- Model deployed on website (long sessions, click-based, research behavior)
- Same product but the conversion funnel is different — session length runs 5× longer on web
- Bounce rate means different things on mobile versus web — one metric name, two real behaviors
- Model optimized for mobile impulse → fails on web deliberation

**Example:** E-commerce model trained on mobile "add to cart in 30 seconds" behavior predicts web users (who browse for 10 minutes) as "not interested" — killing recommendations for deliberate shoppers.

### Visualization (canvas `c1`, declared 720×300, drawn at 720×200)

Side-by-side funnels: short mobile journey vs long web journey.

- **Mobile funnel (left):** blue `#2980b9` header "Mobile Journey" at (80, 20). Three steps "See", "Tap", "Buy" as 40px-tall centered trapezoid bars starting at x≈60, widths 80/65/50 px, fill `rgba(41,128,185, 0.9 − i·0.2)`, white step labels; vertical spacing 50px starting y=35. Caption "~30 sec" in `#1a5276` at (60, 195).
- **Web funnel (right):** red `#e74c3c` header "Web Journey" at (420, 20). Seven steps "Land", "Browse", "Compare", "Read Reviews", "Add Cart", "Checkout", "Buy" as 20px-tall centered bars around x=380–580, widths 200 shrinking by 20px per step, fill `rgba(231,76,60, 0.9 − i·0.08)`, white 12px labels; vertical spacing 23px starting y=30. Caption "~10 min" in `#1a5276` at (440, 195).

## One Geography → Another

- US user behavior ≠ India ≠ Japan ≠ Brazil
- Payment preferences differ (credit card vs UPI vs cash-on-delivery)
- Content consumption patterns differ (Western individualism vs collectivist sharing)
- Price sensitivity differs by 10×
- Model trained on US data deployed in India: every assumption wrong

**Example:** US-trained "high intent" signal (adds item + enters payment) fails in India where 60% choose cash-on-delivery and adding items is exploratory, not committed.

### Visualization (canvas `c2`, declared 720×300, drawn at 720×200)

Grouped bar chart: three metrics across four geographies.

- **Title (17px, `#1a5276`, at (180, 18)):** "Same Metric, Completely Different Patterns".
- **Geographies (x labels `#333` at y=190):** US, India, Japan, Brazil; group start x = 100 + i·150.
- **Series (three 20px-wide bars per group, 4px apart, baseline y=170, height = value × 1.5):**
  - "Credit Card %" `#2980b9`: `[85, 15, 70, 40]`
  - "Mobile %" `#27ae60`: `[60, 90, 80, 75]`
  - "Avg Cart $" `#e74c3c`: `[80, 12, 45, 25]`
- **Legend (13px, swatches at y=30, spaced 150px starting x=100):** colored 12px square + metric label in `#333`.

## One Product's Users → Another Product

- One music streaming platform's listening patterns ≠ a rival music service's listening patterns
- The two services differ in demographics, in catalog, and in algorithm — three shifts at once
- One streaming service's viewing ≠ another's (different content library creates different behavior)
- You can't transfer user models across products because the PRODUCT shaped the behavior you measured

**Example:** One music platform's "discover weekly" logic applied to a rival music service fails — the first platform's users were TRAINED by the algorithm to explore; the rival's users stick to known artists.

### Visualization (canvas `c3`, declared 720×300, drawn at 720×200)

Two overlaid Gaussian distribution curves, shifted and scaled differently.

- **Title (17px, `#1a5276`, at (150, 18)):** "User Behavior Distributions: Product A vs Product B".
- **Product A curve:** blue `#2980b9`, width 2.5; Gaussian peak height 80px, center at x-offset 250 of a 600px span (plot origin x=60, baseline y=170), sigma 80.
- **Product B curve:** red `#e74c3c`, width 2.5; Gaussian peak height 55px, center at x-offset 380, sigma 120.
- **Labels:** "Product A users" in `#2980b9` at (140, 55); "Product B users" in `#e74c3c` at (420, 95).
- **Caption (13px, `#666`, at (220, 195)):** "Looks similar but shifted — transfer fails".

## Historical Era → Current

- Desktop-era (2005-2012): long sessions, few pages deep, bookmark-based navigation
- Mobile-first (2015+): short sessions, app-based, notification-driven
- Training on historical web data for a mobile product yields the wrong session model entirely
- The same transfer also breaks the engagement model and the conversion model — every layer wrong
- The MEDIUM changed everything

**Example:** "Average session = 12 minutes" from 2010 desktop data used to set mobile engagement thresholds where average session is 45 seconds — everything looks like "low engagement."

### Visualization (canvas `c4`, declared 720×300, drawn at 720×200)

Timeline with two era blocks above and below it.

- **Title (17px, `#1a5276`, at (180, 18)):** "Behavior Patterns Flipped Between Eras".
- **Timeline:** horizontal gray `#999` line (width 2) at y=120 from x=60 to x=680; tick marks and 13px `#666` year labels at 2005, 2008, 2010, 2012, 2015, 2018, 2022 (spaced 88px starting x=80).
- **Desktop era block (above line, left):** 300×70 rectangle at (70, 40), fill `rgba(41,128,185,0.3)`; 15px `#2980b9` text: "Desktop Era: 12min sessions, bookmarks" / "deep navigation, few pages".
- **Mobile era block (below line, right):** 300×50 rectangle at (370, 145), fill `rgba(231,76,60,0.3)`; 15px `#e74c3c` text: "Mobile Era: 45sec sessions, notifications" / "app-based, swipe & tap".

## App vs Web for SAME User

- Same person: on app = quick check (30 seconds, specific intent), on web = research mode (20 minutes, browsing)
- Their APP behavior predicts nothing about their WEB behavior
- Same human, different context → different distribution
- Combining app+web data without a "source" feature: garbage

**Example:** User checks bank app 2×/day for 15 seconds (balance check). Same user on web banking: 25-minute session (paying bills, transfers). Merged data shows "bipolar" engagement pattern.

### Visualization (canvas `c5`, declared 720×300, drawn at 720×200)

Side-by-side session profiles for the same user.

- **Title (17px, `#1a5276`, at (170, 18)):** "Same User — Two Completely Different Profiles".
- **App profile (left):** blue label "App" at (80, 45); eight short random spikes (15px wide, 10–25px tall, random heights) in `rgba(41,128,185,0.7)`, baseline y=80, spaced 35px starting x=60. Caption (13px, `#666`): "30s each, quick checks" at (70, 100).
- **Web profile (right):** red label "Web" at (430, 45); three long blocks (70px wide, 50–90px tall, random heights) in `rgba(231,76,60,0.7)`, baseline y=140, spaced 100px starting x=410. Caption (13px, `#666`): "20min sessions, deep research" at (420, 160).
- **Connector:** dashed `#333` line (dash 5/3, width 1.5) from (340, 70) to (400, 70) with 14px `#333` labels "SAME" / "PERSON" beside it.

## B2B Model Applied to B2C

- B2B: 5 decision makers, 6-month sales cycle, $50K ACV, relationship-driven
- B2C: one buyer, impulse, $50 purchase, ad-driven
- "Churn prediction" model from B2B assumes long relationships and slow degradation signals
- Applied to B2C, that same model instead meets instant churn with zero warning signal beforehand
- The result: completely wrong features, wrong timescales, and wrong signals across the board

**Example:** B2B churn model uses "30-day declining login trend" — in B2C, users churn in ONE day with no warning signal. The observation window is longer than the entire B2C lifecycle.

### Visualization (canvas `c6`, declared 720×300, drawn at 720×200)

Two timelines contrasting lifecycle cadences.

- **Title (17px, `#1a5276`, at (220, 18)):** "Incompatible Lifecycle Cadences".
- **B2B timeline (top):** blue `#2980b9` label "B2B" at (30, 55); horizontal blue line (width 3) at y=50 from x=80 through six 95px-spaced tick markers labeled in 11px `#333` (positioned above the line): Prospect, Demo, Trial, Negotiate, Close, Onboard. Caption (13px, `#666`): "← 6 months →" at (300, 70).
- **B2C timeline (bottom):** red `#e74c3c` label "B2C" at (30, 140); short red line (width 3) at y=135 from x=80 to x=180 with tick markers at both ends; 12px `#333` labels "See Ad" at (75, 155) and "Buy" at (170, 155). Caption (13px, `#666`): "← 5 minutes →" at (95, 175) and "B2B churn window (30 days) > entire B2C lifecycle" at (250, 150).

## English-Trained NLP on Multilingual Data

- Model trained on English text applied to translated text and code-switched text (Hindi+English mixed)
- The same model also meets formal vs informal registers and domain-specific jargon it never saw
- "Sentiment = negative" in English sarcasm ≠ negative in Japanese indirect speech
- Tone, formality, politeness all encode differently across languages/cultures

**Example:** English sentiment model flags Japanese business email as "negative" — the formal/indirect phrasing ("it might be difficult...") is actually polite decline, not negativity.

### Visualization (canvas `c7`, declared 720×300, drawn at 720×200)

Horizontal diverging sentiment bars for four languages expressing the same dissatisfaction.

- **Title (17px, `#1a5276`, at (210, 18)):** "Same Sentiment, Different Expression".
- **Rows (spaced 40px starting y=45; name in 14px `#333` at x=30, quoted text in 12px `#666` at x=120):**
  - English — '"This is terrible"' — score −0.9, bar color `#e74c3c`
  - Japanese — '"It might be difficult..."' — score −0.2, bar color `#f39c12`
  - Hindi — '"Kya baat hai!" (sarcastic)' — score +0.6, bar color `#27ae60`
  - Brazilian PT — '"Que legal..." (ironic)' — score +0.4, bar color `#2980b9`
- **Sentiment bars:** gray `#eee` 250×16 track at x=380 per row; colored fill extends from the center mark proportional to score (negative left, positive right); thin `#333` center tick.
- **Axis captions (12px, `#666`, at y=195):** "Negative" at x=380, "Positive" at x=590, and "ALL expressing dissatisfaction — model reads them differently" at x=160.

## Synthetic/Simulated Training → Real Deployment

- Model trained in simulator (clean, controlled, perfect physics) deployed in real world (noisy, messy, unexpected)
- Self-driving in sim: 99.9% safe. Real world: 95% (the 5% gap = lives)
- Game AI trained against bots → fails against humans (humans are irrational)
- Fraud model trained on synthetic fraud patterns → misses real fraud (which doesn't look like the simulation)

**Example:** Autonomous drone trained in simulation (perfect GPS, no wind, clean images) deployed outdoors: GPS drift, wind gusts, sun glare — performance drops from 99% to 70%.

### Visualization (canvas `c8`, declared 720×300, drawn at 720×200)

Two performance lines over time: smooth high simulation vs volatile lower real world.

- **Title (17px, `#1a5276`, at (220, 18)):** "Simulator vs Real World Performance".
- **Axes:** gray `#999` L-shaped axes from (60, 30) down to (60, 175) and across to (680, 175); 12px `#666` labels: "Performance" (left), "Time / Scenarios" (bottom center), y-tick labels "99.9%" (y≈45), "95%" (y≈80), "70%" (y≈130).
- **Sim line:** blue `#2980b9`, width 2.5; nearly flat around y=40 with a tiny sine ripple (±3px), spanning x=70 to 670.
- **Real line:** red `#e74c3c`, width 2.5; centered around y=100 with large deterministic pseudo-random noise (±20px, seeded LCG) plus a slow sine drift (±10px).
- **Legend (12px):** blue line swatch labeled "Simulation (stable, high)" and red swatch "Real World (volatile, lower)" at top right (x≈480, y=35–60).

## Regeneration instructions

- **Layout:** standard detail-page structure — one `<h2>` per pitfall (1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px), each followed by a one-row `.obj-table`: left `<td>` (40%) holds `.obj-title` (same text as h2) + bullet list + bold-labeled Example paragraph; right `<td>` (60%, centered) holds the canvas. Even rows background `#fafcfe`; cell borders `1px solid #e0e0e0`, padding 20px 24px.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; ul 0.9em `#333`; p 0.95em `#333`; `strong` in `#1a5276`; `.obj-title` 1.05em weight 600 `#1a5276`. `.philosophy` callout style defined (background `#f0f4f8`, left border `4px solid #2980b9`) but unused. No nav bar, no back/home links.
- **Canvas:** HTML attributes declare `width="720" height="300"`, but the shared `setupCanvas(id)` helper renders each at 720×200 CSS pixels — it sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Default font 17px system; smaller fonts (11–15px) for labels as noted.
- **Palette:** primary blue `#1a5276`/`#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#f39c12`, gray text `#666`/`#999`/`#333`.
- Card links elsewhere point to this page as `domains/110-cross-domain-transfer.html` in regenerated HTML.
