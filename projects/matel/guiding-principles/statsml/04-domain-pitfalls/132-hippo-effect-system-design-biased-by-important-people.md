# Evaluation Distribution Bias (HiPPO / Visibility Bias)

**Page type:** detail page (h2 section heading per pitfall, each followed by a one-row two-column obj-table: text left ~40%, canvas right ~60%)
**HTML title tag:** 132. Evaluation Distribution Bias (What Gets Evaluated Gets Optimized, Everything Else Rots)

**Subtitle:** When the evaluation distribution differs from the usage distribution, quality concentrates where evaluation looks and everything else rots.

## "Important Query" Bias in Search/E-Commerce

- "iphone" query hand-tuned to perfection (the vendor is influential, everyone demos with it)
- Board types "iphone" → "search works great!" Ship it.
- "usb c cable 6ft braided" (80% of real queries = long-tail) → garbage results, nobody ever checks
- Evaluation: "95% satisfaction on top-100 queries!" (all hand-optimized)
- Actual long-tail quality: 45%

**Example:** The system is fundamentally broken but LOOKS great because only visible/important/demo cases are verified. The big-brand partner's page always works; random seller's page breaks. Bank's top customer always processed correctly; small accounts have bugs nobody notices. The evaluation is biased by the SAME thing that biased the system.

### Visualization (canvas `c1`, 720×200)

Two bars where width encodes traffic volume and height encodes quality.

- **Title (17px `#1a5276`, at 20,25):** "Search Quality: Top-100 Queries vs Long-Tail".
- **Head bar:** green `#27ae60` rect at (80,55) size 120×110 (narrow, tall) with white 15px labels "Top 100" and "95% sat.".
- **Long-tail bar:** red `#e74c3c` rect at (280,110) size 350×55 (wide, short) with white label "Long-tail (80% of traffic): 45% satisfaction".
- **Labels (14px `#333`):** "\"iphone\"" at (100,180); "\"usb c cable 6ft braided\" and millions like it" at (290,180).
- **Note (12px `#666`):** "Width = traffic volume" at (500,195).

## "Pet Query" / Dogfooding Bias

- Employees test product with THEIR interests (not representative of users)
- Engineer who likes woodworking → "table saw" over-optimized
- PM into fitness → "yoga mat" always returns great results
- CEO collects watches → "rolex submariner" is perfect
- These = 0.001% of actual queries. The 99.999% nobody internally types → unverified

**Example:** "Search quality is great!" — as measured by the 50 queries employees personally care about, not the 50 million real queries. Bug reports cluster around employee hobbies, not user pain points. Dogfooding sample ≠ real user distribution. QA is biased toward employee interests.

### Visualization (canvas `c2`, 720×200)

Two circles contrasting the tiny tested query set with the huge untested one.

- **Title (17px `#1a5276`, at 20,25):** "Query Coverage: Employee Dogfooding vs Real Users".
- **Small circle:** solid blue `#2980b9` fill, radius 25 at (150,110), labeled inside in blue-on-fill "Tested" and below (14px `#333`): "50 employee" / "\"pet queries\"".
- **Large circle:** fill `rgba(231,76,60,0.3)` with red `#e74c3c` stroke (width 2), radius 80 at (450,110), labeled "Untested" in red and below: "50M real user queries" / "(unverified)".
- **Caption (12px `#666`):** "table saw, yoga mat, rolex submariner..." at (80,195).

## Demo Video / Investor Pitch Optimization

- Entire product polished for the 3-minute demo scenario
- Everything off the demo path = neglected
- "Let me show you..." flow is perfect, real user flow is broken

**Example:** Startup demo: onboarding → first action → wow moment. Flawless. Real user: settings page crashes, export broken, edge case in billing, password reset loop. Nobody demos those paths.

### Visualization (canvas `c3`, 720×200)

Path diagram: one smooth green demo path vs several broken red paths ending in ✗ marks.

- **Title (17px `#1a5276`, at 20,25):** "Product Quality: Demo Path vs All Other Paths".
- **Demo path:** thick green `#27ae60` line (width 4) rising gently: (80,100) → (200,80) → (320,70) → (440,60); green 13px label "Demo: Onboard → Action → Wow" at (100,55).
- **Other paths:** three thin red `#e74c3c` lines (width 1.5) descending in the lower half (x offset +50 from data arrays `[80,130,150,140,200,160,250,155]`, `[80,145,130,160,180,170,220,165]`, `[80,160,120,175,170,180,210,190]`), each terminating in a red 16px "✗" mark.
- **Annotations:** red 13px "Settings, export, billing, password reset = broken" at (300,150); gray `#666` 12px "Investment follows demo visibility, not user frequency" at (250,195).

## Benchmark Goodhart (ML Models)

- Public benchmarks focus on specific tasks
- Models optimized for benchmark, fail on real usage
- Leaderboard position ≠ production quality

**Example:** Model tops GLUE/SuperGLUE. Deployed: fails on misspelled queries, slang, domain-specific jargon, multi-turn context. Benchmark coverage = 5% of production distribution.

### Visualization (canvas `c4`, 720×200)

Two horizontal bars: benchmark score vs production accuracy.

- **Title (17px `#1a5276`, at 20,25):** "ML Model: Benchmark Score vs Production Accuracy".
- **Bars:** green `#27ae60` rect at (100,55) size 160×50 with white 15px "Benchmark: 96.2%"; red `#e74c3c` rect at (100,120) size 80×50 with white 14px "Prod: 61%".
- **Annotations (14px `#333`):** "Benchmark: clean, curated, in-domain" at (340,75); "Production: misspelled, slang, jargon," at (340,100); "multi-turn, out-of-distribution" at (340,120).
- **Caption (12px `#666`):** "Benchmark coverage ≈ 5% of production distribution" at (300,195).

## App Store Screenshot Optimization

- The 5 screens shown in store are perfected
- The 200 other screens = inconsistent/broken
- First impression optimized, sustained experience neglected

**Example:** Screenshots: beautiful dashboard, clean profile, slick onboarding. Reality: settings page from 2019, broken dark mode on 80% of screens, inconsistent spacing everywhere users actually spend time.

### Visualization (canvas `c5`, 720×200)

Pictogram: 5 large polished screens vs a 3×20 grid of faded broken screens.

- **Title (17px `#1a5276`, at 20,25):** "App Store: Screenshotted Screens vs Total Screens".
- **Polished screens:** 5 green `#27ae60` rects (40×60) with `#1a5276` outlines at x = 80 + i·50, y=60.
- **Broken screens:** 60 small rects (12×20) in translucent red `rgba(231,76,60,0.3)`, 3 rows × 20 columns starting at (380,55), 16px horizontal and 25px vertical spacing (represents the 200 screens).
- **Labels (14px):** green "5 perfect screens" at (100,140); red "200 inconsistent/broken screens" at (380,140).
- **Caption (12px `#666`):** "Users spend 90% of time in the red zone" at (280,195).

## Conference Talk / Public Evaluation Bias

- System works flawlessly for EXACT scenario presented at conference
- Crashes with slight variation from demo script
- J.D. Power survey asks specific areas → those get all investment

**Example:** Conference demo: real-time ML inference on curated dataset. Audience impressed. Production: 40% of inputs trigger edge cases not in demo. "Works on my machine" at scale.

### Visualization (canvas `c6`, 720×200)

Bullseye diagram: green demo core surrounded by a red ring of failing variations.

- **Title (17px `#1a5276`, at 20,25):** "System Robustness: Demo Script vs Input Variations".
- **Center:** solid green `#27ae60` circle, radius 40 at (200,110), white 13px labels inside: "Demo" / "100%".
- **Ring:** red `#e74c3c` stroked circle, line width 20, radius 72 around the same center.
- **Annotations (14px red):** "Slight variations: 60% crash rate" at (340,90); "Real inputs: 40% trigger edge cases" at (340,115); "not in demo script" at (340,135).
- **Caption (12px `#666`):** "\"Works on my machine\" at scale" at (340,195).

## HiPPO Path Optimization

- Highest Paid Person's Opinion determines what gets tested
- CEO's workflow: flawless. Intern's workflow: broken for months.
- Bug priority = f(reporter_seniority), not f(user_impact)

**Example:** CEO reports font size issue → fixed in 2 hours. 10,000 users report data export crash → backlog for 3 months. Resources follow hierarchy, not impact.

### Visualization (canvas `c7`, 720×200)

Two horizontal bars: tiny CEO bug fixed fast vs huge user-impact bug backlogged.

- **Title (17px `#1a5276`, at 20,25):** "Bug Fix Priority: Reporter Seniority vs User Impact".
- **CEO bug:** small red `#e74c3c` rect at (80,60) size 60×30; 13px `#333` labels "CEO: font size → 2hr fix" at (150,80) and "Impact: 1 person" at (150,95).
- **User bug:** wide blue `#2980b9` rect at (80,120) size 500×40 with white 13px label "10,000 users: data export crash → 3 month backlog".
- **Annotations:** 14px `#333` "Priority = f(reporter_seniority)" at (350,80); red "NOT f(user_impact)" at (350,100); gray `#666` 12px "Resources follow hierarchy, not impact" at (300,195).

## The Unified Principle: Evaluation = Optimization Target

- What you measure is what improves (and ONLY what improves)
- Unmeasured dimensions actively deteriorate (resources diverted)
- The evaluation mechanism IS the optimization target
- Fix: evaluate on USAGE distribution, not VISIBILITY distribution

**Example:** Same pattern everywhere: evaluation_distribution ≠ usage_distribution. Top-100 queries, demo paths, employee hobbies, conference scripts, app screenshots, HiPPO workflows — all are VISIBLE subsets that get optimized while the invisible 95% rots. The cure: sample evaluation FROM production traffic, not from stakeholder visibility.

### Visualization (canvas `c8`, 720×200)

Two overlaid distributions: narrow evaluation spike vs wide usage bell curve.

- **Title (17px `#1a5276`, at 20,25):** "The Core Problem: Evaluation ≠ Usage Distribution".
- **Evaluation distribution:** narrow green triangle spike filled `rgba(39,174,96,0.7)` — vertices (150,170), (200,50), (250,170).
- **Usage distribution:** wide Gaussian bell filled `rgba(231,76,60,0.3)` with red `#e74c3c` outline (width 2): y = 170 − 120·exp(−((x−380)/150)²) over x = 80..650, baseline y=170.
- **Labels (14px):** green "What we evaluate" at (130,185); red "What users actually do" at (420,185).
- **Annotation (13px `#1a5276`):** "Fix: sample evaluation FROM production traffic" at (350,50).

## Regeneration instructions

- **Layout:** for each of the 8 pitfalls, an `<h2>` section heading (1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px) followed by a one-row `.obj-table`: full-width table, left `<td>` (40%) holds `.obj-title` + `<ul>` bullets + an `<p><strong>Example:</strong> ...</p>` paragraph, right `<td>` (60%, centered) holds the canvas. Even rows background `#fafcfe`; cell borders `1px solid #e0e0e0`, padding 20px 24px. Note: the h1 text ("Evaluation Distribution Bias (HiPPO / Visibility Bias)") differs from the HTML title tag; keep both as given. Arrows/≠/— in bullets are HTML entities (&rarr;, &ne;, &mdash;) in the source.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; `ul` 0.9em `#333`; `strong` `#1a5276`; `.obj-title` 1.05em weight 600 `#1a5276`. Unused `.philosophy` class: background `#f0f4f8`, left border `4px solid #2980b9`. No nav bar, no back/home links.
- **Canvas:** HTML attributes declare `width="720" height="300"` but a shared init loop overrides every canvas to a 720×200 logical size — backing store sized to rendered width × `window.devicePixelRatio` (display capped via `style.maxWidth`), CSS size fixed at 720×200 px, `ctx.scale` back to logical coordinates. All chart coordinates above are in the 720×200 space. Chart titles 17px, labels 12–15px, `-apple-system, sans-serif`.
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60`, red `#e74c3c`, gray text `#666`/`#333`.
- Card links elsewhere referencing this page use the `.html` extension in regenerated HTML.
