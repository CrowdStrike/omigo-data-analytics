# Dark Patterns / Subtle Psychological Manipulation

**Page type:** detail page (one h2 per pitfall, each followed by a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** 114. Dark Patterns / Subtle Psychological Manipulation

**Subtitle:** Design choices that manufacture consent, engagement, and urgency — the metrics measure manipulation, not preference.

## Default Opt-In Inflates Consent Metrics

- Pre-checked boxes count as "consent" — 90%+ opt-in rates
- Actual informed consent would yield 10-20% opt-in

**Example:** Email marketing: default opt-in = 94% "consent." Same form with unchecked box = 12%. The 82% gap is manufactured consent.

### Visualization (canvas `c1`, 720×200)

Grouped bar chart comparing consent rates under default opt-in vs explicit choice.

- **Background:** `#eaf2f8`. **Title (17px, `#1a5276`):** "Consent Rate: Default Opt-In vs Explicit Choice" at (175, 18).
- **Categories (x labels, 10px `#1a5276`):** Email Mktg, Data Sharing, Location, Cookies, Push Notif.
- **Data:** Default On `[94, 89, 87, 96, 91]` (%); Explicit `[12, 8, 15, 22, 18]` (%).
- **Bars:** per category two 35px-wide bars starting at x=70+i×130 (explicit offset +40); heights scaled by value/100 × 140px, baseline y=175. Default-on bars red `#e74c3c`, explicit bars green `#27ae60`. Percent value labels above each bar.
- **Legend (top right, 12px squares + 11px text `#1a5276`):** red "Default On", green "Explicit".

## Infinite Scroll/Autoplay Inflates Engagement

- Passive consumption ≠ active engagement
- Users "watch" 3 hours but remember nothing — metrics say "highly engaged"

**Example:** Platform reports 47 min avg session. User surveys: "I meant to spend 8 minutes." 83% of "engagement" is inertia, not intent.

### Visualization (canvas `c2`, 720×200)

Grouped bar chart of intended vs actual session duration by platform.

- **Background:** `#fef9e7`. **Title:** "Intended vs Actual Session Duration (minutes)" at (195, 18).
- **Platforms:** Social Feed, Video, News, Shopping, Gaming.
- **Data:** Intended `[8, 12, 10, 15, 20]` min (green `#27ae60`); Actual `[47, 68, 35, 42, 95]` min (red `#e74c3c`). Scale max 100 over 140px, baseline y=175, bars 35px wide at x=70+i×130 (actual offset +40).
- **Value labels:** "Nm" above each bar (e.g. "8m", "47m"), platform names below.
- **Legend:** green "Intended", red "Actual".

## Search Suggestions Steer User Queries

- Autocomplete shapes what users search for, not just predicts it
- Removing a suggestion reduces searches for that term by 40-80%

**Example:** Autocomplete suggests "product X problems" — searches for that spike 340%. Remove the suggestion — they drop to baseline in 48 hours.

### Visualization (canvas `c3`, 720×200)

Line chart of search volume across three phases: before, during, and after an autocomplete suggestion.

- **Background:** `#eafaf1`. **Title:** "Search Volume When Autocomplete Suggestion Added/Removed" at (130, 18).
- **Data (21 daily points, one continuous line):** before `[10, 12, 11, 10, 13, 11, 12]`, during `[11, 25, 38, 44, 42, 45, 44]`, after `[43, 30, 18, 12, 11, 10, 11]`. Scale max 50 over 150px, baseline y=185, x=60+i×30.
- **Line:** blue `#2980b9`, width 2.5.
- **Vertical dashed markers (dash 3/3):** green `#27ae60` at x=60+7×30 labeled "Suggestion added" (11px green); red `#e74c3c` at x=60+14×30 labeled "Suggestion removed" (11px red).
- **Label:** "Baseline" (`#1a5276`) at left near y for value 11.

## Recommendation Framing Manufactures "Preference"

- What you're shown determines what you "choose"
- Recommendation position accounts for 70% of selection probability

**Example:** Item shown in position #1 gets 35% clicks. Same item in position #8 gets 2%. The "preference" is really just placement.

### Visualization (canvas `c4`, 720×200)

Bar chart of click rate by recommendation position.

- **Background:** `#f4ecf7`. **Title:** "Recommendation Position vs Click Rate" at (225, 18).
- **Positions:** #1 through #10; **Click rates:** `[35, 22, 14, 9, 6, 4, 3, 2.5, 2, 1.5]` (%). Scale max 40 over 150px, baseline y=185, bars 40px wide at x=60+i×64.
- **Bar fill:** vertical gradient from `#8e44ad` (top) to `#6c3483` (bottom).
- **Labels:** position "#N" below each bar, percent value above (10px `#1a5276`).
- **Annotation (11px `#8e44ad`, at 420,60):** '"Preference" = Position bias'.

## A/B Tests Optimize Manipulation Not Value

- Tests select for what makes users click, not what helps them
- Winner is the most manipulative variant, not the most helpful

**Example:** A/B test: "Complete your purchase" vs "You'll lose your items forever!" — fear variant wins by 23%. Company ships manipulation.

### Visualization (canvas `c5`, 720×200)

Combined bar + line chart: conversion bars per copy variant with a hidden satisfaction line.

- **Background:** `#fdedec`. **Title:** "A/B Test Results: Helpful vs Manipulative Copy" at (190, 18).
- **Variants:** Neutral, Helpful, Urgent, Fear, Guilt, Shame.
- **Conversion bars:** `[4.2, 4.8, 6.1, 7.8, 8.5, 9.2]` (%), scale max 10 over 80px, baseline y=120, bars 35px wide at x=70+i×108. First two bars green `#27ae60`, remaining four red `#e74c3c`. Percent labels above bars, variant names below (9px).
- **Satisfaction line:** `[72, 78, 45, 28, 22, 15]`, orange `#f39c12` width 2, scale max 85 over 60px anchored at y=190, points at x=87+i×108.
- **Annotations (11px):** green "Conversion ↑" at (580,50); orange "Satisfaction (hidden)" at (540,180).

## Consent Theater (50-Page ToS)

- Nobody reads terms of service — average would take 76 minutes
- "I Agree" is meaningless consent — it's compliance theater

**Example:** Study: reading all ToS encountered in a year = 76 full working days. 97% of users click "agree" in under 3 seconds.

### Visualization (canvas `c6`, 720×200)

Bar chart of ToS reading time required vs actual time spent per service.

- **Background:** `#ebf5fb`. **Title:** "ToS Reading Time: Required vs Actual" at (235, 18).
- **Services:** social network, search engine, premium brand, e-commerce platform, streaming service, ride-hailing app.
- **Required (minutes, blue `#2980b9`, 50px-wide bars):** `[45, 38, 52, 62, 22, 28]`, scale max 70 over 130px, baseline y=180, x=70+i×108. Labels "Nmin" above bars.
- **Actual (seconds, red `#e74c3c`):** `[2.1, 1.8, 2.5, 1.5, 3.2, 1.2]` drawn as a tiny 15×2px red sliver beside each bar with "Ns" labels.
- **Legend (top left):** blue "Time needed", red "Time spent".

## Confirmshaming

- Small gray "no thanks" vs big green "YES!" — asymmetric choice architecture
- Decline option uses guilt/shame language

**Example:** "No thanks, I don't want to save money" in 8px gray vs "YES! SAVE 50% NOW!" in 24px green. Opt-out requires admitting a character flaw.

### Visualization (canvas `c7`, 720×200)

Illustrative mock-up of asymmetric button design (not a data chart).

- **Background:** `#fef9e7`. **Title:** "Confirmshaming: Visual Weight Asymmetry" at (215, 18).
- **Big accept button:** green `#27ae60` rounded rectangle (corner radius 10) spanning roughly x 200–520, y 50–130, with white bold 28px text "YES! SAVE 50% NOW!" at (235, 100).
- **Tiny decline option:** gray `#bdc3c7` 9px text "no thanks, I hate saving money" at (290, 155) — no button shape.
- **Annotations (12px red `#e74c3c`, right side):** "Font: 28px, 320x80 button" at (540, 80); "Font: 9px, no button, guilt text" at (540, 155).
- **Click-rate annotations (11px `#1a5276`):** "Click rate: 89%" at (540, 100); "Click rate: 11%" at (540, 170).

## Urgency Manufacturing

- "Only 2 left!" when inventory is 2,000
- Countdown timers that reset, "limited time" offers that never end

**Example:** "Only 2 rooms left at this price!" — tested: shown to 50,000 simultaneous users. Actual inventory: 847 rooms. Timer resets every visit.

### Visualization (canvas `c8`, 720×200)

Paired bar chart contrasting claimed scarcity with actual inventory.

- **Background:** `#fdedec`. **Title:** "Claimed Scarcity vs Actual Inventory" at (240, 18).
- **Products:** Hotel Room, Flight Seat, Product, Course Spot, Event Ticket.
- **Claimed (tiny red `#e74c3c` bars):** `[2, 3, 1, 5, 4]`, scaled value/20 × 40px, baseline y=170, 35px wide at x=70+i×130; labels '"N left!"' in red above.
- **Actual (tall blue `#2980b9` bars):** `[847, 124, 15000, 9999, 2300]`, scaled value/16000 × 140px (capped at 140px), offset +40; locale-formatted value labels in blue above.
- **Legend (top right):** red "Shown to user", blue "Actual stock". Product names below bars (9px `#1a5276`).

## Regeneration instructions

- **Layout:** domains detail-page convention: h1 + `.subtitle`, then per pitfall an unnumbered `<h2>` followed by a `.obj-table` (full-width, border-collapse) with a single `<tr>`: left `<td>` (40%) holds `.obj-title` (repeating the h2 text), a `<ul>` of bullets, and a `<p>` with a bolded "Example:" lead; right `<td>` (60%, centered) holds the canvas. Even rows get background `#fafcfe`. No thead, no nav, no badges, no cross-page links.
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px solid `#2980b9` bottom border; `.subtitle` `#666` 1.05em; ul 0.9em `#333`; `strong` `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`; `.philosophy` callout style available (background `#f0f4f8`, left border 4px `#2980b9`) though unused on this page.
- **Canvas:** HTML attributes declare `width="720" height="300"`, but the shared `setupCanvas(id)` helper resets each canvas to 720×200 CSS pixels, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. All charts drawn in a 720×200 coordinate space.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`/`#f39c12`, purple `#8e44ad`, gray `#bdc3c7`/`#666`.
- Card links elsewhere pointing to this page use the `.html` extension in regenerated HTML.
