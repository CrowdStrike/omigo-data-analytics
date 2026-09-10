# 4. Leaderboards & Social Proof

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one h2 + table per section, philosophy callouts top and bottom)
**HTML title tag:** 4. Leaderboards & Social Proof

**Subtitle:** How manufactured competition and fabricated scarcity turn browsing into urgency — and why it works even when you know it's happening.

## Callout (philosophy box, top)

**The psychology:** Social proof (Cialdini, 1984) — people use others' behavior as a shortcut for what's correct. Combine that with loss aversion and social comparison, and you get platforms that convert passive users into anxious participants.

## Section 1: Leaderboards Turn Cooperation Into Competition

**Obj-title:** Why Rankings Create Addiction

Math-box 1:

**The mechanics:**

1. Show your rank relative to peers
2. Make rank volatile (can drop if inactive)
3. Create tiers with visible status markers
4. Reset periodically to force re-engagement

`You're not competing against a goal. You're competing against people who never stop.`

Math-box 2:

**Why it works (loss aversion × status):**

- Losing rank feels 2× worse than gaining it feels good (Kahneman & Tversky)
- Your rank is visible to others → social identity tied to number
- Near a threshold? "Just one more" to maintain/advance

The system creates a `treadmill you can't leave without losing something`.

Bullets:

- **Duolingo:** Weekly leagues. Drop a tier if you stop. Pure fabricated competition.
- **LinkedIn:** "Top Voice" badges. SSI scores. Profile strength meters.
- **Gaming:** ELO decay. Seasonal resets force you to regrind.

### Visualization (canvas `canvas1`, 720×380)

League-tier diagram: four horizontal tier bars with promotion/relegation arrows.

- **Title (bold 14px, top center, `#1a5276`):** "The Leaderboard Treadmill".
- **Tier bars:** four rectangles at x=80, width 560, height 42, stroked 2px in tier color with translucent fill (tier color + `22` alpha suffix). Tier name bold 13px in tier color at left (x=95); "Top X%" 12px gray `#666` right-aligned at x=625.
  - Diamond, `#2980b9`, y=55, "Top 0.5%"
  - Gold, `#f39c12`, y=110, "Top 5%"
  - Silver, `#95a5a6`, y=165, "Top 20%"
  - Bronze, `#cd6155`, y=220, "Top 75%"
- **Arrows:** small green (`#27ae60`, 2px) upward arrow at x=350 between Gold and Diamond region (y 108→100); small red (`#e74c3c`) downward arrow at x=370 (y 100→108).
- **Loop labels (12px, y=285):** "↑ Promote top 10 weekly" in `#1a5276` centered at x=300; "↓ Relegate bottom 5 weekly" in `#e74c3c` centered at x=440.
- **Key insight (centered):** bold 12px `#333` at y=320: "You're not learning more. You're just losing less." Then 11px `#666`: "Inactivity = demotion. The leaderboard punishes stopping, not just failing." (y=340) and "Engagement metric goes up. Learning metric stays flat." (y=356).

## Section 2: "X People Are Viewing This Right Now"

**Obj-title:** Manufactured Urgency

Math-box 1:

**The signals:**

- "12 people are looking at this hotel right now"
- "Only 2 left at this price!"
- "Booked 3 times in the last hour"
- "Sale ends in 04:23:17"

None of these require truth. They require `plausible urgency`.

Math-box 2:

**What's actually happening:**

- "12 people viewing" = anyone who loaded the page in the last 30 min
- "Only 2 left" = at THIS price tier (plenty of rooms at $5 more)
- Countdown timers reset when you reload
- "Booked 3 times" = may include cancellations

The information is *technically not false* but `designed to mislead`.

Bullets:

- **Booking.com:** Master of this. Every element on the page pushes urgency.
- **Amazon:** "Only 3 left in stock" — often restocked within hours.
- **Flash sales:** Timer creates artificial deadline. Decision quality drops under time pressure.

### Visualization (canvas `canvas2`, 720×380)

UI mockup of a hotel booking card annotated with the psychological lever behind each urgency signal.

- **Title (bold 14px, top center, `#1a5276`):** "Anatomy of a Booking Page".
- **Card:** rectangle x=80, y=45, 560×280, fill `#fafafa`, 1px border `#ddd`.
- **Hotel name (bold 16px `#333`, left at x=100, y=75):** "Grand Hotel Example ★★★★".
- **Urgency signal lines (13px, left at x=110), each with a right-aligned bold 10px `#999` lever label "← LABEL" at x=620:**
  - y=105, `#e74c3c`: "🔥 In high demand — booked 14 times in last 24 hours" ← Bandwagon
  - y=135, `#e67e22`: "👁 12 people are looking at this right now" ← Competition
  - y=165, `#e74c3c`: "⚡ Only 2 rooms left at this price!" ← Scarcity
  - y=195, `#27ae60`: "⏰ Free cancellation until Aug 20" ← Risk removal
  - y=225, `#8e44ad`: "📉 Price dropped 23% vs last week" ← Anchoring
  - y=255, `#666`: "✓ Last booked 12 minutes ago" ← Recency
- **Price:** bold 22px `#1a5276` right-aligned at x=600, y=295: "$189"; 12px `#999` below: "per night".
- **Bottom notes (11px, centered):** red `#e74c3c` at y=350: "Every line is a separate psychological lever. 6 urgency signals on one card."; gray `#666` at y=368: "None are lies. All are designed to prevent careful comparison-shopping."

## Section 3: The Math of Social Proof

**Obj-title:** Why Numbers Override Judgment

Math-box 1:

**Conformity research (Asch, 1951):**

75% of people gave a clearly wrong answer at least once when confederates all gave the same wrong answer first.

Translated to product design:
"4.8 stars from 12,847 reviews" → you trust it
Even knowing review fraud exists.
`Volume of social signal > quality of signal.`

Math-box 2:

**Bandwagon metrics designed to compound:**

- "Join 2 million+ users" — proof by headcount
- "Trending" labels — self-fulfilling (attention → more attention)
- Star ratings — anchored to 5 (3.8 feels "bad" even if it's fine)
- "Most popular" — placement drives more clicks → stays popular

The metric is both the signal AND the cause of itself.

Bullets:

- **Survivorship:** Only happy customers leave reviews. 4.5 star average is the floor, not the signal.
- **Platform incentive:** Higher-rated items convert better. Platforms have no incentive to fight rating inflation.
- **The 4.7 problem:** Everything is 4.5–4.9. The signal has been compressed to nothing.

### Visualization (canvas `canvas3`, 720×380)

Bar histogram of star-rating distribution showing compression at the top.

- **Title (bold 14px, top center, `#1a5276`):** "Star Rating Distribution (Typical Platform)".
- **Data:** bars for 1★=3%, 2★=2%, 3★=5%, 4★=25%, 5★=65%. Scale: percentages mapped over 210px height with 70% max; bars 70px wide, 26px gap, origin (120, 280).
- **Axes:** x and y axes in `#1a5276`, 1.5px.
- **Bar style:** fill `rgba(26,82,118,0.35)` when pct > 20, else `rgba(26,82,118,0.15)`; 1px `#1a5276` stroke. Star label bold 12px `#333` below each bar; percentage 11px `#1a5276` above each bar.
- **Annotation:** dashed red horizontal line (`#e74c3c`, 1.5px, dash 4/3) at the 25% level; right-aligned 11px red label above it: "90% of products live between 4.2–4.9".
- **Bottom insight (centered):** bold 12px `#333` at y=320: "When everything is 4.5+, the rating carries zero information." Then 11px `#666`: "But \"4.2 vs 4.7\" still FEELS meaningful. That's the trick — compressed signal, intact emotional weight." (y=340) and "Platforms benefit from inflation: higher ratings → higher conversion → more revenue." (y=358).

## Callout (philosophy box, bottom)

**The defense:** Ask — "Would I want this if nobody else did?" and "Is this urgency real, or is the UI generating it?" If the countdown resets when you refresh, it was never a deadline.

## Regeneration instructions

- **Layout:** detail page. h1 + `.subtitle`, `.philosophy` callout, then per section: `<h2>N. Title</h2>` (numbered, bottom border `2px solid #2980b9`) followed by a `.obj-table` with one `<tr>`: left `<td>` (45%) holds `.obj-title`, two `.math-box` divs, and a `<ul>`; right `<td>` (55%, centered) holds the canvas. Closing `.philosophy` callout after the last section.
- **Page CSS:** body `-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif`, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with `border-bottom: 2px solid #2980b9`; subtitle `#666` 1.05em; `.obj-table td` border `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em weight 600 `#1a5276`; `ul` 0.9em `#333`; `strong` `#1a5276`; `.philosophy` background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em; `.math-box` background `#f8fafb`, border `1px solid #e0e0e0`, radius 6px, padding 16px 20px, 0.9em; `.math-box code` background `#eef2f7`, padding 2px 6px, radius 3px. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, bar fill `rgba(26,82,118,0.35)`.
- **Canvas:** intrinsic width/height attributes 720×380; shared `setupCanvas(id, w, h)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- In regenerated HTML, any card links use `.html` extensions (this page has none).
