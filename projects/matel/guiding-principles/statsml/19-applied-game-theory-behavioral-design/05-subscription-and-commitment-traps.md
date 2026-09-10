# 5. Subscription & Commitment Traps

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one h2 + table per section, philosophy callouts top and bottom)
**HTML title tag:** 5. Subscription & Commitment Traps

**Subtitle:** Annual vs monthly, free trials that auto-convert, and the math of why giving things away is the most profitable strategy.

## Callout (philosophy box, top)

**The core mechanic:** Make signing up frictionless, make canceling effortful, and exploit the gap between intention ("I'll cancel before it charges") and action (forgetting, procrastinating, or feeling sunk-cost attachment). The math works even with high churn because the *inertial mass* of non-cancellers compounds.

## Section 1: Annual vs Monthly Pricing

**Obj-title:** The Lock-In Discount

Math-box 1:

**Typical SaaS pricing:**

Monthly: `$15/mo` ($180/yr)
Annual: `$10/mo` ($120/yr) — "Save 33%!"

What you see: a discount.
What the company sees: `guaranteed revenue × 12 months`.

Math-box 2:

**Why annual is better for the company:**

1. Monthly churn = 5%/mo → only 54% survive to month 12
2. Expected revenue per monthly user: ~$100/yr
3. Annual user pays $120 upfront. No churn risk for 12 months.

The "discount" is actually a `20% premium over expected monthly revenue`.
You pay more. It just doesn't feel like it.

Bullets:

- **Sunk cost:** Month 7, you stop using it. But you already paid. "I'll use it eventually."
- **Auto-renewal:** Year 2 charges before you remember to cancel.
- **Cash flow:** Company gets $120 in January. Monthly gives $15. Time value of money favors annual.

### Visualization (canvas `canvas1`, 720×380)

Line chart comparing cumulative expected revenue per user: flat annual line vs churn-decayed monthly curve.

- **Title (bold 14px, top center, `#1a5276`):** "Expected Revenue: Monthly vs Annual (per user)".
- **Axes:** origin (80, 300), plot 550×230, `#1a5276` 1.5px; x-axis label "Month" (11px `#666`), rotated y-axis label "Cumulative Revenue ($)"; month tick labels 1–12 in 10px `#999`.
- **Annual line:** horizontal blue `#2980b9`, 2.5px, at the $120 level (y scale max $180); right label bold 11px `#2980b9`: "Annual: $120 (guaranteed)".
- **Monthly curve:** orange `#e67e22`, 2.5px, starting at origin; cumulative revenue computed per month as `survival *= 0.95; cumRev += 15 * survival` for months 1–12 (ends near $100); right label bold 11px `#e67e22`: "Monthly: ~$100 expected".
- **Gap annotation:** vertical dashed red line (`#e74c3c`, 1px, dash 4/3) near right edge (x = origin+gw−40) between the two lines, with centered bold 12px red label "+$20" at its midpoint.
- **Bottom insight (centered):** bold 11px `#333` at y=345: "The \"33% discount\" actually yields 20% MORE revenue than monthly pricing."; 11px `#666` at y=363: "Because monthly users churn. Annual users are locked in."

## Section 2: Free Trials That Auto-Convert

**Obj-title:** The Math of "First Month Free"

Math-box 1:

**The funnel:**

1,000 sign up for free trial (credit card required)
→ 400 forget or don't bother canceling (40%)
→ 400 auto-convert at $15/mo
→ 200 stay 1 month then cancel ($3,000)
→ 120 stay 3+ months ($5,400+)
→ 80 become long-term ($14,400/yr)

Total revenue from 1,000 free signups: `~$22,800/yr`

Math-box 2:

**Why this works even at 60% immediate cancel:**

Cost of 1,000 free months: $0 marginal cost (digital product)
Revenue from the 40% who don't cancel: $22,800
CAC (customer acquisition cost) per paying user: $0

Compare to paid acquisition: $30-50 per signup on Google Ads
`Free trial converts better AND cheaper than advertising.`

Bullets:

- **The "credit card required" filter:** Eliminates tire-kickers. Only intent-qualified people enter.
- **Status quo bias:** Once set up, canceling requires active effort. Inertia favors the company.
- **Endowment effect:** After using it free, it feels like something you "have" and would "lose."

### Visualization (canvas `canvas2`, 720×380)

Centered funnel diagram of trapezoid stages narrowing downward.

- **Title (bold 14px, top center, `#1a5276`):** "Free Trial Conversion Funnel (1,000 signups)".
- **Stages (trapezoids, centered horizontally; start y=50, stage height 52, gap 10; each fills with stage color + `33` alpha, 1.5px stroke in stage color; label 12px `#333` and count bold 13px in stage color inside):**
  - "Free trial signups", 1,000, top width 500, `#2980b9`
  - "Don't cancel (inertia)", 400, width 340, `#27ae60`
  - "Stay 1+ months", 320, width 280, `#e67e22`
  - "Stay 3+ months", 120, width 180, `#8e44ad`
  - "Long-term (12+ mo)", 80, width 130, `#1a5276` (bottom narrows to 70% of its width)
- **Revenue annotation (centered):** bold 13px `#27ae60` at y=345: "Total revenue from \"free\" trial: ~$22,800/yr"; 11px `#666` at y=365: "Cost of 1,000 free months: ~$0 (digital marginal cost)".

## Section 3: Asymmetric Friction (Easy In, Hard Out)

**Obj-title:** The Dark Pattern Arsenal

Math-box 1:

**Sign up:** 1 click. Google SSO. Done.
**Cancel:** Settings → Account → Subscription → Manage → Cancel → "Are you sure?" → "Here's 50% off" → "We're sorry to see you go" → "Please tell us why" → "Your cancellation will take effect at..."

`5-7 steps vs 1 step.` Every step is a chance you give up.

Math-box 2:

**Retention offers in the cancel flow:**

- "Pause for 1 month instead?" — resets the clock
- "50% off for 3 months?" — locks you in again
- "Downgrade to free tier?" — keeps your data, keeps you in ecosystem

Each option is designed for a different objection.
The goal isn't to make you happy — it's to make you `not-cancel`.

Bullets:

- **NYT / cable:** Must call to cancel. Agents trained to retain, not process.
- **Amazon Prime:** 6 clicks to cancel. At each step, shown what you'll "lose."
- **Gym memberships:** Join online, cancel in-person or by certified mail.

### Visualization (canvas `canvas3`, 720×380)

Side-by-side step-count comparison: one green sign-up box vs seven red cancel boxes.

- **Title (bold 14px, top center, `#1a5276`):** "Asymmetric Friction: Sign Up vs Cancel".
- **Left column (header bold 13px `#27ae60`, centered at x=190, y=50):** "SIGN UP". One box (x=60, y=75, 260×32) with `#27ae60` 1.5px stroke and `#27ae6022` fill containing 12px `#333` text "Click \"Start Free Trial\"". Below: bold 36px `#27ae60` "1 step" (y=180) and 12px `#666` "~10 seconds" (y=200).
- **Right column (header bold 13px `#e74c3c`, centered at x=520, y=50):** "CANCEL". Seven numbered boxes (x=380, start y=65, step 38, 280×30, fill `#e74c3c08`, stroke `#e74c3c44` 1px, 11px `#333` text): "1. Settings → Account", "2. Subscription → Manage", "3. \"Are you sure?\"", "4. \"Here's 50% off\"", "5. \"Tell us why\"", "6. Confirm cancellation", "7. Wait for confirmation email". Below: bold 14px `#e74c3c` "7 steps" (y=345) and 12px `#666` "~5-15 minutes" (y=365).
- **Divider:** bold 24px `#999` "vs" centered between the columns at y=180.

## Callout (philosophy box, bottom)

**The defense:** Calendar reminders 3 days before trial ends. Never give annual unless you've used it monthly for 3+ months. Use virtual cards with spending limits for trials. The system profits from your forgetfulness — make forgetting impossible.

## Regeneration instructions

- **Layout:** detail page. h1 + `.subtitle`, `.philosophy` callout, then per section: `<h2>N. Title</h2>` (numbered, bottom border `2px solid #2980b9`) followed by a `.obj-table` with one `<tr>`: left `<td>` (45%) holds `.obj-title`, two `.math-box` divs, and a `<ul>`; right `<td>` (55%, centered) holds the canvas. Closing `.philosophy` callout after the last section.
- **Page CSS:** body `-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif`, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with `border-bottom: 2px solid #2980b9`; subtitle `#666` 1.05em; `.obj-table td` border `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em weight 600 `#1a5276`; `ul` 0.9em `#333`; `strong` `#1a5276`; `.philosophy` background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em; `.math-box` background `#f8fafb`, border `1px solid #e0e0e0`, radius 6px, padding 16px 20px, 0.9em; `.math-box code` background `#eef2f7`, padding 2px 6px, radius 3px. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, bar fill `rgba(26,82,118,0.35)`.
- **Canvas:** intrinsic width/height attributes 720×380; shared `setupCanvas(id, w, h)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- In regenerated HTML, any card links use `.html` extensions (this page has none).
