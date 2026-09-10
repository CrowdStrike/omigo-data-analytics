# 2. Streaks, Coins & Variable Rewards

**Page type:** detail page (numbered h2 sections, each an obj-table row: text left 45% with math-boxes and bullets, canvas right 55%; philosophy callouts at top and bottom)
**HTML title tag:** 2. Streaks, Coins & Variable Rewards

**Subtitle:** How learning apps, social platforms, and games use behavioral psychology to create daily habits — loss aversion, variable reward schedules, and artificial currencies.

## Callout (philosophy box, top)

**The underlying psychology:** Humans are more motivated by losing something they have than gaining something equivalent (loss aversion, Kahneman & Tversky 1979). A 30-day streak feels like an asset — breaking it feels like a loss. The app didn't give you a reward for showing up. It gave you something to lose for NOT showing up.

## 1. Streaks — Loss Aversion as Engagement

**Obj-title:** You're Not Gaining. You're Avoiding Loss.

**Math-box 1:**
**The setup:**

Day 1: You open Duolingo. "Day 1 streak! 🔥"
Day 15: "15-day streak! Keep it going!"
Day 30: You're tired. Don't feel like it.

But: `"I'll lose my 30-day streak."`

You open the app. Not because you want to learn. Because you don't want to lose something you "built."

**Math-box 2:**
**Why it works:**

- Loss aversion: losing a streak feels `2× worse` than gaining it felt good (Kahneman's ratio)
- Sunk cost: "I've invested 30 days — can't waste that now"
- Escalating commitment: the longer the streak, the more painful the loss → the more compelled you are

The app converts a *positive habit* into an *anxiety obligation*.

- **Duolingo:** Streak + streak freeze (paid) + push notifications at night
- **Snapchat:** Snap streaks between friends — social pressure added to loss aversion
- **GitHub:** Contribution graph — blank squares feel like failure

### Visualization (canvas `canvas1`, 720×380)

Line chart: anxiety of breaking a streak vs streak length — logarithmic growth curve.

- **Title (bold 14px `#1a5276`, centered):** "Anxiety of Breaking Streak vs Streak Length" at y=20.
- **Axes:** origin (80, 300), plot width 560, height 230, stroke `#1a5276` width 2. X label "Streak Length (days)"; y label "Anxiety of Missing a Day" rotated −90°. X tick labels 11px `#666` at days 0, 7, 14, 30, 60, 90 (rendered as "0d"…"90d", positioned at d/90 of width).
- **Curve:** red `#e74c3c` width 3, anxiety(d) = log(d+1)/log(91) for d = 1…90 (steep at first, then plateaus); area under curve filled `rgba(231,76,60,0.1)`.
- **Annotations (11px):** green `#27ae60` left-aligned near origin: `"Meh, whatever"`; red `#e74c3c` right-aligned near curve top: `"I CANNOT miss today"`.
- **Streak freeze callout (orange `#e67e22`, 11px, centered near the 30-day x position, offset +80px):** "💎 \"Buy streak freeze: $4.99\"" / "(monetizing your anxiety)".

## 2. Variable Rewards — The Slot Machine Inside Your App

**Obj-title:** Unpredictable Rewards Are More Addictive Than Guaranteed Ones

**Math-box 1:**
**The experiment (Skinner, 1950s):**

Rat A: Press lever → always gets food pellet. Presses when hungry.
Rat B: Press lever → SOMETIMES gets food pellet (random). `Presses compulsively, even when full.`

Variable ratio reinforcement creates the strongest behavioral persistence.

**Math-box 2:**
**Where you see it:**

- **Pull to refresh:** Sometimes new content, sometimes not → you keep pulling
- **Loot boxes:** Random reward of unknown value → compulsive opening
- **Notifications:** Sometimes interesting, mostly not → you always check
- **Social media feed:** Mix of boring and great posts → infinite scroll seeking the next hit

- **Fixed rewards** (same every time) → you check when you need it
- **Variable rewards** (unpredictable) → you check compulsively hoping for the good one
- The uncertainty is the point. Knowing exactly what you'll get kills the loop.

### Visualization (canvas `canvas2`, 720×380)

Two-line time series comparing check frequency under fixed vs variable rewards.

- **Title (bold 14px `#1a5276`, centered):** "Engagement: Fixed vs Variable Rewards" at y=20.
- **Axes:** origin (80, 300), plot width 560, height 220, stroke `#1a5276` width 2. X label "Time →"; y label "Check Frequency" rotated −90°.
- **Fixed line:** blue `#2980b9` width 2, flat at 30% of plot height with small sine jitter (sin(i×0.5)×5 over 100 steps).
- **Variable line:** red `#e74c3c` width 3, high at 70% of plot height, erratic spikes (sin(i×0.8)×15 + sin(i×2.1)×10) with a slight upward trend (+20px over the span).
- **Legend (top right, 12px):** red line swatch + "Variable rewards (compulsive)" in `#e74c3c`; blue swatch + "Fixed rewards (functional)" in `#2980b9`.
- **Example captions below x-axis (11px, centered):** "Social feeds, loot boxes, pull-to-refresh" in `#e74c3c`; "Email, calendar alerts, bank balance" in `#2980b9`.

## 3. Coins & Points — Why Not Just Use Dollars?

**Obj-title:** Abstraction Makes Spending Painless

**Math-box 1:**
**The trick:**

You buy 1,000 "gems" for $9.99.
A power-up costs 150 gems.

Quick: how much did that power-up cost in dollars?
`$1.50.` But you didn't think that. You thought "150 gems — I have 1,000, that's fine."

**Math-box 2:**
**Why it works:**

- **Decoupling:** The pain of paying ($9.99) happened ONCE, in the past. Spending gems doesn't trigger the "paying" pain response.
- **Confusing conversion rates:** 1,000 gems for $9.99 makes per-item math hard. That's deliberate.
- **Leftover waste:** Items cost 150 gems. You buy in bundles of 500. You always have unusable remainders → buy more.
- **Earned + bought mix:** "I earned 200 gems playing, so I'm only spending the 'paid' ones" — money is fungible but the app makes it feel different.

- **Casino chips:** The original version — colored tokens feel like "playing money" not real money
- **V-Bucks, Robux, FIFA Points:** All designed to obscure the dollar-to-item conversion
- **Credit cards:** Same principle — abstract payment hurts less than handing over cash

### Visualization (canvas `canvas3`, 720×380)

Side-by-side scenario boxes comparing the pain of spending real money vs game currency, with an abstraction chain below.

- **Title (bold 14px `#1a5276`, centered):** "Pain of Spending: Real Money vs Game Currency" at y=20.
- **Left box (real money):** rect (60, 60) size 260×130, fill `#fef5f5`, stroke `#e74c3c` width 2. Centered text: 'Direct: "$1.50 for a power-up"' bold 13px `#e74c3c`; "Brain response:" 12px `#333`; '😬 "Meh, not worth it"' bold 20px `#e74c3c`; "Pain of paying activates. You skip it." 11px `#666`.
- **Right box (gems):** rect (390, 60) size 260×130, fill `#f0fff4`, stroke `#27ae60` width 2. Centered text: 'Abstracted: "150 gems"' bold 13px `#27ae60`; "Brain response:" 12px `#333`; '🎮 "I have 1000, sure!"' bold 20px `#27ae60`; "No pain. Spending feels free. You buy it." 11px `#666`.
- **Abstraction chain (below boxes):** heading "The abstraction chain:" 13px `#1a5276` centered; then one left-flowing 12px line of steps: "$9.99 (one-time pain)" in `#e74c3c` → "1,000 gems (abstract pool)" → "150 per item (feels cheap)" (middle steps `#666`) → "buy buy buy" in `#27ae60`, separated by "→".
- **Leftover-waste note (12px `#e67e22`, centered, two lines):** "Bonus trick: items cost 150 gems, bundles sell 500. You always have leftover gems →" / '"might as well buy another bundle to use them up" → the ratchet never stops.'

## Callout (philosophy box, bottom)

**The pattern:** Streaks exploit loss aversion. Variable rewards exploit dopamine prediction errors. Coins exploit payment abstraction. All three make engagement feel like a personal choice when it's a designed behavioral loop.

## Regeneration instructions

- **Layout:** h1 + `.subtitle`, top `.philosophy` callout, then numbered `<h2>` sections (1.4em `#1a5276`, 2px solid `#2980b9` bottom border), each containing a `.obj-table` with one `<tr>`: left `<td>` (45%) holds `.obj-title`, `.math-box` divs, and a `<ul>`; right `<td>` (55%, centered) holds the canvas. Closing `.philosophy` callout after the last section.
- **Page style:** body -apple-system/Segoe UI sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` `#1a5276`; `.philosophy` background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em; `.math-box` background `#f8fafb`, border `1px solid #e0e0e0`, radius 6px, padding 16px 20px, 0.9em, with `code` background `#eef2f7`. No nav bar, no back/home links.
- **Canvas:** intrinsic `width="720" height="380"` attributes; scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id, w, h)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, grays `#666`/`#333`.
- Note: in regenerated HTML, any card links would use `.html` extensions.
