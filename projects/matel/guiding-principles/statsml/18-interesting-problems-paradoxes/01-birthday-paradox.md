# Birthday Paradox

**Page type:** detail page (h2-sectioned two-column obj-table layout: text left 45%, canvas right 55%, one table per section; philosophy callouts at top and bottom)
**HTML title tag:** Birthday Paradox — Case Study

**Subtitle:** How many people do you need in a room before there's a 50% chance two of them share a birthday? The answer is shockingly small.

## Callout (philosophy box, top)

**The question:** You're at a party with 22 other people (23 total). What's the probability that at least two people in the room share a birthday?

**Your gut says:** "23 people, 365 possible birthdays — that's barely 6%. Very unlikely."

**The actual answer:** `50%.` And at 70 people it's 99.9%. This feels impossible. But your brain is counting the wrong thing.

## 1. Why It Feels Impossible But Isn't

**Obj-title:** Your Brain Counts People. Math Counts Pairs.

Math box 1:

**The mistake everyone makes:**
"23 people, 365 slots → only 6.3% full → collisions should be rare."

**But the question isn't:** "Does someone share MY birthday?"
**The question is:** "Do ANY two people share a birthday?"

23 people → `253 unique pairs` of people.
Each pair is a separate chance for a match.
253 chances × 1/365 each → adds up fast.

Math box 2:

**Think of it this way:**

Person 1 joins the room — no possible matches.
Person 2 joins — 1 pair to check.
Person 3 joins — 2 new pairs to check.
Person 10 joins — 9 new pairs to check.
Person 23 joins — 22 new pairs to check.

Total pairs: 1 + 2 + 3 + ... + 22 = `253 pairs`.

You only need ONE pair to match. With 253 shots at it, 50% isn't surprising at all.

### Visualization (canvas `canvas1`, 720×360)

Line chart with filled area: exact birthday-collision probability vs number of people (0–70).

- **Title (bold 14px, top center):** "Birthday Collision Probability".
- **Data:** exact probabilities computed iteratively: `pNoMatch(0)=1`, for n=1..70 `pNoMatch *= (365-(n-1))/365`, plotted value `P(n) = 1 - pNoMatch`; P(0)=0. (Key values: P(23)≈0.507, P(70)≈0.999.)
- **Plot area:** origin x=70, baseline y=300, width 600, height 240. Axes in `#1a5276`, 2px.
- **Axis labels:** x "Number of People" (13px `#1a5276`, centered below); y "P(at least one shared birthday)" rotated −90° at left. Y tick labels 0%, 25%, 50%, 75%, 100% (11px `#666`, right-aligned) with light gridlines `#eee`; x tick labels 0, 10, 20, 30, 40, 50, 60, 70.
- **Curve:** red `#e74c3c`, 3px; area under the curve filled `rgba(231,76,60,0.1)`.
- **50% reference line:** horizontal dashed gray `#999` (dash 6/4, 1px) at y=50%.
- **n=23 marker:** vertical dashed line `#1a5276` (dash 4/3, 1.5px) from baseline up to the 50% line at x=23, with bold 12px `#1a5276` label above it: "n = 23 → 50%".

## 2. The Mental Math

**Obj-title:** Why 23 People → 50% Collision

Math box 1:

**Step 1: Count the pairs, not the people.**

23 people → how many unique pairs?
`23 × 22 / 2 = 253 pairs`

Each pair has a 1/365 chance of matching.

**Step 2: Flip the question.**
P(no match in one pair) = 364/365 = 0.9973
P(no match in 253 pairs) ≈ 0.9973²⁵³ ≈ `0.50`

P(at least one match) = 1 − 0.50 = `50%`

Math box 2:

**The quick rule:**
For N possible values, you hit 50% collision probability at roughly `√N × 1.2` items.

365 days → √365 ≈ 19 × 1.2 ≈ `23 people`
1,000,000 IDs → √1M = 1000 × 1.2 ≈ `1,200 items`
2³² hash values → √(4B) ≈ `65,000 items`

### Visualization (canvas `canvas1` duplicate id, 720×360)

The HTML declares a second `<canvas id="canvas1" width="720" height="360">` in this section — the same id as Section 1's canvas. No separate drawing routine exists for it; the JS `getElementById('canvas1')` targets only the first canvas, so this one renders blank. To regenerate faithfully, reuse the Section 1 chart spec here (the intended visual is the same collision-probability curve); ideally give it a unique id and draw the same chart.

## 3. Why Intuition Fails

**Obj-title:** Your Brain Counts Items. Math Counts Pairs.

Math box:

**What your brain computes:**
"23 people, 365 slots → only 6.3% full → collisions unlikely"

**What actually matters:**
23 people → 253 pairs → 253 chances for collision

**The gap:**
- 5 people → 10 pairs
- 10 people → 45 pairs
- 23 people → 253 pairs
- 50 people → 1,225 pairs
- 100 people → 4,950 pairs

Items grow linearly. Pairs grow as `n²/2`. That's the surprise.

Bullets:

- **The error:** Thinking about "how full is the space" instead of "how many comparison opportunities exist"
- **Why it persists:** We naturally ask "what's the chance someone shares MY birthday?" (1/365 per person) instead of "what's the chance ANY two people share?" (pairs × 1/365)

### Visualization (canvas `canvas2`, 720×360)

Two-line growth comparison: items (linear) vs pairs (quadratic) for n = 0 to 50.

- **Title (bold 14px `#1a5276`, top center):** "Items Grow Linearly. Pairs Grow Quadratically."
- **Plot area:** origin x=70, baseline y=300, width 600, height 240; axes `#1a5276` 2px.
- **Axis labels:** x "Number of Items (n)"; y "Count" (rotated −90°). Y tick labels 0, 250, 500, 750, 1000, 1225 (scale max 1225 = 50×49/2) with `#eee` gridlines; x tick labels 0, 10, 20, 30, 40, 50.
- **Series 1 (items):** green `#27ae60`, 2px, y = n on the shared 0–1225 scale (nearly flat along the bottom).
- **Series 2 (pairs):** red `#e74c3c`, 3px, y = n(n−1)/2.
- **Legend (top left, 12px):** green line swatch + "Items (n) — linear"; red line swatch + "Pairs (n²/2) — quadratic".

## 4. Where This Breaks Real Systems

**Obj-title:** Collisions in Practice

Math box 1:

**Hash collisions:**
Using a 32-bit hash (4 billion values).
"That's 4 billion possibilities — collision is impossible!"
Reality: at `~77,000 items`, you have 50% chance of collision.

**UUID v4 (122 random bits):**
Space = 5.3 × 10³⁶. Feels infinite.
50% collision at: `2.7 × 10¹⁸` IDs (2.7 quintillion).
That's actually safe. But 32-bit? Not safe at all.

Math box 2:

**Session tokens (real incident):**
A system generated 16-bit session IDs (65,536 possible values).
"We only have 1,000 concurrent users — plenty of space!"

50% collision at √65,536 × 1.2 = `~307 concurrent sessions`.
With 1,000 users: collision probability = `99.95%`.
Two users sharing a session = security breach.

Bullets:

- **A/B test user assignment:** Hash user_id to bucket. Short hashes → users collide into same bucket → contamination
- **Database sharding:** Shard key with limited range → uneven distribution sooner than expected
- **File deduplication:** MD5 (128-bit) collision at ~2⁶⁴ files. SHA-256 is safe; MD5 is not for adversarial inputs

### Visualization (canvas `canvas3`, 720×360)

Canvas-drawn table: 50% collision point per hash size.

- **Title (bold 14px `#1a5276`, top center):** "50% Collision Point for Different Hash Sizes".
- **Table layout:** starts at x=140, header row at y=60, row height 48, column x-offsets 0/80/220/360 (widths 80, 140, 140, 100); header underline `#1a5276` 2px spanning 480px; even rows have `#f8fafb` background stripes (500px wide).
- **Header (bold 12px `#1a5276`):** "Hash Size", "Total Space", "50% Collision At", "Safe?".
- **Rows (13px, `#333`; hash size bold):**
  - 16-bit | 65K | ~307 | ✗ Dangerous (bold red `#e74c3c`)
  - 32-bit | 4B | ~77K | ✗ Dangerous (bold red `#e74c3c`)
  - 64-bit | 1.8×10¹⁹ | ~5B | ✓ Safe (bold green `#27ae60`)
  - 128-bit | 3.4×10³⁸ | ~2×10¹⁹ | ✓ Safe (bold green `#27ae60`)
  - 256-bit | 1.2×10⁷⁷ | ~4×10³⁸ | ✓ Safe (bold green `#27ae60`)
- **Bottom notes (12px `#666`, left-aligned at x=140, y≈320/338):** "Rule of thumb: 50% collision at √(space) × 1.2" and "If your item count can ever reach √(space), your hash is too small."

## Callout (philosophy box, bottom)

**One sentence:** Whenever you think "the space is large enough that collisions won't happen" — count the PAIRS, not the items. You'll hit 50% collision at √N, not at N/2.

## Regeneration instructions

- **Layout:** case-study detail page. h1, `.subtitle`, `.philosophy` callout, then per numbered section: `<h2>` (1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px) followed by an `.obj-table` (full-width, one `<tr>`; left `<td>` 45% with `.obj-title` + `.math-box` blocks + bullets, right `<td>` 55% centered holding the canvas). Closing `.philosophy` callout at the end. No nav bar, no back/home links.
- **Math boxes:** `.math-box` — background `#f8fafb`, border `1px solid #e0e0e0`, radius 6px, padding 16px 20px, 0.9em; inline `code` on `#eef2f7` background, padding 2px 6px, radius 3px.
- **Callout style:** `.philosophy` — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`; ul 0.9em `#333`, margin `8px 0 8px 20px`.
- **Canvas:** intrinsic 720×360 per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id, w, h)` helper. Chart fonts are `-apple-system, BlinkMacSystemFont, sans-serif`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gridlines `#eee`, gray text `#666`/`#333`/`#999`.
