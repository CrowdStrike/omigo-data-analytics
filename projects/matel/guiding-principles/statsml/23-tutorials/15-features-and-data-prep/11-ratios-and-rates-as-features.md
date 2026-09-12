# Ratios & Rates as Features

**Page type:** detail page (tutorial page: h1 + subtitle, 4 `.card-section` blocks each with an h2 and a `table.layout` — sections 2–4 use text left 50% / canvas right 50%; section 1 uses a 3-column row: text 38% + two canvases 31% each)
**HTML title tag:** Ratios &amp; Rates as Features

**Subtitle:** A raw count mixes together "how big" and "how bad" — dividing by the right denominator separates the two

## 500 Tickets: Disaster or Business as Usual?

**Tags:** `core idea` (blue), `running example` (green)

- **The report** — two products each logged 500 support tickets last month
- **Identical counts** — on the tickets chart, A and B look exactly the same
- **The missing number** — product A has 10,000 users; product B has 600
- **Divide** — A: 500 ÷ 10,000 = 5% of users; B: 500 ÷ 600 = 83%
- **The verdict** — A is routine noise; B is on fire

*Example:* Same 500 tickets: for A that's one user in twenty, for B it's five users in six.

**Key point:** A count answers "how many happened". A rate answers "how likely per user" — and only the rate lets you compare a big product with a small one.

### Visualization (canvas `c1a`, 420×340)

Two-bar chart of raw ticket counts — visually identical bars.

- **Title (bold 15px, `#1a5276`, top center):** "Raw Count: Tickets Last Month".
- **Bars (width 100, baseline y=265, chart height 190, scale max 600):** "Product A" 500 and "Product B" 500, both muted gray `#6b7280` at 0.6 alpha; bold 14px `#1a5276` value labels "500" above each; 12px `#333` product labels below; thin `#999` baseline.
- **Caption (bold 13px muted, bottom center):** "identical — the count can't tell them apart".

### Visualization (canvas `c1b`, 400×340)

Two-bar chart of tickets per 100 users — hugely different bars.

- **Title (bold 15px, `#1a5276`, top center):** "Rate: Tickets per 100 Users".
- **Bars (width 100, baseline y=265, chart height 190, scale max 100, 0.72 alpha):** "Product A" 5% green `#008300` with 11px muted sub-label "500 / 10,000"; "Product B" 83% red `#e74c3c` with sub-label "500 / 600". Bold 14px value labels "5%" / "83%" above in the bar color; thin `#999` baseline.
- **Caption (bold 13px red `#e74c3c`, bottom center):** "same 500 tickets — 17x the rate".

## The Division, Checked by Hand

**Tags:** `worked example` (green)

- **Pick the denominator** — "tickets per what?" — per user is what we care about here
- **Product A** — 500 ÷ 10,000 = 0.05 → 5 tickets per 100 users
- **Product B** — 500 ÷ 600 = 0.833 → 83 tickets per 100 users
- **Sanity check** — 5% of 10,000 is 500 ✓; 83.3% of 600 is 500 ✓
- **Read it out** — B's users hit problems about 17 times as often as A's

*Example:* Per-100 framing keeps it concrete: 5 out of every 100 A users, 83 out of every 100 B users.

**Key point:** Always multiply back as a check — rate × denominator must return the original count. If it doesn't, you divided by the wrong thing.

### Visualization (canvas `c2`, 720×300)

Two 10×10 dot grids (100 dots each) showing filled-dot proportions per product.

- **Title (bold 15px, `#1a5276`, top center):** "Out of Every 100 Users, How Many Filed a Ticket?".
- **Grids:** 10×10 arrays of 4px-radius dots on an 11px cell pitch; unfilled dots `#e2e6ea`. Left grid at (140, 70): first 5 dots filled green `#008300`, labeled below in bold 14px green "Product A: 5 per 100" with 12px muted sub-label "500 ÷ 10,000 = 0.05". Right grid at (460, 70): first 83 dots filled red `#e74c3c`, labeled "Product B: 83 per 100" with sub-label "500 ÷ 600 = 0.833".
- **Divider:** dashed light-gray (`#e5e9ef`, dash 4/3) vertical line at x=360 between the grids.
- **Caption (bold 13px orange `#d95926`, bottom center):** "check: 0.05 × 10,000 = 500 and 0.833 × 600 = 500 — both recover the count".

## Why Models Want Rates, Not Counts

**Tags:** `where it's used` (blue), `rule of thumb` (blue)

- **Counts track size** — bigger products get more tickets, more clicks, more everything
- **A count feature** — a model given raw ticket counts mostly learns "this product is big"
- **A rate feature** — tickets per user ignores size and measures behavior directly
- **The scatter shows it** — ticket counts rise with users; only the rate flags product B
- **Common rate features** — clicks per view, returns per order, tickets per user, spend per visit

*Example:* A fraud model fed raw transaction counts flags every large merchant; fed declines per transaction, it flags the risky ones.

**Rule of thumb:** When a raw count could differ just because the entity is bigger, divide by the size — otherwise the model learns size and calls it behavior.

### Visualization (canvas `c3`, 720×300)

Scatter plot of users vs tickets with a trend line and one rate outlier.

- **Title (bold 15px, `#1a5276`, top center):** "Ticket Counts Mostly Track Product Size (illustrative)".
- **Axes:** L-shaped `#999` axes; padding top 50, bottom 52, left 70, right 40; x maps users 0–12,000, y maps tickets 0–700. Axis labels in 12px `#444`: "users" (bottom center) and rotated "tickets last month" (left).
- **Normal products (blue `#2a78d6` 5px dots at 0.7 alpha), [users, tickets]:** [800, 42], [1500, 70], [2200, 118], [3000, 145], [4000, 210], [5000, 240], [6500, 330], [7500, 385], [8500, 410], [10000, 500], [11000, 560].
- **Trend line:** dashed blue (dash 5/4, width 1.5) from (0, 0) to (12000, 600), labeled in bold 12px blue: `the "normal" line: ~5 tickets per 100 users`.
- **Outlier:** red `#e74c3c` 7px dot at (600, 500) with two bold 13px red annotation lines: "Product B: 600 users, 500 tickets" and "the rate (83%) screams; the count (500) whispers".

## The Tiny-Denominator Trap

**Tags:** `common mistake` (red), `watch out` (orange)

- **The trap** — product C: 2 users, 2 tickets → 100%, "the worst product ever"
- **Why it happens** — with 2 users the rate can only be 0%, 50%, or 100%
- **The pattern** — small denominators swing to extremes; big ones settle near the truth
- **The tell** — the best AND worst rates on a leaderboard usually belong to the smallest groups
- **Simple guards** — set a minimum denominator, or show the count next to every rate

*Example:* One unlucky user flips product C from "perfect" to "catastrophic" — that's not a signal, that's noise.

**Common mistake:** Ranking by rate without checking the denominator. A rate built on 2 users is a coin toss wearing a percentage sign.

### Visualization (canvas `c4`, 720×300)

Funnel scatter: rate vs denominator on a log x-axis, extremes clustered at small user counts.

- **Title (bold 15px, `#1a5276`, top center):** "Small Denominators Swing to Extremes (illustrative)".
- **Axes:** L-shaped `#999` axes; padding top 50, bottom 52, left 70, right 40; x is log10 scale of users from 1 to 10,000; y is rate 0–100%. Axis labels 12px `#444`: "users (log scale: 1 → 10,000)" (bottom center) and rotated "ticket rate, %" (left).
- **True-rate band:** translucent green rectangle rgba(0,131,0,0.10) spanning rates 3–12% across the full width, labeled in bold 12px green `#008300`: "typical true rate: 3–12%".
- **Points ([users, rate%], 5px dots at 0.75 alpha):** [2, 100], [2, 0], [3, 67], [4, 0], [5, 60], [6, 83], [8, 0], [10, 30], [15, 20], [20, 5], [30, 17], [50, 12], [80, 9], [120, 3], [200, 8], [350, 6], [600, 10], [1000, 6], [2000, 7], [4000, 5], [6000, 6], [10000, 5]. Points with rate > 25%, or rate 0% with users < 100, are colored red `#e74c3c`; the rest blue `#2a78d6`.
- **Product C annotation (bold red, near the [2,100] point):** 13px `Product C: 2 users, 2 tickets = "100%"` and 12px "with 2 users the only possible rates are 0%, 50%, 100%".
- **Caption (bold 13px orange `#d95926`, centered just below the x-axis, above the axis title):** "extremes live on the left — big denominators settle into the band".

## Regeneration instructions

- **Layout:** tutorial detail page. `<h1>` + `.subtitle` paragraph, then four `.card-section` divs, each an `<h2>` (bottom border `2px solid #2980b9`) followed by a `table.layout`. Section 1 row uses `td.text-col3` (38%) + two `td.viz-col3` (31% each) holding canvases c1a (420×340) and c1b (400×340); sections 2–4 use `td.text-col` (50%) + `td.viz-col` (50%) with one 720×300 canvas. Left cells hold `.tags` pills, a `<ul>` of bold-term bullets, an italic `.example` line, and a `.key-point` callout (lead-ins "Key point:", "Rule of thumb:", "Common mistake:").
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with `2px solid #2980b9` bottom border; `.subtitle` `#666` 0.95rem; h2 1.3rem `#1a5276`. `ul` 0.92rem; `li b` in `#1a5276`. `.example` italic `#555` 0.9rem. `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. Canvases `width:100%`, border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links.
- **Tag pills:** `.tag` inline-block, 0.72rem bold, padding 2px 10px, radius 10px; `.tag.blue` background rgba(26,82,118,0.12) color `#1a5276`; `.tag.green` rgba(39,174,96,0.15) `#27ae60`; `.tag.red` rgba(231,76,60,0.12) `#e74c3c`; `.tag.orange` rgba(230,126,34,0.15) `#e67e22`.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** the shared `setup(id)` helper reads each canvas's `width`/`height` attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), and calls `ctx.scale` so drawing stays in logical coordinates.
- In regenerated HTML, any card links use `.html` extensions (this page has no outgoing links).
