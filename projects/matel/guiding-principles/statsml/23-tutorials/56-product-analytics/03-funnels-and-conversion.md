# Funnels & Conversion

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Funnels & Conversion

**Subtitle:** A funnel counts how many users survive each step toward a goal — and "conversion" means nothing until you say which step is the denominator

## Where 10,000 Visits Become 410 Purchases

**Tags:** `core idea` (blue), `worked example` (green), `e-commerce` (orange)

- **The funnel** — an online shop tracks five steps: visit → product page → add to cart → checkout → purchase
- **The counts** — one week of traffic: 10,000 → 4,200 → 1,300 → 640 → 410 users survive each step
- **Step conversion** — each step over the one before: 42.0%, then 31.0%, then 49.2%, then 64.1%
- **Overall conversion** — the last step over the first: 410 / 10,000 = 4.1% of visitors buy
- **The leaks** — the drop at each step is a leak: 5,800 then 2,900 then 660 then 230 users lost

*Example (italic):* Of 10,000 visitors, 4,200 open a product page (42.0%), but only 1,300 of those add to cart (31.0%) — the funnel narrows at every step down to 410 buyers.

**Key point:** A funnel turns one vague number ("4.1% convert") into four step conversions, each pointing at a specific leak between two adjacent steps.

### Visualization (canvas `c1`, 720×300)

Horizontal funnel bar chart: five bars whose widths are proportional to the user counts, with step-conversion labels between rows and lost-user counts on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Checkout Funnel: 10,000 Visits → 410 Purchases".
- **Rows (bars start at x=150, top edges at y = 55, 97, 139, 181, 223, height 26):** step labels 12px `#444` right-aligned at x=142: "Visit", "Product page", "Add to cart", "Checkout", "Purchase".
- **Bars:** fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` border; widths proportional to counts `[10000, 4200, 1300, 640, 410]` at 460px max: `[460, 193, 60, 29, 19]`; the "Purchase" bar instead solid green `#008300`.
- **Count labels (bold 12px `#2c3e50`)** at each bar's right end + 8px: "10,000", "4,200", "1,300", "640", "410".
- **Step-conversion labels (12px `#6b7280`)** between rows at x=400: "42.0% continue", "31.0% continue", "49.2% continue", "64.1% continue".
- **Lost-user labels (11px `#e74c3c`, right-aligned at x=700)** between rows: "−5,800", "−2,900", "−660", "−230".
- **Annotation (bold 13px green `#008300`, near x=250, y=245):** "overall: 410 / 10,000 = 4.1%".
- **Caption (12px `#444`, bottom right):** "counts illustrative, percentages exact".

## Three Correct Answers to "What's Our Conversion?"

**Tags:** `worked example` (blue), `the denominator` (red)

- **One question** — three teams report "purchase conversion" for the same week, same 410 purchases
- **Marketing** — purchases / visits: 410 / 10,000 = 4.1%, because ads pay for visits
- **Growth** — purchases / add-to-cart: 410 / 1,300 = 31.5%, because carts signal intent
- **Payments** — purchases / checkout: 410 / 640 = 64.1%, because checkout is their surface
- **All correct** — same product, same week, 4.1% vs 31.5% vs 64.1% — only the denominator moved
- **The rule** — a conversion number must travel with its definition: numerator, denominator, window

*Example (italic):* "Conversion is 64.1%" and "conversion is 4.1%" describe the same 410 purchases — one divides by 640 checkouts, the other by 10,000 visits.

**Key point:** Conversion is a fraction, and the denominator is a choice; report "purchases / checkouts, same session" — never a bare percentage.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: the same 410 purchases divided by three different denominators, producing three wildly different "conversion rates".

- **Title (bold 15px, `#1a5276`, top center):** "Same 410 Purchases, Three Denominators, Three 'Conversions'".
- **Rows (bars start at x=250, top edges at y = 70, 130, 190, height 28), left-aligned 12px `#444` labels at x=20:** "410 / 10,000 visits", "410 / 1,300 add-to-carts", "410 / 640 checkouts".
- **Bars:** widths proportional to the rates `[4.1, 31.5, 64.1]` at 440px for 64.1%: `[28, 216, 440]`; fills blue `#2a78d6`, aqua `#199e70`, green `#008300`.
- **Rate labels (bold 13px, same color as each bar)** at each bar's right end + 8px: "4.1%", "31.5%", "64.1%".
- **Annotation (bold 13px magenta `#d55181`, centered near y=250):** "16× apart — and every one of them is 'correct'".
- **Caption (12px `#444`, bottom right):** "counts illustrative, division exact".

## Which Leak to Fix — and What the Blend Hides

**Tags:** `where it's used` (blue), `segments` (green), `prioritization` (orange)

- **Biggest leak** — visit → product page loses 5,800 users, the most of any step by far
- **Volume × fixability** — 5,800 lost to browsing is normal; 230 lost at payment may be one bug
- **The blend** — the 4.1% overall hides two devices: mobile 150/6,000 = 2.5%, desktop 260/4,000 = 6.5%
- **Segment funnels** — mobile's checkout → purchase step runs 51.7% vs desktop's 74.3%
- **The find** — the blended funnel says "checkout is fine at 64.1%"; the split says mobile pay is broken

*Example (italic):* Splitting the funnel by device shows mobile losing 140 of 290 checkouts (48.3%) while desktop loses 90 of 350 (25.7%) — one payment form to fix, not a whole funnel.

**Key point:** Work the leak with the best volume × fixability, not just the biggest count — and always split the funnel by segment before deciding, because a blended step rate can hide a broken segment.

### Visualization (canvas `c3`, 720×300)

Grouped vertical bar chart: step conversion at each of the four steps, mobile vs desktop side by side, exposing the payment-step gap the blended funnel hides.

- **Title (bold 15px, `#1a5276`, top center):** "Step Conversion by Device: the Blend Hides Mobile's Payment Problem".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = step conversion 0 to 80%, gridlines `#e5e9ef` at 20/40/60, 12px `#444` tick labels; x = four groups centered at x = 135, 285, 435, 585 with 12px `#444` labels "visit→product", "product→cart", "cart→checkout", "checkout→buy".
- **Bars (each 40px wide, 6px gap inside a group):** mobile violet `#4a3aa7` heights from `[38.3, 27.8, 45.3, 51.7]` → `[86, 63, 102, 116]` px; desktop aqua `#199e70` heights from `[47.5, 34.7, 53.0, 74.3]` → `[107, 78, 119, 167]` px.
- **Value labels (11px, bar color)** above each bar: "38.3%", "27.8%", "45.3%", "51.7%" / "47.5%", "34.7%", "53.0%", "74.3%".
- **Legend (12px, top right at y=55):** violet swatch "mobile", aqua swatch "desktop".
- **Annotation (bold 13px red `#e74c3c`, left of the last group near (500, 95)):** "22.6-pt gap at payment".
- **Caption (12px `#444`, bottom right):** "segment counts illustrative, rates exact".

## The Trend That Was Just a Definition Change

**Tags:** `common mistake` (red), `time window` (orange)

- **The window** — "converted" needs a clock: same-session, or any purchase within 7 days of the visit
- **The gap** — in April, same-session finds 420 buyers (4.2%); 7-day finds 520 (5.2%) — returners count
- **The switch** — in April the dashboard quietly moves from same-session to 7-day attribution
- **The fake trend** — the chart jumps 4.1% → 5.2% and the team celebrates a +1.1-point win
- **The truth** — April's same-session rate was 4.2%; the real move was +0.1 points, not +1.1
- **The check** — before calling a trend, confirm denominator AND window are identical across periods

*Example (italic):* March 4.1% (same-session) vs April 5.2% (7-day) is not growth — like-for-like, April was 4.2%, and the other 1.0 point is the window change.

**Common mistake:** Comparing conversion numbers with different denominators or time windows and calling the difference a trend — the metric's definition changed, not the users.

### Visualization (canvas `c4`, 720×300)

Line chart of monthly "conversion" as the dashboard shows it (window silently switched in April) vs the like-for-like same-session rate.

- **Title (bold 15px, `#1a5276`, top center):** "A +1.1-Point 'Win' That Was a Window Change".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = months at x = 130, 280, 430, 580 with 12px `#444` labels "Jan", "Feb", "Mar", "Apr"; y = conversion 0 to 6%, gridlines `#e5e9ef` at 2%/4% (y = 185, 125), 12px `#444` tick labels.
- **Dashboard line:** red `#e74c3c` 3px through rates `[4.0, 4.0, 4.1, 5.2]` → points y = `[125, 125, 122, 89]`, 4px dots; bold 12px red label "dashboard: 5.2%" near (580, 75).
- **Like-for-like line:** blue `#2a78d6` 2px dashed (dash 6/4) through same-session rates `[4.0, 4.0, 4.1, 4.2]` → y = `[125, 125, 122, 119]`; bold 12px blue label "same-session: 4.2%" near (560, 140).
- **Switch marker:** vertical dashed `#6b7280` (dash 4/3) line at x=505 (between Mar and Apr), 12px `#6b7280` label "window switched to 7-day" at its top.
- **Annotation (bold 13px orange `#d95926`, near x=200, y=80):** "the metric changed, not the users".
- **Caption (12px `#444`, bottom right):** "monthly rates illustrative; Apr: 420 same-session vs 520 seven-day buyers per 10,000 visits".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); funnel counts `[10000, 4200, 1300, 640, 410]`, device split (mobile `[6000, 2300, 640, 290, 150]`, desktop `[4000, 1900, 660, 350, 260]`, columns summing to the blended funnel), and monthly rates are invented and labeled illustrative; every percentage (42.0 / 31.0 / 49.2 / 64.1 / 4.1 / 31.5 / 5.2, all step and segment rates) is the exact quotient of the stated counts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
