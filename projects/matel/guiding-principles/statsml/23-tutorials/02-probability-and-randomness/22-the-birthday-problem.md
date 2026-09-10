# The Birthday Problem

**Page type:** detail page (tutorial card-sections: h2 with blue underline per section, two-column layout table — text left 50%, canvas right 50%)
**HTML title tag:** The Birthday Problem

**Subtitle:** In a room of just 23 people, it's a coin flip that two share a birthday — because chances to match come in pairs, and pairs pile up fast

## Twenty-Three People, Even Odds

**Tags:** `core idea` (blue), `counter-intuitive` (orange)

- **The setup** — a room of 23 people, each birthday equally likely among 365 days
- **The surprise** — the chance that some two of them share a birthday is 50.7%
- **Why it feels wrong** — you picture 23 people, but matches live in pairs of people
- **Counting pairs** — 23 people make 23×22/2 = 253 pairs, each a chance to match
- **The pattern** — pairs grow as n(n−1)/2, so doubling the room ~quadruples the pairs

*Example:* With 50 people there are 1,225 pairs — a shared birthday is 97% likely.

**Key point:** Don't count people, count pairs — the opportunities to match grow much faster than the group does.

### Visualization (canvas `c1`, 720×300)

Bar chart: number of pairs vs number of people, with the 23-person bar highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "Pairs Grow Much Faster Than People".
- **Data:** people `[5, 10, 15, 20, 23, 30, 40, 50]`, pairs `[10, 45, 105, 190, 253, 435, 780, 1225]`; y scale max 1300.
- **Axes:** padding top 52, bottom 52, left 62, right 25; axis lines `#999`; x-axis label "people in the room" (12px `#444`, bottom center); rotated y-axis label "pairs = n(n−1)/2".
- **Bars:** width 52px, evenly gapped; the 23-people bar filled orange `#d95926`, all others `rgba(26,82,118,0.35)`; pair-count value labels 12px `#444` above bars, people labels 12px `#222` below.
- **Annotation (bold 13px orange, at ~36% plot width, near top):** "23 people = 253 chances to match".

## Working It Out by Hand

**Tags:** `worked example` (green), `core idea` (blue)

- **Flip the question** — first find the chance that NOBODY shares, then subtract from 1
- **Person 2** — must avoid 1 taken date: 364/365 = 0.9973
- **Person 3** — must avoid 2 taken dates: multiply by 363/365, giving 0.9918
- **Five people** — the product is 0.973, so P(match) is only 2.7%
- **At 23 people** — the product falls to 0.493, so P(match) = 1 − 0.493 = 50.7%

*Example:* Five friends: 364/365 × 363/365 × 362/365 × 361/365 = 0.973 — check it on any calculator.

**Key point:** "At least one match" is hard to compute directly; "no match at all" is one clean chain of multiplications.

### Visualization (canvas `c2`, 720×300)

Line chart: probability of a shared birthday vs group size, S-curve with 23-person point highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "Chance of a Shared Birthday vs Group Size".
- **Data:** people `[5, 10, 15, 20, 23, 30, 40, 50, 60]`, probability % `[2.7, 11.7, 25.3, 41.1, 50.7, 70.6, 89.1, 97.0, 99.4]`; x from 0 to 60, y from 0 to 100.
- **Axes:** padding top 52, bottom 52, left 62, right 30; axis lines `#999`; x ticks at 0, 10, 20, 30, 40, 50, 60 (12px `#222`); x-axis label "people in the room"; rotated y-axis label "P(some pair shares), %" (12px `#444`).
- **50% reference line:** dashed (5/4) `#bbb`, width 1, at y=50, labeled "50%" in 12px `#888` at the left.
- **Series:** connected line in blue `#2a78d6`, width 3; 4px dots at each point, the 23-person point orange `#d95926` and 6px.
- **Annotations:** bold 13px orange "23 people → 50.7%" to the right of the 23-person point; bold 12px green `#008300` "50 people → 97%" near the top of the curve (at x≈43).

## Same Math, Different Disguise: Hash Collisions and Duplicate IDs

**Tags:** `where it's used` (blue), `common mistake` (red)

- **Random IDs** — handing out random IDs is the birthday problem with more "days"
- **10,000 possible IDs** — a duplicate becomes 50-50 after only ~118 IDs are issued
- **Square-root rule** — expect trouble near the square root of the ID space, not near the space
- **Where it bites** — session tokens, hash buckets, short URLs, test-user IDs, cache keys
- **The fix** — make the ID space enormous (UUIDs) or check for duplicates on write

*Example:* A 4-digit order suffix (10,000 values) starts colliding within the first ~120 orders.

**Key point:** Duplicates show up near √N draws — thousands of times sooner than "we have 10,000 IDs" suggests.

### Visualization (canvas `c3`, 720×300)

Line chart: duplicate-ID collision probability curve for a pool of 10,000 IDs.

- **Title (bold 15px, `#1a5276`, top center):** "Chance of a Duplicate — Random IDs From a Pool of 10,000".
- **Data:** IDs issued `[0, 25, 50, 75, 100, 118, 150, 200]`, probability % `[0, 3.0, 11.5, 24.2, 39.0, 49.9, 67.3, 86.3]`; x from 0 to 200, y from 0 to 100.
- **Axes:** padding top 52, bottom 52, left 62, right 30; axis lines `#999`; x tick labels are the data x-values (except 0), 12px `#222` under each point; x-axis label "random IDs issued so far"; rotated y-axis label "P(some duplicate), %" (12px `#444`).
- **50% reference line:** dashed (5/4) `#bbb`, width 1, at y=50, labeled "50%" in 12px `#888`.
- **Series:** connected line in violet `#4a3aa7`, width 3; 4px violet dots, the 118-ID point orange `#d95926` and 6px.
- **Annotations:** bold 13px orange "50-50 duplicate after just 118 of 10,000 IDs" (at x≈30, y≈72%); 12px mute `#6b7280` "same n(n−1)/2 pair count, bigger calendar" just below it.

## The Confusion: "My Birthday" vs "Any Birthday"

**Tags:** `common mistake` (red), `rule of thumb` (blue)

- **Two questions** — "someone matches ME" is not "some two people match"
- **Matching you** — only 22 pairs involve you, so that chance is just 5.9%
- **Matching anyone** — all 253 pairs count, and the chance jumps to 50.7%
- **The trap** — your gut answers the "me" question when asked the "anyone" question

*Example:* In a 23-person room, bet on "some pair matches" (50.7%), never on "someone matches me" (5.9%).

**Key point:** When a match probability surprises you, check whether one item or every pair is in play — the gap here is roughly 9×.

### Visualization (canvas `c4`, 720×300)

Two-bar comparison: probability of matching YOU vs any two people matching.

- **Title (bold 15px, `#1a5276`, top center):** "Same Room of 23 People, Two Very Different Questions".
- **Data:** bars "someone matches YOUR birthday" = 5.9% (blue `#2a78d6`) and "ANY two people match" = 50.7% (orange `#d95926`); y scale max 60%.
- **Axes:** padding top 56, bottom 66, left 62, right 30; axis lines `#999`; rotated y-axis label "probability, %" (12px `#444`).
- **Bars:** 150px wide at 28% and 72% of plot width, filled at 75% alpha in their colors; value labels bold 16px in the bar's color above each bar ("5.9%", "50.7%"); below each bar its question label (bold 12px `#222`) and a sub-label in 12px `#666`: "22 pairs involve you" / "all 253 pairs count".
- **Annotation (bold 13px orange, top center of plot):** "≈9× more likely — because 253 pairs get to try, not 22".

## Regeneration instructions

- **Layout:** tutorial detail page. `<h1>` + `.subtitle`, then 4 `.card-section` blocks, each with an `<h2>` (1.3rem, `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` (width 100%, border-collapse) with one `<tr>`: left `td.text-col` (50%) holding `.tags` pills, a `<ul>` of bullets, an italic `.example` line, and a `.key-point` callout; right `td.viz-col` (50%) holding a `<canvas>` 720×300 at `width:100%` with 1px `#e0e0e0` border, 4px radius.
- **Page CSS:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `<ul>` 0.92rem; `li b` colored `#1a5276`.
- **Tag pills:** `.tag` inline-block 0.72rem weight 600, padding 2px 10px, radius 10px; blue = `rgba(26,82,118,0.12)` bg / `#1a5276` text; green = `rgba(39,174,96,0.15)` / `#27ae60`; red = `rgba(231,76,60,0.12)` / `#e74c3c`; orange = `rgba(230,126,34,0.15)` / `#e67e22`.
- **Callout style:** `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.example` — italic, `#555`, 0.9rem.
- **Canvas:** logical size 720×300 per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Hardcoded data arrays, no `Math.random()`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
