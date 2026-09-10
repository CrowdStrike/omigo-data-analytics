# Randomization

**Page type:** detail page (tutorial layout: `.card-section` blocks, each an h2 + two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Randomization

**Subtitle:** Let a coin flip decide who goes in which group — chance balances everything you didn't think of, automatically.

## Flip a Coin for Every User

**Tags:** `core idea` (blue), `running example` (green)

- **The setup** — 1,000 users, one new checkout page, one question: does it help?
- **The rule** — flip a fair coin per user: heads → new page (Group A), tails → old page (Group B)
- **The result** — 503 users land in A, 497 in B; nobody chose their own group
- **The magic** — every user trait, known or unknown, gets shared roughly evenly
- **The definition** — randomization = letting pure chance, not people, assign groups

*Example:* User #482 is a night-owl Android user from Brazil — the coin doesn't know and doesn't care.

**Key point:** Because chance built the groups, any systematic difference bigger than chance noise comes from the page — not from who happened to be in them.

### Visualization (canvas `c1`, 720×300)

Flow diagram: 1,000 users → coin → Group A / Group B.

- **Title (bold 15px, `#1a5276`, top center):** "One Coin Flip per User Splits 1,000 Users in Two".
- **Users box** at (55, 105), 150×84: fill `rgba(42,120,214,0.12)`, stroke blue `#2a78d6`; bold 15px "1,000 users" plus gray 12px lines "all ages, devices," / "timezones mixed".
- **Coin:** circle radius 34 centered at (345, 147), fill `#fdf3e3`, stroke yellow `#c98500` width 3, bold 13px yellow labels "fair" / "coin". Gray arrow from the users box to the coin.
- **Branch arrows:** green `#008300` arrow up-right labeled "heads" (12px green at (420, 96)); violet `#4a3aa7` arrow down-right labeled "tails" (12px violet at (420, 202)).
- **Group boxes** at x=480, 190×66 each: Group A at y=52, stroke/text green, fill `rgba(0,131,0,0.08)`, bold 14px "Group A — 503 users", gray 12px "sees the NEW page"; Group B at y=178, stroke/text violet, fill `rgba(74,58,167,0.08)`, "Group B — 497 users", "keeps the OLD page".
- **Bottom annotation (bold 13px orange `#d95926`, centered, y=282):** "nobody picks a side — chance does, so both groups carry the same mix of users".

## Checking the Balance the Coin Created

**Tags:** `worked example` (green), `illustrative numbers` (blue)

- **Before the split** — of 1,000 users: 40% on mobile, 30% under age 30, 26% weekend shoppers
- **Group A (503 users)** — 41% mobile, 31% under 30, 27% weekend shoppers
- **Group B (497 users)** — 39% mobile, 29% under 30, 25% weekend shoppers
- **Nobody planned this** — the coin never saw age or device, yet the groups match
- **Unmeasured traits too** — timezone, mood, income get balanced the same blind way

*Example:* The coin balanced "owns a pet" as well — even though the dataset has no pet column.

**Key point:** Randomization balances every trait at once — including the ones you never thought to measure.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: A vs B on three traits.

- **Title (bold 15px, `#1a5276`, top center):** "Traits the Coin Never Saw Come Out Balanced (illustrative)".
- **Data:** traits `['% on mobile', '% under age 30', '% weekend shoppers']`; Group A values `[41, 31, 27]` (green `#008300` bars), Group B values `[39, 29, 25]` (violet `#4a3aa7` bars). Bars 55px wide, 8px within-pair gap, group width 190px starting at padL 60 + 35.
- **Axes:** y 0–50%, baseline y=240, chart height 165; gridlines and gray `#6b7280` labels every 10%; grid `#e5e9ef`, axis `#999`. Value labels bold 12px `#2c3e50` above each bar; trait names 12px gray below.
- **Legend (top right at x=560):** green swatch "Group A (503)", violet swatch "Group B (497)".
- **Bottom annotation (bold 13px orange `#d95926`, centered, y=284):** "every gap is ~2 points — the coin balanced traits it never measured".

## What Happens Without the Coin

**Tags:** `common mistake` (red), `selection bias` (orange)

- **The lazy split** — first 500 signups get the new page, last 500 keep the old one
- **Hidden skew** — early signups are keen early adopters: 24% mobile vs 56% in the late group
- **Poisoned result** — if A wins, was it the page or the eager users? You can't tell
- **The jargon** — two explanations tangled together like this is called confounding
- **The fix costs nothing** — one coin flip per user removes the skew entirely

*Example:* A hospital that gives a new drug to its healthiest patients will "prove" any drug works.

**Key point:** A comparison is only as good as the way its groups were formed — chance is the only assigner with no agenda.

### Visualization (canvas `c3`, 720×300)

Two-panel bar chart: mobile share per group under a signup-order split vs a coin-flip split.

- **Title (bold 15px, `#1a5276`, top center):** "% Mobile Users per Group: Signup-Order Split vs Coin Flip (illustrative)".
- **Left panel (x0=80):** label "split by signup order"; bars "first 500" = 24% and "last 500" = 56%, both magenta `#d55181`; gap annotation bold 13px magenta "32-point gap".
- **Right panel (x0=420):** label "split by coin flip"; bars "Group A" = 41% (green `#008300`) and "Group B" = 39% (violet `#4a3aa7`); gap annotation bold 13px green "2-point gap".
- **Shared axes:** y 0–60%, labels every 20% in gray; baseline y=235, chart height 160, bar width 80, 30px between bars; value labels bold 13px above bars, bar names 12px gray below, panel labels 12px gray beneath.
- **Bottom annotation (bold 13px orange `#d95926`, centered, y=290):** "the signup-order split confounds page and audience — the coin removes the gap".

## Random Doesn't Mean Perfectly Equal

**Tags:** `common confusion` (orange), `rule of thumb` (blue)

- **Luck exists** — with only 10 users per group, one group can end up 70% mobile by luck alone
- **Typical gap** — the expected A-vs-B gap shrinks like 1/√n as groups grow
- **At 10 per group** — the mobile shares typically differ by about 22 points
- **At 500 per group** — the typical gap is about 3 points; at 1,000, about 2
- **So** — randomize AND use enough users; the coin needs room to average out

*Example:* Flipping 10 coins can give 8 heads; flipping 1,000 almost never gives 800.

**Key point:** Randomization guarantees fairness on average — sample size is what makes any single split come out close to even.

### Visualization (canvas `c4`, 720×300)

Bar chart: typical A-vs-B gap shrinking with group size.

- **Title (bold 15px, `#1a5276`, top center):** "Typical Gap in Mobile Share Between the Two Coin-Flip Groups".
- **Data:** group sizes `['10', '50', '100', '500', '1,000']`, gaps `[22, 10, 7, 3.1, 2.2]` points. First two bars orange `#d95926`, last three blue `#2a78d6`. Bar width 82, gap 42, starting at padL 70 + 25.
- **Axes:** y 0–25 with gray labels "0 pt".."25 pt" every 5; baseline y=235, chart height 160; grid `#e5e9ef`, axis `#999`. Value labels bold 13px above bars ("22 pt", …), size labels 12px gray below.
- **X caption (12px gray, centered):** "users per group (typical gap = 1 standard deviation, illustrative)".
- **Annotations (bold 13px):** orange "tiny groups can be badly lopsided by pure luck" at (250, 78); blue "big groups: the coin averages out" at (545, 158).

## Regeneration instructions

- **Layout:** tutorial detail page. h1 + `.subtitle`, then four `.card-section` blocks; each has an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` with one row: `.text-col` td (50%) and `.viz-col` td (50%), 12px padding.
- **Left column structure per section:** a `.tags` row of colored pill spans, a `<ul>` of one-line bullets each opening with `<b>bold term</b>` (bold terms in `#1a5276`), one italic `.example` paragraph (`#555`, 0.9rem), one `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem) starting with `<strong>Key point:</strong>`.
- **Tag pill styles:** 0.72rem, weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 0.95rem. No nav bar, no back/home links.
- **Canvas:** each 720×300 intrinsic, `width:100%` CSS, `1px solid #e0e0e0` border, radius 4px; scaled by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). All data as hardcoded literal arrays — no `Math.random()`.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Doc palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
