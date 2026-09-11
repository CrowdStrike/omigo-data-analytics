# Prisoner's Dilemma & Repeated Games

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Prisoner's Dilemma & Repeated Games

**Subtitle:** Betraying pays no matter what the other side does — yet mutual betrayal leaves everyone poorer. Meeting the same rival again and again is what makes cooperation rational

## One Week on Taco Street

**Tags:** `core idea` (blue), `payoff matrix` (green), `dominant strategy` (orange)

- **The setup** — two rival food trucks each pick a weekly price: hold at $8 or undercut to $6
- **Both hold** — the crowd splits calmly and each truck banks $500 profit for the week
- **One undercuts** — the cheap truck steals the crowd: $700 for it, $200 for the holder
- **Both undercut** — same split of customers but on thin margins: only $300 each
- **The trap** — whatever the rival does, undercutting pays more, so both cut and get $300
- **The name** — this structure is the prisoner's dilemma: individual logic, collective loss

*Example (italic):* Each owner reasons: "if they hold I earn $700 not $500; if they cut I earn $300 not $200 — cut either way."

**Key point:** A prisoner's dilemma is any game where defection beats cooperation against every rival move, yet mutual defection ($300 each) is worse for both than mutual cooperation ($500 each).

### Visualization (canvas `c1`, 720×300)

2×2 payoff matrix with dominance arrows and the Nash (both-undercut) cell highlighted, plus right-side annotations.

- **Title (bold 15px, `#1a5276`, top center):** "Weekly Profit Matrix: Hold $8 vs Undercut $6 (illustrative)".
- **Grid:** origin x=180, y=80; cells 150 wide × 75 tall; 1px `#999` grid lines. Column headers bold 13px `#2c3e50` centered above: "Truck B: hold", "Truck B: undercut". Row headers right-aligned at x=168, two lines each: "Truck A:" / "hold" (centered on row 1) and "Truck A:" / "undercut" (row 2).
- **Cell fills:** top-left (hold, hold) tinted `rgba(0,131,0,0.10)`; bottom-right (undercut, undercut) tinted `rgba(217,89,38,0.14)` with a 3px orange `#d95926` border.
- **Cell payoffs (bold 13px, two lines per cell, centered):** Truck A's line in blue `#2a78d6`, Truck B's in green `#008300`. (hold,hold): "A: $500" / "B: $500"; (hold,undercut): "A: $200" / "B: $700"; (undercut,hold): "A: $700" / "B: $200"; (undercut,undercut): "A: $300" / "B: $300".
- **Dominance arrows (navy `#1a5276`, 2px, filled triangular heads):** two vertical arrows crossing the row border at x = gx+22 in each column (A pulled toward undercut), two horizontal arrows crossing the column border at y = gy+12 (row 1) and y = gy+138 (row 2) (B pulled toward undercut).
- **Right-side annotations (left-aligned at x=500, bold 12px):** green "both hold: $500 each" / "— best joint outcome" (y=96/112); navy plain 12px "arrows: undercut pays" / "more against either move" (y=140/156); orange bold "both undercut: $300 each" / "— where they end up" (y=184/200).
- **Caption (12px `#444`, centered, y=278):** "each cell shows one week of profit: Truck A in blue, Truck B in green".

## The Same Rivals, Every Friday

**Tags:** `worked example` (blue), `tit-for-tat` (green), `repeated game` (orange)

- **Week after week** — the trucks meet every Friday, so today's undercut invites revenge
- **Tit-for-tat** — start by holding, then simply copy whatever the rival did last week
- **The bait** — always-undercut wins week 1 ($700 vs $200) but earns $300 forever after
- **The ledger** — by week 12, two tit-for-tats bank $6,000 each; the defector only $4,000
- **Shadow of the future** — the value of future cooperation is what disciplines today

*Example (italic):* One undercut earns a one-time $200 bonus, then costs $200 every following week — it pays for itself backwards.

**Key point:** Repetition changes the game. Cooperation stops being charity and becomes an investment, enforced by the credible threat of retaliation next week.

### Visualization (canvas `c2`, 720×300)

Three cumulative-profit lines over 12 weeks: a tit-for-tat pair, an always-undercut player facing tit-for-tat, and the tit-for-tat player it exploits.

- **Title (bold 15px, `#1a5276`, top center):** "Cumulative Profit Over 12 Fridays (illustrative)".
- **Data (cumulative dollars, weeks 1–12):** both tit-for-tat `[500, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000, 5500, 6000]` (green `#008300`); always-undercut vs tit-for-tat `[700, 1000, 1300, 1600, 1900, 2200, 2500, 2800, 3100, 3400, 3700, 4000]` (orange `#d95926`); tit-for-tat facing the defector `[200, 500, 800, 1100, 1400, 1700, 2000, 2300, 2600, 2900, 3200, 3500]` (magenta `#d55181`). Derivation: mutual cooperation pays $500/week; the defector gets $700 in week 1, then mutual defection pays $300/week; its victim gets $200 in week 1, then $300/week.
- **Axes:** x from 65 to 560, baseline y=250, chart height 195, y scale 0–6,000; light `#e5e9ef` gridlines with right-aligned labels "$0", "$2k", "$4k", "$6k" at 0/2000/4000/6000; week numbers 1–12 (12px `#444`) below the baseline, "week" centered below them; 1px `#999` axis lines.
- **Lines:** 3px with 3px dots at every week, drawn magenta then orange then green (green on top).
- **End labels (bold 12px, left-aligned at x=570):** green "$6,000 each:" / "both tit-for-tat" at the $6,000 level; orange "$4,000:" / "always-undercut" at the $4,000 level; magenta "$3,500: tit-for-tat" / "facing the defector" slightly below the $3,500 level (offsets +22/+37 to clear the orange label).
- **Week-1 annotation:** orange 2px circle (radius 8) around the defector's week-1 point ($700), with bold 12px orange text "defector ahead only here" to its upper right.

## Where the Break-Even Sits

**Tags:** `where it's used` (blue), `shadow of the future` (green), `rule of thumb` (orange)

- **Price wars** — rival stores matching discounts is mutual undercut: every margin shrinks
- **API courtesy** — teams sharing a rate limit can hog it; all-hog gets everyone throttled
- **Code review** — rushing sloppy reviews saves time until teammates rush yours right back
- **The math** — cooperating pays once the chance of another round beats (700−500)/(700−300) = 50%
- **Design lever** — make interactions repeat and visible, and cooperation pays for itself

*Example (italic):* With an 80% chance of another round, cooperating is worth an expected $2,500 vs $1,900 for defecting; at 20% it flips to $625 vs $775.

**Key point:** You don't need contracts or altruism — just a high enough chance of meeting again. Raise that chance and purely selfish players start cooperating on their own.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart: expected total profit for "cooperate throughout" vs "undercut once, then punished forever" at three continuation probabilities.

- **Title (bold 15px, `#1a5276`, top center):** "Expected Total Profit by Chance of Another Round (illustrative)".
- **Data:** at continuation chance δ, cooperate forever = 500/(1−δ); defect once against a grim-trigger rival = 700 + δ·300/(1−δ). Groups "20% chance", "50% chance", "80% chance"; cooperate values `[625, 1000, 2500]`, defect values `[775, 1000, 1900]`.
- **Legend (top, 14px swatches + 12px `#444` text):** green swatch `rgba(0,131,0,0.5)` "cooperate throughout" at x=120; orange swatch `rgba(217,89,38,0.55)` "undercut once, then punished forever" at x=320.
- **Bars:** baseline y=240, chart height 165, scale max 2,600; group centers x=165/370/575; bars 62px wide with an 8px gap, cooperate (green fill) left, defect (orange fill) right; bold 12px value labels above each bar in the bar's line color ("$625", "$775", "$1,000", "$1,000", "$2,500", "$1,900"); 1px `#999` baseline from x=60 to x=660.
- **Group labels:** 12px `#444` group name below the baseline, then a bold 13px verdict below it: "defect wins" (orange), "break-even" (navy `#1a5276`), "cooperate wins" (green).
- **Takeaway (bold 13px navy, centered, y=290):** "break-even at 50% = (700−500)/(700−300): the shadow of the future has a formula".

## The Last-Week Trap

**Tags:** `common mistake` (red), `backward induction` (green)

- **Known ending** — announce "10 weeks, then Truck B leaves town" and cooperation collapses
- **Week 10** — with no future left to protect, both undercut in the final week, guaranteed
- **Week 9** — since week 10 is lost anyway, retaliation is toothless: undercut here too
- **The unraveling** — the logic cascades backward until defection is "rational" in week 1
- **The fix** — indefinite horizons: nobody cuts when every week might not be the last

*Example (italic):* A vendor contract with a hard, public end date invites last-quarter corner-cutting — evergreen renewals don't.

**Common confusion:** Repetition alone is not enough. A fixed, commonly known number of rounds unravels via backward induction — it is the indefinite "we may meet again" that sustains cooperation.

### Visualization (canvas `c4`, 720×300)

Two 10-week timelines: a fixed known horizon where every week defects (with a backward cascade arrow), vs an indefinite horizon where every week cooperates.

- **Title (bold 15px, `#1a5276`, top center):** "A Known Final Week Unravels Cooperation Backward".
- **Box geometry (both strips):** 10 boxes starting at x=70, each 52 wide × 40 tall with 6px gaps; inside each box a bold 13px verdict word and an 11px `#444` "wk N" label.
- **Top strip (fixed horizon):** heading bold 12px `#2c3e50` at y=58 "fixed 10-week horizon (end date known): every week defects"; boxes at y=68 filled `rgba(231,76,60,0.12)` with 1px `#e74c3c` borders, each reading "cut" in red `#e74c3c`.
- **Cascade arrow:** red `#e74c3c` 2px arrow below the boxes running right-to-left from the center of week 10 to the center of week 1, with bold 12px red caption "week 10 has no future → defect; so week 9 → ... → week 1".
- **Bottom strip (indefinite horizon):** heading at y=192 "indefinite horizon (every week may continue): cooperation holds"; boxes at y=202 filled `rgba(0,131,0,0.10)` with 1px green `#008300` borders, each reading "hold" in green; bold 16px green "...?" just past the last box.
- **Takeaway (bold 13px magenta `#d55181`, centered, y=272):** "no known last week = nothing to anchor the unraveling".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. A shared `arrow(ctx, x1, y1, x2, y2, color)` helper draws a 2px line with a filled triangular head (used by c1 and c4). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
