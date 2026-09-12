# KL Divergence

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** KL Divergence

**Subtitle:** KL divergence is the price of a wrong belief — the extra yes/no questions (bits) you pay per observation for acting on distribution Q when the world actually runs on distribution P

## One Coffee Cart, Two Beliefs About Orders

**Tags:** `core idea` (blue), `wrong belief` (green), `extra bits` (orange)

- **The cart** — a coffee cart sells three drinks; the true order mix is latte 50%, tea 25%, mocha 25%
- **The guess** — a new manager, never having watched a rush, guesses latte 25%, tea 50%, mocha 25%
- **The game** — each order is identified by yes/no questions; smart plans ask about common drinks first
- **The price** — a question plan built on the wrong guess wastes questions, order after order
- **The name** — that average waste, in bits per order, is the KL divergence from truth P to belief Q

*Example (italic):* The manager's plan asks "tea?" first because he believes tea rules — but half the customers want lattes, so most orders cost an extra question.

**Key point:** KL divergence measures how costly it is to act on belief Q when reality follows P — zero when the guess is exactly right, and growing as the guess gets worse.

### Visualization (canvas `c1`, 720×300)

Grouped bar chart: for each drink, two bars side by side — the true share P (blue) and the manager's believed share Q (orange) — making the latte/tea swap visible at a glance.

- **Title (bold 15px, `#1a5276`, top center):** "True Mix P vs the Manager's Guess Q".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 180; y axis = share of orders 0% to 60%, 12px `#444` tick labels "0%", "20%", "40%", "60%" with light `#e5e9ef` gridlines; x axis = three drink groups centered at x = `[190, 370, 550]` with bold 13px `#2c3e50` labels "latte", "tea", "mocha" below the baseline.
- **Bars:** per group two 54px-wide bars 6px apart; truth P blue `#2a78d6` at heights for `[0.50, 0.25, 0.25]`, guess Q orange `#d95926` at heights for `[0.25, 0.50, 0.25]`; bold 12px value label in the bar's color above each bar: "50%", "25%", "25%" (blue) and "25%", "50%", "25%" (orange).
- **Legend (12px, top left inside plot):** blue swatch "truth P" and orange swatch "guess Q".
- **Annotation (bold 12px magenta `#d55181`, two lines, near x=430, y=80):** "P and Q swap latte and tea —" / "every latte order will overpay".
- **Caption (12px `#444`, bottom right):** "illustrative — one day at the cart".

## Counting the Extra Bits, Drink by Drink

**Tags:** `worked example` (blue), `by hand` (green)

- **Truth-built code** — with P known, ask "latte?" first: latte costs 1 question, tea 2, mocha 2
- **Truth-built cost** — average = 0.50×1 + 0.25×2 + 0.25×2 = 1.50 questions (bits) per order
- **Guess-built code** — the manager asks "tea?" first: tea costs 1 question, latte 2, mocha 2
- **Guess-built cost** — same customers, wrong plan: 0.50×2 + 0.25×1 + 0.25×2 = 1.75 bits per order
- **The gap** — 1.75 − 1.50 = 0.25 extra bits per order; that gap IS KL(P‖Q)
- **The formula** — sum of p×log2(p/q): 0.50×log2(2) + 0.25×log2(0.5) + 0.25×log2(1) = 0.5 − 0.25 + 0 = 0.25

*Example (italic):* Over 100 orders the manager's plan burns about 175 questions where 150 would do — 25 wasted questions traced entirely to believing Q instead of P.

**Key point:** KL(P‖Q) = Σ p·log2(p/q) = 0.25 bits — exactly the extra cost per order of a code built on the guess Q but paid for at the truth P's frequencies.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart of cost contributions: for each drink, the bits it contributes to the average under the truth-built code vs the guess-built code, with the 1.50 vs 1.75 totals called out.

- **Title (bold 15px, `#1a5276`, top center):** "Bits per Order, Drink by Drink: 1.50 vs 1.75".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 180; y = contribution to average (bits) 0 to 1.2, 12px `#444` tick labels "0", "0.4", "0.8", "1.2" with light `#e5e9ef` gridlines; x = three drink groups centered at x = `[190, 370, 550]`, bold 13px `#2c3e50` labels "latte (50%)", "tea (25%)", "mocha (25%)".
- **Bars:** per group two 54px-wide bars 6px apart; truth-built code green `#008300` at contributions `[0.50, 0.50, 0.50]` (0.50×1, 0.25×2, 0.25×2), guess-built code orange `#d95926` at contributions `[1.00, 0.25, 0.50]` (0.50×2, 0.25×1, 0.25×2); bold 12px value labels above each bar in the bar's color: "0.50", "0.50", "0.50" and "1.00", "0.25", "0.50".
- **Legend (12px, top right inside plot):** green swatch "code built on truth — total 1.50", orange swatch "code built on guess — total 1.75".
- **Annotation (bold 13px magenta `#d55181`, two lines, near x=190, y=70, beside the tall latte bar):** "the common drink got the long code —" / "gap = 1.75 − 1.50 = 0.25 bits = KL(P‖Q)".
- **Caption (12px `#444`, bottom right):** "code lengths in whole questions; illustrative".

## Where the Extra-Bits Penalty Runs the Show

**Tags:** `where it's used` (blue), `loss functions` (green), `drift` (orange)

- **Training models** — cross-entropy = entropy floor + this bill; the floor is fixed, so training shrinks exactly the KL
- **Zero means right** — KL is 0 only when Q matches P everywhere; any mismatch costs strictly positive bits
- **Blow-up warning** — believing a common outcome is rare is catastrophic: as q→0 with p large, KL explodes
- **Drift alarms** — production monitors compare this week's data to training data via KL; a rising value flags drift
- **Model comparison** — of two candidate models, the one with smaller KL from the data wastes fewer bits per prediction

*Example (italic):* Slide the manager's believed latte share from 50% (truth, 0 bits) down to 5%, and the penalty climbs from 0 to 1.29 bits per order — wrongness is priced, not just flagged.

**Key point:** KL is the quantity most classifiers are literally trained to shrink — a mismatch meter that reads zero only at the truth and punishes confident wrongness hardest.

### Visualization (canvas `c3`, 720×300)

Single-panel curve: KL(P‖Q) in bits as the manager's believed latte share q slides from 5% to 70% (tea takes the rest, mocha fixed at 25%), showing the zero at the truth and the blow-up on the left.

- **Title (bold 15px, `#1a5276`, top center):** "The Penalty Curve: Zero at the Truth, Explosive When Wrong".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 180; x = believed latte share q, 0 to 0.75, 12px `#444` tick labels "0%", "10%", ..., "70%" every 0.1; y = KL(P‖Q) in bits, 0 to 1.4, 12px `#444` tick labels "0", "0.5", "1.0" with light `#e5e9ef` gridlines; 12px `#444` axis captions "believed latte share q" (below) and "extra bits per order" (rotated left).
- **Curve:** violet `#4a3aa7` 3px line through hardcoded points at q = `[0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70]`, KL = `[1.29, 0.82, 0.55, 0.38, 0.25, 0.16, 0.09, 0.04, 0.01, 0.00, 0.01, 0.05, 0.14, 0.34]`; fill under the curve `rgba(74,58,167,0.10)`.
- **Truth marker:** green `#008300` 7px dot at (0.50, 0.00) on the baseline; bold 13px green label above: "q = truth: 0 extra bits".
- **Guess marker:** orange `#d95926` 7px dot at (0.25, 0.25); vertical dashed orange line (dash 4/3) down to the baseline; bold 12px orange label beside the dot: "manager's guess: 0.25".
- **Annotation (bold 12px `#e74c3c`, two lines, near x=0.09 on the rising left arm, y=85):** "calling a common drink rare" / "is the expensive mistake".
- **Caption (12px `#444`, bottom right):** "Q = (q, 0.75−q, 0.25) against P = (0.50, 0.25, 0.25); illustrative".

## Why KL Is Not a Distance

**Tags:** `common mistake` (red), `asymmetry` (orange)

- **The habit** — people call KL a "distance between distributions", but real distances read the same both ways
- **A pessimist's menu** — take belief Q2 = latte 5%, tea 90%, mocha 5% against the true P = 50/25/25
- **One direction** — KL(P‖Q2) = 1.78 bits: coding real orders with the pessimist's plan is very costly
- **The other direction** — KL(Q2‖P) = 1.38 bits: coding the pessimist's world with the truth's plan costs less
- **Why they differ** — the penalty is paid at the FIRST argument's frequencies; latte alone contributes 1.66 of the 1.78
- **Say it right** — "the divergence from P to Q", never "the distance between P and Q"

*Example (italic):* Same two menus, two different numbers — 1.78 bits one way, 1.38 the other — so a "how far apart" question must always say which one is the truth.

**Common mistake:** Treating KL(P‖Q) and KL(Q‖P) as interchangeable. They answer different questions, and the gap between them is largest exactly when Q calls a common outcome rare.

### Visualization (canvas `c4`, 720×300)

Two horizontal bars on a shared bits axis: the same pair of distributions measured in both directions, with visibly different lengths proving the asymmetry.

- **Title (bold 15px, `#1a5276`, top center):** "Same Two Menus, Two Different Numbers".
- **Axis:** horizontal 2px `#999` line at y=235 from x=250 to x=680 (width 430), bits 0 to 2.0; 12px `#444` tick labels "0", "0.5", "1.0", "1.5", "2.0" below, light `#e5e9ef` vertical gridlines above each tick.
- **Row 1 (bar centered at y=105), left-aligned 12px `#444` two-line label at x=20:** "KL(P ‖ Q2)" / "truth P, pessimist's code"; bar 26px tall from 0 to 1.78, fill `rgba(217,89,38,0.30)`, 2px orange `#d95926` border; bold 13px orange value label "1.78 bits" just right of the bar end.
- **Row 2 (bar centered at y=175), label at x=20:** "KL(Q2 ‖ P)" / "pessimist's world, truth's code"; bar 26px tall from 0 to 1.38, fill `rgba(42,120,214,0.30)`, 2px blue `#2a78d6` border; bold 13px blue value label "1.38 bits" just right of the bar end.
- **Gap bracket:** thin dashed `#6b7280` (dash 4/3) vertical guides at bits 1.38 and 1.78 from y=75 to y=200; 12px `#6b7280` label "0.40-bit gap" between them at y=68.
- **Annotation (bold 13px magenta `#d55181`, centered near y=272):** "order matters — a real distance would read the same both ways".
- **Caption (12px `#444`, bottom right):** "P = (0.50, 0.25, 0.25), Q2 = (0.05, 0.90, 0.05); illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar heights, curve points, and both direction values are the hardcoded arrays above (no randomness); the c3 curve values are true KL(P‖Q) in bits for Q = (q, 0.75−q, 0.25) against P = (0.50, 0.25, 0.25), rounded to 2 decimals; the 1.78/1.38 pair are true KL values for P vs Q2 = (0.05, 0.90, 0.05), rounded to 2 decimals; text numbers match chart numbers throughout.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
