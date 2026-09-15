# One Good Deed as Cover: The Offset Nobody Prices

**Page type:** detail page — card-section template (see `05-cognitive-biases/25-mere-exposure-effect.html`)
**HTML title tag:** One Good Deed as Cover — Common Bad Practices

**Subtitle:** One loudly advertised good result is offered as the answer to a pile of quiet bad ones, and the arithmetic of the trade is never actually performed.

**Why this page sits in bad practices rather than cognitive biases:** this is a strategy an organization
runs, not a reasoning error an analyst commits. It works by exploiting a weakness in the audience —
that nobody prices the other column — rather than by suffering from one.

---

## Section 1 — Net Positive, Asserted but Never Added Up

**Tags:** `the argument` (blue), `one side priced` (red), `net asserted` (yellow)

**Bullets:**
- **The claim** — one funded programme is offered as the answer to a stack of quiet complaints
- **The credit side** — named, dated and carrying a figure: a 250,000 grant with a press release
- **The debit side** — three items named only in adjectives: significant, small per account, under review
- **The move** — the word "net" gets used, but only one column of the ledger ever gets a number
- **When someone prices it** — 900,000 accounts × 0.80 in fees, 66,000 outage hours × 15, 40,000 letters × 1.50
- **The total** — the harm prices at 1,770,000 against a 250,000 offset, so the net is −1,520,000
- **The size of the offset** — 14% of the harm, which is a rounding error presented as an answer
- **The framework switch** — judge us on total outcomes, judge everyone else on the rule they broke

**Key point:** "Net" is a subtraction. If only one column carries a number, no subtraction happened — the word is doing the job a calculation was supposed to do. Price both columns and the claim usually changes sign.

**Source note (`.src`):** Illustrative Example — Vendor A's ledger; every debit line is quantity × unit cost, and both totals plus the net are summed in the draw function from the same rows the chart plots. On offsetting at company scale, Kotchen & Moon (2012) find firms raise their good-works activity roughly in proportion to their bad-works activity.

### Visualization — canvas `c1`, 720×400

The same ledger drawn twice side by side: as it is presented, with a figure on the credit side and an
empty dashed box on the debit side, and as it prices out once someone fills that box in.

- **Shared construction (used by charts 1 and 3):** one literal ledger, since the counts carry the
  lesson. Credit `[{label: 'data-literacy grant', n: 1, per: 250000}]`. Debit
  `[{'notice-free fee', n: 900000, per: 0.80}, {'outage hours', n: 66000, per: 15},
  {'remediation letters', n: 40000, per: 1.50}]`. Every amount is `n × per` computed in JS:
  **720,000 + 990,000 + 60,000 = 1,770,000** against a **250,000** offset, so the net is
  **−1,520,000** and the offset is **14%** of the harm. Chart 3 reads the same 14% out of the same
  constants, so the two sections cannot drift.
- **Title (bold 15px `P.ink`, centered, y=21):** "One Ledger, Priced Two Ways"
- **Panels:** left panel x 44–336 headed bold 12px `P.ink` "AS PRESENTED"; right panel x 396–688
  headed "AS PRICED". A 1px `P.grid` vertical rule at x=366 separates them. Bars share one scale,
  `BASEY = 290`, `TOPY = 84`, height `(v / 1,770,000) × (BASEY − TOPY)`, so the 250,000 credit bar
  stands at 14% of the plot — small but plainly non-zero.
- **Left panel:** a green (`rgba(0,131,0,0.45)` stroked `P.green`) credit bar of 250,000 with its
  figure in bold 13px above and "the good deed" in 12px `P.mute` below; beside it a dashed 1.5px
  `P.mute` box spanning the full plot height holding 12px `P.mute` lines "significant",
  "small per account", "under review" and a bold `P.magenta` "no number". Footer in bold 12px
  `P.magenta`: "net: asserted, never computed".
- **Right panel:** the identical credit bar, then a stacked debit bar of three segments — fee
  `rgba(213,81,129,0.50)`/`P.magenta`, outage hours `rgba(217,89,38,0.50)`/`P.orange`, letters
  `rgba(201,133,0,0.50)`/`P.yellow` — each segment's amount printed in 12px to the right of the bar
  in its own colour, and the total in bold 13px above.
- **Footers (right panel, 12px then bold 13px):** "1,770,000 priced harm − 250,000 offset", then
  `P.magenta` "net −1,520,000", then `P.mute` "the offset is 14% of the harm". All four figures
  printed from the computed variables.
- **Caption (bold 13px `P.magenta`, centered, `h−10`):** "One column had a number. The other had adjectives."

---

## Section 2 — The Cover Only Works While the Harm Stays Arguable

**Tags:** `the cover` (violet), `where it flips` (magenta), `narrow range` (yellow)

**Bullets:**
- **What the deed buys** — not forgiveness, only room to argue that the harm is still unclear
- **While the harm is a rumour** — the good deed is worth +9.0 points of audience trust
- **Once the harm is documented** — the same deed reads as hypocrisy and adds a second charge
- **Where the cover stops paying** — at about 60% documented, past which the deed costs more than it earns
- **Fully documented harm** — the actor lands 6.0 points below one that had said nothing at all
- **Why worse than nothing** — the record now holds the harm and the contradiction, two items instead of one
- **The narrow operating range** — the strategy runs on ambiguity, so it fails exactly when evidence arrives
- **Bought in advance** — a stock of prior good works cushions the share price when bad news lands later

**Key point:** The cover is not a general-purpose shield; it has an operating range. It pays while the harm is arguable and turns negative once the harm is plain, because a documented harm next to an advertised good deed is a second finding, not a wash.

**Source note (`.src`):** Illustrative Example — a two-line model on a 0–100% documented axis; the crossing point and both endpoint gaps are interpolated from the plotted series at render time. The individual-level version of this is called moral licensing: the direction has support, but the size is not settled — a meta-analysis puts it small and several individual paradigms have failed to replicate, so no effect size is quoted here.

### Visualization — canvas `c2`, 720×340

Two trust-change lines against how well documented the harm is: one for an actor that also ran a
good deed, one for an actor that stayed silent. The gap between them is the value of the cover, and
it crosses zero partway along.

- **Construction:** `x` is the share of the harm that is documented, 0 (rumour) to 1 (fully
  documented), stepped 0.01. Silent actor: `without(x) = −12x`. Actor with the deed:
  `with(x) = −12x + 9 − 15x`, i.e. goodwill of 9 points, minus a hypocrisy penalty of 15 points that
  only bites in proportion to how documented the harm is. The value of the cover is therefore
  `9 − 15x`, positive to the left of `x = 0.6` and negative to the right. Non-degenerate: the gap is
  **+9.0** at `x = 0`, **0** only at the single crossing point, and **−6.0** at `x = 1`; no
  probability, no division, nothing pinned at exactly 0 or 1.
- **Title (bold 15px `P.ink`, centered, y=21):** "Trust Change Against How Well Documented the Harm Is"
- **Axes:** plot box `PX = 64`, `PW = w − 110`, `TOPY = 64`, `BASEY = 248`; vertical range −20 to +12
  with `P.grid` gridlines every 8 and 12px `P.mute` labels; horizontal ticks at 0, 25, 50, 75, 100%
  labelled "0% rumour" and "100% documented" at the ends.
- **Zero line:** solid 1.5px `P.mute`, labelled 12px "no change".
- **Lines:** silent actor in dashed 2.5px `P.mute`; actor with the deed in solid 3px `P.violet`.
  The band between them is filled per step — `rgba(39,174,96,0.16)` where the deed is ahead,
  `rgba(213,81,129,0.16)` where it is behind — so the two operating ranges read as two shaded regions.
- **Crossing marker:** dashed 2px `P.magenta` vertical at the interpolated crossing, with bold 19px
  `P.magenta` "60%" and bold 12px "documented — the cover stops paying" beside it.
- **Endpoint callouts (bold 12px):** `P.violet` "+9.0 points while it stays a rumour" at the left end,
  `P.magenta` "6.0 points worse than staying silent" at the right end — both differences read off the
  plotted arrays, not typed in.
- **Caption (bold 13px `P.violet`, centered, `h−10`):** "Past the crossing, the good deed is the second charge on the record."

---

## Section 3 — Four Checks That Do Not Require Trusting Anyone's Motive

**Tags:** `the test` (green), `behaviour only` (aqua), `four checks` (orange)

**Bullets:**
- **Check one, timing** — did the good deed come before the harm, or only after somebody noticed it
- **The counterfactual** — would that deed have happened with no complaint sitting on the table
- **Check two, size** — is the offset priced at the size of the harm or at the cost of the announcement
- **What size says here** — Vendor A offsets 14% of the priced harm; Division B offsets 140% and clears it
- **Check three, the fix** — does the actor also oppose the change that would make the deed unnecessary
- **Check four, disclosure** — does the claim hold under full disclosure, or only while the harm is vague
- **The two useful ones** — the fix and the disclosure test read straight off behaviour, no motive needed
- **How the three cases score** — Vendor A clears none of the four, Team C two, Division B all four

**Key point:** Motive is not observable, so build the test out of things that are. Two checks need no stated intention at all: whether the actor also fights the structural fix, and whether the claim survives full disclosure. An offset that requires the harm to stay vague is not an offset.

**Source note (`.src`):** Illustrative Example — three constructed cases scored on four checks; each size ratio is computed as offset ÷ priced harm and each row's pass count is tallied in the draw function. Vendor A's 14% is the same figure the first chart computes, from the same constants. The environmental form of this pattern is usually called greenwashing; the general form, reputation laundering.

### Visualization — canvas `c3`, 720×360

A three-case by four-check scorecard, with the two checks that need only observed behaviour banded
off, and each case's pass count tallied at the right.

- **Data (literal, because the pattern of marks is the lesson):** rows are
  `Vendor A — the offset as cover` `[fail, fail, fail, fail]`,
  `Team C — mixed, hard to call` `[pass, fail, unknown, pass]`,
  `Division B — a real net positive` `[pass, pass, pass, pass]`.
  The size column carries its own numbers: Vendor A `250,000 / 1,770,000` from the shared ledger,
  Team C `260,000 / 1,180,000`, Division B `420,000 / 300,000`. Ratios computed in JS to
  **14%**, **22%**, **140%**. Pass counts tallied to **0/4**, **2/4**, **4/4**.
- **Title (bold 15px `P.ink`, centered, y=21):** "Three Cases, Four Checks, No Motive Required"
- **Grid:** case labels in bold 12px `P.ink` in the left column x 26–190; four check columns 108px
  wide starting at x = 200; score column at x = 644. Row centres at y = 132, 192, 252; header at
  y = 62 and 78 in bold 12px `P.mute`, two lines per column: "1 timing /
  before or after", "2 size / offset vs harm", "3 the fix / opposes it?",
  "4 disclosure / survives it?". Horizontal 1px `P.grid` rules between rows.
- **Band:** a `rgba(25,158,112,0.10)` rectangle behind check columns 3 and 4 with a dashed 1.5px
  `P.aqua` outline, labelled bold 12px `P.aqua` "observable from behaviour alone — no stated motive needed"
  beneath the grid.
- **Marks:** pass is a `P.green` filled disc with a white tick, fail a `P.magenta` disc with a white
  cross, unknown a hollow `P.mute` disc with a dash. Under each size-column mark, the computed ratio
  in bold 12px, coloured by its own verdict.
- **Score column:** each row's tally in bold 19px, `P.green` at 4, `P.yellow` at 2, `P.magenta` at 0,
  with "checks passed" once in 12px `P.mute` in the header.
- **Footnote (12px `P.mute`):** "checks 1 and 2 need a counterfactual and a price on the harm — checks
  3 and 4 need only the record".
- **Caption (bold 13px `P.aqua`, centered, `h−10`):** "An offset that needs the harm to stay vague was never an offset."

---

## Regeneration instructions

- **Template:** the card-section layout copied verbatim from
  `05-cognitive-biases/25-mere-exposure-effect.html` — the whole `<style>` block, the
  `setup()` canvas helper, the `lcg()` seeded PRNG, the `P` palette object, the `__charts` array and
  the debounced resize tail, the `table.layout` / `text-col` / `viz-col` 50/50 structure, the `.tags`
  pills, `.key-point` and `.src` conventions.
- **Structure:** exactly three `.card-section` blocks, each with an `<h2>` and a `table.layout` row
  of `td.text-col` (50%) then `td.viz-col` (50%). The 50/50 split is fixed; a chart is shrunk through
  the canvas cap, never by narrowing the viz column.
- **Text column order:** `.tags` row of three pills → `<ul>` of 8 bullets → one `.key-point` →
  one `.src`. Bullet form is `<b>short bold label</b> — a phrase`, around 90–100 characters.
- **Canvas:** intrinsic `width="720"` with heights 400, 340, 360; CSS `width: 100%`, 720px cap.
- **Determinism:** no `Math.random()`. The canonical `lcg()` helper is present and used where a draw
  is needed; all three charts here are driven by literal constants because the ledger amounts, the
  two-line model and the scorecard marks are the content, so nothing is sampled. Every printed
  figure — each debit amount, both totals, the net, the 14% ratio, the crossing point, both endpoint
  gaps, all three size ratios and all three pass counts — is computed inside the draw function and
  printed from that variable.
- **Reconciliation:** the 1,770,000 harm, the 250,000 offset, the −1,520,000 net and the 14% ratio
  come from one shared `LEDGER` constant used by charts 1 and 3, and the prose repeats those same
  digits.
- **Naming and tone:** organizations only — Vendor A, Team C, Division B. No real companies, no real
  people, no politics. Professional and neutral; the page describes a pattern rather than condemning
  one. Banned wording avoided throughout: no "consequentialist rationalization", "deontological",
  "special pleading", "legitimizing myth", "passive revolution", "philanthrocapitalism",
  "moral figleaf" or "externality" — the last is written as "a cost nobody put a number on".
- **Sparingly used terms, once each and never in a heading:** moral licensing (section 2 `.src`, with
  the effect size explicitly left unquoted), greenwashing and reputation laundering (section 3 `.src`).
- **No navigation, no back or home links, no cross-page links, no status badges, no `.nav` CSS.**
