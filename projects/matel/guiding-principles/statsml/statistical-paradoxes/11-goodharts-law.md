# Goodhart's Law: The Number You Pay For Stops Telling You The Truth

**Page type:** paradox detail page. Layout, CSS, `P` palette, `setup()`, `lcg()`, `fit()`,
`.card-section` block and all rules come verbatim from `/tmp/paradox-skeleton.html`.
Four sections, canvases `c1`–`c4`, 50/50 columns.

**HTML title tag:** Goodhart's Law — Statistical Paradoxes

**Subtitle:** A measure that describes the world honestly can stop describing it the moment
someone is paid to move it

---

## 1. The Support Desk That Closed Twice As Many Tickets And Fixed Fewer Problems

**Tags:** core idea (blue) · incentives (green) · decoupling (orange)

- **The measure** — tickets closed per agent per week, cheap to count and easy to chart.
- **The goal** — problems that stay fixed, counted as tickets with no repeat contact in 30 days.
- **Before the target** — closing a ticket almost always meant a problem was actually solved.
- **The target moment** — in May a bonus is attached to closures, and nothing else changes.
- **The cheap route** — close on first reply, split one issue into three, reopen as a new ticket.
- **After the target** — closures climb steeply while genuine fixes drift the other way.
- **Why the link snaps** — closing was a *side effect* of fixing, and side effects can be produced alone.
- **What the dashboard shows** — one rising green line, because nobody plots the second series.

**Example:** From May to December closures go 14 → 29 per agent-week (+107%), while tickets that
stay fixed go 10.5 → 8.4 (−20%): the share of closures that were real fixes falls from 75% to 29%.

**Key point:** A measure survives only while producing it is harder than producing the thing it
measures; a bonus makes the shortcut worth finding.

**Source note:** Illustrative Example.

**Chart c1** — two lines over 12 months, height 320:
- x-axis Jan–Dec; y-axis "tickets per agent per week", 0 to 32.
- `P.magenta` line = closures `[12,13,13,14,14,17,20,23,25,27,28,29]`, thick, labelled at its end.
- `P.green` line = tickets that stayed fixed `[9.0,9.8,9.8,10.5,10.5,10.4,10.0,9.6,9.2,8.8,8.6,8.4]`.
- Vertical `P.orange` dashed line at May with the note "bonus attached to closures".
- Shaded `P.grid` band between the two lines after May to make the gap visible.
- Big bold 19px figure printed from the arrays: the +107% / −20% pair.
- Caption: "Same measure, same definition — only the incentive changed."

---

## 2. Training A Model On The Label You Can Collect Instead Of The Outcome You Want

**Tags:** machine learning (blue) · proxy labels (green) · optimization (orange)

- **The proxy label** — clicks, watch time and reactions are logged for free; usefulness is not.
- **As a measurement** — clicks genuinely track helpfulness across the articles people already wrote.
- **As an objective** — the model is now free to search for clicks *without* helpfulness.
- **Where it lands** — a click target yields bait headlines, a time target yields padding.
- **An engagement target** — rewards whatever provokes a reaction, and outrage provokes reliably.
- **The ML-specific trap** — the label you can collect is rarely the outcome you actually want.
- **Why validation misses it** — the held-out set is scored on the proxy, so the model looks better.
- **The tell** — the proxy improves while every metric you did not optimize quietly worsens.

**Example:** Across existing articles clicks and helpfulness move together; the optimizer's picks
push mean clicks 0.43 → 1.10 while mean helpfulness falls 0.37 → −0.38, and pooling both groups
turns the relationship negative. (Chart prints the two link strengths: +0.92 ⇒ −0.41.)

**Key point:** A proxy is trustworthy in the range where it was measured and untrustworthy exactly
where an optimizer is pushing it.

**Source note:** Illustrative Example.

**Chart c2** — scatter, height 320, seeded `lcg(42)`, x = click rate, y = actual helpfulness:
- 45 `P.blue` dots = existing articles, quality `q` and small bait; `click = 0.75q + 1.10·bait`,
  `help = 0.90q − 0.85·bait`, both with a small seeded jitter.
- 18 `P.magenta` dots = what the click-optimizer selects: low `q`, high bait; same two formulas.
- `P.blue` fitted line through the 45 blue dots only, with its r printed via `fmtR`.
- `P.orange` arrow from the blue cloud's centre to the magenta cloud's centre, labelled
  "where the optimizer pushes".
- Small gray parenthetical giving the pooled r computed over all 63 dots.
- Caption: "A fine ruler, pointed in a direction it was never measured in."

---

## 3. England's Four-Hour Emergency Department Clock

**Tags:** documented case (blue) · public reporting (green) · clock stops (orange)

- **The rule** — from the mid-2000s English emergency departments reported the share of patients
  dealt with inside four hours.
- **The measure chosen** — time from arrival to admission, transfer or discharge, per patient.
- **What it was for** — long, unsafe waits, which the number does describe when nobody is scored on it.
- **What the distribution showed** — departures cluster hard in the minutes just before the deadline.
- **Documented routes** — holding patients in corridors or in ambulances so the clock has not started.
- **More routes** — moving patients to short-stay or observation areas, which stops the clock early.
- **The published finding** — the target was largely met while the pattern of waits shifted around it.
- **The analytic point** — a threshold rewards the minute before it and nothing after it.

**Example:** In the illustrative distribution shown, 1,044 of 1,099 patients are dealt with inside
four hours, the final quarter-hour before the deadline holds well over twice its neighbouring bins,
and the bin just after the deadline holds about a tenth as many patients as the bin before it.

**Key point:** A threshold target reshapes the distribution around the threshold; the average wait
can barely move while the histogram grows a spike.

**Source note:** Mechanism documented in the health services literature — Bevan & Hood, *Public
Administration* 84(3), 2006; Locker & Mason, *BMJ* 330, 2005; Mason et al., *Annals of Emergency
Medicine* 59(5), 2012. Histogram below is an Illustrative Example, not reproduced data.

**Chart c3** — histogram, height 320, x = time in department in 15-minute bins to 6 hours:
- Bin counts `[4,12,26,44,62,78,88,92,90,84,76,68,60,54,58,148,14,11,8,7,5,4,3,3]`.
- Bins before the deadline `P.aqua`; the deadline bin `P.magenta`; bins after it `P.mute`.
- Vertical `P.orange` dashed line at four hours labelled "reported deadline".
- Bold 19px figure = the within-deadline share, computed by summing the array.
- Small gray parenthetical = spike-to-neighbour ratio, also computed from the array.
- Caption: "Nothing here is about how long care takes — it is about where the line was drawn."

---

## 4. Measures That Survive Being Watched

**Tags:** boundary (blue) · guard metrics (green) · audit (orange)

- **Two ingredients** — the law needs someone with an incentive *and* room to move the measure alone.
- **Missing the incentive** — a number nobody is rewarded on stays an honest instrument for years.
- **Missing the room** — if the only way to raise the number is to do the real work, the target is safe.
- **Expensive to move** — externally verified or physically constrained measures resist shortcuts.
- **Guard metric** — pair each target with the number the cheap route would visibly damage.
- **Reserve instruments** — keep some measures deliberately un-targeted so you retain a true reading.
- **Audit the gap** — periodically re-measure the goal directly and compare it against the proxy.
- **Rotate and dilute** — many weakly-weighted measures are harder to move than one heavily-paid one.

**Example:** Over 12 periods the gap between measure and goal reaches 51 points when the measure is
rewarded and cheap to move, 5 points when it is rewarded but expensive to move, and 1 point when it
is watched but nobody is paid on it.

**Key point:** Goodhart's law is a statement about incentives and slack, not about people — remove
either ingredient and the measure keeps working.

**Source note:** Illustrative Example.

**Chart c4** — three gap-over-time lines, height 320, x = 12 periods, y = measure minus goal:
- Series A (`P.magenta`, "rewarded, cheap to move"): measure
  `[50,51,52,53,54,62,71,79,86,91,95,98]`, goal `[50,51,52,52,53,53,52,51,50,49,48,47]`.
- Series B (`P.blue`, "rewarded, expensive to move"): measure
  `[50,51,52,53,54,57,60,62,64,66,67,68]`, goal `[50,51,52,53,54,56,58,59,61,62,63,63]`.
- Series C (`P.green`, "watched, not rewarded"): measure
  `[50,52,53,54,55,56,57,58,59,60,61,62]`, goal `[50,51,53,54,55,55,57,58,58,60,61,61]`.
- Gaps computed in the draw function; each line's final gap printed at its right end.
- Vertical `P.orange` dashed line at period 5 labelled "target set".
- Caption: "The measure drifts only where the shortcut is cheap and someone is paid to take it."
