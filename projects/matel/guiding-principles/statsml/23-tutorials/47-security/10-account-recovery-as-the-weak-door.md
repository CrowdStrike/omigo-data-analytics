# Account Recovery as the Weak Door

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Account Recovery as the Weak Door

**Subtitle:** Forgot password is a second login — and it often checks less than the first one

## Alice's Front Door and the Side Door Nobody Locked

**Tags:** `core idea` (blue), `forgot-password flow` (orange), `defensive` (green)

- **The front door** — Alice's account uses one long unique password, 16 characters drawn from a 72-symbol set
- **The side door** — the same account also stores one security question: the name of her first school
- **The attack** — Bob never touches the password; he clicks "forgot password" and is asked the question
- **The answer** — Alice's public profile page lists her schools, so Bob reads the answer rather than guessing it
- **What opened** — the recovery flow emails a reset link, Bob sets a new password, and the strong one is gone
- **The framing** — two independent ways in means account strength is the weaker door, not the stronger one

*Example (italic):* The password would take an attacker roughly 10^29 guesses; the question took Bob one search and one form submission.

**Key point:** A recovery flow is a second login path. If it authenticates with something weaker than the password, the password stops being what protects the account.

### Visualization (canvas `c1`, 720×300)

Two-door diagram: the attacker ignores the strong front door and walks to the weak recovery door, both of which reach the same account.

- **Title (bold 15px, `#1a5276`, top center):** "The Attacker Walks Past the Strong Door".
- **Attacker box:** grey rounded box at x=28, y=132, 108×48, fill `rgba(107,114,128,0.12)`, 2px `#6b7280` border, centered 12px `#2c3e50` text "attacker".
- **Front-door box:** green rounded box at x=248, y=52, 168×64, fill `rgba(0,131,0,0.12)`, 2px `#008300` border, three lines of centered text: bold 12px "front door: password", 12px "16 chars from 72 symbols", bold 12px `#008300` "≈ 98.7 bits".
- **Recovery-door box:** orange rounded box at x=248, y=188, 168×64, fill `rgba(217,89,38,0.14)`, 2px `#d95926` border, three lines: bold 12px "side door: security question", 12px "name of first school", bold 12px `#d95926` "≈ 3.5 bits".
- **Account box:** blue rounded box at x=548, y=120, 148×60, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, two lines of 12px text "Alice's account" / "same access either way".
- **Ignored path:** dashed 2px `#6b7280` line (dash 6/5) from the attacker box up to the front-door box, with a 12px `#6b7280` label "not attempted" at its midpoint; no arrowhead.
- **Taken path:** solid 3px `#d95926` line from the attacker box down to the recovery-door box with an arrowhead, 12px bold `#d95926` label "forgot password" beside it.
- **Door-to-account arrows:** 2px `#008300` dashed line from the front-door box to the account box; solid 3px `#d95926` arrow from the recovery-door box to the account box.
- **Annotation (bold 13px violet `#4a3aa7`, centered near x=360, y=290):** "account strength = min(front door, recovery door)".
- **Box style:** 8px corner radius, centered text.
- **Caption (12px `#444`, right-aligned just under the title, y=42):** "illustrative example".

## Counting a Thousand Answers to One Question

**Tags:** `worked example` (blue), `guessable answers` (orange)

- **The survey** — ask 1,000 people the same question, "name of your first school", and tally every answer
- **The shape** — a handful of very common school names dominate, then a long tail of near-unique answers
- **The tally** — the top ten answers are given by 90, 70, 55, 45, 35, 30, 25, 20, 17 and 13 people
- **The sum** — those ten answers cover 90+70+…+13 = 400 of 1,000 people, which is 400/1,000 = 40%
- **The consequence** — an attacker allowed ten guesses per account succeeds on 40% of accounts
- **Fewer guesses** — three guesses already cover 90+70+55 = 215 of 1,000, so 215/1,000 = 21.5%
- **The tail** — the other 470 answers are shared by only 513 people, and no attacker bothers with them

*Example (italic):* Birth city, pet name and mother's maiden name all cluster the same way — a few dozen answers cover most of any population (distribution illustrative).

**Key point:** A security question's answer is not drawn uniformly. Ten guesses against a distribution this skewed is not a long shot — here it is a 40% chance, computed from the tally.

### Visualization (canvas `c2`, 720×300)

Bar chart of the answer distribution: 20 named answers in descending order plus a not-to-scale long-tail block, with the top-10 coverage computed from the plotted bars.

- **Title (bold 15px, `#1a5276`, top center):** "1,000 People, One Question: Answers Cluster Hard".
- **Data (hardcoded literal array, no randomness):** `counts = [90,70,55,45,35,30,25,20,17,13,12,11,10,9,9,8,8,7,7,6]`; total population 1,000; tail = 1,000 − 487 = 513 people across 470 further answers.
- **Axes:** origin x=60, baseline y=250, plot height 175; y = people, 0 to 90, gridlines `#e5e9ef` at 30/60/90 with 12px `#444` right-aligned tick labels; 2px `#999` x-axis from x=60 to x=630.
- **Bars:** 20 slots of 21px starting at x=62, bar width 15px, heights `counts[i] / 90 * 165` px. Bars 1–10 fill `rgba(217,89,38,0.45)` with 2px `#d95926` border (the guessable head); bars 11–20 fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` border.
- **Tail block:** dashed 2px `#6b7280` rectangle (dash 5/4), x=505 to x=625, top y=224, height 26, fill `rgba(107,114,128,0.10)`; two 12px `#6b7280` lines centered above it at y=204 and y=190: "470 rarer answers, 513 people" and "(not to scale)".
- **Rank labels (12px `#444`, below baseline at y=268):** "1", "5", "10", "15", "20" under the corresponding bar centers; axis caption 12px `#444` centered at (345, 288): "answers ranked by popularity".
- **Head bracket:** 2px `#d95926` line from the left edge of bar 1 to the right edge of bar 10 at y=64, with 6px downward ticks at both ends.
- **Annotation (bold 13px `#d95926`, centered above the bracket at y=52):** text built at render time from the array — "top 10 answers = 400 of 1,000 people = 40.0%" (numerator and percentage computed from `counts`, never hardcoded in the string).
- **Caption (12px `#444`, bottom right):** "distribution illustrative".

## Ten Guesses Against a Hundred Bits

**Tags:** `where it's used` (blue), `security questions` (orange), `rule of thumb` (green)

- **Password space** — 16 characters from 72 symbols is 72^16 ≈ 5.2 × 10^29 possibilities, about 98.7 bits
- **Guesses for 40%** — covering 40% of that space needs 0.4 × 5.2 × 10^29 ≈ 2.1 × 10^29 guesses
- **Same 40%, other door** — the question reaches the same 40% in ten guesses: a gap of 28 orders of magnitude
- **Effective strength** — the question's most common answer is 90/1,000 = 9%, so −log2(0.09) ≈ 3.5 bits
- **Often not guessed at all** — public profiles, obituaries and quiz posts make many answers simply readable
- **Cannot be rotated** — a leaked password is replaced in a minute; a first school's name is true forever
- **Chained doors** — recovery email and phone are recovery paths too, so the weakest link runs down the chain
- **The fix** — store a random string as the answer in a password manager, or use one-time recovery codes

*Example (italic):* Treating the answer as just another random secret makes the recovery door as strong as the password, because it is then the same kind of secret.

**Key point:** Compare the two doors in guesses, not in feelings: 10 versus 2.1 × 10^29 for the same 40% chance. Recovery answers must be random secrets, not facts about a person.

### Visualization (canvas `c3`, 720×300)

Log-scale horizontal bars: guesses needed for a 40% chance of entry through each door.

- **Title (bold 15px, `#1a5276`, top center):** "Guesses Needed for a 40% Chance of Entry".
- **Axis:** log10 scale from 0 to 30, origin x=190, plot width 460, so 1 decade = 460/30 px; gridlines `#e5e9ef` with 12px `#444` tick labels "1", "10^10", "10^20", "10^30" at log10 = 0, 10, 20, 30 placed at y=250; 2px `#999` baseline at y=240 from x=190 to x=650.
- **Bars (30px tall, left edge x=190):** row 1 top y=86 — security question, value 10 guesses, log10 = 1, width `1/30*460` ≈ 15px, fill `rgba(217,89,38,0.50)`, 2px `#d95926` border; row 2 top y=156 — password, value 2.1 × 10^29, log10 ≈ 29.32, width ≈ 450px, fill `rgba(0,131,0,0.45)`, 2px `#008300` border. Bar widths computed in JS as `Math.log10(value)/30*460`.
- **Row labels (right-aligned 12px `#444`, ending at x=180):** two lines each — "security question" / "(10 guesses)" at y≈98/114, and "16-char password" / "(2.1 × 10^29 guesses)" at y≈168/184.
- **Value labels:** bold 12px `#d95926` "10" just right of the short bar; bold 12px `#008300` "2.1 × 10^29" inside the long bar's right end, right-aligned at x=642.
- **Annotation (bold 13px violet `#4a3aa7`, left-aligned at x=190, y=62):** "same 40% chance, 28 orders of magnitude apart" — the 28 computed as `Math.round(29.32 − 1)`, the decade gap between the two bars.
- **Caption (12px `#444`, bottom right):** "40% from the tally above; password space exact".

## Two Doors Do Not Add: AND Versus OR

**Tags:** `common mistake` (red), `logic` (violet)

- **The assumption** — adding a security question feels like adding a lock, so the account feels safer
- **AND means adding** — required together, password + question ≈ 98.7 + 3.5 = 102.2 bits of work
- **OR means the minimum** — either one alone lets you in, so the work is min(98.7, 3.5) = 3.5 bits
- **Which one shipped** — nearly every "forgot password" flow is an OR: the question replaces the password
- **The arithmetic of it** — an alternative path can only subtract; it never adds to the account's strength
- **The audit question** — ask of each recovery path: is it an extra check, or a way around the checks?
- **Doing it right** — keep recovery as an AND (question plus emailed code), or make its secret random too

*Example (italic):* Two independent 3.5-bit questions chained as AND give 7 bits; offered as OR they give 3.5 bits, the same as one.

**Common mistake:** Counting recovery as extra security. Under OR, the account's strength is the weakest enabled path — so removing a weak recovery question can raise security without touching the password.

### Visualization (canvas `c4`, 720×300)

Vertical bars in bits: password alone, password AND question, password OR question.

- **Title (bold 15px, `#1a5276`, top center):** "Bits of Work: AND Adds, OR Takes the Minimum".
- **Axes:** origin x=70, baseline y=248, plot width 580, plot height 180; y = bits, 0 to 110, gridlines `#e5e9ef` at 25/50/75/100 with 12px `#444` right-aligned tick labels; 2px `#999` x-axis.
- **Bars (86px wide, centered at x = 175, 350, 525), values computed in JS from `pw = 98.7`, `q = 3.5`:** bar 1 = `pw` (98.7), fill `rgba(42,120,214,0.35)` / 2px `#2a78d6`; bar 2 = `pw + q` (102.2), fill `rgba(0,131,0,0.40)` / 2px `#008300`; bar 3 = `Math.min(pw, q)` (3.5), fill `rgba(231,76,60,0.35)` / 2px `#e74c3c`. Heights `value / 110 * 180`.
- **Value labels (bold 13px in each bar's border color, centered above the bar top):** "98.7", "102.2", "3.5" — printed from the computed numbers with one decimal.
- **X labels (12px `#444`, two lines below the baseline at y=266 and y=282):** "password only" / "(one door)"; "password AND question" / "(both required)"; "password OR question" / "(either one opens)".
- **Annotation (bold 13px `#e74c3c`, centered at x=525, two lines at y=120 and y=138):** "OR = min(98.7, 3.5)" / "a 3.5-bit account" — both numbers printed from the computed bits.
- **Caption (12px `#444`, bottom right):** "bits from the two doors above".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`, violet `rgba(74,58,167,0.12)`/`#4a3aa7`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` used only for the OR/alarm bar.
- **Data and arithmetic (all hardcoded literals, no `Math.random()`):**
  - Answer distribution: `[90,70,55,45,35,30,25,20,17,13,12,11,10,9,9,8,8,7,7,6]` out of a 1,000-person population; the first 20 answers cover 487 people, leaving 513 people across 470 further answers. Labeled illustrative.
  - Top-10 coverage 400/1,000 = 40.0% and top-3 coverage 215/1,000 = 21.5% are summed from that array in JS at render time, not written as string constants.
  - Password space: 72^16 = 521,578,814,501,447,328,359,509,917,696 ≈ 5.2 × 10^29; log2 = 16 × log2(72) ≈ 98.7 bits. Guesses for a 40% chance = 0.4 × 72^16 ≈ 2.1 × 10^29 (log10 ≈ 29.32).
  - Question min-entropy: most common answer 90/1,000 = 0.09, so −log2(0.09) ≈ 3.47, quoted as 3.5 bits.
  - AND = 98.7 + 3.5 = 102.2 bits; OR = min(98.7, 3.5) = 3.5 bits — both computed in JS via `pw + q` and `Math.min(pw, q)`.
  - Every figure in the prose matches the chart to the digit.
- **Axis titles:** c2 and c4 carry a rotated 12px `#444` y-axis title ("people", "bits") drawn with `ctx.save`/`translate`/`rotate(-π/2)`/`restore`; c3 carries a 12px x-axis caption "guesses (log scale)".
- **Naming and safety:** fictional Alice (account owner) and Bob (attacker); no real companies, no real biographies. No credential strings — the password is described only as "16 characters from a 72-symbol set".
- **Framing:** defensive/educational — the page explains why recovery paths are the weak link so readers store random answers or use recovery codes; no operational attack guidance.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
