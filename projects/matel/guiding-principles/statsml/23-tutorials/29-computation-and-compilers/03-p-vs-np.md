# P vs NP

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** P vs NP

**Subtitle:** Some questions are quick to check but, as far as anyone knows, brutally slow to solve — P vs NP asks whether that gap between checking and solving is real

## A Guest List That Is Easy to Grade

**Tags:** `core idea` (blue), `check vs solve` (green), `SAT` (orange)

- **The party** — you are planning a dinner and every friend is a yes/no decision: invite or don't
- **The rules** — friends come with constraints: "Amy and Cara had a fight", "someone must bring the cake"
- **Checking is fast** — hand me any proposed guest list and I grade it in seconds: one look per rule
- **Solving is slow** — finding a list that passes every rule may mean trying the whole pile of lists
- **The pile explodes** — 3 friends give 8 possible lists, 10 give 1,024, 20 give 1,048,576, 30 give 1,073,741,824
- **The name** — this invite puzzle is SAT (boolean satisfiability), the poster child of P vs NP

*Example (italic):* With 30 friends a proposed list is graded in about 45 quick looks, but there are roughly a billion lists to search through if you have to find one yourself.

**Key point:** P vs NP asks: whenever an answer is quick to check, is it also quick to find? Nobody has proved it either way.

### Visualization (canvas `c1`, 720×300)

Grouped bar chart on a log scale: for 3, 10, 20, 30 friends, a short bar for "steps to check one list" next to a tall bar for "lists to search", showing the checking cost staying flat while the search pile explodes.

- **Title (bold 15px, `#1a5276`, top center):** "Grading One List Stays Cheap — the Pile of Lists Explodes".
- **Axes:** baseline y=245, plot from x=90 to x=690; x axis = four groups centered at x = `[165, 315, 465, 615]` with 12px `#444` labels below: "3 friends", "10 friends", "20 friends", "30 friends"; y axis is log10 steps, 20px of height per log10 unit, light `#e5e9ef` gridlines at log10 = 3, 6, 9 with 11px `#6b7280` labels "1 thousand", "1 million", "1 billion" at x=85 right-aligned.
- **"Check one list" bars (blue `#2a78d6`, 40px wide, left of each group center):** step counts `[4, 15, 30, 45]`, log10 = `[0.60, 1.18, 1.48, 1.65]`, so pixel heights `[12, 24, 30, 33]`; 12px blue label with the literal count ("4", "15", "30", "45") above each bar.
- **"Lists to search" bars (orange `#d95926`, 40px wide, right of each group center):** list counts `[8, 1024, 1048576, 1073741824]`, log10 = `[0.90, 3.01, 6.02, 9.03]`, so pixel heights `[18, 60, 120, 181]`; 12px orange labels above: "8", "1,024", "~1 million", "~1 billion".
- **Legend (12px, top left at x=95, y=60):** blue swatch "steps to check one list", orange swatch "lists to search".
- **Annotation (bold 13px orange `#d95926`, near x=395, y=86):** two lines: "checking grows gently —" / "the search pile doubles per friend".
- **Caption (12px `#444`, bottom right):** "step counts illustrative; list counts exact (2 per friend, multiplied out)".

## Three Friends, Four Rules, Eight Tries

**Tags:** `worked example` (blue), `brute force` (green)

- **The friends** — Amy, Ben, Cara: three yes/no choices, so 2 × 2 × 2 = 8 possible guest lists
- **Rule 1, cake** — Amy or Ben must come, because only they can bake the cake
- **Rule 2, feud** — Amy and Cara had a fight, so not both of them on the list
- **Rule 3, driver** — Ben or Cara must come, because someone has to drive
- **Rule 4, shy Ben** — Ben only shows up if Amy is coming too
- **Brute force** — grade all 8 lists against all 4 rules; exactly one list passes everything

*Example (italic):* The list "Amy yes, Ben yes, Cara no" passes all four rules — cake covered by Amy, no feud since Cara stays home, Ben drives, and Ben has Amy there.

**Key point:** Each list takes only 4 quick rule-checks to grade, yet finding the winner took walking all 8 lists — check fast, solve slow, in miniature.

### Visualization (canvas `c2`, 720×300)

Pass/fail grid: 8 rows (one per guest list) by 4 rule columns plus a verdict column, with green checks and red crosses, and the single all-green row highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "All 8 Guest Lists, Graded Against All 4 Rules".
- **Header row (bold 12px `#1a5276`, y=68):** column labels "cake", "feud", "driver", "shy Ben", "verdict" centered at x = `[300, 390, 480, 570, 665]`.
- **Row labels (12px `#444`, left-aligned at x=20, rows at y = 90, 113, 136, 159, 182, 205, 228, 251):** "Amy no · Ben no · Cara no", "Amy no · Ben no · Cara yes", "Amy no · Ben yes · Cara no", "Amy no · Ben yes · Cara yes", "Amy yes · Ben no · Cara no", "Amy yes · Ben no · Cara yes", "Amy yes · Ben yes · Cara no", "Amy yes · Ben yes · Cara yes".
- **Marks (14px, centered in each cell):** green `#008300` check or red `#e74c3c` cross per row, hardcoded as `[["x","v","x","v","x"], ["x","v","v","v","x"], ["v","v","v","x","x"], ["v","v","v","x","x"], ["v","v","x","v","x"], ["v","x","v","v","x"], ["v","v","v","v","v"], ["v","x","v","v","x"]]` (v = check, x = cross; last entry is the verdict).
- **Highlight:** row 7 ("Amy yes · Ben yes · Cara no") gets a full-width `rgba(0,131,0,0.10)` background band and its verdict check drawn bold.
- **Annotation (bold 13px green `#008300`, right side near x=560, y=285):** "only 1 of the 8 lists passes".
- **Caption:** none needed — the grid is the exact worked example, no invented numbers.

## Why a Million Dollars Rides on This

**Tags:** `where it's used` (blue), `NP-complete` (orange), `open problem` (green)

- **P** — the problems solvable fast (in polynomial time): sorting a list, finding a shortest route on a map
- **NP** — the problems checkable fast: a proposed answer, like a guest list, can be graded quickly
- **SAT is the keystone** — every NP problem can be dressed up as a SAT puzzle (the Cook–Levin theorem)
- **One domino** — a fast SAT solver would make scheduling, chip layout, and puzzle-solving all fast at once
- **Crypto stakes** — much of modern encryption leans on "solving is slow"; P = NP would shake that ground
- **The bounty** — the Clay Mathematics Institute offers $1,000,000 for a proof either way; open since 1971

*Example (italic):* A 60-friend party has about 1.15 quintillion guest lists; at a billion lists per second, brute force needs roughly 36 years — yet grading any one list still takes under a minute.

**Key point:** SAT is NP-complete: crack it fast and every quick-to-check problem becomes quick to solve — that is why one puzzle carries a million-dollar prize.

### Visualization (canvas `c3`, 720×300)

Two growth curves on a shared axis: gentle n² (a fast, in-P algorithm) versus 2^n (brute-force search), with the 2^n curve blowing through the top of the chart while n² stays low.

- **Title (bold 15px, `#1a5276`, top center):** "Fast Growth vs Explosive Growth: n² versus 2^n".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 185; x axis = problem size n from 0 to 20, 12px `#444` tick labels at 0, 5, 10, 15, 20; y axis = steps 0 to 1,000, light `#e5e9ef` gridlines at 250, 500, 750, 1,000 with 11px `#6b7280` labels.
- **n² curve (blue `#2a78d6`, 3px):** points at n = `[0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20]`, steps = `[0, 4, 16, 36, 64, 100, 144, 196, 256, 324, 400]`; 12px blue label "n² — stays on the chart" near its right end (n≈17, below the curve).
- **2^n curve (orange `#d95926`, 3px):** points at n = `[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]`, steps = `[1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024]`; clip the stroke at the top of the plot; 12px orange label "2^n" just left of where it exits.
- **Exit marker:** vertical dashed `#6b7280` (dash 4/3) line at n=10 from the top of the plot to the baseline, 11px `#6b7280` label "n = 10" at its foot.
- **Annotation (bold 13px orange `#d95926`, near x=330, y=80):** two lines: "2^n leaves the chart at n = 10 —" / "n² has only reached 100".
- **Caption (12px `#444`, bottom right):** "step counts are the exact formulas n² and 2^n, no scaling".

## NP Does Not Mean "Not Polynomial"

**Tags:** `common mistake` (red), `naming trap` (orange)

- **The misreading** — NP is short for "nondeterministic polynomial", not "not polynomial"
- **What NP really says** — a proposed answer can be checked fast; it says nothing about finding one
- **P sits inside NP** — every quick-to-solve problem is also quick to check, so P is a subset of NP
- **Unsolved, not settled** — no one has proved P ≠ NP; most experts believe it, but belief is not proof
- **Hard is not hopeless** — real SAT solvers crack many huge industrial instances; only the worst case bites

*Example (italic):* Sorting is in NP too — a sorted list is easy to check — it just also happens to be in P, which is exactly why "NP = hard" is a misreading.

**Common mistake:** Reading "problem X is in NP" as "X is intractable". NP is the easy-to-check club, P is the easy-to-solve club inside it, and the million-dollar question is whether the two clubs are secretly the same.

### Visualization (canvas `c4`, 720×300)

Two side-by-side diagrams of the possible worlds: on the left the believed picture (P a small circle inside the NP oval, SAT sitting on the NP-complete rim), on the right the shocking alternative (one merged shape where P = NP).

- **Title (bold 15px, `#1a5276`, top center):** "The Two Possible Worlds — Nobody Has Ruled Either Out".
- **Left panel (centered at x=200):** oval "NP" centered (200, 165), radii 130×85, 2px `#1a5276` stroke, fill `rgba(42,120,214,0.08)`; inner circle "P" centered (160, 185), radius 42, 2px blue `#2a78d6` stroke, fill `rgba(42,120,214,0.18)`, bold 13px blue label "P" inside; a 7px orange `#d95926` dot at (295, 130) on the oval's rim labeled bold 12px orange "SAT (NP-complete)"; bold 13px `#1a5276` label "NP" at the oval's top inside edge; 12px `#444` panel caption below at y=272: "if P ≠ NP (what most experts believe)".
- **Right panel (centered at x=540):** single circle centered (540, 165), radius 85, 2px green `#008300` stroke, fill `rgba(0,131,0,0.10)`, bold 13px green two-line label inside: "P = NP" / "checking fast ⇒ solving fast"; 12px `#444` panel caption below at y=272: "if P = NP (the million-dollar upset)".
- **Divider:** vertical 1px `#e5e9ef` line at x=370 from y=55 to y=255.
- **Annotation (bold 13px violet `#4a3aa7`, centered at x=370, y=45):** "one proof either way settles it — none exists yet".
- **Caption (12px `#444`, bottom right):** "region sizes illustrative — set membership is the point, not area".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar heights, curve points, and grid marks are the hardcoded arrays above (no randomness); list counts are exact powers of two, curve values are exact n² and 2^n, and the 8×5 pass/fail grid is the true truth table of the four stated rules; only the check-step counts are illustrative and are captioned as such.
- **Data integrity check:** the numbers in the text (8, 1,024, 1,048,576, 1,073,741,824 lists; 4 rules; 1 passing list of 8; 2^n exiting at n=10 while n² sits at 100) must match the chart arrays exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
