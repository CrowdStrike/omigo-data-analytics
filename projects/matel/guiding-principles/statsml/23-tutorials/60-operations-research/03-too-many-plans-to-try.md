# Too Many Plans to Try

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Too Many Plans to Try

**Subtitle:** Real schedules have more possible plans than the universe has time for — computers don't try them all, they rule out whole families at once

## Counting Nurse Schedules

**Tags:** `core idea` (blue), `combinatorial explosion` (orange)

- **Three nurses** — 3 nurses to 3 shifts: 6 possible orderings — you can list them on a napkin
- **Ten nurses** — 10 nurses to 10 shifts: 3,628,800 orderings — no napkin survives that
- **Twenty stops** — a driver's 20-stop route: about 2.4 quintillion possible orderings
- **The clock** — at 1 billion checks per second, trying all 20-stop routes takes about 77 years
- **The pattern** — each added nurse or stop multiplies the count; growth is factorial, not gradual

*Example (italic):* Going from 3 nurses to 10 didn't add a few options — it multiplied the plan count more than 600,000-fold.

**Key point:** Plan counts grow factorially — brute force dies around a dozen items, and real rosters and routes have hundreds.

### Visualization (canvas `c1`, 720×300)

A log-scale bar chart of n vs n! for n = 3, 5, 10, 15, 20, with the 77-years annotation on the tallest bar.

- **Title (bold 15px, `#1a5276`, top center):** "How Many Orderings? n! Explodes".
- **Axes:** 1px `#999`, y from (70, 62) to (70, 226), x from (70, 226) to (690, 226); y-axis caption "plan count (log scale)" 11px mute at (70, 52) left-aligned.
- **Bars:** x = 100/220/340/460/580, width 84; heights proportional to log10(n!) scaled over 160px with max 19: values `[0.78, 2.08, 6.56, 12.12, 18.39]`; fill `rgba(42,120,214,0.5)`, 1px blue `#2a78d6` stroke.
- **Count labels (bold 13px blue, centered above each bar):** "6", "120", "3.6 million", "1.3 trillion", "2.4 quintillion".
- **x labels (12px `#444`, y=242 and y=257):** "3 nurses" / "5 nurses" / "10 nurses" / "15 items" / "20 stops".
- **Annotation (bold 13px magenta `#d55181`, right-aligned at (688, 92), two lines 18px apart):** "checking all 20-stop routes:" / "~77 years at 1 billion/sec".
- **Caption (12px `#6b7280`, centered at y=284):** "each bar is n! — the count of orderings of n items (log scale)".

## The Obvious Plan Loses

**Tags:** `worked example` (blue), `greedy` (orange)

- **The shortcut** — "always drive to the nearest next stop": the greedy plan feels unbeatable
- **The table** — miles: depot–A 1, depot–B 2, depot–C 3, A–B 3, A–C 2, B–C 5
- **Greedy route** — depot→A (1), then A→C (2), then C→B (5): total 8 miles
- **Smarter route** — depot→B (2), then B→A (3), then A→C (2): total 7 miles
- **The catch** — greedy grabbed the cheap first mile and paid for it with the 5-mile last leg

*Example (italic):* Check it from the table yourself: 1 + 2 + 5 = 8 miles for greedy, 2 + 3 + 2 = 7 for the smarter order.

**Key point:** The locally obvious move can lose globally — "just pick the best next step" is not how good schedules and routes get made.

### Visualization (canvas `c2`, 720×300)

Two route maps side by side on the same four points (B, depot, A, C laid out on a line), greedy on the left, best on the right, distances on every leg and totals annotated.

- **Title (bold 15px, `#1a5276`, top center):** "Greedy Route vs Best Route (miles)".
- **Divider:** 1px `#e5e9ef` vertical line at x=360 from y=44 to y=252.
- **Point layout (both panels, baseline y=160):** positions mapped at 38px per mile from mile marks B=−2, depot=0, A=1, C=3. Left panel x = 136 + 38·mile → B 60, depot 136, A 174, C 250. Right panel adds 360 → B 420, depot 496, A 534, C 610.
- **Points:** depot as a 7px ink `#1a5276` square, stops A/B/C as 6px filled circles (`#2c3e50`); bold 12px labels under each point at y=182: "B", "depot", "A", "C".
- **Panel headers (bold 13px, centered at y=58):** left "greedy: nearest next stop" in orange `#d95926`; right "best order" in green `#008300`.
- **Legs as quadratic arcs with mid-arc mile labels (bold 12px, matching panel color):**
  - Left (orange, 2px): depot→A arc above (control y=128), label "1"; A→C arc above (control y=118), label "2"; C→B long arc below (control y=232), label "5". Small "1st/2nd/3rd" 11px mute labels near each arc.
  - Right (green, 2px): depot→B arc above (control y=122), label "2"; B→A arc below (control y=210), label "3"; A→C arc above (control y=120), label "2".
- **Totals (bold 13px, centered at y=270):** left at x=180 in orange: "greedy total: 1+2+5 = 8 miles"; right at x=540 in green: "best total: 2+3+2 = 7 miles".
- **Insight (bold 13px magenta `#d55181`, centered at y=292):** "the cheap first mile cost three extra miles later".

## Ruling Out Families at Once

**Tags:** `core idea` (blue), `pruning` (green)

- **Not faster leaves** — solvers don't race through plans one by one; they skip huge groups whole
- **A proof, not a try** — "every plan with Nurse A on nights already costs more than our best so far"
- **One cut** — with 10 nurses, that single proof discards all 362,880 plans under that branch
- **Branch and bound** — grow plans as a tree, bound each branch's best possible cost, cut losers
- **Why it scales** — every cut removes a family, so the remaining search shrinks multiplicatively

*Example (italic):* One check — "plans under this branch need at least 12 overtime hours; our best uses 9" — erases the whole branch unexamined.

**Key point:** Solvers win by proving whole families of plans can't win — they prune branches instead of checking leaves.

### Visualization (canvas `c3`, 720×300)

A small decision tree: the root choice for Nurse A, one branch kept and expanded, one branch crossed out with the skip-count annotation.

- **Title (bold 15px, `#1a5276`, top center):** "One Proof Cuts a Whole Branch (illustrative)".
- **Root box:** x=270 y=48 w=180 h=40, fill `#fbfcfd`, 2px ink `#1a5276` border, bold 12px ink centered text "assign Nurse A first".
- **Left branch (kept):** 2px green `#008300` line from (330, 88) to (210, 128); box x=120 y=128 w=180 h=38, 2px green border, fill `#fbfcfd`, bold 12px green text "A on day shift ✓". Three 1.5px mute lines fanning down from (210, 166) to (140, 206) / (210, 206) / (280, 206), each ending in a small 24×24 `#fbfcfd` box with 1px `#b8c4cf` border and 11px mute "…" — the search continues here; caption 11px mute centered at (210, 250): "keep exploring this side".
- **Right branch (cut):** 2px grey `#b0b8c1` line from (390, 88) to (510, 128); box x=420 y=128 w=180 h=38, 2px grey `#b0b8c1` border, fill `#f4f6f8`, bold 12px `#6b7280` text "A on night shift"; bold 16px red `#e74c3c` "✕" centered on the branch line at (450, 104) — final position on the box: "✕" at (510, 122) just above the box.
- **Skip annotation (bold 13px red `#e74c3c`, centered at x=510, y=192 and y=210, two lines):** "362,880 plans skipped" / "in one step (illustrative)"; sub-line 11px mute at y=228: "9! orderings of the remaining nurses".
- **Insight (bold 13px magenta `#d55181`, centered at y=282):** "prune the branch, never visit the leaves".

## The Buzzwords You'll Hear

**Tags:** `where it's used` (blue), `vocabulary` (orange)

- **Linear programming** — objective and rules are all straight-line arithmetic: sums and multiples
- **Integer programming** — decisions come in whole units: you can't open 2.4 checkout lanes
- **Solver** — the off-the-shelf search engine: you state the problem, it prunes and searches
- **Your job** — state the objective and constraints well; don't brute-force, don't build the engine
- **Recognition only** — enough to read a paper or a vendor pitch without flinching; no math needed

*Example (italic):* "We modeled the roster as an integer program and handed it to a solver" now reads as a plain sentence.

**Key point:** You don't try all the plans and you don't write the search — you state the problem well and let a solver prune.

### Visualization (canvas `c4`, 720×300)

A three-box pipeline (your sentence → solver → best plan) with three one-line vocabulary rows beneath it.

- **Title (bold 15px, `#1a5276`, top center):** "You State It, the Solver Searches".
- **Pipeline boxes (y=54, h=56):** "YOUR SENTENCE" at x=44 w=190, 2px blue `#2a78d6` border, sub-line 12px `#2c3e50` "objective + constraints"; "SOLVER" at x=280 w=160, 2px violet `#4a3aa7` border, sub-line "off-the-shelf engine"; "BEST PLAN" at x=486 w=190, 2px green `#008300` border, sub-line "optimal roster or route". All fills `#fbfcfd`, bold 13px colored headers at box top+24, sub-lines at top+44.
- **Arrows:** 2px mute `#6b7280` from (238, 82) to (274, 82) and from (444, 82) to (480, 82), filled arrowheads.
- **Vocabulary rows (term bold 13px left-aligned at x=60, definition 12px `#2c3e50` left-aligned at x=250; y = 156 / 192 / 228):**
  - "linear programming" in blue `#2a78d6` — "objective and rules are straight-line arithmetic"
  - "integer programming" in orange `#d95926` — "decisions are whole yes/no units, like lanes or nurses"
  - "solver" in violet `#4a3aa7` — "the engine that does the pruning — you never write it"
- **Row separators:** 1px `#e5e9ef` horizontal lines at y=168 and y=204 from x=60 to x=680 — final layout: separators at y = 170 and 206.
- **Insight (bold 13px magenta `#d55181`, centered at y=280):** "the skill is stating the problem — the searching is a solved problem".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) (and the CSS-width ratio) and calls `ctx.scale(...)`; charts stored in an array, redrawn on debounced window resize.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` only for the pruned-branch cross and skip annotation. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data integrity (all hardcoded, verified):** factorials 3!=6, 5!=120, 10!=3,628,800, 15!≈1.3 trillion, 20!≈2.4 quintillion (2.43×10^18 / 10^9 per sec ≈ 77 years); log10 heights `[0.78, 2.08, 6.56, 12.12, 18.39]`. Distance table (miles, collinear points B=−2, depot=0, A=1, C=3): depot–A 1, depot–B 2, depot–C 3, A–B 3, A–C 2, B–C 5; greedy depot→A→C→B = 1+2+5 = 8; best depot→B→A→C = 2+3+2 = 7. Pruning count 9! = 362,880 (10 nurses, Nurse A's slot fixed). Invented setups carry "(illustrative)".
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
