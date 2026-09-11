# Puzzles & Interview Classics

**Page type:** grid page (tutorials category grid: single flat 4-column nav-grid of cards with topic tags)
**HTML title tag:** Puzzles & Interview Classics

**Subtitle:** Classic brain teasers and coding-interview favorites — each one a small story whose punchline is a reusable problem-solving idea.

## Cards

Each card links to a topic page under `puzzles-interviews/`. The card shows a colored uppercase subcategory label (`.card-num`), a numbered title, a one-line description, and 2-4 topic tag pills. All cards sit in one flat `.nav-grid`; the colored labels carry the grouping.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | BRAIN TEASERS | The Bridge and Torch Puzzle | [28-puzzles-and-interview-classics/01-the-bridge-and-torch-puzzle.md](28-puzzles-and-interview-classics/01-the-bridge-and-torch-puzzle.md) | Four hikers, one torch, a bridge that holds two — the winning plan sends the two slowest across together so one slow walk comes free. | greedy fails, pairing strategy, optimization |
| 2 | BRAIN TEASERS | 100 Prisoners and a Light Bulb | [28-puzzles-and-interview-classics/02-100-prisoners-and-a-light-bulb.md](28-puzzles-and-interview-classics/02-100-prisoners-and-a-light-bulb.md) | A hundred people who can never talk must all confirm they've visited one room — and their only channel is a single on/off light bulb. | one-bit channel, protocol design, designated counter |
| 3 | BRAIN TEASERS | Weighing Puzzles | [28-puzzles-and-interview-classics/03-weighing-puzzles.md](28-puzzles-and-interview-classics/03-weighing-puzzles.md) | A balance scale answers a three-way question, so three weighings can tell 27 stories apart — enough to find one bad jar among 12. | three-way answers, information, balance scale |
| 4 | BRAIN TEASERS | The Mutilated Chessboard | [28-puzzles-and-interview-classics/04-the-mutilated-chessboard.md](28-puzzles-and-interview-classics/04-the-mutilated-chessboard.md) | Sixty-two tiles and thirty-one two-tile mats look like a perfect match — a coloring argument proves the fit impossible before you try a single layout. | invariant, parity, impossibility proof |
| 5 | BRAIN TEASERS | The Josephus Problem | [28-puzzles-and-interview-classics/05-the-josephus-problem.md](28-puzzles-and-interview-classics/05-the-josephus-problem.md) | Kids in a circle, every second one tapped out — and the winning seat pops out of one binary move on the head count. | circular elimination, binary trick, recurrence |
| 6 | BRAIN TEASERS | The Two-Egg Problem | [28-puzzles-and-interview-classics/06-the-two-egg-problem.md](28-puzzles-and-interview-classics/06-the-two-egg-problem.md) | With two phones and a hundred floors, shrinking jumps starting at floor 14 cap every possible outcome at 14 drops. | worst case, shrinking steps, search strategy |
| 7 | CODING PATTERNS | Two Sum & the Hash-Map Trade | [28-puzzles-and-interview-classics/07-two-sum-and-the-hash-map-trade.md](28-puzzles-and-interview-classics/07-two-sum-and-the-hash-map-trade.md) | Remember what you've already seen and ask for the missing half — spending a little memory to make each question instant. | hash map, one pass, space vs time |
| 8 | CODING PATTERNS | Climbing Stairs | [28-puzzles-and-interview-classics/08-climbing-stairs.md](28-puzzles-and-interview-classics/08-climbing-stairs.md) | Count the ways up a staircase in moves of one step or two — the counts are Fibonacci in disguise, and spotting the recurrence is the whole trick. | recurrence, Fibonacci, counting paths |
| 9 | CODING PATTERNS | Floyd's Cycle Detection | [28-puzzles-and-interview-classics/09-floyds-cycle-detection.md](28-puzzles-and-interview-classics/09-floyds-cycle-detection.md) | Send a slow walker and a fast runner down the same path — if it ever loops, the runner must catch the walker, and that meeting proves the loop. | fast & slow pointers, cycle detection, constant memory |
| 10 | CODING PATTERNS | Merge Intervals | [28-puzzles-and-interview-classics/10-merge-intervals.md](28-puzzles-and-interview-classics/10-merge-intervals.md) | Sort the bookings by start time and sweep through once, gluing every booking that overlaps the block you're building. | sort then sweep, overlaps, calendar blocks |
| 11 | CODING PATTERNS | Valid Parentheses | [28-puzzles-and-interview-classics/11-valid-parentheses.md](28-puzzles-and-interview-classics/11-valid-parentheses.md) | Brackets nest like boxes inside boxes — you must close the one opened most recently, and the structure that remembers "most recently" is a stack. | stack, last in first out, matching pairs |
| 12 | CODING PATTERNS | Top-K Elements | [28-puzzles-and-interview-classics/12-top-k-elements.md](28-puzzles-and-interview-classics/12-top-k-elements.md) | Name the 3 best sellers out of thousands without sorting the whole list — keep a tiny shortlist that its weakest member guards. | heap, shortlist, quickselect |
| 13 | DP & GRAPHS | Coin Change & Knapsack | [28-puzzles-and-interview-classics/13-coin-change-and-knapsack.md](28-puzzles-and-interview-classics/13-coin-change-and-knapsack.md) | Grabbing the biggest piece first feels obviously right and can quietly cost you — building the best answer up from smaller amounts never does. | greedy fails, dynamic programming, build up |
| 14 | DP & GRAPHS | Longest Common Subsequence | [28-puzzles-and-interview-classics/14-longest-common-subsequence.md](28-puzzles-and-interview-classics/14-longest-common-subsequence.md) | Compare two versions of a list and ask what survived in the same order — a small grid of numbers finds that longest shared thread, and every diff tool is built on it. | grid of scores, diff tools, shared thread |
| 15 | DP & GRAPHS | Island Counting | [28-puzzles-and-interview-classics/15-island-counting.md](28-puzzles-and-interview-classics/15-island-counting.md) | Walk a grid map square by square and, each time you step onto unpainted land, paint that whole island before moving on — that move is flood fill. | flood fill, grid traversal, connected components |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorials category grid. Single page: h1, `.subtitle` paragraph, then one flat `.nav-grid` of `.nav-card` anchors (no h2 section headings).
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, 15px top margin; responsive: 3 columns below 1400px, 2 below 1100px, 1 below 600px.
- **Links:** the tables above link to the `.md` versions for markdown navigation; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">SUBCATEGORY LABEL</div>`, `<h3>N. Topic Title</h3>` (unpadded index number matching the 2-digit zero-padded file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` pills.
- **Category label colors:** applied by a small script mapping `.card-num` text to color — "BRAIN TEASERS" `#2980b9`, "CODING PATTERNS" `#27ae60`, "DP & GRAPHS" `#8e44ad`; the CSS default for `.card-num` is `#2980b9`, 0.75em bold, 4px bottom margin.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`, transition on border-color/transform; hover: border `#2980b9`, `translateY(-2px)`. h3 `#1a3a4a` 1em with 6px bottom margin; description `#555` 0.85em. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em `#666`, in a flex-wrap row with 4px gap and 8px top margin.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; universal `* { margin:0; padding:0; box-sizing:border-box }` reset; h1 1.8em `#2980b9` with 10px bottom margin; subtitle `#666` 1.05em with 30px bottom margin. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22` (page accents here use `#2980b9`, `#27ae60`, `#8e44ad`).
- **Canvases:** none on this page; any canvases elsewhere in this series use `window.devicePixelRatio` scaling.
