# 100 Prisoners and a Light Bulb

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** 100 Prisoners and a Light Bulb

**Subtitle:** A hundred people who can never talk must all confirm they have each visited one room — and their only channel is a single light bulb that is either on or off

## One Bulb, One Hundred Strangers

**Tags:** `core idea` (blue), `the puzzle` (green), `one shared bit` (orange)

- **The setup** — 100 prisoners; each day the warden takes one of them, chosen at random, into a room
- **The room** — it holds nothing but a light bulb, starting off; the visitor may flip it or leave it
- **The win** — any prisoner may one day declare "all 100 have visited"; right frees everyone, wrong is fatal
- **The catch** — no notes, no talking after day zero; the bulb's on/off state is the only shared memory
- **The plan** — before day one they agree: one prisoner is the counter, the other 99 are flippers
- **The rule** — a flipper turns the bulb on once in their life; the counter turns it off and adds one

*Example (italic):* A flipper who walks in, sees the bulb off, and has never flipped before turns it on — that "on" is a message saying "one more new person has been here".

**Key point:** The bulb carries exactly one bit — "a fresh visitor's token is waiting" — and the counter collects those tokens until the count hits 99.

### Visualization (canvas `c1`, 720×300)

Left-to-right protocol schematic: a cluster of flippers, the bulb in the middle as the single shared bit, and the counter with a tally on the right, connected by two labeled arrows.

- **Title (bold 15px, `#1a5276`, top center):** "The Counter Protocol: 99 Tokens Through One Bulb".
- **Flippers (left):** 3×3 grid of 9 filled blue `#2a78d6` circles (radius 11) centered near x=115, y=155, spaced 46px; 12px `#444` label below the grid at y=245: "99 flippers — each holds one token"; a small white 11px bold "T" inside each circle.
- **Bulb (middle):** circle radius 30 at (360, 150), fill yellow `#c98500` with 3px `#1a5276` outline, two short 2px `#c98500` rays at 45° angles; bold 13px `#1a5276` label below at y=210: "the bulb = one shared bit"; 12px `#6b7280` line under it: "on / off is all anyone can say".
- **Counter (right):** filled green `#008300` circle radius 16 at (600, 150); above it four 2px green tally strokes near (600, 100) with 12px green label "count so far"; 12px `#444` label below at y=195: "the counter".
- **Arrow 1:** 3px blue `#2a78d6` arrow from the flipper grid (x≈190) to the bulb (x≈325) at y=150 with arrowhead; 12px blue label above it: "turns it ON (once ever)".
- **Arrow 2:** 3px green `#008300` arrow from the bulb (x≈395) to the counter (x≈580) at y=150 with arrowhead; 12px green label above it: "turns it OFF, adds 1".
- **Annotation (bold 13px orange `#d95926`, centered near x=360, y=265):** "when the count reaches 99, the counter declares — with certainty".
- **Caption (11px `#444`, bottom right):** "schematic — 9 dots stand in for the 99 flippers".

## Five Prisoners, Fourteen Days

**Tags:** `worked example` (blue), `step by step` (green)

- **Shrink it** — same game with 5 prisoners: A is the counter, B, C, D, E are flippers; declare at count 4
- **The visits** — the warden's random draw runs B, C, A, A, D, B, A, E, C, A, D, C, B, A over 14 days
- **First token** — day 1: B finds the bulb off and turns it on; day 3: A turns it off, count = 1
- **Repeats do nothing** — day 6: B returns, but B already flipped once, so the bulb stays untouched
- **Blocked token** — day 9: C wants to flip but the bulb is already on, so C waits for a later off day
- **The finish** — A hits count 2 on day 7, count 3 on day 10, count 4 on day 14 — and declares

*Example (italic):* By day 14 the counter A has personally switched the bulb off four times, so A knows B, C, D, E have all visited — and A has obviously visited too.

**Key point:** The counter never guesses: each of the 4 offs it performed was one distinct flipper's once-in-a-lifetime token, so count 4 proves all 5 have been in the room.

### Visualization (canvas `c2`, 720×300)

Two-band timeline over 14 days: the top band shows the bulb's state after each day's visit as colored blocks with the visitor's letter, the bottom shows the counter's tally as a step line climbing 0 to 4.

- **Title (bold 15px, `#1a5276`, top center):** "One Run With 5 Prisoners: the Tally Reaches 4 on Day 14".
- **Layout:** plot from x=70 to x=690 (14 slots, 44px each); day tick labels "1"–"14" (12px `#444`) along the bottom axis at y=262.
- **Visitor row (y=68):** 12px bold `#2c3e50` letters per day: `["B","C","A","A","D","B","A","E","C","A","D","C","B","A"]`; the four counter days that decrement (3, 7, 10, 14) get green `#008300` letters, flip days (1, 5, 8, 12) blue `#2a78d6`, all others `#6b7280`.
- **Bulb band (y=90 to y=130):** one 40px-wide rounded block per day; bulb state after each visit = `["on","on","off","off","on","on","off","on","on","off","off","on","on","off"]`; "on" blocks fill yellow `rgba(201,133,0,0.55)` with 11px `#7a5200` label "on", "off" blocks fill `#e5e9ef` with 11px `#6b7280` label "off"; 12px `#444` row label "bulb" at x=20, y=112.
- **Tally step line (baseline y=250, top y=160):** green `#008300` 3px step line over the day slots with values after each day = `[0, 0, 1, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 4]`; a 6px green dot at each of the four step-ups (days 3, 7, 10, 14) with bold 12px green labels "1", "2", "3", "4" above; 12px `#444` row label "count" at x=20, y=205.
- **Declare marker:** vertical dashed `#d95926` (dash 4/3) line at day 14 from y=60 to y=250; bold 13px orange `#d95926` label to its left: "count = 4 → declare".
- **Annotation (bold 12px blue `#2a78d6`, near day 9, y=150):** "day 9: bulb already on — C's token must wait".
- **Caption (11px `#444`, bottom right):** "one illustrative random schedule; letters = that day's visitor".

## Why One Shared Bit Shows Up Everywhere

**Tags:** `where it's used` (blue), `distributed systems` (green), `cost of certainty` (orange)

- **Real twin** — machines that share one lock, flag, or token instead of a conversation face this puzzle
- **Protocol design** — the lesson is designing rules so tiny shared state still adds up to a guarantee
- **Certainty is slow** — with 100 prisoners the counter plan needs roughly 10,400 days, about 28 years
- **Coupon collector** — on average everyone has visited by about 520 days; certainty costs 20× the wait
- **The trade** — the protocol buys a zero-error guarantee by paying a huge price in expected time

*Example (italic):* A fleet of servers sharing a single status flag can still confirm "everyone finished" — but only through a slow one-at-a-time token scheme like the prisoners'.

**Key point:** One shared bit is enough to reach certainty across 100 uncoordinated actors — the price is time, and that certainty-versus-speed trade is the heart of protocol design.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: expected days for the counter protocol to finish at five group sizes, showing the cost of certainty exploding roughly with the square of the group size.

- **Title (bold 15px, `#1a5276`, top center):** "Certainty Gets Expensive: Expected Days Until the Counter Declares".
- **Axes:** bars start at x=170, max bar width 480 (scale: 10,400 days = 480px); baseline vertical 2px `#999` line at x=170 from y=60 to y=255; x tick labels "0", "2,500", "5,000", "7,500", "10,000" (12px `#444`) at y=272 with light `#e5e9ef` gridlines.
- **Rows (y = 80, 118, 156, 194, 232), 24px-tall bars, left 12px `#444` labels at x=20:** group sizes and expected days = `[["5 prisoners", 27], ["10 prisoners", 105], ["25 prisoners", 650], ["50 prisoners", 2600], ["100 prisoners", 10400]]`; the first four bars fill blue `rgba(42,120,214,0.55)`, the 100-prisoner bar fill orange `rgba(217,89,38,0.65)` with a 2px `#d95926` outline.
- **Value labels:** bold 12px at each bar's right end — blue `#2a78d6` "27", "105", "650", "2,600" and orange `#d95926` "10,400 days ≈ 28 years" on the last bar.
- **Reference marker:** vertical dashed green `#008300` (dash 4/3) line at 520 days (x≈194) from y=60 to y=255; 12px green label at its top: "on average all have visited by ~520 days".
- **Annotation (bold 13px orange `#d95926`, near x=420, y=205):** "guaranteed 'all 100' costs ~20× the likely wait".
- **Caption (11px `#444`, bottom right):** "illustrative approximations — expected days grow roughly with the square of group size".

## Why Not Just Wait Long Enough?

**Tags:** `common mistake` (red), `probability vs proof` (orange)

- **The shortcut** — after enough days it feels safe to declare without any protocol; everyone has "surely" been in
- **The math** — with 100 prisoners the chance all have visited is about 52% by day 500 and 97% by day 800
- **Never 100%** — the curve keeps climbing but never touches certain; some prisoner may still be unpicked
- **The stakes** — one wrong declaration loses everything, so 99.9% sure is still a loaded gamble
- **The lesson** — the counter's tally converts "very likely" into "provably true"; that is the whole point

*Example (italic):* A prisoner who declares on day 800 is wrong about 3 times in 100 — with everyone's freedom on the line, the bulb protocol's slow certainty beats the fast guess.

**Common mistake:** Treating a very high probability as a proof. Waiting shortens the odds but can never close them; only the counted tokens make the declaration safe.

### Visualization (canvas `c4`, 720×300)

Single-panel line chart: the probability that all 100 prisoners have visited versus days waited, climbing steeply but flattening below 100%, contrasted with the protocol's flat certainty line.

- **Title (bold 15px, `#1a5276`, top center):** "Waiting Gets You Close to Sure — Never All the Way".
- **Axes:** origin x=70, baseline y=250, plot width 600, plot height 180; x = days 0 to 1,200 with 12px `#444` tick labels "0", "200", "400", "600", "800", "1,000", "1,200"; y = probability 0% to 100% with 12px `#444` tick labels "0%", "25%", "50%", "75%", "100%" and light `#e5e9ef` gridlines.
- **Guess curve:** magenta `#d55181` 3px line through hardcoded points, days = `[0, 200, 300, 400, 500, 600, 700, 800, 1000, 1200]`, probability (%) = `[0, 0, 1, 16, 52, 79, 91, 97, 99.6, 99.9]`; fill under the curve `rgba(213,81,129,0.12)`; 12px magenta label near x=430 on the curve: "declare by gut feel".
- **Certainty line:** solid green `#008300` 3px horizontal line at exactly 100% across the plot; bold 12px green label above its left end: "counter protocol: 100% when it declares".
- **Gap marker:** vertical dashed `#6b7280` (dash 4/3) line at day 800 from the guess curve (97%) up to the 100% line, with 11px `#6b7280` label beside it: "still a 3% gamble".
- **Annotation (bold 13px magenta `#d55181`, near x=340, y=95):** two lines: "52% sure at day 500, 97% at day 800 —" / "one wrong call and everyone loses".
- **Caption (11px `#444`, bottom right):** "probabilities computed for random daily draws, rounded — illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** every visitor sequence, bulb-state array, tally step, bar value, and probability point is the hardcoded literal array above (no `Math.random()`); the 14-day run in c2 is a valid execution of the counter protocol and its numbers match the section text exactly; c3 bar values and c4 probabilities are approximations and keep their "illustrative" captions.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
