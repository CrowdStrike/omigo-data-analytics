# Monty Hall & Conditional Traps

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Monty Hall & Conditional Traps

**Subtitle:** When someone who knows the answer removes an option for you, the remaining options are no longer equally likely — switching wins the three-cup game 2/3 of the time

## Three Cups and a Gold Coin

**Tags:** `core idea` (blue), `conditional probability` (green), `the setup` (orange)

- **The game** — a barista hides a gold coin under one of three cups; you point at Cup 1
- **The flip** — the barista, who knows where the coin is, flips over Cup 3 to show it's empty
- **The offer** — you may stay with Cup 1 or switch to Cup 2 before the reveal
- **The gut answer** — two cups remain, so almost everyone says it's 50/50 either way
- **The truth** — staying wins 1/3 of the time; switching wins 2/3 — the flip was not random

*Example (italic):* You pick Cup 1, the barista flips empty Cup 3, and switching to Cup 2 doubles your chance of the coin.

**Key point:** The barista's flip carries information because she was never allowed to reveal the coin. An informed removal is not a coin toss — it reshapes the odds.

### Visualization (canvas `c1`, 720×300)

Flow diagram of one round: three cups, your pick, the informed flip, and the stay/switch arrows with their win rates.

- **Title (bold 15px, `#1a5276`, top center):** "One Round of the Three-Cup Game".
- **Cups:** three rounded rectangles 120×70, top edge y=70, centered at x=140, x=360, x=580; 2px `#1a5276` borders, fill `#f8f9fa`; labels "Cup 1", "Cup 2", "Cup 3" bold 13px `#2c3e50` centered inside.
- **Your pick:** blue `#2a78d6` 3px outline drawn 4px outside Cup 1; bold 13px blue label "you pick Cup 1" centered above Cup 1 at y=58.
- **The flip:** Cup 3 fill `#f1f1f1`, two orange `#d95926` 3px diagonal cross lines corner to corner; bold 12px orange two-line label under Cup 3 (y=160): "barista flips it —" / "empty (she knew)".
- **Stay arrow:** blue `#2a78d6` 3px looped arrow from the bottom of Cup 1 back to itself (via y=200); bold 13px blue label "stay: win 1/3" at (140, 225).
- **Switch arrow:** green `#008300` 3px curved arrow from bottom of Cup 1 (140, 145) to bottom of Cup 2 (360, 145) dipping through y=200; bold 13px green label "switch: win 2/3" at (360, 225).
- **Takeaway (bold 13px magenta `#d55181`, centered at y=272):** "two cups left, but NOT 50/50 — the flip was informed".

## Listing Every Possible Game

**Tags:** `worked example` (blue), `enumeration` (green)

- **Only 3 cases** — you always pick Cup 1, so only the coin's location varies: Cup 1, 2, or 3
- **Case 1** — coin under Cup 1: barista flips Cup 2 (or 3); stay WINS, switch loses
- **Case 2** — coin under Cup 2: barista is forced to flip Cup 3; stay loses, switch WINS
- **Case 3** — coin under Cup 3: barista is forced to flip Cup 2; stay loses, switch WINS
- **Tally** — stay wins in 1 of 3 cases; switch wins in 2 of 3 — no formula needed, just count

*Example (italic):* Write the 3 cases on a napkin and count: switch collects the coin in 2 of the 3 equally likely worlds.

**Key point:** In 2 of 3 cases the barista has no choice — her forced flip points straight at the coin. Switching wins exactly when your first pick was wrong, which is 2/3 of the time.

### Visualization (canvas `c2`, 720×300)

Case-enumeration table drawn on canvas: three rows (one per coin location, you always pick Cup 1) with stay/switch outcomes, plus a tally line.

- **Title (bold 15px, `#1a5276`, top center):** "All 3 Equally Likely Games (You Always Pick Cup 1)".
- **Column headers (bold 12px `#6b7280`, y=62):** "coin under" at x=110, "barista flips" at x=290, "STAY on Cup 1" at x=470, "SWITCH" at x=620 (all centered).
- **Rows:** row centers y=100, y=145, y=190; light `#e5e9ef` 1px separator lines between rows spanning x=40 to x=680.
- **Row data (13px `#2c3e50` in the first two columns):** row 1: "Cup 1", "Cup 2 (her choice)", stay "WIN", switch "lose"; row 2: "Cup 2", "Cup 3 (forced!)", stay "lose", switch "WIN"; row 3: "Cup 3", "Cup 2 (forced!)", stay "lose", switch "WIN".
- **Outcome styling:** "WIN" bold 13px green `#008300` on a `rgba(0,131,0,0.12)` rounded pill 56×22; "lose" 13px magenta `#d55181`, no pill; "(forced!)" rendered bold in orange `#d95926` within the flip text.
- **Tally bars (from y=225):** two horizontal bars starting at x=180, 18px tall, scale 3 cases = 420px; "stay wins" bar 140px long, fill `rgba(42,120,214,0.55)`, bold 12px blue label "1 of 3" at its right end; "switch wins" bar 280px long at y=252, fill `rgba(0,131,0,0.4)`, bold 12px green label "2 of 3"; row labels 12px `#444` left of the bars.
- **Takeaway (bold 13px green, centered at y=290):** "switch wins whenever your first pick was wrong — 2 cases out of 3".

## The Host's Knowledge Is the Whole Trick

**Tags:** `where it's used` (blue), `conditional trap` (orange), `failure mode` (red)

- **Change one rule** — suppose the barista flips a cup at random and just got lucky it was empty
- **Now 6 cases** — 3 coin spots × 2 random flips (Cup 2 or Cup 3) are equally likely
- **2 cases die** — in 2 of the 6, her random flip exposes the coin and the round is voided
- **The survivors** — of the 4 empty-flip cases, stay wins 2 and switch wins 2 — genuinely 50/50
- **The trap** — identical table scene, opposite answer; what changed is HOW Cup 3 came to be flipped

*Example (italic):* A filter that "happened to" drop the bad rows is a knowing barista — analysts who treat it as random get the odds wrong.

**Key point:** Conditioning on WHAT you saw is not enough — you must condition on the process that decided to show it to you. This is the same trap behind survivorship bias and peeking at data.

### Visualization (canvas `c3`, 720×300)

Dual-panel comparison: knowing barista (3 cases, left) vs random barista (6 cases with 2 voided, right), each with stay/switch win bars, split by a dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Knowing Barista vs Random Barista (empty cup shown in both)".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.
- **Left panel (knowing):** heading bold 13px `#1a5276` "she KNOWS — never flips the coin" centered at (185, 60); case dots: 3 circles radius 9 at y=95, x=95/185/275, filled `rgba(42,120,214,0.5)` with 12px `#444` labels "1","2","3" inside; bars from y=140, starting x=110, scale 3 cases = 180px: "stay" bar 60px `rgba(42,120,214,0.55)` labeled bold 12px blue "1/3", "switch" bar 120px at y=175 `rgba(0,131,0,0.4)` labeled bold 12px green "2/3"; bar row labels 12px `#444` left-aligned at x=50; caption 12px `#444` at (185, 225) "3 equally likely cases".
- **Right panel (random):** heading bold 13px `#1a5276` "she flips at RANDOM — got lucky" centered at (540, 60); case dots: 6 circles radius 9 at y=95, x=430/475/520/565/610/655, four filled `rgba(0,131,0,0.25)`, two (positions 3 and 5) filled `rgba(217,89,38,0.35)` with orange `#d95926` 2px X drawn over them; bold 11px orange label "2 voided: flip exposed the coin" centered at (540, 122); bars from y=150, starting x=470, scale 4 cases = 160px: "stay" bar 80px `rgba(42,120,214,0.55)` labeled bold 12px blue "2/4", "switch" bar 80px at y=185 `rgba(0,131,0,0.4)` labeled bold 12px green "2/4"; row labels 12px `#444` at x=410; caption 12px `#444` at (540, 230) "4 surviving cases — now truly 50/50".
- **Takeaway (bold 13px magenta `#d55181`, centered at y=272):** "same empty cup on the table — the flipping RULE decides the odds".

## Why 50/50 Feels Right but Isn't

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **The 100-cup version** — same game with 100 cups: you pick Cup 1, the coin hides under one of 100
- **The big flip** — the knowing barista flips 98 empty cups, leaving only your Cup 1 and Cup 62
- **Stay** — your Cup 1 was right with chance 1/100, and no flip of OTHER cups changed that
- **Switch** — the remaining 99/100 chance is now funneled entirely into Cup 62
- **Scale it down** — three cups is the same funnel: 1/3 stays on your cup, 2/3 lands on the other

*Example (italic):* With 100 cups almost nobody says 50/50 — the barista's 98 deliberate flips visibly scream where the coin is.

**Common mistake:** Counting the remaining options (two cups, so 50/50) instead of tracking where the probability flowed. Options left on the table are not automatically equally likely.

### Visualization (canvas `c4`, 720×300)

Strip of 100 cups with 98 flipped, plus stay/switch probability bars showing the 1% vs 99% split.

- **Title (bold 15px, `#1a5276`, top center):** "100 Cups: the Barista Flips 98 Empty Ones".
- **Cup strip:** 100 small ticks (2px wide, 14px tall) at y=85, evenly spaced from x=60 to x=660; 98 of them `#c9ced6` (flipped/empty); tick 1 replaced by a blue `#2a78d6` filled circle radius 7 with bold 12px blue label "your Cup 1" above at y=62; tick 62 replaced by a green `#008300` filled circle radius 7 with bold 12px green label "Cup 62" above at y=62.
- **Strip caption (12px `#6b7280`, centered at y=118):** "98 gray cups = deliberately flipped empty, only two remain".
- **Probability bars (from y=150):** horizontal bars starting at x=170, 22px tall, scale 100% = 480px; "stay on Cup 1" bar 5px long (drawn minimum 5px for 1%), fill `rgba(42,120,214,0.55)`, bold 13px blue label "1/100 = 1%" right of it; "switch to Cup 62" bar 475px at y=190, fill `rgba(0,131,0,0.4)`, bold 13px green label "99/100 = 99%" inside its right end; row labels 12px `#444` right-aligned at x=160.
- **Annotation (bold 13px orange `#d95926`, centered at y=240):** "her 98 informed flips funneled 99% onto one cup".
- **Takeaway (bold 13px magenta `#d55181`, centered at y=272):** "three cups is the same picture: 1/3 stays put, 2/3 funnels to the other cup".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- All case enumerations are exhaustive and hardcoded (3 knowing-host cases, 6 random-host cases, 100-cup strip) — no randomness anywhere in the charts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
