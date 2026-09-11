# Types of Games

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Types of Games

**Subtitle:** Three yes-or-no questions — do gains sum to zero, do players move at once, do they meet again — sort almost any strategic situation into a family with known tools

## Does One Side's Win Cost the Other?

**Tags:** `core idea` (blue), `zero-sum` (orange)

- **Zero-sum** — a $100 poker pot: every dollar you win is a dollar someone else loses
- **Fixed pie** — however the hands play out, the payoffs always add up to the same pot
- **Non-zero-sum** — a used-car sale: the seller values the car at $5,000, the buyer at $6,000
- **Made value** — any price between those creates $1,000 of shared gain; the deal makes value
- **Why it matters** — zero-sum means pure conflict; non-zero-sum leaves room for deals

*Example (italic):* At a $5,400 sale price the seller gains $400 and the buyer gains $600 — $1,000 of value that only exists because they traded.

**Key point:** First question: do the gains sum to zero? If yes, helping the other side always hurts you; if no, cooperation can pay both sides at once.

### Visualization (canvas `c1`, 720×300)

Two bar panels split by a light divider at x=360: a poker pot that sums to zero on the left, a value-creating car sale on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Do the Gains Sum to Zero? (illustrative)".
- **Divider:** 1px `#e5e9ef` vertical line at x=360 from y=40 to y=268.
- **Left panel (poker):** header bold 13px `#2c3e50` centered at (195, 52): "poker: a $100 pot changes hands". Dashed (4/3) 1px `#999` zero line from x=70 to x=330 at y=165, labeled "$0" (12px `#444`, right-aligned at x=64). Winner bar: fill `rgba(0,131,0,0.45)`, x=110, width 70, rising 60px above the zero line, value label bold 13px green `#008300` "+$60" above it. Loser bar: fill `rgba(217,89,38,0.45)`, x=210, width 70, dropping 60px below the zero line, value label bold 13px orange `#d95926` "−$60" below it. Category labels 12px `#444` "winner" / "loser" at y=255. Sum line bold 13px `#2c3e50` centered at (195, 272): "sums to $0".
- **Right panel (car sale):** header bold 13px `#2c3e50` centered at (540, 52): "car sale at $5,400"; sub-caption 12px `#6b7280` at (540, 70): "seller values it $5,000 · buyer values it $6,000". Baseline 1px `#999` from x=420 to x=680 at y=225; bar height scale maps $600 to 130px. Seller bar at x=455 (width 70) = $400 and buyer bar at x=555 = $600, both fill `rgba(0,131,0,0.45)`, value labels bold 13px green "+$400" / "+$600" above, names 12px `#444` "seller" / "buyer" below. Sum line bold 13px green centered at (540, 272): "sums to +$1,000".
- **Insight (bold 13px magenta `#d55181`, centered at y=294):** "zero-sum: pure conflict · non-zero-sum: room for deals".

## Do Players Move Together or in Turns?

**Tags:** `core idea` (blue), `simultaneous vs sequential` (green)

- **Simultaneous** — sealed bids, rock-paper-scissors: you commit without seeing the other's move
- **Sequential** — salary counter-offers, chess: you see the move on the table before answering
- **The picture** — simultaneous games are drawn as a payoff matrix, one cell per combination
- **The tree** — sequential games are drawn as a game tree, one branch per move in order
- **Why it matters** — hidden moves and visible moves call for different reasoning tools

*Example (italic):* In a sealed-bid auction you write your number blind; in a negotiation you answer an offer you can read.

**Key point:** Second question: do players move at once or in turns? The answer decides the picture you draw — a matrix for hidden simultaneous moves, a tree for visible turns.

### Visualization (canvas `c2`, 720×300)

Two icons split by a light divider at x=360: a small 2×2 payoff-matrix grid on the left, a two-level game tree on the right.

- **Title (bold 15px, `#1a5276`, top center):** "One Question Decides the Picture You Draw".
- **Divider:** 1px `#e5e9ef` vertical line at x=360 from y=40 to y=268.
- **Left icon (matrix):** 2×2 grid at origin (120, 84), 70px square cells; fill `rgba(42,120,214,0.10)`, 1.5px blue `#2a78d6` borders; each cell holds "$, $" bold 14px blue centered. Label bold 13px blue centered at (190, 254): "simultaneous → payoff matrix"; caption 12px `#6b7280` at (190, 274): "moves hidden — you commit blind".
- **Right icon (tree):** 7 nodes, radius 10 — root (540,84) solid green `#008300`, others fill `rgba(0,131,0,0.15)` with 1.5px green stroke: level 1 at (470,158) and (610,158); level 2 at (435,228), (505,228), (575,228), (645,228). Green 1.5px edges root→children→grandchildren. Small 12px `#6b7280` edge labels "offer" at (562,78) and "counter" at (618,118). Label bold 13px green centered at (540, 254): "sequential → game tree"; caption 12px `#6b7280` at (540, 274): "moves visible — you answer what you see".

## Do They Meet Once or Again and Again?

**Tags:** `worked example` (blue), `repeated games` (green)

- **One-shot** — a tourist-trap restaurant can overcharge: it never sees the same guest again
- **Repeated** — a neighborhood cafe can't: tomorrow's visit is on the line every single day
- **The supplier** — dealing fairly earns $10 a round; over 10 rounds that adds up to $100
- **The cheat** — cheating in round 3 pays $15 once, but the burned buyer switches away
- **The bill** — after cheating the supplier earns just $5 a round: 10+10+15+5×7 = $70 total

*Example (italic):* Fair dealing beats the round-3 cheat by $100 − $70 = $30 — repetition makes honesty the profitable strategy.

**Key point:** Third question: do the players meet again? Repetition puts future payoffs on the line, which makes promises and honesty enforceable without any contract.

### Visualization (canvas `c3`, 720×300)

Two cumulative-earnings lines over rounds 1–10: fair dealing climbs steadily to $100 while the round-3 cheater spikes once and flattens to $70.

- **Title (bold 15px, `#1a5276`, top center):** "Ten Rounds With a Supplier: Fair Dealing vs Cheating".
- **Axes:** origin x=70, baseline y=240, plot width 560, plot height 175; y maps $0–$110. X ticks 1–10 (12px `#444`), x-axis caption "round" centered below; y labels "$0", "$50", "$100" right-aligned left of the axis.
- **Fair line (green `#008300`, 3px):** cumulative values by round `[10,20,30,40,50,60,70,80,90,100]`; end label bold 13px green "$100 fair" right of round 10.
- **Cheat line (orange `#d95926`, 3px):** cumulative values `[10,20,35,40,45,50,55,60,65,70]`; end label bold 13px orange "$70 cheat" right of round 10.
- **Cheat marker:** magenta `#d55181` 7px dot at (round 3, $35); annotation bold 12px magenta "cheats here: +$15 once" at (xOf(3)+12, yOf(35)+22), sub-line 12px `#6b7280` "buyer switches — only $5/round after" 18px lower.
- **Insight (bold 13px green, left-aligned at x = xOf(4.2), y = yOf(100)+8):** "fair play wins by $30 (illustrative)".

## Sorting Real Situations

**Tags:** `where it's used` (blue), `rule of thumb` (green)

- **Chess** — one side's win is the other's loss, moves alternate, and the match is played once
- **Ad auction** — one slot makes it zero-sum-ish, bids are sealed, and it repeats every query
- **Price war** — two shops share customers (non-zero-sum), set prices at once, repeat daily
- **Salary talks** — both sides can gain, offers go in turns, and it's one-shot per job
- **The habit** — three quick questions place a new situation in a family with known tools

*Example (italic):* Before analyzing any strategic situation, run the three questions first — the answers tell you which chapter of game theory applies.

**Key point:** Rule of thumb: classify before you analyze. Zero-sum or not, at once or in turns, once or repeated — the three answers pick your tools.

### Visualization (canvas `c4`, 720×300)

A 4-row × 3-column chip grid classifying four scenarios by the three questions; green chips for yes, orange chips for no.

- **Title (bold 15px, `#1a5276`, top center):** "Four Situations, Three Questions".
- **Column headers (bold 13px `#1a5276`, centered at y=62):** "zero-sum?" at x=330, "at once?" at x=468, "repeated?" at x=606.
- **Row labels (bold 13px `#2c3e50`, right-aligned at x=262, row centers y = 94 / 141 / 188 / 235):** "chess", "sealed-bid ad auction", "price war (two shops)", "salary negotiation".
- **Chips:** rounded pills 88×26 (13px corner radius) centered on each column x at the row center; yes chips fill `rgba(0,131,0,0.12)` with 1.5px green `#008300` border and bold 12px green text, no chips fill `rgba(217,89,38,0.12)` with orange `#d95926` border and text.
- **Chip values:** chess = yes / no / no; sealed-bid ad auction = yes-ish / yes / yes; price war = no / yes / yes; salary negotiation = no / no / no ("yes-ish" styled as a yes chip).
- **Row separators:** 1px `#e5e9ef` horizontal lines from x=60 to x=660 between rows.
- **Insight (bold 13px magenta `#d55181`, centered at y=284):** "three answers place any strategic situation in a family with known tools".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
