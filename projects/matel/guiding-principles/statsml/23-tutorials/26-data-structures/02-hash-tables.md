# Hash Tables

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Hash Tables

**Subtitle:** A hash table files every item into a numbered bucket computed from its key, so finding one later means checking a handful of items in one small bucket instead of the whole pile

## Ten Boxes Instead of One Big Pile

**Tags:** `core idea` (blue), `buckets` (green), `hash function` (orange)

- **The pile** — Rosa's coffee shop keeps 40 paper loyalty cards; finding one means flipping through all 40
- **The trick** — she buys 10 numbered boxes and files each card by the last digit of its phone number
- **The hash** — that "take the last digit" rule is a hash function: any key in, a box number 0–9 out
- **The buckets** — the boxes are buckets; 40 cards across 10 boxes means each holds about 4
- **The payoff** — to fetch a card she computes the digit, opens that one box, and flips through ~4 cards

*Example (italic):* Maya's number ends in 7, so her card lives in box 7 — Rosa never touches the other nine boxes.

**Key point:** A hash table is a rule that turns a key into a bucket number, plus the buckets themselves — lookups shrink from the whole pile to one small bucket.

### Visualization (canvas `c1`, 720×300)

Single-panel bar chart: card counts in each of the 10 boxes after filing all 40 cards by last phone digit, with the per-box average marked.

- **Title (bold 15px, `#1a5276`, top center):** "40 Loyalty Cards, 10 Boxes — the Last Digit of the Phone Picks the Box".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = box number 0–9, one 60px slot per box with 12px `#444` tick labels "0"–"9"; 12px `#444` axis label "last digit of phone = box number" centered at y=280; y = cards per box 0 to 8, light `#e5e9ef` gridlines at 2, 4, 6, 8 with 12px `#444` labels.
- **Bars:** 40px wide, centered in each slot, counts = `[4, 5, 3, 4, 6, 4, 3, 5, 2, 4]` (sums to 40); fill `rgba(42,120,214,0.35)`, 2px `#2a78d6` stroke; 12px `#2a78d6` count label above each bar.
- **Average line:** horizontal dashed `#6b7280` (dash 4/3) line at count 4 across the plot; 11px `#6b7280` label "average: 4 cards per box" at its right end.
- **Annotation (bold 12px orange `#d95926`, near x=430, y=80):** "find any card: ~4 flips, not 40".
- **Caption (12px `#444`, bottom right):** "illustrative — 40 cards spread by last digit".

## Two Cards Land in Box 7

**Tags:** `worked example` (blue), `collisions` (orange)

- **Four cards** — Maya …4217, Ben …3082, Ana …5557, Leo …9930 arrive; last digits are 7, 2, 7, 0
- **A clash** — Maya and Ana both end in 7, so both cards land in box 7: that is a collision
- **Not a bug** — collisions are expected; the box simply keeps a short stack (a chain) of cards
- **Lookup** — to find Ana: digit 7 → open box 7 → flip past Maya, hit Ana on the second card
- **Still fast** — two flips instead of four; a lookup costs the chain length, not the table size

*Example (italic):* Ana's card takes two flips inside box 7; in one unsorted pile of 40 she could be the 40th card checked.

**Key point:** Collisions are normal, not errors — each bucket holds a short list, and a lookup costs the length of that one short list.

### Visualization (canvas `c2`, 720×300)

Flow diagram: four labeled cards at the top, arrows down into a row of ten boxes, with box 7 visibly holding a two-card stack.

- **Title (bold 15px, `#1a5276`, top center):** "Last Digit Decides the Box: 4217 → 7, 3082 → 2, 5557 → 7, 9930 → 0".
- **Card row (y=52, height 34):** four rounded rects 130px wide centered at x = 130, 290, 450, 610; 1.5px borders and text in each card's color — Maya `#2a78d6`, Ben `#008300`, Ana `#d95926`, Leo `#199e70`; bold 12px name plus 11px "…4217" / "…3082" / "…5557" / "…9930" inside.
- **Box row (y=170, height 62):** ten rects 56px wide at x = 70 + i×60 (i = 0–9), 1.5px `#1a5276` stroke, white fill; 11px `#444` labels "box 0"–"box 9" centered below each at y=248.
- **Card slips inside boxes:** box 0 one 11px aqua `#199e70` slip "Leo"; box 2 one 11px green `#008300` slip "Ben"; box 7 two stacked 11px slips — "Maya" (blue `#2a78d6`) above "Ana" (orange `#d95926`).
- **Arrows:** 2px lines with small arrowheads from each card's bottom edge to its box's top edge, in the card's color — Maya → box 7 (x=518), Ben → box 2 (x=218), Ana → box 7 (x=518), Leo → box 0 (x=98).
- **Annotation (bold 12px magenta `#d55181`, near x=560, y=140):** two lines: "two keys, one box — a collision" / "the box keeps a short stack".
- **Caption (12px `#444`, bottom right at y=292):** "illustrative — four cards into ten buckets".

## When the Boxes Fill Up, Double Them

**Tags:** `where it's used` (blue), `resizing` (green), `load factor` (orange)

- **Growth** — the shop thrives: 50 cards, then 80; the 10 boxes now average 5 to 8 cards each
- **Load factor** — cards ÷ boxes is the load factor; the shop's rule: at 5 per box, add boxes
- **Doubling** — at 50 cards Rosa doubles to 20 boxes and files by the last two digits mod 20
- **Rehash** — every card gets re-filed once, because the box rule changed with the box count
- **Payback** — right after the move the 20 boxes average 2.5 cards; lookups drop back to a couple of flips
- **In software** — Python dicts and Java HashMaps do exactly this: grow, rehash, keep lookups flat

*Example (italic):* At 80 cards, staying with 10 boxes means ~8 flips per lookup; the resized 20 boxes mean ~4.

**Key point:** Resizing keeps the load factor low — a one-time re-filing of every item buys back fast lookups from then on.

### Visualization (canvas `c3`, 720×300)

Single-panel line chart: average cards per box as the card pile grows from 10 to 80, one line that keeps 10 boxes forever versus one that doubles to 20 boxes at 50 cards.

- **Title (bold 15px, `#1a5276`, top center):** "The Shop Rule: When Boxes Average 5 Cards, Double the Boxes".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = total cards 10 to 80, 12px `#444` tick labels every 10; y = average cards per box 0 to 8, light `#e5e9ef` gridlines at 2, 4, 6, 8 with 12px `#444` labels.
- **Rule line:** horizontal dashed `#6b7280` (dash 4/3) line at average 5; 11px `#6b7280` label "shop rule: 5 per box" at its left end.
- **Keep-10-boxes line:** orange `#d95926` 2px dashed (dash 6/4) through (cards, average) points `[10,1], [20,2], [30,3], [40,4], [50,5], [60,6], [70,7], [80,8]`; 12px orange label "keep 10 boxes" near its right end.
- **Double-at-50 line:** green `#008300` 3px solid through `[10,1], [20,2], [30,3], [40,4], [50,2.5], [60,3], [70,3.5], [80,4]`, drawn with a vertical step down from 5 to 2.5 at cards=50; bold 12px green label "double to 20 boxes" above the line near cards≈65 (y=160).
- **Resize marker:** vertical dashed blue `#2a78d6` (dash 4/3) line at cards=50 from baseline to y=70; bold 13px blue label at its top: "50 cards: 10 → 20 boxes, re-file once".
- **Annotation (bold 12px green `#008300`, near cards≈62, y=200):** "back to ~2.5 per box after the move".
- **Caption (12px `#444`, bottom right):** "illustrative — averages, not per-box counts".

## A Bad Filing Rule Puts Everything in One Box

**Tags:** `common mistake` (red), `hash quality` (orange)

- **The blunder** — a new hire re-files the cards by the FIRST digit; every local number starts with 5
- **One box** — all 40 cards pile into box 5 while the other nine boxes sit completely empty
- **Formula vs spread** — the rule still "hashes", but a key part that never varies cannot spread
- **Back to the pile** — a lookup in box 5 is up to 40 flips: the table quietly became the pile again
- **The fix** — hash the part of the key that actually varies (last digits, or mix all of them)

*Example (italic):* Same 40 cards, same 10 boxes — first-digit filing gives one box of 40, last-digit filing gives ten boxes of about 4.

**Common mistake:** Assuming any hash rule spreads keys. A hash built on a part of the key that barely varies clumps everything into one bucket — check the spread, not the formula.

### Visualization (canvas `c4`, 720×300)

Two side-by-side bar panels on the same 0–40 count scale: the first-digit filing (one tower in box 5) versus the last-digit filing (ten short, even bars).

- **Title (bold 15px, `#1a5276`, top center):** "Same 40 Cards, Same 10 Boxes — Two Filing Rules".
- **Left panel (bad hash):** plot area x=60 to 340, baseline y=245, plot height 165; bold 13px `#e74c3c` panel label above: "first digit — every number starts with 5"; x = boxes 0–9 (11px `#444` tick labels), y = 0 to 40 with light `#e5e9ef` gridlines at 10, 20, 30, 40 (12px `#444` labels); counts = `[0, 0, 0, 0, 0, 40, 0, 0, 0, 0]`; the box-5 bar fill `rgba(231,76,60,0.35)` with 2px `#e74c3c` stroke and bold 12px `#e74c3c` label "40" above.
- **Right panel (good hash):** plot area x=410 to 690, same baseline, height, and 0–40 y scale; bold 13px `#2a78d6` panel label above: "last digit — spreads evenly"; counts = `[4, 5, 3, 4, 6, 4, 3, 5, 2, 4]`; fill `rgba(42,120,214,0.35)`, 2px `#2a78d6` stroke; 11px `#2a78d6` count labels above the bars.
- **Annotation (bold 12px red `#e74c3c`, over the left panel near x=200, y=95):** two lines: "one bucket does all the work —" / "lookups are back to 40 flips".
- **Caption (12px `#444`, bottom right):** "illustrative — same scale on both panels".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar counts, line points, and box contents are the hardcoded arrays above (no randomness); the c1 and c4 last-digit counts are the same `[4, 5, 3, 4, 6, 4, 3, 5, 2, 4]` array summing to 40, and the c3 averages equal cards ÷ boxes exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
