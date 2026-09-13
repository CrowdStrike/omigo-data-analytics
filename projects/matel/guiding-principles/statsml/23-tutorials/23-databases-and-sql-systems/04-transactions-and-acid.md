# Transactions & ACID

**Page type:** detail page (tutorial page: h2 card-sections, each a two-column table.layout — text left 50%, canvas right 50%)
**HTML title tag:** Transactions & ACID

**Subtitle:** Moving $100 between two accounts takes two writes — a transaction guarantees both happen or neither does

## Paying the Shop Is Two Writes, Not One

Tags: `core idea` (blue), `running example` (green)

- **The payment** — Asha pays the shop $100 from her account balance
- **Before** — Asha has $500, the shop has $200; together $700
- **Write 1** — take $100 out of Asha's row: 500 → 400
- **Write 2** — put $100 into the shop's row: 200 → 300
- **The invariant** — the $700 total must survive: money moves, it never appears or vanishes

*Example (italic):* One "payment" in real life is two separate row updates inside the database — and anything can happen between them.

**Key point:** Real-world actions rarely map to one write. A transaction is how you tell the database "these writes are one action."

### Visualization (canvas `c1`, 720×300)

Diagram: before/after account tables with two write-step boxes between them, connected by arrows.

- **Title (bold 15px, `#1a5276`, top center):** "One Payment = Two Row Updates".
- **Left accounts table** at (55, 70), labeled "before" (bold 13px `#2c3e50`, centered above): header row `#1a5276` with white bold text "owner" / "balance"; rows "Asha $500", "Shop $200" (alternating white/`#f4f6f8`); border `#cfd8e0`; below the table, centered bold total "total $700" in green `#008300` (totals equal to 700 render green, otherwise red `#e74c3c`). Table 150px wide, 26px row height.
- **Right accounts table** at (515, 70), labeled "after": rows "Asha $400", "Shop $300", both highlighted green `rgba(39,174,96,0.18)` and bold; total "total $700" in green.
- **Middle write boxes** (x=265, width 190):
  - Box 1 at y=78, 36px tall: fill `rgba(42,120,214,0.08)`, stroke `#2a78d6` 2px, bold 12px blue text "write 1: Asha  500 → 400".
  - Box 2 at y=132: fill `rgba(25,158,112,0.08)`, stroke `#199e70` 2px, bold aqua text "write 2: Shop  200 → 300".
- **Arrows:** blue (`#2a78d6`) arrows from before-table to box 1 and from box 1 to after-table; aqua (`#199e70`) arrows from before-table to box 2 and from box 2 to after-table (filled triangular heads).
- **Annotations:** bold 13px orange `#d95926`, centered at y=232: "the gap between write 1 and write 2 is where trouble lives". Muted 12px `#6b7280`, centered at y=258: "$700 before, $700 after — money moved, none was created or destroyed".

## Crash Between the Writes: $100 Vanishes

Tags: `worked example` (green), `failure mode` (red)

- **No transaction** — the two updates run as independent statements
- **Write 1 lands** — Asha's row now says $400
- **The crash** — power cut, process kill, network drop — before write 2 runs
- **After restart** — Asha $400, shop $200: the books total $600
- **Nobody has it** — the $100 isn't in either row; it simply no longer exists

*Example (italic):* Follow it by hand: 500 + 200 = 700 before; 400 + 200 = 600 after the crash — $100 gone without any error in a log.

**Failure mode:** A half-finished multi-write action leaves the data in a state that was never true in the real world.

### Visualization (canvas `c2`, 720×300)

Timeline diagram with a crash marker plus before/after account tables.

- **Title (bold 15px `#1a5276`, top center):** "No Transaction + Crash After Write 1".
- **Timeline:** horizontal gray line (`#999`, 2px) from x=60 to x=660 at y=60, with three 6px-radius dots and bold 12px labels above:
  - x=110: "write 1 runs" in blue `#2a78d6`.
  - x=330: "CRASH" in red `#e74c3c`, with a red 3px zigzag lightning bolt drawn above the dot.
  - x=560: "write 2 never runs" in muted `#6b7280`.
- **Left accounts table** at (80, 110): Asha $500 / Shop $200, total "total $700" in green; label "before" centered below (bold 12px `#2c3e50`).
- **Middle accounts table** at (300, 110): Asha $400 (highlighted red `rgba(231,76,60,0.15)`, bold) / Shop $200; total "total $600" in red `#e74c3c`; label "after restart" below.
- **Right annotation (left-aligned at x=510):** bold 15px red "$100 is in neither row" at y=150; muted 12px lines "not an error anyone saw —" (y=174) and "just books that no longer balance" (y=192).
- **Bottom caption (bold 13px red, centered, y=278):** "500 + 200 = 700 became 400 + 200 = 600".

## BEGIN … COMMIT: Both or Neither

Tags: `worked example` (green), `core idea` (blue)

- **Wrap the writes** — BEGIN, the two UPDATEs, COMMIT
- **Until COMMIT** — the changes are provisional; other readers still see 500/200
- **Crash mid-way?** — on restart the database rolls back: 500/200, as if nothing ran
- **COMMIT succeeds?** — both rows flip together: 400/300, total still $700
- **Only two exits** — fully done or fully undone; the half-state can't be seen

SQL block:

```sql
BEGIN;
UPDATE accounts SET balance = balance - 100
  WHERE owner = 'Asha';
UPDATE accounts SET balance = balance + 100
  WHERE owner = 'Shop';
COMMIT;
```

*Example:* Replay the crash with this wrapper: it lands between the UPDATEs, the restart rolls back, and Asha's row still says $500.

**Key point:** A transaction turns "two writes and a prayer" into one all-or-nothing step. The $600 state becomes impossible.

### Visualization (canvas `c3`, 720×300)

Pipeline diagram of the transaction with its two possible endings, both balanced.

- **Title (bold 15px `#1a5276`, top center):** "Inside a Transaction: Two Possible Endings, Both Balanced".
- **Pipeline:** four boxes in a row starting at x=70, y=52, each 92×32 with 24px gaps: "BEGIN", "write 1", "write 2", "COMMIT" — fill `rgba(42,120,214,0.08)`, stroke `#2a78d6` 1.5px, bold 12px blue labels, blue arrows between consecutive boxes.
- **Note under pipeline (12px muted, left-aligned):** "changes stay provisional in here".
- **Exit 1 (crash):** red 3px lightning zigzag descending from below the "write 2" box, with bold 12px red label "crash before COMMIT" beneath it.
- **Left result table** at (120, 192): Asha $500 / Shop $200 (unchanged), total $700 green; bold 12px green caption above (y=182, centered at x=195): "ROLLBACK: as if nothing ran". Dashed red arrow points from the crash area to this table.
- **Right result table** at (460, 192): Asha $400 / Shop $300 both highlighted green and bold, total $700 green; bold green caption above (centered x=535): "COMMIT: both writes land". Solid green arrow points from the pipeline to this table.
- **Bottom caption (bold 13px orange `#d95926`, centered, y=292):** "the $600 half-state is impossible".

## A, C, I, D — One Plain Line Each

Tags: `core idea` (blue), `why it matters` (orange)

- **Atomic** — all the writes happen, or none do; no halfway
- **Consistent** — the rules hold before and after: totals add up, keys point somewhere
- **Isolated** — a query running mid-transfer never sees the $600 moment
- **Durable** — once COMMIT returns, a crash one second later loses nothing
- **For analysts** — a warehouse loaded without transactions can hold half-finished actions

*Example (italic):* A revenue dashboard that reads mid-transfer would report $600 — a number that was never true for anyone.

**Why it matters:** When a metric looks impossible, ask whether the pipeline read half of someone's transaction — or wrote half of its own.

### Visualization (canvas `c4`, 720×300)

Four-box grid, one box per ACID letter.

- **Title (bold 15px `#1a5276`, top center):** "Four Promises, One Payment".
- **Boxes:** 2×2 grid, each 300×92, starting at (40, 48), gaps 40px horizontal / 26px vertical; white fill, 2.5px colored stroke, bold 15px colored title at top-left, two 12px `#2c3e50` body lines below:
  - "A — Atomic" (blue `#2a78d6`): "both writes or neither;" / "no half-payment exists".
  - "C — Consistent" (green `#008300`): "rules survive: the books" / "total $700 before and after".
  - "I — Isolated" (violet `#4a3aa7`): "a report running mid-transfer" / "never sees $600".
  - "D — Durable" (orange `#d95926`): "after COMMIT, a crash" / "cannot undo the payment".
- **Bottom caption (bold 13px `#1a5276`, centered, y=290):** "every serious database keeps all four — pipelines you write by hand often keep none".

## Regeneration instructions

- **Layout:** tutorial detail page. `<h1>` + `.subtitle`, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border), then `table.layout` (full width, border-collapse) with one `<tr>`: left `td.text-col` (50%) holding `.tags` pills, a `<ul>` of bullets (each opening with `<b>` term in `#1a5276`), an optional `.sql` `<pre>` block, an italic `.example` paragraph, and a `.key-point` callout; right `td.viz-col` (50%) holding one `<canvas>` 720×300 with `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `ul` 0.92rem; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, 0.9rem; `.sql` background `#f8f9fa`, left border 3px solid `#1a5276`, ui-monospace 0.8rem.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Palette:** primary blue `#1a5276` (ink), green `#27ae60`, red `#e74c3c`, orange `#e67e22`; chart palette object P: blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`.
- **Canvas:** intrinsic 720×300 attributes; scale by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). A shared `accounts(ctx,x,y,asha,shop,opts)` helper draws the two-row owner/balance table (150px wide, 26px rows, `#1a5276` header, `#cfd8e0` border, total line green when 700 / red otherwise) and a shared `arrow` helper draws 2px lines with filled triangular heads (optional 5/4 dash). No nav bar, no back/home links. In regenerated HTML, any card links use .html extensions.
