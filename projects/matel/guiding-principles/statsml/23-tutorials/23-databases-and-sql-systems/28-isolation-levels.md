# Isolation Levels

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Isolation Levels

**Subtitle:** How much of another transaction's unfinished work a database lets you see — from everything (fast, risky) to nothing (safe, slow)

## Two Clerks, One Bank Balance

**Tags:** `core idea` (blue), `concurrency` (green), `dirty read` (red)

- **The account** — account #4471 holds $500; two bank clerks touch it in the same second
- **Clerk A** — starts a $300 withdrawal; mid-transaction the balance is briefly $200, uncommitted
- **Clerk B** — checks the balance for a loan decision and sees $200 — money still in limbo
- **The rollback** — A's card check fails, the withdrawal undoes itself; the balance was $500 all along
- **The contract** — the isolation level is the database's rule for what B may see of A's unfinished work

*Example (italic):* Clerk B declines the loan because the account "only has $200" — a balance that never officially existed.

**Key point:** Isolation levels define what one transaction can see of another's in-progress changes — from everything (read uncommitted) to a world where clerks appear to work one at a time (serializable).

### Visualization (canvas `c1`, 720×300)

Two-lane interleaved timeline: Clerk A's withdraw-then-rollback on the top lane, Clerk B's dirty read on the bottom lane, with a dashed arrow marking the leak.

- **Title (bold 15px, `#1a5276`, top center):** "The Dirty Read: B Sees A's Uncommitted $200".
- **Lanes:** two horizontal 2px `#e5e9ef` lanes at y=110 ("Clerk A") and y=200 ("Clerk B"), 12px `#444` lane labels at x=20; time arrow 2px `#999` at y=255 from x=90 to x=660 labeled "time →" (12px `#6b7280`).
- **Event boxes (110×34, 8px radius, 12px `#2c3e50` text, centered at x = `[130, 260, 400, 540]`):**
  - Lane A, x=130, blue `rgba(42,120,214,0.15)` border `#2a78d6`: "read: $500"
  - Lane A, x=260, yellow `rgba(201,133,0,0.15)` border `#c98500`: "write: $200 (uncommitted)"
  - Lane B, x=400, red `rgba(231,76,60,0.12)` border `#e74c3c`: "read: sees $200"
  - Lane A, x=540, violet `rgba(74,58,167,0.12)` border `#4a3aa7`: "ROLLBACK → $500"
- **Leak arrow:** dashed `#e74c3c` (dash 4/3) 2px arrow from the bottom of the x=260 box on lane A to the top of the x=400 box on lane B, 12px `#e74c3c` label "dirty read" beside it.
- **Annotation (bold 13px red `#e74c3c`, near x=430, y=250):** "B acted on a balance that never existed".
- **Caption (12px `#444`, bottom right):** "balances illustrative".

## The Anomaly Ladder: Dirty, Non-Repeatable, Phantom

**Tags:** `worked example` (blue), `anomaly ladder` (green), `SQL standard` (orange)

- **Dirty read** — clerk B sees A's uncommitted $200; blocked at read committed and above
- **Non-repeatable read** — B reads $500, A commits $200, B re-reads: two answers in one transaction
- **Its cure** — repeatable read pins B's rows: both reads return $500 no matter what A commits
- **Phantom** — B counts 3 accounts over $400; A inserts a $450 account; B recounts and gets 4
- **Its cure** — only serializable guards the condition "over $400", not just rows B already read
- **The ladder** — each level up kills one anomaly: uncommitted → committed → repeatable → serializable

*Example (italic):* At repeatable read, clerk B's recount still finds the phantom 4th account — only serializable makes both counts say 3.

**Key point:** Each isolation level is defined by the anomalies it forbids: read committed stops dirty reads, repeatable read stops rows changing under you, serializable stops phantom rows appearing.

### Visualization (canvas `c2`, 720×300)

Matrix chart: 3 anomaly rows by 4 isolation-level columns, each cell a green check (blocked) or red cross (still possible).

- **Title (bold 15px, `#1a5276`, top center):** "Which Level Blocks Which Anomaly".
- **Grid:** column headers 12px bold `#1a5276` at y=80 over x = `[230, 350, 470, 590]`: "read uncomm.", "read comm.", "repeatable", "serializable"; row labels 12px `#444` at x=20 for y = `[125, 180, 235]`: "dirty read", "non-repeatable read", "phantom"; 1px `#e5e9ef` gridlines separating rows and columns.
- **Cells (bold 16px symbol centered at each column x / row y):** red `#e74c3c` "✗" = anomaly possible, green `#008300` "✓" = blocked:
  - dirty read: `["✗", "✓", "✓", "✓"]`
  - non-repeatable read: `["✗", "✗", "✓", "✓"]`
  - phantom: `["✗", "✗", "✗", "✓"]`
- **Highlight:** serializable column wrapped in a 2px `#008300` rounded rectangle (x 555–650, y 95–255), fill `rgba(0,131,0,0.06)`.
- **Annotation (bold 13px green `#008300`, near x=470, y=278):** "only serializable blocks all three".
- **Caption (12px `#444`, bottom right):** "SQL-standard guarantees; some engines block more than required".

## Why Serializable Is Slow — and Rarely the Default

**Tags:** `where it's used` (blue), `performance` (orange), `defaults` (green)

- **The price** — serializable must produce results as if transactions ran one at a time, even when they didn't
- **How** — the database takes extra locks or aborts any transaction whose reads would break the illusion
- **Retries** — aborted transactions must be retried by your code; under contention the retries pile up
- **Defaults differ** — PostgreSQL defaults to read committed; MySQL's InnoDB to repeatable read
- **Analytics** — a dashboard summing rows at a weak level can read a transfer halfway through

*Example (italic):* The same workload runs at 9,200 tps under read committed but 3,400 tps under serializable — most shops take the fast lane and live with the anomalies.

**Key point:** Serializable is the only level correct for every interleaving — the cost is locks, aborts, and retries, which is why almost no major database ships it as the default.

### Visualization (canvas `c3`, 720×300)

Vertical bar chart: transactions per second at each isolation level, dropping sharply at serializable.

- **Title (bold 15px, `#1a5276`, top center):** "Throughput by Isolation Level (Same Workload)".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = transactions/sec 0 to 10,000, gridlines `#e5e9ef` at 2,500/5,000/7,500 with 12px `#444` tick labels; x labels 12px `#444` under each bar.
- **Bars (80px wide, centered at x = `[150, 290, 430, 570]`):** levels `["read uncomm.", "read comm.", "repeatable", "serializable"]`, tps `[9600, 9200, 7800, 3400]`, fills blue `#2a78d6`, aqua `#199e70`, yellow `#c98500`, orange `#d95926`; bold 12px value labels ("9,600" etc.) 6px above each bar top in the bar's color.
- **Annotation (bold 13px orange `#d95926`, near x=430, y=95):** "aborts + retries cost ~2/3 of throughput".
- **Caption (12px `#444`, bottom right):** "throughput numbers illustrative".

## The Default Is Not Serializable

**Tags:** `common mistake` (red), `defaults` (orange)

- **The assumption** — developers write code as if every transaction runs alone; the default disagrees
- **The report** — sums account X ($500) and account Y ($400); the true total is $900
- **The interleave** — after reading X=$500, a $300 X→Y transfer commits; the report then reads Y=$700
- **The result** — the report prints $1,200: half-old, half-new data adds $300 out of thin air
- **No error raised** — read committed permits this; the database did exactly what its default promises

*Example (italic):* Finance chases the phantom $300 surplus for a day; re-run at serializable (or on a snapshot), the report says $900 every time.

**Common mistake:** Assuming the default isolation level is serializable. Almost no database ships that way — if a report must see one consistent moment in time, you have to ask for it explicitly.

### Visualization (canvas `c4`, 720×300)

Two-lane interleaved timeline: a transfer committing between the report's two reads, ending in a wrong total.

- **Title (bold 15px, `#1a5276`, top center):** "Read Committed Report: $500 + $700 = a $1,200 Total That Never Existed".
- **Lanes:** two horizontal 2px `#e5e9ef` lanes at y=110 ("Transfer") and y=200 ("Report"), 12px `#444` lane labels at x=20; time arrow 2px `#999` at y=255 from x=90 to x=660 labeled "time →" (12px `#6b7280`).
- **Event boxes (120×34, 8px radius, 12px `#2c3e50` text):**
  - Lane Report, centered x=160, blue `rgba(42,120,214,0.15)` border `#2a78d6`: "read X: $500"
  - Lane Transfer, centered x=340, green `rgba(0,131,0,0.12)` border `#008300`: "commit: X=$200, Y=$700"
  - Lane Report, centered x=500, blue `rgba(42,120,214,0.15)` border `#2a78d6`: "read Y: $700"
  - Lane Report, centered x=635, red `rgba(231,76,60,0.12)` border `#e74c3c`: "total: $1,200 ✗"
- **True-total marker:** dashed `#6b7280` (dash 4/3) horizontal reference note, 12px `#6b7280` text "true total at any instant: $900" at (x=340, y=75).
- **Annotation (bold 13px red `#e74c3c`, near x=430, y=250):** "$300 appears from thin air".
- **Caption (12px `#444`, bottom right):** "balances illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded literals above (no randomness); account balances ($500/$200/$400/$700, totals $900/$1,200), the 3-vs-4 phantom count, and throughput figures (9,600 / 9,200 / 7,800 / 3,400 tps) are invented and labeled illustrative; the anomaly matrix (`✗`/`✓` per level) follows the SQL-standard definitions; the PostgreSQL and InnoDB default levels are documented behavior.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
