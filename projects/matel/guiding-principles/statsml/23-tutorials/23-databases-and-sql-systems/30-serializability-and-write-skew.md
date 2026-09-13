# Serializability & Write Skew

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Serializability & Write Skew

**Subtitle:** Two transactions can each read a consistent snapshot, write different rows, both commit — and still break a rule that spans rows; only serializable isolation catches it

## The Night Both On-Call Doctors Went Home

**Tags:** `core idea` (blue), `concurrency` (green), `hidden anomaly` (orange)

- **The rule** — a hospital requires at least 1 doctor on call at all times; tonight Alice and Bob both are
- **The check** — before going off call, a doctor's app counts who is on call and allows leaving if count ≥ 2
- **The race** — at 6:00pm Alice and Bob both tap "end my shift" within the same second
- **The snapshot** — each transaction reads a frozen snapshot of the table: both count 2 doctors on call
- **The writes** — Alice's transaction updates only Alice's row, Bob's updates only Bob's row
- **The break** — no two writes touch the same row, so both commit — and now 0 doctors are on call

*Example (italic):* Both apps ran the same safety check, both saw "2 on call, safe to leave", and the hospital ended the night with nobody on call.

**Key point:** Write skew is two transactions reading the same data, deciding based on it, and writing to *different* rows — each is fine alone, but together they break an invariant no single row owns.

### Visualization (canvas `c1`, 720×300)

Two-lane swimlane diagram: Alice's and Bob's transactions running in parallel, each lane showing read → decide → write → commit, with the shared invariant collapsing at the end.

- **Title (bold 15px, `#1a5276`, top center):** "6:00pm: Two Transactions, Two Different Rows, One Broken Rule".
- **Lanes:** two horizontal lanes, Alice at y=95 and Bob at y=185, each with a left 12px `#444` label ("Alice's txn" at x=20, y=95; "Bob's txn" at x=20, y=185) and a 1px `#e5e9ef` lane divider at y=140.
- **Alice boxes (blue `#2a78d6` 1.5px border, fill `rgba(42,120,214,0.15)`, ~120px wide, 34px tall, 6px radius, 12px `#2c3e50` text), centered at x = 170, 320, 470, 600 on y=95:** "read: 2 on call", "2 ≥ 2 — ok", "set Alice off", "commit ✓".
- **Bob boxes (aqua `#199e70` border, fill `rgba(25,158,112,0.15)`, same size), centered at x = 200, 350, 500, 630 on y=185:** "read: 2 on call", "2 ≥ 2 — ok", "set Bob off", "commit ✓".
- **Arrows:** 2px lane-colored arrows between consecutive boxes in each lane.
- **Result box:** red `#e74c3c` 2px-border box, fill `rgba(231,76,60,0.12)`, centered at x=635, y=262, ~150px wide, labeled bold 12px red "on call now: 0".
- **Annotation (bold 13px orange `#d95926`, centered near x=360, y=250):** "no write touched the same row — both commits succeed".
- **Caption (12px `#444`, bottom left):** "timings illustrative".

## Stepping Through Both Transactions

**Tags:** `worked example` (blue), `snapshot reads` (green)

- **Step 1 (6:00:00)** — Alice's transaction begins; its snapshot freezes the table: Alice on, Bob on
- **Step 2 (6:00:01)** — Bob's transaction begins; his snapshot also shows Alice on, Bob on
- **Step 3** — Alice counts on-call doctors in her snapshot: 2, so 2 − 1 = 1 remains — check passes
- **Step 4** — Bob counts in his snapshot: also 2, so his check passes too
- **Steps 5–6** — Alice writes `on_call = false` on her row; Bob writes it on his — different rows
- **Steps 7–8 (6:00:06–07)** — Alice commits, then Bob; no conflict is detected; real count hits 0

*Example (italic):* Each snapshot kept showing "2 on call" from begin to commit, while the real table went 2 → 1 → 0 underneath both of them.

**Key point:** Each transaction is internally consistent — its reads never contradict each other — yet the decision each based on its snapshot was stale by the time it committed.

### Visualization (canvas `c2`, 720×300)

Step chart over the 8 steps: the count each snapshot believes (flat at 2) vs the actual number of on-call doctors after each commit (dropping 2 → 1 → 0), with the invariant floor at 1.

- **Title (bold 15px, `#1a5276`, top center):** "What the Snapshots See vs What Is Actually True".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = steps 1 to 8 with 12px `#444` tick labels "s1".."s8"; y = doctors on call 0 to 3, gridlines `#e5e9ef` at 1 and 2, 12px `#444` y labels 0/1/2/3.
- **Snapshot line:** blue `#2a78d6` 3px line through steps `[1, 2, 3, 4, 5, 6, 7, 8]`, values `[2, 2, 2, 2, 2, 2, 2, 2]` — flat; bold 12px blue label "both snapshots: 2" near step 3, above the line.
- **Actual line:** red `#e74c3c` 3px step line through steps `[1, 2, 3, 4, 5, 6, 7, 8]`, values `[2, 2, 2, 2, 2, 2, 1, 0]` — drops to 1 at Alice's commit (s7) and 0 at Bob's (s8); bold 12px red label "actual: 0" near step 8.
- **Invariant floor:** dashed `#c98500` (dash 5/4) horizontal line at y-value 1 with 12px `#c98500` label "rule: at least 1" at its left end.
- **Commit markers:** vertical dashed `#6b7280` (dash 4/3) lines at steps 7 and 8, 11px `#6b7280` labels "Alice commits" and "Bob commits" at their tops.
- **Annotation (bold 13px magenta `#d55181`, near step 5, y=70):** "the reads were true once — just not at commit time".
- **Caption (12px `#444`, bottom right):** "step timings illustrative".

## Every Check-Then-Write Rule Is at Risk

**Tags:** `where it's used` (blue), `fixes` (green), `invariants` (orange)

- **The pattern** — read some rows, check a condition, write other rows: every such rule can skew
- **Inventory floor** — two orders each see 1 unit in stock, both sell it, and stock lands at −1
- **Budget cap** — two teams each see $9,500 spent of a $10,000 cap, each add $400: total $10,300
- **Double booking** — two people check a room is free for 2pm, and both insert a booking row
- **Fix 1: serializable** — run at serializable isolation; the database aborts one of the pair, it retries
- **Fix 2: lock the check** — `SELECT ... FOR UPDATE` on the rows you counted makes the second reader wait

*Example (italic):* The doctors' fix is one line — lock the on-call rows during the count, or run the shift-change transaction at serializable and retry on abort.

**Key point:** Any constraint that spans more than one row — floors, caps, uniqueness across rows — is invisible to per-row conflict checks; you must lock what you read or run serializable.

### Visualization (canvas `c3`, 720×300)

Horizontal grouped bars for three scenarios: the value each transaction saw at its check (blue) vs the actual value after both commit (red), each with its limit marked.

- **Title (bold 15px, `#1a5276`, top center):** "What Each Transaction Checked vs What Both Commits Produced".
- **Layout:** three row groups at y = 80, 150, 220; left-aligned 12px `#444` scenario labels at x=20: "on-call doctors (min 1)", "stock (min 0)", "budget of $10,000 (cap)".
- **Bars:** for each group, two 16px-tall bars starting at x=230, max width 420, 11px value labels at bar ends:
  - on-call: blue `#2a78d6` "checked: 2" width 160; red `#e74c3c` "actual: 0" width 4
  - stock: blue "checked: 1" width 80; red "actual: −1" width 4 (label to the right of the tiny bar)
  - budget: blue "checked: $9,500" width 350; red "actual: $10,300" width 400
- **Limit markers:** vertical dashed `#c98500` (dash 5/4) tick per group at the limit position — on-call at width 80 (limit 1), stock at width 0 (limit 0), budget at width 380 (cap $10,000) — each with an 11px `#c98500` label "limit".
- **Bar style:** blue fill `rgba(42,120,214,0.30)` with 1.5px `#2a78d6` border; red fill `rgba(231,76,60,0.25)` with 1.5px `#e74c3c` border.
- **Annotation (bold 13px violet `#4a3aa7`, right side near y=255):** "every check passed; every limit broke".
- **Caption (12px `#444`, bottom right):** "amounts illustrative; bar widths schematic".

## "Snapshot Isolation" Is Not "Serializable"

**Tags:** `common mistake` (red), `isolation levels` (orange)

- **The belief** — "my reads are consistent and my writes don't conflict, so it's as if I ran alone"
- **The truth** — snapshot isolation checks write-write conflicts on the *same* rows, nothing more
- **The gap** — write skew has zero same-row conflicts, so snapshot isolation waves it through
- **Naming trap** — some databases label snapshot behavior with a stronger-sounding level name
- **The test** — ask: does any transaction write based on rows it only *read*? If yes, skew is possible
- **True serializable** — also tracks read-write conflicts and aborts one transaction of the doctor pair

*Example (italic):* The doctors' app was "fully isolated" by snapshot rules — the outcome (0 on call) still matches no serial order, because whoever ran second would have counted 1 and stayed.

**Common mistake:** Assuming snapshot isolation gives serializability. It prevents dirty reads and lost updates, but write skew is exactly the anomaly it misses — that gap is why serializable exists as a separate, stricter level.

### Visualization (canvas `c4`, 720×300)

Anomaly-coverage matrix: three isolation levels as rows, three anomalies as columns, filled with blocked/allowed cells — the write-skew column is the one snapshot isolation fails.

- **Title (bold 15px, `#1a5276`, top center):** "Which Level Blocks Which Anomaly".
- **Grid:** 3 rows × 3 columns of cells ~150px wide, 44px tall, 6px radius, starting at x=250; row centers y = 105, 165, 225; row labels 12px `#444` right-aligned at x=240: "read committed", "snapshot isolation", "serializable"; column headers bold 12px `#1a5276` at y=68 over each column: "dirty read", "lost update", "write skew".
- **Cell data (hardcoded matrix, rows top to bottom):** read committed `["blocked", "allowed", "allowed"]`; snapshot isolation `["blocked", "blocked", "allowed"]`; serializable `["blocked", "blocked", "blocked"]`.
- **Cell style:** "blocked" cells fill `rgba(0,131,0,0.12)`, 1.5px `#008300` border, bold 12px `#008300` text "blocked ✓"; "allowed" cells fill `rgba(231,76,60,0.12)`, 1.5px `#e74c3c` border, bold 12px `#e74c3c` text "allowed ✗".
- **Highlight:** 2.5px `#d95926` rounded rectangle drawn around the snapshot-isolation / write-skew cell.
- **Annotation (bold 13px orange `#d95926`, centered near y=272):** "this one cell is the doctors' bug — and the whole reason serializable exists".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the doctor scenario (2 on call → 0), c2 step lines (snapshot `[2,2,2,2,2,2,2,2]`, actual `[2,2,2,2,2,2,1,0]`), c3 scenario numbers (stock 1 → −1, budget $9,500 + $400 + $400 = $10,300 vs $10,000 cap), and the c4 blocked/allowed matrix are invented and labeled illustrative/schematic where numeric.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
