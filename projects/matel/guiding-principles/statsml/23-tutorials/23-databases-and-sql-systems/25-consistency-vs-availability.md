# Consistency vs Availability

**Page type:** detail page (tutorial layout: `.card-section` blocks, each an h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Consistency vs Availability

**Subtitle:** When two copies of the data can't talk to each other, you must choose: refuse to answer, or answer and risk being wrong

## Two Bank Branches and a Cut Phone Line

**Tags:** `core idea` (blue), `running example` (green)

- **The setup** — one account, $100 balance, recorded in the ledgers of two branches
- **Normal day** — every withdrawal is phoned to the other branch, so both ledgers agree
- **The cut** — a storm takes the phone line down; each branch is now on its own
- **The dilemma** — a customer is at the counter, and the branch can't check the other ledger
- **Two honest answers** — "come back later" (safe) or "here's your cash" (risky)

*Example (italic):* The teller can see her own ledger says $100 — what she can't see is whether the other branch just paid some of it out.

**Key point:** With one copy of the data there is no dilemma. The moment there are two copies that can lose contact, "correct" and "always answering" become competing goals.

### Visualization (canvas `c1`, 720×300)

Diagram of two bank branches with duplicate ledgers and a severed phone line between them.

- **Title (bold 15px, `#1a5276`, top center):** "One Account, Two Ledgers — and the Line Between Them Is Down".
- **Branch A** (left): 190×130 box at (60,70), fill `rgba(42,120,214,0.10)`, blue `#2a78d6` stroke, bold 14px "BRANCH A"; inner white 140×60 box labeled 12px "ledger copy" with bold 16px green `#008300` "balance: $100"; grey 12px below: "Alice is at this counter".
- **Branch B** (right): 190×130 box at (470,70), fill `rgba(74,58,167,0.08)`, violet `#4a3aa7` stroke, bold 14px "BRANCH B"; inner white box "ledger copy" with green "balance: $100"; grey "Bob is at this counter".
- **Cut line:** dashed grey line (dash 8/5, width 2) between the branches, broken in the middle by a thick red `#e74c3c` X (width 4); red bold 13px label above: "phone line cut"; grey 12px below: '(the "partition")'.
- **Takeaway** (red bold 14px bottom center): "each ledger says $100 — neither knows what the other just paid out".

## Playing Out Both Choices: Alice Wants $80, Bob Wants $70

**Tags:** `worked example` (green), `trade-off` (orange)

- **During the cut** — Alice asks branch A for $80; Bob asks branch B for $70
- **Choice 1: stay consistent** — both branches refuse; the $100 stays intact, two customers fume
- **Choice 2: stay available** — both pay out: $80 + $70 = $150 leaves a $100 account
- **Reconcile later** — line restored, ledgers compared: the account is $50 overdrawn
- **No third option** — while the line is down, nothing lets both branches be right AND helpful

*Example (italic):* Neither teller made an error — each ledger truthfully said $100 when they paid.

**Key point:** Consistent-but-unavailable loses business; available-but-inconsistent loses money. The choice is which loss you'd rather manage.

### Visualization (canvas `c2`, 720×300)

Split panel playing out both choices side by side.

- **Title (bold 15px, `#1a5276`, top center):** "The Same Cut, Two Endings: $80 + $70 Against a $100 Account".
- **Divider:** vertical dashed grey line (`#bdc3c7`, dash 4/3) at x 360.
- **Left panel** — header blue `#2a78d6` bold 13px: "CHOICE 1: stay consistent". Two 120×52 request boxes (fill `rgba(42,120,214,0.10)`, blue stroke): "Alice asks for $80" and "Bob asks for $70", each stamped red bold 12px "REFUSED". Grey arrows converge to a 140×46 result box (fill `#eef7ee`, green stroke): bold 14px green "balance: $100" with grey 11px "still correct everywhere". Notes: blue bold 12px "correct, but the bank" / "turned away 2 customers"; grey 12px "consistent + unavailable".
- **Right panel** — header orange `#d95926` bold 13px: "CHOICE 2: stay available". Two request boxes (fill `rgba(217,89,38,0.10)`, orange stroke): "Alice gets $80" and "Bob gets $70", each stamped green bold 12px "PAID". Arrows converge to a result box (fill `rgba(231,76,60,0.10)`, red stroke): bold 14px red "balance: −$50" with grey 11px "found at reconciliation". Notes: red bold 12px "$150 left a $100 account —" / "discovered when the line returns"; grey 12px "available + inconsistent".

## The Partition Forces the Choice — Not the Designer

**Tags:** `where it's used` (blue), `trade-off` (orange)

- **Partition** — the network split is called a partition; the cut phone line is one
- **While connected** — you can have both: every answer is correct and instant
- **While split** — every replicated system faces the tellers' dilemma, ready or not
- **Where you meet it** — a dashboard counter that briefly disagrees with the billing table
- **Reconciliation debt** — available systems must clean up later; that job lands in your data

*Example (italic):* The event counter said 10,340 signups; finance said 10,290 — the pipeline chose availability and reconciled overnight.

**Key point:** You don't pick when the choice happens — the partition does. You only pick, in advance, which way each system will jump.

### Visualization (canvas `c3`, 720×300)

Decision-fork flow diagram: normal operation, then a partition event forking into two outcomes.

- **Title (bold 15px, `#1a5276`, top center):** "While Connected You Get Both — the Split Creates the Fork".
- **Top box** (220×46 centered at x 360, fill `#eef7ee`, green stroke): bold 13px green "network fine" over 12px "answers are correct AND instant".
- **Partition box** (200×40 below, fill `rgba(231,76,60,0.10)`, red stroke): bold 13px red "PARTITION: copies lose contact"; grey arrow from top box down.
- **Fork arrows:** blue arrow down-left, orange arrow down-right.
- **Left outcome box** (220×56, fill `rgba(42,120,214,0.10)`, blue stroke): bold 13px blue "refuse to answer" over 12px "consistent — some requests fail".
- **Right outcome box** (220×56, fill `rgba(217,89,38,0.10)`, orange stroke): bold 13px orange "answer from the local copy" over 12px "available — answer may be stale".
- **Takeaway** (violet bold 13px bottom center): "the split decides WHEN — you only pre-decide WHICH WAY".

## Real Systems Choose Per Operation, Not Per System

**Tags:** `common mistake` (red), `core idea` (blue)

- **Not one switch** — a bank isn't "a consistent system"; each operation gets its own rule
- **Check balance** — answer from the local ledger, maybe stale: availability wins
- **Withdraw $5,000** — must confirm with the other copy or refuse: consistency wins
- **ATM offline mode** — small withdrawals allowed up to a $50 cap: bounded risk, still available
- **The cap is the tell** — a limit on possible damage is how systems buy availability safely

*Example (italic):* The same ATM that pays $50 blind during an outage refuses a $5,000 transfer until the line is back.

**Common mistake:** Labeling a whole database "consistent" or "available." Ask instead: for THIS operation, what happens during a partition — refuse, or answer stale?

### Visualization (canvas `c4`, 720×300)

Per-operation dot matrix: four operation rows each mapped to an AVAILABLE or CONSISTENT column.

- **Title (bold 15px, `#1a5276`, top center):** "One Bank, Four Operations, Two Different Rules During an Outage".
- **Column headers** (bold 12px): orange `#d95926` "AVAILABLE" at x 455, blue `#2a78d6` "CONSISTENT" at x 570; dashed grid-colored vertical guide lines under each.
- **Rows** (240×34 operation boxes at x 40, fill `#f8f9fa`, grid stroke, bold 12px label; a 9px-radius dot in the chosen column connected by a thin grid line; grey 11px note at x 610):
  - "check balance" → AVAILABLE (orange dot) — note "risk: stale number".
  - "ATM withdrawal ≤ $50" → AVAILABLE (orange dot) — note "risk: $50 lost".
  - "withdraw $5,000" → CONSISTENT (blue dot) — note "refused during split".
  - "transfer between accounts" → CONSISTENT (blue dot) — note "refused during split".
- **Takeaways** (bottom center): violet bold 13px "the $50 cap is the trick: risk is bounded, so availability is affordable"; grey 12px "big, irreversible operations buy consistency; small, capped ones buy availability".

## Regeneration instructions

- **Template:** tutorials topic-page layout (see `tutorials/CLAUDE.md`). h1 (no index number) with 2px `#2980b9` bottom border, `.subtitle` grey one-liner, then four `.card-section` blocks: each has an `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) and a `table.layout` with `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, one-line `<ul>` bullets each opening with `<b>bold term</b>` (`#1a5276`), one italic `.example` paragraph, one `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem).
- **Tag pills:** inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors: blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; canvases `width:100%`, `1px solid #e0e0e0` border, radius 4px.
- **Canvas:** each declared 720×300 intrinsic; scaled by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Shared `box()` and `arrow()` (filled triangular head) helpers.
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
