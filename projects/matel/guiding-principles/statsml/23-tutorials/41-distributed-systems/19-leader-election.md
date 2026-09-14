# Leader Election

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Leader Election

**Subtitle:** When a group of identical machines must do a job exactly once, they first agree on who is in charge — someone must be the leader, and only one someone

## Five Workers, One Invoice Batch

**Tags:** `core idea` (blue), `coordination` (green), `exactly once` (orange)

- **The team** — five identical workers (W1–W5) can each send the daily 500-invoice batch
- **The rule** — exactly one may send it; two senders means every customer is billed twice
- **The leader** — the workers agree on one boss, W3; only the leader talks to the invoice service
- **The standbys** — W1, W2, W4, W5 do nothing but watch the leader's heartbeats, ready to step in
- **The problem** — machines crash; when W3 dies a new leader must take over, never two at once

*Example (italic):* At 9am worker W3 alone sends the 500 invoices; the other four confirm a leader exists and stay idle.

**Key point:** Leader election lets a group of equals agree on a single one in charge — so a "do this exactly once" job has exactly one doer, even though any worker could do it.

### Visualization (canvas `c1`, 720×300)

Flow diagram: five worker boxes in a row, the elected leader highlighted, a single arrow from the leader down to the invoice service — the four standbys have no arrow.

- **Title (bold 15px, `#1a5276`, top center):** "Five Could Send It — the Election Picks Exactly One".
- **Worker row (y=80):** five rounded boxes 110px wide, 44px tall, 8px radius, at x = `[35, 170, 305, 440, 575]`; W1, W2, W4, W5 in blue fill `rgba(42,120,214,0.15)` with 12px `#2c3e50` label "W1 — standby" etc.; W3 (x=305) in green fill `rgba(0,131,0,0.12)`, 2px `#008300` border, bold 12px label "W3 — leader (epoch 7)".
- **Invoice service box (x=280, y=200):** 160px wide, 44px tall, ink `#1a5276` border, label "invoice service" with 12px `#6b7280` sub-label "500 invoices/day".
- **Arrow:** one 3px green `#008300` arrow from W3's bottom edge straight down to the service box, 12px green label "sends the batch" beside it.
- **Standby marks:** dashed 1px `#6b7280` short vertical stubs (dash 4/3) under W1, W2, W4, W5 ending in nothing, 11px `#6b7280` label "no send path" under W1.
- **Annotation (bold 13px green `#008300`, right side near y=225):** "exactly one arrow in — exactly one sender".
- **Caption (12px `#444`, bottom right):** "worker count and batch size illustrative".

## Three Missed Heartbeats, Then a Vote

**Tags:** `worked example` (blue), `heartbeats` (green), `majority vote` (orange)

- **The heartbeat** — leader W3 pings the group every 1 second: "still alive, still in charge"
- **The crash** — W3 dies at t=3s; heartbeats arrive at 1s, 2s, 3s, then silence
- **The timeout** — after 3 missed beats (t=6s) the four standbys declare the leader dead
- **The vote** — each worker votes for a successor; W5 gets W1, W2, and its own vote — 3 of 5
- **The epoch** — 3 of 5 is a majority, so W5 becomes leader of epoch 8 and stamps everything "8"
- **Why majority** — only one candidate can reach 3 of 5, so two workers can never both win

*Example (italic):* Dead is declared at t=6s, the votes are counted by t=7s, and W5 (3 of 5 votes) starts heartbeating as the epoch-8 leader.

**Key point:** Detection is a timeout, the choice is a majority vote, and the epoch number records whose turn it is — the whole handover takes a few seconds.

### Visualization (canvas `c2`, 720×300)

Two-lane timeline over 12 seconds: W3's heartbeats stopping at the crash, the 3-second timeout window, the election moment, then W5's heartbeats beginning under a new epoch.

- **Title (bold 15px, `#1a5276`, top center):** "Crash at t=3s, Declared Dead at t=6s, New Leader by t=7s".
- **Axes:** origin x=60, baseline y=245, plot width 600 (50px per second); x = seconds 0 to 12 with 12px `#444` tick labels every 2s; two lanes: "W3 (epoch 7)" at y=110 and "W5 (epoch 8)" at y=180, 12px `#444` lane labels at x=8, thin `#e5e9ef` lane lines.
- **W3 heartbeats:** green `#008300` filled circles (r=6) on the W3 lane at seconds `[1, 2, 3]`; red `#e74c3c` bold 16px "✗" at t=3 just above the lane with 12px red label "W3 crashes".
- **Timeout window:** orange fill `rgba(217,89,38,0.12)` band from t=3 to t=6 across both lanes, 12px `#d95926` label "3 missed beats" centered in it.
- **Election marker:** violet `#4a3aa7` dashed vertical line (dash 4/3) at t=6, bold 12px violet label "declared dead — vote: W5 gets 3 of 5" at its top.
- **W5 heartbeats:** green `#008300` filled circles (r=6) on the W5 lane at seconds `[7, 8, 9, 10, 11, 12]`, 12px green label "epoch 8 begins" above the t=7 dot.
- **Annotation (bold 13px green `#008300`, near x=9s, y=60):** "3 of 5 votes — no rival can also get 3".
- **Caption (12px `#444`, bottom right):** "timings and vote counts illustrative".

## One Primary, or Split Brain

**Tags:** `where it's used` (blue), `databases` (green), `split brain` (red)

- **Primary databases** — one replica accepts writes and the rest copy it; an election picks that one
- **Job schedulers** — nightly backups, report runs, invoice batches: exactly-once jobs need one owner
- **Lock services** — ZooKeeper, etcd, and Raft-based stores exist largely to run this election
- **Split brain** — a network split with no majority rule can leave two nodes both acting as leader
- **The damage** — two leaders each send the 500-invoice batch: 500 customers billed twice

*Example (italic):* A 30-second network split with two self-appointed leaders produced 500 duplicate invoices — one for every customer in the batch.

**Key point:** Wherever a system says "the primary" or "the master", an election chose it — and the majority rule is what stops a split network from crowning two leaders at once.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: invoices sent under one elected leader vs under split brain, with the duplicated half of the split-brain bar highlighted in red.

- **Title (bold 15px, `#1a5276`, top center):** "Split Brain Doubles the Batch: 500 Customers Billed Twice".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, scale 0.44px per invoice (max width 440 = 1,000 invoices); left-aligned 12px `#444` row labels at x=20.
- **Rows (bars 26px tall at y = 90 and y = 175):**
  - "one elected leader": blue `#2a78d6` fill `rgba(42,120,214,0.30)` bar width 220 (500 invoices), 11px `#444` end label "500 sent — 0 duplicates" with a bold 12px green `#008300` "✓".
  - "split brain — two leaders": blue bar width 220 (first 500) followed by a solid red `#e74c3c` segment width 220 (the duplicate 500), total width 440 (1,000 sends), bold 12px red end label "500 billed twice".
- **Gridline ticks:** 11px `#6b7280` labels "0", "500", "1,000" under the axis at x = 230, 450, 670, thin `#e5e9ef` vertical gridlines.
- **Annotation (bold 13px magenta `#d55181`, centered near y=250):** "a majority of 5 is 3 — the two sides of a split can't both have it".
- **Caption (12px `#444`, bottom right):** "batch and duplicate counts illustrative".

## The Leader That Wasn't Dead

**Tags:** `common mistake` (red), `fencing` (orange), `epochs` (blue)

- **The pause** — W3 never crashed; a 5-second garbage-collection pause froze it from t=3s to t=8s
- **The return** — W3 wakes at t=8s still believing it is the leader; it never saw the election
- **Two leaders** — for a moment W3 (epoch 7) and W5 (epoch 8) both think they are in charge
- **The fence** — the invoice service remembers the highest epoch it has seen, and 8 beats 7
- **The rejection** — W3's batch arrives stamped epoch 7 and is refused; only epoch-8 batches pass

*Example (italic):* At t=9s the woken W3 submits the 500-invoice batch stamped epoch 7; the service, already at epoch 8, rejects it — zero duplicates.

**Common mistake:** Assuming a silent leader is a dead leader. A timeout cannot tell crashed from paused — so the receiver must check the epoch (the fencing token) instead of trusting whoever shows up claiming to be in charge.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the paused leader's stale batch with no fencing (accepted — duplicates) vs with an epoch check (rejected), shown as batch boxes flowing into the invoice service.

- **Title (bold 15px, `#1a5276`, top center):** "The Paused Leader Returns: the Epoch Number Is the Bouncer".
- **Row 1 (y=95), label 12px `#444` at x=20:** "no fencing"; blue `#2a78d6` rounded box at x=150 labeled "W3 batch — epoch 7" (12px), 3px arrow to a red `#e74c3c` box at x=400 labeled "service accepts anyone" with bold 12px red "✗ 500 duplicates".
- **Row 2 (y=205), label:** "with fencing"; blue box at x=150 "W3 batch — epoch 7", 3px arrow to a green `#008300` box at x=360 labeled "highest epoch seen: 8" with bold 12px red "✗ rejected" above the arrow tip; second green box at x=560 labeled "W5 batch — epoch 8 ✓" fed by a short 3px green arrow from below.
- **Box style:** 150–175px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "the token does the policing — the timeout only starts the election".
- **Caption (12px `#444`, bottom right):** "pause length and epochs illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the scenario numbers are invented and labeled illustrative — 5 workers, a 500-invoice daily batch, heartbeats at seconds `[1, 2, 3]`, crash at t=3s, a 3-missed-beat timeout declaring death at t=6s, a 3-of-5 majority vote for W5 by t=7s, epochs 7 → 8, W5 heartbeats at seconds `[7, 8, 9, 10, 11, 12]`, split-brain bars 500 vs 1,000 sends (500 duplicates), and a 5-second GC pause from t=3s to t=8s.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
