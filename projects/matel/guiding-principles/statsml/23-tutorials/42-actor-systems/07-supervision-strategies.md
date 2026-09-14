# Supervision Strategies

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Supervision Strategies

**Subtitle:** When a child actor crashes, its parent picks the fix — resume, restart, stop, or escalate — instead of the child defending itself with try/catch everywhere

## A Poisoned Refund Kills Worker 3

**Tags:** `core idea` (blue), `parent decides` (green), `actors` (orange)

- **The pool** — a supervisor actor owns 8 payment workers, each buffering a few in-flight payments
- **The crash** — at 10:04 a refund with a negative amount makes worker 3 throw and die
- **The split** — the worker never handles its own failure; its supervisor decides what happens next
- **Four directives** — resume (keep state), restart (wipe state), stop (kill for good), escalate (pass up)
- **The default** — classic Akka and Erlang restart: fresh state, same mailbox (Akka Typed stops)

*Example (italic):* At 10:04 worker 3 dies on the bad refund; the supervisor answers with one word — restart — and a fresh worker 3 is taking messages again 40ms later.

**Key point:** In an actor system, error handling lives in the parent, not in the failing code — the supervisor picks resume, restart, stop, or escalate for every child crash.

### Visualization (canvas `c1`, 720×300)

Flow diagram: the crashed worker reports to its supervisor, which fans out to the four possible directives.

- **Title (bold 15px, `#1a5276`, top center):** "One Crash, Four Possible Answers from the Supervisor".
- **Crashed worker box:** red-tinted rounded box at x=25, y=140, 145×44, fill `rgba(231,76,60,0.12)`, 2px `#e74c3c` border, 12px `#2c3e50` two-line text "worker 3 / crashes at 10:04".
- **Supervisor box:** blue rounded box at x=235, y=140, 150×44, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 12px text "supervisor / decides"; 3px `#6b7280` arrow from crashed box to it, 11px `#6b7280` label "failure signal" above the arrow.
- **Directive boxes (each 235×38, rounded 8px, at x=460, y = 50 / 115 / 180 / 245), with 2px arrows fanning from the supervisor's right edge:**
  - green `#008300` border, fill `rgba(0,131,0,0.12)`: "Resume — keep state, skip bad msg"
  - blue `#2a78d6` border, fill `rgba(42,120,214,0.15)`: "Restart — wipe state, keep mailbox"
  - orange `#d95926` border, fill `rgba(217,89,38,0.12)`: "Stop — remove the worker for good"
  - violet `#4a3aa7` border, fill `rgba(74,58,167,0.10)`: "Escalate — supervisor's parent decides"
- **Box text:** 12px `#2c3e50`, directive name in bold.
- **Annotation (bold 13px `#1a5276`, bottom left near y=285):** "the child fails; the parent chooses".

## One-for-One or All-for-One

**Tags:** `worked example` (blue), `blast radius` (green)

- **The buffers** — the 8 workers hold [3, 2, 4, 5, 1, 4, 3, 5] in-flight payments — 27 in total
- **One-for-one** — the restart directive hits worker 3 alone; its 4 buffered payments are wiped
- **The survivors** — workers 1, 2, 4–8 keep running; their 23 buffered payments are untouched
- **All-for-one** — the same directive hits all 8 siblings; all 27 buffered payments reset to 0
- **Hand-check** — one-for-one keeps 27 − 4 = 23 payments; all-for-one keeps 27 − 27 = 0
- **When all-for-one** — choose it when siblings share state so tightly that one clean sibling is a lie

*Example (italic):* The negative refund kills worker 3: one-for-one wipes 4 payments and keeps 23; all-for-one wipes all 27 so the pool restarts in one consistent state.

**Key point:** The strategy sets the blast radius — one-for-one restarts only the failing child; all-for-one restarts every sibling because their states depend on each other.

### Visualization (canvas `c2`, 720×300)

Two rows of 8 worker squares showing which workers restart under each strategy, with buffer counts before → after.

- **Title (bold 15px, `#1a5276`, top center):** "Blast Radius: One-for-One Restarts 1 Worker, All-for-One Restarts 8".
- **Grid:** 8 squares per row, each 56×44, rounded 6px, first at x=150, horizontal gap 14 (last square ends at x=696); row 1 top at y=75, row 2 top at y=185; row labels 12px `#444` at x=20, vertically centered: "one-for-one" and "all-for-one".
- **Row 1 (one-for-one), buffers `[3, 2, 4, 5, 1, 4, 3, 5]`:** worker 3 orange `#d95926` border, fill `rgba(217,89,38,0.15)`, bold 12px orange text "4→0"; the other 7 squares green `#008300` border, fill `rgba(0,131,0,0.10)`, 12px `#2c3e50` text showing their kept count ("3", "2", "5", "1", "4", "3", "5").
- **Row 2 (all-for-one):** all 8 squares orange border and fill as above, each with bold 12px orange "→0" ("3→0", "2→0", "4→0", "5→0", "1→0", "4→0", "3→0", "5→0").
- **Worker numbers:** 11px `#6b7280` "w1"–"w8" centered above each column at y=65.
- **Annotation (bold 13px `#1a5276`, centered near y=265):** "one-for-one keeps 23 buffered payments; all-for-one keeps 0".
- **Caption (12px `#444`, bottom right):** "buffer counts illustrative".

## Why Let It Crash Beats Try/Catch Everywhere

**Tags:** `why it matters` (blue), `let it crash` (green), `Erlang` (orange)

- **The old reflex** — wrap every handler in try/catch, log the error, and keep the worker running
- **The hidden cost** — a swallowed exception can leave a half-updated balance no log line reveals
- **Let it crash** — write only the happy path; anything unexpected kills the worker on purpose
- **The recovery** — supervision restarts it to a known-good empty state in about 40ms
- **The tell** — in the chart, 120 correct payments/min silently becomes 84 after the swallowed throw
- **Error kernel** — keep precious state high in the tree, risky work in cheap restartable leaves

*Example (italic):* The try/catch pool keeps running after 10:04 but quietly mis-handles 36 payments every minute; the supervised pool loses 40ms and nothing else.

**Key point:** "Let it crash" trades tiny, visible restarts for the elimination of a worse failure mode — a worker that survives its own bug and keeps computing on corrupted state.

### Visualization (canvas `c3`, 720×300)

Line chart of correct payments per minute from 10:00 to 10:10: try/catch degrades silently after the bug, supervision dips briefly and recovers.

- **Title (bold 15px, `#1a5276`, top center):** "After the Bad Refund: Silent Degradation vs a 40ms Restart".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = time "10:00" to "10:10" with 12px `#444` tick labels every 2 minutes; y = correct payments/min 0 to 140, gridlines `#e5e9ef` at 40/80/120 with 12px `#444` labels.
- **Try/catch line:** red `#e74c3c` 3px line through minutes `[0, 2, 4, 4.2, 6, 8, 10]`, values `[120, 120, 120, 84, 84, 84, 84]` — steps down at 10:04 and never recovers.
- **Supervised line:** green `#008300` 3px line through the same minutes, values `[120, 120, 120, 118, 120, 120, 120]` — barely a dent.
- **Crash marker:** vertical dashed `#6b7280` (dash 4/3) line at minute 4, 12px `#6b7280` label "bad refund arrives" at its top.
- **Annotation (bold 13px red `#e74c3c`, near minute 7, y=150):** "36 wrong payments every minute, no alarm".
- **Annotation (bold 12px green `#008300`, near minute 6, y=85):** "restarted in 40ms".
- **Caption (12px `#444`, bottom right):** "rates illustrative".

## Resume Is Not a Safe Default

**Tags:** `common mistake` (red), `corrupted state` (orange)

- **The temptation** — resume looks kind: no lost buffer, no restart, the worker just keeps going
- **The trap** — the crash happened mid-update; resuming keeps a balance that is already half-written
- **The rule** — resume only when the failure provably never touched state (bad input rejected up front)
- **Restart storms** — a poisoned message re-crashes the fresh worker; cap restarts (10 per 60s), then stop
- **Escalate honestly** — if the parent cannot pick a safe directive, pass the failure up; don't guess

*Example (italic):* Resuming worker 3 after its mid-update crash leaves one refund counted once in the ledger and twice in the daily total; restart would have cost only 4 buffered payments.

**Common mistake:** Reaching for Resume because Restart loses state. Restart is the safe default — Resume is only correct when you can prove the failure left the worker's state untouched.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: resuming after a mid-update crash carries the corruption forward; restarting wipes it and replays cleanly from the event log.

- **Title (bold 15px, `#1a5276`, top center):** "Resume Keeps the Bug's Leftovers; Restart Throws Them Away".
- **Row 1 (y=95), label 12px `#444` at x=20:** "resume"; blue `#2a78d6` rounded box at x=140 labeled "crash mid-update / {ledger 1, total 2}" (12px, two lines), 3px arrow to a red `#e74c3c` box at x=420 labeled "resume: corrupted / total survives" with bold 12px red "✗ every later report is wrong" to its right/below.
- **Row 2 (y=205), label:** "restart"; blue box at x=140 "crash mid-update / {ledger 1, total 2}", 3px arrow to a green `#008300` box at x=370 labeled "restart: state / wiped to {0, 0}", then arrow to a green box at x=570 labeled "replay from log: / {ledger 1, total 1}" with bold 12px green "✓"; muted 11px `#6b7280` note at (140, 193): "replay needs persistence — the failed message is not retried by default".
- **Box style:** 150–170px wide, 44px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=275):** "restart costs 4 buffered payments; resume can cost the ledger".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); worker buffer counts `[3, 2, 4, 5, 1, 4, 3, 5]` (sum 27), the 40ms restart, and the 120→84 correct-payments rates are invented and labeled illustrative; the 23-kept / 0-kept totals in c2 must match the bullet arithmetic (27 − 4 and 27 − 27).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
