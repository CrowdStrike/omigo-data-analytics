# Partial Failures

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Partial Failures

**Subtitle:** On one machine a crash stops everything, so you always know; across two machines any step can half-work — and the caller cannot tell which half

## The Payment That Timed Out

**Tags:** `core idea` (blue), `two machines` (green), `uncertainty` (orange)

- **The request** — a coffee shop's card terminal sends a $4.50 charge to a payment server
- **The wait** — the terminal waits 5 seconds for "approved", hears nothing, and gives up
- **The question** — did the charge fail before the card was charged, or after? Nobody at the till knows
- **One machine** — if the till itself crashes, everything stops together; the failure is total and obvious
- **Two machines** — the terminal, the network, and the server can each fail alone while the others keep going
- **The definition** — a partial failure is when some parts of a system fail while the rest keeps running

*Example (italic):* The barista sees "timeout" on the terminal at 8:14am and has no way to know whether the customer's card was charged $4.50 or not.

**Key point:** The moment a program spans two machines, failure stops being all-or-nothing — pieces fail independently, and the surviving pieces can't see which piece died.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: one machine (crash stops everything, failure is visible) vs two machines (three independent failure points, failure is invisible to the caller).

- **Title (bold 15px, `#1a5276`, top center):** "One Machine Fails Whole; Two Machines Fail in Pieces".
- **Row 1 (y=105), label 12px `#444` at x=20:** "one machine"; a single blue `#2a78d6` rounded box at x=170, width 300, labeled "till + charge logic (one process)" (12px); bold 14px red `#e74c3c` "✗ crash" centered on the box; 12px green `#008300` label at x=530: "everything stops — you know".
- **Row 2 (y=215), label 12px `#444` at x=20:** "two machines"; blue rounded box at x=150, width 130, labeled "card terminal"; dashed 2px `#6b7280` network line (dash 5/4) from x=290 to x=430 with 11px `#6b7280` label "network" above; aqua `#199e70` rounded box at x=440, width 150, labeled "payment server"; three small bold 13px red `#e74c3c` "✗" marks at x≈300 (network out), x≈400 (network back), x≈510 (server), each with an 11px `#6b7280` tick below.
- **Box style:** 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` and `rgba(25,158,112,0.15)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=272):** "three places to die, one identical silence at the terminal".
- **Caption (12px `#444`, bottom right):** "diagram schematic".

## Three Ways One Request Can Die

**Tags:** `worked example` (blue), `failure points` (orange), `same symptom` (red)

- **The path** — one charge is three steps: send the request, server charges the card, send the reply
- **Point A: send lost** — the network drops the request; the server never hears; card NOT charged
- **Point B: crash mid-work** — the server dies while processing; the card MAYBE charged, maybe not
- **Point C: reply lost** — the server charges the card, but "approved" is dropped; card IS charged
- **The symptom** — in all three cases the terminal sees exactly the same thing: 5 seconds of silence
- **Hand-check** — walk the path yourself: A = not charged, B = unknown, C = charged; caller view identical

*Example (italic):* Order #218 times out at 5s; the true state is "not charged" (A), "unknown" (B), or "charged $4.50" (C) — and the terminal's screen looks identical in all three.

**Key point:** Three different truths — not charged, unknown, charged — collapse into one indistinguishable observation at the caller: a timeout.

### Visualization (canvas `c2`, 720×300)

Three-lane flow diagram: the same send → charge → reply pipeline drawn three times, with the red X at a different stage in each lane, and an identical "caller sees" label on every lane.

- **Title (bold 15px, `#1a5276`, top center):** "Three Different Deaths, One Identical Timeout".
- **Lanes (y = 85, 160, 235), each with a left-aligned 12px `#444` label at x=20:** "A: send lost", "B: crash mid-work", "C: reply lost".
- **Each lane:** three rounded boxes 110px wide, 34px tall, 8px radius at x=140 ("send", fill `rgba(42,120,214,0.15)`, edge `#2a78d6`), x=300 ("charge $4.50", fill `rgba(25,158,112,0.15)`, edge `#199e70`), x=460 ("reply", fill `rgba(74,58,167,0.12)`, edge `#4a3aa7`), joined by 2px `#6b7280` arrows; 12px `#2c3e50` box text.
- **Failure marks (bold 15px red `#e74c3c`):** lane A: "✗" on the send→charge arrow (x≈275); lane B: "✗" on the charge box (x≈355); lane C: "✗" on the charge→reply arrow (x≈435); boxes downstream of the ✗ drawn with `#e5e9ef` edges and `#6b7280` text (never reached).
- **Card-state labels (11px, right of reply box at x≈580, above the caller label):** lane A green `#008300` "not charged", lane B yellow `#c98500` "unknown", lane C orange `#d95926` "charged".
- **Caller labels (bold 12px red `#e74c3c`, x=590 on each lane):** "sees: timeout" — identical text on all three lanes.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=278):** "the caller's evidence is the same in all three worlds".
- **Caption (12px `#444`, bottom right):** "order #218 illustrative".

## Retries Without Double Charges

**Tags:** `where it's used` (blue), `retries` (green), `idempotency` (orange)

- **The instinct** — timeouts get retried; every payment terminal resends the charge after silence
- **The trap** — of 100 timed-out charges (illustrative), 38 were case C: already charged, reply lost
- **Blind retry** — resending all 100 charges again double-charges those 38 customers
- **Idempotency key** — the terminal sends a unique id (`pay-218`) with the charge, same id on retry
- **The dedupe** — the server remembers ids it has processed and answers repeats without charging again
- **The lesson** — this is why distributed programming is qualitatively harder: uncertainty, not speed

*Example (italic):* Retrying 100 timeouts blindly creates 38 double charges; retrying the same 100 with idempotency keys creates 0, because the server recognizes `pay-218` the second time.

**Key point:** You can't eliminate the uncertainty, so you make repeats harmless — an idempotency key turns "charge the card" into "charge the card at most once for this id", making retries safe.

### Visualization (canvas `c3`, 720×300)

Grouped vertical bar chart: 100 timed-out charges retried two ways — blind retry vs retry with idempotency key — showing customers double-charged.

- **Title (bold 15px, `#1a5276`, top center):** "Retrying 100 Timeouts: Blind vs With an Idempotency Key".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 180; y = customers double-charged 0 to 40, gridlines `#e5e9ef` at 10/20/30/40 with 12px `#444` tick labels; 2px `#999` baseline.
- **Bars (90px wide):** "blind retry" centered at x=220, height for value 38, fill red `#e74c3c` at `rgba(231,76,60,0.75)`, bold 13px red value label "38 double-charged" above the bar; "with idempotency key" centered at x=470, value 0 drawn as a 3px green `#008300` stub on the baseline, bold 13px green value label "0 double-charged" above it.
- **Bar labels:** 12px `#444` centered under each bar at y=265.
- **Side note (11px `#6b7280`, top right):** "38 of 100 timeouts had actually charged (reply lost)".
- **Annotation (bold 13px green `#008300`, near x=470, y=110):** "same 100 retries — the key makes them harmless".
- **Caption (12px `#444`, bottom right):** "counts illustrative".

## A Timeout Is Not a "No"

**Tags:** `common mistake` (red), `timeouts` (orange)

- **The mistake** — reading a timeout as "the charge didn't happen" and acting on that guess
- **What it really means** — a timeout only says "no reply within 5s"; it says nothing about the card
- **The tally** — of 200 timeouts in a day (illustrative), 120 never charged but 80 actually did
- **The fallout** — treat all 200 as "didn't happen" and you resend 80 charges that already landed
- **The honest state** — after a timeout the only correct label is "unknown", until you ask the server
- **The fix** — retry with an idempotency key, or query the server for `pay-218` before acting

*Example (italic):* A report that counts all 200 timeouts as failed payments is wrong about 80 of them — those customers were charged and will call about duplicates.

**Common mistake:** Treating a timeout as a negative answer. A timeout is the absence of an answer — the real outcome is unknown, and 80 of these 200 "failures" actually succeeded.

### Visualization (canvas `c4`, 720×300)

Two horizontal stacked bars comparing the assumption ("timeout = not charged") with the reality (200 timeouts split into not-charged and charged).

- **Title (bold 15px, `#1a5276`, top center):** "200 Timeouts: What Teams Assume vs What Actually Happened".
- **Layout:** bars start at x=230, max width 440 (= 200 timeouts), 26px tall, left-aligned 12px `#444` row labels at x=20.
- **Row 1 (y=100), label "the assumption":** single blue bar, fill `rgba(42,120,214,0.30)`, edge `#2a78d6`, width 440, 12px `#2c3e50` centered label "all 200 not charged".
- **Row 2 (y=185), label "the reality":** blue segment fill `rgba(42,120,214,0.30)` width 264 labeled "120 not charged" (12px `#2c3e50`), then orange segment fill `rgba(217,89,38,0.55)`, edge `#d95926`, width 176 labeled "80 charged" (bold 12px white).
- **Scale ticks:** 11px `#6b7280` labels "0", "100", "200" under the bars at x=230, 450, 670, with 1px `#e5e9ef` vertical guides.
- **Annotation (bold 13px magenta `#d55181`, near x=420, y=250):** "80 'failures' were successes wearing a timeout".
- **Caption (12px `#444`, bottom right):** "split illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded numbers above (no randomness); the $4.50 charge, order #218, the 5-second timeout, the 38-of-100 already-charged retries, and the 120/80 split of 200 timeouts are invented and labeled illustrative; the three failure points (send lost / crash mid-work / reply lost) and their card states (not charged / unknown / charged) are the exact enumeration used in both text and charts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
