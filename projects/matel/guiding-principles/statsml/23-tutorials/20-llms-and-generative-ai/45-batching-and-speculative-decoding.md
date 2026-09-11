# Batching & Speculative Decoding

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Batching & Speculative Decoding

**Subtitle:** One step of a language model costs a GPU nearly the same for one user or sixteen — batching cashes that in as throughput for the operator, and speculative decoding uses a fast drafter plus a slow checker to cut each user's wait

## The Homework Bot's Half-Empty Van

**Tags:** `core idea` (blue), `batching` (green), `running example` (orange)

- **The bot** — a town library runs a homework-helper chatbot for students, all on one shared GPU
- **One word at a time** — the model writes each answer word by word; every word is one full pass, a "step"
- **The heavy part** — a step mostly hauls the model's weights through memory, like a van driving its route
- **Full or empty** — the van's route takes the same time with 1 package or 16: one step is 50 ms alone, 60 ms for sixteen
- **Batching** — advancing many users' answers by one word in the same step; the extra work rides almost free
- **The catch** — it is a shared ride: everyone moves together, and a request may wait for the van to load

*Example (italic):* At 4pm sixteen students ask questions at once; the GPU advances all sixteen answers by one word in a single 60 ms step — barely slower than the 50 ms it takes for one.

**Key point:** One step of the model costs nearly the same for 1 user or 16 — batching is the trick of never letting the van drive empty.

### Visualization (canvas `c1`, 720×300)

Two stacked horizontal-bar panels comparing one step at batch size 1 vs 16: the top panel shows step time barely growing, the bottom panel shows work done growing sixteen-fold.

- **Title (bold 15px, `#1a5276`, top center):** "One Step of the Model: Time vs Work Done".
- **Top panel:** heading bold 12px mute `#6b7280` at x=60, y=68: "time for one step (ms)"; bars start at x=230, scale 0 to 70 ms mapped to 420 px; rows at y=84 and y=116, 22px tall — "batch of 1" 50 ms (blue `#2a78d6`), "batch of 16" 60 ms (violet `#4a3aa7`); row names 12px `#444` right-aligned at x=222; bold 12px value labels "50 ms" / "60 ms" right of the bars.
- **Bottom panel:** heading bold 12px `#6b7280` at x=60, y=180: "answers advanced in that step"; same bar geometry at rows y=196 and y=228, scale 0 to 16 words — "batch of 1" 1 word (blue), "batch of 16" 16 words (violet); bold 12px value labels "1 word" / "16 words".
- **Annotation (bold 13px orange `#d95926`, near x=430, y=162):** "16x the work for 20% more time".
- **Caption (12px `#444`, bottom right):** "illustrative step times for the library's model".

## Filling the Van: 20 to 267 Words per Second

**Tags:** `worked example` (blue), `throughput vs latency` (green)

- **Step times** — measured (illustrative): batch 1 → 50 ms, 2 → 51 ms, 4 → 53 ms, 8 → 56 ms, 16 → 60 ms
- **Solo throughput** — 1 word / 0.050 s = 20 words per second, all of it for one student
- **Batched throughput** — 16 words / 0.060 s = 267 words per second across all sixteen students
- **Per-student speed** — 1 / 0.060 s = 16.7 words/s each; only about 17% slower than having the GPU alone
- **The trade** — 13x total output for the library, at a small per-student slowdown plus time queued for a seat

*Example (italic):* The librarian's dashboard shows the same GPU doing 20 words/s when one student is on, and 267 words/s at the 4pm rush — redo it: 16 / 0.060 = 267.

**Key point:** Batching buys throughput (the GPU's total words/s) at a small cost in latency (each user's words/s) — the operator's favorite trade, paid for in user wait.

### Visualization (canvas `c2`, 720×300)

Two side-by-side panels computed from the step times in the bullets: left, total words/s bars climbing with batch size; right, per-user words/s line barely dipping.

- **Title (bold 15px, `#1a5276`, top center):** "Throughput Climbs 13x, Per-User Speed Barely Drops".
- **Left panel (plot x=60–360, baseline y=240, height 160):** vertical bars for batch sizes `[1, 2, 4, 8, 16]`, total words/s `[20, 39, 75, 143, 267]`; scale 0 to 280 with light `#e5e9ef` gridlines at 50, 100, 150, 200, 250 and 12px `#444` y tick labels; bars 40px wide, fill `rgba(42,120,214,0.35)`, 2px `#2a78d6` stroke; bold 12px blue value labels above each bar; batch sizes as 12px `#444` labels below the baseline; panel heading bold 12px `#6b7280`: "total words/s (all users)".
- **Right panel (plot x=420–690, baseline y=240, height 160):** line chart over the same batch sizes, per-user words/s `[20.0, 19.6, 18.9, 17.9, 16.7]`; scale 0 to 24, gridlines at 5, 10, 15, 20; 2px orange `#d95926` line with 6px orange dots; bold 12px orange labels on the end points "20.0" and "16.7"; panel heading bold 12px `#6b7280`: "words/s each user sees".
- **Annotations:** bold 13px green `#008300` in the left panel near y=70: "13x total"; bold 12px orange in the right panel near y=90: "only ~17% slower each".
- **Caption (12px `#444`, bottom right):** "illustrative — computed from step times 50 → 60 ms".

## The Intern Drafts, the Expert Approves

**Tags:** `core idea` (blue), `speculative decoding` (green), `worked example` (orange)

- **Still slow** — even alone, a student watches words land every 50 ms; the big model cannot step faster
- **The intern** — a tiny draft model guesses likely next words at 5 ms each, and easy words it gets right
- **Draft four** — the intern writes 4 words ahead in 4 × 5 = 20 ms while the expert would still be on word one
- **One check** — the big model verifies all 4 guesses in a single 50 ms pass and keeps the correct prefix
- **The math** — 3 of 4 survive in this simple telling: 3 per 70 ms ≈ 43 words/s vs 20 alone — 2.1x faster
- **Same words** — every kept word is exactly what the big model would have written; only the clock changes

*Example (italic):* In "The capital of France is Paris", the intern nails the easy words and the expert only slows things down where the sentence actually gets hard.

**Key point:** Speculative decoding = cheap guesses, one expensive check. The output is identical to the big model's; the wait per word drops from 50 ms to about 23 ms — and real systems do a little better still, since the verify pass also yields its own corrected word.

### Visualization (canvas `c3`, 720×300)

Two horizontal timelines over the same 210 ms of wall clock: the big model alone producing 4 words, versus draft-then-verify cycles producing 9.

- **Title (bold 15px, `#1a5276`, top center):** "Same 210 ms of Wall Clock: Expert Alone vs Draft-then-Verify".
- **Time axis:** horizontal 2px `#999` line at y=250 from x=120 to x=680 (560 px = 210 ms); 12px `#444` tick labels "0", "50", "100", "150", "200 ms" every 50 ms.
- **Row 1 (blocks centered on y=105, 30px tall; 12px `#444` label "big model alone" at x=15):** four consecutive 50-ms blocks (each ≈133 px), fill `rgba(42,120,214,0.35)`, 2px `#2a78d6` stroke, bold 12px blue centered labels "word 1" … "word 4"; row ends at 200 ms.
- **Row 2 (blocks centered on y=185, 30px tall; label "draft + verify"):** three consecutive 70-ms cycles; each cycle = four 5-ms draft ticks (≈13 px each, fill aqua `#199e70`) then one 50-ms verify block (fill `rgba(74,58,167,0.35)`, 2px violet `#4a3aa7` stroke, 12px violet centered label "verify"); under each cycle a bold 12px green `#008300` label "+3 words" and an 11px `#6b7280` note "1 guess tossed" under the first cycle only.
- **Row totals (bold 13px, near x=688):** blue "4 words" beside row 1, green "9 words" just above row 2.
- **Annotation (bold 13px green `#008300`, near x=330, y=60):** "9 words vs 4 in the same clock — and the text is identical".
- **Caption (12px `#444`, bottom right):** "illustrative — expert step 50 ms, drafter 5 ms/word, 3 of 4 guesses kept".

## Two Levers, Two Beneficiaries

**Tags:** `common mistake` (red), `where it's used` (blue)

- **Different levers** — batching raises the GPU's total words/s; speculation cuts one user's wait per word
- **Different pockets** — batching shrinks the operator's per-answer cost; speculation improves the user's stream
- **Mistake #1** — "a bigger batch speeds up my answer": no — each user runs slightly slower and may queue first
- **Mistake #2** — "speculative decoding degrades answers": no — the big model approves every word it keeps
- **Together** — real servers do both: continuous batching keeps every step full, drafting keeps streams snappy

*Example (italic):* At peak hour the library's server batches sixteen students per step for cost, while speculation keeps each answer streaming at a readable pace.

**Common mistake:** Reporting "tokens per second" without saying whose — the GPU's total (throughput, the batching lever) or one user's stream (latency, the speculation lever). The same phrase names two different numbers.

### Visualization (canvas `c4`, 720×300)

Two mini rows with independent scales, each showing a before-to-after arrow: batching moving the operator's throughput number, speculation moving the user's wait-per-word number.

- **Title (bold 15px, `#1a5276`, top center):** "Two Levers: Who Gets Faster?".
- **Row 1 (y=110; 12px `#444` label at x=20, two lines):** "operator's lever — batching:" / "total words/s per GPU"; thin 2px `#999` axis segment at y=128 from x=280 to x=680, scale 0 to 280 with 11px `#6b7280` end labels "0" and "280 words/s"; blue `#2a78d6` 7px dot at 20, 3px blue arrow with arrowhead to a green `#008300` 7px dot at 267; bold 12px labels above the dots: blue "20", green "267".
- **Row 2 (y=200; label two lines):** "user's lever — speculation:" / "wait per word (ms)"; same axis geometry at y=218, scale 0 to 60 ms, end labels "0" and "60 ms"; blue 7px dot at 50, 3px blue leftward arrow to a green 7px dot at 23; bold 12px labels: blue "50 ms", green "23 ms".
- **Row notes (11px `#6b7280`, below each axis):** row 1: "each user slightly slower, may queue for a seat"; row 2: "same words, sooner".
- **Annotation (bold 13px magenta `#d55181`, centered near y=282):** "batching pays the operator, speculation pays the user — say which lever you pulled".
- **Caption (12px `#444`, bottom right):** "numbers from the worked examples above — illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, violet `#4a3aa7`, orange `#d95926`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar heights, line points, timeline blocks, and arrow endpoints are the hardcoded literal values above (no randomness); `c2` values follow exactly from the step-time list (total = batch/step, per-user = 1/step); `c3` and `c4` reuse 50 ms, 60 ms, 20 ms draft, 70 ms cycle, 43 vs 20 words/s, 23 ms/word so text and charts match.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
