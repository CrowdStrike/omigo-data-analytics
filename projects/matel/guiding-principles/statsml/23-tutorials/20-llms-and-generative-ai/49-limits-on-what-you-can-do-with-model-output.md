# Limits on What You Can Do with Model Output

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Limits on What You Can Do with Model Output

**Subtitle:** Vendor terms typically hand you the output and then narrow what you may build from it — training a rival model on it is the clause almost everyone trips over

## The Output Is Yours, Under Whichever Contract You Accepted

**Tags:** `core idea` (blue), `contract not copyright` (green), `per-endpoint` (orange)

- **Two clauses, not one** — terms typically carry both: one assigns you rights in the text, another lists uses you may not make
- **Contract, not copyright** — copyright asks who owns the words; the terms ask what you are permitted to do
- **Independent of ownership** — a use can be barred by contract even where copyright gives nobody a claim to enforce
- **Remedy and reach** — breach typically means lost access or indemnity, not an infringement suit; the accepting account is bound
- **Per-endpoint, not per-company** — the same weights reached four ways can carry four different sets of restrictions
- **Marketplace vs direct API** — a marketplace layers its terms on the owner's; direct API clauses are typically strictest
- **Chat plan vs open weights** — consumer terms are typically looser and may allow training on your inputs; weights ship a licence
- **Practical read** — always two questions: do I hold the rights, and does the contract allow this particular use?

*Example (italic):* The summaries a model returns can be owned by the team that requested them and still be barred from use as training data for a rival summarizer — both can be true at once.

**Key point:** Ownership and permission are separate questions — and the permission you hold depends on which endpoint's contract you accepted. It is common to assume output is yours to use freely; the terms of service can decide otherwise.

### Visualization (canvas `c1`, 720×300)

Top half: the three-stage grant → limits → breach flow. Bottom half: the same weights reached through four doorways, each with its own contract posture.

- **Title (bold 15px, `#1a5276`, top center):** "Rights Granted, Then Narrowed — By Whichever Contract You Accepted".
- **Three stage boxes:** rounded rects 200×82, r=6, at x = `[25, 260, 495]`, y=36. Each: 2px colored border, matching 0.07-alpha fill; bold 12.5px colored header centered at y=56; 11px `#2c3e50` centered lines at y=74/89; bold 11px colored foot at y=110.
  - **Stage 1 (green `#008300`, fill `rgba(0,131,0,0.07)`):** header "clause 1: the grant"; lines "output rights assigned", "to you by the vendor"; foot "you may use it".
  - **Stage 2 (magenta `#d55181`, fill `rgba(213,81,129,0.07)`):** header "clause 2: the limits"; lines "…except for these", "specific listed uses"; foot "the real constraint".
  - **Stage 3 (orange `#d95926`, fill `rgba(217,89,38,0.07)`):** header "if you breach"; lines "access suspended,", "indemnity lost"; foot "not a copyright suit".
- **Arrows:** 1.5px `#6b7280` horizontal at y=77 with arrowheads, from x=225→254 and x=460→489.
- **Annotation (bold 12px `#4a3aa7`, centered, y=138):** "copyright asks who owns the words — the contract asks what you may do with them".
- **Divider:** 1px `#e5e9ef` horizontal line at y=152 from x=25 to x=695.
- **Sub-header (bold 12px `#1a5276`, centered, y=170):** "the same weights, four doorways, four contracts".
- **Four doorway boxes:** rounded rects 160×72, r=6, at x = `[25, 195, 365, 535]`, y=182; 2px colored border, 0.07-alpha fill; bold 11.5px colored header centered at y=199; 11px `#2c3e50` centered lines at y=215/230; bold 11px colored foot at y=248.
  - blue `#2a78d6` "cloud marketplace": "marketplace terms +", "model owner terms"; foot "check both".
  - aqua `#199e70` "direct API": "strictest clauses,", "strongest data terms"; foot "read per model".
  - yellow `#c98500` "consumer chat plan": "looser terms; inputs", "may train the model"; foot "not for prod data".
  - violet `#4a3aa7` "open weights": "a licence, not a", "service contract"; foot "travels with file".
- **Annotation (bold 11.5px `#d95926`, centered, y=272):** "one terms page read once does not cover four doorways".
- **Caption (11px `#444`, bottom right, y=292):** "simplified; not legal advice".

## The Clauses Vendors Add — and How to Check Yours

**Tags:** `clause families` (blue), `where it's used` (orange), `checklist` (green)

- **No competing model** — the most commonly seen clause: don't use output to train or improve a rival model
- **No redistribution** — raw output usually may not be resold or published as a standalone dataset
- **Disclosure duties** — some terms require marking generated content or telling end users a model produced it
- **No reverse-engineering** — probing outputs to recover weights, architecture, or the system prompt is barred
- **Prohibited-use carve-outs** — medical, legal, and high-stakes automated decisions ride along with the output terms
- **Benchmark limits** — some terms restrict publishing head-to-head comparisons, and the limit varies by endpoint
- **Downstream reach** — whoever you pass the output to typically inherits the same list of restrictions
- **Version drift** — terms are revised per release, so an answer you checked last year may not hold today
- **The checklist** — per endpoint: who accepted, which document governs, which dated version, what it forbids

*Example (italic):* Vendor A permits commercial use of generated marketing copy but bars publishing the same text as a training corpus — the form of reuse decides, not the words.

**Key point:** Restrictions attach to the use, not the content — and to each endpoint's own dated terms, not one page read once.

### Visualization (canvas `c2`, 720×300)

Map of the recurring restriction families as a 3+3+1 box grid.

- **Title (bold 15px, `#1a5276`, top center):** "The Restriction Families That Recur Across Terms".
- **Six boxes:** rounded rects 210×56, r=6, at x = `[25, 255, 485]`, tops y = `[48, 116]`. Each: 2px colored border, matching 0.07-alpha fill, bold 12px colored header centered at y+22, 11px `#2c3e50` sub centered at y+41.
  - Row 1: magenta `#d55181` "no competing model" / "don't train a rival on the output"; blue `#2a78d6` "no redistribution" / "no reselling output as a dataset"; aqua `#199e70` "disclosure duties" / "mark content as AI-generated".
  - Row 2: violet `#4a3aa7` "no reverse-engineering" / "no probing for weights or prompt"; yellow `#c98500` "prohibited-use carve-outs" / "medical, legal, high-stakes calls"; orange `#d95926` "benchmark limits" / "publishing comparisons may need OK".
- **Wide box (row 3):** rounded rect x=25, y=184, 445×56, r=6, 2px `#1a5276`, fill `rgba(26,82,118,0.07)`; bold 12px `#1a5276` header "downstream inheritance" left-aligned at (41, 206); 11px `#2c3e50` at (41, 225): "whoever you pass the output to is bound by the same list".
- **Side note:** bold 11px `#6b7280` right-aligned at (705, 206): "check the dated version —"; and at (705, 225): "families vary by vendor".
- **Annotation (bold 12px `#d95926`, centered, y=266):** "the restriction attaches to the use, not to the words themselves".
- **Caption (11px `#444`, bottom right, y=292):** "illustrative; not legal advice".

## Training on Outputs: The Most Commonly Cited Restriction

**Tags:** `common mistake` (red), `distillation` (orange)

- **Why it exists** — a strong model's answers can teach a weaker one cheaply, transferring paid capability for free
- **The mechanism** — collect prompt-and-response pairs, fine-tune a small model on them, inherit much of the behaviour
- **Fine-tuning trap** — training an in-house model on logged assistant replies is the textbook restricted case
- **Synthetic-data trap** — generating a corpus to fine-tune anything else is usually reached by the same clause
- **Eval sets** — some terms allow purely internal evaluation, others treat any model-improving use as training
- **LLM-as-judge** — judge scores used to pick or tune checkpoints look like model improvement to the clause
- **"Competing" is theirs to define** — a narrow internal model may or may not count; the definition sits in the terms

*Example (italic):* Illustrative: fine-tuning a small in-house model on a year of logged assistant replies lowers inference cost, and is the activity such a clause typically names.

**Common mistake:** Assuming "internal only, not a product" exempts the work — many clauses restrict the training itself, not the selling of the result.

### Visualization (canvas `c3`, 720×300)

Four innocent-looking projects placed along a "clearly restricted → vendor's call" axis.

- **Title (bold 15px, `#1a5276`, top center):** "Four Ordinary Projects, One Clause".
- **Four chips:** rounded rects 250×34, r=17, centered on the given x, tops at the given y; 2px colored border, 0.08-alpha fill, bold 12px colored label centered at top+22.
  - magenta `#d55181` "fine-tune on logged replies", center x=175, top y=62.
  - orange `#d95926` "generate synthetic training data", center x=250, top y=104.
  - yellow `#c98500` "LLM-as-judge scoring dataset", center x=395, top y=146.
  - aqua `#199e70` "internal eval / regression set", center x=505, top y=188.
- **Axis:** 1.5px `#6b7280` horizontal line y=234 from x=60 to x=670, arrowheads at both ends; 11px `#444` "commonly read as restricted" left-aligned at (60, 252); "depends on the vendor's definition" right-aligned at (670, 252).
- **Dashed divider:** 1.5px `#6b7280` dashed vertical at x=430 from y=50 to y=230; bold 11px `#4a3aa7` centered at (430, 44): "the vendor defines this line".
- **Annotation (bold 12px `#d95926`, centered, y=278):** "each one logs output and then uses it to shape a model — exactly the clause's target".
- **Caption (11px `#444`, bottom right, y=296):** "illustrative; not legal advice".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`), matched to the sibling copyright page. Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then three `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas inventory:** exactly three canvases, `c1`, `c2`, `c3`, in that order, one per section, each intrinsic 720×300 — no `c4`. The `__charts` array registers three draw functions in section order (`c1` = grant/limits/breach flow plus the four-doorway row, `c2` = restriction-family grid, `c3` = four projects on the restricted axis).
- **Canvas mechanics:** shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`, logical size fixed at 720×300) and calls `ctx.scale` so drawing stays in logical coordinates. Draw functions are registered in the `__charts` array and re-run on window resize (debounced 150ms). Helpers `roundRect` and `arrowHead` as in the sibling page.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`.
- **Content discipline:** no vendor is named anywhere — use "Vendor A/B/C", "several major providers", "a common clause"; No named-actor scenarios; no legal conclusions, only "commonly", "typically", "check your terms"; the unsourced anecdote is labeled illustrative; every canvas carries a "not legal advice" caption. One bullet states explicitly that this is contract territory, distinct from copyright.
- **Data:** all three canvases are diagrams with fully hardcoded coordinates and labels — no generated data, no statistics, no `Math.random()` anywhere.
