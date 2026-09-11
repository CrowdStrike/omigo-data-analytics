# Do Your Chats Train the Model?

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Do Your Chats Train the Model?

**Subtitle:** Whether your conversation becomes training data depends on which door you came in through — consumer chat, business API, or enterprise contract

## Three Doors, Three Defaults

**Tags:** `core idea` (blue), `read the tier` (green)

- **The setup** — the same underlying model can be reached through three tiers whose data defaults differ
- **Door 1: consumer chat** — free and paid personal tiers may default to using chats for model improvement
- **Door 2: developer API** — business and API tiers typically do not train on submitted content by default
- **Door 3: enterprise contract** — a signed agreement can add a written no-training commitment plus retention limits
- **The same weights** — the model behind all three doors can be identical; only the paperwork differs
- **So the question changes** — "does it train on my data" is a question about the contract, not about the model
- **Per-account scope** — the tier and its default ride with the account, so a personal login at work is another door
- **Where to look** — the answer lives in the tier's terms and the account's data-controls page, not in the chat UI

*Example (italic):* The same draft pasted into a personal chat account and sent through a company API reaches the same model under two different data defaults.

**Key point:** Nothing about the model tells you whether your text is training material; the product tier you signed up under does. It is common to assume chats are private — the terms of service can decide otherwise.

### Visualization (canvas `c1`, 720×300)

Three-column tier-vs-default matrix: one shared model, three sets of defaults.

- **Title (bold 15px, `#1a5276`, top center):** "One Model, Three Doors, Three Defaults".
- **Shared model bar:** rounded rect x=30, y=46, 660×30, fill `rgba(26,82,118,0.08)`, 2px `#1a5276` border; bold 12px `#1a5276` centered at (360, 66): "the same underlying model sits behind all three doors".
- **Three columns:** rounded rects 210×150 at x = `[30, 255, 480]`, y=96; borders 2px `#d55181` / `#2a78d6` / `#008300`; fills `rgba(213,81,129,0.06)` / `rgba(42,120,214,0.06)` / `rgba(0,131,0,0.06)`; bold 13px colored headers centered at y=120:
  - **Col 1 (magenta `#d55181`):** header "consumer chat"; 12px `#2c3e50` centered lines at y=146/166/186: "personal free or paid", "improvement training", "often ON by default"; bold 11px `#d55181` at y=226: "you must opt out".
  - **Col 2 (blue `#2a78d6`):** header "developer API"; lines "business / build tier", "content typically NOT", "used for training"; bold 11px `#2a78d6` at y=226: "off by default".
  - **Col 3 (green `#008300`):** header "enterprise contract"; lines "negotiated agreement", "no-training promise", "plus retention limits"; bold 11px `#008300` at y=226: "written into the contract".
- **Annotation (bold 12px orange `#d95926`, centered at y=268):** "the model is the same — the door you entered through sets the default".
- **Caption (11px `#444`, bottom right, y=292):** "typical patterns, simplified; check your own terms".

## What "Improve Our Services" Actually Covers

**Tags:** `worked example` (blue), `terms of service` (orange), `not retroactive` (red)

- **The vague phrase** — "we may use your data to improve our services" bundles several distinct activities
- **Activity 1: training** — message content becomes examples that shape a future version of the model
- **Activity 2: human review** — sampled or flagged conversations can be read by reviewers for quality work
- **Activity 3: safety monitoring** — abuse and policy checks run on content regardless of training settings
- **Activity 4: retention** — content is stored for a fixed window even when training is switched off
- **Activity 5: telemetry** — counts, latencies, and error rates are aggregated separately from message text
- **Activity 6: feedback and vendors** — ratings and edits, plus hosting or moderation subprocessors, have own clauses
- **Not retroactive** — the switch governs the future; already-ingested text stays in, with no per-message undo
- **Delete is local** — clearing a chat from your history is not removal from every downstream store

*Example (italic):* Switching training off in March does not clear the retention window already running, and a model shipped in February has already seen January's chats.

**Key point:** Opting out of training usually does not opt you out of retention, safety review, or feedback-signal collection — and it never reaches backwards.

### Visualization (canvas `c2`, 720×300)

Fan-out: one vague phrase branching into six activities, with only the training branch greyed when training is off.

- **Title (bold 15px, `#1a5276`, top center):** "One Phrase, Six Different Activities".
- **Phrase box:** rounded rect x=24, y=112, 176×70, fill `rgba(26,82,118,0.08)`, 2px `#1a5276` border; bold 11px `#1a5276` centered three lines at y=134/150/166: "\"we may use your", "data to improve", "our services\"".
- **Six branch rows:** rounded rects 390×30 (radius 5) at x=290, tops y = `[40, 76, 112, 148, 184, 220]`.
  - Row 1 — "train future model versions", `#6b7280`, fill `rgba(107,114,128,0.10)`, **dashed** 1.5px border; right note bold 10px `#6b7280`: "OFF from now on".
  - Row 2 — "human review of sampled chats", violet `#4a3aa7`, fill `rgba(74,58,167,0.07)`; right note: "still on".
  - Row 3 — "abuse & safety monitoring", orange `#d95926`, fill `rgba(217,89,38,0.07)`; right note: "still on".
  - Row 4 — "retention for a fixed window", magenta `#d55181`, fill `rgba(213,81,129,0.07)`; right note: "still on".
  - Row 5 — "aggregate telemetry", aqua `#199e70`, fill `rgba(25,158,112,0.07)`; right note: "still on".
  - Row 6 — "feedback signals & subprocessors", blue `#2a78d6`, fill `rgba(42,120,214,0.07)`; right note: "own clause".
  - Row label: bold 12px in the row color, left-aligned at x=302, baseline top+20. Right note: right-aligned at x=672, baseline top+20, bold 10px in the row color.
- **Arrows:** 1.5px `#6b7280` from the phrase box right edge (200, 147) fanning to each row's left edge x=284 at top+15, with arrowheads.
- **Annotation line 1 (bold 12px orange `#d95926`, centered at y=258):** "the opt-out switch greys exactly one of the six branches".
- **Annotation line 2 (11px `#6b7280`, centered at y=276):** "and only going forward — text already ingested cannot be pulled back out".
- **Caption (11px `#444`, right-aligned at x=708, y=294):** "typical patterns, simplified; check your own terms".

## Why Memorization Makes This Concrete

**Tags:** `why it matters` (blue), `memorization` (orange), `rule of thumb` (green)

- **The mechanism** — training data can in principle be reproduced later by the trained model
- **Repetition is the driver** — a string seen thousands of times is far more likely to resurface than one seen once
- **Two risk classes** — one typed value is diffuse; 10,000 identical copies sits at the other end of the curve
- **One template, many times** — 500 people pasting the same block 20 times each is 10,000 copies of one string
- **Others' data rides along** — pasted client text, uploads, and connector output take exactly the same path
- **Redact first** — replace names, identifiers, and secrets with placeholders before the text leaves your machine
- **Pick the tier** — for anything sensitive, use the door whose contract says no training and bounded retention
- **Never credentials** — keys and passwords do not belong in a chat box under any tier or any setting

*Example (italic):* One prompt template containing an internal system description, pasted by 500 people 20 times each, is 500 × 20 = 10,000 identical occurrences.

**Key point:** The risk is not "my one message trains the model" — it is "our whole organisation pastes the same block until it becomes a pattern the model can learn."

### Visualization (canvas `c3`, 720×300)

Bar chart of occurrence counts, with every count computed from a people×pastes data array at render time.

- **Title (bold 15px, `#1a5276`, top center):** "Occurrences of One Pasted String (illustrative)".
- **Data array (hardcoded, three rows):** `[{label:'1 person, 1 paste', people:1, pastes:1, col:green}, {label:'5 people, 20 pastes', people:5, pastes:20, col:yellow}, {label:'500 people, 20 pastes', people:500, pastes:20, col:orange}]`. Occurrence count = `people * pastes` → 1, 100, 10,000 — computed in JS, never written as a literal.
- **Axes:** origin x=90, baseline y=228, plot top y=76; y-axis label 12px `#444` left-aligned at (66, 56): "occurrences of the same string (log scale)"; 1px `#e5e9ef` gridlines at the five decade positions `y = 228 − (l+1)/5 × 152` for `l = 0..4`, with 10px `#6b7280` right-aligned decade labels ("1", "10", "100", "1,000", "10,000") at x=84.
- **Log scale:** bar height `h = (log10(count) + 1) / 5 × 152` so counts 1 / 100 / 10,000 map to 1 / 3 / 5 decades; bar width 100, centred at x = `[180, 360, 540]`.
- **Bar colors:** `#008300`, `#c98500`, `#d95926`; fill at 0.18 alpha with a 2px solid stroke in the same hue.
- **Bar value labels:** bold 13px in the bar color, centered 8px above each bar top, printing `count.toLocaleString()` — so "1", "100", "10,000" all come from the arithmetic.
- **Category labels:** 12px `#2c3e50` centered under the baseline at y=246, using each row's `label`.
- **Arithmetic check line:** 11px `#6b7280` centered at (360, 264), built at render time from the third row as `people + " × " + pastes + " = " + count.toLocaleString() + " identical occurrences of one string"`.
- **Risk markers:** 11px bold `#008300` centered at (180, 190) "negligible"; 11px bold `#d95926` centered at (540, 66) "a learnable pattern".
- **Annotation (bold 12px orange `#d95926`, centered at y=284):** "one paste is noise; ten thousand identical pastes is a pattern".
- **Caption (11px `#444`, bottom right, y=297):** "illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`), copied from `48-copyright-complications.html`. Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then **three** `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Canvas inventory:** exactly three canvases, ids `c1`, `c2`, `c3` — one per section, in order, no gaps.
  - `c1` — "One Model, Three Doors, Three Defaults" (tier-vs-default matrix), draw function `// c1: one model, three doors, three defaults`.
  - `c2` — "One Phrase, Six Different Activities" (fan-out, training branch dashed/greyed), draw function `// c2: one phrase, six activities`.
  - `c3` — "Occurrences of One Pasted String (illustrative)" (log-scale bars from people×pastes), draw function `// c3: occurrences of one pasted string`.
  - The earlier opt-out timeline canvas has been **removed**; its lesson now lives in c2's second annotation line and in section 2's "Not retroactive" and "Delete is local" bullets.
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Bullet budget:** sections 1 and 3 carry eight bullets, section 2 carries nine (it absorbed the retroactivity material). Never more than nine in a section; merge two related facts into one ~100-char line rather than adding a tenth bullet, and never drop a fact to hit the count.
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Shared `roundRect` and `arrowHead` helpers as in the reference page.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`.
- **Data integrity:** no `Math.random()` anywhere; all chart data is hardcoded literal arrays. Every number printed next to generated data is computed at render time — c3's bar values and the "500 × 20 = 10,000" line are computed from the `people`/`pastes` fields and never written as literals.
- **Scope boundary:** this page owns the **tier-vs-default** question and the **decomposition of "improve our services"**. The operational leak paths (shadow use, relays, connectors, admin controls) belong to the sibling page `53-leaking-private-data-through-the-wrong-setting`, and retention windows and deletion mechanics belong to `54-data-retention-in-chat-apps`; both are touched here only where the training question requires it.
- **Content discipline:** **no vendor is named anywhere** — only "consumer chat", "developer API", "enterprise contract", and "Vendor A/B/C" if a placeholder is needed. No named-actor scenarios. Patterns are described as typical, never as a specific company's policy. No fake credential or token strings; secrets referred to generically. Every policy canvas carries a "typical patterns, simplified; check your own terms" or "illustrative" caption.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
