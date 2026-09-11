# Copyright Complications

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Copyright Complications

**Subtitle:** Who owns model output, what training on scraped text means, and why memorized passages end up in court — three separate questions people constantly merge into one

## Three Questions Hiding in One Bedtime Story

**Tags:** `core idea` (blue), `three questions` (green)

- **The moment** — Alice asks a model for a bedtime story and the result closely echoes a published book
- **Question 1** — training: was it lawful to copy that book into the training set at all?
- **Question 2** — ownership: who, if anyone, owns the story the model just produced?
- **Question 3** — copying: is this particular output similar enough to the book to infringe?
- **Why it matters** — the three have different rules, different courts, and often different answers

*Example (italic):* "Is AI text legal?" is three questions wearing one trench coat — lawful training, ownable output, and non-copying output are independent.

**Key point:** Untangle which of the three questions is being asked before accepting any confident answer about AI and copyright.

### Visualization (canvas `c1`, 720×300)

Flow diagram: one generated story passing through three separate legal gates.

- **Title (bold 15px, `#1a5276`, top center):** "One Output, Three Separate Legal Gates".
- **Story box:** rounded rect x=30, y=110, 150×64, fill `rgba(26,82,118,0.08)`, 2px `#1a5276` border; bold 12px `#1a5276` centered two lines: "the generated" / "bedtime story" at y=136/154.
- **Three gate boxes:** rounded rects 150×110 at x = `[230, 400, 570−20→550]` — use lefts `[225, 395, 545]`, tops y=88:
  - **Gate 1 (blue `#2a78d6`, fill `rgba(42,120,214,0.07)`):** bold 12px header "gate 1: training" at top +20; 11px `#2c3e50` lines at +42/+58/+74: "was copying the book", "into the training set", "lawful?"; bold 11px `#2a78d6` at +96: "fought in court now".
  - **Gate 2 (aqua `#199e70`, fill `rgba(25,158,112,0.07)`):** header "gate 2: ownership"; lines "who owns the", "story that", "came out?"; bold 11px `#199e70` at +96: "maybe no one".
  - **Gate 3 (magenta `#d55181`, fill `rgba(213,81,129,0.07)`):** header "gate 3: copying"; lines "is this output too", "similar to the", "original book?"; bold 11px `#d55181` at +96: "checked per output".
- **Arrows:** 1.5px `#6b7280` from the story box to gate 1, gate 1 to gate 2, gate 2 to gate 3, at mid-height y=142, with arrowheads.
- **Annotation (bold 12px orange `#d95926`, centered at y=254):** "different rules, different courts — passing one gate says nothing about the others".
- **Caption (11px `#444`, bottom right, y=292):** "simplified; not legal advice".

## Gate 1: Was the Training Lawful?

**Tags:** `where it's fought` (blue), `training data` (orange)

- **The fact** — models train on huge scraped corpora that include copyrighted books and articles
- **Rights-holders say** — copying works into a training set requires permission or a license
- **Developers say** — training is transformative use of the text, not republishing it
- **Jurisdictions differ** — US courts weigh fair use case by case; the EU allows text-and-data mining with opt-outs; Japan has a broad allowance
- **Status** — actively litigated; some cases settle, some set precedents, few questions are fully closed

*Example (italic):* The same training run can be treated differently in three countries — the dataset doesn't change, the law around it does.

**Key point:** "Is training on scraped text legal?" has no single answer today — it depends on the country and, in the US, on ongoing case-by-case fair-use fights.

### Visualization (canvas `c2`, 720×300)

Three-column map of how major jurisdictions treat training on copyrighted text.

- **Title (bold 15px, `#1a5276`, top center):** "Same Training Data, Three Rulebooks (high level)".
- **Columns:** three rounded rects 210×170 at x = `[30, 255, 480]`, y=52; borders 2px `#2a78d6` / `#199e70` / `#4a3aa7`; fills `rgba(42,120,214,0.06)` / `rgba(25,158,112,0.06)` / `rgba(74,58,167,0.06)`; bold 13px colored headers centered at y=76: "United States", "European Union", "Japan".
  - **US lines (12px `#2c3e50`, centered, y=104/124/144):** "fair use argued", "case by case", "in active lawsuits"; bold 11px `#2a78d6` at y=176: "outcome: still being decided".
  - **EU lines:** "text & data mining", "allowed, but", "rights-holders can opt out"; bold 11px `#199e70` at y=176: "outcome: opt-out driven".
  - **Japan lines:** "broad allowance for", "data analysis uses,", "with limits"; bold 11px `#4a3aa7` at y=176: "outcome: most permissive".
- **Annotation (bold 12px orange `#d95926`, centered at y=258):** "one dataset, three answers — 'legal to train on' has no universal answer".
- **Caption (11px `#444`, bottom right, y=292):** "high-level sketch as of the mid-2020s; not legal advice".

## Gate 2: Who Owns What Comes Out?

**Tags:** `ownership` (blue), `human authorship` (green)

- **The US baseline** — copyright requires human authorship; purely machine-made work gets none
- **Prompt-only output** — typing a request is generally not authorship; the raw output may be owned by no one
- **Human shaping** — selecting, editing, and arranging outputs can earn protection for the human's contribution
- **Provider terms** — providers typically assign whatever interest they have in outputs to the user
- **Terms ≠ copyright** — a contract clause can't create a copyright the law says doesn't exist

*Example (italic):* Bob's prompt-only poem may be unownable by anyone, while the heavily reworked chapter Alice built from model drafts protects her edits and arrangement.

**Key point:** Ownership follows the human contribution — the more you shaped the output, the more there is that's yours.

### Visualization (canvas `c3`, 720×300)

Decision path from "who made it" to "who owns it" with three end states.

- **Title (bold 15px, `#1a5276`, top center):** "The Ownership Path (US baseline)".
- **Start box:** rounded rect x=40, y=110, 160×56, fill `rgba(26,82,118,0.08)`, 2px `#1a5276` border; bold 12px `#1a5276` centered two lines "how was the" / "output made?".
- **Three outcome rows (rounded rects 400×54 at x=280, tops y = `[56, 124, 192]`):**
  - **Row 1 (magenta `#d55181`, fill `rgba(213,81,129,0.07)`):** bold 12px header "machine alone (prompt-only)" at left x=296, y=78; 11px `#2c3e50` "no human authorship → possibly no copyright at all" at x=296, y=98 (left-aligned).
  - **Row 2 (yellow `#c98500`, fill `rgba(201,133,0,0.07)`):** header "human selects & edits"; line "the human's edits and arrangement are protectable".
  - **Row 3 (green `#008300`, fill `rgba(0,131,0,0.07)`):** header "human writes, model assists"; line "normal human authorship — the tool doesn't change it".
- **Arrows:** 1.5px `#6b7280` from the start box's right edge (200,138) fanning to each row's left edge with arrowheads.
- **Annotation (bold 12px orange `#d95926`, centered at y=272):** "the dial is human contribution — provider terms hand you their interest, not a new copyright".
- **Caption (11px `#444`, bottom right, y=294):** "US Copyright Office guidance, simplified; not legal advice".

## Gate 3: When the Model Quotes Its Homework

**Tags:** `common mistake` (red), `memorization` (orange)

- **It happens** — models can reproduce rare training passages verbatim, especially text repeated many times
- **Shown in research** — extraction studies have pulled memorized passages out of deployed models
- **Why it matters** — a verbatim passage in your output can infringe regardless of how it was produced
- **The defenses** — providers deduplicate training data and filter outputs; enterprises add overlap checks and indemnity clauses
- **The test is the same** — substantial similarity, judged exactly as if a human ghostwriter had handed you the text

*Example (illustrative):* A marketing team shipped copy containing two sentences later found nearly verbatim in a trade book — the takedown letter didn't care that a model wrote them.

**Common mistake:** Treating "the model wrote it" as a copyright defense — the output is judged by what it contains, not by who or what typed it.

### Visualization (canvas `c4`, 720×300)

Curve: how often a passage gets reproduced vs how many times it appeared in training, with the risk zone marked.

- **Title (bold 15px, `#1a5276`, top center):** "Seen Once vs Seen a Thousand Times (illustrative)".
- **Axes:** origin x=80, baseline y=225, plot right x=650, top y=70; x-axis label 12px `#444` "times the passage appeared in training data" centered at (365, 262); x tick labels 11px `#444` under the baseline at x = `[120, 250, 380, 510, 630]`: "1", "10", "100", "1,000", "10,000" (log-style spacing); y-axis label 12px `#444` rotated or placed at top-left (80, 58): "chance of verbatim reproduction".
- **Curve (2.5px `#d95926`):** rising S-curve through hardcoded points (x, y): (120, 218), (250, 212), (380, 190), (510, 140), (630, 88); smooth polyline; light fill `rgba(217,89,38,0.08)` down to the baseline.
- **Zone markers:** 11px `#008300` bold "rare text: almost never reproduced" at (215, 196); 11px `#e74c3c` bold "heavily repeated text: the risk zone" at (490, 118).
- **Dashed guide:** 1.5px `#6b7280` dashed vertical line at x=510 from baseline to the curve.
- **Annotation (bold 12px orange `#d95926`, centered at y=282):** "famous, oft-quoted passages are exactly the ones a model can quote back".
- **Caption (11px `#444`, bottom right, y=297):** "shape illustrative, based on published extraction research trends".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Content discipline:** neutral educational tone; no outcomes asserted for pending litigation; jurisdiction notes stay high-level; Alice/Bob for people; the marketing-team anecdote is labeled illustrative; every canvas that touches legal content carries a "not legal advice" or "illustrative" caption.
- **Data:** the c4 curve uses the five hardcoded points above; all positions and values fixed, no randomness.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
