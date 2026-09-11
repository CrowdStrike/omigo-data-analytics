# Agent Skills

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Agent Skills

**Subtitle:** A folder of written instructions the agent loads only when the task matches — reusable know-how added without any retraining

## A Recipe Card, Not a Cooking Class

**Tags:** `core idea` (blue), `reusable instructions` (green)

- **The problem** — Alice keeps re-typing the same brand-voice rules into every writing request
- **The skill** — she writes the rules once into a named folder: instructions, examples, a checklist
- **The trigger** — each skill carries a one-line description saying when it applies
- **The load** — when a task matches, the agent opens the folder and follows the instructions
- **The win** — the know-how is written once, versioned like code, and shared with the whole team

*Example (italic):* A new hire doesn't retrain their brain for expense reports — they open the handbook page and follow it; a skill is that page for an agent.

**Key point:** A skill teaches by document, not by training — edit the file and the agent behaves differently on the very next task.

### Visualization (canvas `c1`, 720×300)

Flow diagram: a task arrives, the agent scans a catalog of one-line skill descriptions, one matches, its full instructions load, the task runs.

- **Title (bold 15px, `#1a5276`, top center):** "The Skill Loads Only When the Task Matches".
- **Task box:** rounded rect x=30, y=104, 130×56, fill `rgba(26,82,118,0.08)`, 2px `#1a5276` border; bold 12px `#1a5276` centered lines "task: write the" / "launch email" at y=128/146.
- **Catalog box:** rounded rect x=220, y=62, 200×150, fill `rgba(201,133,0,0.06)`, 2px `#c98500` border; bold 12px `#c98500` centered header "skill catalog (one-liners)" at (320, 84); four 11px `#2c3e50` left-aligned lines at x=236, y=106/126/146/166: "· brand-voice writing", "· sql-review", "· weekly-report", "· data-viz-style"; the first line drawn bold in `#008300` instead (the match) with a 1.5px `#008300` rounded rect 168×20 around it (x=232, y=92).
- **Skill folder box:** rounded rect x=490, y=62, 200×88, fill `rgba(0,131,0,0.07)`, 2px `#008300` border; bold 12px `#008300` centered "brand-voice writing" at (590, 84); 11px `#2c3e50` centered lines "full instructions," / "examples, checklist" at (590, 106/124); bold 11px `#008300` "loaded just now" at (590, 142).
- **Output box:** rounded rect x=490, y=176, 200×44, fill `rgba(42,120,214,0.07)`, 2px `#2a78d6` border; bold 12px `#2a78d6` centered "email in the house voice" at (590, 202).
- **Arrows (1.5px `#6b7280`, arrowheads):** task → catalog (160,132 → 214,132); catalog match → skill folder (420,102 → 484,102); skill folder → output (590,150 → 590,170).
- **Annotation (bold 12px orange `#d95926`, centered at y=254):** "the other skills stay closed — the agent read only their one-line descriptions".
- **Caption (11px `#444`, bottom right, y=292):** "catalog illustrative".

## Count the Words the Agent Must Read

**Tags:** `worked example` (blue), `progressive disclosure` (orange)

- **The setup** — the team agent has 20 skills, each about 500 words of instructions (illustrative)
- **Load everything** — 20 × 500 = 10,000 words crowding the context before any work starts
- **Load a catalog** — 20 one-line descriptions × 15 words = 300 words; that's all it reads at first
- **Load on match** — the one matching skill adds its 500 words: 300 + 500 = 800 total
- **The claim to check** — 800 vs 10,000 is a 92% smaller read for the same behavior

*Example (italic):* It's the difference between memorizing the whole employee handbook every morning and reading the table of contents plus today's one relevant page.

**Key point:** Descriptions are always in view, instructions load on demand — that's what keeps a large skill library from drowning the context window.

### Visualization (canvas `c2`, 720×300)

Bar chart: words the agent reads under "load all skills" vs "catalog + one match".

- **Title (bold 15px, `#1a5276`, top center):** "20 Skills: Words Read Before the Task (illustrative)".
- **Axes:** baseline y=230, plot top y=64; y = words 0–10,000 with `#e5e9ef` gridlines at 2,500 / 5,000 / 7,500 / 10,000 and 12px `#444` right-aligned tick labels ("2,500" etc.) at x=78; axis lines 1px `#999` from (84,64) to (84,230) to (660,230); scale 0.0166px per word.
- **Bars (110px wide, centered x = `[260, 500]`):**
  - "all 20 loaded" 10,000 words, fill `#d55181`; bold 13px `#d55181` value "10,000" above the bar.
  - "catalog + 1 match" 800 words, fill `#008300`; bold 13px `#008300` value "800" above; the 800 bar drawn as two stacked segments: bottom 300 (fill `#199e70`) and top 500 (fill `#008300`), with 11px right-side labels "300 catalog" (`#199e70`) and "500 one skill" (`#008300`) at x=565, aligned left.
- **X labels (12px `#444`, centered at y=250):** "all 20 loaded", "catalog + 1 match".
- **Annotation (bold 12px orange `#d95926`, centered at (372, 86)):** "92% less to read — the library scales, the context does not have to".
- **Caption (11px `#444`, bottom right, y=292):** "word counts illustrative".

## What Lives Inside a Skill

**Tags:** `where it's used` (blue), `no retraining` (green)

- **The folder** — a skill is just files: a main instruction file plus optional scripts and references
- **The header** — name and one-line description; this is the part the agent always sees
- **The body** — step-by-step instructions, rules, and worked examples for the task
- **The extras** — helper scripts the agent can run, reference docs it can consult mid-task
- **In the wild** — coding agents like Claude Code load skills this way; teams keep them in git

*Example (italic):* Bob's "sql-review" skill is one instruction file, a lint script, and the team's naming-convention doc — three files that outlive any single conversation.

**Key point:** Because a skill is plain files, it gets the whole software toolkit for free — diffs, reviews, versions, and rollbacks.

### Visualization (canvas `c3`, 720×300)

Anatomy diagram: a skill folder opened to show its parts, with the always-visible header highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "Anatomy of One Skill Folder".
- **Folder outline:** rounded rect x=60, y=52, 380×210, fill `rgba(26,82,118,0.04)`, 2px `#1a5276` border; bold 13px `#1a5276` left-aligned label "sql-review/" at (76, 76).
- **Part rows (rounded rects 330×44 at x=86, tops y = `[90, 144, 198]`):**
  - **Header row (green `#008300`, fill `rgba(0,131,0,0.08)`, border 2.5px):** bold 12px "name + one-line description" at (102, 108) left-aligned; 11px `#2c3e50` "always visible to the agent" at (102, 126).
  - **Instructions row (blue `#2a78d6`, fill `rgba(42,120,214,0.07)`):** bold 12px "instruction file — steps, rules, examples"; 11px "read when the skill triggers".
  - **Extras row (yellow `#c98500`, fill `rgba(201,133,0,0.07)`):** bold 12px "scripts & reference docs"; 11px "run or consulted mid-task".
- **Right-side notes (left-aligned at x=470):** bold 12px `#008300` "the hook" at y=108 with 11px `#6b7280` "costs ~15 words" at y=124; bold 12px `#2a78d6` "the know-how" at y=162 with 11px `#6b7280` "loads on demand" at y=178; bold 12px `#c98500` "the toolkit" at y=216 with 11px `#6b7280` "optional" at y=232.
- **Annotation (bold 12px orange `#d95926`, centered at y=282):** "plain files in git — edit, diff, review, and roll back like any other code".
- **Caption (11px `#444`, bottom right, y=297):** "structure simplified".

## A Skill Is Not a Tool, Not RAG, Not Fine-Tuning

**Tags:** `common mistake` (red), `know-how vs capability` (orange)

- **A tool** — a capability the agent can call, like "run SQL"; a skill says how and when to use it
- **RAG** — fetches facts to quote; a skill is a procedure to follow, not content to look up
- **Fine-tuning** — changes model weights with training runs; a skill changes nothing but files
- **The tell** — if you'd fix it by editing a document, it's skill territory; no GPU required
- **The trap** — reaching for fine-tuning to teach a five-rule style guide a skill states directly

*Example (italic):* "Always test before committing" is not a new capability, not a fact to retrieve, and not worth a training run — it is one line in a skill.

**Common mistake:** Calling every agent add-on a "skill" — capabilities are tools, knowledge is retrieval, behavior styles can be weights; a skill is written procedure.

### Visualization (canvas `c4`, 720×300)

Four-panel comparison: skill vs tool vs RAG vs fine-tuning, each with what it changes and when to reach for it.

- **Title (bold 15px, `#1a5276`, top center):** "Four Add-Ons People Mix Up".
- **Panels:** four rounded rects 320×92 in a 2×2 layout at (36, 52), (376, 52), (36, 158), (376, 158):
  - **Skill (green `#008300`, fill `rgba(0,131,0,0.07)`, border 2.5px):** bold 13px header "skill — written procedure" at panel center x, top +22; 11px `#2c3e50` centered lines "a document the agent follows" / "change it: edit a file" at +44/+62.
  - **Tool (blue `#2a78d6`, fill `rgba(42,120,214,0.06)`):** header "tool — callable capability"; lines "something the agent can do" / "change it: ship new code".
  - **RAG (yellow `#c98500`, fill `rgba(201,133,0,0.06)`):** header "RAG — knowledge lookup"; lines "facts fetched to answer with" / "change it: update the corpus".
  - **Fine-tuning (violet `#4a3aa7`, fill `rgba(74,58,167,0.06)`):** header "fine-tuning — weight change"; lines "behavior baked into the model" / "change it: run training again".
- **Annotation (bold 12px orange `#d95926`, centered at y=274):** "ask what you would edit to change the behavior — that names the mechanism".
- **Caption (11px `#444`, bottom right, y=294):** "boundaries simplified".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** the c2 arithmetic must read exactly 20 × 500 = 10,000 vs 300 + 500 = 800 to match the text; all values hardcoded, no randomness.
- **Content discipline:** Alice/Bob for people; product mentions limited to documented public behavior (Claude Code loads skills as folders of instructions); skill names like "brand-voice writing" and "sql-review" are illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
