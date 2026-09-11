# Next-Token Prediction

**Page type:** detail page (tutorial layout: `.card-section` blocks, each a two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Next-Token Prediction

**Subtitle:** Everything a language model does is one trick, repeated: given the text so far, put a probability on every possible next piece — then add one and go again

## "The capital of France is ___" — a Score for Every Candidate

**Tags:** `core idea` (blue), `running example` (green)

- **The setup** — the model reads "The capital of France is" and must propose what comes next
- **It scores everything** — every token in its vocabulary gets a probability, even silly ones
- **The table** — Paris 0.92, Lyon 0.03, "the" 0.02, Marseille 0.01, everything else 0.02
- **Sums to 1** — 0.92 + 0.03 + 0.02 + 0.01 + 0.02 = 1.00: one unit of belief, shared out
- **One is chosen** — picked by a weighted draw: often, but not always, the most likely

*Example:* Autocomplete on your phone plays exactly this game — with a much smaller brain.

**Key point:** An LLM's one and only skill is producing this table: a probability for every possible next token.

### Visualization (canvas `c1`, 720×300)

Horizontal probability bar chart of the candidate table.

- **Title (bold 15px, `#1a5276`, top center):** '"The capital of France is ___" — the Probability Table'.
- **Data:** candidates `['Paris', 'Lyon', '"the"', 'Marseille', 'all others']` with probabilities `[0.92, 0.03, 0.02, 0.01, 0.02]`.
- **Bar colors (per row):** green `#008300`, blue `#2a78d6`, violet `#4a3aa7`, aqua `#199e70`, mute gray `#6b7280`.
- **Layout:** left margin 120, right 80, top 52, row height 42, bars 24px tall; bar width = prob × available width, minimum 3px. Candidate labels right-aligned left of bars (first row "Paris" bold); probability value bold to the right of each bar (2 decimals). Thin vertical gray `#999` axis line at the bar baseline x.
- **Caption (bold 14px orange `#d95926`, bottom center):** "0.92 + 0.03 + 0.02 + 0.01 + 0.02 = 1.00 — one unit of belief, shared out".

## Generating a Sentence, One Table at a Time

**Tags:** `worked example` (green), `core idea` (blue)

- **Step 1** — "The capital of France is" → Paris wins at 0.92; append it
- **Step 2** — "…is Paris" → "." 0.71, "," 0.11, "and" 0.05, rest 0.13; append "."
- **Step 3** — "…is Paris." → end-of-text 0.62, "It" 0.14, "The" 0.09, rest 0.15; stop
- **Check step 2 by hand** — 0.71 + 0.11 + 0.05 + 0.13 = 1.00, like every step
- **The loop** — predict, append, repeat: a 500-word answer is ~650 of these tables

*Example:* The model never plans the sentence — it rebuilds the whole table after every single token.

**Key point:** Generation = next-token prediction in a loop. There is no other machinery behind the curtain.

### Visualization (canvas `c2`, 720×300)

Three-row generation-loop diagram: each row shows the context so far plus a mini bar chart of top candidates.

- **Title (bold 15px `#1a5276`, top center):** "Predict → Append → Repeat".
- **Steps data (context / candidates / probabilities / pick):**
  1. "The capital of France is" — `['Paris', 'Lyon', 'rest']` `[0.92, 0.03, 0.05]`, pick Paris
  2. "The capital of France is Paris" — `['.', ',', 'rest']` `[0.71, 0.11, 0.18]`, pick "."
  3. "The capital of France is Paris." — `['<end>', 'It', 'rest']` `[0.62, 0.14, 0.24]`, pick `<end>`
- **Layout:** row baselines at y = 66, 142, 218. Left of each row: bold violet `#4a3aa7` label "step 1/2/3" at x=20, then the context text plus " _" in 13px monospace `#2c3e50` at x=78. Right side: 3 horizontal bars starting at x=420, max width 200, height 14, 5px vertical gap; picked candidate bar green `#008300` with bold green label "name prob", others light gray `#c9d2dc` bars with muted `#6b7280` labels; min bar width 3px.
- **Between rows 1→2 and 2→3:** orange `#d95926` vertical arrow at x=48 with 11px label "append the winner".
- **Caption (bold 14px orange, bottom center):** "each step is a fresh table that sums to 1.00 — a long answer is just this, hundreds of times".

## Why It Matters: The Blank Always Gets Filled

**Tags:** `where it's used` (blue), `hallucination` (red)

- **Everything is this** — chat, code, summaries, translation: the same loop, different prompts
- **No "I don't know" row** — some token must receive the probability, so one always does
- **Made-up country** — "The capital of Zenovia is" still returns confident-sounding names
- **Fluent ≠ true** — high probability means "plausible continuation", not "verified fact"
- **Hallucination** — is this loop working perfectly on a question it has no facts for

*Example:* Ask about fictional Zenovia and "Zenograd" arrives in the same confident tone as Paris.

**Key point:** Treat every output as a prediction of plausible text — never as a database lookup that either hits or honestly misses.

### Visualization (canvas `c3`, 720×300)

Two side-by-side horizontal-bar panels comparing a real question and a made-up question, split by a dashed divider.

- **Title (bold 15px `#1a5276`, top center):** "Real Question vs Made-Up Question: Both Get an Answer".
- **Divider:** vertical dashed `#bdc3c7` line (dash 4/3) at x=360 from y=38 to h−40.
- **Left panel (x0=20), title:** '"The capital of France is ___"' — candidates `['Paris', 'Lyon', 'all others']`, probs `[0.92, 0.03, 0.05]`, colors green `#008300`, blue `#2a78d6`, mute `#6b7280`; note below in bold green: "one candidate dominates — the model has the fact".
- **Right panel (x0=390), title:** '"The capital of Zenovia is ___"  (fictional)' — candidates `['Zen City', 'Zenograd', 'Port Zen', 'all others']`, probs `[0.31, 0.24, 0.18, 0.27]`, colors magenta `#d55181`, violet `#4a3aa7`, aqua `#199e70`, mute `#6b7280`; note in bold magenta: "mass spreads over inventions (illustrative)".
- **Panel bar geometry:** labels right-aligned, bars 20px tall, max width 190, row height 36, top y=70, bold value labels after bars.
- **Caption (bold 14px red `#e74c3c`, bottom center):** 'neither table has an "I don\'t know" row — the blank must be filled either way'.

## The Confusion: It Doesn't Always Pick the Top Token

**Tags:** `common mistake` (red), `temperature` (orange)

- **Sampling** — systems usually draw randomly from the table instead of taking the max
- **Temperature** — a dial that reshapes the table before the draw is made
- **Low (0.2)** — Paris soaks up ~0.995 of the mass: same answer nearly every run
- **High (1.5)** — Paris drops to ~0.58; Lyon, "the", Marseille get real chances
- **Why answers vary** — same model, same prompt, different draws from the same table
- **For pipelines** — set temperature near 0 when you need reproducible outputs

*Example:* The "creative" setting literally means: give unlikely tokens a bigger slice of the wheel.

**Key point (labeled "Common mistake:"):** Reading run-to-run variation as the model "changing its mind" — it is a sampling choice you control, not indecision.

### Visualization (canvas `c4`, 720×300)

Grouped vertical bar chart: two temperature settings over the same five candidates.

- **Title (bold 15px `#1a5276`, top center):** "Same Model, Same Prompt — Temperature Reshapes the Table".
- **Subtitle (12px mute `#6b7280`):** 'P(next token) for "The capital of France is ___" (illustrative)'.
- **Data:** candidates `['Paris', 'Lyon', '"the"', 'Marseille', 'others']`; temperature 0.2 = `[0.995, 0.002, 0.001, 0.001, 0.001]`; temperature 1.5 = `[0.58, 0.13, 0.10, 0.08, 0.11]`.
- **Axes:** y 0 to 1.0 with labels 0, 0.5, 1.0 and light gridlines `#e5e9ef`; gray `#999` L-shaped axis; padding top 56, bottom 60, left 60, right 170.
- **Bars:** per candidate group, blue `#2a78d6` bar (temp 0.2) left of orange `#d95926` bar (temp 1.5), each 30px wide, 3px from group center, min height 2px; 11px value labels above bars (temp-0.2 values <0.01 shown as "~0"); candidate names below baseline.
- **Legend (right side, x = w−155):** blue swatch "temperature 0.2", orange swatch "temperature 1.5".
- **Caption (bold 13px orange, bottom center):** "at 1.5, Paris still leads — but 42% of draws now land somewhere else".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`, social-graph reference skeleton). `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks; each has an `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) and a `table.layout` with `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` row of colored pill spans first, then a `<ul>` of one-line bullets each opening with `<b>` (bold terms colored `#1a5276`), one italic `.example` paragraph, one `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, `<strong>` label).
- **Tag pill colors:** blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`. Pills: 0.72rem, weight 600, padding 2px 10px, radius 10px.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem; canvases `width:100%` with `1px solid #e0e0e0` border, radius 4px. No nav bar, no back/home links, no cross-page links.
- **Canvas:** all charts 720×300 logical, scaled by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Hardcoded data arrays only — no `Math.random()`.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
