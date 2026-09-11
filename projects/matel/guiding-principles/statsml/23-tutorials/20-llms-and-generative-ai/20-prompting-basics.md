# Prompting Basics

**Page type:** detail page (tutorial layout: h1 + subtitle, then one `.card-section` per concept, each with h2 and a two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Prompting Basics

**Subtitle:** The words you type are part of the program — the same model turns "summarize this" and a specific request into completely different outputs

## One Email, Two Prompts, Two Very Different Summaries

**Tags:** `core idea` (blue), `running example` (green)

- **The email** — a customer writes 150 words about order #4127 arriving 9 days late
- **Prompt A** — "Summarize this." leaves length, audience, and format for the model to guess
- **Prompt B** — names a role, an audience, a length, and a format in one sentence
- **Output A** — a 62-word paragraph that retells the story in different words
- **Output B** — two short bullets plus one action a manager can take right now
- **Same model** — both prompts were followed exactly; only one said what "good" looks like

*Example (italic):* "Summarize this" is like telling a new intern "handle it" — the result depends on their guess.

**Key point:** The prompt is part of the program. The model ran your instructions faithfully — the vague ones just under-specified the output.

### Visualization (canvas `c1`, 720×300)

Side-by-side flow diagram: vague prompt vs specific prompt on the same email; vertical dashed divider (`#bdc3c7`, dash 4/3) at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Same Email About Order #4127 — Two Prompts, Two Outputs"
- **Left panel:**
  - Prompt box (290×34 at x=40, y=40; fill `#f4f6f8`, border muted `#6b7280`): bold 12px monospace centered text '"Summarize this." + email'
  - Muted arrow down to output box
  - Output box (290×130, white fill, muted border) containing 7 gray squiggle lines (5px round-cap strokes in `#c6ccd4`, lengths 250/262/240/258/232/246/180) representing a paragraph
  - Caption (bold 12px muted `#6b7280`, centered): "62 words — retells the whole story"
- **Right panel:**
  - Prompt box (290×74 at x=390, y=40; fill `rgba(42,120,214,0.07)`, blue `#2a78d6` border) with four 11px monospace lines, one per prompt part, each in its own color: violet `#4a3aa7` '"You are a support lead.'; aqua `#199e70` ' Summarize for a busy manager'; yellow `#c98500` ' in 2 bullets, max 15 words each;'; orange `#d95926` ' end with one action." + email'
  - Blue arrow down to output box
  - Output box (290×94; fill `rgba(0,131,0,0.05)`, green `#008300` border) with 11px text: "• Order #4127 arrived 9 days late;" / "   customer asks for a refund." / "• Second late order for this customer." and bold 11px green "Action: approve refund, flag the carrier."
  - Caption (bold 12px green, centered): "2 bullets + 1 action — paste-ready"
- **Takeaway (bold 13px orange `#d95926`, bottom center):** "same model, same email — only the prompt changed (outputs illustrative)"

## The Four Parts You Add: Role, Audience, Length, Format

**Tags:** `worked example` (green), `rule of thumb` (blue)

- **Role** — "You are a support lead" sets vocabulary and what counts as important
- **Audience** — "for a busy manager" says what to keep: the decision, not the storyline
- **Length** — "2 bullets, max 15 words each" replaces the model's guess with your number
- **Format** — "end with one recommended action" makes the output paste-ready
- **Redo it by hand** — add the parts one at a time and watch the word count fall: 62, 55, 41, 28

*Example (italic):* With no length given the model guessed 62 words; with "max 15 words per bullet" it produced 28.

**Key point:** Every part you leave out is a guess you delegate. Specific beats clever — plain named constraints did all the work here.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: output length falls as each prompt part is added.

- **Title (bold 15px, `#1a5276`, top center):** "Output Length as Each Part Is Added (words, illustrative)"
- **Layout:** bars start at x=250, plot width 380, rows 42px apart from y=48, bar height 22px; x scale 0–70 with vertical gridlines (`#e5e9ef`) and muted 12px tick labels at 0, 10, 20, 30, 40, 50, 60, 70; x-axis caption (muted 12px): "output words"
- **Data (row label → words, bar color at 0.75 alpha, bold 13px value label in bar color right of bar; labels 12px `#2c3e50` right-aligned):**
  - '"Summarize this."' → 62, muted `#6b7280`
  - '+ role: support lead' → 55, violet `#4a3aa7`
  - '+ audience: busy manager' → 41, aqua `#199e70`
  - '+ length: 2 bullets × 15' → 28, yellow `#c98500`
  - '+ format: one action line' → 33, orange `#d95926`
- **Budget line:** vertical dashed green `#008300` line (dash 6/4, 2px) at 30 words, labeled bold 12px green: "requested budget: 2 × 15 words"
- **Takeaway (bold 13px orange `#d95926`, bottom center):** "each added part removed one guess — 62 words down to about the budget (format adds 5 back)"

## Why a Data Scientist Should Care

**Tags:** `where it's used` (blue), `watch out` (orange)

- **Prompts scale** — one prompt runs over 5,000 tickets tonight; a vague one fails 5,000 times
- **Measured gap** — on 20 sample emails, the vague prompt gave paste-ready output 7 times; the specific one 18
- **Right facts, wrong shape** — the 13 failures weren't wrong summaries, just unusable ones
- **Cheapest fix** — rewriting one sentence beats retries, cleanup code, or fine-tuning
- **Version it** — prompts are code: keep them in git and test on a sample before scaling

*Example (italic):* A downstream parser expecting bullets breaks on a paragraph — the model isn't the bug, the prompt is.

**Key point:** Treat the prompt like a function signature — it defines the output contract for every row it touches.

### Visualization (canvas `c3`, 720×300)

Two-bar comparison: paste-ready outputs out of 20 test emails.

- **Title (bold 15px, `#1a5276`, top center):** "20 Test Emails: How Many Outputs Were Usable As-Is? (illustrative)"
- **Bars (130px wide, baseline y=235, chart height 165, y scale max 22, fill at 0.75 alpha):**
  - '"Summarize this."' → 7, muted `#6b7280`; note below (11px muted): "13 needed manual editing"
  - 'role + audience + length + format' → 18, green `#008300`; note: "2 needed manual editing"
- **Value labels:** bold 16px in bar color above each bar: "7 / 20", "18 / 20"; bar labels 12px `#2c3e50` below the baseline
- **Ceiling line:** dashed muted line (dash 6/4, 1.5px) at y for 20, labeled bold 12px muted: "all 20 emails"
- **Baseline:** thin `#999` line
- **Takeaway (bold 13px orange `#d95926`, bottom center):** "7 → 18 usable outputs — the model never changed, one sentence did"

## Re-asking Is Not Re-specifying

**Tags:** `common mistake` (red)

- **The trap** — the output is vague, so people hit retry and hope for a better one
- **Same input** — the same underspecified prompt draws another guess, not a better answer
- **The fix** — put what was missing into the prompt: audience, length, format
- **Show, don't tell** — pasting one example of a good output beats three sentences describing it
- **The tell** — if you explained what you wanted after seeing the output, that belongs in the prompt

*Example (italic):* Two retries of "summarize this" changed nothing; adding "2 bullets for a manager" fixed it in one shot.

**Common mistake (key-point callout):** Retrying re-rolls the dice on the same missing information. Re-specifying changes what the model is asked to do.

### Visualization (canvas `c4`, 720×300)

Side-by-side flow diagram: retry loop vs one re-specified attempt; vertical dashed divider (`#bdc3c7`, dash 4/3) at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Retrying the Same Prompt vs Adding the Missing Spec"
- **Left panel — header bold 13px muted `#6b7280`, centered at x=185:** "retry, retry, retry"
  - Three stacked boxes (260×40 at x=55, y = 62/122/182; fill `#f4f6f8`, muted border) chained by muted arrows; each contains 11px `#2c3e50` text 'attempt N: "Summarize this."' and bold 11px red `#e74c3c` "→ another ~60-word paragraph"
  - Verdict (bold 12px red, centered): "same missing info, same guess"
- **Right panel — header bold 13px green `#008300`, centered at x=535:** "re-specify once"
  - Prompt box (270×56 at x=400, y=62; fill `rgba(42,120,214,0.07)`, blue border): 'attempt 1: "Summarize in 2 bullets' / 'for a manager; end with one action."'
  - Green arrow down to result box (270×52; fill `rgba(0,131,0,0.05)`, green border): bold 11px green "✓ 2 bullets + action, first try" and 11px `#2c3e50` "the spec carried the information"
- **Takeaway (bold 13px orange `#d95926`, bottom center):** "a retry re-rolls the dice; a spec changes the game"

## Regeneration instructions

- **Template:** tutorials topic-page skeleton (social-graph reference style). Structure: `<h1>` (no index number), `.subtitle` paragraph, then four `.card-section` blocks; each has an `<h2>` and a `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) holding `.tags` pills, a `<ul>` of one-line bullets with `<b>` lead terms, an italic `.example` paragraph, and a `.key-point` callout; right `<td class="viz-col">` (50%) holding one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border. Section h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border. `.subtitle` `#666` 0.95rem. Canvas `width:100%`, 1px `#e0e0e0` border, 4px radius. `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem. `ul` 0.92rem; `li b` colored `#1a5276`. Inline code in `li` and `.example`: ui-monospace/Menlo, 0.9em, background `#f4f6f8`, padding 1px 4px, radius 3px.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors — blue: bg `rgba(26,82,118,0.12)` text `#1a5276`; green: bg `rgba(39,174,96,0.15)` text `#27ae60`; red: bg `rgba(231,76,60,0.12)` text `#e74c3c`; orange: bg `rgba(230,126,34,0.15)` text `#e67e22`.
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas helpers:** shared `setup(id)` (720×300 logical, backing store sized to rendered width × `window.devicePixelRatio` (display capped via `style.maxWidth`), `ctx.scale` back to logical coordinates, clear), plus `box(ctx, x, y, w, h, stroke, fill)` for bordered rectangles and `arrow(ctx, x1, y1, x2, y2, color)` for 2px lines with filled triangular heads. All data hardcoded (no randomness).
- No nav bar, no back/home links. In regenerated HTML any card links would use `.html` extensions (this page has none).
