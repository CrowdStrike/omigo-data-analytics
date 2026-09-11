# Tool Use & Agents

**Page type:** detail page (tutorial layout: `.card-section` blocks, each an h2 + two-column `table.layout`, text left 50%, canvas right 50%)
**HTML title tag:** Tool Use & Agents

**Subtitle:** Instead of guessing, the model asks a calculator and a database — an agent is just this loop: think, call a tool, read the result, answer

## "What's 12.4% of Our Q3 Revenue?" — Don't Answer From Memory

**Tags:** `core idea` (blue), `running example` (green)

- **The question** — it needs a private number (your revenue) and exact arithmetic
- **Guessing fails twice** — the model doesn't know your revenue, and it multiplies big numbers unreliably
- **Give it tools** — a database query tool and a calculator it is allowed to call
- **The model writes requests** — it outputs "call get_revenue for Q3"; your code runs it
- **Reads and continues** — the result is pasted back and the model takes the next step

*Example:* Without tools the model must either refuse or invent a revenue figure — both lose.

**Key point:** The model never executes anything — it asks, your code runs the tool, and the model reads the result.

### Visualization (canvas `c1`, 720×300)

Four-step loop diagram with a loop-back arrow.

- **Title (bold 15px, `#1a5276`, centered):** "The Agent Loop, Annotated With the Running Example".
- **Step boxes** (each 130×46 at y=70, white fill, colored 1.5px stroke; bold 12px colored step name, 11px mute sub-label inside; bold 11px monospace colored note below the box):
  - x=45, blue `#2a78d6`: "THINK" / "what do I need?" — note: "\"I need Q3 revenue\""
  - x=225, violet `#4a3aa7`: "CALL TOOL" / "write a request" — note: "get_revenue(\"Q3\")"
  - x=405, aqua `#199e70`: "READ RESULT" / "paste it back" — note: "2,450,000"
  - x=585, green `#008300`: "ANSWER" / "or loop again" — note: "\"$303,800\""
  - Mute arrows connect consecutive boxes.
- **Loop-back arrow:** orange `#d95926`, 2px, rectangular path from below READ RESULT (x=470) down and left back up into THINK (x=110), with an upward filled arrowhead; centered bold 12px orange label on the return leg: "not done? loop — ours loops once for the calculator".
- **Annotations (centered):** 12px `#2c3e50`: "the model decides and writes requests — your code actually runs the tools"; bold 13px orange: "two tool calls replace two guesses: a lookup and a multiplication".

## The Four Steps, With Numbers You Can Check

**Tags:** `worked example` (green)

- **Step 1** — the model decides it needs a number: calls `get_revenue(quarter="Q3")`
- **Step 2** — the database returns 2,450,000; the result is pasted back to the model
- **Step 3** — the model calls `calc(2450000 × 0.124)`; the calculator returns 303,800
- **Step 4** — final answer: "12.4% of Q3 revenue ($2,450,000) is $303,800"
- **Hand check** — 10% is 245,000 and 2.4% is 58,800; together 303,800 ✓

*Example:* Two tool calls, two results, one answer — the whole "agent" fits in this short transcript.

**Key point:** An agent is a model in a loop with tools: think, call, read, repeat — until it can answer.

### Visualization (canvas `c2`, 720×300)

Sequence/swimlane diagram: model lane (x=200) and tools lane (x=540), light grid-colored vertical lifelines from y=56 to y=262.

- **Title (bold 15px, `#1a5276`, centered):** "The Full Transcript: Two Calls, Two Results, One Answer".
- **Lane headers:** bold 13px blue `#2a78d6` "model" and violet `#4a3aa7` "tools (run by your code)".
- **Messages** (horizontal arrows between lanes, bold 11px monospace label above each, centered at x=370):
  - y=84, model→tools, violet: "get_revenue(quarter=\"Q3\")"
  - y=122, tools→model, aqua `#199e70`: "returns 2,450,000"
  - y=160, model→tools, violet: "calc(2450000 × 0.124)"
  - y=198, tools→model, aqua: "returns 303,800"
- **Final answer box:** (80, 218) 240×40, stroke green `#008300`, fill `rgba(0,131,0,0.05)`, bold 11px green centered: "\"12.4% of Q3 revenue" / "($2,450,000) is $303,800.\"".
- **Hand check (right side, x=570):** 11px text "check it by hand:", then monospace "10%   245,000" / "2.4%   58,800", a thin mute rule, and bold green monospace "      303,800".
- **Annotation (bold 13px orange `#d95926`, bottom center):** "every number in the answer traces back to a logged tool call".

## Why a Data Scientist Should Care

**Tags:** `where it's used` (blue), `best practice` (green)

- **Exact where it counts** — databases and calculators don't approximate; language models do
- **Fresh and private** — tools reach data the model was never trained on
- **Auditable** — every tool call is logged; you can trace where each number came from
- **Fails loudly** — a bad SQL query errors; a hallucinated number looks fine and isn't
- **Your job shifts** — you design the tools and the checks, not just the prompt

*Example:* On 20 finance questions, the no-tools run got the exact number 8 times; with tools, 19.

**Key point:** Use the model for language and tools for facts — anything exact, current, or private goes through a tool.

### Visualization (canvas `c3`, 720×300)

Two-bar comparison chart.

- **Title (bold 15px, `#1a5276`, centered):** "20 Finance Questions: Exact Number Produced (illustrative)".
- **Bars** (plot x=140, width 440, baseline y=230, chart height 160, y max 22, bar width 140, alpha 0.75): "model alone" 8 in mute `#6b7280`, note below: "12 fluent answers with invented figures"; "model + database + calculator" 19 in green `#008300`, note: "1 miss: queried the wrong quarter". Bold 16px value labels "8 / 20" and "19 / 20" above bars in the bar's color; labels 12px `#2c3e50`, notes 11px mute.
- **Reference line:** dashed (6/4) mute horizontal line at 20 labeled bold 12px "all 20" at the right. Thin `#999` baseline.
- **Annotation (bold 13px orange `#d95926`, bottom center):** "the dangerous failures are the fluent ones — they look identical to the right answers".

## An Agent Is Not a Smarter Model

**Tags:** `common mistake` (red), `watch out` (orange)

- **Same model** — the "agent" is the ordinary model called several times inside a loop
- **Your code drives** — the loop, the tools, and the stop condition are your program
- **Tools don't think** — the calculator computes what it's asked; a wrong plan gives a wrong exact number
- **Loops can wander** — cap the number of steps; an agent will happily keep calling tools
- **Read the trace** — when the answer looks off, the bug is usually in a tool call, not the math

*Example:* One run computed 12.4% of Q4 revenue perfectly — exact math on the wrong quarter.

**Common mistake:** Tools make the arithmetic exact, not the reasoning — check the plan, not just the answer.

### Visualization (canvas `c4`, 720×300)

Two side-by-side trace cards, split by a vertical dashed divider at x=360 (`#bdc3c7`, dash 4/3).

- **Title (bold 15px, `#1a5276`, centered):** "Exact Math, Wrong Plan: the Trace Shows the Bug".
- **Left trace (title bold 13px green `#008300`): "the run that was asked for (Q3)"** — box (45, 64) 280×130, stroke grid `#e5e9ef`, fill `#fafbfc`, 11px monospace lines: "get_revenue(\"Q3\")" (text color) / "→ 2,450,000" (mute) / "calc(2450000 × 0.124)" / "→ 303,800" (mute) / bold green "answer: $303,800"; verdict below in bold 12px green: "✓ right quarter, right answer".
- **Right trace (title bold 13px red `#e74c3c`): "the buggy run"** — same card layout at x=395: "get_revenue(\"Q4\")" (red) / "→ 2,610,000" / "calc(2610000 × 0.124)" / "→ 323,640" / bold red "answer: $323,640"; verdict in bold 12px red: "✗ arithmetic perfect, quarter wrong".
- **Annotations (centered):** bold 13px orange `#d95926`: "both traces are exact — only reading the tool calls reveals which one answered the question"; 12px mute: "same model, same tools, one wrong argument — cap steps and audit traces".

## Regeneration instructions

- **Layout:** tutorial page — `<h1>` + `.subtitle`, then 4 `.card-section` blocks; each has an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` with one row: left `td.text-col` (50%) holding `.tags` pills, a `<ul>` of bullets each opening with a `<b>` term (`#1a5276`), an italic `.example` paragraph, and a `.key-point` callout; right `td.viz-col` (50%) holding one `<canvas>` 720×300 (CSS `width:100%`, `1px solid #e0e0e0` border, 4px radius).
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; bullets 0.92rem; inline `code` in ui-monospace on `#f4f6f8`. No nav bar, no back/home links.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue = `rgba(26,82,118,0.12)`/`#1a5276`, green = `rgba(39,174,96,0.15)`/`#27ae60`, red = `rgba(231,76,60,0.12)`/`#e74c3c`, orange = `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvas:** each canvas declares intrinsic `width="720" height="300"`; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Shared `box()` (fill + 1.5px stroke rect) and `arrow()` (2px line with filled triangular head) helpers. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange. All numbers are hardcoded literal arrays (no `Math.random()`); invented numbers carry an "illustrative" label.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
