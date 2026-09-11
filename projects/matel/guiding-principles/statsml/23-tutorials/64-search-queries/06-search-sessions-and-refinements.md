# Search Sessions & Refinements

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Search Sessions & Refinements

**Subtitle:** One goal, several tries — a sitting of queries grows more specific until a click ends it, and every rewrite says what the last results were missing

## One Sitting, One Goal

**Tags:** `core idea` (blue), `sessions` (green)

- **One sitting** — a session is every query one user types in one sitting toward one goal
- **The gap rule** — roughly 30 minutes of no activity ends a session; the next query starts a new one
- **One try** — a single query is just one attempt; the session is the whole story of the search
- **Same goal** — "printer not working" and "hp printer offline" are two tries at one problem
- **The ending** — a click with a long stay usually marks the goal reached and the session done

*Example (italic):* At 9:00 Alice types three printer queries in nine minutes, then nothing until 10:15 — that silence splits her morning into two sessions.

**Key point:** Group queries by user and by gaps in time, not one by one — the session, not the single query, is the unit that carries the goal.

### Visualization (canvas `c1`, 720×300)

Horizontal timeline of one morning: four dots for the printer session (three queries plus a click), a shaded silence band, then one dot for a new session; query chips staggered above the line, session brackets below.

- **Title (bold 15px ink `#1a5276`, top center, y=22):** "One Sitting Splits into Two Sessions (times illustrative)".
- **Timeline:** 1px `#999` line at y=200 from x=40 to x=690. Dots r=5: blue `#2a78d6` at x=80, 190, 300 (queries); green `#008300` r=6 at x=380 (click); violet `#4a3aa7` at x=610 (new session's query). Time labels 11px `#6b7280` centered under dots at y=222: "9:00", "9:03", "9:07", "9:09", "10:15".
- **Query chips (white fill, 2px border in the dot color, bold 11px `#2c3e50` text, h=22), each joined to its dot by a 1px `#bbb` connector:** "printer not working" (w=150, cx=110, top y=50, blue); "hp printer offline" (w=140, cx=195, y=95, blue); "hp envy 6055 offline windows 11" (w=220, cx=300, y=140, blue); "click · 4-min stay" (w=130, cx=430, y=95, green, tint fill `rgba(0,131,0,0.08)`); "best pasta recipes" (w=140, cx=610, y=95, violet).
- **Silence band:** `rgba(201,133,0,0.12)` rect x=395 to x=585, y=185 to y=215; bold 12px yellow `#c98500` label centered at x=490, y=170: "66 min of silence"; 11px `#6b7280` at x=490, y=232: "> ~30 min gap → new session".
- **Session brackets (2px, square ends, at y=245 with 8px up-ticks):** blue x=65–395, bold 12px blue label centered y=264: "session 1 — one goal: fix the printer"; violet x=585–655, bold 12px violet label centered y=264: "session 2 — new goal".
- **Caption (11px `#6b7280`, centered, y=290):** "time axis not to scale — times illustrative".

## Three Tries to Fix a Printer

**Tags:** `worked example` (blue), `rewrite chain` (orange)

- **Try 1** — "printer not working": too vague; the results cover every printer ever made
- **Try 2** — "hp printer offline": the rewrite adds the brand and the exact symptom word
- **Try 3** — "hp envy 6055 offline windows 11": adds the model and the operating system
- **The ending** — a click and a long stay on a fix-it page; no fourth query follows
- **Each rewrite adds** — every rewrite supplies the words the last results were missing
- **The funnel** — 100 sessions → 38 → 17 → 9 still searching after each try (illustrative)

*Example (italic):* The searcher knew "envy 6055" all along — the first two result pages just never asked for it.

**Key point:** Read the chain top to bottom and each arrow tells you exactly what the previous results failed to cover.

### Visualization (canvas `c2`, 720×300)

Left half: the three-query rewrite chain flowing down into a click box, with the newly added words bold orange inside each query and each arrow labeled by what the rewrite added. Right half: four horizontal funnel bars, aligned to the chain rows, showing how many of 100 sessions are still searching.

- **Title (bold 15px ink, top center, y=22):** "The Rewrite Chain and the Funnel Around It (illustrative)".
- **Divider:** dashed `#bdc3c7` vertical line at x=370, y=35 to y=282 (dash 4/3).
- **Chain boxes (x=30, w=310, h=26, white fill, tops at y = 46, 110, 174, 238; text centered at cx=185):** rows 1–3 have 2px blue `#2a78d6` borders and 12px query text with the NEW words bold orange `#d95926` (segment widths via `measureText`): row 1 all-plain "printer not working"; row 2 bold "hp " + plain "printer " + bold "offline"; row 3 plain "hp " + bold "envy 6055" + plain " offline " + bold "windows 11". Row 4 is the terminal: 2px green `#008300` border, `rgba(0,131,0,0.08)` fill, bold 12px green "click · 4-min stay on a fix-it page".
- **Arrows between boxes:** 2px orange arrowDown at x=100 (from box bottom +2 to next top −2); arrow into the click box green. Arrow labels bold 12px orange, left-aligned at x=115 at the arrow midpoints: "adds: brand, symptom word", "adds: model, operating system"; the last label bold 12px green: "the goal is reached".
- **Funnel (right half):** header bold 12px ink centered at x=545, y=40: "how many still searching (illustrative)". Bars x=390, h=20, vertically centered on the chain rows (centers y = 59, 123, 187, 251); widths 2.5px per session: 100→250, 38→95, 17→43, 9→23; fill `rgba(42,120,214,0.15)`, 2px blue border. Value labels bold 13px blue just right of each bar: "100", "38", "17", "9". Row captions 11px `#6b7280` at x=390, y = center+24: "sessions type try 1", "still searching → type try 2", "still searching → type try 3", "still searching after try 3".
- **Caption (11px `#6b7280`, centered, y=296):** "funnel counts are illustrative".

## What Engines Learn from Rewrites

**Tags:** `where it's used` (blue), `feedback signal` (green)

- **Mined pairs** — engines collect millions of rewrite-then-click pairs from session logs
- **Failure label** — a rewrite followed by a click says "the first results failed" louder than any survey
- **Synonyms** — "not working" rewritten to "offline" teaches the engine those words are neighbors
- **Spelling fixes** — "prnter" rewritten to "printer" becomes a free spelling-correction example
- **Missing words** — added brand and model terms show which words the results should have covered
- **Session, not query** — the unit of feedback is the whole session, not any single query in it

*Example (italic):* No one filled in a feedback form — the lesson "not working means offline" was left behind by ordinary typing.

**Key point:** A rewrite-then-click pair is a labeled training example the searcher wrote for free — and it only exists once queries are grouped into sessions.

### Visualization (canvas `c3`, 720×300)

One rewrite-then-click pair at the top fanning down into three lesson cards: synonyms, missing words, and spelling fixes, each with a concrete mined example.

- **Title (bold 15px ink, top center, y=22):** "One Session Log, Three Free Lessons".
- **Pair box (x=140, y=40, w=440, h=30, white fill, 2px ink border):** bold 12px centered text '"printer not working" → "hp printer offline" → click' with the word "click" in green `#008300`, the rest `#2c3e50` (drawn in segments via `measureText`).
- **Fan arrows:** 2px `#6b7280` lines with arrowheads from the box bottom (x=360, y=72) to the three card tops (x = 140, 360, 580 at y=110).
- **Lesson cards (w=200, h=120, tops at y=112, centers x = 140, 360, 580; white fill, 2px border in the card color; header band h=24 in the color's tint with bold 12px colored label centered):**
  - *synonyms (green, tint `rgba(0,131,0,0.10)`):* lines 12px `#2c3e50` centered — '"not working"', '≈ "offline"', then 11px `#6b7280` "words that swap in rewrites".
  - *missing words (blue, tint `rgba(42,120,214,0.12)`):* 'added: "hp", "envy 6055"', 'the brand + model', 11px mute "words the results lacked".
  - *spelling fixes (magenta `#d55181`, tint `rgba(213,81,129,0.12)`):* '"prnter" → "printer"', 'typed, then corrected', 11px mute "a free correction example".
- **Annotation (bold 12px ink, centered, y=272):** 'rewrite followed by a click = "the first results failed"'.
- **Caption (11px `#6b7280`, centered, y=292):** "millions of such pairs are mined from session logs".

## Not Every Rewrite Is a Failure

**Tags:** `common mistake` (red), `rule of thumb` (orange)

- **Not all failure** — people also explore, compare, and drift to new topics inside one sitting
- **Refinement** — words carry over and get more specific: same goal, previous results lacking
- **Drift** — a rewrite sharing no words, printer fix → pasta recipes, is a new goal, not a failure
- **Poisoned labels** — treating every rewrite as failure teaches the engine that fine results failed
- **Word overlap** — shared words between consecutive queries are the simplest refinement-vs-drift cue

*Example (italic):* Alice fixed her printer, then searched dinner ideas in the same sitting — nothing about the printer results failed.

**Key point:** Only rewrites that keep the goal count as failure labels — separating refinement from drift comes before any mining.

### Visualization (canvas `c4`, 720×300)

Side-by-side pair of two-query snippets: a refinement on the left (shared words bold blue, valid failure label) and a topic drift on the right (no shared words, must not be labeled a failure).

- **Title (bold 15px ink, top center, y=22):** "Refinement Keeps the Goal — Drift Changes It".
- **Divider:** dashed `#bdc3c7` vertical line at x=360, y=35 to y=278.
- **Left half (centered x=180):** header bold 13px blue `#2a78d6` at y=50: "refinement — same goal". Chip 1 (w=200, h=26, top y=64, 1px `#ccc` border): "hp printer offline" with "hp" and "offline" bold blue, "printer" plain (segments via `measureText`, centered). Blue arrowDown x=180 from y=94 to y=122. Chip 2 (w=260, h=26, top y=126): "hp envy 6055 offline windows 11" with "hp" and "offline" bold blue, the rest plain. Verdict bold 12px green `#008300` centered y=186: "words carry over → valid failure label". Callout: `rgba(0,131,0,0.10)` rect (w=230, h=24, top y=200), bold 11px green centered: 'use: "the try-1 results failed"'.
- **Right half (centered x=540):** header bold 13px violet `#4a3aa7` at y=50: "drift — new goal". Chip 1 (w=260, h=26, top y=64): "hp envy 6055 offline windows 11", all plain 12px `#2c3e50`. Violet arrowDown x=540 from y=94 to y=122. Chip 2 (w=170, h=26, top y=126): "best pasta recipes", all plain. Verdict bold 12px red `#e74c3c` centered y=186: "no words carry over → topic changed". Callout: `rgba(231,76,60,0.10)` rect (w=250, h=24, top y=200), bold 11px red centered: "do not label the printer results failed".
- **Annotation (bold 12px yellow `#c98500`, centered, y=252):** "shared words between tries — the simplest refinement-vs-drift cue".
- **Caption (11px `#6b7280`, centered, y=276):** "treating drift as failure poisons the training labels".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` (no index number); subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). `arrowDown` helper for vertical arrows with filled arrowheads.
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Blue = queries/refinement, green = the ending click, orange = words added by a rewrite, violet = a new session/goal, yellow = the silence gap and the overlap cue, red only for the poisoned-label warning in c4.
- **Data:** everything is hardcoded (no randomness): the c1 timestamps (9:00/9:03/9:07/9:09/10:15) and the 66-min gap, the c2 rewrite chain and funnel 100/38/17/9, the c3 lesson-card examples, the c4 snippet pairs — timestamps and counts are invented and labeled "illustrative". Text numbers match chart numbers (30-min rule and the gap in section 1, 100 → 38 → 17 → 9 in section 2).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
