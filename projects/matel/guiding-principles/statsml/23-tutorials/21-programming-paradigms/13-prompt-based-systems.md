# Prompt-Based Systems

**Page type:** detail page (tutorial card-sections: h2 per section, two-column table.layout with text left 50% / canvas right 50%)
**HTML title tag:** Prompt-Based Systems

**Subtitle:** One sentence of prose now plays every role at once — what a function does, how two services agree, and what the whole job is — with no checker for any of them.

## One Sentence Doing Three Jobs

Tags: `core idea` (blue pill), `running example` (green pill)

- **The job** — sort each incoming support message into refund, shipping, or bug
- **The coded way** — a keyword chain, plus a signature, plus a schema, plus a ticket describing intent
- **The prompt way** — one sentence: "Reply with one word: refund, shipping, or bug"
- **As function spec** — that sentence states the inputs it accepts and the value it returns
- **As service contract** — it is also the only agreement between the caller and the responder
- **As task spec** — and it is the statement of intent a person would have written in the ticket
- **What vanished** — three separate artifacts, each of which used to be reviewed on its own

*Example (italic):* Change "one word" to "the category" and you have edited the return type, the contract, and the intent — in a single stroke, with nothing to warn you.

**Key point:** Earlier paradigms kept these three layers apart, and each had its own checkable form. A prompt collapses all three into prose, so a single reword can silently move any of them.

### Visualization (canvas `c1`, 720×300)

Collapse diagram: three separate specs on the left, one sentence on the right.

- **Title (bold 15px ink `#1a5276`, top center):** "Three Specs Collapse Into One Sentence"
- **Left column header (bold 13px blue `#2a78d6`, centered at x=150, y=52):** "WRITTEN SYSTEM — 3 artifacts"
- **Three stacked spec boxes** at x=30, 240 wide, 52 tall, y=64/128/192, each fill `rgba(42,120,214,0.10)`, stroke blue, two centered lines (bold 12px label, 11px detail):

| y | label | detail |
|---|-------|--------|
| 64 | function spec | `route(msg) -> str` |
| 128 | service contract | schema: one enum field |
| 192 | task spec | ticket: "triage support mail" |

- **Small check tags** (bold 10px green `#008300`, right-aligned at x=282, at each box's y+46): "checked by compiler", "checked by validator", "reviewed by a human".
- **Three blue arrows** converging from each box's right edge (x=272) into the single prompt box's left edge at (430,150).
- **Prompt box** at (432,110), 250×80: fill `rgba(217,89,38,0.10)`, stroke orange `#d95926`, 2px. Header inside (bold 12px orange, centered, y=132): "PROMPT — 1 artifact". Two centered 12px italic lines at y=152/170: "\"Reply with one word:", "refund, shipping, or bug\"".
- **Un-check tag** (bold 11px red `#e74c3c`, centered at x=557, y=206): "checked by nothing".
- **Bottom annotation (bold magenta `#d55181` 13px, centered, y=278):** "one reword can move the return type, the contract, or the intent — indistinguishably"

## Six Messages Through the Keyword Chain

Tags: `worked example` (green pill), `where it's used` (blue pill)

- **Rule as written** — "money back"/"refund" → refund; "arrive"/"ship" → shipping; "crash"/"error" → bug
- **Handled** — "I want my money back", "hasn't arrived", "the app crashes" — 3 clean hits
- **Missed** — "charged me twice, please reverse one" matches no keyword at all
- **Missed** — "been waiting two weeks, still nothing" never says ship or arrive
- **Ambiguous** — "refund me, and the page errors out" hits refund first, drops the bug
- **Score** — 3 of 6 routed correctly, 2 unrouted, 1 half-routed

*Example (italic):* Every missed phrasing is a line of code nobody thought to write, and the fix is always one more keyword.

**Key point:** The keyword result is fully determined — you can trace all six by hand. That traceability is exactly what you trade away when the contract becomes prose.

### Visualization (canvas `c2`, 720×300)

Matrix chart: six messages, keyword verdict beside prompt verdict.

- **Title (bold 15px ink, top center):** "Six Phrasings, One Keyword Chain"
- **Column headers (bold 12px ink, y=52):** "support message" left-aligned at x=20; "keyword chain" centered at x=455; "prompt" centered at x=600.
- **Six rows**, y = 70 + i×30, message text in 12px `#2c3e50` left-aligned at x=20 (truncated to fit ~370px); verdict pills 100×22 at x=405 and x=550 (radius via plain rects), fill = colour at 0.15 alpha, stroke = colour, centered bold 11px label:

| message | keyword verdict | colour | prompt verdict (illustrative) |
|---|---|---|---|
| "I want my money back for order 4127" | refund | green `#008300` | refund |
| "Where is my package? It hasn't arrived." | shipping | green `#008300` | shipping |
| "The app crashes when I tap checkout" | bug | green `#008300` | bug |
| "Charged me twice, please reverse one" | unknown | red `#e74c3c` | refund |
| "Been waiting two weeks, still nothing" | unknown | red `#e74c3c` | shipping |
| "Refund me, and the page errors out" | refund only | orange `#d95926` | refund + bug |

- **Prompt column is drawn at 55% opacity** with a bold 11px orange "illustrative" tag at (600, 268), because a model's output is not something this page can compute.
- **Bottom annotation (bold ink 13px, left-aligned at x=20, y=288):** "keyword chain: 3 of 6 correct, 2 unrouted, 1 partial — every gap is a keyword nobody wrote"

## One Reworded Word Changes the Interface

Tags: `why it matters` (blue pill), `worked example` (green pill)

- **Downstream code** — the caller does an exact match on a single lowercase word
- **Prompt A** — "Reply with one word: refund, shipping, or bug" → "shipping" → parses
- **Prompt B** — "Reply with the category: refund, shipping, or bug" → "Shipping issue" → fails
- **What changed** — two words of prose; zero function signatures; zero type errors
- **What did not fire** — no compiler warning, no schema mismatch, no failing unit test
- **The consequence** — prompts need review, versioning, and tests the way code does

*Example (italic):* "Reply with one word" is not a stylistic preference — it is the return type, written in a language with no way to declare one.

**Key point:** Because the spec lives in prose, an edit that reads like a harmless clarification can silently change the interface — and nothing in the toolchain notices.

### Visualization (canvas `c3`, 720×300)

Two stacked lanes: prompt A parses, prompt B breaks.

- **Title (bold 15px ink, top center):** "A Two-Word Edit Is an Interface Change"
- **Lane A (y band 56–128, green `#008300`):** prompt box at (20,60), 300×56, fill `rgba(0,131,0,0.08)`, stroke green, two 12px lines "\"Reply with **one word**:" / "refund, shipping, or bug\"" (the phrase `one word` in bold green). Arrow (322,88)→(392,88) green. Output box at (395,66), 150×44, fill white, stroke green, bold 12px `"shipping"`. Arrow (547,88)→(600,88) green. Verdict at (610,93): bold 13px green "parses ✓".
- **Lane B (y band 168–240, orange `#d95926`):** prompt box at (20,172), 300×56, fill `rgba(217,89,38,0.08)`, stroke orange, two 12px lines "\"Reply with **the category**:" / "refund, shipping, or bug\"" (the phrase `the category` in bold orange). Arrow (322,200)→(392,200) orange. Output box at (395,178), 150×44, fill white, stroke orange, bold 12px `"Shipping issue"`. Arrow (547,200)→(600,200) orange. Verdict at (610,205): bold 13px red `#e74c3c` "no match ✗".
- **Diff marker:** dashed orange bracket under the changed phrase in lane B and a bold 12px orange label at (170,252): "only these two words changed".
- **Middle divider:** dashed gray horizontal line at y=148 across x=20..700.
- **Bottom annotation (bold magenta 13px, centered, y=286):** "no signature changed — so no tool, test, or reviewer flagged an interface change"

## The Common Confusion: Prose Fails Quietly

Tags: `common mistake` (red pill), `trade-off` (orange pill)

- **It looks like documentation** — but the sentence executes; it is the running spec
- **Typed boundary** — a wrong type throws at the boundary and stops right there
- **Prose boundary** — a misread instruction returns a well-formed, confident, wrong answer
- **Why that is worse** — nothing crashes, so the wrong value flows on downstream untouched
- **Not unprecedented** — human org charts always ran on ambiguous prose instructions
- **What is new** — machines now use that ambiguous prose as their calling convention

*Example (italic):* A crash tells you where to look; a plausible answer to a slightly different question tells you nothing at all.

**Key point:** The failure mode flips from loud and local to quiet and travelling — which is why prompt-based systems are debugged by inspecting outputs, not by reading stack traces.

### Visualization (canvas `c4`, 720×300)

Two lanes contrasting where a failure stops.

- **Title (bold 15px ink, top center):** "Where the Failure Stops"
- **Lane 1 — typed boundary (y=100):** label at (20,80) bold 12px ink "typed boundary". Caller box at (20,86), 120×36, fill `rgba(42,120,214,0.10)`, stroke blue, "caller". Arrow (142,104)→(196,104) blue. Red burst marker (bold 20px `#e74c3c` "✗") at (215,111) with a red circle radius 16 outline at (215,104). Three downstream boxes at x=270/400/530, 110×36, y=86, fill `#f4f4f4`, stroke `#cfcfcf`, gray 11px labels "step 2", "step 3", "report" — drawn faded to show they never ran. Bold 12px red at (400,140): "stops at the boundary — 1 place to look".
- **Lane 2 — prose boundary (y=210):** label at (20,190) bold 12px ink "prose boundary". Caller box at (20,196), 120×36, fill `rgba(217,89,38,0.10)`, stroke orange, "caller". Orange arrows chaining through three downstream boxes at x=200/340/480, 120×36, y=196, fill `rgba(217,89,38,0.06)`, stroke orange, 11px labels "step 2", "step 3", "report" — all drawn solid to show they all ran. Small orange "wrong value" tag (bold 11px) above the first arrow at (170,188). Bold 12px orange at (400,250): "flows all the way to the report — every step is a suspect"
- **Bottom annotation (bold magenta 13px, centered, y=286):** "same defect: one crash you can locate, or a clean-looking number you cannot"

## Regeneration instructions

- **Template/layout:** tutorials detail page. `<h1>` + `.subtitle`, then four `.card-section` blocks each with `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` (width 100%, border-collapse, td padding 12px, vertical-align top; `td.text-col` 50% / `td.viz-col` 50%).
- **Text column structure:** `.tags` pill row first (pills 0.72rem bold, 2px 10px padding, 10px radius: blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); then a `<ul>` (0.92rem) of one-line bullets each opening with `<b>` in `#1a5276`; one italic `.example` line (0.9rem `#555`); one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem).
- **Page CSS:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. Canvases have `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** all canvases 720×300 intrinsic; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; shared `box()` and `arrow()` drawing helpers. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Data integrity:** the keyword-chain verdicts in section 2 are derived from the stated rule and are reproducible by hand (3 correct / 2 unknown / 1 partial). The prompt column is model output, so it is drawn faded and tagged "illustrative"; no page text claims it as measured.
- In regenerated HTML, any card/page links use `.html` extensions.
