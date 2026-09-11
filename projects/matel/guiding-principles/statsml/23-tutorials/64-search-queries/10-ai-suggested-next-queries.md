# AI-Suggested Next Queries

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** AI-Suggested Next Queries

**Subtitle:** The machine proposes the next whole query — related searches, follow-up questions, and research agents that write and run queries on your behalf

**Grid-card description:** The machine proposes the next whole query — related searches, follow-up questions, and research agents that write and run queries on your behalf.

## The Results Page Proposes What to Ask Next

**Tags:** `core idea` (blue), `next query` (green)

- **The answered query** — you search "is this used sedan worth buying" and read the answer
- **Related searches** — the classic footer: a short list of new queries at the bottom of the page
- **People also ask** — boxes mid-page offering whole questions other searchers asked next
- **AI follow-ups** — assistants now end every answer with two or three suggested next questions
- **Research agents** — go furthest: you state one goal and the AI writes and runs the queries itself

*Example (italic):* After answering "is this used sedan worth buying", the assistant offers "want me to check open recalls?".

**Key point:** After one query is answered, the system proposes the next whole query — from a footer of links to an agent that searches on its own.

### Visualization (canvas `c1`, 720×300)

Three side-by-side panels showing the escalating forms: a related-searches footer, AI follow-up chips after an answer, and a research agent that runs its own queries.

- **Title (bold 15px ink `#1a5276`, top center, y=22):** "Three Ways the Machine Proposes the Next Query".
- **Three panels (y=45, w=200, h=205, white fill, 1px `#ccc` border) at x=25, 260, 495.** Panel headers bold 12px colored, centered at each panel's midline, y=63: "related searches" blue `#2a78d6`, "AI follow-up chips" violet `#4a3aa7`, "research agent" green `#008300`.
- **Panel 1 (results-page footer):** mini search box (x=35, y=75, w=180, h=20, 1px ink border) with 11px `#2c3e50` text "used sedan worth buying?"; three light-grey result bars (`#e5e9ef`, x=35, w=180, h=5) at y=104, 114, 124; label 11px `#6b7280` centered at x=125, y=150: "at the bottom of the page:"; two suggestion bands (x=33, w=184, h=18, fill `rgba(42,120,214,0.12)`) with bold 11px blue text at y=165 and y=190: "sedan reliability by year", "used car inspection tips"; caption 11px `#6b7280` centered at y=237: "the classic footer".
- **Panel 2 (AI follow-up chips):** answer bubble (x=270, y=75, w=180, h=44, fill `#f4f6f9`, 1px `#ccc` border) with 11px `#6b7280` lines "the sedan scores well" (y=93) and "for its class…" (y=108); two chips (x=272, w=176, h=22, fill `rgba(74,58,167,0.12)`, 1px violet border) at y=140 and y=172 with bold 11px violet centered text "check common failures?", "estimate insurance cost?"; caption 11px `#6b7280` centered at y=237: "after every answer".
- **Panel 3 (research agent):** goal box (x=505, y=75, w=180, h=24, white fill, 2px green border) bold 11px green centered "goal: worth buying?"; green arrow down at x=595 from y=99 to y=120; action box (x=505, y=124, w=180, h=22, fill `rgba(0,131,0,0.10)`, 1px green border) bold 11px green centered "writes & runs its own queries"; three query lines 11px `#6b7280` at x=515, y=166, 184, 202: "· reliability by year", "· failures at 80k miles", "· open recalls"; caption 11px `#6b7280` centered at y=237: "you state the goal only".
- **Annotation (bold 12px violet `#4a3aa7`, centered, y=285):** "each form hands the machine more of the steering".

## Steering Keystrokes vs Steering the Session

**Tags:** `two levels` (blue), `not autocomplete` (orange)

- **Not autocomplete** — autocomplete works within a single query, finishing the tokens you are typing
- **Within a query** — "how to make pi…" becomes "how to make pizza dough" before you press enter
- **Between queries** — next-query suggestion waits until one answer lands, then proposes a new question
- **A complete question** — the suggestion is a whole new query, not the tail of a half-typed one
- **Two levels** — one steers keystrokes inside the box; the other steers where the session goes next

*Example (italic):* Autocomplete saves you nine keystrokes; a suggested next query hands you a question you never started typing.

**Key point:** Autocomplete finishes the query you are typing; next-query suggestion writes the one you haven't asked yet — keystroke level vs session level.

### Visualization (canvas `c2`, 720×300)

A two-level diagram: the keystroke level (autocomplete completing a prefix within one query) above a dashed divider, and the session level (a finished query producing a proposed next query) below it.

- **Title (bold 15px ink, top center, y=22):** "Keystroke Level vs Session Level".
- **Lane 1 label (bold 13px blue, left-aligned at x=30, y=52):** "WITHIN one query — autocomplete".
- **Lane 1 row:** typed box (x=30, y=66, w=200, h=28, white fill, 2px ink border) bold 12px `#2c3e50` "how to make pi" plus a 1.5px ink cursor bar after the text (via `measureText`); blue arrow right from x=240 to x=280 at y=80; completion box (x=285, y=66, w=230, h=28, fill `rgba(42,120,214,0.12)`, 2px blue border) bold 12px blue centered "how to make pizza dough"; caption 11px `#6b7280` left-aligned at x=525, y=84: "finishes what you type".
- **Divider:** dashed `#bdc3c7` horizontal line at y=140, x=25 to x=695 (dash 4/3).
- **Lane 2 label (bold 13px green, left-aligned at x=30, y=172):** "BETWEEN queries — suggested next query".
- **Lane 2 row:** answered box (x=30, y=186, w=230, h=40, white fill, 2px ink border) with bold 12px `#2c3e50` "used sedan worth buying?" at y=203 and 11px `#6b7280` "answer shown" at y=219, both centered at x=145; green arrow right from x=265 to x=305 at y=206; proposed box (x=310, y=186, w=250, h=40, fill `rgba(0,131,0,0.10)`, 2px green border) with bold 12px green centered lines "how reliable is this sedan" (y=203) and "by model year?" (y=219); caption 11px `#6b7280` left-aligned at x=570, y=210: "a new, complete question".
- **Annotation (bold 12px violet, centered, y=278):** "one steers keystrokes — the other steers the whole session".

## One Human Query Becomes a Six-Query Session

**Tags:** `worked example` (blue), `research agent` (green)

- **The goal** — "is this used sedan worth buying" — the only query the human actually types
- **The fan-out** — the assistant proposes and runs five more, each attacking one angle of the goal
- **The angles** — reliability by model year, common failures at 80,000 miles, resale value after 3 years
- **Two more** — an insurance cost estimate and an open recall check round out the plan
- **The tally** — the log records 6 queries: 1 human-authored, 5 machine-authored

*Example (illustrative, italic):* The human pressed enter once; the session log shows six searches.

**Key point:** One typed goal fans into a machine-written research plan — most of the session's queries were never typed by anyone.

### Visualization (canvas `c3`, 720×300)

A small tree: the single human-typed goal on the left fanning into the five machine-written queries on the right, each connector arrowed, with the 1-vs-5 authorship tally.

- **Title (bold 15px ink, top center, y=22):** "One Goal Fans Into Five Machine Queries".
- **Right column header (bold 12px green, centered at x=557, y=40):** "machine-authored — 5 queries".
- **Root label (bold 12px blue, centered at x=130, y=112):** "human-authored — 1 query".
- **Root box (x=25, y=124, w=210, h=48, fill `rgba(42,120,214,0.12)`, 2px blue border):** bold 12px blue centered at x=130, two lines: "is this used sedan" (y=144), "worth buying?" (y=160).
- **Five machine boxes (x=420, w=275, h=28, fill `rgba(0,131,0,0.10)`, 2px green border) at y = 52, 100, 148, 196, 244:** bold 11px green centered at x=557: "reliability by model year", "common failures at 80,000 miles", "resale value after 3 years", "insurance cost estimate", "open recall check".
- **Connectors (2px green):** a line from the root's right edge (235, 148) to (412, mid) for each box mid-height, ending in a small filled green triangle arrowhead at x=420.
- **Caption (11px `#6b7280`, centered at x=360, y=292):** "the session log records 6 queries — the human typed only the first".

## The Query Log Gets a Second Author

**Tags:** `where it's used` (blue), `log authorship` (orange), `common mistake` (red)

- **Mixed authorship** — query logs increasingly blend human-typed and machine-written searches
- **Different phrasing** — machine queries run longer, form complete sentences, and never misspell
- **"What people search"** — starts to include what machines search on people's behalf
- **Two populations** — analysis of query data has to separate human demand from machine fan-out
- **The old blind spot** — picked-because-suggested and asked-alone still write identical log rows

*Example (illustrative, italic):* A trend report showing "queries are getting longer" may be measuring the assistants, not the people.

**Key point:** The query log is becoming a human-AI collaboration — read it as one population and every statistic about "what people ask" quietly drifts.

### Visualization (canvas `c4`, 720×300)

Left panel: a mixed session log with author chips, showing the human's short typo'd query next to the machine's long clean ones. Right panel: two bars comparing average words per query for the two authors (illustrative).

- **Title (bold 15px ink, top center, y=22):** "Two Authors, Two Phrasings".
- **Divider:** dashed `#bdc3c7` vertical line at x=380, y=35 to y=270 (dash 4/3).
- **Left header (bold 12px ink, centered at x=200, y=50):** "one session's log (illustrative)".
- **Left rows at y = 78, 112, 146, 180:** author chip (x=30, w=58, h=17, tint fill + 1px colored border, bold 11px colored centered text) then 11px `#2c3e50` query text at x=96. Row 1 human (blue chip, `rgba(42,120,214,0.12)`): "sedan wrth buying?". Rows 2–4 machine (green chips, `rgba(0,131,0,0.10)`): "How reliable is this sedan by model year?", "What are common failures at 80,000 miles?", "Is there an open recall on this model?".
- **Left captions (11px `#6b7280`, centered at x=200):** y=218: "short and typo'd vs long and clean"; y=236: "machine queries never misspell".
- **Right subtitle (bold 12px ink, centered at x=550, y=50):** "avg words per query (illustrative)".
- **Right bars:** baseline 1px `#999` at y=232, x=430 to x=690. Two bars w=70, 14px per word: human-typed = 4 words (blue tint fill, 2px blue border, height 56) at x=460; machine-written = 11 words (green tint, 2px green border, height 154) at x=590. Bold 13px colored labels above each bar: "4 words", "11 words"; 11px `#444` labels below the baseline at y=250: "human-typed", "machine-written".
- **Annotation (bold 12px yellow `#c98500`, centered, y=290):** "separate the two populations before reading the log".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` (no index number); subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Shared helpers `arrowDown` and `arrowRight` draw 2px lines with small filled triangle heads.
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. No red in charts (no error state on this page).
- **Data:** everything is hardcoded (no randomness): the c3 fan-out of exactly five machine queries (reliability by model year, common failures at 80,000 miles, resale value after 3 years, insurance cost estimate, open recall check) and the 1-human / 5-machine / 6-total tally; the c4 word counts 4 vs 11 — all invented numbers labeled "illustrative". Text numbers match chart numbers (1/5/6 in section 3; the five query angles listed in section 3 match the five c3 boxes).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
