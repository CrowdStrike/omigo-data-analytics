# RAG: Giving the Model a Reference Book

**Page type:** detail page (tutorial layout: `.card-section` blocks, each an h2 + two-column `table.layout`, text left 50%, canvas right 50%)
**HTML title tag:** RAG: Giving the Model a Reference Book

**Subtitle:** Search your documents first, paste the best paragraph into the prompt, and the model answers from that page instead of from memory

## "What's Our Refund Policy?" — A Question the Model Cannot Know

**Tags:** `core idea` (blue), `running example` (green)

- **The question** — a customer asks your company chatbot "What's our refund policy?"
- **The problem** — the model never saw your handbook; it was trained on the public internet
- **Without help** — it answers a plausible generic policy, stated with full confidence
- **RAG step 1: retrieve** — search your documents for the paragraph that matches the question
- **RAG step 2: generate** — paste that paragraph into the prompt, right above the question
- **The answer** — now quotes your real rule: 30 days refund, store credit to day 90

*Example:* Asked cold, the model said "most companies offer 14 days" — plausible, confident, and wrong.

**Key point:** RAG = retrieve, then generate. The model answers from a page you hand it, not from memory.

### Visualization (canvas `c1`, 720×300)

Two horizontal flow diagrams: memory-only vs retrieve-then-generate.

- **Title (bold 15px, `#1a5276`, centered):** "The Same Question, With and Without the Reference Page".
- **Top flow ("from memory:", bold 12px mute `#6b7280`):** box (40, 60) 150×40, stroke mute, fill `#f4f6f8`, 11px text: "\"What's our" / "refund policy?\"" → mute arrow → box (262, 60) 90×40 mute/`#f4f6f8` labeled "model" → mute arrow → box (424, 60) 256×40, stroke red `#e74c3c`, fill `rgba(231,76,60,0.05)`, bold 11px red: "\"Most companies offer 14 days.\"" plus 11px red: "confident, generic, wrong".
- **Bottom flow ("retrieve, then generate:", bold 12px green `#008300`):** box (40, 146) 110×44 stroke blue `#2a78d6` fill `rgba(42,120,214,0.06)`: "same" / "question" → blue arrow → box (198, 146) 100×44 stroke violet `#4a3aa7` fill `rgba(74,58,167,0.06)`, bold 11px violet: "search the" / "handbook" → violet arrow → box (346, 140) 150×56 stroke aqua `#199e70` fill `rgba(25,158,112,0.06)`, 11px monospace aqua: "\"Full refund within" / " 30 days; store credit" / " day 31–90.\"", captioned below in bold 11px aqua: "chunk pasted into prompt" → aqua arrow → box (542, 146) 70×44 mute/`#f4f6f8` labeled "model" → green downward arrow → box (424, 222) 256×44, stroke green `#008300`, fill `rgba(0,131,0,0.05)`, bold 11px green: "\"Full refund within 30 days, store credit" / "to day 90 — per Returns policy §2.\" ✓".
- **Annotation (bold 13px orange `#d95926`, centered at x=232, y=250):** "same model — the pasted paragraph is the whole difference".

## One Retrieval You Can Check by Hand

**Tags:** `worked example` (green)

- **The library** — the handbook is split into 40 paragraph-sized chunks ahead of time
- **The search** — the question is compared to every chunk by meaning, not exact words
- **The scores** — Refunds & returns 0.82, Shipping times 0.41, Warranty claims 0.28
- **The paste** — the winning chunk goes into the prompt right above the question
- **The answer** — "Full refund within 30 days; store credit to day 90" citing that chunk

*Example:* "Can I get my money back?" lands on the same 0.82 chunk — the match is by meaning, not by the word "refund".

**Key point:** The model never searched anything — your code found the paragraph; the model just read it and answered.

### Visualization (canvas `c2`, 720×300)

Left: horizontal similarity-score bars; right: the assembled prompt. Vertical dashed divider at x=400 (`#bdc3c7`, dash 4/3).

- **Title (bold 15px, `#1a5276`, centered):** "Search Scores Over the Handbook Chunks — Top One Gets Pasted".
- **Left — score bars** (heading bold 12px ink: "match score vs the question (0–1)"): three horizontal bars starting at x=190, full width 180, height 24, row spacing 52, alpha 0.75, right-aligned labels and bold 13px value labels after each bar: "Refunds & returns" 0.82 (green `#008300`), "Shipping times" 0.41 (mute `#6b7280`), "Warranty claims" 0.28 (mute). Below in bold 12px green: "winner — pasted into the prompt", with a green arrow pointing from that note up to the winning bar.
- **Right — assembled prompt** (heading bold 12px ink: "the assembled prompt:"): box (425, 62) 265×150, stroke blue `#2a78d6`, fill `rgba(42,120,214,0.05)`, 11px monospace lines — mute: "Answer using this excerpt:"; aqua `#199e70`: "[Returns policy §2]" / "\"Full refund within 30 days" / " of purchase; store credit" / " from day 31 to day 90.\""; text `#2c3e50`: "Question: What's our" / "refund policy?". Green downward arrow beneath, then bold 11px green: "→ answer quotes 30 / 90 days, cites §2".
- **Caption (bold 13px orange `#d95926`, centered at x=210, y=288):** "scores are illustrative — the ranking is the point".

## The Whole Class of Hallucinations It Kills

**Tags:** `where it's used` (blue), `best practice` (green)

- **Private data** — the model knows nothing behind your firewall; retrieval brings it in
- **Fresh answers** — policy changed last month? update the document, not the model
- **Checkable** — a cited chunk lets a human verify the answer in ten seconds
- **Measured** — on 50 policy questions: plain model 31 correct, with retrieval 47
- **The 3 misses** — all three were retrieval misses: the right chunk never reached the prompt

*Example:* Confident invented policies simply disappear once the real text sits inside the prompt.

**Key point:** Most RAG failures are search failures — when the right chunk is retrieved, the model rarely fumbles it.

### Visualization (canvas `c3`, 720×300)

Two-bar comparison chart.

- **Title (bold 15px, `#1a5276`, centered):** "50 Policy Questions: Correct Answers (illustrative)".
- **Bars** (plot x=140, width 440, baseline y=230, chart height 160, y max 55, bar width 140, alpha 0.75): "plain model (memory only)" 31 in mute `#6b7280`, note below: "19 wrong — mostly invented policies"; "with retrieval (RAG)" 47 in green `#008300`, note: "3 wrong — all retrieval misses". Bold 16px value labels "31 / 50" and "47 / 50" above bars in the bar's color; labels 12px `#2c3e50`, notes 11px mute.
- **Reference line:** dashed (6/4) mute horizontal line at 50 labeled bold 12px "all 50" at the right.
- **Baseline:** thin `#999` line spanning the plot.
- **Annotation (bold 13px orange `#d95926`, bottom center):** "the surviving errors are search errors, not model errors".

## RAG Is Not Retraining

**Tags:** `common mistake` (red), `watch out` (orange)

- **No learning** — the model is unchanged; the paragraph rides along in the prompt each time
- **Garbage in** — retrieve the shipping chunk and the model answers refunds with shipping rules
- **Stale library** — RAG answers from your documents; if they are outdated, so is the answer
- **Context limit** — you paste the few best chunks, not the whole handbook
- **Debug the search** — when the answer is wrong, look at what was retrieved before blaming the model

*Example:* When an old 2024 policy chunk outranked the new one, the model politely cited the wrong year.

**Common mistake:** The model faithfully answers from whatever you paste — retrieval quality, not model smarts, sets the ceiling.

### Visualization (canvas `c4`, 720×300)

Flow diagram of a failed retrieval producing a confident wrong answer, plus a debug-order note.

- **Title (bold 15px, `#1a5276`, centered):** "When Retrieval Fails, the Model Cites the Wrong Page Faithfully".
- **Flow:** box (40, 60) 150×44, stroke blue `#2a78d6`, fill `rgba(42,120,214,0.06)`: "\"What's our" / "refund policy?\"" → violet arrow → box (242, 48) 190×68, stroke red `#e74c3c`, fill `rgba(231,76,60,0.05)`: bold 11px red "retrieved: Shipping times", 11px monospace text "\"Orders ship within" / " 5 business days...\"", and bold 11px red beneath: "(the refunds chunk was never fetched)" → mute arrow → box (484, 60) 90×44 mute/`#f4f6f8` labeled "model" → red downward arrow → box (370, 156) 310×52, stroke red, fill `rgba(231,76,60,0.05)`, bold 11px red: "\"Refunds are handled within 5 business days," / "per our shipping policy.\" — fluent and wrong".
- **Debug note (left, at x=45):** bold 12px green `#008300` "debug order:", then 12px text lines: "1. look at what was retrieved" / "2. fix chunking / search" / "3. only then blame the model".
- **Annotations (centered):** bold 13px orange `#d95926`: "the model answers from whatever you paste — retrieval sets the ceiling"; 12px mute: "and nothing was learned: the next request starts from the same blank memory".

## Regeneration instructions

- **Layout:** tutorial page — `<h1>` + `.subtitle`, then 4 `.card-section` blocks; each has an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` with one row: left `td.text-col` (50%) holding `.tags` pills, a `<ul>` of bullets each opening with a `<b>` term (`#1a5276`), an italic `.example` paragraph, and a `.key-point` callout; right `td.viz-col` (50%) holding one `<canvas>` 720×300 (CSS `width:100%`, `1px solid #e0e0e0` border, 4px radius).
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; bullets 0.92rem; inline `code` in ui-monospace on `#f4f6f8`. No nav bar, no back/home links.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue = `rgba(26,82,118,0.12)`/`#1a5276`, green = `rgba(39,174,96,0.15)`/`#27ae60`, red = `rgba(231,76,60,0.12)`/`#e74c3c`, orange = `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvas:** each canvas declares intrinsic `width="720" height="300"`; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Shared `box()` (fill + 1.5px stroke rect) and `arrow()` (2px line with filled triangular head) helpers. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange. All numbers are hardcoded literal arrays (no `Math.random()`); invented numbers carry an "illustrative" label.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
