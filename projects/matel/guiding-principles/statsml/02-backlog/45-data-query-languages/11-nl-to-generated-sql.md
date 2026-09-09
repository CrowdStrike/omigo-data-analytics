# 11. NL to Generated SQL

**Page type:** detail page (kusto-style 2-col text/viz layout: intro callout, numbered h2 sections each with text left 45% / canvas or code right 55%)
**HTML title tag:** 11. NL to Generated SQL

**Subtitle:** Plain language as input — it still compiles to SQL, not a new language

**Intro callout:** Natural language is an input to the pipeline, not a new link in it. It resolves by being translated into SQL and executed normally. The failure mode is not a syntax error — the query runs cleanly and answers a different question than the one asked.

## 1. How It Works

- **Pattern:** A Large Language Model translates a natural language question into a SQL query, which executes against a database
- **Role:** The LLM acts as a compiler from ambiguous intent to precise structured query
- **Still SQL:** Natural language is just a new front-end to the same execution engine
- **Hard part:** The hard problem is semantic correctness, not syntax

**Key point:** The failure mode is not a syntax error — the generated query runs cleanly and answers a different question than the one asked.

### Visualization (canvas `c1`, 720×300)

Pipeline flow diagram with a silent-failure path in red.

- **Title (bold 14px, top center, `#1a5276`):** "Pipeline: The Failure Is Semantic, Not Syntactic".
- **Boxes (white fill, 2px colored stroke, 13px `#222` centered labels, height 44px, top y=95):** "NL question" at x=20 w=120 (`#1a5276`); "LLM" at x=180 w=90 (`#1a5276`); "generated SQL" at x=310 w=130 (`#1a5276`); "engine" at x=480 w=90 (`#1a5276`); "answer" at x=610 w=95 (`#27ae60`).
- **Arrows:** solid, width 2, with filled arrowheads, between consecutive boxes — blue `#1a5276` for the first three, green `#27ae60` into "answer".
- **Sub-labels (13px `#444`, below boxes):** "ambiguous intent" under the NL question box; "precise query" under the generated SQL box.
- **Failure path:** red (`#e74c3c`) dashed (dash 6/4, width 2) polyline dropping from the bottom of the LLM box (x=225) down to y=235, across to x=657, then a solid red arrow back up into the bottom of the "answer" box.
- **Failure annotation (bold 13px red, two lines starting at x=245):** "misread intent: SQL runs cleanly, no error —" / "but answers a different question".

## 2. Where It Fits

- **Strength:** Zero SQL knowledge needed; fast exploration and iteration; democratizes data access
- **Strength:** Good for simple, common query patterns; reduces data-team bottleneck for routine questions
- **Weakness:** Wrong queries run silently — no syntax error when semantics are wrong; hard to validate results without SQL knowledge
- **Weakness:** Schema context limits accuracy; hallucinated column/table names; ambiguous questions get confident but arbitrary interpretations; fails on complex joins and business logic
- **Use case:** Business-user self-service analytics, chat-with-your-data products, rapid query prototyping, reducing ticket load for simple questions

*Example: the conversion-rate query below runs without error, yet computes over all users ever created instead of the conversion window.*

### Code block (in viz column, above canvas `c2`)

```
-- User asks: "How many users signed up last month?"

-- Generated SQL (looks correct):
SELECT COUNT(*) FROM users
WHERE created_at >= '2024-02-01'
  AND created_at < '2024-03-01';
-- Result: 3,847 ✓

-- User asks: "What's our conversion rate?"

-- Generated SQL (subtly wrong):
SELECT
    COUNT(CASE WHEN purchased = true THEN 1 END) * 100.0
    / COUNT(*) AS conversion_rate
FROM users;
-- Result: 4.2%

-- Problem: includes ALL users ever created, not just
-- those in the conversion window. The real rate for
-- recent cohorts is 11.3%. Query ran cleanly.
-- No error. Wrong answer. Confident delivery.
```

### Visualization (canvas `c2`, 720×300)

Two-bar comparison of delivered vs intended conversion rate.

- **Title (bold 14px, top center, `#1a5276`):** "\"What's our conversion rate?\" — Delivered vs Intended".
- **Plot area:** padding top 50, bottom 60, left 70, right 260; axes stroked `#999` 1px; y-axis labeled "14%" at top and "0%" at bottom (13px `#444`); y scale 0–14.
- **Bars (width 120px, gap 90px, starting 40px right of the y-axis):**
  - "generated SQL: all users ever" — 4.2% — red `#e74c3c`
  - "intended: recent cohorts" — 11.3% — green `#27ae60`
- **Value labels:** bold 14px `#222` centered above each bar ("4.2%", "11.3%"); bar labels 12px `#222` below the axis.
- **Side annotations (right of plot):** bold 13px orange (`#e67e22`): "same question, 2.7x apart"; below it, 13px `#444`, two lines: "both queries ran without error;" / "only one answers what was asked".

## Takeaway (key-point callout, full width at page bottom)

**The takeaway:** The dangerous failure of NL-to-SQL is silent — the generated query runs cleanly and delivers a confident answer to a different question than the one asked.

## Regeneration instructions

- **Layout:** backlog detail page. h1 with 2px `#2980b9` bottom border, `.subtitle`, then an `.intro` callout (background `#f0f4f8`, left border 4px solid `#2980b9`, padding 12px 16px, 0.95rem). Each numbered section is a `.bias-section` (margin-bottom 40px) with an h2 (1.3rem `#1a5276`, 2px `#2980b9` bottom border) and a `table.layout` (width 100%, td padding 12px): left `td.text-col` 45% holds bullets + `.key-point` + `.example`, right `td.viz-col` 55% holds the canvas (section 2 also has a `<pre>` code block above its canvas). A final full-width `.key-point` takeaway sits after the sections. The h1 carries the index number "11." matching the file index.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `.key-point` background `#f8f9fa` with left border 3px solid `#e74c3c`, 0.9rem; `.example` italic `#555` 0.9rem; ul 0.92rem; `<pre>` background `#f8f9fa`, 1px `#e0e0e0` border, radius 4px, padding 12px, 0.85rem, 'SF Mono'/Consolas monospace. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300, CSS `width: 100%` with 1px `#e0e0e0` border and 4px radius; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`.
- Any card links in regenerated HTML use `.html` extensions.
