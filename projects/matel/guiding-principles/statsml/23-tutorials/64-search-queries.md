# Search Queries

**Page type:** grid page (tutorials category grid: single flat 4-column nav-grid of cards with colored category labels and topic tags — no h2 section headings)
**HTML title tag:** Search Queries

**Subtitle:** What people type into search boxes, taken apart — the kinds of queries, how they behave inside a session, and what whole populations of queries reveal.

## Cards

Each card links to a topic page under `queries/`. The card shows a colored uppercase category label (`.card-num`), a numbered title, a one-line description, and 2-4 topic tag pills. All cards sit in one `.nav-grid` — the colored card labels carry the grouping; there are no section headings.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | KINDS OF QUERIES | Query Intent Types | [64-search-queries/01-query-intent-types.md](64-search-queries/01-query-intent-types.md) | Every search wants one of three things — to reach a place, to learn something, or to get something done — and the results page is built around that guess. | navigational, informational, transactional |
| 2 | KINDS OF QUERIES | Broad & Ambiguous Queries | [64-search-queries/02-broad-and-ambiguous-queries.md](64-search-queries/02-broad-and-ambiguous-queries.md) | One word like "jaguar" carries several meanings at once — engines show every major sense and let clicks vote on the mix. | multiple intents, diversification, click votes |
| 3 | KINDS OF QUERIES | Topic Queries vs Question Queries | [64-search-queries/03-topic-queries-vs-question-queries.md](64-search-queries/03-topic-queries-vs-question-queries.md) | "rome hotels" names a topic; "which hotel is closest to the colosseum" asks a question — one wants a list, the other wants an answer. | keywords vs questions, direct answers, voice search |
| 4 | KINDS OF QUERIES | Copy-Paste Queries | [64-search-queries/04-copy-paste-queries.md](64-search-queries/04-copy-paste-queries.md) | A developer pastes an error message straight into the search box — long, exact, once-only queries where matching the exact string is the whole game. | error messages, debugging, exact match |
| 5 | KINDS OF QUERIES | Queries Without Keywords | [64-search-queries/05-queries-without-keywords.md](64-search-queries/05-queries-without-keywords.md) | A job feed matches you without a search box — your profile is compiled into an elaborate query, the same trick behind ads, dating matches, and "more like this". | profile as query, compiled queries, matching |
| 6 | QUERY BEHAVIOR | Search Sessions & Refinements | [64-search-queries/06-search-sessions-and-refinements.md](64-search-queries/06-search-sessions-and-refinements.md) | One goal, several tries — a sitting of queries grows more specific until a click ends it, and every rewrite says what the last results were missing. | sessions, rewrites, refinement chains |
| 7 | QUERY BEHAVIOR | Navigation & Filters as Queries | [64-search-queries/07-navigation-and-filters-as-queries.md](64-search-queries/07-navigation-and-filters-as-queries.md) | Clicking Menswear → Shoes → Running and checking two filter boxes states a complete query without typing a word. | browse paths, facets, filters |
| 8 | QUERY BEHAVIOR | Autocomplete | [64-search-queries/08-autocomplete.md](64-search-queries/08-autocomplete.md) | The engine proposes the query before you finish typing — saving keystrokes, fixing spelling, and steering what gets searched. | suggestions, popularity, steering |
| 9 | QUERY BEHAVIOR | Intent Filters & Clarification Chips | [64-search-queries/09-intent-filters-and-clarification-chips.md](64-search-queries/09-intent-filters-and-clarification-chips.md) | Type "red shoes" and size chips appear; type something broad and the page asks which aspect you meant — clicked chips are query words in disguise. | generated filters, clarification cards, chip clicks |
| 10 | QUERY BEHAVIOR | AI-Suggested Next Queries | [64-search-queries/10-ai-suggested-next-queries.md](64-search-queries/10-ai-suggested-next-queries.md) | The machine proposes the next whole query — related searches, follow-up questions, and research agents that write and run queries on your behalf. | related searches, follow-up questions, agent-run queries |
| 11 | QUERY POPULATIONS | Head, Torso & Tail Queries | [64-search-queries/11-head-torso-and-tail-queries.md](64-search-queries/11-head-torso-and-tail-queries.md) | A handful of queries repeat millions of times while millions of queries appear only once — and each zone needs a different method. | zipf shape, frequency, rare queries |
| 12 | QUERY POPULATIONS | Queries Across Domains | [64-search-queries/12-queries-across-domains.md](64-search-queries/12-queries-across-domains.md) | The same person types differently in web search, a store, a map, and a video app — length, vocabulary, and intent shift with the search box. | web vs e-commerce, maps, video search |
| 13 | QUERY POPULATIONS | Query Trends & Culture | [64-search-queries/13-query-trends-and-culture.md](64-search-queries/13-query-trends-and-culture.md) | Rising and falling query volume traces seasons, events, and places — what a city searches most says what it cares about. | trends, seasonality, cities & countries |
| 14 | QUERY POPULATIONS | The Query Log as a Dataset | [64-search-queries/14-the-query-log-as-a-dataset.md](64-search-queries/14-the-query-log-as-a-dataset.md) | Every query carries a time and a place — a demand stream with forecastable surges, local pockets, and searches for things you don't have yet. | forecasting, locality, missing inventory |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorials category grid. Single page: h1, `.subtitle` paragraph, then ONE flat `.nav-grid` of `.nav-card` anchors directly after the subtitle — no `<h2>` subcategory headings; the colored card labels carry the grouping.
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, 15px top margin; responsive: 3 columns below 1400px, 2 below 1100px, 1 below 600px.
- **Links:** the table above links to the `.md` versions for markdown navigation; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">CATEGORY LABEL</div>`, `<h3>N. Topic Title</h3>` (unpadded index number matching the 2-digit zero-padded file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` pills.
- **Category label colors:** applied by a small script mapping `.card-num` text to color — "KINDS OF QUERIES" `#2980b9`, "QUERY BEHAVIOR" `#27ae60`, "QUERY POPULATIONS" `#e67e22`; the CSS default for `.card-num` is `#2980b9`, 0.75em bold, 4px bottom margin.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`, transition on border-color/transform; hover: border `#2980b9`, `translateY(-2px)`. h3 `#1a3a4a` 1em with 6px bottom margin; description `#555` 0.85em. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em `#666`, in a flex-wrap row with 4px gap and 8px top margin.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; h1 1.8em `#2980b9`; subtitle `#666` 1.05em. No nav bar, no back/home links.
