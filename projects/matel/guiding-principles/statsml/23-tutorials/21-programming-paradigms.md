# Programming Paradigms

**Page type:** grid page (tutorials category grid: single flat 4-column nav-grid)
**HTML title tag:** Programming Paradigms

**Subtitle:** Different ways of thinking about how a program should be told what to do — each one changes how you write, read, and reason about code.

## Cards

Each card links to a topic page under `programming-paradigms/`. The card shows a colored uppercase subcategory label (`.card-num`), a numbered title, a one-line description, and a row of topic-tag pills. All cards sit in one flat `.nav-grid`; the colored labels carry the grouping.

| # | Category | Title | Link | Description | Tags |
|---|----------|-------|------|-------------|------|
| 1 | TELLING THE COMPUTER | Imperative vs Declarative | [21-programming-paradigms/01-imperative-vs-declarative.md](21-programming-paradigms/01-imperative-vs-declarative.md) | Spell out every step, or describe the result you want and let the system figure out the steps. | how vs what, step-by-step, core idea |
| 2 | TELLING THE COMPUTER | SQL as Declarative Programming | [21-programming-paradigms/02-sql-as-declarative-programming.md](21-programming-paradigms/02-sql-as-declarative-programming.md) | A query says which rows you want, never how to fetch them — the database plans the work itself. | SQL, query planner, worked example |
| 3 | TELLING THE COMPUTER | Functional Style | [21-programming-paradigms/03-functional-style.md](21-programming-paradigms/03-functional-style.md) | Build programs from small functions that take input and return output without changing anything else. | pure functions, map/filter, no side effects |
| 4 | TELLING THE COMPUTER | Object-Oriented, Intuitively | [21-programming-paradigms/04-object-oriented-intuitively.md](21-programming-paradigms/04-object-oriented-intuitively.md) | Bundle data with the actions that belong to it, so each thing in the program knows how to behave. | objects, methods, encapsulation |
| 5 | DATA-ORIENTED STYLES | Vectorized Thinking | [21-programming-paradigms/05-vectorized-thinking.md](21-programming-paradigms/05-vectorized-thinking.md) | Operate on whole columns of numbers at once instead of looping over them one value at a time. | arrays, no loops, numpy-style |
| 6 | DATA-ORIENTED STYLES | Dataflow & Pipelines | [21-programming-paradigms/06-dataflow-and-pipelines.md](21-programming-paradigms/06-dataflow-and-pipelines.md) | Chain small stages so data flows from one step to the next, like items moving along an assembly line. | stages, chaining, ETL |
| 7 | DATA-ORIENTED STYLES | MapReduce: Split, Work, Combine | [21-programming-paradigms/07-mapreduce-split-work-combine.md](21-programming-paradigms/07-mapreduce-split-work-combine.md) | Divide a huge job across many workers, let each handle its piece, then merge the partial answers. | divide & conquer, parallel, big data |
| 8 | DATA-ORIENTED STYLES | Immutability | [21-programming-paradigms/08-immutability.md](21-programming-paradigms/08-immutability.md) | Never change data in place — make a fresh copy with the change, so nothing silently shifts under you. | no in-place edits, copies, safe sharing |
| 9 | REACTING TO THE WORLD | Event-Driven Programming | [21-programming-paradigms/09-event-driven-programming.md](21-programming-paradigms/09-event-driven-programming.md) | The program sits idle until something happens — a click, a message, a sensor reading — then responds. | events, handlers, triggers |
| 10 | REACTING TO THE WORLD | Reactive Patterns | [21-programming-paradigms/10-reactive-patterns.md](21-programming-paradigms/10-reactive-patterns.md) | Declare how values depend on each other, and downstream results update automatically when inputs change. | streams, auto-update, spreadsheet-like |
| 11 | REACTING TO THE WORLD | Polling vs Push | [21-programming-paradigms/11-polling-vs-push.md](21-programming-paradigms/11-polling-vs-push.md) | Keep asking "anything new yet?" on a timer, or wait quietly and let the source notify you when there is. | check vs notify, latency, trade-offs |
| 12 | REACTING TO THE WORLD | Async: Don't Wait, Get Called Back | [21-programming-paradigms/12-async-dont-wait-get-called-back.md](21-programming-paradigms/12-async-dont-wait-get-called-back.md) | Start a slow task, keep doing other work, and pick up the result later instead of standing still. | non-blocking, callbacks, await |
| 13 | NOBODY WROTE THE CODE | Prompt-Based Systems | [21-programming-paradigms/13-prompt-based-systems.md](21-programming-paradigms/13-prompt-based-systems.md) | One sentence of prose serves as function spec, service contract, and task spec at once — with no checker for any of them. | prompt, no schema, quiet failures |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorials category grid page (nav-grid style, see `docs/statsml/ui-templates/02-nav-grid`). Single page: h1, `.subtitle` paragraph, then one flat `.nav-grid` of `.nav-card` anchors (no h2 section headings).
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, 15px top margin; responsive: 3 columns below 1400px, 2 below 1100px, 1 below 600px.
- **Links:** the tables above link to `.md` versions for markdown navigation; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead (files zero-padded, e.g. `programming-paradigms/01-imperative-vs-declarative.html`).
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">SUBCATEGORY</div>`, `<h3>N. Title</h3>` (unpadded index number matching the file index, running 1..13 across the whole page), `<p>description</p>`, and `<div class="topics">` with one `<span class="topic-tag">` per tag.
- **Category label colors** (applied by a small script mapping `.card-num` text to color): TELLING THE COMPUTER `#2980b9`, DATA-ORIENTED STYLES `#27ae60`, REACTING TO THE WORLD `#8e44ad`, NOBODY WROTE THE CODE `#d95926`; default `.card-num` color `#2980b9`.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`; hover: border `#2980b9`, `translateY(-2px)`. `.card-num` 0.75em bold; h3 `#1a3a4a` 1em; description `#555` 0.85em. `.topic-tag`: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em, `#666`; `.topics` is flex with 4px gap, 8px top margin.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; h1 1.8em `#2980b9`; subtitle `#666` 1.05em. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- No canvases on this page; where canvases appear elsewhere in this project they use `window.devicePixelRatio` scaling.
