# Programming Languages

**Page type:** grid page (card navigation grid, auto-fit columns min 300px)
**HTML title tag:** Programming Languages — Discussion Backlog

**Subtitle:** Each language that mattered brought one unique idea — what it is, why it won its niche, and what trade-off it accepted.

## Cards

Each card links to a detail page under `programming-languages/`. The card shows a colored uppercase category label, an unnumbered title, a one-sentence description, and a row of topic tag pills. Cards have no index numbers in their h3 titles.

| # | Category | Title | Link | Description | Tags |
|---|----------|-------|------|-------------|------|
| 1 | PROCEDURAL | C | [programming-languages/01-c.md](programming-languages/01-c.md) | Procedural, closest to the metal — the language kernels and embedded systems are still written in. | procedural, low-level, kernel |
| 2 | OBJECT ORIENTED | C++ | [programming-languages/02-cpp.md](programming-languages/02-cpp.md) | OOP on C's foundation — templates, RAII, zero-cost abstractions for high-performance complex systems. | OOP, templates, memory-management |
| 3 | VIRTUAL MACHINE | Java | [programming-languages/03-java.md](programming-languages/03-java.md) | Simplest OOP with virtual memory management — interpreter + JIT compiler, disciplined and portable. | JVM, GC, enterprise |
| 4 | PLUMBING | Python | [programming-languages/04-python.md](programming-languages/04-python.md) | Plumbing language with simple syntax — glues C libraries together, owns ML and scripting. | glue, ML, scripting |
| 5 | WEB BROWSER | JavaScript | [programming-languages/05-javascript.md](programming-languages/05-javascript.md) | Simple Java-like syntax for the browser — became the only language the frontend runs natively. | frontend, event-loop, ubiquitous |
| 6 | MICROSERVICES | Go | [programming-languages/06-go.md](programming-languages/06-go.md) | Event streams and microservices — no exceptions, channels, small functions, highly modular. | concurrency, channels, microservices |
| 7 | MEMORY MANAGEMENT | Rust | [programming-languages/07-rust.md](programming-languages/07-rust.md) | Memory safety without garbage collection — ownership model eliminates use-after-free at compile time. | ownership, safety, systems |
| 8 | FUNCTIONAL | Scala | [programming-languages/08-scala.md](programming-languages/08-scala.md) | Best of Java and FP — built for data pipelines, reactive systems, actors, and creating DSLs. | functional, JVM, Spark, Akka |
| 9 | STATISTICS | R | [programming-languages/09-r.md](programming-languages/09-r.md) | Statistical computing and science — C wrapper focused on stats, not designed as a general language. | statistics, science, ggplot |
| 10 | HISTORICAL | Notable Languages — Influential but Not Mainstream | [programming-languages/10-notable-languages.md](programming-languages/10-notable-languages.md) | Assembly, COBOL, Lisp, Perl, D, Lua, Erlang, Haskell, OCaml — each brought an idea, none held the mainstream. | historical, academic, niche |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** nav-grid style (see `docs/statsml/ui-templates/02-nav-grid`). Single page: h1, `.subtitle` paragraph, then one `.nav-grid` of `.nav-card` anchors.
- **Layout:** `.nav-grid` is CSS grid, `repeat(auto-fit, minmax(300px, 1fr))`, 16px gap, margin-top 15px.
- **Links:** the table above links to the `.md` versions for navigation in markdown; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num" style="color:CATEGORY_COLOR">CATEGORY</div>`, `<h3>Title</h3>` (no index numbers in titles on this page), `<p>description</p>`, and `<div class="topics">` holding one `<span class="topic-tag">` per tag.
- **Category label colors** (inline style per card): PROCEDURAL, OBJECT ORIENTED, MEMORY MANAGEMENT `#1a5276`; VIRTUAL MACHINE, MICROSERVICES `#27ae60`; PLUMBING `#e67e22`; WEB BROWSER `#2980b9`; FUNCTIONAL `#8e44ad`; STATISTICS `#e74c3c`; HISTORICAL `#95a5a6`.
- **Card style:** background `#f8f9fa`, border `1px solid #e0e0e0`, radius 4px, padding 20px; hover: border `#2980b9`, `translateY(-2px)`. `.card-num` 0.75em weight 600; h3 `#1a5276` 1em; description `#555` 0.85em; `.topic-tag` pills background `#eaf2f8`, color `#1a5276`, radius 4px, padding 2px 8px, 0.72em weight 600.
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`, padding-bottom 8px; subtitle `#666` 0.95rem. No nav bar, no back/home links.
- **Palette:** #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange. Canvases (none on this page) would use `window.devicePixelRatio` scaling.
