# Causal Chains: What Enabled What

**Page type:** other (single-page long doc: intro callout, one full-width dependency-graph canvas, a stack of `.chain` boxes each with a title + monospace arrow flow + mermaid diagram slot + description paragraph, one purple "waited" list callout, one closing caution callout)
**HTML title tag:** Causal Chains in Technology

**Subtitle:** The industry's history read as dependencies rather than dates — which constraint lifted, and what became possible the moment it did.

## Intro callout (blue chain box, background #f0f4f8, left border 4px solid #2980b9)

**Why this is a separate view:** a timeline says two things happened in the same decade. It cannot say that one of them was the reason the other stopped being impossible. Most of the significant jumps in computing were not new ideas — the idea was often decades old and sitting in a paper nobody could act on. What changed was a constraint: something became cheap, fast, standard or abundant, and an idea that had been theoretically correct all along abruptly became practical. Reading the history this way makes the pattern usable: when you see a constraint about to lift, you can often predict which dormant idea is next.

### Visualization (canvas `graph`, responsive width × 614 CSS px)

Layered dependency graph (DAG): 18 labeled boxes arranged in 6 horizontal layers × 4 column slots, connected by downward bezier "enabled" arrows.

- **Canvas sizing:** width is responsive — `max(clientWidth, 720)` CSS px; height computed as TOP(34) + 6 layers × 92px + BOT(28) = 614 CSS px. Redraws on window resize (120ms debounce). Wrapped in `.viz-wrap` (100% width, horizontal scroll); canvas CSS `width:100%; min-width:720px`.
- **Layout constants:** 6 layers, 4 slots; layer height 92px, top pad 34px, bottom pad 28px, horizontal pad 14px; column width = (cssW − 28)/4; box width = min(colW − 18, 190), box height 40.
- **Nodes** (id, label, layer, slot):
  - Layer 0: `games` "Games / 3D demand" (slot 0); `storage` "Cheap commodity storage" (slot 1); `web` "The web" (slot 2); `virt` "Virtualisation" (slot 3)
  - Layer 1: `gpu` "GPUs at volume" (slot 0); `hadoop` "Distributed storage" (slot 1); `corpora` "Web-scale corpora" (slot 2); `cloud` "Rented compute" (slot 3)
  - Layer 2: `cuda` "CUDA / GPGPU" (slot 0); `nosql` "NoSQL, schema-on-read" (slot 1); `lm` "Language modelling" (slot 2); `saas` "SaaS & subscriptions" (slot 3)
  - Layer 3: `dl` "Deep learning" (slot 0); `attn` "Attention / transformers" (slot 2)
  - Layer 4: `infer` "Cheap inference" (slot 1); `gen` "Code & content generation" (slot 3)
  - Layer 5: `feed` "Recommendation feeds" (slot 1); `agents` "Coding agents" (slot 3)
- **Edges** (drawn behind boxes, bezier curves, stroke `rgba(160,64,0,0.42)` width 1.4, filled arrowhead `rgba(160,64,0,0.72)` at target box top): games→gpu, gpu→cuda, cuda→dl, dl→attn, storage→hadoop, hadoop→nosql, nosql→corpora, web→corpora, corpora→lm, lm→attn, virt→cloud, cloud→saas, cloud→dl, attn→infer, attn→gen, dl→infer, infer→feed, gen→agents, saas→gen.
- **Node style:** near-white fill `rgba(255,255,255,0.96)` rect, 1.4px stroke colored per layer from palette `['#1d4ed8','#0369a1','#0f766e','#15803d','#92400e','#b91c1c']` (layer index mod 6); label text in same layer color, font `600 12.5px -apple-system`, centered, wrapped to two lines when wider than box − 14px.
- **Caption (`.viz-note`, below canvas, 0.88em #555):** "Arrows read as "enabled". Layers are rough eras. Only the load-bearing links are drawn — nearly everything here has additional causes not shown."

## The Enabling Chains

Each chain is a `.chain` box (background #f8fbfe, border 1px solid #cfe0f0, radius 6px, padding 14px 18px) containing: bold `.chain-title`, a monospace `.chain-flow` line with orange (#a04000) bold arrows, an empty `.mermaid-slot` div, and an `.era-desc` paragraph. Mermaid diagrams are rendered from JS-held sources into the slots at load; when mermaid is ready, `body.mermaid-ready` hides the `.chain-flow` fallback text. If the CDN is blocked/offline, slots stay empty and the monospace text remains visible.

### Chain 1: Cheap commodity storage → distributed data → schema-on-read

**Flow:** Falling disk cost per byte → keeping everything cheaper than choosing → Hadoop / HDFS → NoSQL and schema-on-read

**Mermaid diagram (graph LR):** nodes "Disk cost&lt;br/&gt;per byte falls" → "Keeping everything&lt;br/&gt;beats choosing" → "Hadoop / HDFS" → "NoSQL,&lt;br/&gt;schema-on-read"; plus a faded side branch "Disk cost per byte falls" → "Normalisation&lt;br/&gt;stops paying" → "NoSQL, schema-on-read". Faded classDef: fill #fdfaf5, stroke #c8a165, color #8a6d3b.

This is the clearest example of economics driving architecture. Relational normalisation was not a preference, it was a response to storage being expensive: you removed duplication because you could not afford it. Once commodity disks made the petabyte affordable, the cost of deciding what to keep exceeded the cost of keeping it all, and the tradeoff inverted. Schema-on-read followed necessarily — if you store data before knowing the questions, you cannot impose a schema at write time. NoSQL did not win on elegance; it won because the constraint that justified strict schemas had lifted.

### Chain 2: Graphics demand → parallel arithmetic hardware → deep learning

**Flow:** Video games and 3D → GPUs sold at consumer volume → CUDA makes them programmable → deep networks become trainable

**Mermaid diagram (graph LR):** "Games / 3D&lt;br/&gt;demand" → "GPUs at&lt;br/&gt;consumer volume" → "CUDA makes them&lt;br/&gt;programmable" → "Deep networks&lt;br/&gt;become trainable"; plus an idea-styled node "Backprop, 1986&lt;br/&gt;waiting 25 years" → "Deep networks become trainable". Idea classDef: fill #fdf6ff, stroke #7e22ce, color #6c3483.

The most cited case, and the most instructive. Backpropagation was published in usable form in 1986 and the underlying mathematics is older still. Nothing conceptual was missing for roughly twenty-five years — what was missing was the arithmetic throughput to train a large network in a tolerable amount of time. That throughput arrived as a side effect of an unrelated consumer market: millions of people buying graphics cards for games funded hardware that happened to be extremely good at the dense matrix multiplication neural networks need. CUDA then made it addressable for general computation. The theory waited on an accident of the games industry.

### Chain 3: Digitised text at web scale → statistical language modelling → transformers

**Flow:** The web → crawlable text corpora → statistical then neural language models → attention and scaling

**Mermaid diagram (graph LR):** "The web" → "Crawlable&lt;br/&gt;text corpora" → "Statistical, then&lt;br/&gt;neural language models" → "Attention&lt;br/&gt;and scaling"; plus an other-styled node "GPUs" → "Attention and scaling". Other classDef: fill #fff6ef, stroke #d35400, color #a04000.

Language models are trained on text that had to exist in machine-readable form first. Before the web there was no corpus of remotely sufficient size, so language modelling was necessarily rule-based and hand-built. The web produced the raw material as a by-product of people publishing for other reasons entirely. Attention then removed the sequential bottleneck in recurrent models, which mattered specifically because it made training parallelisable — and parallelisable training is only valuable if you already have the corpus and the GPUs. Three enablers had to be in place at once.

### Chain 4: Clock speeds stall → concurrency moves into software

**Flow:** Physical limits on frequency scaling around 2004 → multicore instead of faster cores → concurrency primitives in languages → Go, Rust, async everywhere

**Mermaid diagram (graph LR):** "Frequency scaling&lt;br/&gt;hits power wall" → "More cores&lt;br/&gt;instead of faster" → "Concurrency moves&lt;br/&gt;into languages" → "Go, Rust,&lt;br/&gt;async everywhere"; plus side branch "More cores instead of faster" → "Immutability&lt;br/&gt;becomes practical" → "Go, Rust, async everywhere".

For roughly thirty years, software got faster by waiting. Programmers wrote sequential code and the next processor generation ran it faster, so parallelism was a specialist concern. When heat and power limits stopped frequency scaling, the free improvement ended and the extra transistors went into more cores instead. That pushed a hardware problem directly into language design: goroutines, async/await and ownership models are all responses to a physical ceiling, not to a change in taste. A generation of language features exists because of thermodynamics.

### Chain 5: Virtualisation → renting compute → startups without capital expenditure

**Flow:** Hardware virtualisation → one machine safely shared → compute rented by the hour → cloud, and startups with no servers

**Mermaid diagram (graph LR):** "Hardware&lt;br/&gt;virtualisation" → "One machine&lt;br/&gt;safely shared" → "Compute rented&lt;br/&gt;by the hour" → "Cloud, and startups&lt;br/&gt;with no servers"; plus branch "Compute rented by the hour" → "Serverless:&lt;br/&gt;unit is an invocation".

Virtualisation made it safe to run untrusted tenants on shared hardware, which is what made metered compute sellable. The downstream effect was economic rather than technical: launching a service stopped requiring capital for machines, which changed who could start a company and how failure was priced. The entire venture pattern of trying many cheap experiments depends on a hypervisor. Serverless is the same chain taken one step further, with the unit shrinking from a machine to an invocation.

### Chain 6: Distributed version control → contribution without permission → modern open source

**Flow:** Git's cheap branching and offline commits → forking as a normal act → GitHub and the pull request → open source at present scale

**Mermaid diagram (graph LR):** "Cheap branching,&lt;br/&gt;offline commits" → "Forking becomes&lt;br/&gt;a normal act" → "GitHub and&lt;br/&gt;the pull request" → "Open source&lt;br/&gt;at present scale"; plus later-styled branch "GitHub and the pull request" → "Reviewable history" → "Coding agents,&lt;br/&gt;2021+". Later classDef: fill #f4fbf6, stroke #27ae60, color #1e8449.

Centralised version control required commit access, so contributing meant being granted permission by a maintainer. Distributed history made a fork a first-class operation rather than a hostile act, and the pull request turned an unsolicited change into a reviewable proposal. The social convention that now governs most open source is downstream of a data structure decision about how history is stored.

### Chain 7: Smartphone components → sensors become cheap → entire product categories

**Flow:** Phone manufacturing volume → cheap cameras, accelerometers, GPS, radios → drones, wearables, IoT, ride hailing

**Mermaid diagram (graph LR):** "Phone manufacturing&lt;br/&gt;volume" → "Cheap cameras, GPS,&lt;br/&gt;accelerometers, radios" which fans out to three nodes: "Drones", "Wearables, IoT", "Ride hailing".

Component prices fall with volume, and phones bought sensors in quantities no other product could. A drone is largely phone parts in a different arrangement; so is a fitness tracker. Ride hailing needed accurate location in every pocket, which was not a mapping breakthrough but a consequence of GPS chips becoming nearly free. One dominant product category subsidised the bill of materials for several others.

### Chain 8: Always-on connectivity → software as a service → the subscription

**Flow:** Home broadband and mobile data → assuming a network → server-side software → continuous delivery and subscription pricing

**Mermaid diagram (graph LR):** "Broadband and&lt;br/&gt;mobile data" → "Software can&lt;br/&gt;assume a network" → "Code lives on&lt;br/&gt;the vendor's server" which fans out to "Continuous delivery" and "Subscription pricing"; then "Subscription pricing" → "Purchase becomes&lt;br/&gt;tenancy".

When connectivity was intermittent, software had to work offline, which meant shipping it to the customer's machine and therefore selling versions. Once being online became the default, the code could live on the vendor's server — which changed the release model to continuous, the pricing model to recurring, and the relationship from purchase to tenancy. Boxed software did not die of obsolescence; its distribution assumption stopped holding.

### Chain 9: Recommendation quality → reach decoupled from followers → short video

**Flow:** Cheap inference at serving time → ranking by predicted watch time → distribution independent of the social graph → short-form video feeds

**Mermaid diagram (graph LR):** "Cheap inference&lt;br/&gt;at serving time" → "Rank by predicted&lt;br/&gt;watch time" → "Distribution leaves&lt;br/&gt;the social graph" which fans out to "Short-form&lt;br/&gt;video feeds" and "Reach decoupled&lt;br/&gt;from followers".

For a decade, reach on a platform was a function of accumulated followers, which advantaged incumbents and old accounts. Serving a model cheaply enough to rank candidate videos per user per request replaced the graph as the distribution mechanism, so a new account could reach millions immediately. This was the first serious break in the social graph's moat, and it happened because inference got cheap, not because anyone redesigned the social network.

### Chain 10: Code generation → familiarity stops selecting languages → legacy revival

**Flow:** Capable code generation → writing unfamiliar syntax costs less → well-documented older languages become viable again → migration of long-frozen systems

**Mermaid diagram (graph LR):** "Capable code&lt;br/&gt;generation" → "Unfamiliar syntax&lt;br/&gt;costs less to write" → "Familiarity stops&lt;br/&gt;selecting languages" which fans out to "Large-corpus languages&lt;br/&gt;gain: SQL, JS, COBOL" and "Long-frozen systems&lt;br/&gt;become migratable"; then the latter → open-styled node "Outcome still&lt;br/&gt;unsettled". Open classDef: fill #fffaf0, stroke #e67e22, color #a04000, stroke-dasharray 4 3.

Language choice has always been partly a labour-market question: teams picked what they could hire for and already knew. When generating correct syntax in a language you do not know well becomes cheap, that constraint weakens, and languages with large public corpora — SQL, JavaScript, even COBOL — gain rather than lose. This chain is still in progress and the outcome is genuinely unsettled, unlike the ones above.

## Ideas That Waited for an Enabler

In each case the concept was published, understood and essentially correct long before it became useful. Nothing was wrong with the theory — the surrounding conditions had not arrived. The gap column is roughly how long the idea sat available and unused.

Purple `.waited` callout (background #fdf6ff, left border 4px solid #7e22ce) containing a bullet list; each bullet ends with a `.lag` pill (0.78em bold, color #6c3483, background #f4ecfa, border 1px solid #d7bde2, radius 3px):

- **Neural networks and backpropagation** — formulated in the 1970s–80s, unusable at depth until GPU arithmetic and large labelled datasets arrived [lag pill: ~25 years]
- **Virtual machines and hypervisors** — IBM ran virtualised mainframes in the late 1960s; the idea only became load-bearing when commodity x86 hardware and metered billing made renting compute a business [lag pill: ~35 years]
- **Hypertext** — described in the 1940s and demonstrated in the 1960s, dormant until cheap networking and a simple markup format made it deployable [lag pill: ~45 years]
- **The relational model** — published in 1970 and initially dismissed as too slow; it needed faster random access and better optimisers before it could beat hand-navigated records [lag pill: ~10 years]
- **Functional programming** — Lisp dates from 1958; immutability and higher-order functions returned to the mainstream only when multicore made shared mutable state genuinely painful [lag pill: ~50 years]
- **Attention over recurrence** — the shortest gap here, and the exception that shows the pattern: the corpora and the accelerators were already waiting, so adoption took a few years rather than decades [lag pill: ~3 years]
- **Reinforcement learning** — the theory is from the 1980s; it needed cheap simulation and function approximators before it could act on problems of real size [lag pill: ~30 years]

## How to Read Causal Claims Here

Orange caution callout (`.chain` box with background #fffaf0, left border 4px solid #e67e22):

These chains are simplifications and should be treated as such. Three specific cautions. **First, causes are plural:** deep learning needed GPUs, but also labelled datasets, better initialisation, and activation functions that did not saturate — naming one enabler is a convenience, not a complete account. **Second, the direction is easy to get backwards:** demand for deep learning now drives accelerator design, so what began as hardware enabling software became mutual. **Third, hindsight flatters:** at the time, each of these looked like one option among several, and the paths not taken are invisible now precisely because they failed. The pattern worth keeping is not any single arrow but the general shape — that available theory tends to accumulate faster than the conditions to use it, so the binding constraint is usually not the idea.

## Regeneration instructions

- **Layout:** single long page in document order: h1, `.subtitle`, blue intro `.chain` callout, `.viz-wrap` canvas + `.viz-note`, h2 "The Enabling Chains" then ten `.chain` boxes, h2 "Ideas That Waited for an Enabler" with intro paragraph + `.waited` list, h2 "How to Read Causal Claims Here" with orange caution `.chain` box. h2s have `border-bottom: 2px solid #2980b9`, padding-bottom 8px.
- **Chain box structure:** `.chain` (background #f8fbfe, border 1px solid #cfe0f0, radius 6px, padding 14px 18px, margin 14px 0) → `.chain-title` (bold, #1a5276, 1.06em), `.chain-flow` (ui-monospace, 0.88em, #1f618d, line-height 1.9, with `<span class="arrow">→</span>` separators in #a04000 bold), empty `.mermaid-slot`, `.era-desc` (#333).
- **Mermaid rendering:** ES module script imports mermaid@11 from jsdelivr CDN; `startOnLoad:false`, `securityLevel:'strict'`, `theme:'base'`, flowchart `{curve:'basis', nodeSpacing:30, rankSpacing:44, padding:6, useMaxWidth:true}`, themeVariables: background #ffffff, primaryColor #eef5fb, primaryBorderColor #2980b9, primaryTextColor #1a5276, lineColor #a04000, fontSize 13px. Diagram sources live in a JS array (one per chain, document order, all `graph LR`); each is rendered with a unique id (`mmd`+index) into the corresponding `.mermaid-slot`, then `body.mermaid-ready` is added, which hides `.chain-flow` text and gives slots margin 6px 0 12px. On failure (blocked CDN/offline) the try/catch leaves slots empty and monospace fallback text visible. `.mermaid-slot svg { max-width:100%; height:auto; display:block; }`.
- **Canvas:** the `graph` canvas uses `window.devicePixelRatio` scaling (backing store multiplied by dpr, `ctx.setTransform(dpr,0,0,dpr,0,0)`), responsive width with resize redraw as specced above.
- **Other styles:** `.waited` purple callout and `.lag` pill as described; `.gone` (background #fff5f4, left border 4px solid #c0392b) and `.badge-gone` styles exist in CSS but are unused on this page; `.obj-table` styles (th background #1a5276 white text, td border 1px solid #cfe0f0, even-row background #f7fbff) and `.era-years`/`.era-title`/`.era-langs` also defined but unused here.
- **Page style:** body system sans-serif, white background, text #2a2a2a, padding 40px 20px, line-height 1.6, font-size 15px; h1 1.8em #1a5276; h2 1.3em #1a5276; subtitle #666 1.0em; `strong` #1a5276. No nav bar, no back/home links.
- **Palette:** primary blue #1a5276, green #27ae60, red #e74c3c, orange #e67e22; graph layer strokes #1d4ed8/#0369a1/#0f766e/#15803d/#92400e/#b91c1c; arrow orange #a04000.
- Regenerated HTML pages link with `.html` extensions (this page has no card links).
