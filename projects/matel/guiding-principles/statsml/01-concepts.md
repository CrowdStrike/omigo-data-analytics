# Concepts — Theoretical Foundations

**Page type:** grid page (nav-card grid, 4 columns, cards with topic tags)
**HTML title tag:** Concepts — Theoretical Foundations for Data Science

**Subtitle:** Theoretical foundations — focused learning material for the ideas behind omigo data science.

## Cards

Every card links to the page itself (`01-concepts.html` — placeholder self-links; no detail pages exist yet). Each card shows a colored uppercase category label, a numbered title, a one-to-two sentence description, and a row of topic tags.

| # | Category | Title | Link | Description | Topics |
|---|----------|-------|------|-------------|--------|
| 1 | STATISTICS | Statistical Tests | [01-concepts.md](01-concepts.md) | Inference, distributions, hypothesis testing, estimation, experimental design. | inference, hypothesis, estimation |
| 2 | STATISTICS | A/B Testing | [01-concepts.md](01-concepts.md) | Experiment design, sample size, statistical power, guardrail metrics, peeking problems. | sample size, power, guardrails |
| 3 | STATISTICS | Data Distribution | [01-concepts.md](01-concepts.md) | How real-world data is shaped — bell curves, fat tails, spikes, U-shapes, and why the shape tells you what model to use (or avoid). | shape recognition, real-world patterns, model selection |
| 4 | ML | Machine Learning | [01-concepts.md](01-concepts.md) | Supervised, unsupervised, feature selection, model evaluation, generalization. | supervised, unsupervised, evaluation |
| 5 | AI | AI | [01-concepts.md](01-concepts.md) | Neural networks, deep learning, transformers, agents, reasoning systems. | deep learning, transformers, agents |
| 6 | ML | Perceptron | [01-concepts.md](01-concepts.md) | Linear classifiers, convergence theorem, margin, kernel trick, from single neuron to networks. | linear, convergence, kernel |
| 7 | ML | Markov Chains | [01-concepts.md](01-concepts.md) | State transitions, stationary distributions, absorbing states, mixing time, hidden Markov models. | stochastic, stationary, HMM |
| 8 | ML | Viterbi Algorithm | [01-concepts.md](01-concepts.md) | Dynamic programming, sequence decoding, trellis graphs, MAP estimation, speech/NLP applications. | dynamic programming, decoding, MAP |
| 9 | GRAPH | Graph Algorithms | [01-concepts.md](01-concepts.md) | Traversal, shortest path, PageRank, community detection, graph embeddings. | traversal, PageRank, embeddings |
| 10 | GRAPH | Graph Mining | [01-concepts.md](01-concepts.md) | Frequent subgraph discovery, motifs, influence propagation, link prediction, knowledge graphs. | motifs, link prediction, knowledge graph |
| 11 | ENGINEERING | Engineering | [01-concepts.md](01-concepts.md) | Software engineering principles, architecture, systems thinking, scalability. | architecture, systems, scalability |
| 12 | PATTERNS | Design Patterns | [01-concepts.md](01-concepts.md) | Strategy, pipeline, registry, observer, factory — patterns in data systems. | strategy, pipeline, registry |
| 13 | CONCURRENCY | Actor Model | [01-concepts.md](01-concepts.md) | Message passing, supervision trees, location transparency, fault tolerance, Erlang/Akka. | message passing, supervision, fault tolerance |
| 14 | DISTRIBUTED | Eventual Consistency | [01-concepts.md](01-concepts.md) | CAP theorem, consensus, conflict resolution, CRDTs, distributed state. | CAP, CRDTs, consensus |
| 15 | MESSAGING | Queues | [01-concepts.md](01-concepts.md) | FIFO, priority queues, dead-letter queues, backpressure, at-least-once vs exactly-once delivery. | backpressure, delivery, DLQ |
| 16 | MESSAGING | Message Bus | [01-concepts.md](01-concepts.md) | Pub/sub, event-driven architecture, topics, partitions, Kafka, RabbitMQ, decoupling. | pub/sub, event-driven, Kafka |
| 17 | DISTRIBUTED | CAP Theorem | [01-concepts.md](01-concepts.md) | Consistency, availability, partition tolerance — tradeoffs, PACELC, real-world system choices. | tradeoffs, PACELC, partition |
| 18 | DISTRIBUTED | Vector Clocks | [01-concepts.md](01-concepts.md) | Logical time, causality tracking, conflict detection, version vectors, happens-before relation. | causality, version vectors, happens-before |
| 19 | DISTRIBUTED | Distributed Locking | [01-concepts.md](01-concepts.md) | Mutual exclusion, fencing tokens, Redlock, lease-based locks, split-brain safety. | mutex, fencing, Redlock |
| 20 | DISTRIBUTED | Leader Election | [01-concepts.md](01-concepts.md) | Bully algorithm, Raft, Paxos, ZooKeeper, consensus-based leadership, failover. | Raft, Paxos, failover |
| 21 | ARCHITECTURE | REST (Fielding's Architectural Constraints) | [01-concepts.md](01-concepts.md) | Fielding's 6 architectural constraints — why they produce scalability, evolvability, and independent deployability. | constraints, connectors, RESTful services |
| 22 | PIPELINE | ETL | [01-concepts.md](01-concepts.md) | Extract-transform-load, orchestration, idempotency, lineage, batch vs streaming. | orchestration, idempotency, lineage |
| 23 | PIPELINE | SEDA Architecture | [01-concepts.md](01-concepts.md) | Staged event-driven architecture, thread pools per stage, adaptive load shedding, bounded queues. | staged, load shedding, bounded |
| 24 | PIPELINE | Scaling Patterns | [01-concepts.md](01-concepts.md) | Horizontal vs vertical scaling, sharding, replication, partitioning strategies, auto-scaling policies. | sharding, replication, auto-scale |
| 25 | PIPELINE | Data Organization | [01-concepts.md](01-concepts.md) | Partitioning schemes, bucketing, indexing, data lakes vs warehouses, catalog management. | partitioning, indexing, catalog |
| 26 | TECHNOLOGY | Spark | [01-concepts.md](01-concepts.md) | Distributed compute, RDDs, DataFrames, lazy evaluation, partitioning, shuffle optimization. | distributed, lazy eval, shuffle |
| 27 | TECHNOLOGY | S3 | [01-concepts.md](01-concepts.md) | Object storage, eventual consistency, partitioning schemes, lifecycle policies, access patterns. | object storage, lifecycle, consistency |
| 28 | TECHNOLOGY | Akka | [01-concepts.md](01-concepts.md) | Actor model, message passing, supervision, clustering, streams, reactive systems. | actors, clustering, reactive |
| 29 | TECHNOLOGY | Parquet | [01-concepts.md](01-concepts.md) | Columnar storage, predicate pushdown, schema evolution, compression, row groups. | columnar, pushdown, compression |
| 30 | LANGUAGE | Programming Languages | [01-concepts.md](01-concepts.md) | Type systems, memory models, concurrency primitives, paradigms, runtime tradeoffs. | type systems, concurrency, paradigms |
| 31 | LANGUAGE | Query Languages | [01-concepts.md](01-concepts.md) | SQL, graph queries, dataframe DSLs, query optimization, declarative vs imperative. | SQL, optimization, declarative |
| 32 | ML | Gradient Descent | [01-concepts.md](01-concepts.md) | Iterative optimization via negative gradients — learning rate, convergence, saddle points, stochastic vs batch tradeoffs. | optimization, learning rate, convergence |
| 33 | PIPELINE | Data Lineage | [01-concepts.md](01-concepts.md) | Tracking data from source to output — provenance, dependency graphs, impact analysis, debugging transformations. | provenance, dependency graph, impact analysis |
| 34 | GOVERNANCE | Data Compliance | [01-concepts.md](01-concepts.md) | Regulatory requirements shaping data systems — GDPR, CCPA, retention policies, right to deletion, audit trails. | GDPR, retention, audit trail |
| 35 | STATISTICS | Bayesian A/B Testing | [01-concepts.md](01-concepts.md) | Bayesian approach to experimentation — priors, posteriors, credible intervals, probability of being best, when it beats frequentist testing and when it doesn't. | priors, credible intervals, early stopping |
| 36 | ML | Multi-Armed Bandits | [01-concepts.md](01-concepts.md) | Explore-exploit algorithms and their real-life challenges — delayed rewards, non-stationary arms, batch updates, attribution, why textbook regret bounds rarely hold in production. | explore-exploit, Thompson sampling, non-stationarity |
| 37 | GRAPH | Graph Theory | [01-concepts.md](01-concepts.md) | The mathematical foundations — paths, cycles, connectivity, coloring, matching, planarity, spectral properties. | connectivity, coloring, matching |
| 38 | MATH | Combinatorics | [01-concepts.md](01-concepts.md) | Counting without enumerating — permutations, combinations, inclusion-exclusion, pigeonhole, generating functions, cardinality explosions. | counting, inclusion-exclusion, pigeonhole |
| 39 | REASONING | Logical Thinking | [01-concepts.md](01-concepts.md) | Structured reasoning — deduction, contrapositives, necessary vs sufficient conditions, common logical fallacies in technical arguments. | deduction, necessary vs sufficient, fallacies |
| 40 | REASONING | Induction | [01-concepts.md](01-concepts.md) | Reasoning from cases to general claims — mathematical induction, inductive generalization from examples, real-life instances and where each breaks down. | base case, generalization, real-life examples |
| 41 | LANGUAGE | Compilers and Interpreters | [01-concepts.md](01-concepts.md) | How code becomes execution — lexing, parsing, ASTs, bytecode, JIT compilation, compile-time vs runtime trade-offs. | parsing, AST, JIT |
| 42 | THEORY | P and NP-Complete | [01-concepts.md](01-concepts.md) | Computational complexity — P vs NP, reductions, NP-completeness, and recognizing intractable problems before trying to solve them exactly. | complexity classes, reductions, intractability |
| 43 | ARCHITECTURE | Client vs Server Architecture | [01-concepts.md](01-concepts.md) | Where computation and state live — thick vs thin clients, request-response vs push, offline behavior, trust boundaries, evolution from mainframes to edge. | thick vs thin client, state, trust boundary |
| 44 | PIPELINE | Realtime Analytics | [01-concepts.md](01-concepts.md) | Analytics under latency constraints — streaming vs micro-batch, windowing, approximate counting, sketches, lambda vs kappa architectures. | streaming, windowing, sketches |
| 45 | GRAPH | Graph Analytics and Visualization | [01-concepts.md](01-concepts.md) | Making graphs readable and queryable — layout algorithms, community rendering, large-graph sampling, interactive exploration tools. | layouts, large graphs, exploration |
| 46 | VISUALIZATION | Visualization Libraries | [01-concepts.md](01-concepts.md) | Survey of charting libraries — d3, matplotlib, plotly, ECharts, canvas vs SVG vs WebGL, declarative vs imperative APIs, pros and cons by use case. | d3, canvas vs SVG, declarative APIs |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** nav-grid style (see `docs/statsml/ui-templates/02-nav-grid`). Single page: h1, `.subtitle` paragraph, then one `.nav-grid` of `.nav-card` anchors. No callout on this page.
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, margin-top 15px; responsive: 3 columns below 1400px, 2 columns below 1100px, 1 column below 600px.
- **Links:** the table above links to `.md` versions for navigation in markdown; in the regenerated HTML, every card's `href` is `01-concepts.html` (self-link placeholders) with an `.html` extension.
- **Card structure:** `<a class="nav-card" href="01-concepts.html">` containing `<div class="card-num" style="color:CATEGORY_COLOR">CATEGORY</div>`, `<h3>N. Title</h3>` (unpadded index number), `<p>description</p>`, and `<div class="topics">` holding one `<span class="topic-tag">` per topic.
- **Category label colors (inline style on `.card-num`):** STATISTICS, MATH, REASONING `#2980b9`; ML, AI `#e67e22`; GRAPH, THEORY `#8e44ad`; ENGINEERING, PATTERNS, CONCURRENCY, DISTRIBUTED, MESSAGING, ARCHITECTURE `#1a5276`; PIPELINE, GOVERNANCE `#27ae60`; TECHNOLOGY, VISUALIZATION `#e74c3c`; LANGUAGE `#795548`.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, box-shadow `0 2px 4px rgba(0,0,0,0.05)`, transition on border-color/transform; hover: border `#1a5276`, `translateY(-2px)`. `.card-num` 0.75em bold; h3 `#1a3a4a` 1em; description `#555` 0.85em. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em, `#666`, flex-wrap with 4px gap, margin-top 8px.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em, margin-bottom 30px. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Canvases:** none on this page; site-wide canvases use `window.devicePixelRatio` scaling.
