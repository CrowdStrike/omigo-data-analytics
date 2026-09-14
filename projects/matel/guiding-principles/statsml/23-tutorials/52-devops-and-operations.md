# DevOps & Operations

**Page type:** grid page (tutorials category grid: single flat 4-column nav-grid of cards with topic tags)
**HTML title tag:** DevOps & Operations

**Subtitle:** How software gets shipped, watched, and kept alive in production — the pipelines, platforms, signals, and safety nets behind a running system.

## Cards

Each card links to a topic page under `devops/`. The card shows a colored uppercase subcategory label (`.card-num`), a numbered title, a one-line description, and 2-4 topic tag pills. All cards sit in one flat `.nav-grid`; the colored labels carry the grouping.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | SHIP & RUN | CI/CD | [52-devops-and-operations/01-ci-cd.md](52-devops-and-operations/01-ci-cd.md) | Every push is built and tested by an automated pipeline, and releases roll out — and roll back — as small, rehearsed steps. | pipelines, automated tests, rollbacks |
| 2 | SHIP & RUN | Containers & Orchestration | [52-devops-and-operations/02-containers-and-orchestration.md](52-devops-and-operations/02-containers-and-orchestration.md) | Ship the entire environment with the code so it runs identically anywhere — and let an orchestrator keep hundreds of copies alive. | images, kubernetes, desired state |
| 3 | SHIP & RUN | Cloud Computing — IaaS, PaaS, SaaS | [52-devops-and-operations/03-cloud-computing-iaas-paas-saas.md](52-devops-and-operations/03-cloud-computing-iaas-paas-saas.md) | Cloud service models form a rental ladder — each rung up rents one more layer of the stack, trading control for having less to run. | rental ladder, managed services, control vs convenience |
| 4 | SHIP & RUN | Serverless & FaaS | [52-devops-and-operations/04-serverless-and-faas.md](52-devops-and-operations/04-serverless-and-faas.md) | Deploy a function, not a server — the platform runs it per event, scales from zero to thousands and back, and bills by the millisecond. | functions, scale to zero, pay per use |
| 5 | OBSERVE & MEASURE | Observability | [52-devops-and-operations/05-observability.md](52-devops-and-operations/05-observability.md) | Metrics, logs, and traces each answer a different question about a failing system — IS something wrong, WHAT happened, and WHERE. | metrics, logs, traces |
| 6 | OBSERVE & MEASURE | Distributed Tracing | [52-devops-and-operations/06-distributed-tracing.md](52-devops-and-operations/06-distributed-tracing.md) | One trace ID rides along with a request across every service it touches, so the whole journey reassembles into a timed tree. | trace id, spans, slow hop |
| 7 | OBSERVE & MEASURE | Alerting Design | [52-devops-and-operations/07-alerting-design.md](52-devops-and-operations/07-alerting-design.md) | Page a human only when users are hurting — alert on symptoms like errors and latency, not causes like high CPU. | symptoms not causes, pager fatigue, on-call |
| 8 | OBSERVE & MEASURE | SLOs, SLIs & Error Budgets | [52-devops-and-operations/08-slos-slis-and-error-budgets.md](52-devops-and-operations/08-slos-slis-and-error-budgets.md) | Reliability becomes a number you agree on — measure it (SLI), target it (SLO), contract it (SLA), and spend the allowed failure like a budget. | SLI vs SLO vs SLA, error budget, reliability target |
| 9 | SURVIVE FAILURE | Incident Response & Postmortems | [52-devops-and-operations/09-incident-response-and-postmortems.md](52-devops-and-operations/09-incident-response-and-postmortems.md) | How mature teams handle outages — mitigate first, diagnose later, and never write "human error" as the root cause. | mitigate first, blameless, root cause |
| 10 | SURVIVE FAILURE | Chaos Engineering | [52-devops-and-operations/10-chaos-engineering.md](52-devops-and-operations/10-chaos-engineering.md) | Your system claims it survives a server dying — chaos engineering breaks things on purpose, in controlled conditions, to check the claim. | fault injection, controlled blast, test the claim |
| 11 | SURVIVE FAILURE | Backups & Disaster Recovery | [52-devops-and-operations/11-backups-and-disaster-recovery.md](52-devops-and-operations/11-backups-and-disaster-recovery.md) | Two numbers turn "we have backups" into engineering — how much data you can lose (RPO) and how long you can be down (RTO). | RPO, RTO, restore drills |
| 12 | SURVIVE FAILURE | Retries, Timeouts, Circuit Breakers | [52-devops-and-operations/12-retries-timeouts-circuit-breakers.md](52-devops-and-operations/12-retries-timeouts-circuit-breakers.md) | The defensive trio for every service-to-service call — a timeout so it can't hang, retries for blips, a circuit breaker for outages. | timeout, retry with backoff, fail fast |
| 13 | PLATFORM WIRING | API Gateways & Service Mesh | [52-devops-and-operations/13-api-gateways-and-service-mesh.md](52-devops-and-operations/13-api-gateways-and-service-mesh.md) | The traffic layer of microservices — the gateway guards the front door for clients coming in, the mesh manages the chatter inside. | north-south, east-west, sidecars |
| 14 | PLATFORM WIRING | Infrastructure as Code | [52-devops-and-operations/14-infrastructure-as-code.md](52-devops-and-operations/14-infrastructure-as-code.md) | Your data center declared in files — servers, networks, and permissions live in version control, and a tool makes reality match the files. | declarative, version control, drift |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorials category grid. Single page: h1, `.subtitle` paragraph, then one flat `.nav-grid` of `.nav-card` anchors (no h2 section headings).
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, 15px top margin; responsive: 3 columns below 1400px, 2 below 1100px, 1 below 600px.
- **Links:** the tables above link to the `.md` versions for markdown navigation; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">SUBCATEGORY LABEL</div>`, `<h3>N. Topic Title</h3>` (unpadded index number matching the 2-digit zero-padded file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` pills.
- **Category label colors:** applied by a small script mapping `.card-num` text to color — "SHIP & RUN" `#2980b9`, "OBSERVE & MEASURE" `#27ae60`, "SURVIVE FAILURE" `#8e44ad`, "PLATFORM WIRING" `#e67e22`; the CSS default for `.card-num` is `#2980b9`, 0.75em bold, 4px bottom margin.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`, transition on border-color/transform; hover: border `#2980b9`, `translateY(-2px)`. h3 `#1a3a4a` 1em with 6px bottom margin; description `#555` 0.85em. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em `#666`, in a flex-wrap row with 4px gap and 8px top margin.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; universal `* { margin:0; padding:0; box-sizing:border-box }` reset; h1 1.8em `#2980b9` with 10px bottom margin; subtitle `#666` 1.05em with 30px bottom margin. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22` (page accents here use `#2980b9`, `#27ae60`, `#8e44ad`, `#e67e22`).
- **Canvases:** none on this page; any canvases elsewhere in this series use `window.devicePixelRatio` scaling.
