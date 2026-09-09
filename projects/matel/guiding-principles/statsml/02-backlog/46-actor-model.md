# Actor Model

**Page type:** detail page (backlog-style 2-col text/viz layout: numbered h2 sections, text left ~45%, canvas right ~55%)
**HTML title tag:** Actor Model

**Subtitle:** Concurrency through isolated message-passing actors — no shared state, no locks, failure as a first-class concept

## Intro callout

**Core tension:** Shared-memory concurrency (threads + locks) is the default mental model but breaks at scale — deadlocks, race conditions, non-reproducible bugs. The actor model eliminates shared state entirely: each actor owns its state, communicates only via async messages, and can supervise/restart failed children. Trade-off: simplicity of reasoning vs message-ordering complexity and potential mailbox overflow.

## 1. Key Concepts

- **Actor = state + behavior + mailbox.** Processes one message at a time — no concurrency within an actor.
- **Let it crash:** Don't defend against every failure inside the actor. Let it die, let the supervisor restart it cleanly.
- **Supervision trees:** Hierarchical failure handling — parent decides child's fate. Isolates blast radius.
- **Location transparency:** Sending a message to a local actor looks identical to sending to a remote one. Distribution is a deployment decision, not a code decision.

### Visualization (canvas `c1`, 720×320)

Diagram: four actor boxes exchanging async messages.

- **Title (bold 14px `#1a5276`, top center):** "Actor = state + behavior + mailbox; only messages cross the boundary"
- **Actor glyph** (130×76): left strip 26px wide is the mailbox — fill `rgba(230,126,34,0.15)`, orange `#e67e22` 1.2px stroke, three horizontal divider lines suggesting queue slots; body fill `rgba(26,82,118,0.08)` with 1.5px blue `#1a5276` stroke, bold 12px name at top, 11px `#666` lines "private state" and "behavior" below.
- **Actors:** "Actor A" at (70,120), "Actor B" at (300,60), "Actor C" at (300,190), "Actor D (remote)" at (540,120).
- **Message arrows** (orange, 2px, filled triangular heads): A→B mailbox, A→C mailbox, B→D mailbox, C→D mailbox. Orange 11px labels: "async msg" (twice, between A and B/C) and two-line "same API," / "remote node" near the D arrows.
- **Mailbox label:** bold orange 11px right-aligned "mailbox (FIFO queue) →" pointing at Actor A's mailbox strip.
- **Caption (13px `#444`, bottom center):** "Each actor drains its mailbox one message at a time — no locks, no shared memory, no data races"

## 2. Questions to Resolve

Table (`.ex-table`, columns Question / Consideration):

| Question | Consideration |
|----------|---------------|
| Granularity | One actor per feature? Per pipeline stage? Per dataset? What's the natural unit? |
| Supervision strategy | Restart, escalate, stop, or resume on failure? Per-actor or hierarchy-wide? |
| Message ordering | FIFO per sender-receiver pair guaranteed? What about across multiple senders? |
| Backpressure | Unbounded mailbox (risk overflow) vs bounded (risk deadlock) vs pull-based |
| State persistence | In-memory only (fast, volatile) vs event-sourced (recoverable, slower) |
| Location transparency | Same-process vs distributed actors — API identical, failure modes differ |

### Visualization (canvas `c2`, 720×320)

Diagram: three-level supervision tree with a crashing worker.

- **Title (bold 14px `#1a5276`, top center):** "Supervision tree: parent decides the child's fate, blast radius stays local"
- **Nodes** (120×36 boxes, fill `rgba(26,82,118,0.08)`, bold 12px centered label): "root supervisor" at center-x 360 y=50 (blue); "supervisor 1" at 180 y=130 and "supervisor 2" at 540 y=130 (blue); workers at y=215: "worker" at 90, "worker (crash)" at 270 (crashed style: red `#e74c3c` stroke, fill `rgba(231,76,60,0.12)`), "worker" at 460, "worker" at 620 — worker boxes stroked green `#27ae60`. Gray `#999` 1.5px lines connect root→supervisors and supervisors→their two workers.
- **Crash mark:** large red X (2.5px strokes) over the crashed worker.
- **Failure escalation:** red arrow from the crashed worker's link up toward supervisor 1, red 11px label "failure escalates".
- **Restart:** green dashed segment then green arrow from supervisor 1 back down to the crashed worker; bold green 11px right-aligned label "restart with clean state".
- **Captions:** 13px `#444` centered: "\"Let it crash\": the worker dies, its siblings and the other subtree never notice"; below it 12px `#999`: "strategy per supervisor: restart, escalate, stop, or resume — applied to one child or all"

## 3. Implementations

Table (`.ex-table`, columns System / Language / Notable Properties):

| System | Language | Notable Properties |
|--------|----------|--------------------|
| Erlang/OTP | Erlang, Elixir | Battle-tested supervision, hot code reload, telecom-grade uptime |
| Akka | Scala, Java | Typed actors, cluster sharding, persistence |
| Orleans | C# | Virtual actors (auto-activated), grain-based |
| Ray | Python | ML-focused, task/actor hybrid, distributed scheduling |

### Visualization (canvas `c4`, 720×300)

Timeline chart: four implementations plotted by release year.

- **Title (bold 14px `#1a5276`, top center):** "Same model, four decades of implementations — from telecom switches to ML clusters"
- **Axis:** horizontal gray `#999` line at y=170, spanning x=65 to x=675, mapping years 1985–2020; tick marks and 11px labels at 1985, 1995, 2005, 2015.
- **Points** (6px filled dots on the axis, dashed leader line up or down to a bold 13px name plus 11px `#666` "tag · year" line):
  - 1986 Erlang/OTP, blue `#1a5276`, above axis, tag "telecom uptime · 1986"
  - 2009 Akka, green `#27ae60`, below axis, tag "JVM clustering · 2009"
  - 2015 Orleans, orange `#e67e22`, above axis, tag "virtual actors · 2015"
  - 2017 Ray, red `#e74c3c`, below axis, tag "ML workloads · 2017"
- **Caption (13px `#444`, bottom center):** "The abstraction predates its current uses by 30 years — the workloads changed, the model did not"

## 4. Anti-Patterns

- Actors that share state through a back-channel (DB, file, global variable)
- Synchronous request-response disguised as async messages (recreates locks)
- God actor that routes everything (single point of failure, bottleneck)
- No backpressure — fast producer overwhelms slow consumer's mailbox
- Ignoring message ordering assumptions across actor boundaries

### Visualization (canvas `c3`, 720×320)

Diagram: two anti-patterns side by side.

- **Title (bold 14px `#1a5276`, top center):** "Two ways to break the model without noticing"
- **Left — back-channel state:** actor glyphs "Actor A" at (40,70) and "Actor B" at (40,190); a red-bordered box (90×46 at x=230 y=140, fill `rgba(231,76,60,0.08)`) with bold red two-line label "shared DB /" / "global var"; red arrows from both actors into the box; red X mark at (210,163). Captions centered at x=200: bold red 12px "back-channel state"; 11px `#999` "shared memory smuggled back in".
- **Right — god actor:** red-stroked circle (radius 42, fill `rgba(231,76,60,0.10)`) at (545,128) labeled "GOD" / "ACTOR" in bold red 12px; six small blue satellite boxes (48×28, fill `rgba(26,82,118,0.08)`) at (430,65), (660,65), (415,160), (675,160), (470,240), (620,240), each with a gray `#888` arrow pointing into the circle; red X mark below the circle. Captions centered at x=545: bold red 12px "everything routes through one actor"; 11px `#999` "single point of failure, mailbox bottleneck".

## Status footer

Status: stub. Needs brainstorming session. (small gray `#999` 12px text at page bottom)

## Regeneration instructions

- **Template:** backlog detail-page layout — h1 with 2px `#2980b9` bottom border, `.subtitle`, `.intro` callout (background `#f0f4f8`, left border `3px solid #2980b9`, 8px 12px padding, 0.9rem), then one `.lang-section` per numbered section: h2 (1.3rem `#1a5276`, 2px `#2980b9` bottom border) followed by a `table.layout` row with `td.text-col` (45%) holding bullets/tables and `td.viz-col` (55%) holding the canvas. Note canvas order in HTML source is c1, c2, c4 (Implementations), c3 (Anti-Patterns).
- **Table style:** `.ex-table` — full width, collapsed borders, 0.88em; `th` background `#1a5276` white text; `td` 6px 8px padding, `1px solid #ddd` border; even rows `#f8f9fa`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `ul` 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; grays `#444`/`#666`/`#888`/`#999`.
- **Canvas:** intrinsic width 720, heights 320 (c1–c3) and 300 (c4); a shared `setup(id, h)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; shared helpers draw actor glyphs and angled message arrows with filled triangular heads.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
