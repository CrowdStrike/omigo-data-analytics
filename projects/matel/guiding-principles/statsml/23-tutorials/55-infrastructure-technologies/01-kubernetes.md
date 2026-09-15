# Kubernetes

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Kubernetes

**Subtitle:** Google open-sourced the ideas behind Borg so a whole cluster behaves like one computer — you declare what should be running, and controllers push reality toward it forever

## The Node That Died at 2am and Nobody Was Paged

**Tags:** `core idea` (blue), `declarative state` (green), `self-healing` (orange)

- **The declaration** — a team declares "run 3 replicas of this web-app container" and walks away
- **The loop** — a controller compares desired (3) to actual, forever; equal means do nothing
- **The failure** — at 2:00am a node dies, taking one replica with it; actual drops to 2
- **The reaction** — the controller sees 2 ≠ 3 and asks the scheduler to place a replacement pod
- **The recovery** — by 2:01am a new replica runs on a healthy node; real detection can take minutes
- **The silence** — nobody was paged; the fix was the loop doing its only job

*Example (italic):* The 2:00am node failure drops replicas from 3 to 2; the reconciliation loop restores 3 by 2:01am while the on-call engineer sleeps.

**Key point:** Kubernetes is declarative — you state the desired end state ("3 replicas"), not the steps to get there, and controllers reconcile actual toward desired every time they drift apart.

### Visualization (canvas `c1`, 720×300)

Timeline chart of the 2am incident: desired replicas (flat green line at 3) vs actual running replicas (blue line dipping to 2 and recovering), over ten minutes.

- **Title (bold 15px, `#1a5276`, top center):** "2:00am Node Failure: the Loop Restores 3 Replicas in One Minute".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = time "1:56" to "2:06" with 12px `#444` tick labels every 2 minutes; y = replicas 0 to 4, gridlines `#e5e9ef` at 1/2/3.
- **Desired line:** green `#008300` 3px dashed (dash 6/4) horizontal line at replicas = 3 across the full plot, 12px green label "desired: 3" above its left end.
- **Actual line:** blue `#2a78d6` 3px line through minutes `[0, 2, 4, 4.05, 5, 6, 8, 10]` (minute 0 = 1:56), replicas `[3, 3, 3, 2, 3, 3, 3, 3]` — vertical drop to 2 at 2:00, step back to 3 at 2:01.
- **Failure marker:** vertical dashed `#e74c3c` (dash 4/3) line at minute 4, bold 12px `#e74c3c` label "node dies 2:00am" at its top.
- **Recovery marker:** vertical dashed `#6b7280` (dash 4/3) line at minute 5, 12px `#6b7280` label "new pod running 2:01" at its top.
- **Annotation (bold 13px green `#008300`, near minute 7, y=90):** "nobody paged — the controller fixed it".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## Pods, Deployments, Services: Who Does What

**Tags:** `worked example` (blue), `reconciliation loop` (green)

- **The pod** — the unit of running: one web-app container wrapped with its own IP, e.g. pod-a
- **The deployment** — the desired-state record: "image web-app v1, replicas: 3" — nothing more
- **The service** — one stable address that load-balances across whichever 3 pods exist right now
- **Hand-check** — node-2 dies with pod-b; observe 2, desired 3, diff = 1, so schedule 1 new pod
- **The placement** — the scheduler picks node-4 (it has free CPU and memory) and starts pod-d
- **The re-check** — next loop pass observes 3 = 3 and does nothing; that is the steady state

*Example (italic):* Pods a/b/c sit on nodes 1/2/3; node-2 dies, the diff says "need 1 more", pod-d lands on node-4, and the service quietly starts routing to a, c, d.

**Key point:** The loop is just observe → diff → act: count running pods, subtract from the declared count, and schedule (or delete) exactly the difference — repeated forever.

### Visualization (canvas `c2`, 720×300)

Flow diagram of one reconciliation pass drawn as a loop: four rounded boxes connected by arrows, with the failure case taking the action branch and the healthy case short-circuiting back.

- **Title (bold 15px, `#1a5276`, top center):** "One Pass of the Loop: Observe, Diff, Act, Repeat".
- **Box style:** rounded boxes 160px wide, 46px tall, 8px radius, 12px `#2c3e50` two-line text, 3px arrows between them.
- **Box 1 (x=70, y=100), blue fill `rgba(42,120,214,0.15)`:** "DECLARED — deployment: replicas 3".
- **Box 2 (x=290, y=100), blue fill:** "OBSERVE — running pods: 2 (pod-b lost)".
- **Box 3 (x=510, y=100), orange fill `rgba(217,89,38,0.12)`:** "DIFF — 3 − 2 = 1 pod missing".
- **Box 4 (x=290, y=205), green fill `rgba(0,131,0,0.12)`:** "ACT — schedule pod-d on node-4"; arrow down from box 3 then left into box 4, and a return arrow from box 4 back up to box 2 labeled 12px `#6b7280` "loop repeats".
- **Short-circuit label (bold 12px `#008300`, near x=560, y=250):** "next pass: 3 = 3 → do nothing".
- **Service note (12px `#6b7280`, top left under title, x=70, y=70):** "service keeps routing to whichever pods exist".
- **Caption (12px `#444`, bottom right):** "diff arithmetic exact; pod names illustrative".

## From Borg to the Industry's Substrate

**Tags:** `where it's used` (blue), `Borg lineage` (orange), `scheduling` (green)

- **The ancestor** — Kubernetes descends from Borg, the cluster manager Google ran internally for years
- **The release** — Google open-sourced Kubernetes in 2014 and donated it to the CNCF in 2015
- **One computer** — the scheduler packs pods onto nodes by resource fit, so the cluster acts as one big machine
- **The packing** — a pod asks for CPU and memory; the scheduler finds a node with room, like fitting boxes in trucks
- **The substrate** — every major cloud now sells managed Kubernetes; it became the common layer teams build on
- **The payoff** — the same declare-and-reconcile idea now runs databases, batch jobs, and ML training, not just web apps

*Example (italic):* Ten pods requesting different CPU slices get packed onto 4 nodes; when a node fills up, the scheduler simply places the next pod on the emptiest one.

**Key point:** Scheduling is bin-packing: pods declare what they need, nodes advertise what they have, and the scheduler fits the first into the second — the cluster becomes one computer you never address by machine name.

### Visualization (canvas `c3`, 720×300)

Stacked horizontal bar chart of bin-packing: 4 nodes, each a 100-unit CPU bar, with colored segments for the pods packed onto it and grey for free space.

- **Title (bold 15px, `#1a5276`, top center):** "The Scheduler Packs Pods onto Nodes Like Boxes into Trucks".
- **Layout:** 4 horizontal bars at y = 70, 120, 170, 220, each 18px tall; bars start at x=130, full length 460 = 100% of node CPU; left-aligned 12px `#444` labels at x=20: "node-1", "node-2", "node-3", "node-4".
- **node-1 segments (pod CPU %):** `[30, 25, 20]` — blue `#2a78d6`, green `#008300`, aqua `#199e70`; grey `#e5e9ef` remainder 25.
- **node-2 segments:** `[40, 35]` — violet `#4a3aa7`, magenta `#d55181`; grey remainder 25.
- **node-3 segments:** `[30, 30, 25]` — orange `#d95926`, blue `#2a78d6`, green `#008300`; grey remainder 15.
- **node-4 segments:** `[20, 15]` — aqua `#199e70`, violet `#4a3aa7`; grey remainder 65.
- **Segment labels:** 11px white pod CPU numbers centered in each segment; 11px `#6b7280` "free" in each grey remainder.
- **Incoming pod:** small blue-outlined box at x=560, y=250 labeled 12px `#2c3e50` "next pod: 30 CPU", with a 2px `#1a5276` arrow pointing to node-4's free space and bold 12px `#1a5276` label "fits here".
- **Annotation (bold 13px violet `#4a3aa7`, top right near y=55):** "you address the cluster, not the machines".
- **Caption (12px `#444`, bottom right):** "CPU percentages illustrative".

## When Two VMs Would Have Done

**Tags:** `common mistake` (red), `complexity` (orange)

- **The mistake** — adopting Kubernetes for a small steady app that two VMs and a load balancer would serve
- **The price** — control plane, etcd, kubelets, networking layer, ingress, manifests: each is a thing to learn and debug
- **The tell** — traffic is flat, the team is 3 people, and deploys happen twice a week — none of it needs a scheduler
- **The famous complaint** — Kubernetes is widely called overkill for small teams; its power targets fleet-scale problems
- **The honest test** — count what reconciliation buys you; if desired state is "2 boxes, always", a health check does that
- **The middle path** — managed platforms give the self-healing without the cluster; graduate to Kubernetes when scale demands it

*Example (italic):* A 3-person team spends its first month debugging cluster networking for an app whose desired state never changes — two VMs behind a load balancer needed an afternoon.

**Common mistake:** Choosing Kubernetes because it is the industry substrate, not because the workload needs reconciliation — the loop's power is wasted on a desired state that never drifts, but its operational cost is charged in full.

### Visualization (canvas `c4`, 720×300)

Paired horizontal bar chart: moving parts to operate and setup time, for "two VMs + load balancer" vs "self-managed Kubernetes", same small app on both.

- **Title (bold 15px, `#1a5276`, top center):** "Same Small App, Two Bills: Two VMs vs a Kubernetes Cluster".
- **Group 1 — moving parts to operate (label bold 12px `#444` at x=20, y=70):** bars start at x=230, 16px tall; "two VMs: 4" green `#008300` bar width 80 at y=85 (VM ×2, load balancer, deploy script); "Kubernetes: 12" red `#e74c3c` bar width 240 at y=115 (control plane, etcd, 2 nodes, kubelets, CNI, ingress, registry, manifests, RBAC, monitoring, upgrades); 12px `#444` count labels at bar ends.
- **Group 2 — setup time, days (label bold 12px `#444` at x=20, y=175):** "two VMs: 1 day" green bar width 30 at y=190; "Kubernetes: 15 days" orange `#d95926` bar width 450 at y=220; 12px `#444` labels at bar ends.
- **Component list (11px `#6b7280`, right of the red bar, y=115):** "etcd · CNI · ingress · RBAC · upgrades ...".
- **Annotation (bold 13px magenta `#d55181`, centered near y=270):** "the loop is free; operating the loop is not".
- **Caption (12px `#444`, bottom right):** "part counts and days illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); replica timings, pod CPU percentages, part counts, and setup days are invented and labeled illustrative; the reconciliation arithmetic (3 − 2 = 1) is exact; Borg lineage, the 2014 open-sourcing, and the 2015 CNCF donation are documented facts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
