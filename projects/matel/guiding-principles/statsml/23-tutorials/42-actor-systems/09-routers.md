# Routers

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Routers

**Subtitle:** A router is one actor address that quietly forwards each message to a pool of identical workers — senders talk to one name, many hands do the work

## One Address, Four Resize Workers

**Tags:** `core idea` (blue), `one address, many workers` (green), `routing strategies` (orange)

- **The service** — a photo site resizes every upload; one actor at `/user/resizer` accepts the jobs
- **The bottleneck** — a single actor processes one message at a time, so uploads queue behind it
- **The router** — `/user/resizer` becomes a router holding four identical routee workers behind it
- **The forward** — each incoming job is handed to one routee; senders never learn workers exist
- **The strategies** — round-robin takes turns, smallest-mailbox picks the least busy, broadcast copies to all
- **Broadcast use** — one "reload watermark logo" message fans out so all four workers update at once

*Example (italic):* Twelve resize jobs arrive at `/user/resizer`; round-robin deals them like cards, so worker 1 gets jobs 1, 5, 9 and worker 4 gets jobs 4, 8, 12.

**Key point:** A router splits one actor address from one unit of work: senders keep a single stable name while the router forwards each message to one of many interchangeable routees.

### Visualization (canvas `c1`, 720×300)

Flow diagram: three uploader boxes on the left send jobs to one router box, which fans out to four worker boxes on the right with their round-robin job assignments.

- **Title (bold 15px, `#1a5276`, top center):** "One Address In, Four Workers Out".
- **Uploaders:** three rounded boxes 110×34 at x=20, y=80/135/190, fill `rgba(42,120,214,0.15)`, 12px `#2c3e50` labels "uploader A" / "uploader B" / "uploader C"; 2px `#6b7280` arrows from each to the router box.
- **Router box:** rounded box 170×56 centered at x=260, y=122, fill `rgba(74,58,167,0.12)`, 2px violet `#4a3aa7` border, bold 13px `#4a3aa7` label "/user/resizer" with 11px `#6b7280` sublabel "router (round-robin)".
- **Workers:** four rounded boxes 180×40 at x=470, y=48/112/176/240, fill `rgba(0,131,0,0.12)`, 12px `#2c3e50` labels "worker 1 — jobs 1, 5, 9" / "worker 2 — jobs 2, 6, 10" / "worker 3 — jobs 3, 7, 11" / "worker 4 — jobs 4, 8, 12"; 3px green `#008300` arrows from the router box to each.
- **Annotation (bold 13px violet `#4a3aa7`, below the router near y=225):** "senders only ever see one address".
- **Caption (12px `#444`, bottom right):** "job numbering illustrative".

## Round-Robin Meets a 40 MB Panorama

**Tags:** `worked example` (blue), `smallest-mailbox` (green)

- **The jobs** — 12 uploads arrive one per second; each resize takes 1 second, all workers start idle
- **The outlier** — job 1 is a 40 MB panorama that takes 8 seconds instead of 1
- **Round-robin is blind** — jobs 5 and 9 go to worker 1 anyway and queue behind the panorama
- **Hand-check** — job 5 arrives at t=5s but starts at t=9s (wait 4s); job 9 arrives at t=9s, waits 1s
- **Smallest-mailbox looks** — it prefers idle workers over busy ones, so jobs 5 and 9 dodge worker 1
- **The payoff** — under smallest-mailbox every one of the 12 jobs starts the second it arrives

*Example (italic):* With round-robin jobs 5 and 9 sit waiting 4s and 1s behind the panorama; with smallest-mailbox every wait is 0 seconds.

**Key point:** Round-robin balances message counts, not work; when job sizes vary, a load-aware strategy like smallest-mailbox avoids queuing fast jobs behind one slow one.

### Visualization (canvas `c2`, 720×300)

Bar chart of wait time (seconds between arrival and start) for jobs 1–12 under round-robin, with a green zero baseline note for smallest-mailbox.

- **Title (bold 15px, `#1a5276`, top center):** "Round-Robin Wait Times When Job 1 Takes 8 Seconds".
- **Axes:** origin x=60, baseline y=245, plot width 620, plot height 180; x = jobs 1–12, one 30px-wide bar per job on ~50px centers, 12px `#444` tick labels "1"–"12"; y = wait seconds 0 to 5, gridlines `#e5e9ef` at 1/2/3/4 with 12px `#444` labels.
- **Bars:** wait seconds `[0, 0, 0, 0, 4, 0, 0, 0, 1, 0, 0, 0]` for jobs 1–12; zero-wait jobs drawn as 3px-tall blue `#2a78d6` stubs, job 5 solid red `#e74c3c` (height 4s) with bold 12px red label "waits 4s" above, job 9 solid orange `#d95926` (height 1s) with 12px orange label "waits 1s".
- **Panorama marker:** bold 12px `#6b7280` label "job 1 = 8s panorama (worker 1)" with a thin `#6b7280` pointer to the job-1 stub.
- **Smallest-mailbox note:** dashed green `#008300` (dash 4/3) horizontal line along y=0 just above the baseline, bold 13px green label "smallest-mailbox: every wait = 0s" at the right end near x=430, y=215.
- **Caption (12px `#444`, bottom right):** "arrival 1 job/s, service 1s except job 1 — illustrative".

## Same Photo, Same Worker: Consistent Hashing

**Tags:** `where it's used` (blue), `per-key ordering` (green), `affinity` (orange)

- **The sequence** — photo 4471 gets three edits in a row: resize, then watermark, then sharpen
- **The hazard** — round-robin sends the three edits to three workers; sharpen may run before resize
- **The strategy** — consistent hashing routes by a key: `hash(photoId)` always picks the same routee
- **Ordering restored** — all photo-4471 edits land on worker 3 and its mailbox runs them in order
- **Affinity bonus** — worker 3 keeps photo 4471 decoded in memory, so edits 2 and 3 skip a reload
- **Stable-ish** — a hash ring means adding a worker remaps only a slice of keys, not all of them

*Example (italic):* Six messages for photos 4471, 9302, and 5518 arrive interleaved; hashing sends every 4471 edit to worker 3, every 9302 edit to worker 1, and 5518 to worker 4.

**Key point:** Use consistent hashing when messages sharing a key must stay ordered or benefit from landing on the same worker — an actor's mailbox guarantees order only within one actor.

### Visualization (canvas `c3`, 720×300)

Mapping diagram: six message chips in arrival order across the top, colored by photo id, with arrows down to the four worker boxes each key hashes to.

- **Title (bold 15px, `#1a5276`, top center):** "hash(photoId) Sends Every Edit of a Photo to One Worker".
- **Message chips:** six rounded chips 100×30 across the top at y=62, x = 30/140/250/360/470/580, in arrival order: "4471 resize" (blue `#2a78d6`), "9302 resize" (green `#008300`), "4471 wmark" (blue), "5518 resize" (orange `#d95926`), "9302 wmark" (green), "4471 sharpen" (blue); chip fill = 15% alpha of its stroke color, 12px `#2c3e50` text, 11px `#6b7280` sequence labels "1st"–"6th" above each chip.
- **Workers:** four rounded boxes 140×40 at y=210, x = 40/210/380/550, fill `rgba(107,114,128,0.10)`, 12px `#2c3e50` labels "worker 1" / "worker 2" / "worker 3" / "worker 4".
- **Arrows:** 2px lines from each chip's bottom to its worker's top, colored by key — all three 4471 chips (blue) to worker 3, both 9302 chips (green) to worker 1, the 5518 chip (orange) to worker 4; worker 2 receives none.
- **Annotation (bold 13px blue `#2a78d6`, under worker 3 near y=272):** "resize → wmark → sharpen arrive in order".
- **Caption (12px `#444`, bottom right):** "photo ids and hash mapping illustrative".

## Pool or Group: Who Restarts a Dead Worker?

**Tags:** `common mistake` (red), `pool vs group` (orange)

- **Pool router** — creates its own routees as children, supervises them, restarts one that crashes
- **Group router** — is only given the paths of actors created elsewhere; it never owns them
- **The mistake** — deploying a group router and assuming it will heal or resize its worker set
- **What happens** — a group keeps routing to a crashed routee's path; those jobs go to dead letters
- **The rule** — want managed workers, pick a pool; routing to actors you already run, pick a group

*Example (italic):* Worker 3 crashes on a corrupt JPEG; the pool router restarts it and job 7 is only delayed, while a group router keeps sending every third job to a dead address.

**Common mistake:** Treating pool and group routers as interchangeable. Only a pool supervises its routees — a group is a mailing list of paths, and a dead path silently swallows work.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: a worker crash under a pool router (restarted, job delayed) vs under a group router (path dead, jobs lost to dead letters).

- **Title (bold 15px, `#1a5276`, top center):** "Worker 3 Crashes: Pool Restarts It, Group Keeps Mailing a Ghost".
- **Row 1 (y=100), label 12px `#444` at x=20:** "pool router"; violet `#4a3aa7` rounded box at x=130 labeled "router (parent)" (12px), 3px arrow to a red `#e74c3c` box at x=330 labeled "worker 3 crashes", 3px arrow to a green `#008300` box at x=540 labeled "restarted by router" with bold 12px green "✓ job 7 delayed, not lost".
- **Row 2 (y=210), label:** "group router"; violet rounded box at x=130 labeled "router (paths only)", 3px arrow to a red box at x=330 labeled "worker 3 crashes", 3px arrow to a red box at x=540 labeled "path → dead letters" with bold 12px red "✗ every 3rd job vanishes".
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(74,58,167,0.12)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=272):** "a group router never watches its routees — someone else must".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); job counts, timings, wait seconds `[0,0,0,0,4,0,0,0,1,0,0,0]`, and photo-id hash assignments are invented and labeled illustrative; text numbers (waits of 4s and 1s, worker 1 holding jobs 1/5/9) must match the chart data exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
