# Replication

**Page type:** detail page (tutorial layout: `.card-section` blocks, each an h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Replication

**Subtitle:** Keep two live copies of the database — reads spread across three machines, and a copy takes over if the original dies

## One Primary, Two Copies That Follow Along

**Tags:** `core idea` (blue), `running example` (green)

- **The setup** — one primary database plus two replicas, all holding the same 80 GB
- **Writes** — every INSERT and UPDATE goes to the primary, and only the primary
- **The change log** — the primary streams each change to both replicas, which replay it
- **Reads** — 9,000 reads/sec spread out: roughly 3,000 to each of the three machines
- **Two wins, one trick** — spare copies give you speed today and a backup when one dies

*Example (italic):* Like a teacher writing on the board while two students copy every line into their own notebooks.

**Key point:** Replication means the same data lives on several machines. Writes go to one place; reads can come from any copy.

### Visualization (canvas `c1`, 720×300)

Architecture diagram: primary and two replicas with write, change-log, and read arrows.

- **Title (bold 15px, `#1a5276`, top center):** "Writes Go to One Machine — Reads Come From All Three".
- **Machine boxes** (128×56, bold 13px label + grey 12px "same 80 GB" sub-label):
  - "PRIMARY" at (100,120), blue `#2a78d6` stroke, fill `rgba(42,120,214,0.14)`.
  - "replica A" at (420,76) and "replica B" at (420,176), aqua `#199e70` stroke, fill `rgba(25,158,112,0.14)`.
- **Writes:** magenta `#d55181` arrow into the primary from the left, labeled bold 12px "all writes".
- **Change log:** two violet `#4a3aa7` arrows from primary to each replica, labeled bold 12px "change log, streamed" (upper) and "every change, replayed" (lower).
- **Reads:** green `#008300` bold 13px "9,000 reads/sec" with 12px "~3,000 each" at right; three green arrows — to replica A, to replica B, and one long arrow back to the primary, routed below both boxes.
- **Takeaway** (blue bold 13px bottom center): "one machine takes the writes; three machines share the reads".

## 2:00 PM: The Primary Dies, a Copy Takes Over

**Tags:** `worked example` (green), `core idea` (blue)

- **2:00:00** — the primary's disk fails mid-afternoon; writes start erroring
- **2:00:05** — the replicas notice the silence: no heartbeat from the primary
- **2:00:30** — replica A is promoted: it is the new primary, writes resume
- **Reads never stopped** — the two surviving copies served all 9,000 reads/sec throughout
- **The cost** — 30 seconds without writes, and one less spare until a new copy is built

*Example (italic):* Customers kept browsing the whole time; only checkout paused for half a minute.

**Key point:** Failover is just a promotion — a replica already has the data, so it only needs the title. That is why the copies exist.

### Visualization (canvas `c2`, 720×300)

Failover timeline with event ticks and two status bands.

- **Title (bold 15px, `#1a5276`, top center):** "30 Seconds From Crash to New Primary".
- **Timeline axis:** horizontal grey line from x 70 to 650 at y 200.
- **Ticks** (colored tick mark, bold 12px time label below, 12px note above connected by a grid-colored leader line; positions as fraction of axis):
  - 0.0 — "2:00:00" — "primary disk fails" — red `#e74c3c`.
  - 0.28 — "2:00:05" — "no heartbeat — replicas notice" — orange `#d95926`.
  - 0.72 — "2:00:30" — "replica A promoted" — green `#008300`.
  - 1.0 — "2:00:31" — "writes flowing again" — blue `#2a78d6`.
- **No-writes band:** translucent red `rgba(231,76,60,0.10)` rectangle above the axis from tick 0.0 to 0.72, labeled bold 13px red "writes paused: 30 seconds".
- **Reads band:** translucent green `rgba(0,131,0,0.10)` rectangle below the axis spanning the full timeline, labeled bold 13px green "reads never stopped — the 2 surviving copies served all 9,000 reads/sec".
- **Takeaway** (violet bold 13px bottom center): "the copy already had the data — promotion is just a title change".

## Your Dashboard Reads a Copy That Runs Behind

**Tags:** `where it's used` (blue), `trade-off` (orange)

- **Copies lag** — replicas replay changes a moment later: usually ~0.5 s behind
- **Analytics reads replicas** — heavy SELECTs are pointed at copies to spare the primary
- **Lag spikes** — during the 3 AM batch load, the replicas fall up to 45 s behind
- **Counts disagree** — primary says 1,000,042 orders; the replica still says 1,000,010
- **Neither is wrong** — the replica is correct about a moment slightly in the past

*Example (italic):* Two analysts ran the same COUNT(*) one second apart on different replicas and got different answers.

**Key point:** A replica answers "what was true a moment ago." Fine for dashboards and training data; risky for anything that must be exact right now.

### Visualization (canvas `c3`, 720×300)

Line chart of replica lag over 24 hours with a 3 AM spike.

- **Title (bold 15px, `#1a5276`, top center):** "How Far Behind the Replica Runs, Over One Day".
- **Data (hourly lag in seconds, hours 0–23, illustrative):** `[0.5, 0.6, 0.5, 45, 30, 8, 1.5, 0.8, 0.6, 0.7, 0.9, 1.2, 1.0, 0.8, 0.9, 1.1, 1.3, 1.0, 0.8, 0.7, 0.6, 0.5, 0.6, 0.5]`.
- **Axes:** padding top 52, bottom 52, left 65, right 30; y max 50 with ticks at 0/10/20/30/40/50 labeled "0s"–"50s" (grey 12px, `#e5e9ef` gridlines); x ticks at hours 0, 6, 12, 18, 23 labeled "12am", "6am", "noon", "6pm", "11pm".
- **Series:** connected line, aqua `#199e70`, width 3.
- **Annotations:** orange bold 13px at the spike with a short orange leader line: "3 AM batch load: replica falls 45 s behind"; aqua bold 12px near the flat part: "normal lag ~0.5 s".
- **Caption (violet bold 12px bottom center):** "primary count: 1,000,042 — replica during the spike: 1,000,010 (illustrative)".

## The Surprise: You Write It, Then Can't See It

**Tags:** `common mistake` (red), `lag` (orange)

- **t = 0.0 s** — a user saves a new address; the write lands on the primary
- **t = 0.3 s** — the page reloads; that read is routed to replica B
- **Replica B is 2 s behind** — it hasn't replayed the change, so the OLD address shows
- **t = 2.1 s** — replica B catches up; a second refresh shows the new address
- **The fix** — read your own fresh writes from the primary, or wait out the lag

*Example (italic):* "I saved my new address and the site showed the old one" — no data was lost, a stale copy answered.

**Common mistake:** Treating all three machines as "the database." A write and the very next read may hit different copies — and the copy can be seconds behind.

### Visualization (canvas `c4`, 720×300)

Two-lane event timeline (primary lane and replica lane) over t = 0–3 s.

- **Title (bold 15px, `#1a5276`, top center):** "Save, Reload, See the Old Value: One Write, Two Machines".
- **Lanes:** horizontal grid-colored lines from x 130 to 660; "PRIMARY" (blue, y 90) and "replica B" (aqua, y 190) labels bold 13px right-aligned, with grey 11px "(2 s behind)" under the replica label. Vertical gridlines and grey 12px labels at "t = 0 s", "t = 1 s", "t = 2 s", "t = 3 s".
- **Events** (7px-radius filled dots with bold 12px labels):
  - Blue dot on primary lane at t=0: "0.0 s: new address saved".
  - Violet arrow from the primary at t≈0 down to the replica lane at t=2.1, labeled 12px "change log travels / replays".
  - Red `#e74c3c` dot on replica lane at t=0.3: "0.3 s: page reload reads here" / "→ OLD address".
  - Green `#008300` dot on replica lane at t=2.1: "2.1 s: change applied" / "→ new address shows".
- **Takeaway** (red bold 14px bottom center): "the read arrived 1.8 s before the write did — nothing was lost, a stale copy answered".

## Regeneration instructions

- **Template:** tutorials topic-page layout (see `tutorials/CLAUDE.md`). h1 (no index number) with 2px `#2980b9` bottom border, `.subtitle` grey one-liner, then four `.card-section` blocks: each has an `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) and a `table.layout` with `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, one-line `<ul>` bullets each opening with `<b>bold term</b>` (`#1a5276`), one italic `.example` paragraph, one `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem).
- **Tag pills:** inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors: blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; canvases `width:100%`, `1px solid #e0e0e0` border, radius 4px.
- **Canvas:** each declared 720×300 intrinsic; scaled by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Shared `box()` and `arrow()` (filled triangular head) helpers.
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
