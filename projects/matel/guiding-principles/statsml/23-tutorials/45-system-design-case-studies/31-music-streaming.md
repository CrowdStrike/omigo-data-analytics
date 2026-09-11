# Music Streaming

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Music Streaming

**Subtitle:** Streaming the song is the easy part — small files, CDN, prefetch; the product is discovery, and its heavy ML runs as a giant weekly batch job (a design exercise built from publicly-shared engineering-blog concepts)

## The Cheap Part: Getting the Song to Your Ears

**Tags:** `core idea` (blue), `audio delivery` (green), `CDN + prefetch` (orange)

- **The file** — a 3-minute song at 160 kbps is about 3.6 MB; the same 3 minutes of 1080p video is ~112 MB
- **The ratio** — audio is ~30× lighter, so a whole album costs less network than one video clip
- **The edge** — popular tracks sit in CDN caches near listeners, served like any static file on the web
- **The client cache** — the app keeps recently played tracks on the device, so replays cost zero network
- **The prefetch** — the player downloads the next queued track before the current one ends: gapless start
- **The consequence** — playback is a solved problem, so the hard engineering has to live somewhere else

*Example (italic):* Your whole commute playlist is smaller than one minute of HD video — by the time a track's opening note plays, the next track is often already on the phone.

**Key point:** Because audio files are ~30× lighter than video, CDN caching plus client-side prefetching makes delivery cheap — the differentiating system moves up the stack, to deciding what to play next.

### Visualization (canvas `c1`, 720×300)

Horizontal bar chart comparing the byte weight of audio vs video at the same watch/listen time, log-feel via hardcoded pixel widths.

- **Title (bold 15px, `#1a5276`, top center):** "Audio Is the Lightweight of Streaming".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 440; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Rows (top to bottom at y = 80, 140, 200), each with a left-aligned 12px `#444` label at x=20:**
  - "3-min song, 160 kbps — 3.6 MB": green `#008300` bar width 30
  - "3-min 1080p clip, 5 Mbps — 112 MB": blue `#2a78d6` bar width 190
  - "1-hour 1080p movie — 2,250 MB": orange `#d95926` bar width 440
- **Bar style:** 18px tall, fills at 0.35 alpha with a solid 2px border in the same hue, 11px `#444` MB labels at bar ends.
- **Annotation (bold 13px green `#008300`, near x=250, y=255):** "a whole song is smaller than one minute of video — CDN + prefetch solve playback".
- **Caption (12px `#444`, bottom right):** "pixel widths schematic, MB figures exact for those bitrates".

## Users Who Playlist X Also Playlist Y

**Tags:** `worked example` (blue), `collaborative filtering` (green), `cold start` (orange)

- **The signal** — billions of user-made playlists are votes: songs placed side by side probably belong together
- **The count** — song X sits in 1,000 playlists (illustrative); collaborative filtering counts what they share
- **Hand-check** — song A co-occurs in 400 of those playlists (40%), song B in 250 (25%), song C in only 60 (6%)
- **The rank** — recommend A first, then B; C's 6% is barely above chance, so it stays off the list
- **The cold start** — brand-new track D sits in 0 playlists, so co-occurrence counts can say nothing about it
- **The fallback** — its waveform (tempo, energy, timbre) stands in until real listeners start placing it

*Example (italic):* Out of 1,000 playlists containing X, 400 also contain A — so "listeners of X" see A recommended, while day-old track D is matched to X purely by how it sounds.

**Key point:** Collaborative filtering is counting co-occurrence at scale — no music theory involved — and audio/content features exist precisely for the tracks too new to have any co-occurrence counts yet.

### Visualization (canvas `c2`, 720×300)

Vertical bar chart of playlist co-occurrence with song X for three candidate songs plus a zero-history new release.

- **Title (bold 15px, `#1a5276`, top center):** "Of 1,000 Playlists Containing X, How Many Also Contain...".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = playlists shared with X, 0 to 500, gridlines `#e5e9ef` at 125/250/375 with 12px `#444` labels.
- **Bars (70px wide, centered at x = 150, 290, 430, 570):** song A blue `#2a78d6` height for 400; song B aqua `#199e70` height for 250; song C violet `#4a3aa7` height for 60; new track D drawn as a dashed 2px `#6b7280` empty outline (dash 4/3) 70×40 sitting on the baseline.
- **Bar labels:** 12px `#444` beneath each bar: "Song A", "Song B", "Song C", "new track D"; bold 12px value labels above bars: "400 (40%)", "250 (25%)", "60 (6%)".
- **D label (12px `#6b7280`, inside the dashed outline):** "0 playlists".
- **Annotation (bold 13px green `#008300`, near x=480, y=110):** "no playlist history → audio features fill the gap".
- **Caption (12px `#444`, bottom right):** "playlist counts illustrative".

## A Playlist for Every User, Computed While You Sleep

**Tags:** `where it's used` (blue), `batch pipeline` (green), `serving split` (orange)

- **The product** — a personalized 30-track playlist, refreshed weekly for hundreds of millions of listeners
- **The surprise** — it is not a real-time system: a giant offline batch job computes every playlist in advance
- **Hand-check** — 400M users × 30 tracks = 12 billion rows (illustrative), written to a fast store once a week
- **The serving path** — opening the playlist on Monday is one key lookup, ~5 ms, with no model in the request
- **The split** — heavy ML over billions of playlists runs offline; online servers only read the stored result
- **The sizing** — each side is built for its own job — batch for throughput, serving for tail latency

*Example (italic):* The recommendation model may churn through logs for hours over the weekend — the user-facing request on Monday morning touches none of that, just a precomputed list under their user ID.

**Key point:** The design split is the lesson: real-time serving reads precomputed recommendations from a fast store, while the expensive ML runs offline on a schedule — each side sized for its own job.

### Visualization (canvas `c3`, 720×300)

Two-row flow diagram: the offline weekly batch path vs the online per-request path, with the KV store as the meeting point.

- **Title (bold 15px, `#1a5276`, top center):** "Heavy ML Offline, Key Lookup Online".
- **Row 1 (y=95), label 12px `#444` at x=20:** "every weekend"; violet `#4a3aa7` rounded box at x=140 labeled "logs + billions of playlists" (12px), 3px arrow to a blue `#2a78d6` box at x=340 labeled "batch CF + audio models", 3px arrow to a green `#008300` box at x=545 labeled "KV store: 30 tracks/user" with bold 12px violet `#4a3aa7` "12B rows, once a week" beneath the middle box.
- **Row 2 (y=205), label:** "Monday, per request"; blue box at x=140 labeled "app opens playlist", thin 2px arrow pointing right-and-up to the same green KV box region (arrowhead at x=545, y=150), bold 12px green `#008300` "✓ one read, ~5 ms" at x=340, y=215.
- **Box style:** 150–180px wide, 40px tall, 8px radius, fills `rgba(74,58,167,0.12)` / `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=272):** "the model never runs in the request path".
- **Caption (11px `#444`, bottom right):** "user and row counts illustrative".

## Not Everything Needs to Be Real-Time

**Tags:** `common mistake` (red), `freshness budget` (orange)

- **The confusion** — treating "recommend a song" like search: scoring millions of candidates live, per request
- **The blow-up** — ranking a catalog of tens of millions of tracks per request puts model inference at p99
- **The staleness test** — ask of each feature: would the user notice output that is a week old?
- **The split in practice** — discovery tolerates a week; play/pause and search tolerate almost nothing
- **The payoff** — everything that passes the staleness test becomes a cheap cache read at serve time

*Example (italic):* A weekly playlist computed 6 days ago feels identical to one computed 6 seconds ago — but a play button that reacts in 2 seconds feels broken instantly.

**Common mistake:** Defaulting to real-time everywhere. Real-time computation is a cost you pay per request, forever — the design skill is sorting features by the freshness they actually need and batching everything the user can't tell is stale.

### Visualization (canvas `c4`, 720×300)

Freshness-spectrum diagram: product features placed on a log-feel time axis from milliseconds to a week, split into per-request vs batch zones.

- **Title (bold 15px, `#1a5276`, top center):** "Match Each Feature to the Freshness It Actually Needs".
- **Axis:** horizontal 2px `#999` line from x=60 to x=680 at y=170; 12px `#444` tick labels beneath at x = 60, 215, 370, 525, 680: "10 ms", "1 s", "1 min", "1 hour", "1 week" (log-feel positions, hardcoded).
- **Zones:** left region x=60–330 shaded `rgba(231,76,60,0.06)` with 12px `#e74c3c` label "compute per request" at top-left of the zone (y=110); right region x=330–680 shaded `rgba(42,120,214,0.08)` with 12px `#2a78d6` label "precompute in batch" at top-right of the zone (y=110); dashed `#6b7280` (dash 4/3) vertical divider at x=330.
- **Feature dots (8px radius, on the axis, labels 12px alternating above at y=150 and below at y=196):** "play / pause" green `#008300` at x=75; "search-as-you-type" blue `#2a78d6` at x=230; "recently played row" aqua `#199e70` at x=385; "new-release shelf" orange `#d95926` at x=530; "weekly discovery playlist" violet `#4a3aa7` at x=665.
- **Annotation (bold 13px magenta `#d55181`, centered near y=60):** "batch wherever staleness is invisible to the user".
- **Caption (12px `#444`, bottom right):** "positions schematic".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded values above (no randomness); the playlist co-occurrence counts (1,000 / 400 / 250 / 60), user and row counts (400M × 30 = 12B), and the 5 ms lookup are invented and labeled illustrative; the byte figures are exact for the stated bitrates (160 kbps × 180 s = 3.6 MB, 5 Mbps × 180 s = 112 MB, 5 Mbps × 3,600 s = 2,250 MB).
- **Framing:** a design exercise built from publicly-shared engineering-blog concepts (CDN audio delivery, prefetching, playlist-based collaborative filtering, audio features for cold start, weekly offline batch pipelines feeding a fast serving store); it makes no claims about any company's current internal systems.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
