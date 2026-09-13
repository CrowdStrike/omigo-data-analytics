# Late & Re-Arriving Data

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Late & Re-Arriving Data

**Subtitle:** Yesterday's numbers change today — events stamped with Tuesday's time keep trickling in for days, so a "closed" day is never quite closed

## The Tuesday Count That Kept Growing

**Tags:** `core idea` (blue), `event time` (green), `moving target` (orange)

- **The metric** — a mobile app team counts Tuesday's daily active users each morning after the day ends
- **Wednesday's answer** — the first run says 41,200 users were active on Tuesday
- **Thursday's answer** — the same query on the same table now says 42,900 for the same Tuesday
- **The stragglers** — phones that were offline Tuesday upload their Tuesday-stamped events later
- **The settle** — Friday reads 43,380, Saturday reads 43,450, and only then does Tuesday stop moving

*Example (italic):* Between Wednesday and Saturday, 2,250 extra users "appear" in Tuesday — nothing was recounted wrong; their events just arrived late.

**Key point:** Late-arriving data is any record that lands after the pipeline has already processed the period its event time belongs to — so a daily partition keeps changing after the day closes.

### Visualization (canvas `c1`, 720×300)

Line chart of one fixed question — "how many users were active on Tuesday?" — asked on four successive mornings, showing the answer climb toward its final value.

- **Title (bold 15px, `#1a5276`, top center):** "Same Question, Four Mornings: Tuesday's DAU Keeps Growing".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = day the report ran, four evenly spaced ticks labeled "Wed", "Thu", "Fri", "Sat" (12px `#444`); y = Tuesday DAU 40,000 to 44,000, gridlines `#e5e9ef` at 41,000 / 42,000 / 43,000 with 12px `#444` labels "41k" / "42k" / "43k".
- **Reading line:** blue `#2a78d6` 3px line with 5px dots through readings `[41200, 42900, 43380, 43450]`, each dot labeled with its value in bold 12px `#2a78d6` (e.g. "41,200").
- **Final-value line:** horizontal dashed `#6b7280` (dash 4/3) line at y for 43,450, 12px `#6b7280` label "settled value 43,450" at its right end.
- **Annotation (bold 13px orange `#d95926`, near the Wed point, y=110):** "+2,250 users arrive after the day 'closed'".
- **Caption (12px `#444`, bottom right):** "DAU counts illustrative".

## Event Time vs Processing Time: 1,000 Tuesday Events

**Tags:** `worked example` (blue), `two clocks` (green)

- **Two timestamps** — every event has an event time (tapped on the phone) and a processing time (landed in the warehouse)
- **The batch** — follow 1,000 events all stamped with Tuesday event times
- **On time** — 880 of them arrive on Tuesday itself, while the day is still open
- **The tail** — 92 arrive Wednesday (overnight offline phones), 21 Thursday (retries), 7 Friday (an upstream backfill)
- **Hand-check** — cumulative coverage of Tuesday's partition: 880 → 972 → 993 → 1,000, i.e. 88% → 97.2% → 99.3% → 100%

*Example (italic):* A subway commuter's Tuesday 8:14am workout event sits on the phone until Wednesday's wifi — event time Tuesday, processing time Wednesday.

**Key point:** Partition by event time and the day fills in over several days; partition by processing time and the count is stable but puts Tuesday's actions in Wednesday's bucket. You must pick one and know which you picked.

### Visualization (canvas `c2`, 720×300)

Bar chart of arrival day for the 1,000 Tuesday-stamped events, with a cumulative-coverage line overlaid on a second implicit scale.

- **Title (bold 15px, `#1a5276`, top center):** "1,000 Tuesday Events: When They Actually Arrive".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = arrival day, four bars centered at evenly spaced ticks labeled "Tue (same day)", "Wed", "Thu", "Fri" (12px `#444`); y = events 0 to 1,000, gridlines `#e5e9ef` at 250 / 500 / 750.
- **Bars:** 70px wide, fill `rgba(42,120,214,0.35)` with 2px `#2a78d6` border, heights for counts `[880, 92, 21, 7]`, each topped by its count in bold 12px `#2a78d6` ("880", "92", "21", "7").
- **Cumulative line:** green `#008300` 2px line with 4px dots through the bar centers at heights mapped from cumulative counts `[880, 972, 993, 1000]` on the same 0–1,000 scale, each dot labeled in bold 12px `#008300` with "88%", "97.2%", "99.3%", "100%".
- **Annotation (bold 13px violet `#4a3aa7`, near the Wed bar, y=100):** "12% of Tuesday shows up after Tuesday".
- **Caption (12px `#444`, bottom right):** "arrival split illustrative".

## The Lookback Window: Rewriting the Last Three Days

**Tags:** `where it's used` (blue), `reprocessing` (green)

- **The fix** — instead of computing only yesterday, the nightly job recomputes the last 3 event-time days
- **The rewrite** — Friday night's run rebuilds the Wednesday, Thursday, and Friday partitions from scratch
- **Late absorbed** — a Thursday-stamped event arriving Friday lands correctly because Thursday is rebuilt
- **The cutoff** — a Tuesday-stamped event arriving Saturday falls outside Saturday's 3-day window and is missed
- **The trade** — wider windows catch more stragglers but multiply compute; teams size them from the arrival tail

*Example (italic):* With the worked example's tail, the 3-day window reaches 993 of 1,000 (99.3%) — the 7 Friday backfill events need a 4-day window, and a 2-day window would permanently miss 28.

**Key point:** A reprocessing window turns "the day is final at midnight" into "the day is final once it ages out of the lookback" — correctness now has an explicit settling period.

### Visualization (canvas `c3`, 720×300)

Calendar-strip diagram: six day-partitions in a row, with Friday night's job shown reloading the newest three and one very late event falling outside the window.

- **Title (bold 15px, `#1a5276`, top center):** "Friday Night's Job Rebuilds a 3-Day Window".
- **Partition boxes:** six rounded boxes 88px wide, 44px tall, 8px radius, left-to-right starting at x=60 with 10px gaps, centered vertically at y=150; labeled "Mon", "Tue", "Wed", "Thu", "Fri", "Sat" in 12px `#2c3e50`.
- **Window boxes (Wed, Thu, Fri):** fill `rgba(0,131,0,0.12)` with 2px `#008300` border; older boxes (Mon, Tue) fill `rgba(42,120,214,0.15)` with 1px `#2a78d6` border and 12px `#6b7280` tag "frozen" beneath each; Sat box dashed 1px `#6b7280` border, 12px `#6b7280` tag "open" beneath.
- **Job arrow:** bold 13px `#008300` label "nightly job — reload 3 days" at top center (y=70) with three 2px `#008300` arrows down into the Wed, Thu, and Fri boxes.
- **Late event, caught:** small green `#008300` dot at (x≈470, y=240) with 12px green label "Thu event arrives Fri — rebuilt, counted", 2px green arrow up into the Thu box.
- **Late event, missed:** small red `#e74c3c` dot at (x≈180, y=240) with bold 12px red label "Tue event arrives Sat — outside window, missed", 2px red dashed arrow toward the Tue box ending in a red "✗" at the box edge.
- **Caption (12px `#444`, bottom right):** "window size illustrative; teams tune it to the arrival tail".

## The Report That Wouldn't Reconcile

**Tags:** `common mistake` (red), `re-run drift` (orange)

- **The scene** — an analyst runs last week's DAU report on Monday, then re-runs it Wednesday for a deck
- **The drift** — the two runs disagree on the same historical days, and recent days moved the most
- **The false alarm** — the analyst files a pipeline bug; nothing is broken — the partitions were still settling
- **The mistake** — treating a freshly closed partition as final and comparing report runs without a data-as-of date
- **The habit** — stamp every report with "data as of", and only compare days older than the settling period

*Example (italic):* Sunday reads 38,100 on Monday's run but 39,300 on Wednesday's run — a 3.1% jump on the youngest, least-settled day, while Thursday barely moves.

**Common mistake:** Expecting two runs of the same query over event-time-partitioned data to match. Until a day ages past the arrival tail (and the lookback window stops rewriting it), the same day legitimately has different values on different run dates.

### Visualization (canvas `c4`, 720×300)

Grouped bar chart: four days of last week, each with a pair of bars — the value from Monday's report run vs Wednesday's re-run — showing younger days drifting more.

- **Title (bold 15px, `#1a5276`, top center):** "Same Days, Two Run Dates: the Youngest Days Moved Most".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = event day, four groups labeled "Thu", "Fri", "Sat", "Sun" (12px `#444`); y = DAU 36,000 to 44,000, gridlines `#e5e9ef` at 38,000 / 40,000 / 42,000 with 12px `#444` labels "38k" / "40k" / "42k".
- **Monday-run bars:** 32px wide, fill `rgba(42,120,214,0.35)` with 2px `#2a78d6` border, values `[43100, 42800, 39400, 38100]`.
- **Wednesday-run bars:** 32px wide, immediately right of each Monday bar, fill `rgba(0,131,0,0.30)` with 2px `#008300` border, values `[43150, 42950, 39900, 39300]`.
- **Delta labels:** bold 12px above each pair: `#6b7280` "+0.1%" (Thu), `#6b7280` "+0.4%" (Fri), `#d95926` "+1.3%" (Sat), `#e74c3c` "+3.1%" (Sun).
- **Legend (12px, top right inside plot):** blue swatch "run Monday", green swatch "re-run Wednesday".
- **Annotation (bold 13px red `#e74c3c`, near the Sun group, y=105):** "not a bug — Sunday hadn't settled yet".
- **Caption (12px `#444`, bottom right):** "DAU values illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); DAU readings, the 1,000-event arrival split (880/92/21/7 with cumulative 88%/97.2%/99.3%/100%), and both report-run series are invented and labeled illustrative; the +2,250 growth in c1 equals 43,450 − 41,200, and the c4 deltas (+0.1% / +0.4% / +1.3% / +3.1%) follow from the paired bar values.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
