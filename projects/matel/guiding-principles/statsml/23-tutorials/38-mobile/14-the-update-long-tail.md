# The Update Long Tail

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Update Long Tail

**Subtitle:** Shipping v5.0 does not replace v4.x — for weeks your app runs as five versions at once, and your servers and analytics have to serve them all

## One Launch, Five Versions Still Alive

**Tags:** `core idea` (blue), `mobile releases` (green), `the long tail` (orange)

- **The launch** — a recipe-app team ships v5.0 on Monday and celebrates the release
- **The dashboard** — one week later only 50% of active users run v5.0; the rest are on 4.9, 4.8, 4.7, even 3.2
- **The reasons** — auto-update turned off, phones on old OS versions, staged app-store rollouts
- **The shape** — a big head (the new version) and a long thin tail of old versions that shrinks slowly
- **The name** — this lingering spread of old versions in the wild is the update long tail

*Example (italic):* On launch-day-plus-seven the version report reads v5.0 50%, v4.9 22%, v4.8 13%, v4.7 8%, v3.2 3%, other 4% — five versions live at once.

**Key point:** A mobile release is not a switch flip — the old versions stay alive on real phones for weeks, so at any moment your app is really several apps sharing one backend.

### Visualization (canvas `c1`, 720×300)

Vertical bar chart: share of active users by app version, measured one week after the v5.0 launch.

- **Title (bold 15px, `#1a5276`, top center):** "One Week After Launch: Active Users by App Version".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = % of active users 0 to 60, gridlines `#e5e9ef` at 15/30/45 with 12px `#444` labels; x = six bars with 12px `#444` version labels under each.
- **Bars (width 70px, evenly spaced across the plot), versions and heights:** `["v5.0", "v4.9", "v4.8", "v4.7", "v3.2", "other"]`, shares `[50, 22, 13, 8, 3, 4]`.
- **Bar colors:** v5.0 blue `#2a78d6`, v4.9 aqua `#199e70`, v4.8 green `#008300`, v4.7 yellow `#c98500`, v3.2 orange `#d95926`, other mute `#6b7280`; 12px `#444` value labels "50%", "22%"… above each bar.
- **Annotation (bold 13px magenta `#d55181`, upper right, y≈85):** "half your users are not on the version you just shipped".
- **Caption (12px `#444`, bottom right):** "shares illustrative".

## Half the Holdouts Update Each Week

**Tags:** `worked example` (blue), `adoption curve` (green)

- **The rule** — in this app, roughly half of the not-yet-updated users move to v5.0 each week
- **Hand-check** — holdouts go 100 → 50 → 25 → 12 → 6 → 3; v5.0 share is just 100 minus that
- **The floor** — the chain stops at 3%: those phones run an OS too old to install v5.0 at all
- **Week 3 reading** — 88% of users are on v5.0, so 12% of API traffic still comes from pre-5.0 clients
- **The server's view** — until the tail dies, every old request shape must still get a valid answer

*Example (italic):* v5.0's share climbs 0% → 50% → 75% → 88% → 94% → 97% over five weeks, then flattens — the last 3% never arrive.

**Key point:** Adoption is a halving curve with a hard floor, so the question is never "did we launch?" but "what fraction of live traffic is still on the old versions this week?"

### Visualization (canvas `c2`, 720×300)

Two-line chart over weeks 0–8 after launch: v5.0 share rising (green) and old-version holdouts falling (blue), meeting at the 97%/3% plateau.

- **Title (bold 15px, `#1a5276`, top center):** "The Adoption Curve: Half the Holdouts Update Each Week".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = weeks 0 to 8 with 12px `#444` tick labels every week; y = % of users 0 to 100, gridlines `#e5e9ef` at 25/50/75.
- **v5.0 line:** green `#008300` 3px line through weeks `[0, 1, 2, 3, 4, 5, 6, 7, 8]`, shares `[0, 50, 75, 88, 94, 97, 97, 97, 97]`, bold 12px green label "on v5.0" near week 5 above the line.
- **Holdout line:** blue `#2a78d6` 3px line through the same weeks, shares `[100, 50, 25, 12, 6, 3, 3, 3, 3]`, bold 12px blue label "still on old versions" near week 2 above the line.
- **Floor marker:** horizontal dashed `#6b7280` (dash 4/3) line at y = 3%, 12px `#6b7280` label "3% can never update" at its right end.
- **Annotation (bold 13px violet `#4a3aa7`, near week 6, y≈110):** "the last 3% never arrive".
- **Caption (12px `#444`, bottom right):** "halving rule and floor illustrative".

## The Server Must Remember Every Version

**Tags:** `where it's used` (blue), `API versioning` (green), `force upgrade` (orange)

- **API contracts** — every response shape must still work for the oldest client that calls it
- **Event schemas** — v5.0 sends a new checkout event; 4.x events arrive without it for weeks
- **Support window** — an old API path is safe to drop only when its traffic falls under ~1%
- **The stragglers** — v3.2's 3% never update on their own; a force-upgrade screen is the only exit
- **The cost** — each live old version is code, tests, and on-call surface you cannot delete yet

*Example (italic):* With the halving curve, v4.9 traffic drops below 1% at week 6, v4.8 and v4.7 at week 5 — v3.2 never does without a force-upgrade.

**Key point:** The long tail sets your support windows — API versions, event schemas, and old endpoints retire on the tail's schedule, not on the launch date, and force-upgrade is how you cap the tail.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: for each old version, how many weeks until its traffic falls below 1% and its server-side support can be dropped.

- **Title (bold 15px, `#1a5276`, top center):** "Weeks Until Each Old Version Falls Below 1% of Traffic".
- **Axis:** vertical 2px `#999` baseline at x=200, bars extend right, max width 460; linear scale 38px per week with 12px `#444` week ticks at 0/4/8/12 along y=255.
- **Rows (top to bottom at y = 70, 120, 170, 220), each with a left-aligned 12px `#444` label at x=20:**
  - "v4.9 — under 1% at week 6": aqua `#199e70` bar width 228
  - "v4.8 — under 1% at week 5": green `#008300` bar width 190
  - "v4.7 — under 1% at week 5": yellow `#c98500` bar width 190
  - "v3.2 — never on its own": red `#e74c3c` bar width 460 ending in a small arrowhead, bold 12px red label "force-upgrade or support forever" at the bar end
- **Bar style:** 16px tall, solid fills, 11px `#444` week labels ("6 wk", "5 wk", "5 wk") just past each bar end (except the v3.2 row, which has the red label instead).
- **Annotation (bold 13px magenta `#d55181`, bottom right near y=285):** "the tail decides the support window, not the launch date".
- **Caption (12px `#444`, bottom left):** "weeks follow the illustrative halving curve".

## Deleting the Endpoint the Tail Still Uses

**Tags:** `common mistake` (red), `broken clients` (orange)

- **The temptation** — v5.0 no longer calls `/v1/orders`, so the team deletes that path at week 3
- **The reality** — 12% of users still run 4.x, and their app calls `/v1/orders` on every open
- **The spike** — the error rate jumps from 0.5% to 12% the moment the old path goes away
- **The analytics twin** — the new checkout event exists only in v5.0, so an all-versions funnel shows a fake 12% drop
- **The fix** — gate every deletion and every metric on client-version traffic, never on release dates

*Example (italic):* The week-3 cleanup deletes an "unused" endpoint and instantly breaks 12% of app opens — every one of them a paying user on v4.x.

**Common mistake:** Treating the launch as the retirement of old versions. Old clients cannot be patched — only replaced — so an API path or event schema is dead only when the version-split traffic says the tail is gone.

### Visualization (canvas `c4`, 720×300)

Line chart of the daily API error rate across the cleanup: flat near zero, then a cliff up on the day the old endpoint is deleted while 12% of clients still call it.

- **Title (bold 15px, `#1a5276`, top center):** "Week-3 Cleanup: Deleting /v1/orders While 12% Still Call It".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = days 1 to 10 with 12px `#444` tick labels each day; y = error rate % 0 to 15, gridlines `#e5e9ef` at 5/10.
- **Error line:** 3px line through days `[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]`, error % `[0.5, 0.5, 0.5, 0.6, 0.5, 12.0, 11.5, 11.0, 10.5, 10.0]`; blue `#2a78d6` for days 1–5, red `#e74c3c` from day 6 onward.
- **Deletion marker:** vertical dashed `#6b7280` (dash 4/3) line at day 6, 12px `#6b7280` label "endpoint deleted" at its top.
- **Annotation (bold 13px red `#e74c3c`, near day 8, y≈95):** "12% of opens fail — exactly the long tail".
- **Caption (12px `#444`, bottom right):** "error rates illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the week-1 version split (50/22/13/8/3/4), the halving adoption curve (v5.0 share 0/50/75/88/94/97/97/97/97 with holdouts 100/50/25/12/6/3/3/3/3 and a 3% floor), the sub-1% support weeks (6/5/5/never), and the error-rate cliff (0.5% → 12.0% at day 6) are all invented, mutually consistent, and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
