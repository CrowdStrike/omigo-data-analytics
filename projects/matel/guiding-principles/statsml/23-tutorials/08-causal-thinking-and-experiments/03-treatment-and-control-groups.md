# Treatment & Control Groups

**Page type:** detail page (tutorial layout: h2 card-sections, two-column table 50% text / 50% viz per section)
**HTML title tag:** Treatment &amp; Control Groups

**Subtitle:** The control group keeps the old onboarding flow — it shows you what would have happened without your change

## Two Groups, One Difference

Tags: `core idea` (blue), `running example` (orange)

- **Treatment group** — the 500 signups the coin sent to the new onboarding flow
- **Control group** — the 500 who keep the old flow: business as usual
- **Not "nothing"** — control users still onboard; they just get the current version
- **Same everything else** — same week, same app, same servers, same promotions
- **The comparison** — treatment outcome minus control outcome is the effect

*Example (italic):* Both groups sign up on the same Tuesday; only the screen after "Create account" differs.

**Key point:** A control group's job is to be identical to treatment in every way except the one thing you changed.

### Visualization (canvas `c1`, 720×300)

Flow diagram: two parallel three-box pipelines (treatment and control), identical except the middle box.

- **Title (bold 16px, `#1a5276`, centered, y=26):** "Two Journeys, Identical Except One Screen".
- **Treatment lane:** left-aligned bold blue (`#2a78d6`) label "TREATMENT (500 signups)" at (40, 68); boxes (2px stroke, white fill, bold 13px centered text in stroke color): "create / account" at (40, 80, 130×44) in mute gray `#6b7280`; "NEW onboarding / flow" at (220, 80, 160×44) in blue `#2a78d6` with fill `#eef4fd`; "use the app" at (430, 80, 130×44) in mute. Gray arrows (170,102)→(216,102) and (380,102)→(426,102).
- **Control lane:** bold orange (`#d95926`) label "CONTROL (500 signups)" at (40, 188); boxes: "create / account" at (40, 200, 130×44) mute; "OLD onboarding / flow" at (220, 200, 160×44) in orange `#d95926` with fill `#fdf1ea`; "use the app" at (430, 200, 130×44) mute. Gray arrows at y=222.
- **Highlight:** dashed magenta (`#d55181`, dash 6/4, width 2) rectangle (208, 66, 184×190) around the two differing boxes; bold magenta centered label "the ONLY difference" at (300, 285).
- **Side notes (12px, `#2c3e50`, left-aligned at x=590):** "same week" (y=95), "same app" (y=115), "same promos" (y=135), "same crowd," (y=155), "coin-flipped" (y=175).

## The Counterfactual You Can Actually See

Tags: `worked example` (green), `core idea` (blue)

- **The wish** — you'd love to watch the same users both with and without the new flow
- **Impossible** — each user experiences one version; the other stays invisible
- **The stand-in** — the control group is a look-alike crowd living the "without" version
- **Read the numbers** — treatment: 340 of 500 finish setup (68%); control: 290 of 500 (58%)
- **The effect** — 68 − 58 = 10 points caused by the new flow

*Example (italic):* Control's 58% is your best view of what the treatment users would have done under the old flow.

**Key point:** "Counterfactual" just means the road not taken — the control group walks it for you.

### Visualization (canvas `c2`, 720×300)

Two-bar chart: treatment vs control completion rates with counterfactual line and effect bracket.

- **Title (bold 16px, `#1a5276`, centered, y=26):** "Control Shows the World Without Your Feature".
- **Bars:** 140px wide, baseline y=240, chart height 165px, y-scale max 80: treatment (new flow) 68%, label "340 of 500" (white bold 12px inside bar bottom), color blue `#2a78d6`, at x=110; control (old flow) 58%, "290 of 500", orange `#d95926`, at x=370. Value labels ("68%", "58%") bold 14px `#2c3e50` above bars; axis labels below baseline.
- **Baseline:** thin `#999` line from x=60 to x=560.
- **Counterfactual line:** dashed orange (`#d95926`, dash 6/4, width 1.5) horizontal line across the plot at the 58% level.
- **Effect bracket:** green (`#008300`) 2px bracket at x=590-605 spanning from the 68% level to the 58% level, labeled to its right in bold green 13px: "+10 =" / "effect".
- **Annotations:** bold orange 13px centered at (415, just above the 58% line): "58% = what treatment users would do anyway"; bold green 13px centered at bottom: "effect = treatment 68% − control 58% = 10 points".

## No Control, No Truth: the Seasonality Trap

Tags: `common mistake` (red), `where it's used` (blue)

- **The shortcut** — launch to everyone and compare with last week: 52% then, 68% now
- **The claim** — "our flow added 16 points!" — but the weeks differ in more than the flow
- **Promo week** — a holiday campaign brought keener signups who finish setup more anyway
- **Control catches it** — this week's control also rose, to 58%: +6 came free with the season
- **Honest split** — of the 16-point jump, 6 points are season and 10 are your feature

*Example (italic):* Last week (old flow): 52%. This week: control 58%, treatment 68% — the season moved both groups.

**Key point:** Without a control group, whatever the calendar did gets credited to your feature.

### Visualization (canvas `c3`, 720×300)

Three-bar chart decomposing a before/after jump into season and feature components.

- **Title (bold 16px, `#1a5276`, centered, y=26):** "Before/After Says +16 — the Control Says Otherwise".
- **Bars:** 110px wide, baseline y=235, chart height 160px, y-scale max 80: "last week" (sub "old flow, everyone") 52%, mute gray `#6b7280`, x=80; "this week: control" (sub "old flow") 58%, orange `#d95926`, x=270; "this week: treatment" (sub "new flow") 68%, blue `#2a78d6`, x=460. Value labels bold 14px above bars; labels 12px and gray sub-labels below baseline (thin `#999` line x=50 to x=640).
- **Brackets:** yellow (`#c98500`, 2px) diagonal connector from top of 52% bar to top of 58% bar, labeled bold yellow 13px "+6 season"; green (`#008300`, 2px) connector from 58% to 68%, labeled bold green 13px "+10 feature".
- **Bottom annotation (bold magenta `#d55181` 13px, centered):** "the naive +16 claim quietly pockets the season's +6".

## What Counts as a Real Control Group

Tags: `common mistake` (red), `rule of thumb` (green)

- **Last month's users** — not a control: a different crowd in a different season
- **Users who skipped the flow** — not a control: they chose, and choice carries information
- **Another country** — not a control: it differs in a hundred ways besides your feature
- **A real control** — made by the same coin flip, at the same time, from the same crowd
- **Test yourself** — ask: could this group differ from treatment in any way besides the feature?

*Example (italic):* "Compare with users who skipped the new flow" recreates the keen-user problem inside your own experiment.

**Key point:** A control group is created by randomization, never found lying around afterwards.

### Visualization (canvas `c4`, 720×300)

Checklist graphic: four candidate control groups as rows, three rejected with ✗, one accepted with ✓.

- **Title (bold 16px, `#1a5276`, centered, y=26):** "Four Candidate "Controls" — Only One Is Real".
- **Rows:** 600px wide × 48px tall boxes starting at (60, 52), 8px vertical gap. Rejected rows: fill `#fdf4f3`, stroke red `#e74c3c` 2px, bold 22px red "✗" mark; accepted row: fill `#eefaf0`, stroke green `#008300` 2px, bold 22px green "✓".
  1. ✗ "last month's users" — "different season, different crowd"
  2. ✗ "users who skipped the flow" — "self-selected — choice carries information"
  3. ✗ "users in another country" — "differs in a hundred other ways"
  4. ✓ "coin-flipped group, same week" — "same crowd, same time — only the flow differs"
- Row name in bold 13px `#1a5276`; reason in 12px `#2c3e50` below it.
- **Bottom annotation (bold green `#008300` 13px, centered):** "a real control is created by the coin flip, not found afterwards".

## Regeneration instructions

- **Template:** tutorials topic-page layout (see `tutorials/CLAUDE.md`; skeleton copied from `most-powerful-signals/07-social-graph-connections.html`). Page: `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle` line, then four `.card-section` blocks each with `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) and a `table.layout` with one `<tr>`: `.text-col` (50%) and `.viz-col` (50%).
- **Text column structure:** `.tags` pill row first (`.tag.blue` rgba(26,82,118,0.12)/#1a5276, `.tag.green` rgba(39,174,96,0.15)/#27ae60, `.tag.red` rgba(231,76,60,0.12)/#e74c3c, `.tag.orange` rgba(230,126,34,0.15)/#e67e22; 0.72rem bold, 10px radius pills), then a `<ul>` (0.92rem) of one-line bullets each opening with `<b>` (colored `#1a5276`), one italic `.example` paragraph (`#555`, 0.9rem), one `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, 0.9rem).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; canvases `width:100%`, border `1px solid #e0e0e0`, radius 4px.
- **Canvas palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Doc palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** each canvas declares intrinsic `width`/`height` attributes (720×300); a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Shared `box()` and `arrow()` helpers draw labeled rectangles and filled-head arrows. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions (this page has none).
