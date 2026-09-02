# Tracking Data: Version Control Activity Metrics

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Version Control Activity Metrics

**Subtitle:** A commit history is an audit log of a tool, not a record of work. Reading it as productivity requires assumptions the log does not support.

## Section 1: What is it?

**Lede:** An audit log the tool writes whether anyone asked for it.

- **Nothing to switch on:** commits, pushes, pull requests and review events are a side effect of use
- **Durable and attributable** — the most available quantitative record of engineering work, and the most misread
- **A commit carries six things:** author identity, committer identity, both timestamps, diff, message
- **Client-supplied:** author fields, clock and time zone come from the machine that made the commit, and can be set to any value
- **Server-observed:** push and pull-request events are stamped on arrival

**Key point — "Commits by hour of day" is partly a clock chart:** the author timestamp is client-supplied, so the plot measures local clock configuration and workflow habit too.

**Key point — History is rewritable:** rebasing, squashing and amending change the recorded timestamps, so the apparent shape of work reflects branch policy rather than when work happened.

### Visualization (canvas `c1`, 720×320)

Two-panel field-provenance diagram: client-supplied fields vs server-observed fields.

- **Title (bold 16px blue `#2a78d6`, centered at y=20):** "Where each field comes from".
- **Two outlined panels** 330 wide × 186 tall starting at y=32, stroke width 2 — left at x=20 in orange `#d95926`, right at x=370 in green `#008300`.
- **Left panel (orange):** heading bold 15px "Written by the client machine"; sub 13px muted `#6b7280` "settable, and rewritten by rebase / squash / amend"; five bullet rows (6×6 orange square markers, 14px text `#2c3e50`, rows at y = 88 + i×25): "author name + email", "author timestamp + time zone", "committer name + email", "commit message", "the diff itself".
- **Right panel (green):** heading "Observed by the server"; sub "stamped on arrival, not supplied by the sender"; rows: "push received at", "pull request opened at", "review submitted at", "approval recorded at", "merged at".
- **Caption (13px muted, centered at y=234):** "Trust the right column more than the left".

## Section 2: What does it collect?

- **Commit count**, and lines added and deleted per commit
- **Files touched**, and the directories they sit in
- **Author and committer identity**, and both timestamps
- **Pull requests** opened, review comments, approvals
- **Server-side timings** — time to first review, time to merge

**Key point — Measured and modelled sit in one flat row:** the composite score is stored beside the counts it came from, and nothing marks which is which.

**Key point — Unit-of-analysis error:** software work is joint and the log attributes each line to one identity. Pairing, design, mentoring, incident response, review and deleting code produce little attributable authorship.

**Key point — Large diffs need not mean effort:** generated code, vendored dependencies, lockfiles, formatting runs and file moves are big diffs with no matching work.

**Key point — So diff size to effort is neither monotonic nor consistent between people**, and the errors are systematic by role rather than random. A senior engineer whose main output is review and design scores lowest per line while contributing most.

### Visualization (canvas `c2`, 720×320)

Labeled scatter plot: diff size (x, log-ish scale) against actual effort (y), showing no monotonic relation.

- **Title (bold 16px blue, centered at y=22):** "Diff size against actual effort"; subtitle 13px muted: "Illustrative".
- **Axes:** plot region x 92–640, y 196 (baseline) up to 46; L-shaped axes in text color `#2c3e50` width 1. X-axis label centered below: "lines in diff  →"; rotated y-axis label: "effort  →".
- **X ticks (hardcoded, log-ish):** "0" at x=92, "10" at x=170, "100" at x=288, "1k" at x=442, "4k" at x=592 (4px tick marks, muted labels).
- **Points (radius 5, label 13px text color beside each; effort as fraction 0..1 of plot height; side = label left/right of dot):**
  - Blue `#2a78d6`: "design + pairing" (x=96, effort 0.88), "code review" (x=104, 0.66), "concurrency fix" (x=172, 0.92), "feature slice" (x=302, 0.44), "deleting dead code" (x=352, 0.74)
  - Orange `#d95926`: "file move" (x=452, 0.22, label left), "formatter run" (x=522, 0.10, label left), "vendored code" (x=562, 0.14, label right), "lockfile bump" (x=597, 0.05, label left)
- **Caption (13px muted, centered at bottom):** "No monotonic relation, and the error is patterned by role".

**Payload note (italic gray, below canvas):** Sample payload — illustrative structure, not real captured data.

**Payload block (monospace, `#f8f9fa` background, left border 3px solid `#1a5276`):**

```
{
  "developer_id":       "d-4417",
  "week_of":            "2026-08-17",
  // ── recorded in the platform ──
  "commits":            23,
  "lines_added":        1840,
  "lines_deleted":      612,
  "prs_opened":         4,
  "reviews_submitted":  9,
  "author_ts":     "2026-08-19T02:41:08+05:30",
      // clock + time zone from the laptop, not the server
  "committer_ts":  "2026-08-19T09:12:44+00:00",
      // rewritten by the last rebase

  // ── inferred / plausible ──
  "productivity_score": 72.4,
  "focus_time_hours":   18.5,
  "percentile_rank":    0.38,
  "churn_ratio":        0.33
}
```

## Section 3: Why is it collected?

**Label (STATED PURPOSE, blue pill):**

- **Team-level questions the data supports** — process bottlenecks, stalled review queues, where work waits rather than moves

**Label (ADDITIONAL CONSEQUENCE, orange pill):**

- The same rows are **individually attributable and time-stamped**, so they can be repurposed for performance review and stack ranking
- **The data does not change** between uses — only the unit of analysis, from a queue to a person, and that is where validity is lost

**Key point — Every measure has a cheap way to move it that is not more work:** more commits, each smaller; a diff left unpruned; approvals given by reading less. Time-to-approve is the sharpest case — it improves as the reviewing it stands in for degrades. The log is also not an instrument: timestamps come from the client, and rebasing rewrites history by design.

### Visualization (canvas `c3`, 720×320)

Two-line divergence chart: the metric rises while the goal it stands for falls.

- **Title (bold 16px blue, centered at y=22):** "Optimising time-to-approve"; subtitle 13px muted: "Schematic".
- **Axes:** plot region x 70–505, y 190 (baseline) up to 60; L-shaped axes in text color width 1.
- **Series (7 points each, evenly spaced across x, values normalised 0..1 of plot height, line width 2.5 with 3.5px-radius dots):**
  - Metric (green `#008300`, rising): `[0.20, 0.34, 0.50, 0.63, 0.74, 0.82, 0.88]` — end label right-aligned in green: "the metric: approvals get faster".
  - Goal (magenta `#d55181`, falling): `[0.82, 0.78, 0.68, 0.55, 0.41, 0.30, 0.22]` — end label in magenta: "the goal: reviews get read".
- **X-axis label (13px muted, centered below baseline):** "periods after the metric became a target  →".
- **Caption (13px muted, centered at bottom):** "They move in opposite directions".

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` heading + `.lede` + bullets + `.key-point` callouts, right `<td>` (55%, centered) holds the canvas (and, in the "What does it collect?" row, the `.payload-note` + `.payload` block below the canvas, left-aligned).
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; li 0.93em with `li b` in `#1a5276` weight 600.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `<strong>` in `#1a5276`.
- **Label pills:** `.lbl` inline-block uppercase 0.7em weight 700, letter-spacing 0.05em, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` background `#f8f9fa`, left border `3px solid #1a5276`, ui-monospace 0.78em, `white-space: pre`; `.payload-note` italic 0.82em `#666` immediately above it.
- **Canvas:** intrinsic 720×320 per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper that reads the element's own width/height attributes.
- **Palette:** charts use the validated categorical token palette — blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is deliberately not in the rotation (reserved for alarm states). Page chrome uses primary blue `#1a5276` (site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange).
- No nav bar, no back/home links. In regenerated HTML any card links use `.html` extensions.
