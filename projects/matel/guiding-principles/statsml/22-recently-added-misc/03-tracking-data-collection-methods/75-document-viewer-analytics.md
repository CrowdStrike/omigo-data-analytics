# Tracking Data: Document Viewer Analytics

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas/payload right 55%, one row per section)
**HTML title tag:** Tracking Data: Document Viewer Analytics

**Subtitle:** Most tracking in this set reports to a platform or an advertiser. This one reports to a colleague — often one in the reader's management chain.

## Section 1: What is it?

A view log whose audience is the author of the document.

- **Enterprise wikis** commonly show a page view count with a named viewer list
- **Collaborative editors** commonly give the owner an activity or insights panel with the same information
- **Reader identity is visible** on internal and workspace documents in a way it is not on a public web page
- **The data is ordinary:** a row per open, with an account identifier and a timestamp
- **The audience is what is distinct** — the author, inside the same organisation as the reader

**A view event is not a read:** it fires on open, so it cannot separate reading from scrolling past, a tab left open since yesterday, a link preview expanding in chat, a search result clicked and abandoned, or landing on the wrong page. "Who has read this" is an inference from "who opened this URL," and the gap is not small.

**Asymmetric and non-reciprocal:** the author sees the reader list; the reader is usually not shown that the author can see it, and cannot see who else read it. The record means something different to each party — a property of the reporting surface, not the data.

### Visualization (canvas `c1`, 720×320)

Stacked horizontal bar decomposing one view count into five behaviours, with a legend.

- **Title (bold 16px, centered, blue `#2a78d6`, y=22):** "One number, 47 views — five behaviours it cannot tell apart" (the 47 is computed as the sum of the segments).
- **Bar:** single horizontal stacked bar from x=40 to x=690, y=40, height 38; each segment's width proportional to its count; 1px blue `#2a78d6` outline per segment.
- **Segments (count, fill, label):**
  - 8, `#2a78d6`, "Read carefully, start to finish"
  - 14, `rgba(42,120,214,0.55)`, "Opened and skimmed"
  - 11, `#d95926`, "Tab left open, nobody at the desk"
  - 9, `rgba(217,89,38,0.50)`, "Link preview or search result click"
  - 5, `rgba(42,120,214,0.18)`, "Opened by mistake, closed at once"
- **Legend:** five rows starting y=104, 24px apart; 14×14 swatch (same fill, blue outline) at x=46, label text 14px `#2c3e50`, count ("8 views" etc.) bold 14px `#6b7280` at x=400.
- **Caption (centered, 13px `#6b7280`, bottom):** "Illustrative split — the log stores the bar, never the reasons."

## Section 2: What does it collect?

- **Total views** on a page, and unique viewer count
- **Viewer list**, resolved to accounts
- **First-view and last-view** timestamps per viewer
- **Repeat-view counts** per viewer
- **Views by day or week** as a time series
- **Comment and suggestion** attribution, with resolution history
- **Edit history** with per-author attribution and diff size
- **Time-on-page** or session duration, where reported

**The `engaged` field is the whole problem in one line:** a boolean derived from an open count, each of its three values carrying at least two incompatible readings. A field name asserts a conclusion the event cannot support.

**Repeat views have no sign:** six opens can be careful study, or a badly organised document the reader kept returning to for one number. The count is the same and nothing distinguishes them.

**Diff size is not contribution:** a large diff may be a paste of text the group already agreed, and a one-character edit may be the substantive decision. The history records who typed, not who decided.

### Visualization (canvas `c2`, 720×320)

Histogram of time-on-page with a shaded contaminated right tail and three vertical summary-statistic lines (median, trimmed mean, mean).

- **Title (bold 16px, centered, blue `#2a78d6`, y=22):** "Time-on-page: the tail is tabs, not readers".
- **Histogram:** 20 bins over 0–1800 seconds (90s per bin), counts `[14, 31, 26, 17, 11, 7, 5, 4, 3, 2, 2, 1, 1, 1, 1, 1, 0, 1, 0, 2]`; plot from x=70 to x=690, baseline y=176, plot height 100px scaled to max count. Bins starting at ≥600s are drawn orange (`rgba(217,89,38,0.45)` fill, `#d95926` stroke); earlier bins blue (`rgba(42,120,214,0.35)` fill, `#2a78d6` stroke).
- **Shaded region:** the area beyond 600s tinted `rgba(217,89,38,0.10)`, with right-aligned 13px orange label above it: "sessions that were not reading sessions".
- **X-axis:** muted (`#6b7280`) baseline with ticks and 13px labels at 0, 600, 1200, 1800s rendered as "0 min", "10 min", "20 min", "30 min".
- **Summary lines (dashed 4/4, 2px, from baseline up to y=40), computed from the expanded histogram values (bin midpoints):** green `#008300` "median" (label at y=48), blue `#2a78d6` "10% trimmed mean" (y=68), orange `#d95926` "mean" (y=88); each labeled bold 14px in its color with the rounded value in seconds appended (e.g. "median 135s" — values are computed, approximately median 135s, trimmed mean ~186s, mean ~257s).
- **Bottom text (centered, 14px `#2c3e50`, baseline+38):** "the mean sits well above the median, pulled up by the shaded sessions".
- **Caption (centered, 13px `#6b7280`, bottom):** "Schematic".

### Payload (right column, below canvas)

Caption above the block (italic, `.payload-note`): "Sample payload — illustrative structure, not real captured data."

```
{
  "page_id": "wiki_48213",
  "owner": "u_1042",

  // ── typically exposed in an analytics panel ──
  "total_views": 47,
  "unique_viewers": 12,
  "views_by_day": [3, 9, 14, 6, 4, 2, 9],
  "viewers": [
    { "user": "u_2287", "views": 6, "first": "…T09:14Z", "last": "…T17:02Z" },
    { "user": "u_5510", "views": 1, "first": "…T09:31Z", "last": "…T09:31Z" }
  ],

  // ── inferred / plausible — derived, not measured ──
  "not_viewed": ["u_7734"],       // absence, computed against a notified list
  "engaged": {
    "u_2287": true,               // 6 opens: careful study, or kept losing the answer
    "u_5510": false,              // 1 open: skim, or read it fully in one pass
    "u_7734": false               // 0 opens: unaware, or read a pasted excerpt
  },
  "read_rate": 0.67,              // numerator is opens; denominator is unstated
  "avg_time_on_page_sec": 214     // mean over sessions, open tabs included
}
```

## Section 3: Why is it collected?

**Stated purpose** (label pill, blue)

- **Did the document land** — was the design read before the meeting, was a policy change seen
- **Questions about pages** — which are used, which are stale, which need a rewrite

**Additional consequence** (label pill, orange)

- The record is **attributable to a person**, one group-by from a different question: who is paying attention
- Nothing after the open is observed, and **no denominator was recorded** — page-level questions never needed one

**The measurement changes the behaviour:** once views are attributed, opening a document stops being neutral. Some open in order to have opened it, inflating the count without adding a reader; others avoid opening what they would have skimmed. The two push in opposite directions, their relative size is unknown, and neither leaves a trace.

### Visualization (canvas `c3`, 720×320)

Three-bar chart: the same 26 recorded opens divided by three different invented denominators, giving three different "read rates" — one over 100%.

- **Title (bold 13px, centered, ink `#1a5276`, y=24):** "The same 26 recorded opens, divided by three bases the platform never recorded"; subtitle (12px `#6b7280`, y=42): "read rate reported for one document".
- **Baseline:** light-gray (`#e5e9ef`, 1px) horizontal line at y=226 from x=56 to x=680; y-scale max 1.6 (160%) over 150px height.
- **100% line:** dashed (6/4) violet `#4a3aa7` 1.5px horizontal line at the y for rate=1.0, labeled bold 12px violet "100%" at the right end.
- **Bars (width 86, centered at x=180, 360, 540):**
  - 26 ÷ 40 = 65% — fill `rgba(42,120,214,0.30)`, stroke `#2a78d6`; caption "everyone on the" / "distribution list"
  - 26 ÷ 31 = 84% — same blue styling; caption "those it reached" / "before the deadline"
  - 26 ÷ 18 = 144% — over 100%, so fill `rgba(217,89,38,0.45)`, stroke `#d95926`; caption "those the change" / "actually affects"
- **Value labels:** bold 14px above each bar in the bar's color ("65%", "84%", "144%"). Below the baseline per bar: bold 12px `#2c3e50` formula "26 ÷ 40" / "26 ÷ 31" / "26 ÷ 18", then two 12px `#6b7280` caption lines.
- **Bottom text (italic 12px `#2c3e50`, centered, h−24):** "The third base is smaller than the group that opened it, so the rate exceeds 100%."
- **Caption (italic 11px `#6b7280`, centered, h−8):** "Illustrative counts — the arithmetic is the point, not these numbers."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `.lede` + bullets + `.key-point` callouts + `.lbl` pills, right `<td>` (55%, centered) holds the canvas and, in row 2, the `.payload-note` + `.payload` pre block (both left-aligned).
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; `li` 0.93em; `li b` `#1a5276` weight 600. No nav bar, no back/home links.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em; leading `<strong>` in `#1a5276`.
- **Label pills:** `.lbl` inline-block uppercase 0.7em weight 700; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px, monospace (ui-monospace/Menlo) 0.78em, `white-space: pre`; `.payload-note` 0.82em `#666` italic.
- **Canvas:** intrinsic `width="720" height="320"` per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper that reads the element's own width/height attributes.
- **Chart palette (tracking pages use the CVD-checked categorical set, not the site default):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276` (headings/axes only), text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is deliberately excluded from the rotation, reserved for genuine alarm states. Site-wide accents elsewhere: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- Includes a rounded-rect path helper `rr` for canvases.
