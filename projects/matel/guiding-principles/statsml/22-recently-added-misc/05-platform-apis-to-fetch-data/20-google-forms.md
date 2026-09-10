# Google Forms

**Page type:** detail page (two-column obj-table layout: text left 45%, code sample + canvas right 55%, single row under an Overview h2)
**HTML title tag:** Google Forms API — Platform APIs

**Subtitle:** Build Google Forms from a program and pull in the responses people submit.

**Verified badge:** Last verified: August 2026

## Overview

## What You Can Get

- A form's questions, settings, and quiz configuration
- Every submitted response, with per-question answers and timestamps
- Quiz scores for graded forms
- Form building from a program — add, edit, move, and delete questions
- Notifications when new responses arrive

**Key-point callout:** **Responses outlive the questions they answered.** Deleting a question does not delete its collected answers, and rewording a question keeps the same internal id — so old and new answers silently merge under one label. Save a snapshot of the form alongside each batch of responses, or the data becomes unlabelable later.

## Watch Out For

- You cannot submit, edit, or delete a response through the API — response data is read-only
- "Skip to section" branching logic is invisible to the API, and re-saving a form through it flattens that logic
- Every answer comes back as a string, even numbers and dates
- Anonymous forms carry no respondent identifier at all, so repeat submitters cannot be detected

## A response holding an answer to a deleted question

Code block (`pre`, JSON with trailing comment):

```
{
  "responseId": "ACYDBNi9...",
  "createTime": "2026-08-19T09:14:22Z",
  "answers": {
    "5e6f7a8b": { "textAnswers": { "answers": [ { "value": "4" } ] } },
    "9c0d1e2f": { "textAnswers": { "answers": [ { "value": "Yes" } ] } }
  }
}

// question 9c0d1e2f was deleted from the form —
// the answer remains, but its label is gone
```

## Schema Drift — Responses vs the Live Form

### Visualization (canvas `driftChart`, width 100% responsive × 380)

Gantt-style timeline chart: questionId lifetimes (horizontal bars over a 12-week axis) versus form edits (vertical dashed markers).

- **Title (bold 13px, `#1a5276`, at 12,8):** "questionId lifetimes vs the form definition you can read today"
- **Subtitle (italic 10px, `#888`, at 12,26):** "forms.get returns only the right-hand edge; responses.list returns every id ever collected"
- **X axis:** weeks 0–12, gridlines `#eee` every 2 weeks labeled "wk 0" … "wk 12" (10px `#888`). Padding: left min(190, 26% of width), right 24, top 56, bottom 96.
- **Edit markers:** vertical dashed purple `#8e44ad` lines (dash 3/3, 1.5px) at weeks 3, 6, 9, labeled above in bold 9px purple: "edit #1", "edit #2", "edit #3".
- **Rows (one horizontal bar per question, bar height min(22, half the band), fill color at ~23% alpha — blue rows use `rgba(26,82,118,0.35)`, others color + `3a` hex — stroke color 1.5px; left labels: question label in 10.5px `#2c3e50` and id in 9.5px monospace in the row color, right-aligned):**
  - id `5e6f7a8b`, label "Q1 scale — clarity", weeks 0–12, color `#27ae60`, fate "stable"
  - id `6f7a8b9c`, label "Q2 checkbox — tools", weeks 0–12, color `#27ae60`, fate "stable"
  - id `9c0d1e2f`, label "Q3 yes/no — mentor?", weeks 0–6, color `#e74c3c`, fate "deleted at edit #2"
  - id `7a8b9c0d`, label "Q4 paragraph — blockers", weeks 3–12, color `#1a5276`, fate "added at edit #1"
  - id `8b9c0d1e`, label "Q5 scale — reworded text", weeks 0–12, color `#e67e22`, fate "text changed, same id"
- **Orphan tail:** for the deleted question (ends before week 12), a dashed red `#e74c3c` outline rectangle continues from week 6 to week 12 with italic 9.5px red text "answers persist, label gone".
- **Legend (bottom, 10.5px, 11×11 swatches in one row):**
  - `#27ae60` — present throughout
  - `#1a5276` — added mid-collection
  - `#e74c3c` — deleted — orphan answers
  - `#e67e22` — reworded, id reused
- **Footnotes (italic 10px, gray `#888`, bottom left, two lines):** "The orange row is the dangerous one: same questionId, different question text, silently pooled answers." / "Snapshot the form definition with every response batch, or the join is unrecoverable later."

## Official API References

- [Google Forms API](https://developers.google.com/forms/api) — form structure, responses, and watches
- [Forms API REST reference](https://developers.google.com/forms/api/reference/rest) — forms, responses, and watch endpoints

## Regeneration instructions

- **Layout:** platform-apis-fetch detail page. h1, `.subtitle` paragraph, `.verified` badge span, h2 "Overview", then one `.obj-table` (full width) with a single `<tr>`: left `<td>` (45%) holds `.section-head` headings ("What You Can Get", "Watch Out For") + bullets + one `.key-point` callout; right `<td>` (55%) holds a `.section-head` + `<pre>` JSON sample, another `.section-head` + the canvas. After the table, h2 "Official API References" with a link list.
- **Page CSS:** body system sans-serif (-apple-system stack), line-height 1.6, text `#2c3e50`, padding 30px 40px, white background; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` badge — background `#eaf2f8`, border `1px solid #2980b9`, color `#1a5276`, padding 2px 10px, radius 4px, 0.8em; h2 1.3em `#1a5276` with 2px `#2980b9` bottom border; `.section-head` bold 0.95em `#1a5276`; `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 10px 14px, 0.93em; `pre` — background `#f4f4f4`, padding 14px, radius 6px, 0.8em, ui-monospace; `li`/`p` 0.93em; links `#1a5276`. No nav bar, no back/home links.
- **Canvas:** `<canvas id="driftChart" height="380">`, CSS `display:block; width:100%`; JS resizes on window resize, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), fixes CSS height to 380px, and applies `ctx.setTransform(dpr,0,0,dpr,0,0)` before drawing.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, bar fill `rgba(26,82,118,0.35)`, gray text `#888`/`#666`/`#2c3e50`.
- In regenerated HTML, any card/page links use `.html` extensions.
