# Slack API

**Page type:** detail page (two-column obj-table layout: text left 45%, code sample + canvas right 55%, one Overview row; second h2 section with reference links)
**HTML title tag:** Slack API — Platform APIs

**Subtitle:** Read the messages, threads and reactions of a Slack workspace — but only from channels your bot was invited to, and only as far back as the plan keeps them.

**Verified badge:** Last verified: August 2026

## Overview

### Left column

**What you can get**

- Messages and thread replies from channels
- Emoji reactions and shared files
- Who is in each channel, and user profiles
- A live feed of new messages as they arrive

**Key-point callout:** **You only see what the bot sees, as things look right now.** A bot reads only the channels it was invited to, and cheaper plans delete older history. Edits overwrite the original text and deleted messages vanish without a trace — so pulling history later never fully recreates what actually happened.

**Watch out for**

- No history from before the bot joined a channel — and busy channels get bots first, so the data skews toward them
- On plans that cap retention, re-running the same pull later returns *less* history — store what you fetch
- Many "messages" are alerts posted by other bots, not people — filter them out before counting activity
- History reads are tightly rate-limited, so large backfills are slow

### Right column

**Payload note:** **A message as the API returns it** — note what is missing.

Code block (pre, JSON):

```
{
  "messages": [
    {
      "user": "U024BE7LH",
      "text": "Rolled back to build 4417, prod is green",
      "ts": "1723731000.000100",
      "edited": { "ts": "1723731420.000000" },
      "reactions": [ { "name": "tada", "count": 2 } ]
    }
  ],
  "has_more": true
}

// "edited" gives the edit TIME, never the original text.
// Deleted messages are simply absent — no tombstone.
```

**Payload note:** **What a backfill can actually see, per channel.**

### Visualization (canvas `coverageChart`, responsive width × 430)

Horizontal Gantt-style coverage chart: one row per channel showing full existing history (dashed outline), the bot-reachable window (blue fill), and the portion lost to retention (red overlay), against a 20-month time axis.

- **Data (ILLUSTRATIVE, per code comment):** SPAN = 20 months of x-axis (oldest at left, "now" at right); RETAIN = 3 months retained on a capped plan. Channels (name, created months-ago, botJoin months-ago, msg/day):
  - `#incidents` — created 19.0, botJoin 16.4, 240 msg/day
  - `#deploys` — created 18.2, botJoin 15.1, 181
  - `#eng-general` — created 19.5, botJoin 12.8, 96
  - `#platform-team` — created 16.0, botJoin 9.2, 61
  - `#data-pipeline` — created 14.1, botJoin 6.0, 34
  - `#design-review` — created 12.4, botJoin 3.7, 18
  - `#hiring-loop` — created 11.0, botJoin 1.9, 9
  - `#office-social` — created 8.6, botJoin 0.7, 4
  - `#archive-2024` — created 19.8, botJoin null (never joined), 2
  - Code comment intent: channels ordered by activity descending; bot tenure falls with activity because busy channels get the integration first — that correlation is the point of the chart.
- **Title (top left, bold 13px `#1a5276`):** "Observable message history per channel: two truncations, stacked"
- **Caption under title (italic 10px `#e74c3c`):** "ILLUSTRATIVE, not measured. Channels ordered by activity — note coverage falls with it."
- **Layout:** padL = min(126, 24% of width), padR 58, padT 78, padB 62; row height = plotH/9, bar height = min(rowH-7, 17); x maps months-ago linearly, 20mo-ago at left edge to now at right edge. Alternate rows get a faint band fill `rgba(26,82,118,0.035)`.
- **Retention region:** area from left edge to x(3mo ago) filled `rgba(231,76,60,0.07)`; vertical dashed red line (`#e74c3c`, dash 5/4, width 1.4) at the 3-month cut. Label right-aligned left of the line in red 10px: "capped plan: history truncated here →"; italic red 10px right of the line: "boundary moves right over time".
- **Column heads (gray `#555`, 10px):** "msg/day" right-aligned left of the plot; "reachable" left-aligned right of the plot.
- **Per row:**
  - Dashed gray (`#aaa`, dash 3/3, width 1) outlined rectangle from x(created) to x(now): history that exists.
  - If bot joined: filled bar `rgba(26,82,118,0.35)` with stroke `#1a5276` (width 0.9) from x(botJoin) to x(now); if botJoin > 3, the segment from x(botJoin) to the retention cut is overlaid with `rgba(231,76,60,0.42)` (lost to retention); an orange (`#e67e22`) downward-pointing triangle marker sits just above the bar at x(botJoin).
  - Channel name in 11px monospace right-aligned at padL−52, colored `#e74c3c` if botJoin is null else `#2c3e50`; msg/day value in 10px `#777` right-aligned at padL−8.
  - Reachable share at right (bold 10px): "0%" in `#e74c3c` if never joined; else round(botJoin/created×100)+"%", colored `#1a5276` if >60 else `#e67e22`.
- **Axes:** gray `#999` L-shaped axis (left + bottom); x tick labels in `#666` every 4 months: "20mo ago", "16mo ago", "12mo ago", "8mo ago", "4mo ago", "now"; below them centered "time →".
- **Legend (bottom row, 10px, labels `#666`):** dashed gray swatch "history that exists"; blue-filled/stroked swatch "bot can reach"; red `rgba(231,76,60,0.42)` swatch "lost to retention"; orange `#e67e22` triangle "bot joined".
- Canvas redraws on window resize; white background fill.

## Official API References

- [Slack API home](https://api.slack.com/) — top-level portal for all Slack platform documentation
- [Web API methods index](https://api.slack.com/methods) — full method list including the conversations.* family

## Regeneration instructions

- **Layout:** single detail page: h1, `.subtitle` paragraph, `.verified` badge span, `h2` "Overview" with a full-width `.obj-table` (one `<tr>`; left `<td>` 45% text, right `<td>` 55% code + canvas), then `h2` "Official API References" with a plain `<ul>` of links. No nav bar, no back/home links.
- **Page CSS:** body system sans-serif (-apple-system stack), line-height 1.6, text `#2c3e50`, white background, padding 30px 40px; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` inline badge — background `#eaf2f8`, border `1px solid #e0e0e0`, text `#1a5276`, padding 2px 10px, radius 4px, 0.8em; h2 1.3em `#1a5276` with `border-bottom: 2px solid #2980b9`; obj-table cells `border: 1px solid #e0e0e0`, padding 16px, top-aligned; `.section-label` bold `#1a5276` block with 16px top margin; li/p 0.93em; links `#1a5276`.
- **Code block:** `pre` — background `#f8f9fa`, `border-left: 3px solid #1a5276`, ui-monospace font 0.78em, padding 12px, radius 4px, horizontal overflow scroll.
- **Callout:** `.key-point` — background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 10px 14px, 0.93em. `.payload-note` 0.85em `#555`.
- **Canvas:** `display:block; width:100%`, fixed `height` attribute 430; sized from `getBoundingClientRect().width`, scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), redrawn on resize.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, gray text `#666`/`#555`.
- In regenerated HTML, any card/page links use `.html` extensions.
