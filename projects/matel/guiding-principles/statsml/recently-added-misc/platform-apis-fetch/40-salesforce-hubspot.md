# Salesforce & HubSpot

**Page type:** detail page (platform-API layout: h1 + subtitle + verified badge, one two-column obj-table row — text left 45%, payload + canvas right 55% — then an official-references list)
**HTML title tag:** Salesforce & HubSpot — Platform APIs

**Subtitle:** Lets you pull accounts, contacts, and deals out of the CRM — and, where history tracking is on, how each deal changed over time.

**Verified badge:** Last verified: August 2026

## Overview

**What you can get**

- Accounts, contacts, leads, and deals with amounts, stages, and close dates
- Change history: when a deal moved stage, and what the amount was before
- Logged activities — calls, emails, meetings, notes
- Marketing email events in HubSpot: sends, opens, clicks, bounces
- Bulk export for large volumes (Salesforce)

**Key point (callout):** The current deal row holds values that were set after the outcome — a win-rate model trained on it validates beautifully and fails in production, because at scoring time those fields do not yet hold their final values. Every interesting question (which stage leaks, whether discounting predicts closing) needs the change history — and history must be switched on in advance, per field. If nobody enabled it two years ago, it cannot be recovered.

**Watch out for**

- Permissions silently filter rows. A query run by an integration user returns only what that user can see, with no warning — a permissions change looks exactly like a business decline.
- Activity data is a record of logging, not of activity. Absence of a logged call is not evidence no call happened, and logging discipline varies by rep in ways that correlate with outcomes.
- Salesforce's API call budget is shared by every integration in the org for a rolling 24 hours. Use bulk export for big pulls or you starve everyone else.
- Email "opens" are tracking-pixel loads, distorted by mail providers' proxying and pre-fetching — not a measurement of attention.

### Payload example

**Payload note (italic, above the block):** Current state vs history. The deal row says $48,000 won in August; the history rows say it opened at $72,000 targeting May and was discounted twice along the way.

```
"StageName": "Closed Won",
"Amount": 48000.00,
"CloseDate": "2026-08-19",

"OpportunityHistories": [
  { "StageName": "Qualification", "Amount": 72000.00,
    "CloseDate": "2026-05-29", "CreatedDate": "2026-02-11" },
  { "StageName": "Proposal",      "Amount": 72000.00,
    "CloseDate": "2026-05-29", "CreatedDate": "2026-03-04" },
  { "StageName": "Proposal",      "Amount": 61000.00,
    "CloseDate": "2026-06-30", "CreatedDate": "2026-05-21" },
  { "StageName": "Negotiation",   "Amount": 52000.00,
    "CloseDate": "2026-07-31", "CreatedDate": "2026-06-18" },
  { "StageName": "Closed Won",    "Amount": 48000.00,
    "CloseDate": "2026-08-19", "CreatedDate": "2026-08-19" }
]
```

### Visualization (canvas `stageChart`, responsive width × 380)

Box-and-whisker chart: distribution of days spent in each pipeline stage, reconstructable only from OpportunityHistory.

- **Title (bold 13px `#1a5276`, top center):** "Days in stage — reconstructed from OpportunityHistory".
- **Subtitle (italic 10px `#888`, centered):** "Illustrative. Box = interquartile range, line = median, whiskers = 10th–90th percentile. Not computable from the Opportunity row alone."
- **Data (stage: p10 / q1 / median / q3 / p90 days, box/whisker stroke color):** Qualification: 3 / 8 / 15 / 27 / 48, `#1a5276`; Needs Analysis: 4 / 9 / 18 / 33 / 62, `#2980b9`; Proposal: 6 / 17 / 34 / 71 / 128, `#e67e22`; Negotiation: 4 / 11 / 22 / 44 / 86, `#8e44ad`; Contract: 2 / 4 / 8 / 15 / 31, `#27ae60`.
- **Axes:** y from 0 to 140 days with 8 gridlines (light gray `#eee`, integer labels `#666`); rotated y-axis label "days in stage" in 10px `#666`; x baseline gray `#999`; margins top 62, bottom 90, left 58, right 26. Five equal-width groups; box width 34% of group width.
- **Box style:** boxes filled rgba(26,82,118,0.35) with per-stage stroke (width 1.6); median a thick (2.6) line in the stage color; whiskers (width 1.4) with short end caps; bold median value label like "15d" in `#2c3e50` above the top whisker; stage label in 10px below the baseline.
- **Highlight annotation:** vertical dashed red line (`#e74c3c`, dash 4/3, width 1.2) alongside the Proposal box spanning q1 to q3, with two italic red lines to its right: "right-skewed and widest — mean here would" / "mislead; the tail is where deals die".
- **Footer (three 11px `#666` lines, left-aligned at bottom):** "Only closed opportunities have complete stage durations. Including open deals" / "censors the longest ones and understates every stage — a survival problem," / "not a descriptive one."

## Official API References

- [Salesforce Developer Documentation](https://developer.salesforce.com/docs) — the docs hub for all Salesforce platform APIs
- [HubSpot Developer Documentation](https://developers.hubspot.com/docs) — CRM v3 objects, associations, engagements, and webhooks

## Regeneration instructions

- **Layout:** platform-API detail page: h1, `.subtitle` paragraph, `.verified` inline badge, `<h2>Overview</h2>` with a bottom border, one `.obj-table` row — left `<td>` (45%) holds `.section-label` headings ("What you can get", "Watch out for") with bullet lists and a `.key-point` callout between them; right `<td>` (55%) holds a `.payload-note` italic paragraph, a `pre.payload` JSON block, and the canvas — then `<h2>Official API References</h2>` with a link list.
- **Page CSS:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; subtitle `#666` 1.05em; `.verified` 0.8em `#888` with `1px solid #e0e0e0` border, radius 4px, padding 2px 10px; h2 1.3em `#1a5276` with `2px solid #2980b9` bottom border; `.section-label` bold `#1a5276`; `.obj-table` cells `1px solid #e0e0e0`, padding 16px, vertical-align top; li/p 0.93em; links `#1a5276`; `pre.payload` background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em, radius 4px; `.key-point` same background/left border, padding 10px 14px, 0.93em; `.payload-note` 0.82em italic `#666`. No nav bar, no back/home links.
- **Canvas:** `display: block; width: 100%`, height attribute 380; drawn responsively from `getBoundingClientRect().width`, scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), redrawn on window resize.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, gray text `#666`/`#888`/`#2c3e50`, box fill rgba(26,82,118,0.35).
