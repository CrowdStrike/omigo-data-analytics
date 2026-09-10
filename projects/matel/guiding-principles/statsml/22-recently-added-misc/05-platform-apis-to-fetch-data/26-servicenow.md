# ServiceNow Table API

**Page type:** detail page (single-row two-column obj-table: text left 45%, payload + canvas right 55%; followed by a references list)
**HTML title tag:** ServiceNow Table API — Platform APIs

**Subtitle:** Read IT tickets, change records, and their edit history from a ServiceNow instance — every module is just a table behind one generic interface.

**Verified badge:** Last verified: August 2026

## What you can get

- Incidents, changes, and service requests with timestamps and assignments
- Field-level history: who changed what, from what to what, and when
- The configuration database — servers, apps, services, and how they depend on each other
- SLA results and pre-computed time-in-state durations
- Users, groups, and the labels behind every status code

**Key point (callout box):** History is **opt-in, per field, and only from the moment it was switched on**. An empty history result cannot tell you whether the field was never audited or simply never changed. And old history is pruned on a retention schedule each company picks itself, so the horizon differs per instance.

## Watch out for

- Fields your account may not read come back blank rather than as errors — indistinguishable from genuinely empty
- Status codes and labels are customized per company; "state 6" is not universally "Resolved"
- Values arrive either formatted-and-localized or raw, depending on a request switch — mixing the two corrupts a time series
- Records describe that work happened, not what the fix was; root cause lives in unreliable free text

## Payload (right column)

**Payload note (italic):** Field-level history record (abbreviated) — one row per field change, the finest grain the platform offers.

```json
{
  "result": [
    {
      "tablename": "incident",
      "fieldname": "state",
      "oldvalue": "1",
      "newvalue": "2",
      "user": "a.jaiswal",
      "sys_created_on": "2026-08-17 05:02:58"
    }
  ]
}
```

### Visualization (canvas `c1`, responsive width × 380)

Horizontal answerability bar chart: one row per ServiceNow table, bar length = answerability level (out of 3), colored by guarantee class, with the question it answers printed inside the bar and a caveat note below it.

- **Title (bold 14px `#1a5276`, top center):** "Answerability by table — what is guaranteed vs conditional".
- **Subtitle (italic 11px `#666`):** "green = always there  •  orange = configuration-dependent  •  red = must have been switched on in advance".
- **Rows (table label right-aligned in monospace 11px `#2c3e50`; level out of 3 sets bar width):**

| Table | Question (bold, inside bar) | Level | Color | Note (italic gray `#777`, inside bar area) |
|---|---|---|---|---|
| incident (current row) | Where is this ticket now? | 3 | `#27ae60` | always available |
| sys_audit | When did each field change, and by whom? | 2 | `#e67e22` | only if audit enabled on that field, and only since |
| sys_journal_field | What did people write about it? | 2 | `#e67e22` | free text — inconsistently filled |
| metric_instance | How long in each state? | 1 | `#e74c3c` | only if a metric definition existed at the time |
| task_sla | Was the SLA met? | 2 | `#e67e22` | respects pause/schedule rules |
| cmdb_rel_ci (current) | What depends on this CI? | 1 | `#e74c3c` | today’s graph only — no history |

- **Geometry:** left margin 18px; label column min(200, 30% width); bar track = remaining width; row height 30px, gap 22px, first row at y=62. Track background `rgba(26,82,118,0.10)`; colored bar at alpha 0.85. Question text white when bar > 150px, else `#2c3e50`.
- **Gridlines:** dashed (3/3) `#ddd` vertical lines at levels 1, 2, 3.
- **Caption (italic 11px red `#e74c3c`, bottom center):** "The red rows cannot be fixed retroactively — an unaudited field has no past."

## Official API References

- [ServiceNow Developer Site](https://developer.servicenow.com/dev.do) — API reference hub and developer guides
- [Table API Reference](https://developer.servicenow.com/dev.do#!/reference/api/latest/rest/c_TableAPI) — CRUD over any table with sysparm query parameters

## Regeneration instructions

- **Layout:** single-row `.obj-table` (full width, border-collapse): left `<td>` 45% with "What you can get" (`.obj-title` + `<ul>`), a `.key-point` callout, then "Watch out for" (`.obj-title` with `margin-top:18px` + `<ul>`); right `<td>` 55% (text-align center) with `.payload-note` (italic), a `<pre class="payload">` JSON block, and the canvas. After the table, an `h2` "Official API References" with a plain `<ul>` of external links.
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` badge — 0.8em `#888`, 1px solid `#ddd` border, inline-block, padding 2px 10px, radius 4px (a `<span>` right after the subtitle); h2 1.3em `#1a5276` with 2px solid `#2980b9` bottom border; `.obj-title` bold 1.1em `#1a5276`; table cell borders `1px solid #2980b9`, padding 16px; li 0.93em; links `#1a5276`.
- **Payload / key-point style:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em, white-space pre, left-aligned; `.payload-note` — 0.82em italic `#666`, left-aligned; `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em.
- **Canvas:** `<canvas id="c1" height="380">` with CSS `display:block; margin:16px auto 0; width:100%`; width taken from `canvas.offsetWidth` at draw time, backing store scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), redrawn on window resize.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)` family. No nav bar, no back/home links.
