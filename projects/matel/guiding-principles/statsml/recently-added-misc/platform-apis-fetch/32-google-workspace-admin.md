# Google Workspace Admin

**Page type:** detail page (single obj-table row: text left 45%, payload + canvas right 55%; verified badge under subtitle; "Official API References" section below)
**HTML title tag:** Google Workspace Admin — Platform APIs

**Subtitle:** Pull activity logs, usage statistics, and the employee directory from a company's Google Workspace (Gmail, Drive, Meet).

**Verified badge:** Last verified: August 2026

## What you can get

- Activity streams per app: logins, Drive sharing, admin changes, third-party app authorizations
- Daily usage counters per user — without touching any content
- Current users, groups, org units, and managed devices
- Security alerts Google itself raised

**Key point (callout):** There is **no single "give me everything" feed** — each application's activity stream must be fetched separately. Skip one (say, the third-party-app stream) and nothing anywhere tells you it is missing. Completeness is a property of your collector's configuration, not of the platform.

## Watch out for

- The delay before events are queryable varies from minutes to more than a day, depending on the app
- Usage numbers for a given day can be revised afterwards — re-fetching returns different totals
- Retention differs per stream; anything needed long-term must be archived out in time
- The directory is now-only: renames, moves, and deletions break joins against old activity

## Sample payload

**Payload note (italic):** Sample Drive activity (abbreviated) — the document's title is in the log; the document itself is not.

```json
{
  "id": {
    "time": "2026-08-18T11:07:44.518Z",
    "applicationName": "drive"
  },
  "actor": { "email": "a.jaiswal@acme.com" },
  "events": [ {
    "type": "acl_change",
    "name": "change_user_access",
    "parameters": [
      { "name": "doc_title", "value": "FY27 Headcount Plan" },
      { "name": "visibility", "value": "shared_externally" },
      { "name": "target_user", "value": "partner@example.com" }
    ]
  } ]
}
```

### Visualization (canvas `c1`, responsive width × 380)

Horizontal range-bar chart on a log scale: indicative ingestion lag band per activity stream, with a small uncertainty whisker extending right of each bar.

- **Title (bold 14px `#1a5276`, top center):** "Indicative ingestion lag before an event is queryable".
- **Subtitle (italic 10px `#666`):** "log scale in hours — bands, not guarantees; lag differs per application and per event type".
- **Data (lag bands in hours, lo–hi, with band label and bar color):**
  - "admin": 0.2–1, band "minutes", `#27ae60`
  - "login": 0.2–1, band "minutes", `#27ae60`
  - "token": 0.5–2, band "sub-hourly", `#1a5276`
  - "meet": 1–4, band "hours", `#e67e22`
  - "drive": 1–6, band "hours", `#e67e22`
  - "usage reports": 12–48, band "a day or more, and revisable", `#e74c3c`
- **Scale:** logarithmic x from 0.1 h to 72 h; vertical `#eee` gridlines with `#aaa` 10px labels at 0.1 ("6 min"), 1 ("1 hr"), 6 ("6 hr"), 24 ("1 day"), 72 ("3 days").
- **Layout:** bars 22px high, rounded corners (radius 3), alpha 0.8, 26px gap between rows; row labels right-aligned 12px `#2c3e50`; a 14px whisker line in the bar color extends right of each bar's hi end; band text italic 10px `#666` to the right of the whisker.
- **Caption (italic 11px `#e74c3c`, bottom center):** "Query a window ending at \"now\" and every one of these rows undercounts."
- **Code comment context:** qualitative lag bands — ordering and magnitude class is the point.

## Official API References

- [Admin SDK Overview](https://developers.google.com/admin-sdk) — hub for Directory, Reports and related admin APIs
- [Reports API](https://developers.google.com/admin-sdk/reports) — activity streams and usage reports

## Regeneration instructions

- **Layout:** platform-APIs detail page. h1, `.subtitle`, `.verified` badge span, then one `.obj-table` (full width, border-collapse) with a single `<tr>`: left `<td>` (45%) holds two `.obj-title` blocks ("What you can get", "Watch out for" with `margin-top: 18px`) with bullet lists and a `.key-point` callout between them; right `<td>` (55%, text-align center) holds `.payload-note`, `<pre class="payload">`, and `<canvas id="c1" height="380">`. Below the table, an h2 "Official API References" with a link list.
- **Page CSS:** body system sans-serif, line-height 1.6, color `#2c3e50`, padding 30px 40px, white background; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` 0.8em `#888`, 1px `#ddd` border, inline-block, padding 2px 10px, radius 4px; h2 `#1a5276` 1.3em with 2px `#2980b9` bottom border; `.obj-title` bold `#1a5276` 1.1em; `.obj-table td` 16px padding, `1px solid #e0e0e0` border, vertical-align top; li 0.93em; links `#1a5276`; `.payload` `#f8f9fa` background, 3px `#1a5276` left border, monospace 0.78em, pre whitespace, left-aligned; `.payload-note` 0.82em `#666` italic left-aligned; `.key-point` `#f8f9fa` background, 3px `#1a5276` left border, padding 10px 14px, 0.93em; canvas block, `width: 100%`, margin 16px auto 0.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** responsive — width from `canvas.offsetWidth` (fallback 600), fixed 380px CSS height, backing store sized to rendered width × `window.devicePixelRatio` (display capped via `style.maxWidth`), `ctx.scale` back to logical coordinates, redrawn on window resize.
- Note: in regenerated HTML, any card/page links use `.html` extensions.
