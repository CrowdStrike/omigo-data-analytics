# Microsoft Entra ID

**Page type:** detail page (single obj-table row: text left 45%, payload + canvas right 55%; verified badge under subtitle; "Official API References" section below)
**HTML title tag:** Microsoft Entra ID — Platform APIs

**Subtitle:** Pull sign-in and audit records from Microsoft Entra ID (formerly Azure AD) — the login system behind Microsoft 365.

**Verified badge:** Last verified: August 2026

## What you can get

- Every authentication attempt, with which access policies were checked and what each decided
- Directory changes: accounts created, groups changed, roles granted
- Risk signals Microsoft itself detected (leaked credentials, impossible travel)
- Current users, groups, and applications

**Key point (callout):** How much history you can query **depends on the licence tier** — roughly a week on the free tier, roughly a month on paid ones. Longer history exists only if an export was configured in advance, and a missing export looks exactly like "nothing happened."

## Watch out for

- Most "sign-ins" are automatic token refreshes, not people — counting raw events measures machinery, by roughly an order of magnitude
- Risk verdicts are re-scored after the fact; take your own snapshots if you need what was believed at the time
- Records appear minutes late, so a window ending at "now" undercounts
- Access policies have no version history — you can read today's rule but not last quarter's

## Sample payload

**Payload note (italic):** Sample sign-in record (abbreviated) — the only place you can see why access was allowed.

```json
{
  "createdDateTime": "2026-08-18T09:14:02Z",
  "userPrincipalName": "a.jaiswal@acme.com",
  "appDisplayName": "Office 365 SharePoint Online",
  "signInEventType": "interactiveUser",
  "conditionalAccessStatus": "success",
  "status": { "errorCode": 0 },
  "location": { "city": "Bengaluru",
                "countryOrRegion": "IN" },
  "appliedConditionalAccessPolicies": [
    { "displayName": "Require MFA for all users",
      "result": "success" }
  ]
}
```

### Visualization (canvas `c1`, responsive width × 380)

Grouped horizontal bar chart: queryable history in days by licence tier, per log type, with a purple "diagnostic setting" bar for self-controlled retention.

- **Title (bold 14px `#1a5276`, top center):** "Queryable history by licence tier (indicative days)".
- **Subtitle (italic 10px `#666`):** "verify against current licensing for your tenant — the ordering is the point, not the exact number".
- **Data (rows of tiered bars, values in days, scale max 110):**
  - "Sign-in logs": Free = 7 (`#e74c3c`), P1 = 30 (`#e67e22`), P2 = 30 (`#27ae60`)
  - "Directory audits": Free = 7 (`#e74c3c`), P1 = 30 (`#e67e22`), P2 = 30 (`#27ae60`)
  - "Risk detections": Free = 7 (`#e74c3c`), P1 = 30 (`#e67e22`), P2 = 90 (`#27ae60`)
  - "Diagnostic setting": single bar labeled "any tier — retention is yours" = 110 (`#8e44ad`)
- **Layout:** bars 15px high, rounded corners (radius 2), alpha 0.85, 4px gap within a group, 26px gap between groups; row labels right-aligned bold 12px `#2c3e50` at group vertical center; per-bar label to the right of each bar in 10px `#2c3e50` formatted "TIER — Nd" (tier name only when value ≥ 100).
- **Gridlines:** vertical light gray `#eee` lines at 0, 30, 60, 90 days with "0d"/"30d"/"60d"/"90d" labels in `#aaa` 10px below the chart.
- **Caption (italic 11px `#666`, bottom center):** "The purple bar is the only one you control — and only from the day you configure it forward."
- **Code comment context:** widths are indicative of the documented tiering, not a substitute for checking current licensing.

## Official API References

- [Microsoft Graph REST API Overview](https://learn.microsoft.com/en-us/graph/api/overview) — v1.0 and beta endpoint reference hub
- [Entra Monitoring and Health](https://learn.microsoft.com/en-us/entra/identity/monitoring-health/) — log retention, diagnostic settings and Log Analytics export

## Regeneration instructions

- **Layout:** platform-APIs detail page. h1, `.subtitle`, `.verified` badge span, then one `.obj-table` (full width, border-collapse) with a single `<tr>`: left `<td>` (45%) holds two `.obj-title` blocks ("What you can get", "Watch out for" with `margin-top: 18px`) with bullet lists and a `.key-point` callout between them; right `<td>` (55%, text-align center) holds `.payload-note`, `<pre class="payload">`, and `<canvas id="c1" height="380">`. Below the table, an h2 "Official API References" with a link list.
- **Page CSS:** body system sans-serif, line-height 1.6, color `#2c3e50`, padding 30px 40px, white background; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` 0.8em `#888`, 1px `#ddd` border, inline-block, padding 2px 10px, radius 4px; h2 `#1a5276` 1.3em with 2px `#2980b9` bottom border; `.obj-title` bold `#1a5276` 1.1em; `.obj-table td` 16px padding, `1px solid #e0e0e0` border, vertical-align top; li 0.93em; links `#1a5276`; `.payload` gray `#f8f9fa` background, 3px `#1a5276` left border, monospace 0.78em, pre whitespace, left-aligned; `.payload-note` 0.82em `#666` italic left-aligned; `.key-point` `#f8f9fa` background, 3px `#1a5276` left border, padding 10px 14px, 0.93em; canvas block, `width: 100%`, margin 16px auto 0.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, `#8e44ad` purple.
- **Canvas:** responsive — width from `canvas.offsetWidth` (fallback 600), fixed 380px CSS height, backing store sized to rendered width × `window.devicePixelRatio` (display capped via `style.maxWidth`), `ctx.scale` back to logical coordinates, redrawn on window resize.
- Note: in regenerated HTML, any card/page links use `.html` extensions.
