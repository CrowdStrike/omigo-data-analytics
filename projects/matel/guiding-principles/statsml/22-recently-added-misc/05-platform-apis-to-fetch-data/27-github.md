# GitHub API

**Page type:** detail page (single-row two-column obj-table: text left 45%, payload + canvas right 55%; followed by a references list)
**HTML title tag:** GitHub API — Platform APIs

**Subtitle:** Pull repositories, commits, issues, and pull requests out of the world's largest code hosting platform.

**Verified badge:** Last verified: August 2026

## What you can get

- Repositories with their activity metadata (language, stars, last push)
- Commits, branches, and releases
- Issues and pull requests with reviews and comments
- CI workflow runs and their outcomes
- Organization and user profiles

**Key point (callout box):** There are two layers of speed limits. Even well under the official hourly budget, firing many requests at once trips a separate abuse detector. For bulk work, a slow one-at-a-time script with backoff is more reliable than a parallel one.

## Watch out for

- Without a token you get only 60 requests/hour; with one, 5,000 (GitHub App installations scale with repo and user count, up to 12,500)
- Search has its own much tighter budget — about 30 requests/minute, and only 10/minute for code search
- Creating issues or comments too fast triggers throttling of its own, separate from the read limits
- Results come at most 100 per page, so large repos mean many sequential calls

## Payload (right column)

**Payload note (italic):** Sample response — one repository from GET /user/repos (abbreviated).

```json
[
  {
    "id": 810432671,
    "name": "analytics-pipeline",
    "full_name": "acme-org/analytics-pipeline",
    "private": true,
    "owner": { "login": "acme-org", "type": "Organization" },
    "description": "ETL jobs for warehouse ingestion",
    "created_at": "2024-03-12T09:41:22Z",
    "pushed_at": "2026-08-20T11:33:08Z",
    "language": "Python",
    "stargazers_count": 12,
    "forks_count": 3
  }
]
```

### Visualization (canvas `c1`, responsive width × 360)

Horizontal bar chart of rate limits by authentication method, rounded bars (radius 4), value labels to the right of each bar.

- **Title (bold 14px `#1a5276`, top center):** "Rate Limits by Authentication Method (requests/hr)".
- **Bars (label right-aligned 12px `#2c3e50`; scale max 5000; bar height 38px, gap 22px, first row at y=50; label column 180px; right margin 70px):**

| Label | Value | Color | Value label |
|---|---|---|---|
| Unauthenticated | 60 | `#e74c3c` | 60 |
| Personal Access Token | 5000 | `#27ae60` | 5,000 |
| GitHub App Installation | 5000 | `#1a5276` | 5,000+ |
| GraphQL (points/hr) | 5000 | `#e67e22` | 5,000 |

- **Bar style:** fill at alpha 0.8, minimum drawn width 8px; value labels bold 13px `#2c3e50` placed 8px right of bar end.
- **Gridlines:** dashed (3/3) `#ddd` vertical lines at 1000, 2000, 3000, 4000, 5000.
- **Annotation (italic 11px `#666`, centered below bars):** "Authenticated = 83x more capacity than unauthenticated".

## Official API References

- [GitHub REST API](https://docs.github.com/en/rest) — full REST endpoint reference and usage guides
- [REST Rate Limits](https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api) — primary and secondary limits by authentication method

## Regeneration instructions

- **Layout:** single-row `.obj-table` (full width, border-collapse): left `<td>` 45% with "What you can get" (`.obj-title` + `<ul>`), a `.key-point` callout, then "Watch out for" (`.obj-title` with `margin-top:18px` + `<ul>`); right `<td>` 55% (text-align center) with `.payload-note` (italic), a `<pre class="payload">` JSON block, and the canvas. After the table, an `h2` "Official API References" with a plain `<ul>` of external links. The verified badge on this page is a `<div class="verified">`.
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` badge — 0.8em `#888`, 1px solid `#ddd` border, inline-block, padding 2px 10px, radius 4px; h2 1.3em `#1a5276` with 2px solid `#2980b9` bottom border; `.obj-title` bold 1.1em `#1a5276`; table cell borders `1px solid #2980b9`, padding 16px; li 0.93em; links `#1a5276`.
- **Payload / key-point style:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em, white-space pre, left-aligned; `.payload-note` — 0.82em italic `#666`, left-aligned; `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em.
- **Canvas:** `<canvas id="c1" height="360">` with CSS `display:block; margin:16px auto 0; width:100%`; width taken from `canvas.offsetWidth` at draw time, backing store scaled by `window.devicePixelRatio` (`ctx.setTransform(dpr,0,0,dpr,0,0)`), redrawn on window resize.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. No nav bar, no back/home links.
