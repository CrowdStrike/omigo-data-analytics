# GitLab & Bitbucket APIs

**Page type:** detail page (single-row two-column obj-table: text left 45%, payload + canvas right 55%; followed by a references list)
**HTML title tag:** GitLab & Bitbucket APIs — Platform APIs

**Subtitle:** Pull merge/pull requests, code reviews, commits, and CI pipeline results from GitLab and Bitbucket.

**Verified badge:** Last verified: August 2026

## What you can get

- Merge/pull requests with their created, merged, and closed timestamps
- Review comments and approvals
- Commits with author identity and dates
- CI pipeline and job outcomes, durations, and (GitLab) queue time
- Deployments and releases

**Key point (callout box):** These records are **metadata about a change, not the reasoning behind it**. They show an approval landed eleven minutes after the request; whether the code was actually read is not in the data. Review-latency numbers measure clock time between clicks — reading them as diligence is an assumption, not a measurement.

## Watch out for

- Squash merges and rebases rewrite history — original commit times are destroyed unless captured before merge
- The name on a commit comes from local git settings and may match no platform account
- Paid-tier features return empty or 403 on free plans, so the same analysis silently degrades across groups
- Bitbucket Cloud and self-hosted Bitbucket Data Center are entirely different APIs — establish which one you have first

## Payload (right column)

**Payload note (italic):** Sample merge request (GitLab, abbreviated) — the timestamps that flow metrics are built on.

```json
{
  "iid": 133,
  "title": "Move ingestion job to new warehouse",
  "state": "merged",
  "created_at": "2026-08-11T07:24:16Z",
  "merged_at": "2026-08-14T10:02:41Z",
  "author":    { "username": "ajaiswal" },
  "merged_by": { "username": "r.mehta" },
  "squash": true,
  "user_notes_count": 14,
  "head_pipeline": { "status": "success",
                     "duration": 512,
                     "queued_duration": 37 }
}
```

### Visualization (canvas `c1`, responsive width × 380)

Feature-parity heatmap matrix: 9 concept rows × 2 platform columns, each cell colored by availability level with a status word inside.

- **Title (bold 14px `#1a5276`, top center):** "Is the concept a typed field, or something you have to infer?"
- **Columns (bold 11px `#1a5276` headers, centered above grid):** "GitLab v4", "Bitbucket Cloud 2.0".
- **Cell values** — 2 = typed field (green `#27ae60`, text "typed field"), 1 = partial/gated (orange `#e67e22`, text "partial / gated"), 0 = not exposed (red `#e74c3c`, text "not exposed"). Cell text bold 9.5px white, drawn only when cell width > 110px.

| Row label | GitLab v4 | Bitbucket Cloud 2.0 |
|---|---|---|
| Merge/PR core timestamps | 2 | 2 |
| Reviewers as a field | 2 | 1 |
| Approval records | 1 | 1 |
| Typed state-change events | 2 | 0 |
| CI pipeline + per-job data | 2 | 2 |
| Queue vs run time split | 2 | 0 |
| Deployments / environments | 2 | 1 |
| Admin audit event feed | 1 | 0 |
| GraphQL alternative | 2 | 0 |

- **Geometry:** left margin 14px; label column min(200, 36% width), right-aligned 10.5px `#2c3e50`; grid starts at y=62; row height min(26, available/9); cells at alpha 0.85 with 2px inset.
- **Legend (10px, swatch + text at `#2c3e50`):** green "typed field"; orange "partial, gated by tier, or prose"; red "not exposed — must be inferred or given up".
- **Caption (italic 11px `#666`, bottom center):** "A cross-platform metric is only as good as its weakest column — the red cells decide what is comparable."

## Official API References

- [GitLab REST API](https://docs.gitlab.com/ee/api/rest/) — v4 reference: resources, authentication, pagination
- [Bitbucket Cloud REST API 2.0](https://developer.atlassian.com/cloud/bitbucket/rest/) — full endpoint reference

## Regeneration instructions

- **Layout:** single-row `.obj-table` (full width, border-collapse): left `<td>` 45% with "What you can get" (`.obj-title` + `<ul>`), a `.key-point` callout, then "Watch out for" (`.obj-title` with `margin-top:18px` + `<ul>`); right `<td>` 55% (text-align center) with `.payload-note` (italic), a `<pre class="payload">` JSON block, and the canvas. After the table, an `h2` "Official API References" with a plain `<ul>` of external links. Verified badge is a `<span class="verified">`.
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` badge — 0.8em `#888`, 1px solid `#ddd` border, inline-block, padding 2px 10px, radius 4px; h2 1.3em `#1a5276` with 2px solid `#2980b9` bottom border; `.obj-title` bold 1.1em `#1a5276`; table cell borders `1px solid #2980b9`, padding 16px; li 0.93em; links `#1a5276`.
- **Payload / key-point style:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em, white-space pre, left-aligned; `.payload-note` — 0.82em italic `#666`, left-aligned; `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em.
- **Canvas:** `<canvas id="c1" height="380">` with CSS `display:block; margin:16px auto 0; width:100%`; width taken from `canvas.offsetWidth` at draw time, backing store scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), redrawn on window resize.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. No nav bar, no back/home links.
