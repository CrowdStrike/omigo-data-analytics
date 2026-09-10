# LinkedIn

**Page type:** detail page (two-column obj-table layout under an "Overview" h2: text left 45%, payload + canvas right 55%, one row)
**HTML title tag:** LinkedIn — Platform APIs

**Subtitle:** Lets an app sign people in, post content, and report on the pages and ad campaigns you own — it does not give out profiles, people search, or the professional network.

**Verified badge:** Last verified: August 2026

## Overview

### What you can get

- Sign-in with the member's name, photo and verified email
- The ability to post content as a member or as your company page
- Aggregate audience statistics for company pages you administer
- Ad campaign performance for your own ad accounts

### Key-point callout

**There is no people search, no connection graph, and no way to look up someone else's profile.** Any product that plans to get candidate or professional-network data "from the LinkedIn API" is not buildable — the profile datasets sold in the market do not come from the official API. This is one of the most expensively misunderstood limitations in the platform landscape.

### Watch out for

- Even your own signed-in user yields only a thin identity record — no job history, skills or connections
- Scraping is against LinkedIn's terms and actively enforced
- Recent statistics arrive incomplete and revise upward for a few days — early snapshots undercount
- Endpoints are versioned monthly and old ones retire, so tutorials more than a year old often describe calls that no longer work

### Sign-in profile — and this is close to everything

Code block (pre, monospace, left border #1a5276):

```
// GET /v2/userinfo
{
  "sub":            "782bbtaQ",
  "name":           "Priya Raman",
  "picture":        "https://media.licdn.com/...",
  "locale":         { "country": "US", "language": "en" },
  "email":          "priya.raman@example.com",
  "email_verified": true
}

// Not obtainable with any scope: headline, positions,
// education, skills, connections, anyone else's profile
```

### Assumed available vs. actually available

### Visualization (canvas `linkedinGapMatrix`, 100% width × 380px CSS height)

Two-column expectation-vs-reality matrix: 15 data-item rows × 2 columns ("assumed available" vs "actually available via API"), each cell a colored pill marked "yes" / "agg" / "no".

- **Title (bold 13px, `#1a5276`, top left):** "What teams assume the LinkedIn API returns, vs. what it returns".
- **Subtitle (italic 10px, `#666`):** "left column is a qualitative reading of common expectations, not survey data".
- **Column headers (bold 10.5px, centered):** "assumed available" in purple `#8e44ad`; "actually available via API" in blue `#1a5276`.
- **Rows (label: assumed, actual)** with 0 = not available red `#e74c3c` "no", 1 = aggregate/partial orange `#e67e22` "agg", 2 = available green `#27ae60` "yes":
  - Full name: 2, 2
  - Verified email: 2, 2
  - Profile picture, locale: 2, 2
  - Headline: 2, 0
  - Current position: 2, 0
  - Full position history: 2, 0
  - Education: 2, 0
  - Skills, endorsements: 2, 0
  - Connection count: 2, 0
  - Connection list: 2, 0
  - 1st / 2nd-degree graph: 1, 0
  - People search by title/company: 2, 0
  - Arbitrary profile lookup: 2, 0
  - Own org page stats: 2, 1
  - Own ad performance: 2, 2
- **Rendering:** padding top 70, right 14, bottom 46, left min(184, 38% of width); cell pills max 120×16px with `rgba(0,0,0,0.12)` border and bold 9px mark text; the "assumed" column drawn at 0.45 alpha with dark `#2c3e50` mark text, the "actual" column at 0.88 alpha with white mark text; zebra bands `rgba(26,82,118,0.04)` on odd rows; row labels 10.5px right-aligned, colored red `#e74c3c` when assumed ≥1 but actual = 0, otherwise `#2c3e50`; grid frame and center divider `#ddd`.
- **Gap bracket:** dashed purple rectangle (`#8e44ad`, width 2, dash 4/3) spanning both columns from the "Headline" row through the "Arbitrary profile lookup" row, with italic 10px purple label above it: "the gap: profile depth, the network and search — withdrawn or never offered".
- **Legend (bottom left, 10px):** color swatches — green "available", orange "aggregate / bucketed only", red "not provided".
- **Conclusion (italic 10px, red `#e74c3c`, bottom left):** "the professional graph is absent from the sanctioned API surface entirely".

## Official API References

- [LinkedIn Developer Documentation](https://learn.microsoft.com/en-us/linkedin/) — Microsoft Learn root for all LinkedIn API products
- [Marketing API](https://learn.microsoft.com/en-us/linkedin/marketing/) — campaign management, reporting, Lead Gen and Conversions documentation

## Regeneration instructions

- **Layout:** single detail page: h1, `.subtitle` paragraph, `.verified` badge span, `h2` "Overview", one `.obj-table` (full-width, border-collapse, one `<tr>`): left `<td>` 45% with `.section-head` headings ("What you can get", "Watch out for"), bullet lists and a `.key-point` callout; right `<td>` 55% with a `.section-head`, a `<pre>` JSON payload and the canvas (`height="380"` attribute). Then an `h2` "Official API References" with a two-link list. Links in HTML are external URLs as given.
- **Page CSS:** body system sans-serif (-apple-system stack), line-height 1.6, text `#2c3e50`, white background, padding 30px 40px; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` badge — background `#eaf2f8`, border `1px solid #2980b9`, color `#1a5276`, 2px 10px padding, 4px radius, 0.8em; h2 1.3em `#1a5276` with `2px solid #2980b9` bottom border; table cells `1px solid #ddd` border, 16px padding, top-aligned; `.section-head` bold `#1a5276` 0.95em; li 0.93em; links `#1a5276`.
- **Pre style:** background `#f8f9fa`, left border `3px solid #1a5276`, ui-monospace 0.78em, 12px padding, 4px radius.
- **Key-point style:** background `#f8f9fa`, left border `3px solid #e74c3c`, padding 10px 14px, 0.93em.
- **Canvas:** `display: block; width: 100%; margin: 16px auto 0`; fixed 380px CSS height set in JS; sized from `getBoundingClientRect()` and scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), redrawn on window resize.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple accent `#8e44ad`; zebra `rgba(26,82,118,0.04)`; grays `#666`/`#ddd`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
