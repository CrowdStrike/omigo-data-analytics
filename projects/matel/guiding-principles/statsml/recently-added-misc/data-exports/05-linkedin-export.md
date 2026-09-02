# LinkedIn — Data Export

**Page type:** detail page (two-column obj-table layout per section: bullets left 45%, code payloads or canvas right 55%)
**HTML title tag:** LinkedIn — Data Export

**Meta line (`.last-verified`, gray):** Last verified: August 2026

## What's Included

- Connections list (with dates added — reveals networking patterns)
- Messages (full InMail and messaging conversations)
- Job applications (positions applied to, with timestamps)
- Profile views (limited — only last 90 days, anonymized beyond 5)
- Endorsements given and received
- Ad targeting categories (seniority, company size, industry, job function)
- Inferred skills and interests
- Articles/posts published, reactions, comments
- Learning history (LinkedIn Learning courses)
- Search history, companies followed

Right column: two code blocks (`pre > code`):

```
# Connections.csv
First Name,Last Name,Email,Company,Position,Connected On
John,Smith,john@example.com,TechCorp,Senior Engineer,2022-03-15
Jane,Doe,StartupXYZ,VP Engineering,2023-01-08
Mike,Chen,mike@co.io,BigBank,Data Scientist,2021-11-22
```

```
{
  "memberTraits": {
    "jobSeniority": "Senior",
    "companySize": "1001-5000",
    "industry": "Technology",
    "jobFunction": "Engineering",
    "yearsOfExperience": "5-10",
    "inferredSalaryRange": "$120k-$180k",
    "skills": ["Python", "Machine Learning", "SQL"]
  }
}
```

## How to Request & Delivery

- Settings → Data Privacy → Get a copy of your data
- Two options: "Want something in particular?" (fast, partial) or "Download larger data archive" (everything)
- Partial export: available in ~10 minutes
- Full archive: 24 hours typical delivery
- Delivered as .zip with CSV files (one per category)
- Can request once every 24 hours approximately
- Data goes back to account creation

### Visualization (canvas `networkGraph`, responsive width×400)

Hub-and-spoke network graph: central "You" node connected to industry clusters, each cluster ringed by individual connection dots.

- **Central node:** filled circle radius 18 at canvas center, `#1a5276`, white bold 11px label "You".
- **Clusters (placed on a circle of radius = 0.33 × min(W,H) around the center, at fixed angles):**
  - Technology — count 12, `#1a5276`, angle −π/6
  - Finance — count 8, `#27ae60`, angle π/4
  - Consulting — count 6, `#e67e22`, angle 2π/3
  - Healthcare — count 4, `#e74c3c`, angle π
  - Education — count 3, `#8e44ad`, angle −2π/3
  - Other — count 5, `#5dade2`, angle −π/2
- **Cluster circles:** radius = 8 + count × 1.5; fill = cluster color at ~13% alpha (hex + `22`), stroke = cluster color at 2px.
- **Spokes:** 1.5px lines from center to each cluster in `rgba(26,82,118,0.25)`.
- **Member dots:** per cluster, `count` dots of radius 3 in the cluster color, spaced evenly by angle at radius clusterRadius + 12 + random(0–8), each connected to the cluster center by a 0.8px line in `rgba(26,82,118,0.15)`.
- **Labels (11px `#2c3e50`, centered below each cluster):** "Technology (12)", "Finance (8)", "Consulting (6)", "Healthcare (4)", "Education (3)", "Other (5)".

## What's Conspicuously Missing

- Profile strength / Social Selling Index algorithm
- Recruiter search ranking (where you appear in recruiter searches)
- Content distribution weights (why some posts get reach, others don't)
- Feed ranking algorithm signals
- Who viewed your profile beyond the 5 shown (full list exists for Premium/Recruiter users)
- InMail response rate scores
- Job-seeker signal detection (LinkedIn knows when you're looking)
- Company page analytics attribution to your profile

Right column: JSON code block (`pre > code`):

```
{
  "adTargeting": {
    "companyCategory": "Fortune 500",
    "memberSchools": "Top 50 University",
    "degreeClass": "Graduate Degree",
    "jobSeekerStatus": "Open to opportunities",
    "profileViewerCompanies": [
      "BigTech Inc",
      "Recruiting Firm LLC"
    ]
  }
}
```

## Key point (callout)

The ad targeting categories are the most revealing — they show LinkedIn's complete professional model of you: inferred salary range, seniority level, job-seeker status, and company tier. This is the data recruiters and advertisers pay to target against.

## Regeneration instructions

- **Layout:** detail page. h1, then `.last-verified` line. Three `h2` sections ("What's Included", "How to Request & Delivery", "What's Conspicuously Missing"), each followed by a one-row `table.obj-table` — left `<td>` (45%) with bullets, right `<td>` (55%) with `<pre><code>` blocks or the canvas. Ends with a `.key-point` callout div. In regenerated HTML, any links use .html extensions.
- **Page CSS:** body system sans-serif, `line-height: 1.6`, text `#2c3e50`, padding 30px 40px, white background. h1 1.8rem `#1a5276`; h2 1.3em `#1a5276`, `border-bottom: 2px solid #2980b9`, padding-bottom 6px, margin-top 32px. `table.obj-table` full width, collapsed borders, margin 16px 0; cells padding 16px, vertical-align top, **no cell borders** on this page. `li` 0.93em, 6px bottom margin.
- **Blocks:** `pre` — background `#f4f6f7`, border `1px solid #dce1e4`, radius 4px, padding 12px 14px, 0.82em; `code` monospace (SF Mono/Consolas). `.key-point` — background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 10px 14px, 0.93em.8em, margin-left 12px.
- **Canvas:** `display: block; margin: 0 auto; width: 100%`, height 400px; scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Note the member-dot ring radius uses small random jitter, so exact dot positions vary per render.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, `#8e44ad` purple, `#5dade2` light blue; text `#2c3e50`. No nav bar, no back/home links.
