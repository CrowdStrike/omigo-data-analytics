# Queries Without Keywords

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Queries Without Keywords

**Subtitle:** Nobody types anything — the system compiles a query out of your profile, your history, or the page you are on

**Grid-card description:** A job feed matches you without a search box — your profile is compiled into an elaborate query, the same trick behind ads, dating matches, and "more like this".

## A Job Feed With No Search Box

**Tags:** `core idea` (blue), `profile as query` (green)

- **The feed** — Alice opens a job site's "jobs for you" tab and types nothing at all
- **The profile** — her page says data engineer, Spark / SQL / Python, 5 years, Pune, open to remote
- **The compiler** — the system rewrites that profile into an elaborate query she never sees
- **The query** — title synonyms, skill clauses, a seniority band, a location-or-remote condition
- **The search** — that generated query runs against the jobs index like any typed query would

*Example (italic):* Alice never searched "data engineer jobs" — her profile asked for her, in far more detail than she would have typed.

**Key point:** The matching feed is ordinary search with the query written by the system — the profile is the query.

### Visualization (canvas `c1`, 720×300)

A left-to-right pipeline: a profile card feeding a query compiler, producing a generated multi-clause query, which runs against the jobs index and returns a ranked feed.

- **Title (bold 15px ink `#1a5276`, top center, y=22):** "The Profile Compiles Into a Query".
- **Profile card (x=20, y=55, w=155, h=170, white fill, 2px blue `#2a78d6` border, radius corners optional):** header bold 12px blue centered at x=97, y=75: "Alice's profile"; five 11px `#2c3e50` lines left-aligned at x=30, y=98/118/138/158/178: "data engineer", "Spark · SQL · Python", "5 years experience", "Pune", "open to remote".
- **Arrow 1 (2px ink with small filled triangle head):** from (180, 140) to (215, 140).
- **Compiler chip (x=218, y=124, w=92, h=32, fill `rgba(74,58,167,0.12)`, 1px violet `#4a3aa7` border):** bold 11px violet centered "query compiler".
- **Arrow 2 (2px ink, triangle head):** from (315, 140) to (348, 140).
- **Generated query box (x=352, y=48, w=220, h=184, fill `rgba(0,131,0,0.08)`, 2px green `#008300` border):** header bold 12px green centered at x=462, y=68: "generated query"; five 11px `#2c3e50` lines left-aligned at x=362, y=92/114/136/158/180: "title: data engineer", "  OR etl developer", "skills: spark, sql, python", "seniority: 4–7 years", "location: pune OR remote"; caption 11px `#6b7280` centered at x=462, y=208: "she never sees this".
- **Arrow 3 (2px ink, triangle head):** from (577, 140) to (608, 140).
- **Results box (x=612, y=96, w=88, h=88, white fill, 1px `#ccc` border):** header bold 11px ink centered at x=656, y=116: "jobs index"; three light-grey result bars (`#e5e9ef`, x=622, w=68, h=6) at y=132, 148, 164.
- **Annotation (bold 12px violet, centered, y=278):** "no keywords typed — the profile is the query".

## Scoring Four Jobs Against the Generated Query

**Tags:** `worked example` (blue), `clause weights` (green)

- **The clauses** — title match +3, each of the 3 skills +2, seniority in band +2, location fits +1
- **Job A** — data engineer, remote: title 3, skills 6, seniority 2, location 1 — total 12 of 12
- **Job B** — ETL developer in Pune: synonym title 3, two skills 4, seniority 2, location 1 — total 10
- **Job C** — senior data scientist, remote: title 0, two skills 4, band miss 0, location 1 — total 5
- **Job D** — data engineer, onsite abroad: title 3, one skill 2, seniority 2, location 0 — total 7

*Example (illustrative, italic):* The feed shows A, B, D, C — a ranking Alice can recompute by hand from four weighted clauses.

**Key point:** Each job is scored clause by clause against the generated query — the "magic" feed is a sum of a few weighted matches.

### Visualization (canvas `c2`, 720×300)

A stacked horizontal bar chart: four jobs, each bar built from colored clause segments (title, skills, seniority, location), sorted by total score, with the clause legend on top.

- **Title (bold 15px ink, top center, y=22):** "Four Jobs, Scored Clause by Clause (illustrative)".
- **Legend (y=46, centered row starting x=120):** four swatches (12×12) with 11px `#2c3e50` labels, gap ~40px: "title +3" blue `#2a78d6`, "skills +2 each" green `#008300`, "seniority +2" violet `#4a3aa7`, "location +1" yellow `#c98500`.
- **Bars:** rows at y = 78, 126, 174, 222 (h=30). Row label bold 12px `#2c3e50` left-aligned at x=25, y = row mid: "Job A — data engineer, remote", then "Job B — ETL developer, Pune", "Job D — data engineer, abroad", "Job C — sr data scientist, remote". Bars start at x=270, scale 32px per point. Segments drawn left to right in legend order with the clause points each job earned: A = 3/6/2/1 (total 12), B = 3/4/2/1 (total 10), D = 3/2/2/0 (total 7), C = 0/4/0/1 (total 5). Segment fills: blue `rgba(42,120,214,0.75)`, green `rgba(0,131,0,0.65)`, violet `rgba(74,58,167,0.65)`, yellow `rgba(201,133,0,0.75)`.
- **Totals:** bold 13px ink right of each bar (8px gap): "12", "10", "7", "5".
- **Baseline:** 1px `#999` vertical line at x=270 from y=70 to y=258.
- **Annotation (bold 12px green, centered, y=284):** "the feed order is just this sum, highest first".

## The Same Trick Across Tech Domains

**Tags:** `where it's used` (blue), `rich examples` (orange)

- **Recruiter search** — the roles swap: a job posting becomes the query, run over candidate profiles
- **Dating apps** — each profile is a query over the other profiles, and it must match both ways
- **Ad serving** — the page plus the viewer compiles into a query over the ad inventory in ~100 ms
- **"More like this"** — the item you are viewing is the query; the catalog is what it searches
- **Job alerts** — a standing query: the profile-query reruns every night against only the new postings
- **AI assistants** — your conversation so far becomes the retrieval query that fetches documents

*Example (italic):* A recommendations row titled "because you watched…" is your viewing history running as a query.

**Key point:** Every matching product is a search engine where something other than typed words plays the query — a profile, an item, a page, a conversation.

### Visualization (canvas `c3`, 720×300)

A two-column ledger: six domain rows, each with a colored domain chip, then "the query is…" and "it searches…" entries, showing what plays the query role in each domain.

- **Title (bold 15px ink, top center, y=22):** "What Plays the Query in Each Domain".
- **Column headers (bold 12px `#6b7280`):** "domain" left-aligned x=30, "the query is…" left-aligned x=210, "it searches…" left-aligned x=470, all at y=52.
- **Header underline:** 1px `#ccc` from x=25 to x=695 at y=60.
- **Six rows at y = 84, 120, 156, 192, 228, 264.** Each row: domain chip (x=25, w=150, h=22, centered text bold 11px colored, tint fill + 1px colored border, top at rowY−15), then 11.5px `#2c3e50` query text at x=210 and target text at x=470 (baseline rowY).
- **Rows (chip label / chip color / query is / it searches):**
  1. "job feed" blue `#2a78d6`: "your profile" / "job postings"
  2. "recruiter search" aqua `#199e70`: "the job posting" / "candidate profiles"
  3. "dating app" magenta `#d55181`: "your profile, both ways" / "other profiles"
  4. "ad serving" orange `#d95926`: "the page + the viewer" / "the ad inventory"
  5. "more like this" violet `#4a3aa7`: "the item on screen" / "the whole catalog"
  6. "AI assistant" green `#008300`: "the conversation so far" / "documents to retrieve"
- **Annotation (bold 12px orange `#d95926`, right-aligned at x=695, y=290):** "same engine — different thing in the query slot".

## No Typed Words Means No Typed Feedback

**Tags:** `why it matters` (blue), `common mistake` (red)

- **The missing log** — there are no query strings to read; applies, swipes, and clicks are the only labels
- **Stale queries** — a profile last edited two jobs ago keeps running as the query every day
- **Cold start** — a thin new profile compiles into a weak query, so the first feed is generic
- **Explainability** — "why am I seeing this" is answered by listing the clauses that matched
- **The confusion** — this is not a different technology from search; it is search with the roles swapped

*Example (illustrative, italic):* Alice's feed still pushes junior roles — her profile, the query, hasn't been told about her promotion.

**Key point:** Compiled queries inherit search's machinery but not its feedback — and they go stale silently, because nobody retypes a profile the way they retype a query.

### Visualization (canvas `c4`, 720×300)

Left panel: the two-sided matching diagram — a candidate card and a job posting card with arrows both ways, each side taking a turn as the query. Right panel: the feedback contrast — a crossed-out search log vs the action log that replaces it.

- **Title (bold 15px ink, top center, y=22):** "Both Directions, and What the Log Records".
- **Divider:** dashed `#bdc3c7` vertical line at x=380, y=40 to y=270 (dash 4/3).
- **Left header (bold 12px ink, centered at x=200, y=50):** "it runs both ways".
- **Candidate card (x=35, y=78, w=130, h=76, white fill, 2px blue border):** bold 12px blue centered at x=100: "candidate" (y=100), 11px `#2c3e50` "profile" (y=118), 11px `#6b7280` "skills · years · city" (y=138).
- **Job card (x=235, y=78, w=130, h=76, white fill, 2px green border):** bold 12px green centered at x=300: "job posting" (y=100), 11px `#2c3e50` "requirements" (y=118), 11px `#6b7280` "title · skills · level" (y=138).
- **Arrow top (2px blue, triangle head):** from (165, 96) to (235, 96); label 11px blue centered at x=200, y=88: "profile queries jobs".
- **Arrow bottom (2px green, triangle head):** from (235, 136) to (165, 136); label 11px green centered at x=200, y=152 below: "posting queries candidates".
- **Left caption (11px `#6b7280`, centered at x=200, y=196):** "same engine, roles swapped".
- **Right header (bold 12px ink, centered at x=550, y=50):** "the feedback that remains".
- **Crossed log box (x=430, y=72, w=240, h=44, white fill, 1px `#ccc` border):** 11px `#6b7280` centered lines "typed query log" (y=90), "(empty — nothing was typed)" (y=106); a 2px red `#e74c3c` diagonal strike from (438, 78) to (662, 110).
- **Action log box (x=430, y=142, w=240, h=96, fill `rgba(0,131,0,0.08)`, 1px green border):** header bold 11px green centered at x=550, y=160: "action log"; three 11px `#2c3e50` lines left-aligned at x=445, y=182/202/222: "applied — Job A", "skipped — Job C", "saved — Job B".
- **Annotation (bold 12px yellow `#c98500`, centered, y=288):** "actions, not words, are the only labels".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` (no index number); subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Shared helper `arrowRight`/`arrowLeft` (or a generic `arrow(x1,y1,x2,y2)`) draws 2px lines with small filled triangle heads.
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` appears only as the strike-through on the empty typed-query log in c4.
- **Data:** everything hardcoded (no randomness). Clause weights (title +3, skill +2 each of 3, seniority +2, location +1, max 12) and the four job scores A=12 (3/6/2/1), B=10 (3/4/2/1), D=7 (3/2/2/0), C=5 (0/4/0/1) appear identically in section 2's text and the c2 chart, labeled "illustrative". The six c3 domain rows match the six bullets of section 3. Alice's five profile lines in c1 match the profile bullet in section 1.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
