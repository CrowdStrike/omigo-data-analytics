# Learning from Examples

**Page type:** detail page (tutorial: 4 card-sections, each a two-column table.layout — text left 50%, canvas right 50%)
**HTML title tag:** Learning from Examples

**Subtitle:** Instead of writing the rules for a spam filter, show it 1,000 emails already marked spam or normal — and let the rules fall out of the data

## Two Ways to Build a Spam Filter

Tags: `core idea` (blue), `running example` (green)

- **The old way** — a person writes rules by hand: "if the email contains 'free', mark it spam"
- **The new way** — show the computer 1,000 emails already marked spam or normal
- **What it finds** — words and habits that separate the two piles, counted from the data
- **The flip** — nobody types the rules; the rules fall out of the labeled examples
- **The name** — this is machine learning: behavior learned from data, not coded by hand

*Example (italic):* A filter shown 1,000 labeled emails learns that "free" screams spam — without anyone telling it.

**Key point:** Machine learning is programming with data: you supply labeled examples, and the rules are fitted to them instead of written by a person.

### Visualization (canvas `c1`, 720×300)

Two side-by-side flow pipelines (boxes and arrows) comparing hand rules vs learning.

- **Title (bold 15px, `#1a5276`, top center):** "Write the Rules vs Learn the Rules"
- **Divider:** vertical dashed gray line (`#bdc3c7`, dash 4/3) at x=360.
- **Left pipeline** (orange `#d95926`, header "Hand-written rules" bold 13px at x=185): three 250×38 outlined boxes stacked at y=62/120/178 with downward arrows between; box texts (two lines each, bold 12px): "person writes:" / "contains \"free\" → spam"; "spammer switches to" / "\"fr-ee\" — the rule misses"; "person writes" / "another rule… forever". Footer (bold 12px orange): "brittle: every trick needs a new rule".
- **Right pipeline** (green `#008300`, header "Learning from examples" at x=540): boxes "1,000 labeled emails" / "400 spam / 600 normal"; "count the patterns" / "that split the two piles"; "filter scores new email" / "retrain on fresh labels". Footer: "the rules come from the data itself".
- **Bottom caption (bold 13px violet `#4a3aa7`, centered):** "same task — but only one side rewrites itself when spam changes"
- Boxes have faint fill `rgba(0,0,0,0.02)` and 2px colored borders; arrows are 2px colored lines with filled triangle heads.

## Counting Words in 1,000 Labeled Emails

Tags: `worked example` (green)

- **The pile** — 1,000 emails: 400 marked spam, 600 marked normal by real users
- **Count "free"** — appears in 240 of 400 spam (60%) but only 30 of 600 normal (5%)
- **Count "winner"** — 140 of 400 spam (35%) vs 6 of 600 normal (1%)
- **Count "meeting"** — 8 of 400 spam (2%) vs 210 of 600 normal (35%)
- **Score a new email** — it contains "free" and "winner": both point to the spam pile

*Example (italic):* Redo it with a pencil: 240/400 = 60% and 30/600 = 5% — "free" is 12x more common in spam.

**Key point:** This counting IS the learning — no magic, just word rates pulled from labeled examples and reused to score new emails.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: word rate in spam vs normal.

- **Title (bold 15px, `#1a5276`, top center):** "How Often Each Word Appears, by Pile (1,000 labeled emails)"
- **Data:** words `["free", "winner", "meeting"]` (quoted in labels); spam % `[60, 35, 2]` in orange `#d95926` (alpha 0.8); normal % `[5, 1, 35]` in blue `#2a78d6` (alpha 0.8).
- **Layout:** y-axis at x=70 with gray labels 0%/35%/70% (scale max 70%, chart height 165, baseline y=240); three groups of two 56px-wide bars, 8px inside gap; bold percent value labels above each bar in the bar's color; bold word labels below the baseline.
- **Legend (top right at x=560):** orange swatch "in spam (of 400)"; blue swatch "in normal (of 600)".
- **Annotation (bold 13px magenta `#d55181`, centered at x=320, y=52):** "\"free\": 60% of spam vs 5% of normal — 12x more common in spam"
- **Bottom caption (12px `#444`, centered):** "share of emails in each pile containing the word"

## Why Rules Break and Learning Keeps Up

Tags: `where it's used` (blue), `watch out` (orange)

- **Spammers adapt** — "free" becomes "fr-ee" and "FR33"; the hand rule silently misses
- **Rule pileup** — each trick needs one more exception, until hundreds of rules collide
- **Retraining** — feed last month's labeled emails back in; the patterns refresh themselves
- **The gap** — here hand rules fall 95% to 70% in 6 months; the retrained model holds ~93%
- **Same story elsewhere** — fraud, churn, ranking: any moving target outgrows hand rules

*Example (italic):* The rulebook that took a year to write decayed in months; the model relearned in one run (illustrative).

**Key point:** Hand-written rules are brittle; learned patterns generalize — and retraining keeps them current as the world shifts.

### Visualization (canvas `c3`, 720×300)

Two-line time series over 7 monthly points.

- **Title (bold 15px, `#1a5276`, top center):** "Spam Caught Over 6 Months (illustrative)"
- **Data:** x = months 0–6; hand rules `[95, 90, 84, 79, 75, 72, 70]` in orange `#d95926`; retrained model `[93, 93, 92, 93, 92, 93, 93]` in green `#008300`. Both lines width 3 with 4px dots.
- **Axes:** y from 60% to 100%, gray tick labels every 10%; L-shaped gray axis; padding top 50 / bottom 55 / left 65 / right 175; month numbers 0–6 below the x-axis; x-axis title (12px `#444`, centered): "months since launch".
- **Annotations (bold 13px):** orange "hand rules decay as spammers adapt: 95% → 70%" near the falling line; green "retrained monthly: holds ~93%" above the flat line.
- **Legend (right side):** orange swatch "hand-written rules"; green swatch "retrained model".

## The Data Is the Program

Tags: `common mistake` (red), `rule of thumb` (blue)

- **More examples help** — accuracy climbs 72% to 93% going from 50 to 1,000 labeled emails
- **Better labels help** — mislabeled emails teach wrong patterns: garbage in, garbage learned
- **Code stays tiny** — the counting code never changes; only the data changes
- **Debug the data** — a bad model usually means bad or too few examples, not a bad algorithm
- **The confusion** — beginners tweak the algorithm first; practitioners inspect the data first

*Example (italic):* Doubling the labeled pile beat a week of clever code tweaks (72% at 50 emails, 93% at 1,000 — illustrative).

**Common mistake (key-point callout):** Blaming the algorithm when the examples are the problem — in ML the examples ARE the program, so improve them first.

### Visualization (canvas `c4`, 720×300)

Single-line learning curve: accuracy vs number of labeled examples.

- **Title (bold 15px, `#1a5276`, top center):** "Filter Accuracy vs Labeled Emails (illustrative)"
- **Data:** x values `[50, 100, 250, 500, 1000]` (spaced proportionally along a numeric axis from 50 to 1000); accuracy `[72, 78, 85, 90, 93]` — blue `#2a78d6` line width 3 with 5px dots and bold blue percent labels above each point.
- **Axes:** y 60%–100% with gray tick labels every 10%; padding top 50 / bottom 60 / left 65 / right 40.
- **Annotation (bold 14px green `#008300`, centered mid-chart):** "more labeled examples beat cleverer code: 72% → 93%"
- **Bottom caption (12px `#444`, centered):** "labeled emails used for training (same counting code every time)"

## Regeneration instructions

- **Template:** tutorials topic-page skeleton (most-powerful-signals compact style). Each `.card-section` has an `<h2>` (1.3rem `#1a5276`, 2px bottom border `#2980b9`) and a `table.layout` with `.text-col` (50%) and `.viz-col` (50%).
- **Left column per section:** `.tags` pill row first (0.72rem bold, 10px radius pills — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`), then a `<ul>` of one-line bullets each opening with `<b>` term in `#1a5276`, then an italic `.example` line (`#555`, 0.9rem), then a `.key-point` callout (background `#f8f9fa`, left border 3px `#e74c3c`, 0.9rem, with a bold lead like "Key point:" or "Common mistake:").
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px bottom border `#2980b9`; `.subtitle` `#666` 0.95rem. No nav bar, no back/home links.
- **Canvas:** all 720×300 intrinsic, CSS `width:100%`, 1px border `#e0e0e0` radius 4px; scaled via `window.devicePixelRatio` in a shared `setup(id)` helper reading width/height attributes (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- **Data:** all values hardcoded literal arrays; no `Math.random()`; invented numbers labeled "illustrative" in chart titles/captions.
- In regenerated HTML, any card links use `.html` extensions.
