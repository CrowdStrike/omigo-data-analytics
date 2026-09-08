# Political / Contextual Ads — Domain Pitfalls

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one table per h2 section)
**HTML title tag:** Political / Contextual Ads - Domain Pitfalls

**Subtitle:** Data and measurement pitfalls in political and contextual advertising.

## Microtargeting Feedback Loops

**Microtargeting Feedback Loops**

- **The loop:** Show the ad to persuadable voters, they respond, and the model logs them as the winning slice.
- **Bad inference:** It concludes that ALL similar-looking people are persuadable and narrows targeting to them.
- **Precision kills reach:** Over-indexing on a narrow slice of responders progressively shrinks the audience.
- **Who gets excluded:** The vast majority of actually-persuadable voters who simply look different on paper.
- **Missed population:** The ever-tightening audience misses the broader persuadable population entirely.

### Visualization (canvas `canvas1`, 720×200 drawn; HTML attribute 720×300)

Shrinking-circles funnel: audience narrows across five targeting stages.

- **Title (17px `#e74c3c`, centered at x=360, y=18):** "Precision kills reach".
- **Circles:** five circles at y=95, centers starting at x=80 with 145px spacing; radii `[55, 42, 30, 20, 12]`; stroke colors `#27ae60`, `#2ecc71`, `#f39c12`, `#e67e22`, `#e74c3c` (2.5px), fill = same color at 20%-alpha (hex + "33").
- **Stage labels (12px `#2c3e50`, centered below each circle at y=165, multi-line):** "Broad / Audience", "Initial / Targeting", "Responders / Only", "Narrow / Lookalikes", "Tiny / Slice".
- **Connectors:** gray `#7f8c8d` 1.5px arrows between consecutive circles with filled triangular arrowheads.

## Dark Posts Invisible to Non-Targets

**Dark Posts Invisible to Non-Targets**

- **Invisible by design:** The ad is shown only to a specific audience, so opponents never see it to respond.
- **No monitoring:** Fact-checkers cannot review content they are never served in the first place.
- **Asymmetric information:** Misleading claims propagate unchecked with no counter-speech in the feed.
- **Excluded challengers:** The very people who would challenge a claim are outside the target audience.
- **Not a bug:** The accountability gap is an architectural feature of micro-targeted ad delivery.

### Visualization (canvas `canvas2`, 720×200 drawn; HTML attribute 720×300)

Diagram: target audience bubble with an ad, three excluded groups crossed out.

- **Target bubble:** blue `#2980b9` circle at (180, 100), radius 60, fill `#2980b9` at ~13% alpha ("22" hex), 2px stroke; centered 13px blue text "Target" / "Audience"; green `#27ae60` rectangle (160, 55, 40×25) with white 11px "AD" inside.
- **Excluded groups:** three red `#e74c3c` circles (radius 28, fill red at ~7% alpha, 1.5px stroke) at (420, 50), (420, 100), (420, 150), each with a red 2px X mark inside and a 12px red label to the right: "Opponents", "Fact-Checkers", "Regulators".
- **Invisibility lines:** dashed (5/5) light gray `#bdc3c7` 1.5px lines from the target bubble edge (240, 100) to each excluded circle.
- **Caption (17px `#1a5276`, centered at (560, 185)):** "Accountability Gap by Design".

## Voter File Staleness

**Voter File Staleness**

- **The scenario:** A voter registered at an address 3 years ago and has moved twice since then.
- **File vs reality:** The record still says "suburban homeowner" while reality is now "urban renter."
- **Decay rate:** Roughly 10-15% of records go stale each year from moves and life changes.
- **Administrative lag:** Registration updates trail the actual move by months, so the gap persists.
- **The cost:** Targeting on stale demographics means wasted spend and wrong messaging.

### Visualization (canvas `canvas3`, 720×200 drawn; HTML attribute 720×300)

Timeline with decay bar: voter record drifts from reality over four years.

- **Timeline:** horizontal light gray `#bdc3c7` 2px line from (60, 80) to (680, 80); four 8px dots at x = 90, 255, 420, 585 (165px spacing) colored `#27ae60`, `#f39c12`, `#e67e22`, `#e74c3c`.
- **Year labels (bold 13px `#2c3e50`, above dots at y=60):** 2021, 2022, 2023, 2024.
- **Event labels (12px, in the dot's color, multi-line below dots starting y=108):** "Registered: / Suburban / Homeowner"; "Moved to / City Apt"; "Moved / Again"; "File Still Says: / \"Suburban / Homeowner\"".
- **Decay bar (bottom):** gray `#ecf0f1` track (90, 165, 530×18); filled to 85% width with a linear gradient `#27ae60` → `#f39c12` → `#e74c3c`; overlaid 11px `#2c3e50` label: "Data Accuracy Decay: ~15%/year".

## Sentiment Manipulation at Scale

**Sentiment Manipulation at Scale**

- **Manufactured consensus:** 1M bot accounts amplifying one narrative fabricate "public opinion."
- **Social listening fooled:** Measured sentiment reflects account volume, not the number of real people.
- **Divergence:** Polls and actual votes diverge because online sentiment does not equal real sentiment.
- **Fringe looks mainstream:** Coordinated inauthentic behavior inflates a minority position into a majority.
- **Strategic damage:** Campaigns using social signals as a proxy for voter intent make distorted decisions.

### Visualization (canvas `canvas4`, 720×200 drawn; HTML attribute 720×300)

Side-by-side bar charts: online sentiment (manipulated) vs actual votes.

- **Left chart (title bold 14px `#1a5276` at (180, 22)):** "Online Sentiment (Bots)" — bars 60px wide, base y=160, max height 120, at x=120 and x=220: Candidate A 72% `#e74c3c`, Candidate B 28% `#3498db`; category labels 12px and bold 13px value labels ("72%", "28%") in `#2c3e50`.
- **Center:** bold 28px red `#e74c3c` "≠" at (365, 105).
- **Right chart (title "Actual Votes" at (540, 22)):** same bar style at x=480 and x=580: Candidate A 44% `#e74c3c`, Candidate B 56% `#3498db`, with "44%" / "56%" value labels.
- **Annotation (11px `#7f8c8d`, centered at (180, 185)):** "1M bots amplifying".

## Suppression vs Persuasion Distinction

**Suppression vs Persuasion Distinction**

- **Goal A — persuasion:** Convince undecided voters to turn out and vote for your candidate.
- **Goal B — suppression:** Convince the opponent's base to NOT vote at all, or to stay home.
- **Same tool:** Targeted messaging serves both of these ethically opposite goals equally well.
- **Data is silent:** The logs alone cannot tell you which of the two goals is being pursued.
- **Morally neutral machinery:** Optimizing "voter turnout" or "voter apathy" looks identical in the pipeline.
- **Human choice:** Which metric gets optimized is a decision made off-platform and invisible in the data.

### Visualization (canvas `canvas5`, 720×200 drawn; HTML attribute 720×300)

Diagram: one central tool box with arrows to two opposite goal circles.

- **Central box:** blue `#2980b9` rectangle (295, 70, 130×50) with white bold 13px text "Targeted" / "Messaging".
- **Goal A (left):** green `#27ae60` circle at (130, 95), radius 45, fill green at ~13% alpha, 2.5px stroke; text: bold 13px "Goal A:", 12px "Persuade to" / "VOTE FOR".
- **Goal B (right):** red `#e74c3c` circle at (590, 95), radius 45, same style; text: bold 13px "Goal B:", 12px "Convince to" / "NOT VOTE".
- **Arrows:** green 2px arrow from box left edge to Goal A; red 2px arrow from box right edge to Goal B (both with filled triangular arrowheads).
- **Bottom label (17px `#7f8c8d`, centered at (360, 175)):** "Same tool — ethically opposite goals — data cannot distinguish".

## Regulatory Disclosure Fragmentation

**Regulatory Disclosure Fragmentation**

- **Patchwork rules:** Disclosure and "paid for by" requirements differ per platform, state, and country.
- **Obscured sources:** Dark money routed through PACs deliberately obscures the attribution chain.
- **Disclosure defeated:** The chain is broken even in jurisdictions where disclosure is legally required.
- **Three verdicts:** One ad is legal in one jurisdiction, needs a disclaimer in another, banned in a third.
- **Simultaneous delivery:** All three verdicts apply at once, to one creative, served from one platform.

### Visualization (canvas `canvas6`, 720×200 drawn; HTML attribute 720×300)

Status matrix (platforms × jurisdictions) plus an obscured attribution chain.

- **Grid:** 3×3 cells starting at (200, 40), cell 120×36 (drawn 116×32 with 4px gaps); column headers bold 12px `#1a5276` "State A", "State B", "Federal"; row labels right-aligned `#2c3e50` "Platform X", "Platform Y", "Platform Z".
- **Cell statuses (fill = status color at 20% alpha, 1.5px border, 12px centered status text):**
  - Platform X: Required / Optional / Banned
  - Platform Y: Optional / Required / Required
  - Platform Z: None / Required / Optional
- **Status colors:** Required `#27ae60`, Optional `#f39c12`, Banned `#e74c3c`, None `#95a5a6`.
- **Attribution chain (bottom):** 13px `#7f8c8d` label "Attribution Chain:" centered at (360, 165); then at y=188 the sequence "Donor ?" → "PAC A" → "PAC B" → "Ad Buy" → "???" with items in red `#e74c3c` 12px and gray `#bdc3c7` 16px arrows between.

## Regeneration instructions

- **Layout:** standard detail-page structure — h1, `.subtitle` paragraph, then per pitfall an `<h2>` section heading followed by a one-row `.obj-table`: left `<td>` (40%) with `.obj-title` div (same text as the h2) + `<ul>` of labeled bullets, right `<td>` (60%, centered) with a `<canvas>` (HTML attributes `width="720" height="300"`). H1 uses an em dash (`&mdash;`) between "Political / Contextual Ads" and "Domain Pitfalls".
- **Page CSS:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 1.05em; `ul` 0.9em `#333`; `strong` `#1a5276`; `.obj-table td` border `1px solid #e0e0e0`, padding 20px 24px, even rows background `#fafcfe`; `.obj-title` 1.05em weight 600 `#1a5276`; `.philosophy` callout class defined (background `#f0f4f8`, left border `4px solid #2980b9`) though unused on this page. No nav bar, no back/home links.
- **Canvas:** shared `setupCanvas(id)` helper scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), default font `17px -apple-system, BlinkMacSystemFont, sans-serif`. Note the drawn size (720×200) overrides the 720×300 HTML attribute.
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`/`#f39c12`, dark slate `#2c3e50`, gray `#7f8c8d`/`#bdc3c7`.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
