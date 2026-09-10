# Social Graph & Connections as Signals

**Page type:** detail page (compact card-sections: one h2 per section, two-column layout table with tag pills + labeled bullets left ~45%, canvas right ~55%)
**HTML title tag:** Social Graph & Connections

**Subtitle:** Who you know predicts what you'll do next — friend clusters, tie strength, and propagation along edges

## Homophily: Your Friends' Attributes Predict Yours

Tags: signal (blue), privacy risk (red), bias (orange)

- **Friend median** — predicts your age, politics, income, churn undisclosed
- **Ad targeting** — Facebook infers politics, life stage from graph alone
- **Credit scoring** — 2015 Facebook patent; Lenddo, Tala use friends' scores
- **Churn contagion** — churned contacts mean 2-7x churn; beats usage features
- **Small world** — Facebook measured ~3.5 average degrees of separation

*Example:* Blank profile, but 80% of friends share one political lean — targeted as if declared.

**Failure mode:** Inferring from neighbors, then recommending within inferred segments, hardens segregation into a self-fulfilling prophecy.

### Visualization (canvas `c1`, 720×300)

Two-panel figure split by a vertical dashed gray (`#bdc3c7`, dash 4/3) divider at x=330; title (bold 14px `#1a5276`, centered): "Blank Profile, Full Prediction: Neighbors Vote".

- **Left panel — ego network:** center at (165,150), radius 76. Eight neighbor nodes (radius 12) evenly spaced starting at top, connected by 1.2px `#bbb` spokes; attribute flags `[1,1,1,0,1,1,0,1]` — flag 1 nodes filled `#1a5276` (blue, shared attribute), flag 0 nodes `#e67e22` (orange, other). Ego node: white 16px-radius circle with dashed gray (`#7f8c8d`, dash 3/3) outline and a bold gray "?" in the center.
- **Left captions:** `#1a5276` 11px "6 of 8 neighbors share the attribute (blue)" at y=262; bold red `#e74c3c` 11px "ego inferred — no disclosure needed" at y=278.
- **Right panel — bar chart:** churn rate vs churned contacts. X labels `['0','1','2','3','4','5+']`, rates `[4, 9, 15, 22, 30, 41]` (%). Plot from x=360 to right edge minus 25, baseline y=245, chart height 165, y scale max 45%. Bars 42px wide; last two bars (indices 4-5) red `#e74c3c`, others `rgba(26,82,118,0.35)`; value labels "N%" in `#444` above bars, x labels in `#222` below.
- **Right titles/annotations:** bold 12px `#1a5276` "Churn rate vs churned contacts (telecom call graph)" at y=52; bold red 11px "~10x hazard, zero profile features used" at y=78; axis label `#444` "churned contacts" below baseline.

## Explicit Graph vs Interaction Graph: Edge Strength

Tags: signal (blue), rule of thumb (blue)

- **80/20 ties** — ~8 of ~300 declared friends carry ~80% of interactions
- **Dunbar layers** — ~5 close, ~15 good, ~50 friends, ~150 acquaintances
- **EdgeRank affinity** — passive profile views as predictive as active signals
- **Cost ordering** — DMs > comments > tags > likes > views
- **Recency decay** — undecayed edges let a 2010 roommate dominate forever

*Example:* One weekly photo comment outranked a hundred stale friendships in EdgeRank.

**Failure mode:** An unweighted declared graph is a museum of past relationships — the interaction edge, recency-decayed, is the fact about now.

### Visualization (canvas `c2`, 720×300)

Rank-ordered bar chart. Title (bold 14px `#1a5276`, centered): "Monthly Interactions per Declared Friend (rank order)".

- **Axes:** L-shaped gray (`#999`) axes; padding top 60, bottom 62, left 55, right 25.
- **Data:** 40 bars, value for rank i = `110 / (i+1)^1.1` (power-law decay), y scale max 115. First 8 bars green `#27ae60`, remaining bars `rgba(26,82,118,0.35)`.
- **Bracket:** green 2px bracket spanning the top 8 bars just above the plot, with bold green 12px label to its right: "top ~8 ties carry ~80% of interactions".
- **Annotation:** bold red 12px inside the plot at ~28% width near the baseline: "remaining ~290 declared \"friends\": dormant edges".
- **Captions:** `#444` 12px centered "friends ranked by interaction count (Dunbar layers: 5 / 15 / 50 / 150)" at h-38; bold orange `#e67e22` 12px centered "edge strength: DMs > comments > tags > likes > views — and decays with recency" at h-14.

## Triadic Closure: PYMK Mechanics

Tags: mechanism (blue), privacy risk (red)

- **Mutual count** — P(new tie) rises steeply; powers Facebook/LinkedIn PYMK growth
- **Adamic-Adar** — a mutual with 50 ties beats one with 5,000
- **Context weight** — same employer, school, city close triangles faster
- **Contact uploads** — seed candidate edges you never created; co-location fused in
- **Awkward failures** — therapist's patients matched, donor children surfaced, secret accounts exposed

*Example:* A psychiatrist's patients, sharing only her phone number, surfaced in each other's PYMK.

**Failure mode:** Context collapse — the awkward suggestion is closure working as designed on edges that should never have been comparable.

### Visualization (canvas `c3`, 720×300)

Two-panel figure, vertical dashed divider at x=300. Title (bold 14px `#1a5276`): "Triadic Closure: Mutual Friends Predict the Next Edge".

- **Left panel — open-triangle sketch:** nodes A (60,155) and B (245,155) in blue `#1a5276`, three mutual nodes M1-M3 at x=152, y=78/138/198 in green `#27ae60` (13px radius, white bold labels). Solid blue 1.5px edges from A and B to each mutual; a dashed orange (`#e67e22`, 2.5px, dash 6/4) quadratic curve from A to B (the predicted edge, bowing down through (152,248)).
- **Left captions:** bold orange 11px "PYMK: \"A, do you know B?\"" at y=268; `#444` 11px "weight mutuals by 1 / log(degree) — Adamic-Adar" at y=285.
- **Right panel — line chart:** P(tie) vs mutual-friend count. X values `[0,1,2,3,4,5,6,7,8]`, probabilities `[0.02, 0.9, 2.5, 4.8, 7.5, 10.5, 13.5, 16.5, 19]` (%), y max 20. Plot from x=330, baseline y=238, height 168; L-shaped gray axes. Blue `#1a5276` 3px line with 4px dots; x labels in `#222`.
- **Right annotations:** bold blue 12px "P(friendship forms), %" at y=46; bold orange 12px "~1000x lift from 0 to 8 mutuals" at y=66; `#444` 11px axis label "mutual friends" below baseline.

## Weak Ties Carry the Novel Information

Tags: signal (blue), best practice (green), trade-off (orange)

- **Granovetter** — weak ties bridge to communities strong ties can't see
- **LinkedIn 20M** — Science 2022: moderately weak ties drive job moves (inverted-U)
- **Task split** — strong ties for influence; weak ties for information
- **Embeddedness** — mutual-friend count cheaply classifies tie type
- **Bridge signature** — high betweenness, low triangle count, high novelty

*Example:* Five years of randomized PYMK for 20M users: weak-tie nudges caused more job changes.

**Failure mode:** Ranking by strong-tie engagement quietly prunes the bridging weak ties — the echo chamber is built into the objective.

### Visualization (canvas `c4`, 720×300)

Two-panel figure, vertical dashed divider at x=305. Title (bold 14px `#1a5276`): "Weak Ties Bridge Clusters — and Move Careers".

- **Left panel — bridge sketch:** two fully-connected 6-node clusters (7px-radius nodes on a 40×34 ellipse; intra-cluster edges at 45% alpha): one centered (95,120) blue `#1a5276`, one centered (225,190) green `#27ae60`. A single thick orange (`#e67e22`, 3px) bridge edge connects one node of each cluster.
- **Left labels:** bold orange 11px "weak tie = bridge" at (170,138); `#444` 11px "strong ties: redundant info inside clusters" at y=262; bold orange 11px "the bridge is the only path for novel info" at y=278.
- **Right panel — inverted-U bar chart:** job transitions per new tie by tie-strength sextile. Values `[42, 68, 100, 78, 52, 30]`, y max 110; 6 bars 44px wide from x=330, baseline y=235, height 160; bar index 2 (peak) green `#27ae60`, others `rgba(26,82,118,0.35)`; x labels 1-6 in `#222`.
- **Right titles:** bold blue 12px "Job transitions per new tie (LinkedIn, 20M users)" at y=52; bold green 12px "moderately weak ties help most — inverted U" at y=74; `#444` 11px axis label "tie strength sextile (1 = weakest, 6 = strongest)" below baseline.

## Influence vs Homophily: The Hard Question

Tags: bias (orange), best practice (green)

- **Aral et al.** — homophily explained over half of apparent adoption contagion
- **Shuffle tests** — permute edges or timestamps; the gap bounds influence
- **Facebook 61M** — 2012 randomized "I voted" moved turnout via close friends
- **Christakis-Fowler** — "contagious obesity" sparked a decade of methods dispute
- **Viral ROI** — no homophily baseline overstates influence roughly 2x

*Example:* Edge-shuffled baselines showed half the referral feature's "lift" was homophily.

**Key point:** Observational influence estimates are upper bounds — randomize exposure when possible, shuffle-test otherwise.

### Visualization (canvas `c5`, 720×300)

Two-curve line chart with shaded gap. Title (bold 14px `#1a5276`): "Adoption After a Friend Adopts: Observed vs Edge-Shuffled Baseline". Padding top 50, bottom 48, left 60, right 170; L-shaped gray axes.

- **Data (12 weekly points, y max 90):** observed `[1, 2, 4, 8, 15, 27, 42, 57, 68, 76, 81, 84]`; edge-shuffled `[1, 1.5, 2.5, 4, 7, 12, 19, 27, 35, 42, 47, 51]`.
- **Series:** observed — solid red `#e74c3c` 3px; shuffled — dashed blue `#1a5276` 3px (dash 6/4). Region between the curves filled `rgba(230,126,34,0.18)`.
- **Annotations:** bold orange 13px "shaded gap = influence (upper bound)" at ~48% width, ~36% height; bold blue 12px "the rest is homophily" at ~62% width, ~78% height.
- **Legend (right side, x = w-158):** red swatch "observed graph"; blue swatch "edge-shuffled".
- **X-axis label:** `#444` centered "weeks since friend adopted" at h-16.

## Simple vs Complex Contagion

Tags: mechanism (blue), rule of thumb (blue)

- **Two regimes** — information needs one exposure; costly behavior needs several
- **Centola** — behavior spread farther on clustered networks with reinforcement
- **Threshold 2-3** — adoption jumps at 2-3 exposed friends, then saturates
- **Seeding** — clusters for behavior change; scatter for awareness
- **Distinct neighbors** — ten reminders from one friend equals one exposure

*Example:* Centola's clustered health community out-spread its identically-sized random-wired twin.

**Failure mode:** Counting total exposures instead of distinct exposed neighbors systematically overpredicts adoption.

### Visualization (canvas `c6`, 720×300)

Two-curve line chart. Title (bold 14px `#1a5276`): "P(Adopt) vs Distinct Exposed Friends: Information vs Behavior". Padding top 50, bottom 52, left 60, right 180; L-shaped gray axes.

- **Data (x = 0..6 distinct exposed friends, y max 100):** information (simple contagion) `[0, 55, 72, 80, 84, 86, 87]`; behavior (complex contagion) `[0, 8, 18, 55, 72, 78, 80]`.
- **Series:** information — solid green `#27ae60` 3px; behavior — solid orange `#e67e22` 3px.
- **Threshold band:** vertical band between x=2 and x=3 filled `rgba(231,76,60,0.10)`.
- **Annotations:** bold red 12px "behavior threshold: 2-3 distinct friends" near x=2.5 at ~88% height; bold green 12px "news: 1 exposure is enough" near x=2.2 near the top.
- **Legend (x = w-168):** green swatch "information (simple)"; orange swatch "behavior (complex)".
- **X-axis label:** `#444` centered "distinct exposed friends — not total reminders" at h-16.

## Structural Virality: Broadcast vs Truly Viral

Tags: signal (blue), abuse (red)

- **Goel et al.** — structural virality = mean pairwise tree distance; most hits are broadcasts
- **Vosoughi 2018** — Science: false news ~6x faster, deeper — humans, not bots
- **Containment** — broadcast dies with one node; viral has no single point
- **Early shape** — depth and branching predict final cascade size in real time
- **Content split** — outrage travels peer-to-peer; institutional news hub-to-leaves

*Example:* A celebrity star dies with one deletion; an eleven-generation tree does not.

**How it's exploited:** Optimizing shares without measuring tree shape rewards the exact propagation shape of misinformation.

### Visualization (canvas `c7`, 720×300)

Two-panel network sketch, vertical dashed divider at x=345. Title (bold 14px `#1a5276`): "Same Reach, Different Shape: Broadcast Star vs Viral Tree".

- **Left panel — broadcast star:** hub node (13px radius, `#1a5276`) at (172,150) with 14 leaf nodes (6px, `rgba(26,82,118,0.35)`) on an 85×70 ellipse, connected by 1px `#aaa` edges.
- **Left captions:** bold blue 12px "Broadcast: depth 1 — remove the hub, cascade dies" at y=268; green 11px "one moderation point" at y=284.
- **Right panel — viral tree:** 15-node 5-level tree rooted at (530,60); root 10px red `#e74c3c`, descendants 6px `rgba(231,76,60,0.5)`, edges 1px `#aaa`. Node positions span x≈412-634, levels at y = 60, 102, 145, 188, 230.
- **Right captions:** bold red 12px "Viral: person-to-person, depth 5" at y=262; bold orange 11px "no single intervention point — false news ran ~6x faster" at y=282.

## Graph Features in Ranking Models

Tags: signal (blue), best practice (green), bias (orange)

- **PageRank lineage** — authority scores now rank reputation, fraud risk, seller trust
- **Log degree** — power-law distribution; log-transform or hubs dominate splits
- **Communities** — Louvain/Leiden memberships feed interest and abuse models
- **Embeddings** — node2vec ~128 dims; Pinterest PinSage runs 3B nodes
- **Simple first** — degree and mutual counts beat GNN baselines surprisingly often

*Example:* PinSage ablations: plain mutual-neighbor counts recover much of the lift.

**Failure mode:** Centrality features create rich-get-richer loops — without exposure correction, yesterday's popularity becomes tomorrow's permanently.

### Visualization (canvas `c8`, 720×300)

Two-panel figure, vertical dashed divider at x=440. Title (bold 14px `#1a5276`): "Power-Law Degree + the Popularity Feedback Loop".

- **Left panel — log-log scatter:** 45 blue `#1a5276` 3px dots following the line log-count = 6 − 2·log-degree with deterministic sinusoidal jitter (`sin(i*7.3)*0.18`), clamped at 0; plot area 350×180 from (60,55); L-shaped gray axes.
- **Left annotations:** bold green 11px "long tail: most users" top-left of plot; bold orange 11px "hubs: rare, dominate raw features" near lower right; `#444` 11px x label "log degree — log-transform before modeling" at h-20; rotated y label "log user count".
- **Right panel — feedback-loop diagram:** three orange-bordered (`#e67e22`, 2px) boxes 175×34 at x=480, y=60/130/200, fill `#fdf2e9`, bold blue 11px labels: "high centrality score" → "ranked & recommended" → "gains more edges". Orange down-arrows between boxes and an orange return arrow along the right side closing the loop back to the top box.
- **Right caption:** bold red 11px, two lines centered under the boxes: "rich-get-richer:" / "the ranked graph feeds the ranker".

## Gaming the Graph: Every Signal Gets Farmed

Tags: gaming (orange), abuse (red), defense (green)

- **Goodhart on graphs** — link farms broke PageRank; follower markets, follow-back rings followed
- **Ring signature** — reciprocity ~1.0, burst creation, few organic edges
- **Engagement pods** — coordinated likes inflate edge strength, hijack feed ranking
- **Defenses** — TrustRank seed propagation, temporal analysis, density penalties
- **Fraud rings** — shared devices, cards, addresses form edges exposing rings

*Example:* Sixty pod accounts form a clique denser than any real friend group.

**How it's exploited:** Attackers optimize node metrics, so defenders read subgraph shape — organic graphs are sparse and asymmetric; fraud is dense and reciprocal.

### Visualization (canvas `c9`, 720×300)

Scatter plot. Title (bold 14px `#1a5276`): "Followers vs Following (log-log): Bot Rings Pin to the Diagonal". Padding top 50, bottom 50, left 60, right 175; L-shaped gray axes; dashed light-gray `#ccc` reference diagonal (ratio = 1) from bottom-left to top-right.

- **Organic accounts:** 60 dots (3px) in `rgba(26,82,118,0.55)` widely scattered across 8-92% of both axes via deterministic pseudo-random hashing.
- **Bot ring:** 30 red `#e74c3c` dots clumped tightly on the diagonal (t between 0.55 and 0.67 with ±7px jitter).
- **Annotations:** bold red 12px "reciprocity ~1.0, dense, created in bursts" at ~52% width, ~22% height; bold blue 11px "organic: sparse and asymmetric" at ~34% width, ~92% height.
- **Legend (x = w-162):** `rgba(26,82,118,0.7)` swatch "organic accounts"; red swatch "follow-back ring".
- **Axis labels:** `#444` "log following" centered at h-16; rotated "log followers" on the left.

## Shadow Profiles: The Graph Describes Non-Members

Tags: privacy risk (red), signal (blue)

- **Facebook 2018** — confirmed non-user contact collection in congressional testimony
- **Garcia 2017** — non-member ties, attributes predictable from members' contact lists
- **Narayanan-Shmatikov** — matched anonymized Twitter to Flickr, ~12% error, edges only
- **No anonymity** — your neighborhood shape is a fingerprint
- **No opt-out** — consent lives in nodes; information lives in edges

*Example:* Thirty uploaded address books assemble a profile for someone who never joined.

**Key point:** The graph's predictive power and its privacy risk are the same property — edges leak information in both directions.

### Visualization (canvas `c10`, 720×300)

Two-panel figure, vertical dashed divider at x=280. Title (bold 14px `#1a5276`): "Shadow Profile: Inference Accuracy vs Member Contacts Who Uploaded".

- **Left panel — ghost node sketch:** center (145,145); six member nodes (11px, blue `#1a5276`) on an 82×68 ellipse connected by 1.5px blue edges to a central white 17px node with dashed gray `#7f8c8d` outline containing two-line gray label "no" / "account".
- **Left captions:** blue 11px "members (blue) upload contact books" at y=248; bold red 11px two lines "each upload adds edges + attributes" (y=264) / "to a person who never joined" (y=279).
- **Right panel — accuracy curve:** x = member contacts who uploaded `[0, 1, 2, 3, 5, 8, 12, 20, 30]` (evenly spaced points), y = inference accuracy `[0, 22, 38, 50, 63, 74, 82, 89, 93]` (%), y max 100. Plot from x=320, baseline y=235, height 165; L-shaped gray axes; red `#e74c3c` 3px line with 4px red dots; x labels in `#222`.
- **Right annotations:** bold red 12px "~90% of location / workplace inferable" near the top of the curve; `#444` 11px axis label "member contacts who uploaded their address book" below baseline; bold blue 11px "inference accuracy, %" at y=46.

## Regeneration instructions

- **Layout:** most-powerful-signals compact style. One `.card-section` per topic: `<h2>` (unnumbered, 1.3rem `#1a5276`, 2px solid `#2980b9` bottom border), then a `table.layout` with one row — left `td.text-col` (45%) holding `.tags` pill row, a `<ul>` of labeled bullets (`<li><b>Label</b> — text`), an italic `.example` paragraph, and a `.key-point` callout; right `td.viz-col` (55%) holding one `<canvas width="720" height="300">` scaled to `width: 100%` with 1px `#e0e0e0` border, 4px radius.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors: blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `<strong>` lead-in label.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `ul` 0.92rem with `li b` in `#1a5276`; `.example` italic `#555` 0.9rem. No nav bar, no back/home links.
- **Canvas:** shared `setup(id)` helper — backing store sized to rendered width × `window.devicePixelRatio` (display capped via `style.maxWidth`), `ctx.scale` back to logical coordinates, logical size 720×300.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, gray text `#444`/`#555`.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
