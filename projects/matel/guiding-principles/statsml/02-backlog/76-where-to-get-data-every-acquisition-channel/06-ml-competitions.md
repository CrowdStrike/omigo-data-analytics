# ML Competitions — Cleaned Data With a Scoreboard Attached

**Page type:** detail page (four titled sections, each a two-column row: text left 45%, canvas right 55%)
**HTML title tag:** ML Competitions — Cleaned Data With a Scoreboard Attached

**Subtitle:** A competition hands you a dataset, a metric, and a leaderboard in one package — the fastest structured way to practice modeling, and a deliberately artificial one, because the organizer has already made every hard decision that real projects begin with.

**Intro callout (blue-left-border box):** For a newcomer, competitions solve the coldest part of the cold-start problem: you get real data, a precise definition of success, and instant feedback against thousands of other attempts. The catch is baked into the format — everything that makes a competition tractable (clean data, one fixed metric, a frozen snapshot) is exactly what production work never gives you.

## 1. How a competition packages data

Every competition ships the same four-part package, and each part is a decision the organizer made so you do not have to.

- **Training set with labels:** target defined, sources joined, labels attached
- **Hidden cost:** that step eats most of a real project's calendar
- **Test set, labels withheld:** you submit, the platform scores
- **Honest evaluation:** scored against answers you never see
- **One fixed metric:** RMSE, AUC, or log loss, chosen in advance
- **Same target for everyone:** all competitors optimize one number
- **External-data rules:** the contest defines what outside data is allowed
- **Boundary pre-drawn:** real projects negotiate that line themselves

Key point: A competition is a modeling problem with the acquisition, labeling, and metric design already done. That is precisely why it is a good classroom and a poor simulation of the job.

### Visualization (canvas `c1`, 720×380)

Package diagram: a dashed "provided by the organizer" container holding four boxes, feeding down into your-model and leaderboard boxes.

- **Title (bold 14px `#1a5276`, centered, y=22):** "The four-part package — the organizer already made the hard decisions"
- **Outer container:** dashed (6/5) 1.5px `#999` rectangle at (30, 44), 660×110; 10px `#999` left-aligned label inside at (42, 60): "PROVIDED BY THE ORGANIZER".
- **Provided boxes (each 150×64 at y=72, white fill, 2px border in box color; bold 12px label in box color, 11px `#666` subline, both centered):**
  - x=48 "Training set" `#1a5276` — "features + labels"
  - x=210 "Test set" `#8e44ad` — "labels withheld"
  - x=372 "Fixed metric" `#e67e22` — "one number, e.g. AUC"
  - x=534 "Rules" `#e74c3c` — "external data policy"
- **Your-model box:** 200×48 at (260, 196), fill `rgba(26,82,118,0.12)`, 2px `#1a5276` border. Bold 12px `#1a5276` centered: "YOUR MODEL"; 11px `#666`: "the only part left to you".
- **Leaderboard box:** 240×48 at (240, 286), white fill, 2px `#27ae60` border. Bold 12px `#27ae60` centered: "LEADERBOARD"; 11px `#666`: "scored on the hidden test".
- **Connectors:** 1px `#bbb` lines from the Training-set box bottom (123, 136) and the Rules box bottom (609, 136) to the your-model box top corners (300, 196) and (420, 196); 1px `#bbb` lines from the Test-set box bottom (285, 136) and Fixed-metric box bottom (447, 136) to the leaderboard box top (330, 286) and (390, 286); 2px `#999` vertical arrow from the your-model box bottom (360, 244) to the leaderboard box top (360, 286) with a small filled down-arrowhead.
- **Caption (12px `#999`, centered, y = h−14):** "You are handed everything except the model — real projects start three steps earlier"

## 2. The platforms and famous contests

Competitions have a history older than most ML libraries, and each era's landmark contest taught the field something beyond the winning score.

- **KDD Cup:** the academic original, running since 1997 at the KDD conference
- **Template it set:** a shared task scored with a shared metric
- **Netflix Prize:** $1M for a 10% RMSE gain, 2006 to 2009
- **Lasting influence:** winning ensemble techniques shaped the field for years
- **The privacy lesson:** "anonymized" ratings re-identified via public reviews
- **Sequel cancelled:** a landmark in data-release caution
- **Kaggle:** the largest general platform
- **Specialist platforms:** DrivenData (social good), AIcrowd (research)
- **Regional platforms:** Zindi (African problems), Signate (Japan)

Key point: The Netflix Prize is remembered twice: once for advancing recommender systems, and once for proving that releasing "anonymized" user data is a decision you cannot take back.

### Visualization (canvas `c2`, 720×360)

Timeline from 1997 to today with five milestone dots, labels alternating above and below the axis.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Three decades of shared tasks"
- **Timeline:** 2px `#999` line at y=180 from x=50 to x=680 with a filled right-arrowhead.
- **Steps (filled dot radius 7 in step color on the line; labels alternate above/below with thin `#ccc` connectors; label bold 12px in step color, subline 11px `#666`, year tag 10px `#999` on the opposite side of the line):**
  - x=80, "1997", "KDD Cup begins" — "the academic original" — `#1a5276` (above)
  - x=245, "2006", "Netflix Prize opens" — "$1M for a 10% RMSE gain" — `#e67e22` (below)
  - x=380, "2009", "Prize won, sequel cancelled" — "ratings re-identified" — `#e74c3c` (above)
  - x=495, "2010", "Kaggle founded" — "competitions go mainstream" — `#27ae60` (below)
  - x=625, "2010s+", "Specialist platforms" — "DrivenData, AIcrowd, Zindi, Signate" — `#8e44ad` (above)
- **Caption (12px `#999`, centered, y = h−14):** "Each landmark taught two lessons — one about modeling, one about the data itself"

## 3. What competing actually teaches

The skills a leaderboard rewards are not toy skills; several of them transfer directly to production work.

- **Feature engineering vs a metric:** squeeze signal from the given columns
- **Scored feedback loop:** the same craft as improving a production model
- **Validation-split discipline:** careless splits make local scores lie
- **CV habits:** rigorous enough to predict the hidden test
- **Leakage hunting:** spot features that secretly encode the answer
- **Production payoff:** the skill that prevents embarrassing launches
- **Reading winners' write-ups:** compact case studies from top teams
- **What they reveal:** what actually moved the metric and what did not

Key point: Treat a competition as a flight simulator: the instrument discipline it builds is real, even though the weather is scripted.

### Visualization (canvas `c3`, 720×340)

Skill-transfer diagram: three competition-habit boxes on the left, three production-skill boxes on the right, green arrows across.

- **Title (bold 14px `#1a5276`, centered, y=22):** "What transfers from the leaderboard to the job"
- **Column headers (bold 11px `#999`, centered, y=58):** "COMPETITION HABIT" at x=170; "PRODUCTION SKILL" at x=550.
- **Left boxes (each 280×58 at x=30, white fill, 2px `#1a5276` border; bold 12px `#1a5276` label, 11px `#666` subline, both centered at x=170):**
  - y=70 "Feature engineering vs a metric" — "iterate against scored feedback"
  - y=150 "Validation-split discipline" — "CV built to predict the hidden test"
  - y=230 "Leakage hunting" — "find features that encode the answer"
- **Right boxes (each 280×58 at x=410, white fill, 2px `#27ae60` border; bold 12px `#27ae60` label, 11px `#666` subline, both centered at x=550):**
  - y=70 "Metric-driven improvement" — "move the number that matters"
  - y=150 "Trustworthy offline evaluation" — "offline scores that predict live traffic"
  - y=230 "Pre-launch leakage audits" — "catch train/serve leaks before shipping"
- **Arrows:** 2px `#27ae60` horizontal arrows from x=310 to x=404 at each row's mid-height (y=99, 179, 259), each with a small filled right-arrowhead.
- **Caption (12px `#999`, centered, y = h−14):** "The discipline transfers even though the environment is scripted"

## 4. Where competitions mislead

Everything that makes a competition tractable is a departure from production, and the differences are systematic, not accidental.

- **Pre-cleaned data:** encodings fixed, duplicates removed, leaks audited
- **Production contrast:** raw data arrives with all those problems intact
- **One metric, frozen:** a single number, chosen once
- **Real systems differ:** several objectives that shift with the business
- **Static snapshot:** competition data never drifts
- **No afterlife:** no deployment, monitoring, or retraining loop
- **Public-leaderboard overfitting:** repeated submissions tune to one split
- **Private-split shake-up:** final standings routinely reshuffle
- **Built-in lesson:** a small dose of generalization reality in the game

Key point: Production work starts where the competition ends: defining the problem, acquiring and cleaning the data, choosing the metric, and living with the model after launch.

### Visualization (canvas `c4`, 720×380)

Side-by-side comparison: competition-world column on the left, production-world column on the right, separated by a dashed red divider.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Competition world vs production world"
- **Left header:** 300×36 at (40, 52), fill `rgba(26,82,118,0.12)`, 2px `#1a5276` border. Bold 12px `#1a5276` centered: "COMPETITION WORLD".
- **Left item boxes (each 300×44 at x=40, white fill, 1.5px `#1a5276` border; bold 12px `#1a5276` label, 11px `#666` subline, both centered at x=190):**
  - y=104 "Pre-cleaned data" — "leaks already audited out"
  - y=162 "One fixed metric" — "a single number, chosen for you"
  - y=220 "Static snapshot" — "no drift, ever"
  - y=278 "Ends at final submission" — "no deployment, no monitoring"
- **Right header:** 300×36 at (380, 52), fill `rgba(230,126,34,0.10)`, 2px `#e67e22` border. Bold 12px `#e67e22` centered: "PRODUCTION WORLD".
- **Right item boxes (each 300×44 at x=380, white fill, 1.5px `#e67e22` border; bold 12px `#e67e22` label, 11px `#666` subline, both centered at x=530):**
  - y=104 "Raw data" — "leaks included, cleaning is your job"
  - y=162 "Many shifting objectives" — "metrics negotiated with the business"
  - y=220 "Live data drifts" — "yesterday's model decays quietly"
  - y=278 "Begins at deployment" — "monitoring and retraining forever"
- **Divider:** dashed (6/5) 2px `#e74c3c` vertical line at x=360 from y=52 to y=322.
- **Caption (12px `#999`, centered, y = h−14):** "The leaderboard ends exactly where the job begins"

## Regeneration instructions

- **Template:** backlog multi-section detail page. After h1, `.subtitle`, and `.intro` callout, one `.lang-section` per numbered section: `h2` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px padding-bottom), then a `table.layout` with one `<tr>`: left `<td class="text-col">` (45%) with lead paragraph, bullets, `.key-point`; right `<td class="viz-col">` (55%) with the canvas. Section h2 headings carry the "1.–4." numbers shown above.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`; `.subtitle` `#666` 0.95rem; `.intro` — background `#f0f4f8`, `border-left: 3px solid #2980b9`, padding 8px 12px, 0.9rem, 32px margin-bottom; `.key-point` — background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem; `ul` 0.92rem; canvases `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px; `.lang-section` 40px margin-bottom.
- **Canvases:** intrinsic width 720, heights 380/360/340/380 as specified; shared `setupCanvas(id, h)` helper scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Canvas fonts use `-apple-system, sans-serif`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`; grays `#555`/`#666`/`#888`/`#999`/`#bbb`/`#ccc`; translucent fills `rgba(26,82,118,0.12)`, `rgba(39,174,96,0.10)`, `rgba(230,126,34,0.10)`.
- **Bullet style:** each bullet is a one-line "**Label:** short phrase" that must not text-wrap; labels render bold in `#1a5276` via `li strong { color: #1a5276; }`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
