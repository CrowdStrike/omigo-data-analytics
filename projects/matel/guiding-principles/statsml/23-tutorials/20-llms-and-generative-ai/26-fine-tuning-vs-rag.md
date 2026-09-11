# Fine-Tuning vs RAG

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Fine-Tuning vs RAG

**Subtitle:** Two ways to make a model know your business: fine-tuning re-trains it on your examples (teaching the employee), while RAG fetches your documents at question time (handing them notes)

## Two Ways to Make a Bot Know Your Bakery

**Tags:** `core idea` (blue), `teach vs hand notes` (green), `two paths` (orange)

- **The bakery** — a small bakery adds a chatbot to answer customers: prices, hours, what's gluten-free
- **The problem** — the base model has never heard of this bakery; it needs the shop's facts and voice
- **Path one: fine-tune** — re-train the model on 500 old chat transcripts so the answers come out changed
- **Path two: RAG** — leave the model alone; when a question arrives, fetch the price sheet and paste it in
- **The difference** — fine-tuning changes the model before any question; RAG changes the input, every question
- **The analogy** — fine-tuning is sending a new hire to bakery school; RAG is taping the menu next to the till

*Example (italic):* Asked "how much is a sourdough loaf?", the fine-tuned bot answers from what it learned in training; the RAG bot reads today's price sheet first, then answers.

**Key point:** Fine-tuning bakes knowledge and style into the model's weights once; RAG hands the model fresh notes at question time and never touches the weights.

### Visualization (canvas `c1`, 720×300)

Two-lane flow diagram: the fine-tuning pipeline on the top lane (work happens before questions) and the RAG pipeline on the bottom lane (work happens per question), each as three rounded boxes joined by arrows.

- **Title (bold 15px, `#1a5276`, top center):** "Teach the Model vs Hand It Notes".
- **Lanes:** two horizontal lanes; top lane centered at y=110, bottom lane at y=215; lane labels bold 13px at x=18 — orange `#d95926` "FINE-TUNE" above the top lane (y=68), green `#008300` "RAG" above the bottom lane (y=173).
- **Top lane boxes (rounded 8px, 2px orange `#d95926` border, fill `rgba(217,89,38,0.08)`, 150×54 each, 12px `#2c3e50` two-line centered text):** box 1 at x=100 "500 old chat / transcripts", box 2 at x=300 "training run — / weights change", box 3 at x=500 "bakery bot / (new model)"; 2px orange arrows with arrowheads between boxes.
- **Bottom lane boxes (rounded 8px, 2px green `#008300` border, fill `rgba(0,131,0,0.08)`, 150×54 each):** box 1 at x=100 "customer / question arrives", box 2 at x=300 "fetch today's / price sheet", box 3 at x=500 "same base model / reads notes + answers"; 2px green arrows between boxes.
- **Timing tags (11px `#6b7280`, right of each lane's last box, right-aligned at the canvas edge x=712):** top "done once, / up front"; bottom "repeats / every / question".
- **Annotation (bold 12px violet `#4a3aa7`, centered at x=360, y=282):** "one changes the model, the other changes the input".

## Ten Questions After the Price Change

**Tags:** `worked example` (blue), `stale vs fresh` (orange)

- **The setup** — the bot was fine-tuned in March on 500 transcripts; in June sourdough goes $6 to $7
- **The test** — ask both bots the same 10 price questions in June, after the menu changed
- **Fine-tuned facts** — it quotes the March prices it memorized: only 3 of 10 answers are still right
- **RAG facts** — it reads the June price sheet before answering: 9 of 10 price answers are right
- **Fine-tuned voice** — its tone matches the shop's cheerful style on 9 of 10 answers; it learned the voice
- **RAG voice** — the plain base model sounds generic: only 5 of 10 answers match the house style

*Example (italic):* "How much is sourdough?" — the fine-tuned bot cheerfully answers the old $6; the RAG bot flatly answers the correct $7.

**Key point:** Same 10 questions, opposite failures — fine-tuned scored 9/10 on voice but 3/10 on current prices; RAG scored 9/10 on prices but 5/10 on voice.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: two question sets on the x axis ("right current price" and "matches house voice"), each with a fine-tuned bar and a RAG bar, out of 10 test questions.

- **Title (bold 15px, `#1a5276`, top center):** "10 June Questions: Fine-Tuned vs RAG (correct out of 10)".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 185; y axis 0 to 10 with 12px `#444` tick labels at 0, 2, 4, 6, 8, 10 and light `#e5e9ef` gridlines; x axis two group labels, bold 13px `#2c3e50`, centered under the groups: "right current price" (x≈230) and "matches house voice" (x≈510).
- **Bars (width 90, gap 20 within a group):** group 1 — fine-tuned orange `#d95926` bar height 3, RAG green `#008300` bar height 9; group 2 — fine-tuned orange bar height 9, RAG green bar height 5; bar values `[3, 9]` and `[9, 5]` drawn as bold 13px labels in the bar color just above each bar top.
- **Legend (12px, top right at x≈540, y=52):** orange swatch "fine-tuned (March)", green swatch "RAG (reads June sheet)".
- **Annotation (bold 12px `#1a5276`, near x=230, y=95):** two lines: "fine-tuned quotes / March prices in June".
- **Caption (12px `#444`, bottom right):** "illustrative scores from a made-up bakery test".

## Fresh Facts vs Learned Habits

**Tags:** `where it's used` (blue), `rule of thumb` (green)

- **Changing facts** — prices, stock, policies, today's specials drift weekly; RAG follows the document
- **Stable habits** — tone, format, refusal rules, jargon barely change; fine-tuning learns them once
- **The rule** — if the answer lives in a document that changes, use RAG; if it's a habit, fine-tune
- **The decay** — in week 0 both bots score 9/10 on facts; by week 8 the fine-tuned bot is down to 3/10
- **RAG holds** — the RAG bot stays at 9/10 all eight weeks because someone keeps the sheet current
- **Both together** — many teams fine-tune for voice and use RAG for facts; the two fixes stack

*Example (italic):* The bakery updates its price sheet every Monday in five minutes; the RAG bot is instantly current, while the March fine-tune keeps aging.

**Key point:** Facts drift, habits don't — RAG tracks whatever the document says today, while a fine-tune is a snapshot that goes stale at the speed your facts change.

### Visualization (canvas `c3`, 720×300)

Two-line time series: fact accuracy (out of 10) over weeks 0 to 8 since the fine-tune, orange line decaying for the fine-tuned bot, green line flat for RAG.

- **Title (bold 15px, `#1a5276`, top center):** "Fact Accuracy After the Fine-Tune: Snapshot vs Living Notes".
- **Axes:** origin x=70, baseline y=245, plot width 570, plot height 180; x = weeks since fine-tune, tick labels "0"–"8" every week (12px `#444`), axis title 12px `#444` "weeks since fine-tune" centered below; y = correct out of 10, ticks 0, 2, 4, 6, 8, 10 with light `#e5e9ef` gridlines.
- **Fine-tuned line:** orange `#d95926` 3px line with 5px dots through weeks `[0, 1, 2, 3, 4, 5, 6, 7, 8]`, scores `[9, 9, 8, 7, 6, 5, 4, 3, 3]`; 12px orange label "fine-tuned" near week 6 below the line.
- **RAG line:** green `#008300` 3px line with 5px dots, scores `[9, 9, 9, 9, 9, 9, 9, 9, 9]`; 12px green label "RAG" near week 6 above the line.
- **Event marker:** vertical dashed `#6b7280` (dash 4/3) line at week 2 from baseline to y=70, 11px `#6b7280` label at its top: "menu changes start".
- **Annotation (bold 12px orange `#d95926`, near week 7, y=150):** two lines: "week 8: 3/10 —" / "the snapshot went stale".
- **Caption (12px `#444`, bottom right):** "illustrative — weekly menu edits, sheet kept current".

## Fine-Tuning Is Not a Memory Upload

**Tags:** `common mistake` (red), `what each changes` (orange)

- **The mistake** — "we'll fine-tune the model on our docs so it knows everything" — it won't, not reliably
- **What tuning learns** — patterns of behavior: tone, format, how to answer; single facts stick poorly
- **What RAG can't do** — pasted notes don't change habits; the bot still writes in its generic voice
- **Fixing one fact** — RAG: edit one line on the sheet, about 2 minutes; fine-tune: rebuild, about 360
- **No receipts** — a fine-tuned answer can't point to a source; a RAG answer can cite the exact line

*Example (italic):* To change one price, the bakery edits one line and RAG is correct in 2 minutes; the fine-tune route means new examples and a retrain, roughly 360 minutes.

**Common mistake:** Treating fine-tuning as a way to load facts into the model. It teaches habits, not a lookup table — one changed price should cost you an edited line, not a retrain.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart with two bars: minutes to correct one changed price via RAG (edit the sheet) vs via fine-tuning (collect examples and retrain), on a shared minutes axis.

- **Title (bold 15px, `#1a5276`, top center):** "Cost of Fixing One Wrong Price (minutes, illustrative)".
- **Axis:** horizontal 2px `#999` line at y=245 from x=230 to x=680 (width 450), minutes 0 to 400; tick labels "0", "100", "200", "300", "400" (12px `#444`) below, with light `#e5e9ef` vertical gridlines.
- **Row 1 (bar centered y=110), label 12px `#444` right-aligned at x=220:** "RAG — edit one line"; green `#008300` bar from 0 to 2 minutes (drawn with a 4px minimum width so it stays visible), bold 13px green value label "2 min" just right of the bar end.
- **Row 2 (bar centered y=185), label:** "fine-tune — examples + retrain"; orange `#d95926` bar from 0 to 360 minutes, bold 13px orange value label "360 min" just right of the bar end.
- **Bar style:** 34px-tall bars, fills `rgba(0,131,0,0.35)` and `rgba(217,89,38,0.35)` with 2px solid borders in the line colors.
- **Annotation (bold 13px magenta `#d55181`, centered near x=430, y=70):** "180x slower to fix one fact by retraining".
- **Caption (12px `#444`, bottom right):** "illustrative timings for a made-up bakery bot".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar heights, line points, and timings are the hardcoded literal arrays above (no randomness); every invented number carries an "illustrative" caption, and the worked-example numbers in the text (3/10, 9/10, 9/10, 5/10; weeks 0–8 decay; 2 vs 360 minutes) match the chart arrays exactly.
- **Color convention across all four charts:** orange `#d95926` = fine-tuning, green `#008300` = RAG; keep this pairing consistent.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
