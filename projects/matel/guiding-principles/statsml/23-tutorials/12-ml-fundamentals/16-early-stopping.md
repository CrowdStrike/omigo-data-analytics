# Early Stopping

**Page type:** detail page (tutorial card-sections: one h2 + two-column table per section, text left 50%, canvas right 50%)
**HTML title tag:** Early Stopping

**Subtitle:** Quit training the moment the model stops improving on fresh data, before it starts memorizing

## A Spam Filter That Studies Too Long

**Tags:** `core idea` (blue), `when to stop` (green)

- **The setup** — a spam filter trains in passes over the same 10,000 emails; each pass is an epoch
- **Two scoreboards** — error on the training emails, and error on 1,000 fresh held-out emails
- **Training error** — keeps falling epoch after epoch: 22, 15, 11, 9, 8, 7 ... down to 5%
- **Fresh-mail error** — falls to 10.5% at epoch 5, then climbs back to 17% by epoch 10
- **The turn** — after epoch 5 the filter is memorizing quirks of old emails, not learning spam
- **The move** — stop at the turn and keep the epoch-5 model; more study makes it worse

*Example (italic):* Epochs 6-10 make the filter better at yesterday's inbox and worse at tomorrow's — the exact opposite of the goal.

**Key point:** **Early stopping** watches held-out error after every epoch and quits when it turns upward — training time itself is a dial, and more is not always better.

### Visualization (canvas `c1`, 720×300)

Two-line chart of training vs held-out error per epoch, with the held-out curve turning upward at epoch 5 and a shaded memorization zone after it.

- **Title (bold 15px, `#1a5276`, top center):** "Error per Epoch: Training Keeps Falling, Fresh Mail Turns (illustrative)"
- **Data (10 epochs, shared across the page):** training error (blue `#2a78d6`) `[22, 15, 11, 9, 8, 7, 6.5, 6, 5.5, 5]`; fresh held-out error (violet `#4a3aa7`) `[23, 17, 13, 11, 10.5, 11, 12, 13.5, 15, 17]`.
- **Axes (shared helper):** x epochs 1–10, label "epoch (passes over the training emails)"; y 0–25% (ticks every 5, formatted "5%" etc.). Axis lines `#999`, tick labels muted `#6b7280` 12px. Padding: top 50, bottom 50, left 62, right 170.
- **Memorization zone:** translucent magenta band `rgba(213,81,129,0.08)` filling the plot from epoch 5 to epoch 10.
- **Series:** width-3 lines in each color.
- **Turn marker:** green `#008300` 8px dot at (epoch 5, 10.5%) with bold 13px green label below: "the turn: epoch 5, 10.5%".
- **Annotations (bold):** magenta `#d55181` 13px near epoch 7 top area: "memorizing: fresh-mail" / "error climbs to 17%"; blue 12px near epoch 6.4 bottom: "training error: down forever".
- **Legend (right margin, 12px swatches):** blue "training emails"; violet "fresh held-out mail".

## The Stopping Rule, Step by Step

**Tags:** `worked example` (green), `validation curve` (blue)

- **Track the best** — after each epoch, compare fresh-mail error to the best seen so far
- **Epochs 1-5** — 23, 17, 13, 11, 10.5: a new best every time, snapshot saved each epoch
- **Epoch 6** — 11 > 10.5: no improvement, strike one
- **Epochs 7, 8** — 12 and 13.5: strikes two and three — patience of 3 is used up, stop
- **Roll back** — deploy the SAVED epoch-5 snapshot, not the epoch-8 model you stopped at
- **By hand** — the whole rule is one comparison per epoch and a counter from 0 to 3

*Example (italic):* Best-so-far after each epoch: 23, 17, 13, 11, 10.5, 10.5, 10.5, 10.5 — the moment it freezes, the clock is ticking.

**Key point:** **Two separate decisions:** patience decides WHEN to stop training (epoch 8), the saved snapshot decides WHICH model ships (epoch 5). Mixing them up ships a worse filter.

### Visualization (canvas `c2`, 720×300)

Validation curve with the best snapshot highlighted, three patience strikes labeled, a dashed stop line, and a rollback arrow from epoch 8 back to epoch 5.

- **Title (bold 15px, `#1a5276`, top center):** "Patience 3: Three Strikes After the Best, Then Roll Back"
- **Data:** the same held-out curve `[23, 17, 13, 11, 10.5, 11, 12, 13.5, 15, 17]` in violet `#4a3aa7`, width 3, with 4.5px dots at every epoch.
- **Axes:** identical shared axes (x epochs 1–10 labeled "epoch (passes over the training emails)", y 0–25%). Padding: top 50, bottom 50, left 62, right 40.
- **Best snapshot:** green `#008300` 9px dot at (epoch 5, 10.5%), bold 13px label below: "best: 10.5% — snapshot saved".
- **Strikes:** bold 13px orange `#d95926` labels "strike 1", "strike 2", "strike 3" above the points at epochs 6, 7, 8.
- **Stop marker:** dashed red `#e74c3c` (dash 6/4, width 2) vertical line at epoch 8; bold 13px red label near the top: "STOP after epoch 8".
- **Rollback arrow:** green quadratic-curve arrow (width 2.5) from above epoch 8 back to the epoch-5 dot, with a filled green arrowhead; bold 12px green label along it: "roll back: ship epoch 5".

## What It Buys You: a Better Model for Less Compute

**Tags:** `overfit guard` (blue), `where it's used` (orange)

- **Better model** — shipping epoch 5 means 10.5% error on fresh mail instead of epoch 10's 17%
- **Cheaper run** — training halted after epoch 8; the last two planned epochs never ran
- **Overfit guard** — it caps memorization without changing the model or the data at all
- **Free regularization** — one held-out set and a counter; no new penalty term to tune
- **Everywhere** — neural network epochs, boosting rounds, iterative solvers all use this brake
- **One flag away** — most libraries expose it as a flag; leaving it off invites silent overfitting

*Example (italic):* Gradient boosting's "number of trees" is the same story: each new tree is an epoch, and validation picks how many.

**Key point:** **Why it matters:** without early stopping, "train longer" quietly becomes "memorize harder" — and the damage never shows on the training scoreboard.

### Visualization (canvas `c3`, 720×300)

Two-panel comparison: left, vertical bars of shipped-model error with vs without early stopping; right, horizontal bars of epochs actually run.

- **Title (bold 15px, `#1a5276`, top center):** "With vs Without the Brake: Model Quality and Compute"
- **Left panel (bold 13px ink subtitle):** "fresh-mail error of the shipped filter". Vertical bars 80px wide on a y-axis 0–20% (ticks every 5%, axis lines `#999` from x=70 to x=340): "early stop (epoch 5)" = 10.5% in green `#008300`; "full run (epoch 10)" = 17% in magenta `#d55181`. Fills at 50% alpha with 2px strokes; bold 14px value labels ("10.5%", "17%") above each bar in the bar's color; 12px labels below.
- **Divider:** dashed light gray `#bdc3c7` vertical line at x=375.
- **Right panel (bold 13px ink subtitle):** "training epochs actually run". Two horizontal bars (260px full scale = 10 epochs, 36px tall, 50% alpha fill + 2px stroke): green "with early stopping: 8" (8/10 width); magenta "without: all 10" (full width); 12px labels above each bar.
- **Takeaway (bold 13px green, centered under right panel):** "a better model AND a shorter bill"

## The Confusion: Wrong Curve, Zero Patience

**Tags:** `common mistake` (red), `validation curve` (green)

- **Wrong curve** — training error almost always keeps falling; watching it, you never stop
- **The signal** — only the held-out curve can tell you learning has turned into memorizing
- **Zero patience** — stopping at the first uptick trusts a single noisy reading too much
- **A noisy run** — fresh-mail error 11.5 → 12.2 at epoch 5 looks like the turn, but it recovers
- **The real bottom** — this run's true best is 10.2% at epoch 7, after that scary blip
- **Rule of thumb** — give it a few strikes (patience 3-10) and always keep the best snapshot

*Example (italic):* Quitting the gym because one workout felt bad — one bad epoch is noise, three in a row is a trend.

**Key point:** **The confusion:** early stopping is not "stop when any number gets worse once" — it is "stop when the held-out score has not beaten its best for several epochs in a row".

### Visualization (canvas `c4`, 720×300)

Noisy validation curve where a single blip at epoch 5 fools zero-patience stopping, while the true best comes later at epoch 7.

- **Title (bold 15px, `#1a5276`, top center):** "A Noisy Run: Zero Patience Quits on a Blip (illustrative)"
- **Data:** noisy held-out error over 10 epochs: `[23, 17, 13, 11.5, 12.2, 10.8, 10.2, 10.6, 11.5, 12.5]`, violet `#4a3aa7` width-3 line with 4.5px dots.
- **Axes:** identical shared axes (x epochs 1–10 labeled "epoch (passes over the training emails)", y 0–25%). Padding: top 50, bottom 50, left 62, right 40.
- **Blip marker:** red `#e74c3c` open circle (10px radius, 2px stroke) around the point at (epoch 5, 12.2); bold 13px red labels: "zero patience stops HERE" above and "(one bad reading: 11.5 → 12.2)" below.
- **True best:** green `#008300` 8px filled dot at (epoch 7, 10.2); bold 13px green two-line label below: "true best: 10.2% at epoch 7" / "patience 3 waits and finds it".
- **Annotation (bold 12px orange `#d95926`, near epoch 4.5 top):** "one bad epoch is noise — three in a row is a trend"

## Regeneration instructions

- **Layout:** tutorial detail page. h1, `.subtitle` paragraph, then four `.card-section` blocks. Each section: `<h2>` with 2px `#2980b9` bottom border, then `table.layout` (full width, border-collapse) with one `<tr>`: left `<td class="text-col">` (50%) holding `.tags` pill row, `<ul>` bullets (each starting with `<b>` term in `#1a5276`), italic `.example` paragraph, and `.key-point` callout; right `<td class="viz-col">` (50%) holding one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; h2 1.3rem `#1a5276`; subtitle `#666` 0.95rem. Canvas `width:100%`, 1px `#e0e0e0` border, 4px radius. `.key-point`: background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem. `.example`: italic `#555` 0.9rem. Bullets 0.92rem.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px. blue: bg rgba(26,82,118,0.12) / `#1a5276`; green: bg rgba(39,174,96,0.15) / `#27ae60`; red: bg rgba(231,76,60,0.12) / `#e74c3c`; orange: bg rgba(230,126,34,0.15) / `#e67e22`.
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Canvas:** intrinsic 720×300 attributes; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Shared data arrays TRAIN `[22, 15, 11, 9, 8, 7, 6.5, 6, 5.5, 5]` and VAL `[23, 17, 13, 11, 10.5, 11, 12, 13.5, 15, 17]` plus a shared `axes()` helper (0–25% y-axis, epochs 1–10 x-axis) used by c1, c2, c4. All data hardcoded and deterministic; invented data labeled "(illustrative)". Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
