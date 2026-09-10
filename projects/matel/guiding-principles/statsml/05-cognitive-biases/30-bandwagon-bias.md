# Bandwagon Bias: The Winner Records Who Went First

**Page type:** detail page — card-section template (see `05-cognitive-biases/25-mere-exposure-effect.html`)
**HTML title tag:** Bandwagon Bias — Cognitive Biases

**Subtitle:** Show each person what everyone before them picked and the top item stops being a fact about the items. Eight near-equal options are enough to see it.

---

## Section 1 — Eight Near-Equal Items, One Visible Counter

**Tags:** `core idea` (violet), `same eight items` (blue), `one thing added` (magenta)

**Bullets:**
- **The eight items** — really are different, but only just: appeal runs 0.45 to 0.55, so item 8 is best
- **The adopters** — 120 people arrive one at a time and each picks exactly one item
- **Choosing blind** — nobody sees earlier picks, so the counts land between 9 and 21, top item on 18%
- **Now show the running counts** — same items, same tastes, nobody's preference is changed at all
- **What happens** — item 1 takes 99 of the 120 picks, 83% of everything, from the same seed
- **Item 1's appeal** — 0.45, the lowest of the eight, so the worst item won the whole market
- **How it got there** — it happened to lead early, which made it more visible, which made it lead more
- **What the 83% measures** — the order the first few people arrived in, not anything about item 1

**Key point:** Both runs held identical items and identical tastes. Adding a counter turned the least appealing of the eight into an 83% winner, so the counts now answer "who went first" rather than "which is best".

**Source note (`.src`):** Illustrative Example — 120 seeded adopters run twice from one seed; every count and share is computed in the draw function.

### Visualization — canvas `c1`, 720×330

Two count panels over the same eight items — blind above, counter visible below — with true appeal drawn underneath as the row both are read against.

- **Data:** seeded Park–Miller LCG with 50 warm-up draws, seed 51. Eight items, appeal `Q[i] = 0.45 + 0.10 × i/7`. 120 sequential adopters. Blind: pick weight is `Q[i]`. Social: weight `Q[i] × (1 + count[i]/t)^6`.
- **Computed:** blind counts **12, 21, 18, 13, 9, 15, 17, 15** — top item **2** on **21 (18%)**. Social counts **99, 2, 6, 2, 2, 2, 4, 3** — top item **1** on **99 (83%)**, and item 1 has the lowest appeal of the eight. Read off the generated arrays.
- **Warm-up is load-bearing, not cosmetic.** Park–Miller's first output from a small seed is near zero, which hands the opening pick to item 1 every time and locks every cascade onto it. The 50 discarded draws remove that; without them the chart shows a property of the generator, not of the mechanism.
- **Title (bold 15px `P.ink`, centered, y=21):** "One Hundred Twenty Adopters, Eight Near-Equal Items"
- **Layout:** two panels sharing eight item slots across `PX=58 … w−44`. Upper baseline y=142, top y=48; lower baseline y=262, top y=168. Both on one shared count scale so the landslide is legible against the flat panel.
- **Bars:** blind `rgba(42,120,214,0.45)` stroked `P.blue`; social `rgba(213,81,129,0.50)` stroked `P.magenta`. The winning bar in each panel labelled bold 19px in its hue with its count and 12px `P.mute` with its share.
- **Panel headers** (bold 12px, left-aligned at `PX`): `P.blue` "CHOOSING BLIND — nobody sees earlier picks" and `P.magenta` "COUNTER VISIBLE — everyone sees the running totals".
- **Appeal row:** item numbers 12px `P.mute` under the lower baseline, then eight small squares on a single-hue ramp built from the appeal values, with 12px `P.mute` "true appeal 0.45 → 0.55" to the left, the best item's square ringed 2px `P.aqua` labelled "best", and the social winner's square ringed 2px `P.magenta` labelled "won 83%".
- **Caption (bold 13px `P.violet`, centered, `h−8`):** "Same items, same tastes — only the counter was added."

---

## Section 2 — Run It Again and a Different Item Wins

**Tags:** `not reproducible` (orange), `four reruns` (yellow), `order decides` (red)

**Bullets:**
- **The rerun** — identical items, identical appeal, identical rules; only the arrival order differs
- **First run** — item 1 runs away with 83%, and it is the least appealing of the eight
- **Second run** — item 4 wins with 52%, a much narrower landslide than the first
- **Third run** — item 6 takes 86%, and items 1 through 5 finish with two picks or fewer between them
- **Fourth run** — item 4 again, this time on 93%, with six of the eight items scoring under five
- **What repeats** — every run produces a runaway winner, so the shape of the outcome is reliable
- **What never repeats** — which item it is, and the genuinely best item won none of the four
- **The reading** — a share that changes hands on rerun describes the mechanism, not the winner

**Key point:** The landslide reproduces and the winner does not. Because more adopters keep copying the same early picks, a bigger market makes this number tighter without making it truer — which is why sample size, the usual fix for a noisy ranking, does not help here.

**Source note (`.src`):** Illustrative Example — the same construction on four seeds; every count, winner and share is computed in the draw function.

### Visualization — canvas `c2`, 720×340

Four small panels, one per rerun, each showing the eight final counts with its winner marked, so the winner moving between panels is the whole point.

- **Data:** the same construction as section 1 on seeds 51, 11, 23, 37, in that order.
- **Computed:** seed 51 → item **1**, **99** picks, **83%**. Seed 11 → item **4**, **62**, **52%**. Seed 23 → item **6**, **103**, **86%**. Seed 37 → item **4**, **112**, **93%**. The genuinely best item (8) wins **none** of the four — verified by comparing each winner index against the best index in the draw function rather than asserted.
- **Title (bold 15px `P.ink`, centered, y=21):** "The Same Market, Run Four Times"
- **Layout:** a 2×2 grid of panels, each ~300×112, origins from x=52 and y=52 on a 336×134 pitch. Every panel uses one shared count scale taken from the largest count across all four, so panel heights are comparable.
- **Bars:** eight per panel, `rgba(217,89,38,0.40)` stroked `P.orange`, the winning bar refilled `rgba(217,89,38,0.75)` and labelled bold 12px `P.orange` above with "item N — 83%" style text, computed per panel.
- **Panel labels:** 12px `P.mute` top-left reading "run 1" … "run 4"; item numbers 12px `P.mute` under each panel's baseline.
- **Best-item marker:** a small 2px `P.aqua` tick under item 8's slot in every panel, labelled 12px `P.aqua` "genuinely best" in the first panel only, so the reader can see it never wins.
- **Note line** (12px `P.mute`, centered, `h−28`): "four runs, three different winners, and the best item took none of them" — the distinct-winner count computed from the four results.
- **Caption (bold 13px `P.orange`, centered, `h−8`):** "The landslide reproduces. The winner does not."

---

## Regeneration instructions

- **Template:** the card-section layout from `05-cognitive-biases/25-mere-exposure-effect.html`. One `.card-section` per section, each an `<h2>` plus a `table.layout` with `td.text-col` 50% / `td.viz-col` 50%.
- **Text column order:** `.tags` pill row → `<ul>` of one-line bullets each opening `<b>label</b>` then an em dash → one `.key-point` callout → `.src` note.
- **Bullet form:** one line at 50% column width, 90–100 characters including the bold label.
- **Page CSS:** identical to `25-mere-exposure-effect.html`. No nav, no `.nav` CSS, no back/home links, no cross-page links.
- **Hue family per section:** 1 violet/blue against magenta, 2 orange with an aqua marker.
- **Canvas:** intrinsic `width="720"`, heights 330 and 340. `setup(id)` caches the logical size in `dataset`, sets `style.maxWidth = 720px`, computes `scale = (cssW/720) × devicePixelRatio`, sizes the backing store to `logical × scale`, and `ctx.scale(scale, scale)`. Draws registered in `__charts`, re-run on debounced (150ms) resize.
- **Canvas fonts:** chart title bold 15px; in-chart header bold 12px; body and axis labels 12px floor; big callout figure bold 19px; caption bold 13px.
- **Palette** (shared `P`): `blue #2a78d6`, `green #008300`, `magenta #d55181`, `yellow #c98500`, `aqua #199e70`, `orange #d95926`, `violet #4a3aa7`, `ink #1a5276`, `text #2c3e50`, `mute #6b7280`, `grid #e5e9ef`.
- **Determinism:** no `Math.random()`. Seeded Park–Miller LCG with **50 discarded warm-up draws** — load-bearing, see the section 1 note. Six markets of 120 adopters in total, so the page draws in well under a frame.
- **Every printed figure is computed in its draw function** — counts, winners and shares from the generated arrays; the distinct-winner count and the best-item-never-wins claim from comparisons, not text.
- **Keep the appeal spread small and real.** An earlier construction used 0.30–0.70; merit then dominated and the social run still ranked items correctly, teaching the opposite lesson. Near-equal options are the point, not a detail.
- **Keep this page at two sections.** An earlier draft added a third that swept 200 markets at each of five sizes up to 51,200 adopters to show sample size does not help. The finding is real but costs a heavy sweep, a log axis and a rank-correlation statistic the reader cannot check; it now lives as one clause in section 2's key point. Do not reinstate it as a chart.
- **No rank correlations.** Whether the top item is the genuinely best one is checkable by eye against the appeal row; a Spearman coefficient is not.
- **Placement note:** this folder covers analyst-facing psychology, so the page is about popularity contaminating a measurement an analyst would read. Bandwagon as a design lever is covered elsewhere in the repo and is deliberately not repeated here.
