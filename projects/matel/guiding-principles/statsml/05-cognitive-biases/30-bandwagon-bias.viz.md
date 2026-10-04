# Bandwagon Bias — Viz

**Page type:** detail page, card-section (template `06-sectioned-cards-callout`)
**HTML title tag:** Bandwagon Bias — Cognitive Biases
**Source note wording:** the sibling `.txt.md` says figures are "computed at render time"; the html `.src` notes say "computed in the draw function".

House style (bullet form, tag-pill markup, text-column order, canvas DPR scaling, shared palette `P`, canvas font floors, section-header borders) lives in `ui-templates/` and is not restated here.

**Figures printed in the sibling `.txt.md` are computed here.** Changing a seed, the appeal spread, or the social-weight exponent invalidates that prose — counts, winners and shares must be re-read from the new draw and the text updated to match.

**Determinism:** seeded Park–Miller LCG, **50 discarded warm-up draws** — load-bearing, see the section 1 note below. Six markets of 120 adopters total, so the page draws in well under a frame. No `Math.random()`.

---

## 1. Eight Near-Equal Items, One Visible Counter

**Tag colors:** `core idea` violet, `same eight items` blue, `one thing added` magenta
**Hue family:** violet/blue against magenta

### canvas `c1` — 720×330

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

## 2. Run It Again and a Different Item Wins

**Tag colors:** `not reproducible` orange, `four reruns` yellow, `order decides` red
**Hue family:** orange with an aqua marker

### canvas `c2` — 720×340

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

## Page-specific constraints

- **Every printed figure is computed in its draw function** — counts, winners and shares from the generated arrays; the distinct-winner count and the best-item-never-wins claim from comparisons, not text.
- **Keep the appeal spread small and real.** An earlier construction used 0.30–0.70; merit then dominated and the social run still ranked items correctly, teaching the opposite lesson. Near-equal options are the point, not a detail.
- **Keep this page at two sections.** An earlier draft added a third that swept 200 markets at each of five sizes up to 51,200 adopters to show sample size does not help. The finding is real but costs a heavy sweep, a log axis and a rank-correlation statistic the reader cannot check; it now lives as one clause in section 2's key point. Do not reinstate it as a chart.
- **No rank correlations.** Whether the top item is the genuinely best one is checkable by eye against the appeal row; a Spearman coefficient is not.
- **Placement note:** this folder covers analyst-facing psychology, so the page is about popularity contaminating a measurement an analyst would read. Bandwagon as a design lever is covered elsewhere in the repo and is deliberately not repeated here.
