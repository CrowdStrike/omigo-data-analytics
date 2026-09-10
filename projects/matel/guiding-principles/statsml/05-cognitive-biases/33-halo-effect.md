# Halo Effect: Good at Two Things, Assumed Good at Everything

**Page type:** detail page — card-section template (see `05-cognitive-biases/25-mere-exposure-effect.html`)
**HTML title tag:** Halo Effect — Cognitive Biases

**Subtitle:** Someone who excels at maths, science and football is not thereby a strong swimmer or a historian. Two things checked become one overall impression, and that impression then answers for everything nobody looked at.

---

## Section 1 — Two Proven Skills Say Almost Nothing About a Third

**Tags:** `core idea` (violet), `eight skills` (blue), `unrelated` (magenta)

**Bullets:**
- **The setup** — 8 separate skills, and 400 people or systems rated on every one of them
- **What you observe** — two of the eight, and both come back in the top quarter of everyone
- **The leap** — the third skill is then assumed to be strong too, without anyone testing it
- **When the skills barely relate** — being top-quarter on two of them means little for the third
- **What actually happens** — about a third of them come out below average on that third skill
- **The number they should have** — a quarter of everyone is top-quarter, and that is roughly it
- **When skills do share a lot** — the leap works, and two thirds of them are top-quarter again
- **So the leap is not silly** — it is right or wrong depending on how related the skills are
- **What nobody ever asks** — how related these particular skills are before making the leap

**Key point:** The halo is not the belief that skills go together — sometimes they do. It is applying the leap as though they went together perfectly, without ever checking. When the skills are close to unrelated, two proven strengths move the estimate on a third almost not at all.

**Source note (`.src`):** Illustrative Example — 400 seeded profiles across 8 skills at two levels of relatedness; every share is counted from the plotted profiles.

### Visualization — canvas `c1`, 720×340

Two panels, one per level of relatedness. In each, the third skill's spread for the people who were top-quarter on the first two, so the left panel spreads across the whole range and the right one bunches high.

- **Data:** seeded Park–Miller LCG with 50 warm-up draws, seed 7. 400 profiles, 8 skills each. Every skill `= L × shared + √(1−L²) × own`, both approximately normal from six uniforms. Two panels at `L = 0.3` (skills barely related) and `L = 0.7` (skills share a lot). Selection: top quarter on skill 1 **and** skill 2. Outcome: where they land on skill 3.
- **Computed:** at `L = 0.3` — **25** selected, **8 of 25 (32%)** are top-quarter on skill 3 against a base rate of 25%, and **9 of 25 (36%)** are below average. At `L = 0.7` — **43** selected, **28 of 43 (65%)** top-quarter, **7 of 43 (16%)** below average. Every count is tallied from the drawn points.
- **Title (bold 15px `P.ink`, centered, y=21):** "Where the Third Skill Lands for People Strong at the First Two"
- **Layout:** two panels side by side across `PX=58 … w−40`, each with a horizontal skill-3 axis at y=196 spanning the full rated range, and the selected profiles as dots in an 80px band above it.
- **Points:** one dot radius 4 per selected profile, vertical position from its index rather than a second draw. Top-quarter on skill 3 filled `P.aqua`; below average filled `rgba(213,81,129,0.55)` stroked `P.magenta`; in between filled `rgba(107,114,128,0.35)`.
- **Reference marks:** a 2px dashed `P.mute` vertical line at the average labelled 12px "average", and a 2px `P.aqua` line at the top-quarter cut labelled 12px `P.aqua` "top quarter".
- **Panel headers** (bold 12px at each panel's left, y=44): `P.magenta` "SKILLS BARELY RELATED — maths and swimming" and `P.aqua` "SKILLS SHARE A LOT — maths and physics".
- **Panel tallies** (under each axis, from the panel's left): bold 19px in the panel hue with the top-quarter share, then 12px `P.mute` "top quarter on the third skill", then bold 12px `P.magenta` with the below-average share and 12px `P.mute` "below average on it".
- **Note line** (12px `P.mute`, centered, `h−28`): "a quarter of everyone is top-quarter to begin with — that is the number the leap has to beat".
- **Caption (bold 13px `P.violet`, centered, `h−8`):** "The same leap is sound on the right and empty on the left."

---

## Section 2 — What Gets Projected Onto the Six Nobody Looked At

**Tags:** `generalizing` (orange), `two seen, six assumed` (yellow), `the overshoot` (red)

**Bullets:**
- **What the person saw** — two skills out of the eight, both clearly strong, and nothing else at all
- **What they keep** — not two findings but one overall impression, with no skill attached to it
- **How the impression gets used** — it answers questions about the six skills nobody ever looked at
- **The generalizing step** — a read on two things quietly turns into a read on the whole set
- **What the impression says** — strong on all six of the rest, because it only has the one setting
- **What is really there** — strong on about 2 of the 6, which is roughly the ordinary spread
- **Also really there** — below average on about 2 of the 6, in those very same profiles
- **How common that is** — 22 of the 25 are below average on at least one of the six
- **The overshoot** — close to 4 skills in 6 assumed strong with nothing at all behind it

**Key point:** The error is not in what was observed — the two strong skills are real. It is that a person carries away one overall impression instead of two specific findings, and an overall impression has no way of saying "I only ever checked two of these."

**Source note (`.src`):** Illustrative Example — the same 400 seeded profiles as above at the barely-related setting; the assumed and actual counts are both computed in the draw function.

### Visualization — canvas `c2`, 720×340

Eight skill slots with the two that were checked marked off, then two bars across the remaining six: what the impression assumes about them, and what they actually hold.

- **Data:** the same construction as section 1 at the barely-related setting, seed 7, so these are the same 25 profiles. Assumed: the impression rates every unchecked skill strong, so 6 of 6. Actual: counted per profile across skills 3–8 and averaged.
- **Computed:** **25** profiles selected on the two checked skills. Assumed strong **6 of 6**. Actually top-quarter **2.2 of 6**. Actually below average **2.1 of 6**. Overshoot **3.8 of 6**. Profiles below average on at least one of the six: **22 of 25**. Every figure counted from the profiles.
- **Title (bold 15px `P.ink`, centered, y=21):** "Two Skills Were Checked. Six Were Assumed."
- **Layout:** a row of eight slots across `PX=62 … w−40` at y=62, height 34, labelled 12px `P.mute` "1" … "8" beneath. Slots 1 and 2 filled `rgba(25,158,112,0.30)` stroked `P.aqua`, tagged bold 12px `P.aqua` "checked" above; slots 3–8 white stroked `P.grid`, tagged 12px `P.mute` "never looked at".
- **Two bars** spanning only the six unchecked slots, height 44 on a 78px pitch from y=150: the assumed bar filled `rgba(217,89,38,0.45)` stroked `P.orange` across all six; the actual bar filled `rgba(25,158,112,0.30)` stroked `P.aqua` across its computed share, with the below-average share drawn from the right end inward in `rgba(213,81,129,0.35)` stroked `P.magenta` and the middle left white.
- **Bar labels** (right of each bar): bold 19px `P.orange` "6 of 6" over 12px `P.mute` "what the impression assumes"; bold 19px `P.aqua` "2.2 of 6" over 12px `P.mute` "actually strong", plus 12px `P.magenta` "2.1 of 6 below average".
- **The overshoot:** a 1.5px `#e74c3c` bracket spanning the gap between the two bars' filled ends, labelled bold 12px `#e74c3c` "3.8 skills assumed with nothing behind them", computed.
- **Note line** (12px `P.mute`, centered, `h−28`): "22 of the 25 are below average on at least one of the six", computed.
- **Caption (bold 13px `P.orange`, centered, `h−8`):** "One impression cannot say which two skills it came from."

---

## Regeneration instructions

- **Template:** the card-section layout from `05-cognitive-biases/25-mere-exposure-effect.html`. One `.card-section` per section, each an `<h2>` plus a `table.layout` with `td.text-col` 50% / `td.viz-col` 50%.
- **Text column order:** `.tags` pill row → `<ul>` of one-line bullets each opening `<b>label</b>` then an em dash → one `.key-point` callout → `.src` note.
- **Bullet form:** one line at 50% column width, 90–100 characters including the bold label.
- **Page CSS:** identical to `25-mere-exposure-effect.html`. No nav, no `.nav` CSS, no back/home links, no cross-page links.
- **Hue family per section:** 1 aqua against magenta with a violet caption, 2 orange with a hard red for wrong answers.
- **Canvas:** intrinsic `width="720"`, both heights 340. `setup(id)` caches the logical size in `dataset`, sets `style.maxWidth = 720px`, computes `scale = (cssW/720) × devicePixelRatio`, sizes the backing store to `logical × scale`, and `ctx.scale(scale, scale)`. Draws registered in `__charts`, re-run on debounced (150ms) resize.
- **Canvas fonts:** chart title bold 15px; in-chart header bold 12px; body and axis labels 12px floor; big callout figure bold 19px; caption bold 13px.
- **Palette** (shared `P`): `blue #2a78d6`, `green #008300`, `magenta #d55181`, `yellow #c98500`, `aqua #199e70`, `orange #d95926`, `violet #4a3aa7`, `ink #1a5276`, `text #2c3e50`, `mute #6b7280`, `grid #e5e9ef`. Hard red `#e74c3c` for below-average outcomes and wrong answers.
- **Determinism:** no `Math.random()`. Both charts use the seeded LCG with **50 discarded warm-up draws**; the first output from a small seed is near zero, which would otherwise pin the opening profile to the middle of every skill.
- **Every printed figure is computed in its draw function** — both selected counts and all four shares in section 1; both wrong counts and both average polish figures in section 2.
- **Say the shares in plain words.** Speak of "top quarter", "below average" and "about a third", not correlations, loadings or quartile cut-points. An earlier version of this material reported skill-pair correlation coefficients; a reader cannot check those against the picture, and the picture already shows the spread.
- **Both panels in section 1 are required.** Showing only the unrelated case would teach that the leap is always wrong. It is right when skills genuinely share a lot, and stating that boundary honestly is the difference between a rule and a caricature.
- **Section 2 is about the person, not the tool.** An earlier draft showed a model's answers arriving equally polished on strong and weak task types, so the reader would blame the output style. That relocates the bias into the tool; this folder is about analyst psychology. The bias is the person generalizing from two observations to a whole set, which is why section 2 measures the assumption against the profiles rather than measuring fluency.
- **Say "generalizing", not "extrapolation" or "transfer".** The plain word is the one that names what the person did.
- **What separates this from card 31.** Card 31 (AI Over-Trust) is about depth — how thoroughly you check one task, and why a clean sample says nothing about a generator. This page is about breadth — carrying a capability estimate from one kind of task to a different kind.
- **What separates this from the domain-pitfalls page.** `04-domain-pitfalls/131-trust-halo-authority-bias-in-organizations` covers the organisational version, where seniority is read as correctness outside someone's field. This page is the estimation error itself, with no hierarchy involved.
- **Keep this page at two sections.**
