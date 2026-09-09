# Compartmental Disease Models

**Page type:** detail page (four titled sections, each a two-column row: text left 50%, canvas right 50%)
**HTML title tag:** Compartmental Disease Models — SIR, R₀, and the Shape of an Epidemic

**Subtitle:** Three coupled differential equations — Susceptible, Infected, Recovered — generate the entire vocabulary of epidemic policy: R₀, herd immunity, and flatten-the-curve.

**Intro callout (blue-left-border box):** The SIR model (Kermack & McKendrick, 1927) is the hydrogen atom of epidemiology — simple enough to solve, rich enough to produce the epidemic curve, the outbreak threshold, and the herd-immunity fraction from just two parameters. Everything a headline says about an epidemic's shape traces back to this small machine, and so do the caveats about trusting it too far.

## 1. The machinery: stocks and flows

The model divides a population into compartments and moves people between them with two flow rates — that is the whole mechanism.

- **Three stocks:** everyone is Susceptible, Infected, or Recovered, and moves S → I → R.
- **Infection flow:** new infections arrive at rate β·S·I/N, from contacts between S and I.
- **Why the product:** infections need a susceptible person to meet an infected one.
- **Recovery flow:** people leave I at rate γ·I, so 1/γ is the average infectious period.
- **Two parameters:** β counts transmission contacts per day; γ counts recoveries per day.
- **Conservation:** nobody enters or leaves — S + I + R stays fixed at N.
- **Deterministic:** the same parameters produce exactly the same curve on every run.

Key point: The entire model is two flows between three boxes. Every famous epidemic quantity — R₀, the peak, the final size — is arithmetic on β and γ.

### Visualization (canvas `c1`, 720×320)

Three labeled compartment boxes S, I, R connected by flow arrows carrying their rate formulas, with a note beneath defining what β and γ count.

- **Title (bold 14px `#1a5276`, centered, y=22):** "SIR: two flows move everyone left to right"
- **S box:** 150×80 at (60, 110), fill `rgba(26,82,118,0.12)`, 2px `#1a5276` border; bold 13px `#1a5276` centered at (135, 145): "S"; 10px `#555` centered at (135, 165): "susceptible".
- **I box:** 150×80 at (285, 110), fill `rgba(231,76,60,0.12)`, 2px `#e74c3c` border; bold 13px `#e74c3c` centered at (360, 145): "I"; 10px `#555` centered at (360, 165): "infected".
- **R box:** 150×80 at (510, 110), white fill, 2px `#27ae60` border; bold 13px `#27ae60` centered at (585, 145): "R"; 10px `#555` centered at (585, 165): "recovered".
- **Flow arrows:** 2px `#999` horizontal arrows with filled arrowheads at y=150, from x=212 to x=283 and from x=437 to x=508.
- **Flow labels:** bold 11px `#e67e22` centered above the arrows at y=138: "β·S·I/N" at x=247 and "γ·I" at x=472; 10px `#666` centered below the arrows at y=170: "infection flow" at x=247 and "recovery flow" at x=472.
- **Parameter note:** 480×46 box at (120, 236), white fill, 1.5px dashed `#8e44ad` border; bold 11px `#8e44ad` centered at (360, 254): "TWO PARAMETERS"; 10px `#666` centered at (360, 270): "β = transmission contacts per person per day · γ = recovery rate, so 1/γ = infectious period".
- **Caption (12px `#999`, centered, y = h−14):** "Same parameters, same curve every run — the model is deterministic"

## 2. R₀ and the epidemic threshold

One ratio of the two parameters decides everything: whether a spark fizzles or becomes an epidemic.

- **The definition:** R₀ = β/γ, the average secondary infections in a fully susceptible population.
- **The threshold:** R₀ > 1 means each case more than replaces itself, so the outbreak grows.
- **Below one:** R₀ < 1 means chains of transmission die out on their own.
- **A sharp edge:** the transition at R₀ = 1 is a knife-edge, not a gradual slope.
- **Effective R falls:** as susceptibles deplete, R_eff = R₀·S/N drops below 1 and the curve turns.
- **Peak before saturation:** the epidemic peaks and declines before everyone is infected.
- **Not a constant:** R₀ bundles behavior, density, and setting — it is not a property of the virus alone.

Key point: R₀ is a threshold parameter, not a severity score: the qualitative fate of an outbreak flips at exactly 1. And because β lives inside it, anything that changes contact behavior changes R₀.

### Visualization (canvas `c2`, 720×360)

Three infected-count curves from the same starting point — R₀ below 1 fizzling, just above 1 forming a slow bump, and well above 1 spiking sharply — with the threshold annotated.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Three fates from one starting spark — the threshold sits at R₀ = 1"
- **Axes:** 1.5px `#999` — vertical from (60, 56) to (60, 300), horizontal from (60, 300) to (660, 300). Labels 10px `#999`: "infected" left-aligned at (66, 52); "time →" centered at (360, 322).
- **Fizzle curve (R₀ = 0.8):** 2px `#27ae60` polyline over x=60..660 with y = 300 − 30 × exp(−(x − 60) / 80) (decays from a small initial count to zero).
- **Slow-bump curve (R₀ = 1.3):** 2px `#e67e22` polyline over x=60..660 with y = 300 − 85 × exp(−((x − 430) / 140)²) (wide low peak arriving late).
- **Spike curve (R₀ = 3):** 2.5px `#e74c3c` polyline over x=60..660 with y = 300 − 215 × exp(−((x − 260) / 62)²) (tall narrow peak arriving early).
- **Legend (top-left block starting (80, 52), 18px line spacing):** 22px line swatch in each curve's color/width + 11px `#555` labels: "R₀ = 0.8 — chains die out", "R₀ = 1.3 — slow, late bump", "R₀ = 3 — sharp early spike".
- **Threshold annotation:** bold 11px `#1a5276` right-aligned at (648, 246): "R₀ = 1 is a knife-edge:", second line at (648, 262): "grow or die, nothing between".
- **Caption (12px `#999`, centered, y = h−14):** "Same spark, same population — only β/γ differs between the three curves"

## 3. Herd immunity and flattening the curve

The model's two most quoted policy results both fall out of the same threshold algebra.

- **Herd immunity:** the outbreak stalls once the immune fraction exceeds 1 − 1/R₀.
- **The mechanism:** with enough immunity, each case finds fewer than one susceptible to infect.
- **Vaccination's role:** it reaches the threshold fraction without paying for it in infections.
- **Worked example:** R₀ = 3 gives a herd-immunity threshold of 1 − 1/3 ≈ 67% immune.
- **Flattening:** interventions that cut β lower the peak and push it later in time.
- **Roughly same area:** flattening reshapes the curve more than it shrinks the total.
- **The real target:** keep the peak under a fixed healthcare capacity, which does not flex.

Key point: Flattening the curve was never about preventing infections so much as scheduling them — the peak, not the area, is what overwhelms a hospital system with fixed beds.

### Visualization (canvas `c3`, 720×360)

Two epidemic curves — sharp unmitigated versus flattened — against a horizontal dashed healthcare-capacity line, with the overflow region above capacity shaded on the sharp curve.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Two epidemics, one capacity line — the peak is the policy variable"
- **Axes:** 1.5px `#999` — vertical from (60, 56) to (60, 300), horizontal from (60, 300) to (660, 300). Labels 10px `#999`: "infected" left-aligned at (66, 52); "time →" centered at (360, 322).
- **Capacity line:** dashed (6/4) 2px `#555` horizontal line at y=185 from x=60 to x=660; bold 11px `#555` left-aligned at (68, 178): "healthcare capacity".
- **Sharp curve:** 2.5px `#e74c3c` polyline over x=60..660 with y = 300 − 190 × exp(−((x − 240) / 72)²).
- **Overflow shading:** fill `rgba(231,76,60,0.12)` — the closed region between the sharp curve and the capacity line wherever the curve rises above y=185 (trace the curve where 190 × exp(−((x − 240) / 72)²) > 115, close along the capacity line).
- **Flattened curve:** 2.5px `#27ae60` polyline over x=60..660 with y = 300 − 105 × exp(−((x − 400) / 132)²).
- **Curve labels:** bold 11px `#e74c3c` left-aligned at (140, 92): "unmitigated"; bold 11px `#27ae60` left-aligned at (430, 176): "with interventions (lower β)".
- **Overflow annotation:** bold 10px `#e67e22` right-aligned at (648, 96): "overflow — demand exceeds beds", with a thin 1px `#e67e22` connector line from (500, 102) to (300, 140).
- **Caption (12px `#999`, centered, y = h−14):** "Roughly the same area under both curves — flattening schedules infections, capacity does not flex"

## 4. Extensions and honest limits

The base model extends gracefully — but its core simplification is false in a way that matters, and its parameters refuse to sit still.

- **SEIR:** an Exposed (latent) stage between S and I delays the whole dynamic.
- **Richer variants:** age structure, waning immunity, and vaccination terms fit real diseases.
- **The core assumption:** homogeneous mixing — everyone contacts everyone equally.
- **Why it is false:** real contact is networked and clustered, which changes spread and thresholds.
- **Parameter drift:** β moves mid-epidemic as behavior shifts, with or without policy.
- **Forecast decay:** long-range epidemic forecasts degrade fast because the model chases a moving β.
- **The defensible use:** compare scenarios — "what does closing X change" — rather than predict dates.

Key point: The model's parameters are entangled with the behavior it is trying to predict, so point forecasts expire quickly. Scenario comparison holds up because both branches share the same drifting β.

### Visualization (canvas `c4`, 720×380)

SEIR box chain across the top; below it, an epidemic curve observed up to "today" with a forecast fan widening rapidly beyond it, annotated with the drifting-β caveat.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Add boxes freely — trust long-range forecasts sparingly"
- **SEIR chain:** four 120×54 boxes with tops at y=44: S at (60, 44), fill `rgba(26,82,118,0.12)`, 2px `#1a5276` border, bold 12px `#1a5276` centered "S" at (120, 66), 10px `#555` centered "susceptible" at (120, 84); E at (220, 44), white fill, 2px dashed `#8e44ad` border, bold 12px `#8e44ad` centered "E" at (280, 66), 10px `#555` centered "exposed (latent)" at (280, 84); I at (380, 44), fill `rgba(231,76,60,0.12)`, 2px `#e74c3c` border, bold 12px `#e74c3c` centered "I" at (440, 66), 10px `#555` centered "infected" at (440, 84); R at (540, 44), white fill, 2px `#27ae60` border, bold 12px `#27ae60` centered "R" at (600, 66), 10px `#555` centered "recovered" at (600, 84).
- **Chain arrows:** 2px `#999` horizontal arrows with filled arrowheads at y=71, from x=182 to x=218, from x=342 to x=378, and from x=502 to x=538.
- **SEIR note:** 10px `#8e44ad` centered at (280, 116): "the new stage delays the peak".
- **Forecast axes:** 1.5px `#999` — vertical from (60, 150) to (60, 330), horizontal from (60, 330) to (660, 330). Labels 10px `#999`: "infected" left-aligned at (66, 146); "time →" centered at (360, 350).
- **Observed curve:** 2px `#1a5276` polyline over x=60..300 with y = 330 − 130 × exp(−((x − 360) / 130)²) (the rising flank only, stopping at x=300).
- **Today marker:** dashed (4/4) 1px `#999` vertical line from (300, 155) to (300, 330); 10px `#999` centered at (300, 148): "today".
- **Forecast fan fill:** fill `rgba(26,82,118,0.12)` — polygon from the observed endpoint (300, y≈259) along the upper branch to (660, 170), down the right edge to (660, 322), and back along the lower branch to (300, y≈259).
- **Upper branch:** 2px dashed (5/4) `#e74c3c` polyline from (300, 259) through (420, 205), (540, 182), (660, 170).
- **Central branch:** 2px `#999` polyline over x=300..660 continuing y = 330 − 130 × exp(−((x − 360) / 130)²).
- **Lower branch:** 2px dashed (5/4) `#27ae60` polyline from (300, 259) through (420, 292), (540, 314), (660, 322).
- **Fan annotation:** bold 11px `#e67e22` right-aligned at (648, 240): "behavior shifts move β under the model", with a thin 1px `#e67e22` connector line from (560, 246) to (500, 265).
- **Caption (12px `#999`, centered, y = h−14):** "Compare scenarios, do not promise trajectories — the fan widens faster than the epidemic moves"

## Regeneration instructions

- **Template:** backlog multi-section detail page. After h1, `.subtitle`, and `.intro` callout, one `.lang-section` per numbered section: `h2` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px padding-bottom), then a `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) with lead paragraph, bullets, `.key-point`; right `<td class="viz-col">` (50%) with the canvas. Section h2 headings carry the "1.–4." numbers shown above. The h1 carries no index number.
- **Bullet style:** each bullet is a bold label + one short complete sentence that fits on one line at normal page width — split dense content into more bullets rather than longer ones. Bullet `<strong>` labels are colored `#1a5276` (via `li strong { color: #1a5276; }`).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`; `.subtitle` `#666` 0.95rem; `.intro` — background `#f0f4f8`, `border-left: 3px solid #2980b9`, padding 8px 12px, 0.9rem, 32px margin-bottom; `.key-point` — background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem; `ul` 0.92rem; canvases `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px; `.lang-section` 40px margin-bottom.
- **Canvases:** intrinsic width 720, heights 320/360/360/380 as specified; shared `setupCanvas(id, h)` helper scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Canvas fonts use `-apple-system, sans-serif`. Every canvas has a bold 14px `#1a5276` centered title at y=22 and a 12px `#999` centered caption at y = h−14.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, accent `#2980b9`; grays `#555`/`#666`/`#999`/`#bbb`/`#ccc`; translucent fills `rgba(26,82,118,0.12)`, `rgba(231,76,60,0.12)`.
- No nav bar, no back/home links, no cross-references to other pages, no item counts.
