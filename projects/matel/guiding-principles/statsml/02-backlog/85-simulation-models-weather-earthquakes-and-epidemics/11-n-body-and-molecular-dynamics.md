# N-body & Molecular Dynamics

**Page type:** detail page (four titled sections, each a two-column row: text left 50%, canvas right 50%)
**HTML title tag:** N-body & Molecular Dynamics — Particles, Forces, and the O(N²) Wall

**Subtitle:** The same recipe simulates galaxies and proteins — compute the force on every particle from every other, step positions forward, repeat — and the whole field is a fight against that recipe's cost.

**Intro callout (blue-left-border box):** N-body simulation is physics at its most literal: no mesh, no aggregate equations, just particles and the forces between them. The catch is arithmetic — naive pairwise forces cost O(N²) per step, and both cosmology and chemistry need billions of steps over millions of particles.

## 1. The recipe: forces, then a step

One loop drives everything from star clusters to solvated proteins: sum the forces on each particle, advance positions and velocities by one small timestep, and repeat until the physics of interest has played out.

- **Universal recipe:** Each particle feels a force from every other — gravity for stars, electrostatics and bonds for atoms.
- **Sum then step:** Every step sums all forces on each particle and nudges its position and velocity forward.
- **Symplectic integrators:** Leapfrog and velocity Verlet are the workhorses because they respect the geometry of mechanics.
- **Energy stays bounded:** A symplectic scheme keeps energy oscillating around the truth instead of drifting away over long runs.
- **Timestep tyranny:** The step must resolve the fastest motion in the system, not the motion you actually care about.
- **Naive cost:** N particles means roughly N²/2 pairwise force evaluations at every single step.
- **The field in one line:** Everything after the recipe is tricks to cut that cost without corrupting the physics.

Key point: The force loop is trivially simple and brutally expensive — O(N²) per step, with the timestep dictated by the fastest motion present, is the wall the entire field is organized around.

### Visualization (canvas `c1`, 720×340)

Left: a cluster of nine particles with force-arrow lines converging on one highlighted particle, line strength fading with distance. Right: a small panel showing the leapfrog scheme as alternating position and velocity updates on a staggered timeline.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Every particle pulls on every other — then take one small step"
- **Left panel:** header bold 12px `#1a5276` centered at (195, 55): "EVERY PAIR INTERACTS". Highlighted particle: filled `#e74c3c` circle radius 6 at (170, 185). Eight other particles: filled `rgba(26,82,118,0.35)` circles radius 4 with 1px `#1a5276` stroke at (90, 100), (240, 85), (315, 145), (265, 245), (120, 270), (55, 195), (215, 150), (330, 265). Force lines: from each other particle toward the highlighted one, stopping 12px short of its center, drawn with filled arrowheads pointing at the highlighted particle; line width and opacity fade with distance — 2px `rgba(26,82,118,0.85)` for the two nearest, 1.5px `rgba(26,82,118,0.55)` for the mid-range four, 1px `rgba(26,82,118,0.3)` for the two farthest. Sub-label 11px `#666` centered at (195, 300): "force on one particle sums all the others".
- **Right panel:** header bold 12px `#27ae60` centered at (550, 55): "LEAPFROG INTEGRATOR". Two horizontal 1px `#ccc` guide lines: position track at y=140 and velocity track at y=215, both from x=425 to x=685. Track labels 11px `#666` left-aligned: "x" at (408, 144) and "v" at (408, 219). Position dots: filled `#1a5276` circles radius 5 at (450, 140), (540, 140), (630, 140), labeled 9px `#999` centered underneath-above at y=126: "t", "t+Δt", "t+2Δt". Velocity dots: filled `#27ae60` circles radius 5 at (495, 215), (585, 215), (675, 215), labeled 9px `#999` centered at y=236: "t+Δt/2", "t+3Δt/2", "t+5Δt/2". Zigzag: 1.5px `#999` arrows with filled arrowheads (450,140)→(495,215), (495,215)→(540,140), (540,140)→(585,215), (585,215)→(630,140), (630,140)→(675,215). Note 10px `#666` centered at (550, 268): "positions and velocities alternate on a staggered grid"; 10px `#999` centered at (550, 284): "symplectic — energy stays bounded over long runs".
- **Caption (12px `#999`, centered, y = h−14):** "Same loop for galaxies and proteins — only the force law and the timestep change"

## 2. Beating O(N²)

The pairwise sum is exact but unaffordable, so every production code replaces most of it with structured approximations whose error is measured, bounded, and tuned below the physics of interest.

- **Barnes-Hut trees:** A tree groups distant particles into single pseudo-masses, dropping the cost to O(N log N).
- **Opening angle:** One tunable knob decides when a distant cell is far enough to collapse into its center of mass.
- **Fast multipole:** Multipole expansions push the cost further, toward O(N) for large systems.
- **Cutoff radii:** Molecular dynamics simply stops evaluating short-range forces beyond a few nanometers.
- **Neighbor lists:** Each atom keeps a cached list of nearby atoms so the cutoff never requires a full scan.
- **Long-range electrostatics:** Grid-based Ewald-style methods handle the slowly decaying Coulomb tail that cutoffs would butcher.
- **Controlled error:** These are not hacks — the approximation error is tuned so the physics of interest survives.

Key point: Approximation is the price of scale, but it is a negotiated price — tree openings, cutoffs, and grid resolutions are dials with known error bounds, set so the corruption stays below the signal.

### Visualization (canvas `c2`, 720×400)

A 2D particle field overlaid with a quadtree — fine cells around a dense nearby cluster, one large cell around a far-away group collapsed to a single big pseudo-particle with one arrow to the highlighted particle. Right side: a small cost-per-step summary panel.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Group the far away, keep the nearby exact"
- **Quadtree frame:** outer square 1.5px `#ccc` from (60, 50) to (380, 370). Level-1 split: 1px `#ccc` lines at x=220 (vertical, y 50–370) and y=210 (horizontal, x 60–380). Lower-left quadrant subdivided: 1px `#ccc` lines at x=140 (y 210–370) and y=290 (x 60–220). Its lower-left subcell subdivided again: 1px `#ddd` lines at x=100 (y 290–370) and y=330 (x 60–140).
- **Highlighted particle:** filled `#e74c3c` circle radius 5 at (105, 325); bold 10px `#e74c3c` left-aligned "our particle" at (115, 318).
- **Nearby cluster:** eight filled `rgba(26,82,118,0.35)` circles radius 3 with 1px `#1a5276` stroke at (80, 305), (95, 345), (125, 310), (130, 350), (70, 330), (150, 330), (115, 295), (160, 355) — dense around the highlighted particle in the finely divided cells.
- **Far group:** nine faint filled `rgba(26,82,118,0.2)` circles radius 2.5 scattered around (310, 115) at (290, 95), (325, 90), (335, 120), (300, 135), (315, 105), (285, 120), (330, 140), (295, 110), (320, 128). Pseudo-particle: filled `#8e44ad` circle radius 9 at (310, 115). One 1.5px `#8e44ad` arrow with filled arrowhead from (300, 125) to (115, 315), stopping 12px short of the highlighted particle.
- **Annotation:** bold 11px `#8e44ad` centered at (310, 68): "distant group ≈ one pseudo-particle"; 10px `#666` centered at (310, 82): "one force instead of nine".
- **Cost panel (right):** header bold 12px `#1a5276` centered at (560, 70): "COST PER STEP". Left-aligned entries at x=460, each a bold 11px colored label then a 10px `#666` phrase on the next line: `#e74c3c` "naive pairwise — O(N²)" at y=105 / "every pair, every step" at y=120; `#1a5276` "Barnes-Hut tree — O(N log N)" at y=150 / "distant cells become pseudo-masses" at y=165; `#27ae60` "fast multipole — toward O(N)" at y=195 / "expansions, not pairs" at y=210; `#e67e22` "MD cutoffs + neighbor lists" at y=240 / "ignore beyond a few nanometers" at y=255; `#8e44ad` "Ewald-style grids" at y=285 / "long-range electrostatics on a mesh" at y=300. Note 10px `#999` left-aligned at (460, 335): "each dial trades measured error for speed".
- **Caption (12px `#999`, centered, y = h−14):** "The tree keeps nearby forces exact and collapses the distant sky into a handful of terms"

## 3. Galaxies to proteins

The same recipe, with different force laws and timesteps, spans about sixty orders of magnitude in mass — from dark-matter particles tracing the cosmic web to atoms folding a protein around a drug molecule.

- **Cosmology:** Billions of dark-matter particles reproduce the cosmic web of filaments and voids from nearly uniform initial conditions.
- **Solar system:** Long integrations of the planets reveal chaos — orbits are predictable for millions, not billions, of years.
- **Molecular dynamics:** Atomistic runs show proteins folding, membranes assembling, and drug molecules binding to their targets.
- **Force fields:** The interatomic forces are fitted functions, calibrated against quantum-mechanical calculations.
- **Materials:** Crack propagation, alloy behavior, and battery chemistry are studied at atomic resolution.
- **Validation differs:** Cosmology checks the statistics of structure; chemistry checks against experiment and quantum references.
- **No single truth test:** Neither field can compare a run against the one true trajectory — both validate distributions and observables.

Key point: One recipe spans the universe and the test tube, but the ground truth differs — cosmologists validate statistical structure, chemists validate against measured and quantum-computed observables, and neither can check a single trajectory.

### Visualization (canvas `c3`, 720×380)

Two side-by-side panels. Left: cosmic-web sketch — dots concentrated along filaments meeting at bright nodes over a dark background rect. Right: simplified protein ribbon as a folded curve with a small ligand dot docking into a pocket.

- **Title (bold 14px `#1a5276`, centered, y=22):** "The same recipe at both ends of the scale"
- **Left panel:** header bold 12px `#1a5276` centered at (190, 50): "COSMIC WEB — DARK MATTER". Background: filled `#1c2833` rectangle from (40, 62) to (340, 302). Nodes: filled `#fff` circles radius 3 at (110, 130), (255, 105), (185, 210), (95, 255), (275, 255). Filaments: 1px `rgba(255,255,255,0.25)` lines connecting node pairs (110,130)–(255,105), (110,130)–(185,210), (255,105)–(185,210), (185,210)–(95,255), (185,210)–(275,255). Particle dots along filaments: for each filament, nine filled `rgba(255,255,255,0.6)` circles radius 1 at evenly spaced interpolation points t = 0.1…0.9, each offset perpendicular by `4 × sin(7t + filament index)` pixels. Sparse void dots: six filled `rgba(255,255,255,0.25)` circles radius 1 at (70, 100), (300, 160), (140, 285), (60, 200), (310, 90), (230, 165). Panel caption 11px `#666` centered at (190, 322): "filaments and voids emerge from near-uniform start".
- **Right panel:** header bold 12px `#27ae60` centered at (540, 50): "PROTEIN & LIGAND". Ribbon: 3px `#27ae60` folded curve drawn as chained quadratic segments through control points (440, 260) → (470, 120) → (530, 250) → (585, 110) → (620, 245) → (655, 150), rendered as quadraticCurveTo calls using the odd points as controls: start (440, 260), curve control (470, 120) to (530, 250), control (585, 110) to (620, 245), then a final quadratic control (655, 150) to (665, 220). Pocket: the concave gap around (560, 195). Ligand: filled `#e67e22` circle radius 6 at (560, 178); 1.5px `#e67e22` arrow with filled arrowhead from (560, 150) to (560, 168); bold 10px `#e67e22` centered "ligand docks into the pocket" at (560, 140). Panel caption 11px `#666` centered at (540, 322): "atoms, bonds, and a drug finding its target".
- **Caption (12px `#999`, centered, y = h−14):** "Sixty orders of magnitude apart in mass — validated by statistics on one side, experiment on the other"

## 4. The timescale wall — and the ML shortcut

Molecular dynamics is limited less by system size than by time: femtosecond steps chasing millisecond events leave a trillion-step gap that hardware, sampling tricks, and now machine learning each close only partly.

- **Femtosecond steps:** The timestep must resolve bond vibrations, and those oscillate on femtosecond scales.
- **Millisecond goals:** Protein folding and many binding events take microseconds to milliseconds to happen.
- **Trillion-step gap:** Twelve orders of magnitude separate the step size from the event of interest.
- **Enhanced sampling:** Biasing methods push the simulation toward rare transitions, then reweight the statistics afterward.
- **Special hardware:** Purpose-built machines bought orders of magnitude of speed — but not the whole gap.
- **ML force fields:** Learned potentials now approximate quantum-accurate forces at nearly classical cost.
- **Endpoint vs path:** Learned structure predictors answer the endpoint question without simulating the journey.
- **Dynamics still simulates:** Mechanisms, rates, and pathways still require actually integrating the motion.

Key point: ML collapsed the endpoint problem — predicting the folded structure — but the pathway problem remains a simulation problem, because rates and mechanisms live in the trajectory that structure predictors never compute.

### Visualization (canvas `c4`, 720×340)

A horizontal log timescale bar from 10⁻¹⁵ s to 10⁻³ s with labeled tick marks, a blue bracket over the reachable range, an orange arrow extending toward milliseconds, and a green note at the far end for ML endpoint prediction.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Twelve orders of magnitude between the step and the event"
- **Axis:** 2px `#999` horizontal line at y=180 from x=60 to x=660; one decade = 50px. Minor ticks: 1px `#ccc` vertical ticks 6px tall below the line at every decade x = 60, 110, …, 660. Major labels 10px `#999` centered at y=202 under x = 60, 210, 360, 510, 660: "10⁻¹⁵ s", "10⁻¹² s", "10⁻⁹ s", "10⁻⁶ s", "10⁻³ s"; 9px `#bbb` centered at y=216 under the same x: "fs", "ps", "ns", "µs", "ms".
- **Motion labels:** 1px `#555` vertical ticks 10px tall above the line, with bold 10px `#555` centered labels: "bond vibration" at x=85 (tick at 85, label y=150); "side-chain motion" at x=225 (label y=132); "loop motion" at x=375 (label y=150); "folding µs–ms" at x=570 (label y=132).
- **Gap annotation:** bold 11px `#e74c3c` centered at (360, 66): "femtosecond steps, millisecond goals — a trillion-step gap"; thin 1px `#e74c3c` horizontal double-arrow at y=80 from x=90 to x=630 with small arrowheads at both ends.
- **Blue bracket:** 2px `#1a5276` horizontal line at y=240 from x=60 to x=510 with 8px upward end ticks at both ends; bold 11px `#1a5276` centered at (285, 260): "brute-force MD — reachable".
- **Orange arrow:** 2px `#e67e22` arrow with filled arrowhead at y=282 from x=510 to x=650; bold 11px `#e67e22` centered at (540, 302): "enhanced sampling + special hardware".
- **Green note:** bold 11px `#27ae60` right-aligned at (665, 120): "ML predicts the endpoint, not the path"; thin 1px `#27ae60` connector line from (655, 126) to (655, 168).
- **Caption (12px `#999`, centered, y = h−14):** "Hardware and sampling stretch the reach — ML jumps to the answer but skips the mechanism"

## Regeneration instructions

- **Template:** backlog multi-section detail page. After h1, `.subtitle`, and `.intro` callout, one `.lang-section` per numbered section: `h2` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px padding-bottom), then a `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) with lead paragraph, bullets, `.key-point`; right `<td class="viz-col">` (50%) with the canvas. Section h2 headings carry the "1.–4." numbers shown above. The h1 carries no index number.
- **Bullet style:** each bullet is a bold label + one short complete sentence that fits on one line at normal page width; split dense content into more bullets rather than longer ones. Bullet `<strong>` labels are colored `#1a5276` (via `li strong { color: #1a5276; }`).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`; `.subtitle` `#666` 0.95rem; `.intro` — background `#f0f4f8`, `border-left: 3px solid #2980b9`, padding 8px 12px, 0.9rem, 32px margin-bottom; `.key-point` — background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem; `ul` 0.92rem; canvases `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px; `.lang-section` 40px margin-bottom.
- **Canvases:** intrinsic width 720, heights 340/400/380/340 as specified; shared `setupCanvas(id, h)` helper scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Canvas fonts use `-apple-system, sans-serif`. Every canvas gets a bold 14px `#1a5276` centered title at y=22 and a 12px `#999` centered caption at y = h−14.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, accent `#2980b9`; grays `#555`/`#666`/`#999`/`#bbb`/`#ccc`; translucent fills `rgba(26,82,118,0.12)`, `rgba(26,82,118,0.35)`; dark panel fill `#1c2833` for the cosmic-web sketch only.
- No nav bar, no back/home links, no cross-references to other pages, no item counts in text.
