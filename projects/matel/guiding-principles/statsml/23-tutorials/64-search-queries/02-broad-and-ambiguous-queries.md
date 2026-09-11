# Broad & Ambiguous Queries

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Broad &amp; Ambiguous Queries

**Subtitle:** One word like "jaguar" carries several meanings at once — the engine shows every major sense on page one and lets past clicks vote on how to split the slots

## One Word, Three Meanings

**Tags:** `core idea` (blue), `multiple intents` (orange)

- **The query** — someone types "jaguar": seven letters, and nothing else about what they want
- **Three senses** — the car brand, the big cat, and the sports team all share that same string
- **The blind spot** — nothing in the text says which meaning this particular user has in mind
- **The definition** — a query is ambiguous when one string maps to several distinct meanings
- **The move** — instead of betting on one sense, the engine puts every major sense on page one

*Example (italic):* A car shopper, a zoologist, and a sports fan can type the identical query within the same minute — the engine sees the same seven letters three times.

**Key point:** An ambiguous query is not one question — it is several questions wearing the same word, and page one has to serve all of them.

### Visualization (canvas `c1`, 720×300)

Branching diagram: one query box on the left fanning out to three meaning boxes on the right, connector thickness proportional to each sense's share.

- **Title (bold 15px, ink `#1a5276`, top center):** 'One String, Three Intents: "jaguar"'.
- **Query box:** rect x=45, y=125, w=140, h=50, fill `#eef4fb`, 2px ink border; bold 16px ink text `"jaguar"` centered; 12px mute `#6b7280` label "the query" centered above the box at y=115.
- **Warning line (bold 12px red `#e74c3c`, centered at x=115):** two lines under the box at y=203 / y=219: "the text alone" / "cannot say which".
- **Meaning boxes (x=450, w=230, h=48; tops at y=55, 126, 197):** each with a light fill, 2px colored border, bold 13px sense name at x+14 (left-aligned) and bold 13px share at x+216 (right-aligned) in the same color:
  - "the car brand" — blue `#2a78d6`, fill `rgba(42,120,214,0.10)`, share "≈55%"
  - "the animal (big cat)" — green `#008300`, fill `rgba(0,131,0,0.08)`, share "≈30%"
  - "the sports team" — orange `#d95926`, fill `rgba(217,89,38,0.10)`, share "≈15%"
- **Connectors:** straight lines from (185, 150) to the left-edge midpoint of each meaning box; line width 6 / 4 / 2.5 and color matching the target box (blue / green / orange).
- **Caption (11px mute, bottom right):** "shares illustrative — section 2 estimates them from clicks".

## Letting 1,000 Clicks Vote

**Tags:** `worked example` (blue), `click votes` (green)

- **The log** — the last 1,000 clicks on "jaguar" results: 550 car pages, 300 animal, 150 team
- **The mix** — divide by 1,000: the intent mix is 55% car, 30% animal, 15% team
- **The vote** — each click is a ballot; click share estimates how often each meaning is wanted
- **The layout** — ten slots split about 5 car, 3 animal, 2 team instead of ten car results
- **The interleave** — the majority sense keeps the top slot; minority senses sit high enough to find

*Example (italic):* Redo it by hand: 300 animal clicks ÷ 1,000 total = 30%, so about 3 of the 10 slots go to the animal sense.

**Key point:** Clicks turn an unanswerable question ("which meaning does this user want?") into a measurable one ("how often is each meaning wanted?").

### Visualization (canvas `c2`, 720×300)

Two-panel chart split by a dashed divider at x=350: left, the click-count bars with the derived percentages; right, a mocked ten-slot page one colored by sense.

- **Title (bold 15px, ink, top center):** "From Click Log to a Diversified Page One (illustrative)".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=350 from y=38 to y=285.
- **Left panel header (bold 12px ink, centered x=180, y=48):** "1,000 past clicks by sense".
- **Left bars:** baseline y=240 (1px `#999` axis line from x=40 to x=320), plot height 150, y max 600; three bars w=65 at x=55, 155, 255 with values `[550, 300, 150]`; fills `rgba(42,120,214,0.55)` / `rgba(0,131,0,0.5)` / `rgba(217,89,38,0.55)` with 2px borders blue `#2a78d6` / green `#008300` / orange `#d95926`.
- **Bar labels:** count bold 13px in the border color at barTop−20 ("550", "300", "150"); share 12px mute at barTop−6 ("55%", "30%", "15%"); sense name 12px `#2c3e50` below the baseline ("car", "animal", "team").
- **Left footer (bold 12px ink, centered x=180, y=272):** "click share = intent mix".
- **Right panel header (bold 12px ink, centered x=535, y=48):** "page one: 10 slots follow the mix".
- **Slots:** two columns (x=380 and x=555), five rows each, rect w=155, h=26, row tops y = 64 + i×36; slot order 1–10 = car, car, animal, car, team, car, animal, car, animal, team (slots 1–5 in the left column, 6–10 in the right); each slot uses its sense's light fill and colored 1.5px border, with bold 12px text in the sense color: "1. car", "2. car", "3. animal", "4. car", "5. team", "6. car", "7. animal", "8. car", "9. animal", "10. team".
- **Right footer (bold 12px ink, centered x=535, y=258):** "5 car / 3 animal / 2 team ≈ 55 / 30 / 15".
- **Caption (11px mute, centered x=535, y=278):** "majority sense keeps slot 1; every sense is findable".

## Why Serving Only the Majority Fails

**Tags:** `where it's used` (blue), `common mistake` (red)

- **The temptation** — the car sense wins 55% of clicks, so why not fill all ten slots with cars?
- **The cost** — 450 of the 1,000 users (45%) then find nothing for their meaning on page one
- **The distribution** — a broad query is ranked against a mix of intents, not one right answer
- **Context shifts** — near a safari park, or during playoffs, the same query's mix moves
- **The metric** — diversity-aware evaluation scores a page on covering intents, not stacking one

*Example (italic):* During the team's championship week, team-page clicks on "jaguar" can outnumber car clicks — yesterday's 15% sense becomes today's majority.

**Key point:** Optimizing a broad query for its majority sense alone fails nearly half the users — covering the mix, not winning the plurality, is the objective.

### Visualization (canvas `c3`, 720×300)

Two horizontal stacked bars on a shared 0–1,000-user axis: an all-car page one versus the diversified page one, showing who finds their meaning.

- **Title (bold 15px, ink, top center):** "Same 1,000 Users: All-Car Page vs Diversified Page (illustrative)".
- **Bar geometry:** user axis maps 0–1,000 users to x=185–665 (0.48 px per user); both bars h=40.
- **Row labels (bold 13px ink, right-aligned at x=172):** "all-car page" at the first bar's vertical center, "diversified page" at the second's.
- **Bar 1 (top y=78):** blue segment 550 users fill `rgba(42,120,214,0.75)` with centered white bold 13px label "550 served"; red segment 450 users fill `rgba(231,76,60,0.75)` with white bold 13px label "450 find nothing".
- **Bar 2 (top y=168):** blue 550 ("550 car"), green 300 fill `rgba(0,131,0,0.6)` ("300 animal"), orange 150 fill `rgba(217,89,38,0.75)` ("150 team"); white bold labels, 12px on the two narrower segments.
- **Between-bars annotation (bold 13px red, centered x=425, y=147):** "45% of users stranded on page one".
- **Axis:** 1px `#999` baseline at y=232 from x=185 to x=665; 12px `#444` tick labels "0", "250", "500", "750", "1,000" with small ticks; axis caption 12px `#444` centered at y=268: "users whose meaning appears on page one".
- **Bottom annotation (bold 12px green `#008300`, centered x=425, y=288):** "diversified: every meaning present — all 1,000 served".

## Ambiguous vs Underspecified

**Tags:** `common confusion` (red), `two fixes` (orange)

- **Two kinds of broad** — "jaguar" and "shoes" both give the engine too little, in different ways
- **Ambiguous** — "jaguar": one string, several unrelated meanings; the senses compete
- **Underspecified** — "shoes": one agreed meaning, but missing size, style, gender, price
- **Fix one** — ambiguous queries call for diversification: one page, every major sense
- **Fix two** — underspecified queries call for refinement: facets, filters, follow-up prompts
- **The tell** — would two users disagree on what the word means, or only on the details?

*Example (italic):* No one disputes what a shoe is — a "shoes" searcher needs narrowing questions, not a page that hedges across meanings.

**Common confusion:** Treating every broad query the same. Different meanings need diversification; missing details need refinement prompts — the wrong fix wastes page one either way.

### Visualization (canvas `c4`, 720×300)

Side-by-side branching diagrams split by a dashed divider at x=360: "jaguar" fanning into three different-colored meanings with a diversify fix banner; "shoes" fanning into three same-colored refinements with a refine fix banner.

- **Title (bold 15px, ink, top center):** "Two Kinds of Broad: Different Meanings vs Missing Details".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=36 to y=250.
- **Left header (bold 13px ink, centered x=180, y=54):** 'Ambiguous — "jaguar"'.
- **Left query box:** rect x=120, y=68, w=120, h=34, fill `#eef4fb`, 2px ink border, bold 13px ink `"jaguar"` centered; 1.5px `#aaa` lines from its bottom center (180, 102) to the top centers of the child boxes.
- **Left children (three rects w=92, h=32, y=138; x=32, 134, 236):** "car brand" blue border/text, "big cat" green, "sports team" orange, each with its light fill; bold 12px labels centered.
- **Left fix banner:** rect x=60, y=196, w=240, h=34, fill `rgba(0,131,0,0.08)`, 2px green border, bold 12px green centered: "fix: diversify — show every sense".
- **Left tag (11px mute, centered x=180, y=252):** "the meanings disagree".
- **Right header (bold 13px ink, centered x=540, y=54):** 'Underspecified — "shoes"'.
- **Right query box:** rect x=480, y=68, w=120, h=34, same style, bold 13px ink `"shoes"`; lines from (540, 102) to the child boxes.
- **Right children (three rects w=92, h=32, y=138; x=392, 494, 596):** "running", "size 9 wide", "under $100" — all violet `#4a3aa7` border/text with fill `rgba(74,58,167,0.08)`; bold 12px labels (one meaning, differing details).
- **Right fix banner:** rect x=420, y=196, w=240, h=34, fill `rgba(217,89,38,0.08)`, 2px orange border, bold 12px orange centered: "fix: refine — ask for the details".
- **Right tag (11px mute, centered x=540, y=252):** "the meaning is agreed; details are missing".
- **Caption (bold 12px ink, centered x=360, y=284):** "the tell: disagree on the meaning → diversify; only on the details → refine".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border (no index number), `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label — "Key point:" on sections 1–3, "Common confusion:" on section 4).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** `P = { blue:'#2a78d6', green:'#008300', magenta:'#d55181', yellow:'#c98500', aqua:'#199e70', orange:'#d95926', violet:'#4a3aa7', ink:'#1a5276', text:'#2c3e50', mute:'#6b7280', grid:'#e5e9ef' }`. Red `#e74c3c` only for the failure states (stranded users, "cannot say which").
- **Data:** every number is a hardcoded literal (no randomness) and labeled illustrative: click log `[550, 300, 150]` of 1,000; intent mix 55/30/15; slot split 5/3/2 with slot order car, car, animal, car, team, car, animal, car, animal, team; c3 served counts 550 vs 1,000 (450 stranded = 45%). Text numbers match chart numbers exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
