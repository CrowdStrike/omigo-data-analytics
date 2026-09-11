# Zero-Shot & One-Shot Learning

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Zero-Shot &amp; One-Shot Learning

**Subtitle:** A "shot" is one example in the prompt — zero-shot asks with none, one-shot shows a single demo, and the model adapts without any training

## Zero, One, or a Handful of Examples

**Tags:** `core idea` (blue), `shots = examples` (green)

- **The task** — Alice wants feedback messages labeled as complaint, praise, or question
- **Zero-shot** — she just asks: "label this message"; no examples, instructions only
- **One-shot** — she shows one solved case first: message in, label out, then asks
- **Few-shot** — she shows a handful of solved cases before asking (its own page)
- **The word** — each example in the prompt is called a "shot"; the count names the technique

*Example (italic):* "Translate to French: cheese →" is zero-shot; adding "sea otter → loutre de mer" first makes it one-shot — one demo, nothing else changed.

**Key point:** The shot count only counts examples in the prompt — the model itself is identical in all three setups.

### Visualization (canvas `c1`, 720×300)

Three side-by-side prompt panels showing the same task with zero, one, and five shots.

- **Title (bold 15px, `#1a5276`, top center):** "The Same Task, Three Prompt Sizes".
- **Panels:** three rounded rects 210×180 at x = `[30, 255, 480]`, y=48; borders 2px `#2a78d6` / `#199e70` / `#4a3aa7`; fills `rgba(42,120,214,0.05)` / `rgba(25,158,112,0.05)` / `rgba(74,58,167,0.05)`; bold 13px colored headers centered at y=70: "zero-shot", "one-shot", "few-shot".
  - **Zero-shot panel (11px `#2c3e50`, left-aligned at x=46):** lines at y=96/112: "Label as complaint,", "praise, or question:"; then bold 11px `#2a78d6` at y=136: "\"box arrived crushed\""; 11px `#6b7280` at y=160: "→ ?"; bold 11px `#2a78d6` "instructions only" centered at (135, 208).
  - **One-shot panel (left-aligned at x=271):** 11px `#199e70` at y=96: "\"love this blender!\""; 11px `#199e70` at y=112: "→ praise"; 1px `#e5e9ef` divider line from (271,122) to (444,122); 11px `#2c3e50` at y=140: "\"box arrived crushed\""; 11px `#6b7280` at y=156: "→ ?"; bold 11px `#199e70` "one worked example" centered at (360, 208).
  - **Few-shot panel (left-aligned at x=496):** three 11px `#4a3aa7` lines at y=92/106/120: "\"love this…\" → praise", "\"where is my…\" → question", "\"it broke…\" → complaint"; 11px `#6b7280` at y=134: "…two more examples…"; divider at y=144; 11px `#2c3e50` at y=162: "\"box arrived crushed\" → ?"; bold 11px `#4a3aa7` "a handful of examples" centered at (585, 208).
- **Annotation (bold 12px orange `#d95926`, centered at y=258):** "same model, same weights — only the prompt grows from left to right".
- **Caption (11px `#444`, bottom right, y=292):** "prompts abbreviated".

## Label 20 Messages, Score Each Setup

**Tags:** `worked example` (blue), `prompting` (orange)

- **The test** — the same 20 feedback messages labeled under each setup (illustrative numbers)
- **Zero-shot** — 14 of 20 right (70%); answers drift in format: "sounds angry", "complaint!!"
- **One-shot** — 17 of 20 right (85%); the single demo locks the format and settles edge cases
- **Few-shot (5)** — 18 of 20 right (90%); rarer confusions get covered by the extra demos
- **The price** — the shots ride along on every call: roughly 0, 40, and 200 extra words

*Example (italic):* The one demo did the heavy lifting: +15 points for 40 words, while the next four examples together bought only +5 more.

**Key point:** The first shot usually buys the biggest jump — it teaches the format; later shots mostly patch rarer mistakes.

### Visualization (canvas `c2`, 720×300)

Bar chart: labeling accuracy for zero-, one-, and few-shot, with the prompt-size cost under each bar.

- **Title (bold 15px, `#1a5276`, top center):** "20 Messages: Accuracy by Shot Count (illustrative)".
- **Axes:** baseline y=220, plot top y=64; y = accuracy 0–100% with `#e5e9ef` gridlines at 25/50/75/100 and 12px `#444` right-aligned tick labels ("25%"…"100%") at x=64; axis lines 1px `#999` from (70,64) to (70,220) to (660,220); scale 1.56px per point.
- **Bars (110px wide, centered x = `[200, 390, 580]`):**
  - "zero-shot" 70%, fill `#2a78d6`; bold 13px `#2a78d6` "14 / 20" above the bar.
  - "one-shot" 85%, fill `#199e70`; bold 13px `#199e70` "17 / 20" above.
  - "few-shot (5)" 90%, fill `#4a3aa7`; bold 13px `#4a3aa7` "18 / 20" above.
- **X labels (12px `#444`, centered at y=240):** "zero-shot", "one-shot", "few-shot (5)".
- **Cost row (11px `#6b7280`, centered at y=258):** "+0 words", "+40 words", "+200 words".
- **Jump markers (bold 11px `#d95926`):** "+15 pts" at (295, 100) and "+5 pts" at (485, 84), each near the gap between bar tops.
- **Annotation (bold 12px orange `#d95926`, centered at y=284):** "the first example buys the most — after that, returns shrink and prompts grow".
- **Caption (11px `#444`, bottom right, y=297):** "accuracy illustrative".

## Learning Without a Training Run

**Tags:** `where it's used` (blue), `in-context learning` (green)

- **The old meaning** — before LLMs, "learning" meant a training run that changed model weights
- **The surprise** — big models adapt from examples in the prompt alone; no weights move
- **The name** — this is in-context learning; the "learning" lives and dies with the prompt
- **The heritage** — zero/one-shot originally described classifiers trained with 0 or 1 examples per class
- **The habit** — practitioners start zero-shot, then add shots only where errors actually show up

*Example (italic):* Bob's classifier project in 2015 needed thousands of labeled rows and a training job; the same task today starts with zero rows and one sentence.

**Key point:** Escalate lazily — zero-shot first, one shot when format wobbles, a few when edge cases persist, fine-tuning only after shots run out.

### Visualization (canvas `c3`, 720×300)

Escalation ladder: four steps from zero-shot to fine-tuning, with effort rising and a "start here" marker.

- **Title (bold 15px, `#1a5276`, top center):** "Escalate Only When Errors Demand It".
- **Steps (rounded rects, staircase layout, each 150 wide × 56 tall):** step 1 at (40, 190), step 2 at (200, 150), step 3 at (360, 110), step 4 at (520, 70); borders 2px `#2a78d6` / `#199e70` / `#4a3aa7` / `#d55181`; fills matching at 0.07 alpha.
  - **Step 1:** bold 12px `#2a78d6` "zero-shot" centered at top +20; 11px `#2c3e50` "just ask" at +40.
  - **Step 2:** bold 12px `#199e70` "one-shot"; 11px "add one demo".
  - **Step 3:** bold 12px `#4a3aa7` "few-shot"; 11px "add a handful".
  - **Step 4:** bold 12px `#d55181` "fine-tuning"; 11px "training run, new weights".
- **Start marker:** bold 12px `#008300` "start here" at (115, 176) with a short 1.5px `#008300` arrow pointing down to step 1's top edge.
- **Effort arrow:** 1.5px `#6b7280` diagonal arrow from (60, 262) to (660, 262)... instead draw along the bottom: from (40, 268) to (660, 268) with arrowhead; 11px `#6b7280` label "effort, cost, and data needed" centered at (350, 284).
- **Divider note:** dashed 1.5px `#6b7280` vertical line at x=505 from y=60 to y=250; bold 11px `#6b7280` rotated not needed — place "prompt changes | weight changes" as two labels: bold 11px `#199e70` "prompt only" at (400, 56) and bold 11px `#d55181` "weights change" at (595, 56).
- **Annotation (bold 12px orange `#d95926`, left-aligned at (40, 84)):** two lines "most tasks never need" / "the last two steps" at y=84/102.
- **Caption (11px `#444`, bottom right, y=297):** "ladder simplified".

## It Forgets the Moment You Hang Up

**Tags:** `common mistake` (red), `nothing persists` (orange)

- **The illusion** — the model "learned" your labels so well it feels trained; it isn't
- **The reality** — close the conversation and the example is gone; next session starts blank
- **Not fine-tuning** — shots change one conversation; fine-tuning changes the model for all of them
- **What a shot teaches** — mostly format and boundaries; it does not add knowledge the model lacks
- **The tell** — if you paste the same example into every session, you've rediscovered why skills exist

*Example (italic):* Alice's perfectly-tuned one-shot prompt worked all Tuesday; Wednesday's fresh session labeled everything "complaint!!" again until she pasted the demo back in.

**Common mistake:** Saying "I trained it" after a good few-shot session — nothing was trained; the examples must ride along in every future prompt to keep working.

### Visualization (canvas `c4`, 720×300)

Two-conversation timeline showing the in-prompt example working, vanishing, then contrasted with fine-tuning.

- **Title (bold 15px, `#1a5276`, top center):** "The Shot Lives Only Inside One Conversation".
- **Row 1 — conversation 1:** rounded rect x=40, y=56, 400×64, fill `rgba(0,131,0,0.05)`, 2px `#008300` border; bold 12px `#008300` left-aligned "conversation 1 (demo pasted in)" at (56, 78); 11px `#2c3e50` at (56, 100): "example + task → labels come back clean"; bold 11px `#008300` "works" centered at (480→ place at (475, 92)) right of the box.
- **Row 2 — conversation 2:** rounded rect x=40, y=136, 400×64, fill `rgba(213,81,129,0.05)`, 2px `#d55181` border; bold 12px `#d55181` "conversation 2 (fresh session)" at (56, 158); 11px `#2c3e50` at (56, 180): "same task, no example → format drifts again"; bold 11px `#d55181` "forgot" at (475, 172).
- **Contrast box:** rounded rect x=530, y=56, 160×144, fill `rgba(74,58,167,0.05)`, 2px `#4a3aa7` border; bold 12px `#4a3aa7` centered "fine-tuning" at (610, 80); 11px `#2c3e50` centered lines at y=104/122/140: "weights updated,", "every session", "remembers"; bold 11px `#4a3aa7` "different mechanism" centered at (610, 168).
- **Divider arrow between rows:** 1.5px `#6b7280` dashed arrow from (240, 120) to (240, 130); 11px `#6b7280` "session ends — context wiped" left-aligned at (250, 129).
- **Annotation (bold 12px orange `#d95926`, centered at y=244):** "in-context learning rents the behavior — fine-tuning buys it".
- **Caption (11px `#444`, bottom right, y=292):** "timeline illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** the c2 bars must read exactly 14/20 (70%), 17/20 (85%), 18/20 (90%) with cost row +0/+40/+200 words to match the text; all values hardcoded, no randomness.
- **Scope note:** this page owns the zero/one/few spectrum and in-context learning; crafting good few-shot examples is the neighboring few-shot page's job — do not duplicate its content.
- **Content discipline:** Alice/Bob for people; accuracy numbers labeled illustrative; the "sea otter → loutre de mer" translation demo is the classic published in-context-learning example.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
