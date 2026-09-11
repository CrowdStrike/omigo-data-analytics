# Encoder-only vs Decoder-only

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Encoder-only vs Decoder-only

**Subtitle:** An encoder reads a whole sentence at once and hands back a judgment; a decoder writes one word at a time, each guessed only from the words before it — BERT reads, GPT writes

## Two Helpers in the Support Inbox

**Tags:** `core idea` (blue), `reading vs writing` (green), `two model families` (orange)

- **The inbox** — a shoe store gets hundreds of customer emails a day and hires two software helpers
- **The sorter** — one helper reads each whole email and stamps a single label: refund, complaint, or praise
- **The writer** — the other helper drafts the reply, producing it one word at a time, left to right
- **Encoder-only** — the sorter is an encoder: input everything, output one judgment (BERT works this way)
- **Decoder-only** — the writer is a decoder: input a prompt, output a stream of next words (GPT works this way)
- **Same building block** — both are transformers under the hood; the difference is what they look at and emit

*Example (italic):* The email "my sneakers fell apart in a week, I want my money back" goes to the sorter, which outputs one word — "refund" — and to the writer, which types out "Hi, we're so sorry..." word by word.

**Key point:** Encoder-only models read everything and output a judgment; decoder-only models output the text itself, one next word at a time.

### Visualization (canvas `c1`, 720×300)

Two-panel flow diagram: left panel shows the sorter (encoder) turning a whole email into a single label; right panel shows the writer (decoder) emitting a reply word by word.

- **Title (bold 15px, `#1a5276`, top center):** "The Sorter Reads, the Writer Writes".
- **Divider:** vertical 1px `#e5e9ef` line at x=360 from y=40 to y=280; panel headings bold 13px at y=55 — left `#2a78d6` "encoder-only (the sorter)" centered at x=180, right `#008300` "decoder-only (the writer)" centered at x=540.
- **Left panel:** email box (rounded rect x=25, y=80, w=140, h=70, fill `rgba(42,120,214,0.12)`, 1px `#2a78d6` border) containing 11px `#2c3e50` lines: "my sneakers fell" / "apart in a week," / "I want my money back"; 2px `#2a78d6` arrow to model box (x=195, y=90, w=70, h=50, fill `#2a78d6`, bold 12px white label "reads it all"); 2px arrow to output pill (x=290, y=97, w=55, h=36, fill `rgba(0,131,0,0.15)`, 1px `#008300` border, bold 12px `#008300` text "refund").
- **Right panel:** prompt box (x=390, y=80, w=110, h=70, fill `rgba(0,131,0,0.10)`, 1px `#008300` border, 11px `#2c3e50` text "reply to this" / "refund email"); 2px `#008300` arrow to model box (x=520, y=90, w=70, h=50, fill `#008300`, bold 12px white label "writes"); then five small word chips left to right at y=165 (each rounded rect h=24, 1px `#008300` border, 11px `#2c3e50` text) reading "Hi,", "we're", "so", "sorry", "..." with tiny 1px `#6b7280` arrows between chips.
- **Annotation (bold 12px `#d95926`):** left panel near x=180, y=200: "one look, one label"; right panel near x=540, y=215: "one word at a time, each fed back in".
- **Caption (12px `#444`, bottom right):** "illustrative — one email through both helpers".

## Counting What Each Model Can See

**Tags:** `worked example` (blue), `attention mask` (green)

- **One sentence** — take the 8-word email line: "The package arrived late and the box broke"
- **Encoder rule** — every word may look at every word: 8 × 8 = 64 looks in total
- **Decoder rule** — word k may look only at itself and the words before it: k looks each
- **Add them up** — 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 = 36 looks; you can check that on paper
- **Why it helps reading** — guessing the blank in "The package arrived ___ and the box broke" is easy with both sides
- **Why it helps writing** — the decoder never peeks ahead, so it can generate words that don't exist yet

*Example (italic):* Cover the 4th word and guess it: seeing "and the box broke" to the right, "late" or "damaged" is obvious — with only "The package arrived" it could be almost anything.

**Key point:** On the 8-word sentence the encoder gets 64 word-to-word looks and the decoder gets 36 — the missing 28 are the "peeks ahead" a writer must not take.

### Visualization (canvas `c2`, 720×300)

Two 8×8 visibility grids side by side for the sentence "The package arrived late and the box broke": encoder grid fully filled, decoder grid filled only on and below the diagonal.

- **Title (bold 15px, `#1a5276`, top center):** "Who May Look at Whom: 64 Looks vs 36".
- **Sentence line (11px `#6b7280`, centered at y=42):** "words 1–8: The · package · arrived · late · and · the · box · broke".
- **Grid headings (bold 13px, y=60):** blue `#2a78d6` "encoder — sees both sides" centered over the left grid, green `#008300` "decoder — left only" centered over the right grid.
- **Grids:** cell size 22×22; encoder grid top-left at x=110, y=78; decoder grid top-left at x=440, y=78; rows = the word doing the looking (1 at top), columns = the word being looked at (1 at left); 11px `#444` index labels "1"–"8" along each grid's top edge and left edge.
- **Encoder cells:** all 64 cells filled `rgba(42,120,214,0.35)` with 1px white gaps.
- **Decoder cells:** cells with column ≤ row filled `rgba(0,131,0,0.35)`; cells with column > row filled `#e5e9ef` (blocked); same 1px white gaps.
- **Count labels (bold 13px, centered under each grid at y=272):** blue "64 looks" under the encoder grid, green "36 looks" under the decoder grid.
- **Annotation (bold 12px `#d95926`, between the grids near x=360, y=160, two lines):** "grey cells =" / "no peeking ahead".

## Picking the Reader or the Writer

**Tags:** `where it's used` (blue), `task fit` (green)

- **Reading jobs** — sorting emails, rating a review 1–5 stars, finding similar past tickets: one judgment out
- **Writing jobs** — drafting replies, summarizing a long thread, chatting: new text out
- **Encoders shine** — when the answer is a label, a score, or a search embedding over text you already have
- **Decoders shine** — when the answer is itself a piece of text nobody has written yet
- **Rule of thumb** — if the output is shorter than the input and picked from a fixed set, think encoder; if it's open-ended text, think decoder

*Example (italic):* The store's search bar ("waterproof running shoes") runs on encoder embeddings, while its chat window ("help me choose a size") runs on a decoder.

**Key point:** Match the model family to the shape of the output — a judgment about text wants a reader, fresh text wants a writer.

### Visualization (canvas `c3`, 720×300)

Horizontal task-fit chart: six inbox tasks listed down the left, two answer columns ("reads — encoder" and "writes — decoder") on the right, a colored dot marking each task's natural home.

- **Title (bold 15px, `#1a5276`, top center):** "Six Inbox Jobs and Their Natural Model".
- **Column headers (bold 13px, y=70):** blue `#2a78d6` "reads (encoder)" centered at x=480, green `#008300` "writes (decoder)" centered at x=630; light `#e5e9ef` vertical guide lines at x=480 and x=630 from y=80 to y=265.
- **Task rows (12px `#444` labels left-aligned at x=30, rows at y = 100, 132, 164, 196, 228, 260):** "sort emails into refund / complaint / praise", "rate a review 1–5 stars", "find similar past tickets (search)", "draft the reply email", "summarize a long thread", "chat with the customer".
- **Dots:** 8px filled circles — rows 1–3 get a blue `#2a78d6` dot at x=480; rows 4–6 get a green `#008300` dot at x=630; a faint dashed 1px `#6b7280` leader line (dash 3/3) from each label's end to its dot.
- **Annotation (bold 12px `#d95926`, near x=480, y=290):** "output is a label → reader; output is new text → writer".

## But GPT Can Read Too, Right?

**Tags:** `common mistake` (red), `cost vs fit` (orange)

- **True** — a decoder can be prompted to answer "refund, complaint, or praise?" and it will do fine
- **The catch** — it drags a huge write-anything model through a job that needs a one-word answer
- **The bill** — labeling 1M emails (illustrative): a small fine-tuned encoder about $8, a large decoder LLM about $400
- **The payoff** — accuracy 94% vs 95%: roughly 50× the cost for one extra point on this job
- **The mistake** — treating the decoder as strictly "better" because it can also chat; it's bigger, not smarter at labels

*Example (italic):* A team pointed their chat LLM at every incoming email for a one-word label, then found a fine-tuned small encoder matched it within a point at a fiftieth of the cost.

**Common mistake:** Assuming the writer replaces the reader. A decoder can label text, but for high-volume read-only jobs a small encoder usually delivers near-equal accuracy at a fraction of the cost.

### Visualization (canvas `c4`, 720×300)

Two-bar cost comparison for labeling one million emails, with accuracy printed above each bar, making the 50× cost gap for a one-point gain visible.

- **Title (bold 15px, `#1a5276`, top center):** "Labeling 1M Emails: Cost vs Accuracy (illustrative)".
- **Axes:** origin x=90, baseline y=250, plot width 560, plot height 175; y axis = cost in dollars 0 to 450 with 12px `#444` tick labels "$0", "$100", "$200", "$300", "$400" and light `#e5e9ef` gridlines every $100; no x axis title.
- **Bars (width 130):** small encoder bar centered at x=250, height for $8 (about 3px — draw a minimum 4px sliver so it stays visible), fill `rgba(42,120,214,0.55)`, 1px `#2a78d6` border; large decoder LLM bar centered at x=490, height for $400, fill `rgba(217,89,38,0.45)`, 1px `#d95926` border.
- **Bar labels (12px `#444`, centered under baseline at y=270):** "small fine-tuned encoder" and "large decoder LLM".
- **Value labels (bold 13px above each bar):** blue "$8 — 94% accuracy" above the encoder bar, orange "$400 — 95% accuracy" above the decoder bar.
- **Annotation (bold 13px `#d95926`, centered near x=370, y=100):** "50× the cost for +1 point".
- **Caption (12px `#444`, bottom right):** "illustrative numbers — one high-volume labeling job".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all boxes, grid cells, dots, and bar values are the hardcoded literals above (no randomness); the 64/36 look counts follow from the 8-word sentence, and the $8/$400/94%/95% figures are invented and labeled illustrative in the chart captions.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
