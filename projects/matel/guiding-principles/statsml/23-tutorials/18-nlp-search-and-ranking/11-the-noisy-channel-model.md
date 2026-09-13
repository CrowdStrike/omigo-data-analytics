# The Noisy Channel Model

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Noisy Channel Model

**Subtitle:** Your phone's autocorrect is Bayes' rule at work — the word you meant is the one that is both common in the language and easy to mangle into what you typed

## A Text That Says "teh"

**Tags:** `core idea` (blue), `clean in, noisy out` (green), `decoding` (orange)

- **The garbled text** — a friend texts "meet at teh cafe"; no dictionary has "teh", yet you read it instantly
- **Two clues** — you know "the" appears everywhere, and you know thumbs often swap two neighboring letters
- **The channel** — the model's picture: a clean word goes in, fingers add noise, "teh" comes out the other end
- **Running it backwards** — correction asks: which clean word most likely went in, given that "teh" came out?
- **The recipe** — score every candidate as (how common the word is) × (how easily it turns into the typo)

*Example (italic):* You decoded "teh" as "the" without thinking — the noisy channel model just writes down the two clues your brain already used.

**Key point:** The noisy channel model treats a typo as a normal word that got distorted in transit — correcting it means running the distortion backwards.

### Visualization (canvas `c1`, 720×300)

Flow diagram: an intended word passes left-to-right through a channel box and comes out as the typo, with a dashed decoder arrow running back underneath asking which word went in.

- **Title (bold 15px, `#1a5276`, top center):** "A Word's Journey Through the Noisy Channel".
- **Intended box:** rounded rect x=55 y=85 w=135 h=58, 2px blue `#2a78d6` border, fill `rgba(42,120,214,0.10)`; 12px `#6b7280` label "intended word" at its top, bold 17px `#2a78d6` "the" centered inside.
- **Channel box:** rounded rect x=270 y=78 w=180 h=72, 2px orange `#d95926` border, fill `rgba(217,89,38,0.10)`; bold 13px `#d95926` "noisy channel" centered, 12px `#6b7280` second line "thumbs on a phone keyboard".
- **Observed box:** rounded rect x=530 y=85 w=135 h=58, 2px `#e74c3c` border, fill `rgba(231,76,60,0.08)`; 12px `#6b7280` label "observed typo" at its top, bold 17px `#e74c3c` "teh" centered inside.
- **Forward arrows:** solid 3px `#1a5276` arrows with arrowheads, x=190→270 and x=450→530, both at y=114.
- **Decoder arrow:** dashed (dash 6/4) 2.5px violet `#4a3aa7` arrow at y=210 running right-to-left from x=595 to x=125, arrowhead on the left end.
- **Annotation (bold 13px violet `#4a3aa7`, centered at x=360, y=195):** "the decoder runs the channel backwards".
- **Candidate line (12px `#2c3e50`, x=125, y=240):** "candidates: the · ten · tea" followed by 12px `#6b7280` " — score each: how common? how easy a slip?".
- **Caption (12px `#444`, bottom right):** "illustrative — one intended word, one slip, one decoding question".

## Scoring the Candidates: Common × Easy Slip

**Tags:** `worked example` (blue), `multiply two clues` (green)

- **Three candidates** — one keystroke away from "teh" sit "the", "ten", and "tea"
- **The prior** — per million words of everyday text: "the" ≈ 50,000, "ten" ≈ 300, "tea" ≈ 100
- **The channel** — chance of each slip: swapping "he"→"eh" 0.02, hitting h for n 0.005, h for a 0.001
- **Multiply** — "the": 50,000 × 0.02 = 1,000; "ten": 300 × 0.005 = 1.5; "tea": 100 × 0.001 = 0.1
- **The verdict** — "the" outscores runner-up "ten" by roughly 667 to 1, so autocorrect fixes it silently

*Example (italic):* Each score is one multiplication you can redo on paper — the winner is the word that is both common and an easy slip away from "teh".

**Key point:** Score = how common the word is × how easily it becomes the typo — here "the" wins at 1,000 against 1.5 and 0.1.

### Visualization (canvas `c2`, 720×300)

Scoreboard chart: three candidate rows, each with a blue "how common" bar, an orange "easy slip" bar, and a green/grey "score" bar, all with true values printed beside them (bar lengths log-scaled so all three rows stay visible).

- **Title (bold 15px, `#1a5276`, top center):** "Scoring 'teh': How Common × How Easy the Slip".
- **Column headers (bold 12px `#6b7280`, y=72):** "word" at x=40, "how common (per million)" at x=150, "how easy the slip" at x=370, "score" at x=555.
- **Rows at y = 115, 170, 225** (bars 18px tall, centered on the row): word labels at x=40 — bold 14px, "the" in blue `#2a78d6`, "ten" and "tea" in `#6b7280`.
- **Common bars (start x=150, fill `rgba(42,120,214,0.35)`, 1.5px `#2a78d6` border):** widths 141, 74, 60 px; 12px `#444` value labels right of each bar: "50,000", "300", "100".
- **Slip bars (start x=370, fill `rgba(217,89,38,0.35)`, 1.5px `#d95926` border):** widths 69, 51, 30 px; 12px `#444` labels: "0.02", "0.005", "0.001".
- **Score bars (start x=555, widths 130, 55, 26 px):** row 1 fill `rgba(0,131,0,0.40)` with 1.5px `#008300` border, rows 2–3 fill `rgba(107,114,128,0.25)` with 1.5px `#6b7280` border; bold 13px labels right-aligned inside/beside: green "1,000", grey "1.5", "0.1".
- **Annotation (bold 13px green `#008300`, x=470, y=95):** "'the' wins ~667-to-1".
- **Caption (12px `#444`, bottom right):** "bar lengths log-scaled; frequencies and slip rates illustrative".

## One Recipe, Many Channels

**Tags:** `where it's used` (blue), `bayes in disguise` (green), `speech & OCR` (orange)

- **Bayes underneath** — the recipe is P(word | typo) ∝ P(typo | word) × P(word), the textbook rule
- **Speech recognition** — intended words pass through air and a microphone; the audio is the noisy output
- **OCR** — a printed page passes through ink smudges and a scanner; the pixels are the noisy output
- **Early translation** — the first statistical translators treated foreign text as English sent through a channel
- **Two dials** — improve the word frequencies (prior) or the noise table (channel) independently

*Example (italic):* Swap "thumbs on a keyboard" for "a cheap microphone" and the exact same multiplication does speech recognition instead of spelling correction.

**Key point:** Whenever a clean signal is observed only through noise — typing, speech, scanning — the same two-factor recipe decodes it.

### Visualization (canvas `c3`, 720×300)

Three-row diagram: spelling, speech, and OCR each drawn as clean-signal box → channel chip → noisy-output box, sharing one formula banner, showing that only the middle chip changes.

- **Title (bold 15px, `#1a5276`, top center):** "Same Recipe, Different Channels".
- **Formula banner (bold 12px violet `#4a3aa7`, centered at x=360, y=58):** "best guess = (how common the clean signal) × (how easily the channel mangles it)".
- **Rows at y-centers 110, 172, 234;** each row: bold 12px `#1a5276` domain label at x=25 ("spelling", "speech", "OCR"); clean box rounded rect x=110 w=150 h=42 (2px `#2a78d6` border, fill `rgba(42,120,214,0.10)`, 12px `#2c3e50` text); channel chip rounded rect x=320 w=160 h=42 (2px `#d95926` border, fill `rgba(217,89,38,0.12)`, 12px `#d95926` text); noisy box rounded rect x=540 w=150 h=42 (2px `#6b7280` border, fill `rgba(107,114,128,0.10)`, 12px `#2c3e50` text); 2px `#1a5276` arrows with arrowheads between boxes at each row's center.
- **Row contents:** spelling — "the" / "typing thumbs" / "teh"; speech — "meet at three" / "air + microphone" / "audio wave" (draw a tiny 12px sine squiggle under the text); OCR — "invoice" / "scanner + smudges" / "1nvoice".
- **Annotation (bold 12px orange `#d95926`, x=330, y=282):** "only the middle chip changes".
- **Caption (12px `#444`, bottom right):** "illustrative".

## The Word That Fools the Dictionary

**Tags:** `common mistake` (red), `prior breaks ties` (orange)

- **The tempting shortcut** — if the typed word is in the dictionary keep it; else pick the closest spelling
- **The trap** — "wether" is in the dictionary (a farm term for a castrated ram), so the shortcut keeps it
- **One edit away** — "whether" and "weather" are each a single inserted letter from "wether"
- **The prior objects** — per million words: "whether" ≈ 240, "weather" ≈ 210, "wether" ≈ 0.1
- **The channel agrees** — dropping one letter is an easy slip, so both repairs are cheap for the channel

*Example (italic):* Someone types "wether or not" — a dictionary check waves it through, while the prior says a 0.1-per-million word is almost surely a mangled "whether".

**Common mistake:** Trusting the dictionary test alone. "wether" passes it, yet at 0.1 uses per million the prior makes "whether" thousands of times more likely — spelling closeness ties constantly, and word frequency is the tie-breaker the shortcut throws away.

### Visualization (canvas `c4`, 720×300)

Two-panel bar chart for the same three words: left panel shows edits needed (where "wether" scores a perfect 0), right panel shows how often each word is actually used, exposing the trap.

- **Title (bold 15px, `#1a5276`, top center):** "'wether': Passes the Dictionary, Fails the Prior".
- **Left panel:** baseline 2px `#999` at y=245 from x=70 to x=330; bold 13px `#2c3e50` panel label "edits needed to fix" centered at x=200, y=78; three bars 55px wide centered at x = 115, 200, 285 for "wether", "whether", "weather" (12px `#444` word labels below baseline); heights 0, 110, 110 px, fill `rgba(42,120,214,0.35)` with 1.5px `#2a78d6` border; bold 12px `#2a78d6` value labels above: "0", "1", "1" (the "0" sits directly on the baseline).
- **Right panel:** baseline 2px `#999` at y=245 from x=410 to x=680; bold 13px `#2c3e50` panel label "uses per million words" centered at x=545, y=78; three bars 55px wide centered at x = 450, 545, 640, same word order; heights 2, 150, 131 px — "wether" a 2px red `#e74c3c` sliver, "whether" and "weather" fill `rgba(0,131,0,0.35)` with 1.5px `#008300` border; bold 12px value labels above: red "0.1", green "240", green "210".
- **Annotation (bold 13px red `#e74c3c`, x=95, y=115):** two lines: "in the dictionary —" / "still almost surely a typo".
- **Caption (12px `#444`, bottom right):** "frequencies illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all frequencies, slip probabilities, scores, edit counts, and bar widths are the hardcoded literals above (no randomness); word frequencies and slip rates are invented and labeled "illustrative"; the text's numbers (50,000/300/100, 0.02/0.005/0.001, 1,000/1.5/0.1, 240/210/0.1) must match the charts exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
