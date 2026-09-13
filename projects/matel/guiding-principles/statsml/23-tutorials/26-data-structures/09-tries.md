# Tries

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Tries

**Subtitle:** A trie stores words letter by letter along shared branches, so finding every word that starts with "ca" means walking two steps and reading what hangs below — no matter how big the dictionary

## Typing "ca" on a Phone Keyboard

**Tags:** `core idea` (blue), `shared prefixes` (green), `autocomplete` (orange)

- **The keyboard** — a phone keyboard suggests words as you type; type "ca" and up pop cap, car, cat
- **One tree** — the dictionary is stored as a tree where each step down adds one letter to the word
- **Shared start** — cap, cape, car, cart, cat all begin "ca", so they share one c→a path in the tree
- **Walk, don't search** — typing "ca" means walking two steps down; the suggestions hang below that spot
- **End marks** — some tree spots are marked "a word ends here", so "car" counts but "ca" alone does not

*Example (italic):* With the six words cap, cape, car, cart, cat, tea in the tree, typing "ca" lands on one node with all 5 ca-words below it — tea sits on a separate branch and is never touched.

**Key point:** A trie is a tree of letters where every word is a path from the top, and words that start the same share the same first steps.

### Visualization (canvas `c1`, 720×300)

Node-link tree diagram of the six-word trie (cap, cape, car, cart, cat, tea), with the c→a path highlighted as "what typing 'ca' walks" and word-end nodes filled green.

- **Title (bold 15px, `#1a5276`, top center):** "Six Words, One Tree — typing 'ca' walks two steps".
- **Nodes:** 22px-diameter circles with bold 13px centered letter labels; root at (360, 60) drawn as a 22px circle labeled "•" in `#6b7280`; level y-coordinates 60, 105, 150, 195, 240.
- **Node positions:** c (260, 105), t (500, 105); a (260, 150), e (500, 150); p (170, 195), r (260, 195), t (350, 195), a (500, 195); e (170, 240), t (260, 240).
- **Edges:** 2px `#6b7280` lines root→c, root→t, c→a, a→p, a→r, a→t, p→e, r→t, t→e (right branch), e→a (right branch).
- **Word-end nodes (cap's p, cape's e, car's r, cart's t, cat's t, tea's a):** fill `rgba(0,131,0,0.18)`, 2.5px green `#008300` outline; all other nodes white fill, 2px `#2a78d6` outline, letters in `#1a5276`.
- **Typed path:** edges root→c and c→a redrawn 4px orange `#d95926`; the c and a circles get a 3px orange outline; bold 12px orange label left of the path (x≈95, y≈125), two lines: "you typed" / "'c' then 'a'".
- **Annotation (bold 12px green `#008300`, near x=395, y=250):** "all 5 'ca…' words hang below this node".
- **Caption (12px `#444`, bottom right):** "six-word toy dictionary — illustrative".

## Building the Tree from Six Words

**Tags:** `worked example` (blue), `insert by hand` (green)

- **Start empty** — insert "cap": no letters exist yet, so it creates 3 new circles: c, a, p
- **Reuse the path** — insert "cape": c, a, p are already there, so only 1 new circle (e) is added
- **Keep going** — "car" adds 1 (r), "cart" adds 1 (t), "cat" adds 1 (t): five ca-words, seven circles
- **New branch** — "tea" shares nothing, so it adds 3 fresh circles: t, e, a
- **The tally** — the six words contain 20 letters in total, but the finished trie holds only 10 circles

*Example (italic):* Inserting cap, cape, car, cart, cat, tea in order creates 3, 1, 1, 1, 1, 3 new circles — 20 typed letters compressed into 10 stored nodes because "ca" is stored once, not five times.

**Key point:** Inserting a word means walking its letters and only creating the circles that are missing — shared prefixes are stored exactly once.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: after each of the six insertions, cumulative letters typed (blue) vs total nodes in the trie (green), showing the two counts pulling apart as prefixes get reused.

- **Title (bold 15px, `#1a5276`, top center):** "20 Letters Typed, Only 10 Nodes Stored".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; y = count 0 to 22 with light `#e5e9ef` gridlines at 5, 10, 15, 20 and 12px `#444` labels; x = six groups labeled below in 12px `#444`: "+cap", "+cape", "+car", "+cart", "+cat", "+tea".
- **Blue bars (letters typed so far):** fill `rgba(42,120,214,0.55)`, values `[3, 7, 10, 14, 17, 20]`.
- **Green bars (nodes in trie):** fill `rgba(0,131,0,0.55)`, values `[3, 4, 5, 6, 7, 10]`; each pair 34px-wide bars, 6px apart, groups evenly spaced.
- **Value labels:** bold 12px above every bar, blue `#2a78d6` over blue bars, green `#008300` over green bars.
- **Legend (12px, top left inside plot):** blue swatch "letters typed", green swatch "nodes stored".
- **Annotation (bold 13px orange `#d95926`, near x=430, y=85):** "gap = letters saved by sharing 'ca'".
- **Caption (12px `#444`, bottom right):** "insertion order cap → tea — illustrative".

## Why Autocomplete Stays Fast at a Million Words

**Tags:** `where it's used` (blue), `speed` (green), `rule of thumb` (orange)

- **The promise** — looking up "cart" takes 4 steps, one per letter, whether the trie holds 6 words or a million
- **Sorted list** — binary search on a sorted list needs about 10 steps at 1,000 words and 20 at 1,000,000
- **Word length wins** — trie cost depends on the word's length, not on how many words are stored
- **Where you meet it** — phone keyboards, search-box suggestions, spell checkers, and IP routing tables
- **The trade** — tries spend extra memory on child pointers to buy that flat, length-only lookup time

*Example (italic):* At 1,000,000 words, binary search needs about 20 comparisons to find "cart"; the trie still walks c, a, r, t — 4 steps.

**Key point:** Trie lookup time scales with the length of the word you type, not the size of the dictionary — that is why suggestions keep up with your fingers.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart across four dictionary sizes: binary-search steps (orange) growing with size vs trie steps for "cart" (blue) flat at 4.

- **Title (bold 15px, `#1a5276`, top center):** "Looking Up 'cart': 4 Steps No Matter the Dictionary".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; y = steps 0 to 22, light `#e5e9ef` gridlines at 5, 10, 15, 20 with 12px `#444` labels; x = four groups labeled below in 12px `#444`: "1,000 words", "10,000", "100,000", "1,000,000".
- **Orange bars (binary search on a sorted list):** fill `rgba(217,89,38,0.55)`, values `[10, 14, 17, 20]`, bold 12px orange `#d95926` value labels above.
- **Blue bars (trie, word 'cart'):** fill `rgba(42,120,214,0.55)`, values `[4, 4, 4, 4]`, bold 12px blue `#2a78d6` value labels above; each pair 40px-wide bars, 8px apart.
- **Flat guide:** horizontal dashed blue `#2a78d6` (dash 4/3) line at steps=4 across the plot.
- **Legend (12px, top left inside plot):** orange swatch "binary search steps", blue swatch "trie steps".
- **Annotation (bold 13px blue `#2a78d6`, near x=420, y=195):** "always 4 — one step per letter of 'cart'".
- **Caption (12px `#444`, bottom right):** "binary search ≈ log2(size), rounded up — illustrative".

## A Trie Is Not a Hash Map

**Tags:** `common mistake` (red), `prefix queries` (orange)

- **Hash map's game** — a hash map answers "is 'cart' a word?" in one jump, and it is great at exactly that
- **The wrong ask** — ask it "which words start with 'ca'?" and it must check every single stored word
- **Trie's game** — the trie walks 2 steps to the "ca" node, then just reads the 5 words hanging below
- **The mistake** — reaching for a hash map when the product feature is a prefix search, not an exact match
- **Rule of thumb** — exact lookups only: hash map; anything "starts with…": trie

*Example (italic):* In a 100,000-word dictionary, the prefix query "ca" makes a hash map examine all 100,000 keys, while the trie does 2 steps of walking plus reading out the matches under that node.

**Common mistake:** Treating a trie as a slower hash map. They answer different questions — the trie's whole point is that words near each other in spelling sit near each other in the tree.

### Visualization (canvas `c4`, 720×300)

Two-row horizontal bar chart comparing work done for the prefix query "ca" on a 100,000-word dictionary: hash map scanning everything vs trie walking two steps.

- **Title (bold 15px, `#1a5276`, top center):** "Prefix Query 'ca' on 100,000 Words: Work Done".
- **Layout:** horizontal 2px `#999` baseline at x=230 from y=90 to y=230; bars grow rightward, max bar width 430px; left-aligned 12px `#444` row labels at x=20.
- **Row 1 (y=120), label:** "hash map — checks every key"; bar fill `rgba(231,76,60,0.45)` with 2px `#e74c3c` outline, full 430px wide, 34px tall; bold 13px red `#e74c3c` label inside near its right end: "100,000 words examined".
- **Row 2 (y=190), label:** "trie — walks the typed letters"; bar fill `rgba(0,131,0,0.45)` with 2px `#008300` outline, 14px wide, 34px tall; bold 13px green `#008300` label to its right: "2 steps, then read the matches".
- **Annotation (bold 13px violet `#4a3aa7`, near x=330, y=260):** "the trie never visits words that don't start with 'ca'".
- **Caption (12px `#444`, bottom right):** "bar widths not to scale — illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all node positions, bar values, and step counts are the hardcoded numbers above (no randomness); the six-word dictionary is cap, cape, car, cart, cat, tea everywhere on the page, and the c2 tallies (letters `[3,7,10,14,17,20]`, nodes `[3,4,5,6,7,10]`) must match the text's per-word counts (3, 1, 1, 1, 1, 3 new nodes).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
