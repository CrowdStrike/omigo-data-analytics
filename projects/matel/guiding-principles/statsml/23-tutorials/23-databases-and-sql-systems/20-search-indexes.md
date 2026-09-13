# Search Indexes

**Page type:** detail page (tutorial layout: `.card-section` blocks, each an h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Search Indexes

**Subtitle:** Instead of reading a million product descriptions to find "wireless headphones", flip the data: keep a list, per word, of which documents contain it

## Two Words, a Million Descriptions

**Tags:** `core idea` (blue), `running example` (green)

- **The setup** — a shop with 1M product descriptions; a user types "wireless headphones"
- **The slow way** — open every description and look for both words: 1M reads
- **The flip** — an inverted index is a lookup from word → list of documents containing it
- **Like a book index** — you don't reread the book to find "regression"; you check the back
- **Two lookups** — fetch the list for `wireless`, the list for `headphones`, intersect them

*Example (italic):* "Inverted" because the stored direction flips: not doc→words, but word→docs.

**Key point:** The index answers "which documents contain this word?" instantly because that exact question was precomputed at write time.

### Visualization (canvas `c1`, 720×300)

Word-to-document arrow diagram: two query-word boxes on the left with posting-list arrows into four document boxes on the right.

- **Title (bold 15px, `#1a5276`, top center):** "The Index Points From Words to Documents".
- **Word boxes** (filled 145×34 rectangles at x 90, white bold 13px monospace label centered, with a bold 11px posting-list label below in the word's color):
  - `wireless` at y 100, blue `#2a78d6`, "→ [1, 3, 4]".
  - `headphones` at y 190, magenta `#d55181`, "→ [1, 2, 4]".
- **Doc boxes** (200×32 at x 480; y 60/115/170/225 for docs 1–4). Docs 1 and 4 (both words): fill `rgba(0,131,0,0.14)`, stroke green `#008300` width 2.5, bold green 12px label; docs 2 and 3: fill `#f4f6f8`, stroke `#b9c2cc`, grey label. Labels: "doc 1  ✓✓ both words", "doc 2  headphones only", "doc 3  wireless only", "doc 4  ✓✓ both words".
- **Arrows:** straight lines (width 2, alpha 0.55) from each word box (x 235) to each doc in its posting list, colored by word (blue for wireless, magenta for headphones).
- **Takeaway** (green bold 13px bottom center): "docs on BOTH lists win: {1, 4} — found without opening any description".

## Building the Index From Four Descriptions

**Tags:** `worked example` (green)

- **Tokenize** — split each description into lowercase words, drop "with", "for", "the"
- **Doc 1** — "Wireless headphones with noise canceling" → wireless, headphones, noise, canceling
- **Post** — add the doc's id to each of its words' lists (a "posting list")
- **Query** — `wireless → [1, 3, 4]` and `headphones → [1, 2, 4]`
- **Intersect** — ids in both lists: {1, 4} — the two matching products

*Example (italic):* Doc 3 has "wireless" but no "headphones"; doc 2 the reverse — the intersection drops both.

**Key point:** Searching never reopens a document — the answer is assembled entirely from the little lists built when documents were added.

### Visualization (canvas `c2`, 720×300)

Tokenize-post-intersect diagram: four description rows on the left, posting-list intersection on the right. Below the canvas the viz column also holds a `pre.payload` code block (verbatim):

```
// the inverted index for the 4 toy descriptions  (illustrative)
{
  "wireless":   [1, 3, 4],
  "headphones": [1, 2, 4],
  "noise":      [1],
  "canceling":  [1],
  "wired":      [2],  "studio":  [2],  "quality": [2],
  "speaker":    [3],  "kitchen": [3],
  "earbuds":    [4],  "alternative": [4]
}
// query "wireless headphones" = intersect two lists → {1, 4}
```

- **Title (bold 15px, `#1a5276`, top center):** "Tokenize, Post, Intersect".
- **Description rows** (400×24 boxes at x 40 starting y 52, 30px apart; monospace 11px text, doc id bold `#1a5276`): doc 1 '"Wireless headphones with noise canceling"' (full match), doc 2 '"Wired headphones, studio quality"', doc 3 '"Wireless speaker for the kitchen"', doc 4 '"Wireless earbuds — headphones alternative"' (full match). Full matches: fill `rgba(0,131,0,0.10)`, green stroke width 2; others white with `#b9c2cc` stroke.
- **Footnote** (grey 11px under rows): "tokenizer: lowercase, split, drop stop-words (with, for, the)".
- **Right column** (x 480, bold 12px monospace): blue "wireless" with `[1, 3, 4]` in `#444`; magenta "headphones" with `[1, 2, 4]`; horizontal rule; green "intersect" with bold 14px "{1, 4}".
- **Orange note** (bold 12px, three lines): "walk two sorted lists" / "once each — cheap even" / "when lists hold thousands".
- **Takeaway** (green bold 13px bottom center): "4 docs in, 11 posting lists out — the query only ever touches 2 of them".

## Ranking: Which Match Comes First

**Tags:** `core idea` (blue), `worked example` (green)

- **Matching isn't enough** — thousands of docs may contain both words; order matters
- **Rare words weigh more** — `wireless` is in 200,000 of 1M docs; `headphones` in 40,000
- **So a hit on "headphones"** says 5x more about relevance than a hit on "wireless"
- **Repeats help a little** — a doc saying "headphones" 3 times outranks one saying it once
- **Length norms** — one mention in a 10-word title beats one in a 500-word spec sheet

*Example (italic):* This weighting scheme — rare-word boost times in-document count — is the classic TF-IDF idea.

**Key point:** A search engine returns a scored ranking, not a row set — the score is why the best match is on top instead of buried at position 4,000.

### Visualization (canvas `c3`, 720×300)

Split panel: word-frequency bars on the left, scored-document bars on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Rare Words Carry the Score (illustrative weights)".
- **Divider:** vertical dashed grey line (`#bdc3c7`, dash 4/3) at x 330 from y 38 to 262.
- **Left panel** — header bold 12px `#1a5276`: "docs containing the word (of 1M)". Horizontal bars (start x 40, width = value/200,000 × 220px, height 20, alpha 0.6):
  - "wireless": 200,000 → "200k docs", blue `#2a78d6`, note bold 11px "common → weight 1".
  - "headphones": 40,000 → "40k docs", magenta `#d55181`, note "rarer → weight 5".
  - Footnote grey 11px, two lines: "the fewer docs a word appears in," / "the more a match on it means".
- **Right panel** — header bold 12px `#1a5276`: "score = wireless hits ×1 + headphones hits ×5". Horizontal bars (start x 360, width = score/12 × 260px, height 20, alpha 0.7, bold score label at bar end):
  - 'doc A: "headphones" ×2, "wireless" ×1' — score 11, green `#008300`.
  - 'doc B: "headphones" ×1, "wireless" ×1' — score 6, aqua `#199e70`.
  - 'doc C: "wireless" ×4, no "headphones"' — score 4, gold `#c98500`.
  - Note green bold 12px: "doc A tops the list: 2×5 + 1×1 = 11".
- **Takeaway** (orange `#d95926` bold 13px bottom center): "four mentions of a common word still lose to two of a rare one".

## Why LIKE '%term%' Can't Compete

**Tags:** `common mistake` (red), `watch out` (orange)

- **The SQL reflex** — `WHERE descr LIKE '%wireless%'` looks like search but isn't
- **Full scan** — a leading `%` defeats normal B-tree indexes: all 1M rows get read, every time
- **Substring, not word** — `'%wire%'` happily matches "wired" and "haywire"
- **No ranking** — LIKE returns an unordered pile; the best match is anywhere in it
- **No language smarts** — "headphone" misses "headphones"; a stemming analyzer folds both to one

*Example (italic):* The scan repeats its 1M reads on every query; the index paid its cost once, at write time.

**Common mistake:** Shipping LIKE as the search box. It sort-of works on 10,000 rows, then collapses at scale — and it never ranked anything to begin with.

### Visualization (canvas `c4`, 720×300)

Two horizontal bars comparing work per search, plus a three-item miss list.

- **Title (bold 15px, `#1a5276`, top center):** "One Query, 1M Descriptions: work done per search (illustrative)".
- **Bars** (start x 60, plot width 600, top 66, row height 72, bar height 26, fill alpha 0.65 with solid stroke; label bold 13px monospace, sub-label grey 11px):
  - "LIKE '%wireless%'" — sub "reads all 1,000,000 rows, every query" — full-width 600px bar, red `#e74c3c`, value label "1,000,000 rows · ~30 s" below the bar.
  - "inverted index" — sub "reads 2 posting lists" — bar proportional to 240,000 on the same scale as the LIKE bar, green `#008300`, value label "2 lists · ~20 ms" beside the bar.
- **Miss list** (starting y 218, 20px apart; red bold "✗" then `#444` 12px text): "no ranking — results in arbitrary order"; "substring matches: '%wire%' hits \"haywire\""; 'no stemming: "headphone" misses "headphones"'.
- **Takeaway** (green bold 13px bottom center): "the index did its heavy work once, at write time — LIKE redoes it on every keystroke".

## Regeneration instructions

- **Template:** tutorials topic-page layout (see `tutorials/CLAUDE.md`). h1 (no index number) with 2px `#2980b9` bottom border, `.subtitle` grey one-liner, then four `.card-section` blocks: each has an `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) and a `table.layout` with `td.text-col` (50%) and `td.viz-col` (50%). Section 2's viz cell contains a canvas plus a `pre.payload` block (background `#f8f9fa`, left border `3px solid #1a5276`, ui-monospace 0.78em, padding 10px).
- **Text column structure:** `.tags` pill row, one-line `<ul>` bullets each opening with `<b>bold term</b>` (`#1a5276`), one italic `.example` paragraph, one `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem).
- **Tag pills:** inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors: blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`. Inline `code` in ui-monospace on `#f4f6f8` background.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; canvases `width:100%`, `1px solid #e0e0e0` border, radius 4px.
- **Canvas:** each declared 720×300 intrinsic; scaled by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow/gold `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
