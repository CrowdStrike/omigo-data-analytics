# Autocomplete

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Autocomplete

**Subtitle:** The engine proposes the query before you finish typing — saving keystrokes, fixing spelling, and steering what gets searched

## Fourteen Keystrokes In, the Engine Takes Over

**Tags:** `core idea` (blue), `prefix match` (green)

- **The unfinished query** — you type "how to make pi" and pause; the engine already has a guess
- **Prefix match** — every popular past query that starts with "how to make pi" becomes a candidate
- **The menu** — "pizza dough", "pie crust", "pickles" complete the phrase in a dropdown below the box
- **Typing becomes choosing** — picking a suggestion turns free typing into selecting from a menu
- **A proposed query** — the engine wrote the words; the searcher only confirmed them

*Example (italic):* On a phone keyboard, most searchers tap a suggestion long before finishing the sentence they meant to type.

**Key point:** Autocomplete matches your prefix against popular past queries and proposes the rest — the final query is half written by the engine.

### Visualization (canvas `c1`, 720×300)

Three-stage flow: the typed prefix in a search box, an arrow into a "popular past queries" log panel with the prefix-matching rows highlighted, and an arrow into the resulting dropdown menu.

- **Title (bold 15px ink `#1a5276`, top center, y=22):** "A Prefix Meets the Query Log".
- **Search box (x=25, y=70, w=195, h=30, white fill, 2px ink border):** bold 13px `#2c3e50` text "how to make pi" at x=35 baseline y=90, followed by a 1.5px ink cursor bar (vertical line, height 16, positioned via `measureText`). Caption 11px `#6b7280` centered at x=122, y=120: "14 keystrokes, then a pause".
- **Arrows (blue `#2a78d6`, 2px, arrowhead):** x=224→260 at y=85, and x=450→486 at y=85.
- **Log panel (x=266, y=48, w=180, h=222, white fill, 1px `#ccc` border):** header bold 11px `#6b7280` centered at x=356, y=66: "popular past queries". Seven rows at y = 88, 114, 140, 166, 192, 218, 244, text at x=276: non-matching rows 11px `#6b7280` ("weather tomorrow", "gmail login", "running shoes size 10", "how to tie a tie"); matching rows bold 11px green `#008300` on a `rgba(0,131,0,0.10)` band (x=270, w=172, h=18) — "how to make pizza dough" (row 2), "how to make pie crust" (row 4), "how to make pickles" (row 6).
- **Dropdown panel (x=490, y=48, w=205):** search box (h=26, 2px blue border) with 12px `#2c3e50` text "how to make pi"; below it (y=80) a dropdown box (h=84, white fill, 1px `#ccc` border) with three suggestion rows at y = 98, 124, 150, each bold 11px blue on `rgba(42,120,214,0.12)` bands (x=494, w=197, h=20): "how to make pizza dough", "how to make pie crust", "how to make pickles". Caption 11px `#6b7280` centered at x=592, y=185: "the menu the searcher sees".
- **Annotation (bold 12px violet `#4a3aa7`, centered, y=290):** "the engine proposes — the searcher picks".

## Nine Keystrokes Saved, One Typo Forgiven

**Tags:** `worked example` (blue), `spelling rescue` (orange)

- **The arithmetic** — "how to make pi" is 14 keystrokes; "how to make pizza dough" is 23 characters
- **One tap** — the top suggestion delivers the full query: 23 − 14 = 9 keystrokes saved
- **Ranked by crowds** — the menu is ordered by how many people ran each query (counts illustrative)
- **The order** — pizza dough 41,000/day, pie crust 18,000, pickles 9,500, pinwheels 2,100
- **Spelling rescue** — typing "restraunt" still surfaces "restaurant near me", with no error shown

*Example (illustrative, italic):* Adding one more letter — "how to make pin" — silently drops every suggestion that no longer matches.

**Key point:** The menu is a popularity contest over past queries — and it quietly fixes misspellings the searcher never notices making.

### Visualization (canvas `c2`, 720×300)

Left panel: a drawn search box with its ranked dropdown, each suggestion carrying a popularity bar and daily count, plus the keystroke arithmetic. Right panel: the spelling rescue — a misspelled query still reaching the right suggestion.

- **Title (bold 15px ink, top center, y=22):** "The Ranked Menu and the Spelling Rescue".
- **Divider:** dashed `#bdc3c7` vertical line at x=430, y=35 to y=280 (dash 4/3).
- **Left search box (x=30, y=48, w=370, h=26, white fill, 2px ink border):** bold 12px `#2c3e50` "how to make pi" at x=40.
- **Left dropdown rows at y = 100, 130, 160, 190** (data: query | count/day | bar value): "how to make pizza dough" | 41,000 | 41000; "how to make pie crust" | 18,000 | 18000; "how to make pickles" | 9,500 | 9500; "how to make pinwheels" | 2,100 | 2100. Query text bold 11px blue `#2a78d6` at x=40; popularity bar at x=222, height 12, width = count/41000 × 110, fill `rgba(42,120,214,0.12)`, 2px blue border on the top row only (1px for the rest); count label 11px `#6b7280` right of the bar. Top row sits on a `rgba(42,120,214,0.08)` band (x=32, y=88, w=366, h=22).
- **Left annotation (bold 12px green `#008300`, centered at x=215, y=228):** "14 typed + 1 tap → 23 characters · 9 keystrokes saved".
- **Left caption (11px `#6b7280`, centered at x=215, y=252):** "daily counts are illustrative".
- **Right subtitle (bold 12px ink, centered at x=575, y=52):** "the spelling rescue".
- **Right typed box (x=495, y=72, w=160, h=28, white fill, 2px ink border):** bold 12px `#2c3e50` "restraunt" centered; a red `#e74c3c` zigzag underline (5 segments, amplitude 2px) under the word.
- **Right arrow (green, 2px, arrowhead):** down from y=104 to y=136 at x=575.
- **Right suggestion box (x=475, y=140, w=200, h=28, fill `rgba(0,131,0,0.10)`, 2px green border):** bold 12px green "restaurant near me" centered.
- **Right captions (11px `#6b7280`, centered at x=575):** y=196: "no error message — just the right query"; y=214: "the menu forgives the typo".

## The Menu Steers What Gets Searched

**Tags:** `where it's used` (blue), `steering effect` (orange)

- **A different flavor** — an autocompleted query is proposed by the engine and picked by the user
- **Work saved** — fewer keystrokes and fewer typos on every search, especially on phones
- **The steering effect** — suggested phrasings get chosen more, so popular queries grow more popular
- **Rich get richer** — top-slot share can drift up week over week (52% → 71% here, illustrative)
- **Log contamination** — the query log partly reflects the suggester's own menu, not raw demand

*Example (italic):* Two searchers who wanted slightly different things can end up running the identical suggested query.

**Key point:** Autocomplete saves work and kills typos — but the log it produces is no longer a clean record of independent demand.

### Visualization (canvas `c3`, 720×300)

Left panel: a four-box feedback loop (popular query → top suggestion → picked more often → count grows → back). Right panel: two bars showing the top suggestion's share of picks drifting from 52% to 71% (illustrative).

- **Title (bold 15px ink, top center, y=22):** "The Loop That Feeds Itself".
- **Divider:** dashed `#bdc3c7` vertical line at x=380, y=35 to y=280.
- **Loop boxes (w=150, h=32, tint fill + 2px colored border + bold 11px colored centered text):** "popular query" blue at (40, 60); "top suggestion" violet `#4a3aa7` at (210, 60); "picked more often" green at (210, 190); "count grows" yellow `#c98500` at (40, 190). Tints: `rgba(42,120,214,0.12)`, `rgba(74,58,167,0.12)`, `rgba(0,131,0,0.10)`, `rgba(201,133,0,0.12)`.
- **Loop arrows (2px, arrowheads, colored like the source box):** right along y=76 from x=192 to x=208; down at x=285 from y=94 to y=188; left along y=206 from x=208 to x=192 (arrowhead pointing left); up at x=115 from y=188 to y=94.
- **Left caption (11px `#6b7280`, centered at x=200, y=268):** "the menu feeds the counts that build the next menu".
- **Right subtitle (bold 12px ink, centered at x=550, y=52):** "top suggestion's share of picks (illustrative)".
- **Right bars:** baseline 1px `#999` at y=240, x=430 to x=690. Two bars w=70: week 1 = 52% (blue tint fill, 2px blue border, height 104px = 2px per point) at x=460; week 8 = 71% (green tint, 2px green border, height 142px) at x=590. Bold 13px colored "%" labels above each bar; 11px `#444` labels below the baseline: "week 1", "week 8 in top slot".
- **Right caption (11px `#6b7280`, centered at x=550, y=290):** "shares are illustrative".

## Picked Is Not the Same as Wanted

**Tags:** `common mistake` (red), `rule of thumb` (orange)

- **Two roads, one row** — wanted-it-anyway and picked-because-offered write the identical log line
- **No flag** — nothing in the log records whether the words came from the user or from the menu
- **Easy overread** — "41,000 people asked for pizza dough" is partly the menu talking
- **The only test** — removing or reshuffling the suggestion is what reveals independent demand

*Example (italic):* A restaurant that circles one dish on every menu shouldn't marvel that the circled dish sells best.

**Key point:** A suggestion being picked often is not evidence people independently wanted it — the log alone cannot separate the two.

### Visualization (canvas `c4`, 720×300)

Two searcher paths — one who meant to type the full query, one who tapped what the menu offered — converging on one identical query-log row.

- **Title (bold 15px ink, top center, y=22):** "Two Searchers, One Log Row".
- **Searcher chips (w=270, h=30, y=48, white fill, 2px border, bold 12px colored centered text):** left centered at x=180, green border/text: "Alice — meant to type it all along"; right centered at x=540, violet border/text: "Bob — tapped what the menu offered".
- **Action boxes (w=200, h=26, y=108, white fill, 1px `#ccc` border, 11px `#2c3e50` centered text):** left "types all 23 characters"; right "taps the top suggestion". Short colored arrows (2px) from chip bottom (y=78) to box top (y=106) at x=180 and x=540.
- **Converging paths (2px, source color):** polyline from (180,134) down to (180,166) across to (345,166), then arrow down to y=194; polyline from (540,134) down to (540,166) across to (375,166), then arrow down to y=194.
- **Log row (x=185, y=198, w=350, h=36, white fill, 2px ink border):** bold 12px `#2c3e50` centered at y=220: 'query log: "how to make pizza dough" +1'.
- **Annotation (bold 12px yellow `#c98500`, centered, y=262):** "picked-because-offered and wanted-anyway look identical here".
- **Caption (11px `#6b7280`, centered, y=284):** "only removing the suggestion separates the two".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` (no index number); subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Shared helpers `arrowDown`, `arrowRight`, `arrowLeft`, `arrowUp` draw 2px lines with small filled triangle heads.
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` only for the misspelling squiggle in c2.
- **Data:** everything is hardcoded (no randomness): the c1 log rows and three matching completions; the c2 counts 41,000 / 18,000 / 9,500 / 2,100 per day and the keystroke arithmetic 14 + 1 tap → 23 characters (9 saved); the c3 share pair 52% / 71% — all invented numbers labeled "illustrative". Text numbers match chart numbers (41,000/18,000/9,500/2,100 and 14/23/9 in section 2, 52/71 in section 3).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
