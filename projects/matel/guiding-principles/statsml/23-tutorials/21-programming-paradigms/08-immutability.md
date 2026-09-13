# Immutability

**Page type:** detail page (tutorial card-sections: h2 per section, two-column table.layout with text left 50% / canvas right 50%)
**HTML title tag:** Immutability

**Subtitle:** The raw orders file is never edited — every cleaning step writes a NEW table, so any step can be redone and any old answer can be explained

## The Raw Orders File Nobody Touches

Tags: `core idea` (blue pill), `running example` (green pill)

- **The rule** — `orders_raw` (10,000 rows) is written once and never edited
- **New tables instead** — cleaning writes `orders_clean`; enriching writes `orders_enriched`
- **Nothing overwritten** — after the run, all three tables sit on disk together
- **Each table has a maker** — one script produced it, from one named input
- **Immutable = unchangeable** — a table, once written, is a fact that stays put

*Example (italic):* Like photo editing that keeps the original: the edit is saved as a new file, never over the negative.

**Key point:** Steps produce new tables instead of changing old ones — so every intermediate answer still exists to be checked.

### Visualization (canvas `c1`, 720×300)

Pipeline flow diagram: chain of new tables, raw locked.

- **Title (bold 16px ink `#1a5276`, top center):** "Every Step Writes a NEW Table"
- **Four table boxes** in a row at y=100, each 120×62, fill `#f8f9fa`, colored stroke; bold 12px colored name line plus 11px dark row-count line:

| x | name | rows | stroke color | padlock |
|---|------|------|-------------|---------|
| 30 | orders_raw | 10,000 rows | blue `#2a78d6` | yes (drawn above box) |
| 220 | orders_clean | 9,600 rows | green `#008300` | no |
| 410 | orders_enriched | 9,600 rows | violet `#4a3aa7` | no |
| 600 | daily_report | 12 rows | orange `#d95926` | no |

- **Padlock glyph** above orders_raw (blue stroke): 14×10 rect plus semicircle arc.
- **Gray arrows** between adjacent boxes, each labeled above midpoint in bold 11px ink: "clean.py", "enrich.py", "report.py".
- **Annotation under raw box (bold blue 12px, centered x=90, y=200):** "written once, never edited"
- **Bottom annotations (centered):** bold magenta `#d55181` 14px at y=240: "after the run, ALL four tables still exist on disk"; gray 12px at y=262: "each arrow is a script; each box is a fact that stays put"

## Tuesday's Bug, Fixed by Rerunning

Tags: `worked example` (green pill)

- **Monday** — pipeline runs; the report ships
- **Tuesday** — order #17 shows price $4,999 — the register logged cents, not dollars
- **The fix** — edit `clean.py` to divide cents by 100: 4999 becomes $49.99
- **The redo** — rerun clean, enrich, report; each writes a fresh table
- **Raw untouched** — `orders_raw` still says 4999, proving what the register sent

*Example (italic):* Redo = rerun the makers. No hand-editing of data, no hunting for which rows were already fixed.

**Key point:** Because raw is intact, the fix is one code change plus a rerun — and you can prove the bug was the register's, not yours.

Below the canvas in the viz column, a `pre.code` block (verbatim):

```
# never: UPDATE orders SET price = price / 100
# instead: fix the maker, rerun it
def clean(raw):
    df = raw.dropna(subset=["price"])
    df["price"] = df["price"] / 100   # the fix
    return df

orders_clean = clean(orders_raw)      # new table
```

### Visualization (canvas `c2`, 720×300)

Split diagram: the Tuesday fix — change the maker, rerun downstream.

- **Title (bold 16px ink, top center):** "Fix the Maker, Rerun the Chain — Raw Never Moves"
- **Vertical dashed divider** at x=270 (light gray `#e5e9ef`, dash 4/3) from y=40 to bottom.
- **Left half — order #17 before/after:** bold ink 13px header "order #17" centered at (145,56).
  - Box (45,70) 200×44, fill `rgba(42,120,214,0.10)` stroke blue: bold blue 12px "orders_raw", dark 12px "price = 4999 (cents)".
  - Green downward arrow (145,118)→(145,142) labeled bold green 11px "clean.py v2: / 100" to its right.
  - Box (45,146) 200×44, fill `rgba(0,131,0,0.10)` stroke green: bold green 12px "orders_clean (new)", dark 12px "price = $49.99".
  - Gray 12px centered two lines at (145, 230/248): "raw still says 4999 —" / "proof of what the register sent".
- **Right half — Monday vs Tuesday runs:** bold ink 13px header "two runs, side by side" centered at (495,56).
  - Two run rows: "Mon" (gray `#6b7280`, y=80, tag v1) and "Tue" (green, y=160, tag v2). Each row: three boxes 86×30 starting x=340, step 120 — labels "clean v1"/"clean v2", "enrich", "report" — Mon fill `#f4f6f8`, Tue fill `rgba(0,131,0,0.10)`, stroked in the run color; light-gray arrows between boxes.
  - Notes in 11px under each row at x=340: Mon: "report says $4,999 — kept as evidence"; Tue: "report says $49.99 — the shipped fix".
- **Bottom-right annotation (bold magenta 13px, centered at 495,248):** "redo any step by rerunning it — no data was hand-edited"

## Mutation Is How Pipelines Lose Their Memory

Tags: `failure mode` (red pill), `why it matters` (blue pill)

- **The mutable version** — an `UPDATE` fixes prices in place, in the one and only table
- **History gone** — "what did raw say before the fix?" now has no answer
- **Unexplainable reports** — Monday's report used values that no longer exist anywhere
- **Double-fix risk** — run the UPDATE twice and $49.99 becomes $0.4999, silently
- **Immutable debugging** — diff `orders_clean` against `orders_raw` and see exactly what cleaning did

*Example (italic):* Auditor: "why does Monday's report say $4,999?" — with mutation, the evidence was overwritten on Tuesday.

**Key point:** Mutation destroys the input that explains the output — an edited-in-place pipeline cannot account for its own old answers.

### Visualization (canvas `c3`, 720×300)

Two-lane comparison: mutable timeline vs immutable versions.

- **Title (bold 16px ink, top center):** "What Did the Data Say on Monday?"
- **Horizontal dashed divider** at y=152 from x=40 to x=680 (light gray, dash 4/3).
- **Top lane — mutable:** bold red `#e74c3c` 13px label at (40,52): "mutable: UPDATE in place". Three boxes 100×40 at x=260/410/560, y=64 for Mon/Tue/Wed: first two fill `#f4f6f8` stroke gray with 11px note "Mon: overwritten" / "Tue: overwritten"; Wed box fill `rgba(231,76,60,0.10)` stroke red, note "Wed: only survivor". Each box titled bold 11px dark "orders". Red arrows between boxes. Bold red 12px annotation at (260,132): "Monday's values exist nowhere — the question has no answer"
- **Bottom lane — immutable:** bold green 13px label at (40,182): "immutable: new table per run". Three green boxes 100×40 at same x positions, y=194, fill `rgba(0,131,0,0.10)`: bold green "orders_v1" / "orders_v2" / "orders_v3" with 11px dark note "Mon: still on disk", "Tue: still on disk", "Wed: still on disk". Violet bracket under v1–v2 (from x=310 to x=460 at y≈252) with bold violet 11px caption at (385,270): "diff v1 vs v2 shows exactly what the fix changed". Bold magenta 12px two lines at left (40, 210/226): "any old report can still" / "be explained"

## "So Mistakes Are Permanent?" — No, Corrections Are Appended

Tags: `common mistake` (red pill)

- **The confusion** — "if tables can't change, errors can never be fixed"
- **The reality** — you fix by writing a corrected NEW version, keeping the old one
- **Accountants knew first** — a ledger entry is never erased; a correcting entry is added
- **Bank statements too** — a wrong charge gets a refund line, not a rewritten history
- **Storage worry** — keeping versions costs disk, which is cheap next to an unexplainable report

*Example (italic):* A $50 overcharge is fixed by a −$50 line below it — both lines stay visible forever.

**Common confusion (key-point callout):** Immutability doesn't forbid fixing — it forbids fixing in a way that hides that anything was ever wrong.

### Visualization (canvas `c4`, 720×300)

Side-by-side ledgers: erase-and-rewrite vs append-a-correction.

- **Title (bold 16px ink, top center):** "Fixing a $50 Overcharge, Two Ways"
- **Vertical dashed divider** at x=360 (light gray, dash 4/3).
- **Left ledger — erase and rewrite:** bold red 13px header centered at (185,52): "erase and rewrite". White box (70,66) 230×118 stroked red. Three monospace 12px lines at x=88 (y=92, step 26): "coffee beans      $12", "oven repair      $175", "flour order      $250". The third line highlighted with `rgba(231,76,60,0.12)` rect (84,130) 200×20. Bold red 11px note at (88,168): "was $300? $280? no trace". Gray 12px centered two lines at (185, 226/244): "the error and the fix are both invisible —" / "and a second \"fix\" can strike twice".
- **Right ledger — append a correction:** bold green 13px header centered at (535,52): "append a correction". White box (420,66) 230×144 stroked green. Four monospace 12px lines at x=438 (y=92, step 26): "coffee beans      $12" (dark), "oven repair      $175" (dark), "flour order      $300" (orange `#d95926`), "correction        -$50" (green). Bold green 11px note at (438,196): "net $250 — and the whole story is visible". Bold magenta 13px centered at (535,232): "fix by adding, never by erasing"; gray 12px at (535,252): "what accountants have done for centuries".

## Regeneration instructions

- **Template/layout:** tutorials detail page. `<h1>` + `.subtitle`, then four `.card-section` blocks each with `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` (width 100%, border-collapse, td padding 12px, vertical-align top; `td.text-col` 50% / `td.viz-col` 50%).
- **Text column structure:** `.tags` pill row first (pills 0.72rem bold, 2px 10px padding, 10px radius: blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); then a `<ul>` (0.92rem) of one-line bullets each opening with `<b>` in `#1a5276`; one italic `.example` line (0.9rem `#555`); one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem).
- **Code blocks:** `pre.code` — background `#f8f9fa`, left border 3px solid `#1a5276`, monospace 0.78rem; inline `code` uses `#f4f6f8` background monospace.
- **Page CSS:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. Canvases have `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** declare intrinsic `width`/`height` attributes per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; shared `box()`, `arrow()`, and `padlock()` drawing helpers. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- In regenerated HTML, any card/page links use `.html` extensions.
