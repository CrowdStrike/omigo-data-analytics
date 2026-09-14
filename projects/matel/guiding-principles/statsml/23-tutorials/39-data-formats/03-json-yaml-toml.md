# JSON, YAML, TOML

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** JSON, YAML, TOML

**Subtitle:** Three ways of writing the same maps-and-lists data on disk — the structure is identical, only the spelling differs (and one spelling silently turns Norway into false)

## One Coffee App Config, Written Three Ways

**Tags:** `core idea` (blue), `same tree` (green), `three syntaxes` (orange)

- **The app** — a coffee shop's ordering app needs a config: name, port, debug flag, ship-to countries
- **The data** — name "brew-hub", port 8080, debug false, countries gb / fr / no — one small tree
- **JSON** — braces, double quotes on every key, commas between entries: 95 characters
- **YAML** — colons and indentation, almost no punctuation: 62 characters
- **TOML** — flat `key = "value"` lines like a classic INI file: 72 characters
- **Same tree** — all three parse to the identical map; only the spelling on disk differs

*Example (italic):* The ops team can store the same four settings as app.json, app.yaml, or app.toml — after parsing, the program cannot tell which file it read.

**Key point:** JSON, YAML and TOML are three spellings of one data model — maps, lists and scalars. Choosing between them is choosing a syntax, not a structure.

### Visualization (canvas `c1`, 720×300)

Three file boxes showing the same config verbatim in each format, arrows converging on one parsed-tree box below.

- **Title (bold 15px, `#1a5276`, top center):** "Three Files on Disk, One Tree in Memory".
- **File boxes (rounded 8px, 215px wide, 128px tall, y=48, at x = 15, 252, 489):** fill `rgba(42,120,214,0.08)`, 1px border in the header color; bold 13px header at box top: "JSON" blue `#2a78d6`, "YAML" green `#008300`, "TOML" violet `#4a3aa7`.
- **Box contents (11px monospace `#2c3e50`, one line per row):**
  - JSON: `{`, `  "name": "brew-hub",`, `  "port": 8080,`, `  "debug": false,`, `  "countries":`, `    ["gb","fr","no"]`, `}`
  - YAML: `name: brew-hub`, `port: 8080`, `debug: false`, `countries: [gb, fr, no]`
  - TOML: `name = "brew-hub"`, `port = 8080`, `debug = false`, `countries = ["gb","fr","no"]`
- **Size labels (12px `#6b7280`, centered under each box at y=192):** "95 chars", "62 chars", "72 chars".
- **Arrows:** 2px `#6b7280` lines from each box bottom converging to the tree box top.
- **Tree box (rounded 8px, 400px wide, 46px tall, centered at y=228):** fill `rgba(0,131,0,0.10)`, 12px `#2c3e50` text `map{ name, port, debug, countries[3] }` with bold 13px green `#008300` header "parsed result — identical".
- **Annotation (bold 13px ink `#1a5276`, bottom left near x=20, y=292):** "same data model: maps, lists, scalars".
- **Caption (12px `#444`, bottom right):** "character counts exact for the snippets shown".

## Strict, Loose, and In Between

**Tags:** `worked example` (blue), `syntax rules` (green), `strictness` (orange)

- **JSON is strict** — no comments, no trailing commas; one stray comma after `"no"]` is a parse error
- **JSON quotes everything** — every key and every string must be double-quoted: `"port": 8080`
- **YAML nests by indent** — two spaces replace braces; a tab used for indentation is a parse error
- **YAML guesses types** — unquoted `false`, `8080`, `3.14` become bool, int, float automatically
- **TOML is explicit** — `[server]` headers group keys; the value's spelling declares its type
- **Hand-check** — add a trailing comma to the countries list: JSON fails, YAML and TOML both accept it

*Example (italic):* Writing `["gb", "fr", "no",]` crashes the JSON parser at that comma, while the identical list is legal YAML and legal TOML.

**Key point:** JSON's strictness is deliberate — fewer features means every parser on earth reads it identically; YAML trades that guarantee for human comfort.

### Visualization (canvas `c2`, 720×300)

Feature matrix: four syntax features as rows, the three formats as columns, allowed/forbidden marks in each cell.

- **Title (bold 15px, `#1a5276`, top center):** "What Each Syntax Lets You Write".
- **Grid:** row labels left-aligned 13px `#2c3e50` at x=30, rows at y = 90, 138, 186, 234; column headers bold 13px at y=58 — "JSON" blue `#2a78d6` x=360, "YAML" green `#008300` x=490, "TOML" violet `#4a3aa7` x=620 (centered); gridlines 1px `#e5e9ef` between rows.
- **Rows and marks (bold 16px: allowed = green `#008300` "✓", forbidden = orange `#d95926` "✗"):**
  - "comments (#)": JSON ✗, YAML ✓, TOML ✓
  - "trailing comma in a list": JSON ✗, YAML ✓, TOML ✓
  - "unquoted strings": JSON ✗, YAML ✓, TOML ✗
  - "multi-line strings": JSON ✗, YAML ✓, TOML ✓
- **Annotation (bold 13px ink `#1a5276`, centered near y=274):** "JSON forbids all four — that strictness is why machines trust it".
- **Caption (12px `#444`, bottom right):** "YAML rules per the common 1.1/1.2 loaders".

## Where Each Format Ended Up

**Tags:** `where it's used` (blue), `configs & APIs` (green)

- **APIs speak JSON** — every browser ships a JSON parser; web APIs send and receive it by default
- **CI speaks YAML** — GitHub Actions, GitLab CI and Kubernetes manifests are all YAML files
- **Packaging picked TOML** — Python's pyproject.toml and Rust's Cargo.toml are TOML by design
- **Humans vs machines** — JSON is mostly written by programs; YAML and TOML by people in editors
- **Rule of thumb** — data crossing a network: JSON; deep nested infra config: YAML; small hand-edited config: TOML

*Example (italic):* The coffee app's API responses are JSON, its deploy pipeline is a YAML file, and its build settings live in a TOML file — one project, all three formats.

**Key point:** The formats stopped competing — JSON won machine-to-machine exchange, YAML won infrastructure config, TOML won small hand-edited config files.

### Visualization (canvas `c3`, 720×300)

Grouped horizontal bar chart: three usage niches as rows, each with a JSON / YAML / TOML bar showing that format's share of the niche.

- **Title (bold 15px, `#1a5276`, top center):** "Each Format Owns One Home Turf".
- **Layout:** row group labels left-aligned bold 13px `#2c3e50` at x=25, groups centered at y = 85, 160, 235; bars start at x=230, max width 400 (= 100%), each bar 16px tall with 6px gaps, 12px value labels at bar ends.
- **Bar colors:** JSON blue `#2a78d6`, YAML green `#008300`, TOML violet `#4a3aa7`; small 12px legend swatches top right.
- **Rows (shares as hardcoded widths, % of 400px):**
  - "Web APIs": JSON 92, YAML 5, TOML 3
  - "CI / infra config": JSON 12, YAML 85, TOML 3
  - "Package manifests": JSON 15, YAML 10, TOML 75
- **Annotation (bold 13px magenta `#d55181`, right side near y=270):** "no format won everywhere — each won somewhere".
- **Caption (12px `#444`, bottom right):** "shares illustrative".

## The Norway Problem

**Tags:** `common mistake` (red), `implicit typing` (orange), `YAML 1.1` (blue)

- **The list grows** — the shop ships to Sweden too: `countries: [gb, fr, no, se]` — unquoted, as usual
- **The surprise** — YAML 1.1 reads bare `no` as boolean false; Norway silently becomes `false`
- **The cousins** — `yes`, `on`, `off`, `y`, `n` all convert the same way; only quoting stops it
- **Version numbers too** — `version: 1.10` parses as the float 1.1; the ".10" release becomes ".1"
- **The fix** — quote anything that must stay a string: `"no"`, `"1.10"`, `"on"`
- **The immune** — JSON and TOML always quote strings, so nothing is ever guessed

*Example (italic):* A deploy pins version 1.10, but the YAML loader hands the script the float 1.1 — and the rollout installs a nine-month-old release.

**Common mistake:** Trusting YAML's type guessing. Unquoted scalars are typed by pattern, not by intent — `no` is false and `1.10` is 1.1 until you add quotes.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the unquoted country list going through a YAML 1.1 parser (Norway becomes false) vs the quoted list (all strings survive).

- **Title (bold 15px, `#1a5276`, top center):** "Unquoted `no` Meets a YAML 1.1 Parser".
- **Row 1 (y=95), label 12px `#444` at x=20:** "unquoted"; blue `#2a78d6` rounded box at x=115 (190px wide) labeled `[gb, fr, no, se]` (12px monospace), 3px `#6b7280` arrow through a small mute box "YAML 1.1 parser" at x=330, arrow to a red `#e74c3c` box at x=480 (215px wide) labeled `["gb","fr",false,"se"]` with bold 12px red "✗ Norway became a boolean" beneath at y=135.
- **Row 2 (y=210), label:** "quoted"; blue box at x=115 labeled `["gb","fr","no","se"]`, same parser box, arrow to a green `#008300` box at x=480 labeled `["gb","fr","no","se"]` with bold 12px green "✓ four strings" beneath at y=250.
- **Box style:** 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` monospace text; parser boxes fill `rgba(107,114,128,0.12)`.
- **Annotation (bold 13px magenta `#d55181`, centered near y=285):** "version: 1.10 has the same disease — it parses as the float 1.1".
- **Caption (12px `#444`, bottom right):** "YAML 1.2 fixed this, but 1.1 loaders remain common".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded literals above (no randomness); the config snippets in c1 are exact and shared with c4's flow; character counts (95 / 62 / 72) are exact for the snippets shown; niche shares (92/5/3, 12/85/3, 15/10/75) are invented and labeled illustrative; the c2 feature marks and the YAML 1.1 conversions (`no` → false, `1.10` → 1.1) are true format behavior.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
