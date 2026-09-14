# Configuration & Environment Variables

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Configuration & Environment Variables

**Subtitle:** Config is everything that varies between deploys; code is identical everywhere — the same build runs in dev, staging, and prod with different environment variables

## The Same Code, Three Different Databases

**Tags:** `core idea` (blue), `twelve-factor` (green), `code vs config` (orange)

- **The service** — an orders API runs in dev, staging, and prod, all three from the same build, #481
- **What varies** — the database URL, credentials, a checkout feature toggle, the connection pool size
- **The rule** — config is everything that varies between deploys; code is identical everywhere
- **The credit** — this is the config factor of the published Twelve-Factor App methodology
- **The test** — could you open-source the repo right now without leaking anything? If not, config is in code
- **The vehicle** — each deploy injects its own values as environment variables; the binary never changes

*Example (italic):* Build #481 runs unchanged in all three environments; only DATABASE_URL, POOL_SIZE, and one feature toggle differ between them.

**Key point:** The Twelve-Factor App's config factor demands strict separation: code is identical across deploys, and everything that varies — URLs, credentials, toggles, resource sizes — lives in the environment, never in the source.

### Visualization (canvas `c1`, 720×300)

Fan-out diagram: one build artifact at the top feeding three environment boxes, each showing different env var values for the same three settings.

- **Title (bold 15px, `#1a5276`, top center):** "Build #481 Everywhere: Only the Environment Changes".
- **Artifact box:** rounded 8px box from x=270 to x=450, y=45 to y=85, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, bold 12px `#1a5276` centered label "orders-api — build #481".
- **Arrows:** three 3px `#6b7280` arrows from the artifact box bottom (y=85) down to y=150, ending at x=130, x=360, x=590.
- **Environment boxes (y=150, width 200, height 105, 8px radius, fill `rgba(26,82,118,0.06)`, 1px `#e5e9ef` border) at x=30 / 260 / 490:** headers bold 13px — "dev" `#2a78d6`, "staging" `#199e70`, "prod" `#008300`; under each header three 11px `#2c3e50` lines:
  - dev: `DATABASE_URL=localhost/orders_dev`, `NEW_CHECKOUT=on`, `POOL_SIZE=5`
  - staging: `DATABASE_URL=stg-db:5432/orders`, `NEW_CHECKOUT=on`, `POOL_SIZE=10`
  - prod: `DATABASE_URL=prod-db:5432/orders`, `NEW_CHECKOUT=off`, `POOL_SIZE=25`
- **Annotation (bold 13px violet `#4a3aa7`, centered at y=285):** "same code everywhere — the environment carries what varies".

## Who Wins: Defaults, Files, Environment

**Tags:** `worked example` (blue), `precedence` (green)

- **Three layers** — defaults in code, then a config file, then env variables; later layers override earlier
- **POOL_SIZE** — default 5 in code, file says 10, prod env says 25 → the service runs with 25
- **TIMEOUT_MS** — default 3000, file says 8000, no env override → 8000 wins
- **LOG_LEVEL** — default "info", not in the file, env says "debug" → debug wins
- **RETRIES** — default 3, no file entry, no env override → the code default 3 survives
- **Why env on top** — env vars are language-agnostic, per-process, injected by the platform at runtime

*Example (italic):* Hand-check the four settings: env decides 2 of them (POOL_SIZE, LOG_LEVEL), the config file 1 (TIMEOUT_MS), and code defaults 1 (RETRIES).

**Key point:** Layer config with explicit precedence — defaults in code → config files → environment overrides — so one artifact behaves correctly everywhere and you can always answer "where did this value come from?".

### Visualization (canvas `c2`, 720×300)

Precedence grid drawn on canvas: 4 setting rows × 3 source columns plus an "effective" column, with the winning cell highlighted per row.

- **Title (bold 15px, `#1a5276`, top center):** "Three Layers, One Answer: the Highest Layer That Speaks Wins".
- **Column headers (bold 12px `#1a5276`, y=68):** "setting" at x=20, "code default" at x=175, "config file" at x=320, "env var" at x=455, "effective" at x=595.
- **Grid:** horizontal 1px `#e5e9ef` lines between rows; row text baselines at y = 105, 150, 195, 240 (row height 45); setting names bold 12px `#2c3e50` at x=20.
- **Rows (default / file / env / effective):**
  - `POOL_SIZE`: 5 / 10 / **25** / 25 — env cell wins
  - `TIMEOUT_MS`: 3000 / **8000** / — / 8000 — file cell wins
  - `LOG_LEVEL`: info / — / **debug** / debug — env cell wins
  - `RETRIES`: **3** / — / — / 3 — default cell wins
- **Cell style:** winning cell gets a rounded pill fill `rgba(0,131,0,0.15)` with bold 12px `#008300` text; overridden values plain 12px `#6b7280`; absent entries an em dash 12px `#6b7280`; effective column bold 12px `#1a5276`.
- **Annotation (bold 12px violet `#4a3aa7`, centered at y=272):** "env decides 2, the file 1, code defaults 1 — precedence is explicit".
- **Caption (12px `#444`, bottom right):** "values illustrative".

## The 3am Failure Modes

**Tags:** `failure modes` (red), `secrets` (orange), `fail fast` (green)

- **Drift** — "works in staging" because staging's env silently differs from prod's; the diff is invisible
- **Untyped strings** — every env var is a string; parse and validate at startup, fail fast with a clear error
- **Secret leakage** — a debug env dump logs the database password; secrets belong in a secret manager
- **Sprawl** — hundreds of undocumented knobs accumulate; nobody remembers what half of them do
- **The countermeasure** — config-as-code: settings that matter live in reviewed files, so a diff shows every change

*Example (italic):* A pool size set to "twenty" instead of "20" should crash at boot with a clear message — not misbehave silently at 3am under load.

**Key point:** Validate and type-convert all config at startup and fail fast; keep secrets in a secret manager rather than plain env; put the settings that matter through the same review as code.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: share of config-related incidents by failure mode (illustrative), matching the four failure modes in the text.

- **Title (bold 15px, `#1a5276`, top center):** "Where Config Bites: Share of Config-Related Incidents".
- **Layout:** left-aligned 12px `#444` row labels at x=20; bars start at x=250; scale 11 px per percentage point; bars 16px tall with 11px value labels at bar ends.
- **Rows (top to bottom at y = 80, 130, 180, 230):**
  - "environment drift — 38%": orange `#d95926` bar, width 418
  - "untyped-string parse bug — 27%": violet `#4a3aa7` bar, width 297
  - "config sprawl / unknown knob — 26%": blue `#2a78d6` bar, width 286
  - "secret leaked via env dump — 9%": red `#e74c3c` bar, width 99
- **Annotation (bold 13px magenta `#d55181`, near x=270, y=262):** "drift and bad parses: nearly two thirds of the pain".
- **Caption (12px `#444`, bottom right):** "shares illustrative".

## Rebuilding Per Environment Is the Anti-Pattern

**Tags:** `common mistake` (red), `build once` (green)

- **The temptation** — bake staging's URL into a staging build and prod's URL into a separate prod build
- **Three artifacts** — each environment gets its own compile; staging tests build #2, prod ships build #3
- **The gap** — the artifact you tested is not the artifact you shipped; any build difference goes untested
- **Build once** — produce one image, promote it dev → staging → prod, and vary only the environment
- **The check** — if changing a database URL requires a rebuild, config has leaked into the build

*Example (italic):* The staging build passes every test; the prod build, compiled an hour later on a newer base image, does not — and prod is where you find out.

**Common mistake:** Rebuilding per environment. Twelve-factor deploys build one artifact and configure it at runtime; every per-environment rebuild ships code that nobody tested.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: rebuild-per-environment (three separate builds, prod's untested) vs build-once (one image promoted through all three environments).

- **Title (bold 15px, `#1a5276`, top center):** "Rebuild Per Environment vs Build Once, Configure Everywhere".
- **Row 1 (boxes centered on y=95), label 12px `#444` at x=20:** "rebuild per env"; three separate 8px-radius boxes 160px wide, 40px tall at x=150 / 340 / 530 — blue `rgba(42,120,214,0.15)` box "build #1 → dev", green `rgba(0,131,0,0.12)` box "build #2 → staging ✓ tested", red `rgba(231,76,60,0.12)` box "build #3 → prod"; bold 12px `#e74c3c` note under the prod box (y=135): "shipped build was never tested".
- **Row 2 (boxes centered on y=205), label:** "build once"; blue box "image #481" at x=130 (130px wide), then 3px `#6b7280` arrows through three 110px-wide boxes at x=300 / 440 / 580 labeled "dev", "staging ✓", "prod ✓", the last two filled `rgba(0,131,0,0.12)` with 12px `#008300` text; bold 12px `#008300` note under the prod box (y=245): "tested artifact = shipped artifact".
- **Box text:** 12px `#2c3e50` unless colored above; all boxes 8px radius.
- **Annotation (bold 13px orange `#d95926`, centered near y=278):** "the environment varies; the artifact must not".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded literals above (no randomness); env var values, the four-setting precedence table, and the incident-share percentages (38 / 27 / 26 / 9) are invented and labeled illustrative; text numbers match chart numbers (POOL_SIZE 25, TIMEOUT_MS 8000, build #481).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
