# Building a Data Analytics System

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Building a Data Analytics System

**Subtitle:** An order placed tonight, on a chart by breakfast — the five pieces every analytics system is made of, and the order to build them in

## The Five Pieces

**Tags:** `big picture` (blue), `five pieces` (green), `build order` (orange)

- **Collect** — a scheduled job pulls new data in every morning (the "pipeline")
- **Store** — pulled data lands somewhere safe: folders of files, or a database
- **Process** — a cleanup job reshapes raw data into tidy tables (an "ETL job")
- **Serve** — a small web service answers questions over the web: sales by day? (an "API")
- **Show** — a web page asks the service and draws the charts (the "dashboard")
- **The order** — build left to right; each piece should work before the next one starts

*Example (italic):* An order placed at 9:41 tonight is pulled at 6:00, tidy by 6:10, and on the dashboard by breakfast.

**Key point:** Every analytics system — hobby project to company platform — is these five pieces wired left to right; only the sizes change.

### Visualization (canvas `c1`, 720×300)

A factory floor seen from the side at daybreak: six pictogram stations standing on one floor line — a delivery truck, a conveyor belt, a shelf rack, a hopper machine with a gear, a service kiosk, and a monitor — with the order's journey times written along the floor.

- **Title (bold 17px, ink `#134e4a`, top center):** "The Data Factory at Daybreak".
- **Annotation (bold 14px copper `#b45309`, centered at y=48):** "build left to right — each piece works before the next starts".
- **Sun (top right sky):** filled circle at (665,70) r=16, fill `rgba(180,83,9,0.25)`, stroke copper `#b45309` 2px, six 2px copper rays from r+5 to r+12 at 60° steps.
- **Floor line:** 2px `#999` from x=20 to x=700 at y=205.
- **Station 1 — truck ("your sources"), cx=65:** body rect (32,174,46,26) + cab rect (78,182,18,18), fill `rgba(107,114,128,0.10)`, stroke mute `#6b7280` 1.5px; wheel circles r=6 at (46,205) and (84,205), mute stroke, white fill.
- **Station 2 — conveyor ("pipeline"), cx=175:** roller circles r=7 at (140,196) and (210,196) teal `#0f766e` stroke; belt = 1.5px teal lines along the roller tops (y=189) and bottoms (y=203); two parcels 16×12 at (150,177) and (178,177), fill `rgba(15,118,110,0.15)`, teal stroke.
- **Station 3 — shelf rack ("storage"), cx=285:** posts = 2px copper `#b45309` vertical lines x=255 and x=315 from y=140 to y=205; boards = 2px copper horizontals at y=165 and y=195 (x=255–315); three crates 18×16 fill `rgba(180,83,9,0.12)` copper stroke: (262,149), (284,149), (272,179).
- **Station 4 — hopper machine ("ETL job"), cx=395:** funnel trapezoid (365,140)→(425,140)→(402,172)→(388,172), fill `rgba(21,94,117,0.12)`, stroke deep `#155e75` 2px; body rect (380,172,30,20) same style; gear at (432,152): circle r=8 deep stroke 2px + 8 radial 2px teeth from r to r+4 + center dot r=2.
- **Station 5 — kiosk ("web service"), cx=505:** booth rect (478,150,54,55), fill `rgba(14,116,144,0.10)`, stroke cyan `#0e7490` 1.5px; window rect (488,160,34,22) white fill, grid `#e5e9ef` stroke; countertop 2px cyan line (474,182)–(536,182).
- **Station 6 — monitor ("dashboard"), cx=615:** screen rect (585,145,60,40), fill `rgba(190,24,93,0.10)`, stroke magenta `#be185d` 1.5px; inside: 1.5px magenta 4-point sparkline (592,172)→(602,164)→(612,168)→(624,158) and two bars 6px wide fill `rgba(190,24,93,0.35)` at (630,166,h=12) and (638,162,h=16); stand = 2px mute line (615,185)–(615,201) + base line (600,203)–(630,203).
- **Flow arrows (1.5px mute, mid-air at y=160):** (102→130), (220→248), (325→358), (444→470), (542→578).
- **Station labels (bold 12px, station color, centered at y=222 / sub 12px mute at y=236):** "your sources / shop, vendors, files" (65, mute-dark `#4b5563` for the bold line); "pipeline / runs 6:00 daily" (175, teal); "storage / files or database" (285, copper); "ETL job / clean + reshape" (395, deep); "web service / answers questions" (505, cyan); "dashboard / draws charts" (615, magenta).
- **Journey times (bold 13px copper time + 12px mute caption, centered):** "9:41 / order placed" at x=65 (y=258/272); "6:00 / pulled" at x=175; "6:10 / tidy" at x=395; "6:15 / on the dashboard" at x=615.
- **Caption (13px `#444`, bottom right at (700,292)):** "times illustrative".

## Collect: The Pipeline That Pulls

**Tags:** `pipeline` (blue), `scheduling` (green), `keep raw` (orange)

- **The schedule** — a timer wakes the job (say 6:00 daily); nobody has to remember to run it
- **The pull** — it asks each source for yesterday's new data: a vendor service, an export, a file drop
- **The landing** — everything is saved exactly as received, under its date: `raw/2026-08-26/orders.csv`
- **The retry** — sources fail sometimes; the job tries again, and complains loudly if it still can't
- **The rerun rule** — pulling the same day twice must not double the data; land by date, overwrite

*Example (italic):* At 6:00 the job pulls 312 orders from the shop system and one file from the ad vendor; both land under yesterday's date.

**Key point:** A pipeline is a scheduled program that pulls, lands data unchanged under its date, and can be rerun safely — raw files are your undo button, keep them.

### Visualization (canvas `c2`, 720×300)

A loading-dock scene at dawn: an alarm clock ringing at 6:00, three delivery trucks backing toward one dock door, and dated crates stacking up on the far side; one truck carries a retry note.

- **Title (bold 17px, ink `#134e4a`, top center):** "The Loading Dock at 6:00".
- **Alarm clock (top left):** circle at (80,75) r=18, white fill, deep `#155e75` stroke 2px; two bell arcs r=6 centered (68,58) and (92,58) deep stroke; hands 2px deep: minute (80,75)→(80,62), hour (80,75)→(80,86); four 1.5px tick marks inside at 12/3/6/9; label (bold 13px ink `#134e4a`, centered at (80,112)): "wakes 6:00 daily".
- **Trucks (left column, facing right; body 52×24 + cab 20×14, wheels r=6 white fill at body-x+16 and body-x+58):** truck 1 "shop system" teal `#0f766e` — body (40,100), cab (92,110), wheels y=130, label bold 12px teal centered (78,93); truck 2 "ad vendor" copper `#b45309` — body (40,155), cab (92,165), wheels y=185, label centered (78,148); truck 3 "file drop" mute `#6b7280` — body (40,210), cab (92,220), wheels y=240, label centered (78,203). Body fills at 0.12 alpha of each stroke color.
- **Dock (center right):** platform rect (330,115,90,150), fill `rgba(21,94,117,0.08)`, deep stroke 1.5px; dock door rect (345,130,60,60), fill `rgba(21,94,117,0.15)`, deep stroke, three horizontal slat lines at y=145/160/175 (x=345–405); label bold 13px deep centered (375,105): "dock — pull job".
- **Truck→dock arrows (1.5px mute):** (114,112)→(330,150); (114,167)→(330,180); (114,222)→(330,210). Retry note on the middle arrow: bold 12px copper left-aligned at (150,163): "failed once — retried ✓", plus a retry arc at the ad-vendor cab: 1.5px copper circle arc at (125,143) r=9 from 0.3π to 1.9π with a small arrowhead.
- **Crates (right):** arrow (420,180)→(452,180) mute; big crate rect (460,150,110,70), fill `rgba(180,83,9,0.12)`, copper stroke 1.5px, lid line at y=165; stamp text bold 13px deep centered (515,190): "raw/2026-08-26/"; two small crates on top: (470,118,55,32) with 12px mute text "orders.csv" centered (497,138), and (530,124,50,26) plain — same fill/stroke.
- **Sub note (12px mute, centered at (515,245)):** "saved exactly as received".
- **Annotation (bold 14px copper, centered at y=282):** "land it raw and dated — rerunning a day must not double it".
- **Caption (13px `#444`, bottom right at (700,292)):** "names illustrative".

## Store: Files or a Database

**Tags:** `storage` (blue), `files vs database` (green), `raw vs clean` (orange)

- **Files** — dated folders of files: nothing to install, cheap, perfect for raw pulls and archives
- **The database** — tables you can question in one line: sales by region last week ("SQL")
- **The split** — keep raw as files forever; load only the cleaned version into the database
- **When files win** — small data, one user, a few charts: files plus a script go surprisingly far
- **When a database wins** — many questions, many users, tables that join, filters that must be fast

*Example (italic):* Every raw pull stays on disk untouched; clean orders load into one database table, and last week's questions become one-liners.

**Key point:** Start with files and add a database the day questions get slow or tables need to join — the raw files stay either way.

### Visualization (canvas `c3`, 720×300)

A storeroom split scene: a shelf rack of dated file boxes on the left, a database cylinder with a query slot on the right, and a pipe with a "clean" valve carrying only the cleaned version across; trade-off lines under each side.

- **Title (bold 17px, ink `#134e4a`, top center):** "Files or a Database — It's Not Either/Or".
- **Headers (bold 14px, centered at y=52):** "files in folders" teal `#0f766e` at x=170; "a database" cyan `#0e7490` at x=550.
- **Shelf rack (left):** posts = 2px teal vertical lines at x=60 and x=280 from y=60 to y=170; boards = 2px teal horizontals at y=100, y=145, y=170 (x=60–280); top-board boxes 50×24 fill `rgba(15,118,110,0.12)` teal stroke at (70,74), (130,74), (190,74) with 12px mute date texts centered inside: "08-24", "08-25", "08-26"; middle-board boxes at (85,119) and (155,119), same style, no text.
- **Database cylinder (right, cx=550):** body fill `rgba(14,116,144,0.10)` cyan stroke 2px — top ellipse center (550,80) rx=70 ry=14, side lines (480,80)→(480,160) and (620,80)→(620,160), bottom front arc (ellipse at y=160, lower half), two band front arcs at y=107 and y=134; query slot rect (505,95,90,18) white fill grid `#e5e9ef` stroke with 12px deep `#155e75` text centered (550,108): "sales by region?".
- **Pipe (shelf → cylinder):** two 2px mute horizontal lines y=120 and y=130 from x=280 to x=480; flow arrow 1.5px cyan (390,125)→(470,125); valve circle r=8 at (370,125), fill `rgba(21,94,117,0.12)`, deep stroke 2px, stem line (370,117)→(370,106) with cross-bar (363,106)→(377,106); valve label bold 12px deep centered (370,97): "clean".
- **Split note (12px copper `#b45309`, centered at (370,148)):** "raw stays as files · only the clean version flows in".
- **Trade-off lines (12px, ✓ teal / ✗ `#e74c3c`, text `#2c3e50`, left-aligned, y=195/211/227/243):** left at x=45: "✓ nothing to install — just folders"; "✓ perfect for raw pulls and archives"; "✗ every question needs a script"; "✗ slows down as data grows". right at x=425: "✓ one-line questions (SQL)"; "✓ fast filters, joins across tables"; "✗ something to set up and care for"; "✗ overkill for a handful of files".
- **Annotation (bold 14px copper, centered at y=275):** "start with files — add a database when questions get slow".

## Process: The Cleanup Job

**Tags:** `ETL` (blue), `worked example` (green), `rebuildable` (orange)

- **The mess** — raw data arrives messy: duplicates, missing fields, three different date formats
- **The job** — a second scheduled program reads raw, fixes it, writes tidy tables (an "ETL job")
- **The shape** — one tidy table per question family: orders_daily, customers, ad_spend
- **The rebuild** — tidy tables can be rebuilt from raw at any time, so mistakes are never fatal
- **The chain** — it runs right after the pull: collect at 6:00, clean at 6:10, ready by 6:15

*Example (italic):* 312 raw orders become 309 tidy rows — two duplicates dropped, one test order removed, every date in one format.

**Key point:** Raw is what happened; tidy tables are what you can ask questions of — and because raw is kept, the bridge between them can always be rebuilt.

### Visualization (canvas `c4`, 720×300)

A cleanup machine: messy parcels ride an in-conveyor into a funnel-and-gear machine, identical clean cubes ride out the other side, and a scrap bin under the machine catches the three rejects.

- **Title (bold 17px, ink `#134e4a`, top center):** "312 Messy Parcels In, 309 Clean Cubes Out".
- **Counts line (bold 14px deep `#155e75`, centered at (360,68)):** "312 raw → 309 tidy".
- **In-conveyor (left):** roller circles r=7 white fill copper `#b45309` stroke at (55,150) and (235,150); belt = 1.5px copper lines y=143 and y=157 (x=55–235); five parcels 16×14 fill `rgba(180,83,9,0.12)` copper stroke on the belt (tops at y=129): (60,129), overlapping duplicate pair (95,129) and (103,125), (140,129), test parcel (175,129), (210,129); 12px magenta `#be185d` notes: "duplicates" centered (99,112) and "test" centered (183,118).
- **In label (bold 13px ink, centered at (145,95)):** "raw orders · 312".
- **Machine (center):** funnel trapezoid (290,110)→(430,110)→(390,170)→(330,170), fill `rgba(21,94,117,0.12)`, deep stroke 2px; body rect (330,170,60,35) same style with bold 13px `#2c3e50` text centered (360,190): "ETL job"; side spout rect (390,180,15,10) same style; gear at (435,120): circle r=11 deep stroke 2px + 8 radial 2px teeth from r to r+5 + center dot r=2.5; 12px mute centered (360,232): "extract · transform · load".
- **Out-conveyor (right):** rollers r=7 white fill teal `#0f766e` stroke at (480,150) and (665,150); belt = 1.5px teal lines y=143 and y=157 (x=480–665); four clean cubes 16×14 fill `rgba(15,118,110,0.15)` teal stroke, tops y=129, at x=495/540/585/630; flow arrow 1.5px mute (405,185)→(473,152).
- **Out label (bold 13px ink, centered at (572,95)):** "orders_daily · 309".
- **Scrap bin (under machine):** open-top bin outline 1.5px magenta — left side (332,245)→(338,272), bottom (338,272)→(382,272), right (388,245)→(382,272); three reject pieces 8×6 fill `rgba(190,24,93,0.30)` at (345,252), (357,258), (367,250); dashed 1px mute drop line (360,217)→(360,242); label 12px magenta left-aligned at (400,262): "2 duplicates · 1 test order".
- **Annotation (bold 14px copper, centered at y=290):** "raw is what happened — tidy is what you can ask".
- **Caption (13px `#444`, right-aligned at (700,110)):** "counts illustrative" — placed under the gear, clear of the bottom annotation.

## Serve: A Web Service for the Numbers

**Tags:** `web service` (blue), `API` (green), `read-only first` (orange)

- **The idea** — a small always-on program answers questions over the web (a "web service" or "API")
- **The questions** — one web address per question: `/sales-by-day` returns dates and totals
- **The reading** — it reads only the tidy tables; the ETL job already did the hard work
- **Why bother** — one service, many screens: dashboards, spreadsheets, phones see the same numbers
- **Keep it boring** — a handful of read-only questions is plenty; no writes, no logins at first

*Example (italic):* The dashboard asks `/sales-by-day`; the service reads orders_daily and replies with thirty date-and-total pairs.

**Key point:** The web service is a thin layer between tables and screens — because every chart asks it, the numbers match everywhere.

### Visualization (canvas `c5`, 720×300)

A ticket-counter scene: the dashboard (a small monitor on a stand) hands a ticket across a service counter to a clerk, who reads the orders_daily ledger from the shelf behind and slides the rows back — four numbered exchanges.

- **Title (bold 17px, ink `#134e4a`, top center):** "One Question at the Counter".
- **Dashboard character (left):** monitor rect (75,120,70,48), fill `rgba(190,24,93,0.10)`, magenta `#be185d` stroke 1.5px, with three tiny bars inside, 8px wide, fill `rgba(190,24,93,0.35)`: (88,152,h=10), (100,148,h=14), (112,144,h=18); stand = 2px mute line (110,168)→(110,180) + base line (95,182)→(125,182); label bold 13px magenta centered (110,202): "dashboard".
- **Counter (center):** countertop rect (250,175,220,10), fill `rgba(21,94,117,0.15)`, deep `#155e75` stroke 1.5px; front panel rect (255,185,210,50), fill `rgba(21,94,117,0.08)`, deep stroke, three vertical slat lines at x=305/360/415 (y=185–235); window frame 1.5px mute stroke rect (275,75,170,100); sign board rect (285,50,150,22), fill `rgba(14,116,144,0.12)`, cyan `#0e7490` stroke, bold 13px cyan text centered (360,65): "web service".
- **Clerk (behind counter):** head circle r=10 at (360,120), white fill, cyan stroke 2px; shoulders = upper half-disc arc at (360,152) r=20 (π to 2π), fill `rgba(14,116,144,0.15)`, cyan stroke 2px.
- **Ledger shelf (right):** shelf board 2px copper `#b45309` line (520,140)→(680,140) with two bracket lines (530,140)→(530,150) and (670,140)→(670,150); ledger book rect (560,100,80,40), fill `rgba(180,83,9,0.12)`, copper stroke 1.5px, spine line (570,100)→(570,140), 12px `#2c3e50` text centered (605,123): "orders_daily".
- **Numbered exchanges (arrows 2px, labels 12px in the arrow's color):** ① deep `#155e75` arrow (155,125)→(268,125), label centered (210,113): "1 · asks /sales-by-day"; ② cyan arrow (455,105)→(550,105), label centered (500,93): "2 · reads the ledger"; ③ copper arrow (550,130)→(455,130), label centered (500,150): "3 · 30 rows"; ④ teal `#0f766e` arrow (268,155)→(155,155), label centered (210,172): "4 · 30 date + total pairs".
- **End marker (bold 13px teal, centered at (110,222)):** "chart drawn ✓".
- **Annotation (bold 14px copper, centered at y=280):** "one service feeds every screen — numbers match everywhere".

## Show: The Dashboard

**Tags:** `dashboard` (blue), `frontend` (green), `one screen` (orange)

- **The page** — one web page with a few charts; it asks the web service and draws what comes back
- **The charts** — a line for the trend, bars for the comparison, one big number for today
- **The refresh** — the page re-asks whenever it's opened; nobody emails screenshots around
- **The restraint** — one screen, five charts at most; a dashboard is for answers, not decoration

*Example (italic):* The morning screen: today's sales as a big number, a 30-day line, top regions as bars — three questions to the service.

**Key point:** The dashboard is the only piece anyone sees — keep it one screen that answers the questions your team actually asks each morning.

### Visualization (canvas `c6`, 720×300)

The morning screen as a physical monitor on a stand, sun rising behind its top-left corner: inside the screen a big-number tile, a 30-day trend line, and five region bars, all in the page's teal/copper theme.

- **Title (bold 17px, ink `#134e4a`, top center):** "The Morning Screen".
- **Sun (behind the monitor, drawn first):** circle at (170,60) r=20, fill `rgba(180,83,9,0.25)`, copper `#b45309` stroke 2px, six 2px copper rays from r+6 to r+14 at 60° steps.
- **Monitor:** screen rounded rect (150,50,470,195), white fill, deep `#155e75` stroke 2px; stand trapezoid (370,245)→(400,245)→(410,262)→(360,262), fill `rgba(229,233,239,0.8)`, mute stroke 1.5px; base line 2px mute (340,266)→(430,266).
- **Screen header (bold 14px ink, left-aligned at (170,74)):** "Sales — daily".
- **Stat tile:** rounded rect (170,88,140,64), fill `rgba(15,118,110,0.10)`, teal `#0f766e` stroke; bold 22px ink "$4,210" centered (240,118); 12px mute "sales today" centered (240,138).
- **Trend panel:** rounded rect (330,88,270,64), fill `rgba(14,116,144,0.05)`, grid `#e5e9ef` stroke; 12px mute "last 30 days" left-aligned (345,102); 2px cyan `#0e7490` polyline through 10 points at px=345+i·26 (x=345..579), y = `[140, 136, 142, 134, 130, 133, 124, 126, 118, 112]`.
- **Bars panel:** rounded rect (170,160,430,72), fill `rgba(14,116,144,0.05)`, grid stroke; 12px mute "sales by region" left-aligned (185,174); five bars 34px wide, fill `rgba(15,118,110,0.35)`, bottoms on y=222, heights `[48, 38, 27, 18, 11]`, at x=210/290/370/450/530; 12px `#444` labels centered under each at y=230: "west", "east", "north", "south", "web".
- **Annotation (bold 14px copper, centered at y=288):** "one screen, a few charts — answers, not decoration".
- **Caption (13px `#444`, right-aligned at (700,40)):** "numbers illustrative" — top right, clear of the annotation.

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then six `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper multiplies the backing store by `window.devicePixelRatio` and calls `ctx.scale(dpr,dpr)`; charts are static (no animation), redrawn once on debounced resize.
- **Chart palette object (page theme: factory floor / daybreak, per `common-howtos/CLAUDE.md`):** teal `#0f766e`, cyan `#0e7490`, deep `#155e75`, copper `#b45309`, magenta `#be185d`, ink `#134e4a`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Fills use theme hues at 0.08–0.18 alpha. Red `#e74c3c` only for ✗ marks. Site palette (page chrome only): primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Viz style:** pictorial scenes per `common-howtos/CLAUDE.md` — pictograms drawn with canvas primitives (trucks, conveyors, crates, gears, a database cylinder, a clock, a counter clerk, a monitor on a stand); no two canvases share the same layout skeleton; helper functions (`truck`, `crate`, `roller`, `gear`, `sun`) live beside `setup`/`rbox`/`arrowTo`.
- **Fonts:** titles bold 17px, primary labels 13px, secondary/mute captions 12px, insight annotations bold 14px; nothing below 12px.
- **Data:** all positions and values are the hardcoded literals above (no randomness); the 9:41 / 6:00 / 6:10 / 6:15 times, the 312 → 309 row counts, the $4,210 figure, the trend offsets, the bar heights, and names like `raw/2026-08-26/orders.csv`, orders_daily, `/sales-by-day` are invented and labeled illustrative; "shop system" and "ad vendor" are generic sources, not real products.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
