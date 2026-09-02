# Evolution of Computing, Data & Tech

**Page type:** other (single-page long doc: h1, subtitle, intro callout, legend paragraph, then one two-column `.obj-table` with one row per strand — text left 42%, timeline canvas right)
**HTML title tag:** Evolution of Computing, Data & Tech

**Subtitle:** Every strand on one page, on one shared timeline — so overlaps between them are visible at a glance.

## Intro callout

**Reading this page:** each row is one strand of the industry. Key points on the left, and on the right a timeline of its major phases. **Every timeline uses the same 1940–2026 scale**, so a name sitting halfway across means the same year in every section — you can scan down the page and see which developments were contemporaneous. Dates are approximate and mark the point of gaining mainstream adoption rather than first release.

## Legend

**Reading the marks:** each technology is written at the year it began gaining mainstream adoption — the name is the mark, so there is nothing to match up against a separate label. A **filled dot** means it reached mainstream use quickly, within a few years; a **hollow dot** means it climbed gradually. The line trailing right shows how long it stayed: it simply runs to the present day if still in use, carries a **vertical tick** at the year it stopped being mainstream practice, or is drawn **dashed** where it survives in niches only. Because most of these are still in use, the arrival year carries the information and the end is deliberately understated. Colour separates neighbouring rows and carries no other meaning.

> Note: in the current HTML the draw function renders only the arrival dot, the label, and a left-edge chevron for spans clamped at 1940 — the trailing line / vertical tick / dashed variants described in the legend are present in the data (status field) but not drawn.

## 1. Programming Languages

**Label:** LANGUAGES · **Span:** 1940s → present

- Machine code gave way to compilers, so source outlived the machine it ran on
- Managed runtimes moved memory handling into the runtime — hardware was getting cheaper faster than programmers
- Clock speeds stalled, so concurrency moved into the language itself
- Python concentrates AI work as a coordination layer over C++/CUDA
- Coding assistance weakens familiarity as a selection criterion

### Visualization (canvas `data-i="0"`, responsive width min 520px × computed height)

Shared-scale timeline (see Regeneration instructions). Rows `[label, start, end, status, adoption]`; `fast` = filled dot, else hollow:

- Assembly, 1945–1968, gone, hollow
- Fortran/COBOL/Lisp, 1957–1980, niche, hollow
- C, Pascal, 1972–2026, alive, hollow
- C++, Obj-C, 1983–2026, alive, hollow
- Perl, 1990–2010, gone, hollow
- Visual Basic, 1991–2008, gone, fast
- Java, Python, JS, 1995–2026, alive, fast
- Flash/ActionScript, 1998–2020, gone, fast
- Go, Rust, TS, 2010–2026, alive, hollow

## 2. Programming Paradigms

**Label:** PARADIGMS · **Span:** 1950s → present

- Procedures became the unit of reuse and remain the substrate underneath everything since
- SQL proved the durable declarative idea: state the result, let the planner find the steps
- Distributed objects failed trying to hide the network — the failure just moved elsewhere
- Multicore made immutability practical rather than academic
- Microservices traded a compile-time boundary for a network one

### Visualization (canvas `data-i="1"`)

- Goto / unstructured, 1950–1972, gone, hollow
- Procedural, 1960–2026, alive, hollow
- Declarative, SQL, 1972–2026, alive, hollow
- Object-oriented, 1980–2026, alive, fast
- CORBA / DCOM, 1991–2004, gone, hollow
- SOA / SOAP, 1999–2012, gone, hollow
- Functional revival, 2007–2026, alive, hollow
- Microservices, 2012–2026, alive, fast
- Serverless, 2015–2026, alive, fast

## 3. Data Footprint

**Label:** DATA SCALE · **Span:** 1950s → present

- Sequential tape forced algorithms to read in order for decades
- Storage was once dear enough that people deleted things to make room
- Petabytes on commodity hardware made keeping everything cheaper than choosing
- Object storage separated storage from compute, which retired the Hadoop cluster
- "Big data" retired as vocabulary once the capability became the default assumption

### Visualization (canvas `data-i="2"`)

- Cards & tape, 1950–1975, gone, hollow
- Floppy disk, 1971–2005, gone, hollow
- MB hard disk, 1980–1998, gone, hollow
- GB disk & RDBMS, 1995–2026, alive, hollow
- Warehouse, TB, 1995–2026, alive, hollow
- "Big data" era, 2006–2016, gone, fast
- Object storage, 2006–2026, alive, fast
- Lakehouse, 2015–2026, alive, hollow
- Web-scale corpora, 2019–2026, alive, hollow

## 4. Data Analytics Sophistication

**Label:** ANALYTICS · **Span:** 1900s → present

- Hand tabulation kept questions simple because every cross-tab cost human labour
- Statistical inference turned "will it rain" into a probability with stated confidence
- Markov models drove speech and translation for decades on a deliberately short memory
- Schema moved from write time to read time once shape was unknown in advance
- Entity resolution reconciles sources with no shared key — where governance questions concentrate

### Visualization (canvas `data-i="3"`)

- Manual tabulation, 1900–1960, gone, hollow (starts before 1940, drawn with left-edge clamp chevron)
- Inference, regression, 1930–2026, alive, hollow (clamped at 1940)
- Operations research, 1950–2026, alive, hollow
- Markov / HMM, 1960–2010, niche, hollow
- OLAP & BI, 1990–2026, alive, hollow
- Data mining, 1993–2012, niche, hollow
- JSON / wide-column, 2004–2026, alive, fast
- Entity resolution, 2010–2026, alive, hollow
- Embeddings, NL query, 2018–2026, alive, fast

## 5. Machine Learning

**Label:** MACHINE LEARNING · **Span:** 1950s → present

- Expert systems collapsed on knowledge acquisition — experts could not articulate what they knew
- A single decision tree overfits, but proved an excellent building block
- Averaging many imperfect models beats tuning one
- Feature engineering carried the work once algorithms were off the shelf
- Gradient boosting still leads on tabular data, right through the deep learning boom

### Visualization (canvas `data-i="4"`)

- Regression, k-NN, 1950–2026, alive, hollow
- Expert systems, 1975–1992, gone, hollow
- Decision trees, 1980–2026, alive, hollow
- SVM & kernels, 1995–2012, niche, hollow
- Random forests, 1996–2026, alive, hollow
- Boosting / GBM, 1997–2026, alive, fast
- Feature engineering, 2000–2016, niche, hollow
- scikit-learn era, 2010–2026, alive, fast
- AutoML & MLOps, 2017–2026, alive, hollow

## 6. Neural Networks & Deep Learning

**Label:** NEURAL NETS · **Span:** 1957 → present

- Two winters: the ideas were present, the training method and the compute were not
- Backprop made depth trainable; vanishing gradients then stalled it again
- ImageNet settled the argument once data, GPUs and training tricks converged
- Attention removed recurrence, so training could parallelise
- Scaling laws turned research into an engineering programme

### Visualization (canvas `data-i="5"`)

- Perceptron, 1957–1969, gone, hollow
- First AI winter, 1969–1986, gone, hollow
- Backprop, shallow, 1986–1998, niche, hollow
- SVM era / 2nd winter, 1998–2006, gone, hollow
- Deep pretraining, 2006–2012, gone, hollow
- CNNs, ImageNet, 2012–2026, alive, fast
- RNN / LSTM, 2014–2020, niche, hollow
- GANs, 2014–2022, niche, hollow
- Transformers, 2017–2026, alive, fast
- Diffusion, scaling, 2020–2026, alive, fast

## 7. System Architectures

**Label:** ARCHITECTURE · **Span:** 1960s → present

- The pendulum has swung between centralised and distributed four times
- Mainframes had properties later designs had to rediscover: one place to secure and back up
- The service bus became both bottleneck and single point of failure
- Containers made the same image run identically everywhere — the precondition for all that followed
- Serverless cut operational burden and portability together

### Visualization (canvas `data-i="6"`)

- Mainframe & terminals, 1960–1990, niche, hollow
- Minicomputer, 1970–1992, gone, hollow
- Client-server, 1985–2005, gone, hollow
- Three-tier web, 1995–2015, niche, hollow
- SOA / ESB, 2000–2012, gone, hollow
- Virtualisation, 2001–2026, alive, fast
- Cloud IaaS, 2006–2026, alive, fast
- Microservices, K8s, 2013–2026, alive, fast
- Serverless, edge, 2015–2026, alive, hollow

## 8. Coding Tools

**Label:** DEV TOOLS · **Span:** 1950s → present

- Batch turnaround made desk-checking a real skill because iteration was expensive
- vi and Emacs are both roughly fifty years old and still in daily use
- Centralised VCS made branching costly, which shaped how teams organised work
- Distributed history made branches cheap and merging routine
- Agentic tools shift the work toward specifying intent and reviewing results

### Visualization (canvas `data-i="7"`)

- Punched cards, 1955–1975, gone, hollow
- Line editors, 1969–1985, gone, hollow
- vi, Emacs, 1976–2026, alive, hollow
- CVS / SVN, 1982–2012, gone, hollow
- Graphical IDEs, 1991–2026, alive, hollow
- Git, 2005–2026, alive, fast
- GitHub, PRs, 2008–2026, alive, fast
- CI/CD, 2011–2026, alive, hollow
- AI assistance, 2021–2026, alive, fast

## 9. Internet & Communication

**Label:** INTERNET · **Span:** 1970s → present

- Early networks organised around the topic; identity came later
- Email predates the web and has outlasted every intended replacement
- Search replaced navigating a hierarchy with querying an index
- The social graph made content arrive by who you knew rather than what you sought
- Recommendation decoupled reach from follower count entirely
- Synthetic video arrives as its own format — minute-long episodes and hour-long vertical dramas produced without a crew

### Visualization (canvas `data-i="8"`)

- Email, 1971–2026, alive, hollow
- BBS & dial-up, 1978–1996, gone, hollow
- Usenet & IRC, 1980–2005, niche, hollow
- Static web, 1993–2003, gone, hollow
- IM clients, 1997–2013, gone, hollow
- Search-centred web, 1998–2026, alive, hollow
- Blogs, RSS, 1999–2026, niche, hollow
- Social networking, 2004–2026, alive, fast
- Mobile-first, 2008–2026, alive, fast
- Short-form video, 2016–2026, alive, fast
- AI-generated video, 2023–2026, alive, fast

## 10. Processors & Compute Hardware

**Label:** PROCESSORS · **Span:** 1940s → present

- Reliability, not speed, was the binding constraint in the tube era
- Free single-threaded speedups ended around 2004 when power density capped frequency
- More cores meant gains stopped being free — software had to change to benefit
- GPU arithmetic getting ~100× cheaper is what made deep learning practical
- Data movement, not arithmetic, now limits large-model performance

### Visualization (canvas `data-i="9"`)

- Vacuum tubes, 1945–1958, gone, hollow
- Transistors, 1955–1970, gone, hollow
- Microprocessor, 1971–2026, alive, hollow
- Clock-speed scaling, 1975–2004, gone, hollow
- GPUs, 1996–2026, alive, fast
- Multicore, 2005–2026, alive, hollow
- CUDA / GPGPU, 2007–2026, alive, fast
- Mobile SoC, ARM, 2008–2026, alive, fast
- TPUs, accelerators, 2016–2026, alive, hollow

## 11. Cloud & Software Delivery

**Label:** DELIVERY · **Span:** 1980s → present

- Boxed software shipped defects that stayed in the field until the next release
- Hosted applications were tried in the 1990s and failed for want of enabling technology
- Renting infrastructure removed capacity planning as a gate on starting anything
- Subscription made software rented, where price and terms can be revised
- Rising bills moved some steady workloads back to owned hardware

### Visualization (canvas `data-i="11"`)

- Boxed licences, 1980–2010, gone, hollow
- On-premise enterprise, 1985–2020, niche, hollow
- ASPs, 1998–2005, gone, hollow
- Web apps, AJAX, 2004–2026, alive, fast
- IaaS, 2006–2026, alive, hollow
- SaaS subscriptions, 2006–2026, alive, fast
- Thin clients, 2011–2026, alive, hollow
- Managed services, 2012–2026, alive, hollow
- AI as metered API, 2021–2026, alive, fast

## 12. The Default Tech Stack

**Label:** STACKS · **Span:** 1960s → present

- Vendor-integrated stacks meant no assembly decision and total dependence
- LAMP's significance was economic: trying an idea cost almost nothing
- The acronym stack showed independent components could form a recognised whole
- Linux on servers with a Unix-like desktop has held for two decades
- Current stacks are assembled per project and mostly go unnamed

### Visualization (canvas `data-i="12"`)

- Mainframe / COBOL, 1965–1995, niche, hollow
- Proprietary Unix, 1980–2000, gone, hollow
- Windows / IIS / ASP, 1996–2012, niche, hollow
- LAMP, 1998–2015, niche, fast
- Java EE / Tomcat, 1999–2020, niche, hollow
- Linux + macOS norm, 2000–2026, alive, hollow
- Rails, Django, 2005–2026, alive, fast
- Node, React, 2012–2026, alive, fast
- Docker, K8s, Postgres, 2015–2026, alive, fast

## 13. Open Source & Shared Resources

**Label:** OPEN SOURCE · **Span:** 1960s → present

- Sharing was the original default; licensing was the change that prompted a response
- Copyleft used copyright law to compel openness downstream
- Linux showed dispersed volunteers could build something large and reliable
- Permissive licences saw wider corporate adoption precisely because they oblige nothing
- Open weights usually lack training data and carry usage limits — not open source as such

### Visualization (canvas `data-i="13"`)

- Academic sharing, 1960–1980, gone, hollow
- GNU & GPL, 1983–2026, alive, hollow
- BSD/MIT/Apache, 1988–2026, alive, hollow
- Linux kernel, 1991–2026, alive, fast
- Apache Foundation, 1999–2026, alive, hollow
- SourceForge, 1999–2012, gone, hollow
- RHEL support model, 2002–2026, alive, hollow
- GitHub, 2008–2026, alive, fast
- Kaggle, datasets, 2009–2026, alive, hollow
- Hugging Face, 2018–2026, alive, fast

## 14. Networking & Data Movement

**Label:** NETWORKING · **Span:** 1960s → present

- A movie-sized file: over a week on dial-up, under a minute on a gigabit link
- Shared cable meant adding computers made the network slower for everyone
- The switch gave each machine its own lane, so capacity scaled instead of dividing
- One fibre strand carries many colours at once, multiplying capacity without new cable
- AI chips now sit idle waiting for data rather than running short of calculating power

### Visualization (canvas `data-i="10"`)

- Dial-up, kilobits, 1960–2000, gone, hollow
- Shared 10 Mbps wire, 1980–1998, gone, hollow
- The switch, 1995–2026, alive, hollow
- Fibre backbones, 1996–2026, alive, hollow
- Home broadband, 1999–2026, alive, fast
- Wi-Fi & mobile data, 2000–2026, alive, fast
- 10–100 Gbps DC, 2007–2026, alive, hollow
- Flash / SSD, 2009–2026, alive, fast
- 400–800 Gbps fabrics, 2015–2026, alive, hollow

## 15. Data Query Languages

**Label:** QUERY LANGUAGES · **Span:** 1950s → present

- The access path once lived in application code, so every new question needed a new program
- SQL's move — state the result, let the optimiser choose the method — is what everything since repeats
- Standardising the grammar let the language outlive every engine underneath it
- MapReduce is the one clear reversal: scale was thought to require giving up the optimiser
- Plain language is an input to the pipeline, not a new link in it — it still compiles to SQL

### Visualization (canvas `data-i="14"`)

- Access path in code, 1955–1980, gone, hollow
- SQL (declarative), 1974–2026, alive, fast
- ANSI SQL standard, 1986–2026, alive, hollow
- Splunk SPL (pipeline), 2003–2026, alive, hollow
- MapReduce (hand-planned), 2004–2012, gone, fast
- Pig Latin, 2006–2015, gone, hollow
- HiveQL, 2008–2026, alive, hollow
- SQL on new engines, 2010–2026, alive, fast
- Spark SQL, 2015–2026, alive, fast
- Kusto KQL, LogScale, 2018–2026, alive, hollow
- Vector similarity, 2021–2026, alive, fast
- NL → generated SQL, 2023–2026, alive, fast

## 16. Big Tech by Technology Segment

**Label:** SEGMENTS · **Span:** 1960s → present

- Hardware vendors bundled software to make machines worth buying; software became the profitable half
- Portals assumed the web needed a curated front door, which search made unnecessary
- Free webmail broke the tie to an internet provider, turning an address into portable identity
- The stored graph of who knows whom was the asset until recommendation made it optional
- Cloud and enterprise platforms quietly fund the consumer products inside the same companies

### Visualization (canvas `data-i="15"`)

- Hardware vendors, bundled sw, 1960–1985, gone, hollow
- OS & office suite, 1981–2026, alive, fast
- Dial-up ISPs as gatekeepers, 1990–2004, gone, hollow
- Web portals & directories, 1994–2003, gone, fast
- E-commerce catalogues, 1995–2026, alive, hollow
- Free webmail, 1996–2026, alive, fast
- Standalone IM clients, 1997–2013, gone, hollow
- Search engines, 1998–2026, alive, fast
- Marketplaces & logistics, 2000–2026, alive, hollow
- Social networks (graph), 2003–2026, alive, fast
- Maps & local, 2005–2026, alive, hollow
- Video upload platforms, 2005–2026, alive, fast
- Cloud & enterprise SaaS, 2006–2026, alive, fast
- Subscription streaming, 2007–2026, alive, hollow
- Mobile photo & messaging, 2010–2026, alive, fast
- Ride hailing & delivery, 2010–2026, alive, hollow
- Ephemeral sharing, 2011–2026, alive, hollow
- Recommendation short video, 2016–2026, alive, fast
- Model providers, 2019–2026, alive, fast

## 17. Data File Formats

**Label:** FILE FORMATS · **Span:** 1960s → present

- CSV survives because no successor kept its one advantage of needing nothing installed
- JSON won by being the smallest format that could carry nested data
- Binary schemas made independent deployment possible — field numbering lets producers add fields safely
- Columnar storage changed what an analytical query costs, not merely how fast it runs
- Table formats moved version history out of documentation and into storage itself

### Visualization (canvas `data-i="16"`)

- Fixed-width text records, 1960–2005, niche, hollow
- CSV / TSV, 1972–2026, alive, hollow
- XML, 1998–2026, niche, fast
- JSON, 2001–2026, alive, fast
- Protobuf / Thrift, 2001–2026, alive, hollow
- YAML, 2004–2026, alive, hollow
- Markdown, 2004–2026, alive, hollow
- Avro, 2009–2026, alive, hollow
- Parquet / ORC (columnar), 2013–2026, alive, fast
- TOML, 2013–2026, alive, hollow
- JSONL / NDJSON, 2015–2026, alive, fast
- Arrow (in-memory columnar), 2016–2026, alive, hollow
- Delta / Iceberg / Hudi, 2017–2026, alive, fast

## Regeneration instructions

- **Layout:** h1, `.subtitle`, `.intro` callout, `.legend` paragraph, then one full-width `.obj-table` (`border-collapse: collapse`) with one `<tr>` per strand. Left `<td>` (42%, background `#fbfdff`) holds `.sec-label` (0.76em bold uppercase `#1f618d`, letter-spacing 0.6px), `.sec-title` (bold `#1a5276` 1.16em, numbered "N. Title" matching the section index), `.sec-span` (0.86em `#4a4a4a`, weight 600), and `.sec-bullets` `<ul>` (0.93em `#333`). Right `<td>` holds `.viz-wrap` (overflow-x auto) with one `<canvas data-i="N">`. Cell borders `1px solid #cfe0f0`, padding 16px 18px, vertical-align top.
- **Canvas order note:** the `data-i` index into the shared `SECTIONS` data array does not follow table order everywhere — section 11 (Delivery) uses `data-i="11"`, section 14 (Networking) uses `data-i="10"`, section 15 uses `data-i="14"`, section 16 uses `data-i="15"`, section 17 uses `data-i="16"`; all others match their position.
- **Intro callout style:** `.intro` — background `#f0f4f8`, left border `4px solid #2980b9`, padding 14px 18px, 0.95em. `.legend` — plain paragraph, 0.86em `#444`.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6, base font 15px; h1 1.9em `#1a5276`; subtitle `#555` 1.05em; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas / shared timeline renderer:** all charts share one fixed scale, YEAR_MIN 1940 to YEAR_MAX 2026, with no left gutter (the name is the mark). Canvases are `width: 100%; min-width: 520px; display: block`; height = `TOP(28) + nRows*ROW(27) + BOT(26)`; backing store scaled by `window.devicePixelRatio` (`setTransform(dpr,0,0,dpr,0,0)`); redraw on resize (debounced 140ms). Rows are drawn sorted by start year ascending. Decade gridlines 1950–2020: `#dde7ee` for years divisible by 50, else `#f2f6f9`, with year labels in `#666` 11px centered below. Per row: an arrival dot of radius 3.6 at the start year (filled with the row colour for `fast` adoption, else white fill with 1.4px coloured stroke); if the start predates 1940, a small left-pointing chevron (1.6px stroke) at the left edge instead of showing the true start; the row label in 13px (600 weight if fast) in the row colour, 7px right of the dot over a white halo rect, flipped left of the dot when it would overflow the right padding (left pad 10px, right pad 14px). The status/end fields exist in the data but the current renderer draws no trailing line, end tick, or dashed variant despite the legend describing them.
- **Row colour palette** (one colour per row, all clear 4.5:1 on white; row j of section si gets `PAL[(j*3 + si*5) % 14]`): `#1d4ed8`, `#0369a1`, `#0f766e`, `#15803d`, `#4d7c0f`, `#92400e`, `#b91c1c`, `#be123c`, `#be185d`, `#a21caf`, `#7e22ce`, `#4338ca`, `#155e75`, `#047857`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22` (page chrome); row colours as listed above.
