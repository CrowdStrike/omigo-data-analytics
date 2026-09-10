# Linear Algebra & Numerical Computing

**Page type:** grid page (tutorials category grid: single flat 4-column nav-grid of cards with topic tags)
**HTML title tag:** Linear Algebra & Numerical Computing

**Subtitle:** The matrix toolkit behind data science — what a table of numbers can do, the structure hidden inside it, and how computers crunch it without amplifying rounding error.

## Cards

Each card links to a topic page under `linear-algebra-numerical/`. The card shows a colored uppercase subcategory label (`.card-num`), a numbered title, a one-line description, and 2-4 topic tag pills. All cards sit in one flat `.nav-grid`; the colored labels carry the grouping.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | WHAT A MATRIX DOES | Matrix Algebra Basics | [03-linear-algebra-and-numerical-computing/01-matrix-algebra-basics.md](03-linear-algebra-and-numerical-computing/01-matrix-algebra-basics.md) | A table of numbers with rules for combining tables — transpose flips it, the identity leaves it alone, the inverse undoes it, and order of multiplication matters. | transpose, identity, inverse, order matters |
| 2 | WHAT A MATRIX DOES | Eigenvalues & Eigenvectors | [03-linear-algebra-and-numerical-computing/02-eigenvalues-and-eigenvectors.md](03-linear-algebra-and-numerical-computing/02-eigenvalues-and-eigenvectors.md) | A direction the matrix stretches but never turns, with the eigenvalue as the stretch factor — PCA is just finding these directions in a data cloud. | stretch directions, stretch factor, PCA |
| 3 | WHAT A MATRIX DOES | SVD (Singular Value Decomposition) | [03-linear-algebra-and-numerical-computing/03-svd-singular-value-decomposition.md](03-linear-algebra-and-numerical-computing/03-svd-singular-value-decomposition.md) | Rewrite any table as a few simple patterns ranked by size — keep the big ones and you get compression, recommendations, and denoising from one trick. | ranked patterns, compression, recommendations |
| 4 | STRUCTURE & INVERTIBILITY | Matrix Rank & Linear Independence | [03-linear-algebra-and-numerical-computing/04-matrix-rank-and-linear-independence.md](03-linear-algebra-and-numerical-computing/04-matrix-rank-and-linear-independence.md) | Rank counts how many columns carry genuinely new information — a column built from other columns adds width but nothing the data didn't already say. | redundant columns, rank, independence |
| 5 | STRUCTURE & INVERTIBILITY | Determinants | [03-linear-algebra-and-numerical-computing/05-determinants.md](03-linear-algebra-and-numerical-computing/05-determinants.md) | One number that says how much a matrix stretches area or volume — and det = 0 means the matrix flattens everything and cannot be undone. | area scaling, det = 0, invertibility |
| 6 | MATRICES IN PRACTICE | Covariance & Correlation Matrices | [03-linear-algebra-and-numerical-computing/06-covariance-and-correlation-matrices.md](03-linear-algebra-and-numerical-computing/06-covariance-and-correlation-matrices.md) | Every pairwise "do these two move together?" answer, packed into one square table — the object PCA decomposes and a Kalman filter updates. | pairwise co-movement, correlation, PCA input |
| 7 | MATRICES IN PRACTICE | Homogeneous Coordinates | [03-linear-algebra-and-numerical-computing/07-homogeneous-coordinates.md](03-linear-algebra-and-numerical-computing/07-homogeneous-coordinates.md) | One extra coordinate — a plain 1 — turns "slide the object" into a matrix multiplication, so rotate, scale, and move all chain into a single 4×4. | extra 1, translation, graphics |
| 8 | MATRICES IN PRACTICE | Numerical Stability | [03-linear-algebra-and-numerical-computing/08-numerical-stability.md](03-linear-algebra-and-numerical-computing/08-numerical-stability.md) | Computers do arithmetic with ~16 digits, and some computations quietly amplify the rounding — the classic self-inflicted wound is inverting a matrix when solving would do. | rounding error, conditioning, don't invert |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorials category grid. Single page: h1, `.subtitle` paragraph, then one flat `.nav-grid` of `.nav-card` anchors (no h2 section headings).
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, 15px top margin; responsive: 3 columns below 1400px, 2 below 1100px, 1 below 600px.
- **Links:** the tables above link to the `.md` versions for markdown navigation; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">SUBCATEGORY LABEL</div>`, `<h3>N. Topic Title</h3>` (unpadded index number matching the 2-digit zero-padded file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` pills.
- **Category label colors:** applied by a small script mapping `.card-num` text to color — "WHAT A MATRIX DOES" `#2980b9`, "STRUCTURE & INVERTIBILITY" `#27ae60`, "MATRICES IN PRACTICE" `#8e44ad`; the CSS default for `.card-num` is `#2980b9`, 0.75em bold, 4px bottom margin.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`, transition on border-color/transform; hover: border `#2980b9`, `translateY(-2px)`. h3 `#1a3a4a` 1em with 6px bottom margin; description `#555` 0.85em. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em `#666`, in a flex-wrap row with 4px gap and 8px top margin.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; universal `* { margin:0; padding:0; box-sizing:border-box }` reset; h1 1.8em `#2980b9` with 10px bottom margin; subtitle `#666` 1.05em with 30px bottom margin. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22` (page accents here use `#2980b9`, `#27ae60`, `#8e44ad`).
- **Canvases:** none on this page; any canvases elsewhere in this series use `window.devicePixelRatio` scaling.
