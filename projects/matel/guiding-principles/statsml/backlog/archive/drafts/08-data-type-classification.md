# Data Type Classification

**Page type:** other (single-page reference doc: h1, subtitle, three h2 sections with tables and one pre block; no canvases)
**HTML title tag:** Data Type Classification

**Subtitle:** Don't assign one type per column. Report the distribution. Each value gets classified independently, then aggregated into column-level scores.

## 1. Value-Level Type Classes

Each value falls into exactly one class:

| Class | Rule | Examples |
|-------|------|----------|
| `missing` | null, NaN, empty, "NA", "?" | `NaN, "", None` |
| `int_binary0` | Integer zero | `0` |
| `int_binary1` | Integer one | `1` |
| `flt_binary0` | Float zero (has decimal point) | `0.0` |
| `flt_binary1` | Float one (has decimal point) | `1.0` |
| `strictly_int` | Integer, not 0 or 1 | `42, -7, 2024` |
| `strict_float` | Genuine decimal precision | `3.14, -0.5, 98.6` |
| `string_fixed_N` | String of exactly N chars | `"CA", "90210"` |
| `string_var` | Variable-length string | `"Private", "Self-emp"` |

## 2. Column-Level Scores

Each score = fraction of non-missing values that qualify. Multiple scores can be high simultaneously — that's the point.

Rendered as a `<pre>` code block, verbatim:

```
flt_num = (n_int + n_float) / n_valid     — any numeric value
int_num = n_int / n_valid                  — integers only (no decimals)
bin     = 1.0 if nunique == 2 else 0.0     — exactly 2 distinct values
int_cat = int_num if (all_int AND unique ≤ 30) else 0.0
cat     = n_string / n_valid               — string values

Set inclusion: flt_num ⊃ int_num ⊃ binary
```

## 3. Multi-Classification (the key concept)

A column can score high on multiple types simultaneously. This is not ambiguity — it's information.

| Column | Sample Values | Scores | What This Tells You |
|--------|---------------|--------|---------------------|
| **Age** | 39, 50, 38, 53, 28 | int_num=1.0 | Pure continuous integer. Treat as numeric. |
| **Education_Num** | 13, 9, 7, 14, 5, 16 | int_num=1.0, int_cat=1.0 | Integer AND categorical (16 unique ≤30). Pipeline runs both numeric and categorical analysis. |
| **Binary_Flag** | 1, 0, 0, 1, 0, 0, 1 | int_num=1.0, int_cat=1.0, bin=1.0 | All three: binary ⊂ int_cat ⊂ int_num. Most specific wins: binary. |
| **Capital_Gain** | 0, 0, 0, 0, 15024, 0, 7688 | int_num=1.0, bin=0.0 | Value dist: int_binary0=92%, strictly_int=8%. "Mostly zero" visible in the distribution. |
| **Mixed_Dirty** | 42, 38.5, "N/A", 51, "refused" | int_num=0.53, flt_num=0.80, cat=0.13 | Fractional scores reveal data quality: 13% string contamination in mostly-numeric column. |
| **Zipcode (as int)** | 90210, 10001, 2134 | int_num=1.0, int_cat=0.0 | Looks numeric but isn't. Type scores alone can't catch this — LLM regex or domain knowledge needed. |
| **Sex** | "Male", "Female" | cat=1.0, bin=1.0 | String + binary. Both fire independently. |

**Key insight:** Traditional type detection says "this column IS numeric." Value-level distribution says "99.7% of values are integers, 0.3% are something else — here's what." The 0.3% IS the signal.

## Regeneration instructions

- **Layout:** single-page long doc. Document order: h1, `.subtitle` paragraph, then three h2 sections. Section 1: intro line + 3-column table (Class / Rule / Examples) with class names and example values in `<code>`. Section 2: intro line + `<pre>` block with the score formulas. Section 3: intro line + 4-column table (Column / Sample Values / Scores / What This Tells You) with column names in `<strong>`, then closing `<p>` with the key insight (label in `<strong>`).
- **Page style:** body -apple-system/system sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6. h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px solid `#2980b9` bottom border, padding-bottom 8px, margin 40px 0 15px; paragraphs `#333` 0.95em; subtitle `#666` 1.05em.
- **Tables:** width 100%, border-collapse collapse, 0.92em; th/td border `1px solid #ddd`, padding 10px 12px, left-aligned; th background `#f0f4f8` color `#1a5276` weight 600; even rows background `#fafafa`.
- **Code/pre:** `code` background `#e8f0f8`, padding 2px 6px, radius 3px, 0.9em, color `#1a5276`. `pre` background `#f8f9fa`, border `1px solid #e0e0e0`, radius 6px, padding 14px 16px, SF Mono monospace 0.88em, horizontal scroll.
- **Palette:** primary blue `#1a5276`, accent `#2980b9` (plus project palette: green `#27ae60`, red `#e74c3c`, orange `#e67e22` — unused on this page).
- No canvases on this page; no nav bar, no back/home links. (Other pages' canvases use `window.devicePixelRatio` scaling.)
