# Anti-Patterns & Design Patterns for Data Systems

**Page type:** grid page (4-column card navigation grid with category labels and topic tags, philosophy callout at the bottom)
**HTML title tag:** Anti-Patterns & Design Patterns for Data Systems

**Subtitle:** Common mistakes in data collection, analysis, and ML — paired with the correct design. If you see yourself on the left: switch to the right.

## Cards

Each card links to a detail page under `anti-pattern-pairs/`. The card shows a colored uppercase category label, a numbered title, a one-line description, and a row of small topic tags.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | DATA COLLECTION | Impute All Missing Values with Column Mean | [anti-pattern-pairs/01-impute-mean.md](anti-pattern-pairs/01-impute-mean.md) | Destroys the signal that absence carries — missingness is often the strongest predictor. | missing data, MNAR, is_missing feature |
| 2 | DATA COLLECTION | UNION Multiple Sources Trusting Schema Match | [anti-pattern-pairs/02-union-sources.md](anti-pattern-pairs/02-union-sources.md) | Same column names + same types ≠ same semantics. "Amount" in USD vs cents vs local currency. | data integration, semantic mismatch, source profiling |
| 3 | DATA COLLECTION | Use Current State Queries for Historical Analysis | [anti-pattern-pairs/03-current-state-queries.md](anti-pattern-pairs/03-current-state-queries.md) | Current values overwrite history — restated earnings, changed addresses, updated codes. | temporal, SCD Type 2, point-in-time |
| 4 | PROFILING & SHAPE | Profile the Entire Column as One Distribution | [anti-pattern-pairs/04-profile-entire-column.md](anti-pattern-pairs/04-profile-entire-column.md) | A mean across two clusters is fiction — it represents a state that never occurs. | bimodal, sub-populations, segment profiling |
| 5 | PROFILING & SHAPE | Use One Bin Count for All Features | [anti-pattern-pairs/05-one-bin-count.md](anti-pattern-pairs/05-one-bin-count.md) | 20 bins for power-law: 1 bin has 80% of data, 19 bins are empty noise. | binning, adaptive resolution, Gini |
| 6 | PROFILING & SHAPE | Report a Single Summary Statistic | [anti-pattern-pairs/06-single-summary-stat.md](anti-pattern-pairs/06-single-summary-stat.md) | If bimodal with peaks at 20 and 80, mean=50 represents a state that NEVER occurs. | shape class, multi-modality, distribution |
| 7 | STATISTICAL TESTING | Report P-Value Without Effect Size | [anti-pattern-pairs/07-pvalue-no-effect-size.md](anti-pattern-pairs/07-pvalue-no-effect-size.md) | At n=100,000: enrichment 1.006× is "significant" but useless for classification. | effect size, Bonferroni, practical significance |
| 8 | STATISTICAL TESTING | Fine-Bucket Rare Classes | [anti-pattern-pairs/08-fine-bucket-rare.md](anti-pattern-pairs/08-fine-bucket-rare.md) | 20 buckets × 0.01% positive = 12 positives per bucket. Below any statistical minimum. | rare events, Mann-Whitney, minimum n |
| 9 | STATISTICAL TESTING | Check A/B Test Results Daily Until Significant | [anti-pattern-pairs/09-peeking-ab-test.md](anti-pattern-pairs/09-peeking-ab-test.md) | Peeking inflates FPR from 5% to 25-50%. Not computing power upfront means you don't know how long to run. | peeking, power analysis, sequential testing |
| 10 | FEATURE ENGINEERING | Use CTR / Engagement as Item Quality Feature | [anti-pattern-pairs/10-ctr-as-quality.md](anti-pattern-pairs/10-ctr-as-quality.md) | CTR is 90% determined by position shown, not item quality. | position bias, debiasing, holdout traffic |
| 11 | FEATURE ENGINEERING | Include Feature Without Checking Causal Direction | [anti-pattern-pairs/11-causal-direction.md](anti-pattern-pairs/11-causal-direction.md) | Collection calls "predict" default — but calls happen BECAUSE of default. | leakage, timeline audit, consequence-as-cause |
| 12 | FEATURE ENGINEERING | Remove Protected Attribute and Assume Fairness | [anti-pattern-pairs/12-remove-protected-attribute.md](anti-pattern-pairs/12-remove-protected-attribute.md) | Zip code, university, first name all correlate 0.8+ with gender — proxies remain. | fairness, proxy features, equalized odds |
| 13 | TRAINING & EVALUATION | Random Train/Test Split on Time-Ordered Data | [anti-pattern-pairs/13-random-split-temporal.md](anti-pattern-pairs/13-random-split-temporal.md) | Be aware of what data goes into training vs testing — make conscious decisions. | temporal split, data awareness, conscious decisions |
| 14 | TRAINING & EVALUATION | Evaluate with Accuracy on Imbalanced Data | [anti-pattern-pairs/14-accuracy-imbalanced.md](anti-pattern-pairs/14-accuracy-imbalanced.md) | "95% accuracy!" on 95/5 data = predicting majority class always. Recall = 0%. | precision-recall, PR curve, minority class |
| 15 | TRAINING & EVALUATION | Select Features on Full Dataset, Then Split | [anti-pattern-pairs/15-feature-selection-leak.md](anti-pattern-pairs/15-feature-selection-leak.md) | Feature selection used test data to decide which features matter — +5-10% inflated. | CV pipeline, SelectKBest, information leak |
| 16 | DEPLOYMENT & MONITORING | Deploy Model and Check Results Quarterly | [anti-pattern-pairs/16-deploy-check-quarterly.md](anti-pattern-pairs/16-deploy-check-quarterly.md) | Model silently degrades for 3 months — thousands of wrong predictions served. | monitoring, drift detection, alerting |
| 17 | DEPLOYMENT & MONITORING | Trust Model Confidence at Face Value | [anti-pattern-pairs/17-uncalibrated-confidence.md](anti-pattern-pairs/17-uncalibrated-confidence.md) | Model says "85% probability" but was never calibrated — actual could be 50% or 95%. | calibration, Platt scaling, reliability diagram |
| 18 | DEPLOYMENT & MONITORING | Train on Batch Features, Serve with Real-Time | [anti-pattern-pairs/18-batch-serving-skew.md](anti-pattern-pairs/18-batch-serving-skew.md) | Batch SQL "30-day average" ≠ streaming pipeline — subtle differences compound silently. | training-serving skew, feature parity, unified pipeline |
| 19 | TRAINING & EVALUATION | Repeated Evaluation on Same Test Set | [anti-pattern-pairs/19-repeated-eval-same-test.md](anti-pattern-pairs/19-repeated-eval-same-test.md) | Each iteration fixes errors THIS dataset surfaces — other failure modes remain invisible. | test set rotation, overfitting to eval, generalization |
| 20 | TRAINING & EVALUATION | Publish Accuracy Without Sample Size or CI | [anti-pattern-pairs/20-publish-accuracy-no-ci.md](anti-pattern-pairs/20-publish-accuracy-no-ci.md) | "99% accuracy" on 100 examples looks identical to 99% on 100,000 — format erases the denominator. | confidence interval, sample size, reporting |
| 21 | TRAINING & EVALUATION | Fix Failures Without Quantifying Frequency | [anti-pattern-pairs/21-fix-without-quantifying.md](anti-pattern-pairs/21-fix-without-quantifying.md) | Spent 2 weeks on a 0.3% problem while a 12% failure class was never investigated. | prioritization, failure taxonomy, impact-first |
| 22 | DATA OPERATIONS | Cherry-Pick the Aggregation Level | [anti-pattern-pairs/22-cherry-pick-aggregation.md](anti-pattern-pairs/22-cherry-pick-aggregation.md) | Same metric, same data — per-user +2%, per-session +12%, per-click +40%. | independence, pseudoreplication, power users |
| 23 | DATA OPERATIONS | No source_id Logged with Ingested Data | [anti-pattern-pairs/23-no-source-id.md](anti-pattern-pairs/23-no-source-id.md) | 5 upstream systems → one table. Something wrong? Can't trace which source produced it. | provenance, source attribution, debugging |
| 24 | DATA OPERATIONS | No Row Count Assertion After JOIN | [anti-pattern-pairs/24-no-row-count-assertion.md](anti-pattern-pairs/24-no-row-count-assertion.md) | Users (10K) JOIN Orders (50K) = 50K rows. Nobody checked. Revenue/user wrong by 5×. | fanout, ETL validation, row count |
| 25 | DATA OPERATIONS | ETL Pipeline with No Quality Checks | [anti-pattern-pairs/25-etl-no-quality-checks.md](anti-pattern-pairs/25-etl-no-quality-checks.md) | Garbage at step 2 propagates silently to step 5 — discovered weeks later by user complaint. | validation gates, Great Expectations, dbt tests |
| 26 | DATA OPERATIONS | Overwrite Yesterday's Data Without Versioning | [anti-pattern-pairs/26-overwrite-without-versioning.md](anti-pattern-pairs/26-overwrite-without-versioning.md) | Bug yesterday → today's table wrong. Yesterday's table gone — can't compare, can't roll back. | partitioning, rollback, date-partitioned |
| 27 | DATA OPERATIONS | Hard-Code Thresholds from One Dataset | [anti-pattern-pairs/27-hardcode-thresholds.md](anti-pattern-pairs/27-hardcode-thresholds.md) | "Anomaly if > 1000" works on THIS dataset. New client: normal values are 5000+. | self-calibrating, percentile-based, magic numbers |
| 28 | DATA OPERATIONS | Relative Time in Pipeline Queries | [anti-pattern-pairs/28-relative-time-queries.md](anti-pattern-pairs/28-relative-time-queries.md) | NOW() - INTERVAL shifts on retry — different data, no error, "success." | idempotent, absolute time, backfill |
| 29 | AI & LLM ANALYSIS | AI Agrees with Everything You Say | [anti-pattern-pairs/29-ai-sycophancy.md](anti-pattern-pairs/29-ai-sycophancy.md) | Confirmation bias as a service — AI agreement is reflexive, not evidence-based. | sycophancy, counter-arguments, critical thinking |
| 30 | AI & LLM ANALYSIS | Using LLM Output as Ground Truth | [anti-pattern-pairs/30-llm-as-ground-truth.md](anti-pattern-pairs/30-llm-as-ground-truth.md) | Output LOOKS authoritative but may be completely fabricated — hallucinated statistics. | hallucination, verification, hypothesis |
| 31 | AI & LLM ANALYSIS | Rephrasing Until AI Gives the Answer You Want | [anti-pattern-pairs/31-rephrase-until-agrees.md](anti-pattern-pairs/31-rephrase-until-agrees.md) | Tried 5 phrasings until AI confirmed your bias — you p-hacked the AI. | prompt p-hacking, neutral framing, first answer |

## Callout (philosophy box, placed AFTER the grid)

**How to use:** For any decision in your pipeline, find the anti-pattern that matches what you're about to do. If you see yourself on the left side: switch to the right side. Every anti-pattern on the left has burned real teams with real consequences — often silently for months before discovery.

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** nav-grid style (see `docs/statsml/ui-templates/02-nav-grid`). Single page: h1, `.subtitle` paragraph, one `.nav-grid` of `.nav-card` anchors, then one `.philosophy` callout below the grid.
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap.
- **Links:** the table above links to the `.md` versions for navigation in markdown; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num" style="color:CATEGORY_COLOR">CATEGORY</div>`, `<h3>N. Title</h3>` (unpadded index number matching the file index), `<p>description</p>`, and `<div class="topics">` with one `<span class="topic-tag">` per tag.
- **Category label colors (inline styles):** DATA COLLECTION `#e67e22`; PROFILING & SHAPE `#2980b9`; STATISTICAL TESTING `#8e44ad`; FEATURE ENGINEERING `#e74c3c`; TRAINING & EVALUATION `#1a5276`; DEPLOYMENT & MONITORING `#27ae60`; DATA OPERATIONS `#d35400`; AI & LLM ANALYSIS `#7f8c8d`.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`; hover: border `#e74c3c`, `translateY(-2px)`. `.card-num` 0.72em, weight 700, uppercase, 0.5px letter-spacing; h3 `#1a3a4a` 1em; description 0.85em `#555`. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em `#666`, flex-wrapped with 4px gap.
- **Callout style:** `.philosophy` — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, margin 30px 0, 0.9em.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. No canvases on this page; any canvases elsewhere use `window.devicePixelRatio` scaling.
