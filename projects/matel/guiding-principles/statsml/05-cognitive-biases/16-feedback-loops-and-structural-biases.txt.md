# Feedback Loops & Structural Biases

How predictions, omissions, and structural position create self-reinforcing distortions that compound over time.

## 1. Self-Fulfilling Prophecy

A model's prediction causes the very outcome it predicted, then that outcome reinforces the model in retraining. The feedback loop makes it impossible to observe the counterfactual.

- Model predicts churn → team deprioritizes customer → customer churns → model "validated"
- Credit score low → denied credit → can't build history → score stays low
- Counterfactual is unobservable: what would have happened without the prediction?

**Impact:** Creates reinforcing loops that appear to validate models that are actually causing outcomes. Requires randomized holdouts or causal inference to detect.

*Example: Churn model flags 1000 users. Team ignores them. 800 churn. Model retrained on this data shows 80% precision—but the precision was manufactured by the action.*

## 2. Omitted Variable Bias

When a confounding variable is left out of the model, the estimated effect of included variables absorbs the confounder's influence, producing a biased and inconsistent estimate.

- "Education → Income" effect is overstated when "Parental Wealth" is omitted
- Parental wealth drives both education access and income independently
- The observed coefficient conflates direct effect with confounded path

**Impact:** Policy decisions based on overstated coefficients waste resources. Adding controls or using instrumental variables is the remedy.

*Example: Naive regression shows each year of education adds $8k income. After controlling for parental wealth, the true effect is $3k.*

## 3. Collider Bias

Conditioning on a common effect (collider) of two independent causes induces a spurious association between those causes. The bias arises from selection: restricting analysis to a subset defined by the collider opens a non-causal path.

- Talent and looks are independent in the population
- Among famous people (fame requires talent OR looks), they appear negatively correlated
- Famous people who lack talent tend to be good-looking, and vice versa

**Impact:** Any filter on the dataset that is caused by two or more variables opens spurious paths. Hospital data, accepted applicants, published papers—all collider-conditioned.

*Example: Among hospitalized patients, disease severity and age appear correlated—but only because admission requires either severe disease OR old age.*

## 4. Position Bias → CTR Feedback Loop

Items shown in top positions get more clicks simply because they're visible. The system interprets clicks as relevance, boosts rank, which generates more clicks—a rich-get-richer (Matthew effect) loop.

- Position 1 gets 30% CTR regardless of quality; position 5 gets 4%
- Video sharing site autoplay counts as "engagement" though user never chose it
- New items never get impressions → permanent cold start starvation

**Impact:** Unlike credit scores, this loop operates at massive scale and compounds hourly. Mitigation requires explicit exploration (epsilon-greedy, Thompson sampling) or position-debiased CTR models.

*Example: Search result at position 1 "wins" every retraining cycle. After 10 cycles, removal shows it had no quality advantage—only position advantage.*
