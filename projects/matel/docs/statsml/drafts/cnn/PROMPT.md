# Prompt: Reproduce CNN Shape Classifier (v3-full)

Use this prompt with a GenAI coding assistant to reproduce the CNN shape classification work from scratch.

---

## Goal

Build a CNN that classifies the **shape** of a data distribution from its histogram image. Think of it as "MNIST for distribution shapes" — given a 64x64 grayscale rendering of a histogram, classify it into one of 11 shape classes.

## Shape Classes (11)

| Class | Description | Example distributions |
|-------|-------------|----------------------|
| bell | Symmetric unimodal | Normal, t with high df, beta(a≈b) |
| right_skew | Tail extends right | Lognormal, gamma, chi-squared |
| left_skew | Tail extends left | Reflected lognormal, beta(a>b) |
| heavy_tail | Fat tails both sides | Student-t (low df), Laplace, Cauchy |
| bimodal | Two peaks | Mixture of two normals |
| multimodal | Three or more peaks | Mixture of 3+ components |
| u_shaped | Mass at both extremes | Beta(a<1, b<1) |
| spike | Extreme concentration at one point | 80-95% at single value + spread |
| descending | Monotone decrease | Exponential, Pareto, geometric |
| ascending | Monotone increase | Reflected exponential, power |
| zero_inflated | Point mass at zero + continuous tail | Insurance claims, capital gains |

## Input Representation

Render the data as a **64x64 single-channel grayscale image**:

- Histogram bars at intensity **1.0** (bright white)
- Per-bucket SE (standard error) band at intensity **0.5** (mid-gray), with band-bar overlap at **0.75**
- Background at **0.0** (black)
- Use **23 bins** (fixed)
- Clip data to [1st, 99th] percentile before binning
- SE per bucket = std(values_in_bucket) / sqrt(count_in_bucket)

The SE band encodes uncertainty — small samples get wide bands, large samples get narrow bands. This gives the CNN a signal about sample reliability.

## Broad Groups

Classes fall into three broad families (useful for hierarchical classification or coarse filtering):

| Group | Classes |
|-------|---------|
| mountain | bell, right_skew, left_skew, heavy_tail |
| valley | bimodal, multimodal, u_shaped |
| spike | spike, descending, ascending, zero_inflated |

## Synthetic Data Generation

Generate training data programmatically (no real datasets needed for training):

- For each of the 11 classes, write multiple generator functions using different parametric families (e.g., bell has 9 variants: pure normal, normal+uniform mix, narrow+wide mixture, blood-pressure-like, credit-score-like, mild-lognormal, mild-beta, high-shape gamma, shifted-normal+tail)
- Vary **sample sizes** across: 50, 100, 200, 400, 800, 1500, 3000, 5000
- This is critical — the classifier must handle noisy small-sample histograms AND smooth large-sample ones
- Apply **bin jitter** during training: randomly vary bin count ±2 from 23 for augmentation

### Dataset Sizes

| Split | Samples per class | Total |
|-------|-------------------|-------|
| Training export | 300 | 3,300 |
| Eval export | 150 | 1,650 |
| Training (in-memory) | 5,000 | 55,000 |
| Validation (in-memory) | 1,000 | 11,000 |

## Architecture

### Two model variants (train both):

**1. Generative (independent sigmoid heads)**
- Shared CNN backbone → shared FC → 11 independent binary heads
- Each head outputs 0-1 via sigmoid: "how much does this look like [class]?"
- Scores are independent (can sum > 100%)
- Loss: BCEWithLogitsLoss

**2. Discriminative (softmax)**
- Same backbone → single FC → 11-class softmax
- Picks exactly one winner (mutually exclusive)
- Loss: CrossEntropyLoss

### CNN Backbone (shared)
```
Conv2d(1, 32, 5, pad=2) → BN → ReLU → MaxPool(2)     # 64→32
Conv2d(32, 64, 3, pad=1) → BN → ReLU → MaxPool(2)    # 32→16
Conv2d(64, 128, 3, pad=1) → BN → ReLU → MaxPool(2)   # 16→8
Conv2d(128, 128, 3, pad=1) → BN → ReLU → AdaptiveAvgPool(4)  # 8→4
Flatten → Linear(2048, 256) → ReLU → Dropout(0.4)
Linear(256, 128) → ReLU → Dropout(0.3)
```

## Training Strategy (Generative)

**Phase 1: Joint training** (20 epochs)
- Train backbone + all heads together on multi-label data
- 5000 samples per class for training, 1000 for validation
- Adam, lr=0.001, weight_decay=1e-4, CosineAnnealing

**Phase 2: Per-class fine-tuning** (20 epochs per head)
- Freeze backbone, train each head independently
- Use **hard negative mining**: 60% of negatives come from known confusion pairs, 40% from all other classes
- 8000 pos + 8000 neg per head
- Adam, lr=0.003

### Confusion Pairs (for hard negative mining)
```
bell ← [bimodal, u_shaped, spike]
right_skew ← [descending, heavy_tail]
left_skew ← [ascending, bell]
heavy_tail ← [bell, bimodal]
bimodal ← [u_shaped, heavy_tail, multimodal]
multimodal ← [bimodal, u_shaped]
u_shaped ← [bimodal, multimodal]
spike ← [zero_inflated, descending]
descending ← [right_skew, spike]
ascending ← [left_skew]
zero_inflated ← [spike, descending]
```

## Training Strategy (Discriminative)

Single phase, 25 epochs:
- 5000 samples per class training, 1000 validation
- Adam, lr=0.001, weight_decay=1e-4, CosineAnnealing(T_max=25)
- Track top-1 and top-2 accuracy, save best top-1

## Evaluation

1. **Per-class top-1 and top-2 accuracy** on held-out synthetic data (different seed)
2. **Confusion matrix** — which classes get misclassified as which
3. **Agreement between generative and discriminative** — they should agree ~95%+
4. **Real data validation** — run on actual dataset features (e.g., Ames Housing, Adult Census) and verify predictions make intuitive sense

## Expected Results

- Top-1 accuracy: ~92-93%
- Top-2 accuracy: ~99%
- Worst confusion: bell↔heavy_tail, bimodal↔u_shaped, right_skew↔descending
- False positive rates on multimodal and u_shaped should be near 0%

## Dependencies

- Python 3.10+
- PyTorch
- NumPy

## Running

```bash
# Train generative model (includes dataset export)
python train_generative.py

# Train discriminative model (imports generators from generative script)
python train_discriminative.py

# Export datasets only (for inspection)
python train_generative.py --export-only
```

## Key Design Decisions

1. **Why grayscale image instead of raw histogram vector?** The CNN learns spatial patterns (peaks, tails, valleys) that are translation-invariant. A 1D vector would work but the 2D conv approach handles varied bin counts and visual patterns more robustly.

2. **Why SE bands?** They encode sample size information directly into the image. A noisy 50-sample histogram looks different from a smooth 5000-sample one — the CNN sees this via band width.

3. **Why independent sigmoid heads (generative)?** Some distributions are legitimately ambiguous (e.g., a mild right-skew that's almost bell-shaped). Independent scores let you see "70% bell, 40% right_skew" instead of forcing a single winner.

4. **Why hard negative mining in phase 2?** Without it, each head gets easy negatives (spike vs bell is obvious). The confusion pairs force the head to learn fine-grained boundaries (bimodal vs u_shaped is hard).

5. **Why 23 bins?** Enough resolution to capture shape detail, few enough that small samples (n=50) still produce meaningful histograms. The ±2 jitter during training makes the CNN robust to bin count choice.
