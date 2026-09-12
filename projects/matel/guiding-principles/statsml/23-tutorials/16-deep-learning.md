# Deep Learning

**Page type:** grid page (tutorials category grid: single flat 4-column card grid)
**HTML title tag:** Deep Learning

**Subtitle:** How stacked layers of simple arithmetic units learn to see, read, and predict — from a single neuron to transformers.

## Cards

Each card links to a topic page under `deep-learning/`. The card shows a colored uppercase subcategory label (`.card-num`), a numbered title, a one-line description, and a row of topic-tag pills. All cards sit in one flat `.nav-grid`; the colored labels carry the grouping.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | NEURAL NETWORK BASICS | What a Neuron Computes | [16-deep-learning/01-what-a-neuron-computes.md](16-deep-learning/01-what-a-neuron-computes.md) | A neuron is just a weighted sum of its inputs plus a nudge, passed through a simple squashing rule. | weighted sum, bias, building block |
| 2 | NEURAL NETWORK BASICS | Layers & Depth | [16-deep-learning/02-layers-and-depth.md](16-deep-learning/02-layers-and-depth.md) | Stacking neurons into layers lets each layer build on the last, turning raw inputs into richer features. | hidden layers, depth, feature building |
| 3 | NEURAL NETWORK BASICS | Activation Functions | [16-deep-learning/03-activation-functions.md](16-deep-learning/03-activation-functions.md) | The small nonlinear bends between layers that let a network draw curved boundaries instead of straight lines. | ReLU, sigmoid, nonlinearity |
| 4 | NEURAL NETWORK BASICS | The Forward Pass | [16-deep-learning/04-the-forward-pass.md](16-deep-learning/04-the-forward-pass.md) | How an input flows layer by layer through the network to become a prediction — the whole trip, step by step. | inference, layer by layer, prediction |
| 5 | NEURAL NETWORK BASICS | Backpropagation | [16-deep-learning/05-backpropagation.md](16-deep-learning/05-backpropagation.md) | How the network learns from a wrong answer by tracing the error backwards and nudging every weight a little. | gradients, error signal, learning |
| 6 | ACTIVATIONS & OUTPUTS | Sigmoid | [16-deep-learning/06-sigmoid.md](16-deep-learning/06-sigmoid.md) | The S-shaped curve that squashes any number into a value between 0 and 1 that reads as a probability. | S-curve, 0 to 1, probability |
| 7 | ACTIVATIONS & OUTPUTS | ReLU | [16-deep-learning/07-relu.md](16-deep-learning/07-relu.md) | max(0, x) — pass positives through unchanged, zero out negatives — the bare clamp that made deep networks trainable. | max(0, x), hinge rule, deep training |
| 8 | ACTIVATIONS & OUTPUTS | Softmax | [16-deep-learning/08-softmax.md](16-deep-learning/08-softmax.md) | Turns a list of raw scores into a probability distribution — every output positive, all summing to 1, the favorite still on top. | probability distribution, raw scores, multi-class |
| 9 | ACTIVATIONS & OUTPUTS | Argmax vs Softmax | [16-deep-learning/09-argmax-vs-softmax.md](16-deep-learning/09-argmax-vs-softmax.md) | The same scores answered two ways — argmax gives one hard choice, softmax gives soft shares that sum to 1. | hard vs soft, one-hot, confidence |
| 10 | TRAINING REALITIES | Batches & Epochs | [16-deep-learning/10-batches-and-epochs.md](16-deep-learning/10-batches-and-epochs.md) | Why training data is fed in small chunks, and what one full pass over the dataset actually means. | mini-batches, epochs, training loop |
| 11 | TRAINING REALITIES | Why GPUs | [16-deep-learning/11-why-gpus.md](16-deep-learning/11-why-gpus.md) | Neural network math is millions of identical multiplications, and GPUs do thousands of them at once. | parallelism, matrix math, hardware |
| 12 | TRAINING REALITIES | Dropout | [16-deep-learning/12-dropout.md](16-deep-learning/12-dropout.md) | Randomly silencing neurons during training so the network cannot lean on any single one — and overfits less. | regularization, overfitting, random masking |
| 13 | TRAINING REALITIES | Transfer Learning | [16-deep-learning/13-transfer-learning.md](16-deep-learning/13-transfer-learning.md) | Starting from a network already trained on a huge dataset and retuning it for your smaller problem. | pretrained models, fine-tuning, small data |
| 14 | TRAINING REALITIES | Batch Normalization | [16-deep-learning/14-batch-normalization.md](16-deep-learning/14-batch-normalization.md) | Curving each layer's outputs like exam scores — centered at 0, spread 1 — so later layers train against a steady ruler. | normalization, stable training, exam curving |
| 15 | TRAINING REALITIES | Vanishing & Exploding Gradients | [16-deep-learning/15-vanishing-and-exploding-gradients.md](16-deep-learning/15-vanishing-and-exploding-gradients.md) | Backprop multiplies one factor per layer, so deep chains can shrink the learning signal to nothing or blow it up. | multiplication chain, deep networks, training failure |
| 16 | TRAINING REALITIES | Knowledge Distillation & Quantization | [16-deep-learning/16-knowledge-distillation-and-quantization.md](16-deep-learning/16-knowledge-distillation-and-quantization.md) | Shrinking a huge model onto a phone — a small student learns from the big model's soft answers, then its numbers are rounded onto a coarser grid. | teacher & student, model compression, deployment |
| 17 | TRAINING REALITIES | Data Augmentation | [16-deep-learning/17-data-augmentation.md](16-deep-learning/17-data-augmentation.md) | When you can't collect more data, make more — flip, tilt, and brighten the examples you already have. | flips & crops, free data, small datasets |
| 18 | ARCHITECTURES | CNNs: Seeing Patterns | [16-deep-learning/18-cnns-seeing-patterns.md](16-deep-learning/18-cnns-seeing-patterns.md) | Networks that slide small pattern detectors across an image, spotting edges first and objects later. | convolution, images, filters |
| 19 | ARCHITECTURES | RNNs: Remembering Sequences | [16-deep-learning/19-rnns-remembering-sequences.md](16-deep-learning/19-rnns-remembering-sequences.md) | Networks that read one step at a time and carry a running memory, so earlier items shape later predictions. | sequences, hidden state, time series |
| 20 | ARCHITECTURES | Transformers & Attention | [16-deep-learning/20-transformers-and-attention.md](16-deep-learning/20-transformers-and-attention.md) | Instead of reading in order, every word looks at every other word and decides which ones matter most. | attention, language models, context |
| 21 | ARCHITECTURES | Embeddings | [16-deep-learning/21-embeddings.md](16-deep-learning/21-embeddings.md) | Turning words, users, or products into lists of numbers so that similar things end up close together. | vectors, similarity, representation |
| 22 | ARCHITECTURES | Convolution | [16-deep-learning/22-convolution.md](16-deep-learning/22-convolution.md) | A tiny window of weights slides across the data and scores every position for one pattern, finding it anywhere. | sliding window, filter, pattern detector |
| 23 | GENERATIVE & SELF-SUPERVISED LEARNING | Autoencoders | [16-deep-learning/23-autoencoders.md](16-deep-learning/23-autoencoders.md) | A network that squeezes its input through a tiny bottleneck and rebuilds it — whatever survives the squeeze is the essence. | bottleneck, compress & rebuild, anomaly detection |
| 24 | GENERATIVE & SELF-SUPERVISED LEARNING | VAEs | [16-deep-learning/24-vaes.md](16-deep-learning/24-vaes.md) | An autoencoder whose codes are fuzzy blobs packed around zero, so any point in the code space decodes to something plausible. | latent space, smooth codes, generation |
| 25 | GENERATIVE & SELF-SUPERVISED LEARNING | GANs | [16-deep-learning/25-gans.md](16-deep-learning/25-gans.md) | A forger network makes fake data and a detective network calls real or fake — each round of feedback sharpens both. | adversarial, generator, discriminator |
| 26 | GENERATIVE & SELF-SUPERVISED LEARNING | Diffusion Models | [16-deep-learning/26-diffusion-models.md](16-deep-learning/26-diffusion-models.md) | A model learns to remove a little noise at a time — run that cleanup on pure static, and a brand-new picture comes out. | denoising, step by step, image generation |
| 27 | GENERATIVE & SELF-SUPERVISED LEARNING | Self-Supervised & Contrastive Learning | [16-deep-learning/27-self-supervised-and-contrastive-learning.md](16-deep-learning/27-self-supervised-and-contrastive-learning.md) | When nobody has labeled the data, manufacture labels from the data itself — two edits of one photo are "same", everything else is "different". | no labels, contrastive, pretext task |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorials category grid page (nav-grid style, see `docs/statsml/ui-templates/02-nav-grid`). Single page: h1, `.subtitle` paragraph, then one flat `.nav-grid` of `.nav-card` anchors (no h2 section headings).
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, 15px top margin; responsive: 3 columns below 1400px, 2 below 1100px, 1 below 600px.
- **Links:** the tables above link to `.md` versions for markdown navigation; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead (files zero-padded, e.g. `deep-learning/01-what-a-neuron-computes.html`).
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">SUBCATEGORY</div>`, `<h3>N. Title</h3>` (unpadded index number matching the file index, running 1..27 across the whole page; cards stay in ascending number order within each section), `<p>description</p>`, and `<div class="topics">` with one `<span class="topic-tag">` per tag.
- **Category label colors** (applied by a small script mapping `.card-num` text to color): NEURAL NETWORK BASICS `#2980b9`, ACTIVATIONS & OUTPUTS `#e67e22`, TRAINING REALITIES `#27ae60`, ARCHITECTURES `#8e44ad`, GENERATIVE & SELF-SUPERVISED LEARNING `#16a085`; default `.card-num` color `#2980b9`.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`; hover: border `#2980b9`, `translateY(-2px)`. `.card-num` 0.75em bold; h3 `#1a3a4a` 1em; description `#555` 0.85em. `.topic-tag`: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em, `#666`; `.topics` is flex with 4px gap, 8px top margin.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; h1 1.8em `#2980b9`; subtitle `#666` 1.05em. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- No canvases on this page; where canvases appear elsewhere in this project they use `window.devicePixelRatio` scaling.
