"""
CNN Shape Classification v3-full: Generative with Grayscale + Band — ALL classes.

Same architecture as v3 (64x64 single-channel, SE band, independent sigmoid heads)
but with all 11 fine-grained shape classes instead of 3 broad ones.

Input: 64x64 single-channel grayscale image
  - Histogram bars rendered bright (~1.0 intensity)
  - Per-bucket variance band rendered mid-gray (~0.5 intensity)
  - Background black (0.0)

Classes (11):
  - bell: symmetric unimodal
  - right_skew: tail extends right (lognormal, gamma, chi-squared)
  - left_skew: tail extends left (reflected distributions)
  - heavy_tail: fat tails both sides (t, Laplace, Cauchy)
  - bimodal: two peaks
  - multimodal: three or more peaks
  - u_shaped: mass at both extremes, valley in center
  - spike: extreme concentration at a single point
  - descending: monotone decrease (exponential, Pareto)
  - ascending: monotone increase (reflected exponential, power)
  - zero_inflated: point mass at zero + continuous tail

Generative: each head outputs independent 0-1 score via sigmoid.
"""

import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader
import os
import csv
import json

# === Shape Classes (11 fine-grained) ===
CLASSES = [
    'bell', 'right_skew', 'left_skew', 'heavy_tail',
    'bimodal', 'multimodal', 'u_shaped',
    'spike', 'descending', 'ascending', 'zero_inflated',
]
NUM_CLASSES = len(CLASSES)
IMG_SIZE = 64
NUM_BINS = 23
SAMPLE_SIZES = (50, 100, 200, 400, 800, 1500, 3000, 5000)


# === Data Generators ===
# Each class has multiple generator variants for diversity

def gen_bell(rng, n):
    choice = rng.integers(0, 9)
    if choice == 0:
        return rng.normal(rng.uniform(30, 70), rng.uniform(5, 20), n)
    elif choice == 1:
        mu, sigma = rng.uniform(30, 70), rng.uniform(5, 15)
        normal_part = rng.normal(mu, sigma, int(n * 0.7))
        uniform_part = rng.uniform(mu - 2*sigma, mu + 2*sigma, n - int(n * 0.7))
        return np.concatenate([normal_part, uniform_part])
    elif choice == 2:
        mu, sigma = rng.uniform(30, 70), rng.uniform(5, 12)
        n1 = int(n * 0.7)
        narrow = rng.normal(mu, sigma * 0.6, n1)
        wide = rng.normal(mu, sigma * 1.8, n - n1)
        return np.concatenate([narrow, wide])
    elif choice == 3:
        # Blood pressure-like: roughly normal around center, slight tail
        main = rng.normal(rng.uniform(115, 125), rng.uniform(10, 15), int(n * 0.85))
        tail = rng.normal(rng.uniform(140, 155), rng.uniform(10, 15), n - int(n * 0.85))
        return np.concatenate([main, tail])
    elif choice == 4:
        # Credit score-like: bounded, slight skew but predominantly bell
        a = rng.uniform(8, 15)
        b = rng.uniform(6, 12)
        return rng.beta(a, b, n) * 550 + 300
    elif choice == 5:
        # Mild right-skew that's still bell-like: lognormal with small sigma
        # Visually dominated by central peak, moderate right tail
        return rng.lognormal(rng.uniform(3, 5), rng.uniform(0.15, 0.35), n)
    elif choice == 6:
        # Mild left-skew bell: beta(a, b) with a slightly > b
        a = rng.uniform(6, 12)
        b = rng.uniform(4, 8)
        return rng.beta(a, b, n) * rng.uniform(50, 200)
    elif choice == 7:
        # House-price-like: gamma with high shape (mild skew, mostly centered)
        shape = rng.uniform(8, 20)  # high shape = more symmetric
        scale = rng.uniform(5, 20)
        return rng.gamma(shape, scale, n)
    else:
        # Mild right-skew via shifted normal + small exponential tail
        mu = rng.uniform(40, 70)
        sigma = rng.uniform(8, 15)
        main = rng.normal(mu, sigma, int(n * 0.85))
        tail = mu + sigma + rng.exponential(sigma * 0.5, n - int(n * 0.85))
        return np.concatenate([main, tail])


def gen_right_skew(rng, n):
    choice = rng.integers(0, 8)
    if choice == 0:
        return rng.lognormal(2, rng.uniform(0.4, 1.2), n)
    elif choice == 1:
        return rng.gamma(rng.uniform(2.5, 8.0), rng.uniform(3, 15), n)
    elif choice == 2:
        return rng.chisquare(rng.uniform(4, 15), n)
    elif choice == 3:
        return rng.weibull(rng.uniform(1.8, 3.5), n) * rng.uniform(10, 50)
    elif choice == 4:
        return rng.gamma(rng.uniform(3, 7), rng.uniform(3, 12), n) + rng.uniform(5, 20)
    elif choice == 5:
        # Income-like: lognormal with salary band bumps
        base = rng.lognormal(10.5, 0.8, int(n * 0.7))
        bands = [30000, 50000, 75000, 100000]
        cluster_data = []
        for band in bands:
            ni = int(n * rng.uniform(0.03, 0.08))
            cluster_data.append(rng.normal(band, band * 0.05, ni))
        remaining = n - len(base) - sum(len(c) for c in cluster_data)
        if remaining > 0:
            cluster_data.append(rng.lognormal(11.5, 0.5, remaining))
        return np.concatenate([base] + cluster_data)
    elif choice == 6:
        # Taxi distance: spike at short + right tail
        short = rng.exponential(rng.uniform(1.5, 3), int(n * 0.6))
        medium = rng.normal(rng.uniform(5, 10), rng.uniform(2, 4), int(n * 0.25))
        long_t = rng.exponential(rng.uniform(15, 30), n - int(n * 0.6) - int(n * 0.25))
        return np.concatenate([short, medium, long_t])
    else:
        # Employee tenure: right-skewed with new-hire spike
        n_new = int(n * rng.uniform(0.15, 0.30))
        new = rng.exponential(0.5, n_new)
        rest = rng.exponential(rng.uniform(3, 7), n - n_new)
        return np.concatenate([new, rest])


def gen_left_skew(rng, n):
    choice = rng.integers(0, 5)
    if choice == 0:
        data = rng.lognormal(2, rng.uniform(0.5, 1.2), n)
        return -data + data.max() + rng.uniform(5, 20)
    elif choice == 1:
        data = rng.gamma(rng.uniform(2.5, 8.0), rng.uniform(3, 15), n)
        return -data + data.max() + rng.uniform(5, 20)
    elif choice == 2:
        return rng.beta(rng.uniform(5, 15), rng.uniform(1.5, 3.0), n) * rng.uniform(50, 100)
    elif choice == 3:
        a = rng.uniform(8, 20)
        return rng.beta(a, rng.uniform(2.5, 4.0), n) * rng.uniform(50, 100)
    else:
        # Credit score-like: pile-up near high end
        a = rng.uniform(5, 12)
        b = rng.uniform(2, 4)
        return rng.beta(a, b, n) * 550 + 300


def gen_heavy_tail(rng, n):
    choice = rng.integers(0, 4)
    if choice == 0:
        return rng.standard_t(rng.uniform(1.5, 4.0), n) * rng.uniform(5, 15) + rng.uniform(30, 70)
    elif choice == 1:
        return rng.laplace(rng.uniform(30, 70), rng.uniform(5, 15), n)
    elif choice == 2:
        center = rng.uniform(30, 70)
        sigma_core = rng.uniform(4, 8)
        n_core = int(n * rng.uniform(0.6, 0.75))
        core = rng.normal(center, sigma_core, n_core)
        tails = rng.normal(center, sigma_core * rng.uniform(2.5, 4.0), n - n_core)
        return np.concatenate([core, tails])
    else:
        return rng.standard_cauchy(n) * rng.uniform(3, 10) + rng.uniform(30, 70)


def gen_bimodal(rng, n):
    choice = rng.integers(0, 4)
    if choice == 0:
        sigma1, sigma2 = rng.uniform(3, 10), rng.uniform(3, 10)
        separation = rng.uniform(1.8, 5.0) * (sigma1 + sigma2) / 2
        mu1 = rng.uniform(20, 40)
        mu2 = mu1 + separation
        mix = rng.uniform(0.3, 0.7)
        n1 = int(n * mix)
        return np.concatenate([rng.normal(mu1, sigma1, n1), rng.normal(mu2, sigma2, n - n1)])
    elif choice == 1:
        # Heights: two overlapping populations
        mix = rng.uniform(0.4, 0.6)
        n1 = int(n * mix)
        pop1 = rng.normal(rng.uniform(162, 167), rng.uniform(5, 7), n1)
        pop2 = rng.normal(rng.uniform(175, 180), rng.uniform(5, 8), n - n1)
        return np.concatenate([pop1, pop2])
    elif choice == 2:
        # Hours: full-time + part-time peaks
        n_ft = int(n * rng.uniform(0.45, 0.65))
        ft = rng.normal(40, rng.uniform(1, 3), n_ft)
        n_pt = int(n * rng.uniform(0.20, 0.35))
        pt = rng.normal(rng.uniform(20, 25), rng.uniform(3, 6), n_pt)
        ot = rng.normal(rng.uniform(50, 55), rng.uniform(5, 10), max(1, n - n_ft - n_pt))
        return np.concatenate([ft, pt, ot])
    else:
        # Temperature: summer + winter
        n_summer = int(n * 0.5)
        summer = rng.normal(rng.uniform(75, 85), rng.uniform(5, 10), n_summer)
        winter = rng.normal(rng.uniform(30, 45), rng.uniform(8, 12), n - n_summer)
        return np.concatenate([summer, winter])


def gen_multimodal(rng, n):
    choice = rng.integers(0, 3)
    if choice == 0:
        num_modes = rng.integers(3, 6)
        base = rng.uniform(10, 30)
        means = [base]
        for _ in range(num_modes - 1):
            means.append(means[-1] + rng.uniform(12, 25))
        sigmas = [rng.uniform(2, 5) for _ in range(num_modes)]
        weights = rng.dirichlet(np.ones(num_modes) * 2)
        data = []
        for i in range(num_modes):
            ni = int(n * weights[i])
            data.append(rng.normal(means[i], sigmas[i], ni))
        remaining = n - sum(len(d) for d in data)
        if remaining > 0:
            data.append(rng.normal(means[0], sigmas[0], remaining))
        return np.concatenate(data)
    elif choice == 1:
        # File sizes: log-scale clusters
        n1 = int(n * rng.uniform(0.4, 0.6))
        small = np.exp(rng.normal(7, 1.2, n1))
        n2 = int(n * rng.uniform(0.25, 0.35))
        medium = np.exp(rng.normal(13, 0.8, n2))
        n3 = n - n1 - n2
        large = np.exp(rng.normal(16, 0.6, max(1, n3)))
        return np.concatenate([small, medium, large])
    else:
        # Pricing spikes: multiple price points
        prices = [9.99, 19.99, 29.99, 49.99, 99.99]
        n_prices = rng.integers(3, 6)
        selected = rng.choice(prices, n_prices, replace=False)
        weights = rng.dirichlet(np.ones(n_prices) * 3)
        data = []
        for price, w in zip(selected, weights):
            ni = int(n * w * 0.7)
            data.append(rng.normal(price, 0.3, ni))
        remaining = n - sum(len(d) for d in data)
        if remaining > 0:
            data.append(rng.uniform(min(selected) * 0.5, max(selected) * 1.2, remaining))
        return np.concatenate(data)


def gen_u_shaped(rng, n):
    choice = rng.integers(0, 4)
    if choice == 0:
        return rng.beta(rng.uniform(0.2, 0.8), rng.uniform(0.2, 0.8), n) * rng.uniform(50, 100)
    elif choice == 1:
        n1 = n // 2
        left = rng.beta(0.5, 3, n1) * 50
        right = 50 + rng.beta(3, 0.5, n - n1) * 50
        return np.concatenate([left, right])
    elif choice == 2:
        return rng.beta(0.5, 0.5, n) * rng.uniform(50, 100)
    else:
        # Bounded uniform with edge pile-up
        data = rng.uniform(0, 100, n)
        n_low = int(n * rng.uniform(0.10, 0.20))
        n_high = int(n * rng.uniform(0.10, 0.20))
        data[:n_low] = rng.uniform(0, 5, n_low)
        data[n_low:n_low + n_high] = rng.uniform(95, 100, n_high)
        return data


def gen_spike(rng, n):
    choice = rng.integers(0, 5)
    center = rng.uniform(30, 70)
    if choice == 0:
        n_spike = int(n * rng.uniform(0.80, 0.95))
        total_range = rng.uniform(60, 100)
        spike = rng.normal(center, total_range * 0.005, n_spike)
        spread = rng.uniform(center - total_range/2, center + total_range/2, n - n_spike)
        return np.concatenate([spike, spread])
    elif choice == 1:
        n_spike = int(n * rng.uniform(0.80, 0.92))
        spike = np.full(n_spike, center) + rng.normal(0, 0.1, n_spike)
        spread_range = rng.uniform(40, 80)
        background = rng.uniform(center - spread_range/2, center + spread_range/2, n - n_spike)
        return np.concatenate([spike, background])
    elif choice == 2:
        n_spike = int(n * rng.uniform(0.75, 0.90))
        spike = rng.normal(center, rng.uniform(0.1, 0.5), n_spike)
        half_range = rng.uniform(30, 50)
        background = rng.uniform(center - half_range, center + half_range, n - n_spike)
        return np.concatenate([spike, background])
    elif choice == 3:
        base = rng.uniform(-1, 1, n)
        concentrated = np.sign(base) * np.abs(base) ** rng.uniform(6, 12)
        return center + concentrated * rng.uniform(30, 50)
    else:
        # Capped sensor: normal but capped at max
        mu = rng.uniform(60, 80)
        sigma = rng.uniform(10, 20)
        cap = mu + rng.uniform(1.5, 3.0) * sigma
        data = rng.normal(mu, sigma, n)
        data[data > cap] = cap + rng.normal(0, 0.1, np.sum(data > cap))
        return data


def gen_descending(rng, n):
    choice = rng.integers(0, 5)
    if choice == 0:
        return rng.exponential(rng.uniform(3, 20), n)
    elif choice == 1:
        return (rng.pareto(rng.uniform(1.5, 5.0), n) + 1) * rng.uniform(5, 20)
    elif choice == 2:
        return rng.geometric(rng.uniform(0.02, 0.1), n).astype(float)
    elif choice == 3:
        # Session duration: spike at short + exponential tail
        n_bounce = int(n * rng.uniform(0.25, 0.40))
        bounce = rng.exponential(2, n_bounce)
        rest = rng.lognormal(rng.uniform(3, 5), rng.uniform(0.8, 1.5), n - n_bounce)
        return np.concatenate([bounce, rest])
    else:
        # Inter-arrival times
        rate = rng.uniform(0.1, 2.0)
        return rng.exponential(1.0 / rate, n)


def gen_ascending(rng, n):
    choice = rng.integers(0, 3)
    if choice == 0:
        data = rng.exponential(rng.uniform(3, 20), n)
        return -data + data.max() + rng.uniform(1, 5)
    elif choice == 1:
        return rng.beta(rng.uniform(2, 10), 1, n) * rng.uniform(50, 100)
    else:
        return rng.power(rng.uniform(3, 8), n) * rng.uniform(50, 100)


def gen_zero_inflated(rng, n):
    choice = rng.integers(0, 3)
    if choice == 0:
        # Capital_Gain-like: 80-95% zeros + right tail
        zero_frac = rng.uniform(0.75, 0.95)
        n_zero = int(n * zero_frac)
        zeros = np.zeros(n_zero) + rng.normal(0, 0.01, n_zero)
        positives = rng.lognormal(rng.uniform(7, 10), rng.uniform(0.5, 1.0), n - n_zero)
        return np.concatenate([zeros, positives])
    elif choice == 1:
        # Discrete counts: Poisson with extra zeros
        lam = rng.uniform(1.5, 4)
        data = rng.poisson(lam, n).astype(float)
        # Add extra zeros
        n_extra = int(n * rng.uniform(0.2, 0.4))
        data[:n_extra] = 0
        return data
    else:
        # Insurance claims: many zeros, some large values
        zero_frac = rng.uniform(0.6, 0.85)
        n_zero = int(n * zero_frac)
        zeros = np.zeros(n_zero) + rng.normal(0, 0.005, n_zero)
        claims = rng.gamma(rng.uniform(1.5, 4), rng.uniform(500, 5000), n - n_zero)
        return np.concatenate([zeros, claims])


# Map each class to its generators
CLASS_GENERATORS = {
    'bell': [gen_bell],
    'right_skew': [gen_right_skew],
    'left_skew': [gen_left_skew],
    'heavy_tail': [gen_heavy_tail],
    'bimodal': [gen_bimodal],
    'multimodal': [gen_multimodal],
    'u_shaped': [gen_u_shaped],
    'spike': [gen_spike],
    'descending': [gen_descending],
    'ascending': [gen_ascending],
    'zero_inflated': [gen_zero_inflated],
}

# Broad-class groupings (for hard-negative mining)
BROAD_GROUPS = {
    'mountain': ['bell', 'right_skew', 'left_skew', 'heavy_tail'],
    'valley': ['bimodal', 'multimodal', 'u_shaped'],
    'spike': ['spike', 'descending', 'ascending', 'zero_inflated'],
}

# Confusion pairs: classes commonly confused with each other
CONFUSION_PAIRS = {
    'bell': ['bimodal', 'u_shaped', 'spike'],  # clear non-bell negatives only
    'right_skew': ['descending', 'heavy_tail'],
    'left_skew': ['ascending', 'bell'],
    'heavy_tail': ['bell', 'bimodal'],
    'bimodal': ['u_shaped', 'heavy_tail', 'multimodal'],
    'multimodal': ['bimodal', 'u_shaped'],
    'u_shaped': ['bimodal', 'multimodal'],
    'spike': ['zero_inflated', 'descending'],
    'descending': ['right_skew', 'spike'],
    'ascending': ['left_skew'],
    'zero_inflated': ['spike', 'descending'],
}


# === Rendering: Grayscale with Band (same as v3) ===

def render_grayscale_with_band(data, num_bins=NUM_BINS, img_size=IMG_SIZE):
    """
    Render data as 64x64 grayscale image:
    - Bars: bright (1.0)
    - Band (per-bucket SE/uncertainty): mid-gray (0.5)
    - Background: black (0.0)
    """
    p_low, p_high = np.percentile(data, [1, 99])
    clipped = data[(data >= p_low) & (data <= p_high)]
    if len(clipped) < 20:
        clipped = data

    data_min, data_max = clipped.min(), clipped.max()
    if data_max == data_min:
        data_max = data_min + 1

    bin_edges = np.linspace(data_min, data_max, num_bins + 1)
    counts = np.zeros(num_bins)
    bin_se = np.zeros(num_bins)

    for i in range(num_bins):
        if i == num_bins - 1:
            mask = (clipped >= bin_edges[i]) & (clipped <= bin_edges[i + 1])
        else:
            mask = (clipped >= bin_edges[i]) & (clipped < bin_edges[i + 1])
        bucket_values = clipped[mask]
        counts[i] = len(bucket_values)
        if len(bucket_values) > 1:
            bucket_std = np.std(bucket_values)
            bin_se[i] = bucket_std / np.sqrt(len(bucket_values))
        else:
            bin_se[i] = (data_max - data_min) / num_bins * 0.5

    max_count = counts.max()
    if max_count == 0:
        return np.zeros((img_size, img_size), dtype=np.float32)

    normalized_counts = counts / max_count

    max_se = bin_se.max()
    if max_se > 0:
        normalized_se = bin_se / max_se
    else:
        normalized_se = np.zeros(num_bins)

    band_max_pixels = max(3, int(img_size * 0.12))

    image = np.zeros((img_size, img_size), dtype=np.float32)
    col_width = img_size / num_bins

    for i in range(num_bins):
        col_start = int(i * col_width)
        col_end = int((i + 1) * col_width)
        col_end = min(col_end, img_size)
        if col_end <= col_start:
            col_end = col_start + 1

        bar_height = int(normalized_counts[i] * (img_size - 2))

        if bar_height > 0:
            bar_top_row = img_size - bar_height
            image[bar_top_row:, col_start:col_end] = 1.0

            band_half = max(1, int(normalized_se[i] * band_max_pixels))
            band_top = max(0, bar_top_row - band_half)

            image[band_top:bar_top_row, col_start:col_end] = 0.5

            actual_band_in_bar = min(band_half, bar_height)
            if actual_band_in_bar > 0:
                image[bar_top_row:bar_top_row + actual_band_in_bar, col_start:col_end] = 0.75

        elif counts[i] == 0 and max_count > 0:
            image[img_size - 1, col_start:col_end] = 0.25

    return image


# === Dataset Classes ===

class PerClassDataset(Dataset):
    """Binary dataset for a single class: positive + hard negatives."""

    def __init__(self, target_class_idx, num_positives=10000,
                 num_negatives=10000, sample_sizes=SAMPLE_SIZES, seed=42,
                 augment=True):
        self.images = []
        self.labels = []
        rng = np.random.default_rng(seed)
        target_class = CLASSES[target_class_idx]

        # Positive samples
        pos_generators = CLASS_GENERATORS[target_class]
        for _ in range(num_positives):
            gen_fn = rng.choice(pos_generators)
            n = int(rng.choice(sample_sizes))
            data = gen_fn(rng, n)
            img = self._render_with_augment(data, rng, augment)
            self.images.append(img)
            self.labels.append(1.0)

        # Negative samples: emphasize confused classes
        confused = CONFUSION_PAIRS.get(target_class, [])
        other_classes = [c for c in CLASSES if c != target_class]

        # 60% negatives from confusion pairs, 40% from all others
        n_confused = int(num_negatives * 0.6) if confused else 0
        n_other = num_negatives - n_confused

        if n_confused > 0:
            per_confused = n_confused // len(confused)
            for conf_class in confused:
                for _ in range(per_confused):
                    gen_fn = rng.choice(CLASS_GENERATORS[conf_class])
                    n = int(rng.choice(sample_sizes))
                    data = gen_fn(rng, n)
                    img = self._render_with_augment(data, rng, augment)
                    self.images.append(img)
                    self.labels.append(0.0)

        # Remaining negatives from all other classes
        if n_other > 0:
            per_other = n_other // len(other_classes)
            for other_class in other_classes:
                for _ in range(per_other):
                    gen_fn = rng.choice(CLASS_GENERATORS[other_class])
                    n = int(rng.choice(sample_sizes))
                    data = gen_fn(rng, n)
                    img = self._render_with_augment(data, rng, augment)
                    self.images.append(img)
                    self.labels.append(0.0)

        self.images = np.array(self.images, dtype=np.float32)
        self.labels = np.array(self.labels, dtype=np.float32)

        perm = rng.permutation(len(self.images))
        self.images = self.images[perm]
        self.labels = self.labels[perm]

    def _render_with_augment(self, data, rng, augment):
        if augment:
            bin_jitter = int(rng.integers(-2, 3))
            num_bins = max(15, NUM_BINS + bin_jitter)
        else:
            num_bins = NUM_BINS
        return render_grayscale_with_band(data, num_bins=num_bins)

    def __len__(self):
        return len(self.images)

    def __getitem__(self, idx):
        image = torch.tensor(self.images[idx]).unsqueeze(0)
        label = torch.tensor(self.labels[idx])
        return image, label


class JointDataset(Dataset):
    """Joint dataset for Phase 1: all classes, multi-label binary."""

    def __init__(self, num_per_class=5000, sample_sizes=SAMPLE_SIZES, seed=42,
                 augment=True):
        self.images = []
        self.labels = []
        rng = np.random.default_rng(seed)

        for class_idx, class_name in enumerate(CLASSES):
            generators = CLASS_GENERATORS[class_name]
            for _ in range(num_per_class):
                gen_fn = rng.choice(generators)
                n = int(rng.choice(sample_sizes))
                data = gen_fn(rng, n)

                if augment:
                    bin_jitter = int(rng.integers(-2, 3))
                    num_bins = max(15, NUM_BINS + bin_jitter)
                else:
                    num_bins = NUM_BINS

                img = render_grayscale_with_band(data, num_bins=num_bins)

                label = np.zeros(NUM_CLASSES, dtype=np.float32)
                label[class_idx] = 1.0

                self.images.append(img)
                self.labels.append(label)

        self.images = np.array(self.images, dtype=np.float32)
        self.labels = np.array(self.labels, dtype=np.float32)

        perm = rng.permutation(len(self.images))
        self.images = self.images[perm]
        self.labels = self.labels[perm]

    def __len__(self):
        return len(self.images)

    def __getitem__(self, idx):
        image = torch.tensor(self.images[idx]).unsqueeze(0)
        label = torch.tensor(self.labels[idx])
        return image, label


# === Model: Shared Backbone + 11 Independent Heads ===

class ShapeCNNGenerativeV3Full(nn.Module):
    """
    Single-channel 64x64 input → shared CNN backbone → 11 independent sigmoid heads.
    Each head answers: "how much does this look like [class]?"
    """
    def __init__(self, num_classes=NUM_CLASSES):
        super().__init__()
        self.num_classes = num_classes

        # Shared feature extractor (same architecture as v3-broad)
        self.backbone = nn.Sequential(
            nn.Conv2d(1, 32, kernel_size=5, padding=2),
            nn.BatchNorm2d(32),
            nn.ReLU(),
            nn.MaxPool2d(2),                                # 64->32

            nn.Conv2d(32, 64, kernel_size=3, padding=1),
            nn.BatchNorm2d(64),
            nn.ReLU(),
            nn.MaxPool2d(2),                                # 32->16

            nn.Conv2d(64, 128, kernel_size=3, padding=1),
            nn.BatchNorm2d(128),
            nn.ReLU(),
            nn.MaxPool2d(2),                                # 16->8

            nn.Conv2d(128, 128, kernel_size=3, padding=1),
            nn.BatchNorm2d(128),
            nn.ReLU(),
            nn.AdaptiveAvgPool2d(4),                        # 8->4
        )

        # Shared feature projection
        self.shared_fc = nn.Sequential(
            nn.Flatten(),
            nn.Linear(128 * 4 * 4, 256),
            nn.ReLU(),
            nn.Dropout(0.4),
            nn.Linear(256, 128),
            nn.ReLU(),
            nn.Dropout(0.3),
        )

        # Independent binary heads (one per class)
        self.heads = nn.ModuleList([
            nn.Sequential(
                nn.Linear(128, 64),
                nn.ReLU(),
                nn.Dropout(0.2),
                nn.Linear(64, 1),
            ) for _ in range(num_classes)
        ])

    def get_features(self, x):
        x = self.backbone(x)
        x = self.shared_fc(x)
        return x

    def forward(self, x):
        features = self.get_features(x)
        logits = torch.cat([head(features) for head in self.heads], dim=1)
        return logits

    def forward_head(self, x, class_idx):
        features = self.get_features(x)
        return self.heads[class_idx](features)

    def predict_scores(self, x):
        logits = self.forward(x)
        return torch.sigmoid(logits)


# === Training ===

def export_dataset(output_path, num_per_class=300, seed=42):
    """Export dataset to TSV for inspection."""
    rng = np.random.default_rng(seed)
    rows = []
    images = []

    for class_name in CLASSES:
        generators = CLASS_GENERATORS[class_name]
        for _ in range(num_per_class):
            gen_fn = rng.choice(generators)
            n = int(rng.choice(SAMPLE_SIZES))
            data = gen_fn(rng, n)

            p_low, p_high = np.percentile(data, [1, 99])
            clipped = data[(data >= p_low) & (data <= p_high)]
            if len(clipped) < 20:
                clipped = data
            data_min, data_max = clipped.min(), clipped.max()
            if data_max == data_min:
                data_max = data_min + 1

            bin_edges = np.linspace(data_min, data_max, NUM_BINS + 1)
            counts = np.zeros(NUM_BINS)
            stds = np.zeros(NUM_BINS)

            for i in range(NUM_BINS):
                if i == NUM_BINS - 1:
                    mask = (clipped >= bin_edges[i]) & (clipped <= bin_edges[i + 1])
                else:
                    mask = (clipped >= bin_edges[i]) & (clipped < bin_edges[i + 1])
                bucket_vals = clipped[mask]
                counts[i] = len(bucket_vals)
                if len(bucket_vals) > 1:
                    stds[i] = np.std(bucket_vals)

            img = render_grayscale_with_band(data)
            images.append(img)

            gen_name = gen_fn.__name__.replace('gen_', '')
            count_str = '\t'.join(f'{c:.0f}' for c in counts)
            std_str = '\t'.join(f'{s:.4f}' for s in stds)
            rows.append(f'{class_name}\t{gen_name}\t{n}\t{count_str}\t{std_str}')

    # Write TSV
    header_parts = ['class', 'generator', 'sample_size']
    header_parts += [f'bin_{i}_count' for i in range(NUM_BINS)]
    header_parts += [f'bin_{i}_std' for i in range(NUM_BINS)]
    header = '\t'.join(header_parts)

    tsv_path = output_path + '.tsv'
    with open(tsv_path, 'w') as f:
        f.write(header + '\n')
        for row in rows:
            f.write(row + '\n')

    npy_path = output_path + '_images.npy'
    np.save(npy_path, np.array(images, dtype=np.float32))

    meta_path = output_path + '_meta.json'
    meta = {
        'classes': CLASSES,
        'num_classes': NUM_CLASSES,
        'num_bins': NUM_BINS,
        'img_size': IMG_SIZE,
        'sample_sizes': list(SAMPLE_SIZES),
        'num_per_class': num_per_class,
        'total_samples': len(rows),
        'broad_groups': BROAD_GROUPS,
        'confusion_pairs': CONFUSION_PAIRS,
        'rendering': {
            'bar_intensity': 1.0,
            'band_intensity': 0.5,
            'band_edge_intensity': 0.75,
            'background': 0.0,
            'band_metric': 'SE = std / sqrt(n_bucket)',
        }
    }
    with open(meta_path, 'w') as f:
        json.dump(meta, f, indent=2)

    print(f"  TSV: {tsv_path} ({len(rows)} rows)")
    print(f"  Images: {npy_path} ({len(images)} x {IMG_SIZE} x {IMG_SIZE})")
    print(f"  Meta: {meta_path}")
    return tsv_path


def train():
    print("=" * 70)
    print("CNN Shape Classification v3-full — Generative (Grayscale + Band)")
    print("  11 classes, independent sigmoid heads")
    print("=" * 70)
    print(f"Classes ({NUM_CLASSES}): {CLASSES}")
    print(f"Image: {IMG_SIZE}x{IMG_SIZE} single-channel grayscale")
    print(f"Rendering: {NUM_BINS} bins, bars=1.0, band=0.5, bg=0.0")
    print(f"Sample sizes: {SAMPLE_SIZES}")
    print()

    # Device
    if torch.cuda.is_available():
        device = torch.device('cuda')
    elif hasattr(torch.backends, 'mps') and torch.backends.mps.is_available():
        device = torch.device('mps')
    else:
        device = torch.device('cpu')
    print(f"Device: {device}")

    model = ShapeCNNGenerativeV3Full(NUM_CLASSES).to(device)
    num_params = sum(p.numel() for p in model.parameters() if p.requires_grad)
    print(f"Model parameters: {num_params:,}")
    print()

    # ==========================================
    # PHASE 1: Joint training (backbone + all heads)
    # ==========================================
    print("=" * 70)
    print("PHASE 1: Joint Training (backbone + all heads)")
    print("=" * 70)

    print("Generating joint training data (5000 per class)...")
    joint_ds = JointDataset(num_per_class=5000, seed=42, augment=True)
    print(f"  Training samples: {len(joint_ds)}")

    joint_val_ds = JointDataset(num_per_class=1000, seed=999, augment=False)
    print(f"  Validation samples: {len(joint_val_ds)}")

    joint_loader = DataLoader(joint_ds, batch_size=128, shuffle=True, num_workers=0)
    joint_val_loader = DataLoader(joint_val_ds, batch_size=256, shuffle=False, num_workers=0)

    criterion = nn.BCEWithLogitsLoss()
    optimizer = optim.Adam(model.parameters(), lr=0.001, weight_decay=1e-4)
    scheduler = optim.lr_scheduler.CosineAnnealingLR(optimizer, T_max=20)

    phase1_epochs = 20
    best_val_loss = float('inf')
    best_state = None

    print(f"\n{'Epoch':>5} | {'Train Loss':>10} | {'Val Loss':>10} | {'Val Recall':>10} | {'Best':>5}")
    print("-" * 55)

    for epoch in range(1, phase1_epochs + 1):
        model.train()
        train_loss = 0.0
        total = 0

        for images, labels in joint_loader:
            images, labels = images.to(device), labels.to(device)
            optimizer.zero_grad()
            logits = model(images)
            loss = criterion(logits, labels)
            loss.backward()
            optimizer.step()
            train_loss += loss.item() * images.size(0)
            total += images.size(0)

        train_loss /= total

        # Validate
        model.eval()
        val_loss = 0.0
        val_total = 0
        correct_per_class = np.zeros(NUM_CLASSES)
        total_per_class = np.zeros(NUM_CLASSES)

        with torch.no_grad():
            for images, labels in joint_val_loader:
                images, labels = images.to(device), labels.to(device)
                logits = model(images)
                loss = criterion(logits, labels)
                val_loss += loss.item() * images.size(0)
                val_total += images.size(0)

                preds = (torch.sigmoid(logits) > 0.5).float()
                for c in range(NUM_CLASSES):
                    mask = labels[:, c] == 1.0
                    if mask.sum() > 0:
                        correct_per_class[c] += (preds[mask, c] == 1.0).sum().item()
                        total_per_class[c] += mask.sum().item()

        val_loss /= val_total
        avg_recall = np.mean(correct_per_class / np.maximum(total_per_class, 1))

        is_best = val_loss < best_val_loss
        if is_best:
            best_val_loss = val_loss
            best_state = {k: v.clone() for k, v in model.state_dict().items()}

        scheduler.step()
        print(f"{epoch:5d} | {train_loss:10.4f} | {val_loss:10.4f} | {avg_recall:9.1%} | {'*' if is_best else ''}")

    if best_state:
        model.load_state_dict(best_state)
    print(f"\nPhase 1 complete. Best val loss: {best_val_loss:.4f}")

    # ==========================================
    # PHASE 2: Per-class fine-tuning (frozen backbone)
    # ==========================================
    print("\n" + "=" * 70)
    print("PHASE 2: Per-Class Fine-Tuning (frozen backbone)")
    print("=" * 70)

    for param in model.backbone.parameters():
        param.requires_grad = False
    for param in model.shared_fc.parameters():
        param.requires_grad = False

    phase2_epochs = 20

    for class_idx, class_name in enumerate(CLASSES):
        print(f"\n  [{class_idx:2d}] Training head: {class_name}")
        confused = CONFUSION_PAIRS.get(class_name, [])
        print(f"       Hard negatives: {confused}")

        class_ds = PerClassDataset(
            target_class_idx=class_idx,
            num_positives=8000, num_negatives=8000,
            seed=42 + class_idx, augment=True
        )
        class_val_ds = PerClassDataset(
            target_class_idx=class_idx,
            num_positives=1500, num_negatives=1500,
            seed=999 + class_idx, augment=False
        )

        class_loader = DataLoader(class_ds, batch_size=128, shuffle=True, num_workers=0)
        class_val_loader = DataLoader(class_val_ds, batch_size=256, shuffle=False, num_workers=0)

        head_optimizer = optim.Adam(model.heads[class_idx].parameters(), lr=0.003, weight_decay=1e-4)
        head_scheduler = optim.lr_scheduler.CosineAnnealingLR(head_optimizer, T_max=phase2_epochs)
        head_criterion = nn.BCEWithLogitsLoss()

        best_f1 = 0.0
        best_head_state = None

        for epoch in range(1, phase2_epochs + 1):
            model.train()
            for images, labels in class_loader:
                images, labels = images.to(device), labels.to(device)
                head_optimizer.zero_grad()
                with torch.no_grad():
                    features = model.get_features(images)
                logit = model.heads[class_idx](features).squeeze()
                loss = head_criterion(logit, labels)
                loss.backward()
                head_optimizer.step()

            # Validate
            model.eval()
            tp, fp, tn, fn = 0, 0, 0, 0
            with torch.no_grad():
                for images, labels in class_val_loader:
                    images, labels = images.to(device), labels.to(device)
                    features = model.get_features(images)
                    logit = model.heads[class_idx](features).squeeze()
                    preds = (torch.sigmoid(logit) > 0.5).float()
                    tp += ((preds == 1) & (labels == 1)).sum().item()
                    fp += ((preds == 1) & (labels == 0)).sum().item()
                    tn += ((preds == 0) & (labels == 0)).sum().item()
                    fn += ((preds == 0) & (labels == 1)).sum().item()

            precision = tp / (tp + fp) if (tp + fp) > 0 else 0
            recall = tp / (tp + fn) if (tp + fn) > 0 else 0
            f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0

            if f1 > best_f1:
                best_f1 = f1
                best_head_state = {k: v.clone() for k, v in model.heads[class_idx].state_dict().items()}

            head_scheduler.step()

        # Restore best head
        if best_head_state:
            model.heads[class_idx].load_state_dict(best_head_state)
        print(f"       Best F1: {best_f1:.3f} | Final P={precision:.3f} R={recall:.3f}")

    # Unfreeze for saving
    for param in model.backbone.parameters():
        param.requires_grad = True
    for param in model.shared_fc.parameters():
        param.requires_grad = True

    # Save
    save_path = os.path.join(os.path.dirname(__file__), 'shape_cnn_generative_v3_full.pth')
    torch.save(model.state_dict(), save_path)
    print(f"\nModel saved to {save_path} ({num_params:,} params)")

    # === Evaluation ===
    print("\n" + "=" * 70)
    print("EVALUATION — Confusion Matrix")
    print("=" * 70)

    model.eval()
    rng = np.random.default_rng(777)

    # Full confusion: for each true class, score on all 11 heads
    n_test = 200
    print(f"\n{'True \\ Pred':<14s}", end='')
    for c in CLASSES:
        print(f" | {c[:7]:>7s}", end='')
    print()
    print("-" * (14 + 10 * NUM_CLASSES))

    all_scores = {}  # true_class -> avg scores array

    for true_idx, true_class in enumerate(CLASSES):
        generators = CLASS_GENERATORS[true_class]
        scores_acc = np.zeros(NUM_CLASSES)

        for _ in range(n_test):
            gen_fn = rng.choice(generators)
            n = int(rng.choice([200, 400, 800, 1500, 3000]))
            data = gen_fn(rng, n)
            img = render_grayscale_with_band(data)
            t = torch.tensor(img).unsqueeze(0).unsqueeze(0).to(device)
            with torch.no_grad():
                s = model.predict_scores(t).cpu().numpy().squeeze()
            scores_acc += s

        avg_scores = scores_acc / n_test * 100
        all_scores[true_class] = avg_scores

        print(f"{true_class:<14s}", end='')
        for pred_idx in range(NUM_CLASSES):
            marker = '*' if pred_idx == true_idx else ' '
            print(f" | {avg_scores[pred_idx]:5.1f}%{marker}", end='')
        print()

    # Summary metrics
    print(f"\n{'Class':<14s} | {'Recall':>7s} | {'Max FP':>7s} | {'FP From':>10s}")
    print("-" * 50)

    for true_idx, true_class in enumerate(CLASSES):
        scores = all_scores[true_class]
        recall = scores[true_idx]
        # Max false positive: highest off-diagonal score
        fp_scores = np.copy(scores)
        fp_scores[true_idx] = 0
        max_fp_idx = np.argmax(fp_scores)
        max_fp = fp_scores[max_fp_idx]
        print(f"{true_class:<14s} | {recall:6.1f}% | {max_fp:6.1f}% | {CLASSES[max_fp_idx]}")

    # Real data
    print("\n" + "=" * 70)
    print("REAL DATA — Ames Housing")
    print("=" * 70)

    try:
        ames_path = "<PATH-TO-AMES-HOUSING-FROM-KAGGLE>/AmesHousing.csv"
        with open(ames_path, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)
            rows = [r for r in reader if len(r) == len(header)]

        skip_cols = {'Order', 'PID'}
        print(f"\n  {'Feature':<22s} | {'Top-1':>25s} | {'Top-2':>25s} | {'Top-3':>25s}")
        print("  " + "-" * 90)

        for i, col_name in enumerate(header):
            if col_name in skip_cols:
                continue
            vals = []
            for row in rows:
                try:
                    vals.append(float(row[i]))
                except (ValueError, IndexError):
                    pass
            if len(vals) < 100 or len(set(vals)) < 8:
                continue

            data = np.array(vals)
            img = render_grayscale_with_band(data)
            t = torch.tensor(img).unsqueeze(0).unsqueeze(0).to(device)
            with torch.no_grad():
                scores = model.predict_scores(t).cpu().numpy().squeeze() * 100

            # Top-3
            ranked = np.argsort(scores)[::-1]
            t1 = f"{CLASSES[ranked[0]]}({scores[ranked[0]]:.0f}%)"
            t2 = f"{CLASSES[ranked[1]]}({scores[ranked[1]]:.0f}%)"
            t3 = f"{CLASSES[ranked[2]]}({scores[ranked[2]]:.0f}%)"
            print(f"  {col_name:<22s} | {t1:>25s} | {t2:>25s} | {t3:>25s}")

    except FileNotFoundError:
        print("  (Ames dataset not found)")

    # Real data - Adult
    print("\n" + "=" * 70)
    print("REAL DATA — Adult Census")
    print("=" * 70)

    try:
        adult_path = '<PATH-TO-ADULT-UCI-KAGGLE-DATASET>/adult_train.csv'
        with open(adult_path, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)
            rows = [r for r in reader if len(r) == len(header)]

        print(f"\n  {'Feature':<22s} | {'Top-1':>25s} | {'Top-2':>25s} | {'Top-3':>25s}")
        print("  " + "-" * 90)

        for i, col_name in enumerate(header):
            col_name = col_name.strip()
            vals = []
            for row in rows:
                try:
                    vals.append(float(row[i].strip()))
                except (ValueError, IndexError):
                    pass
            if len(vals) < 100 or len(set(vals)) < 8:
                continue

            data = np.array(vals)
            img = render_grayscale_with_band(data)
            t = torch.tensor(img).unsqueeze(0).unsqueeze(0).to(device)
            with torch.no_grad():
                scores = model.predict_scores(t).cpu().numpy().squeeze() * 100

            ranked = np.argsort(scores)[::-1]
            t1 = f"{CLASSES[ranked[0]]}({scores[ranked[0]]:.0f}%)"
            t2 = f"{CLASSES[ranked[1]]}({scores[ranked[1]]:.0f}%)"
            t3 = f"{CLASSES[ranked[2]]}({scores[ranked[2]]:.0f}%)"
            print(f"  {col_name:<22s} | {t1:>25s} | {t2:>25s} | {t3:>25s}")

    except FileNotFoundError:
        print("  (Adult dataset not found)")

    print(f"\nDone. Model: {save_path}")
    print("Output: independent 0-100% score per class (sigmoid, not softmax)")


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Train v3-full shape classifier (11 classes)')
    parser.add_argument('--export-only', action='store_true',
                        help='Only export datasets, skip training')
    parser.add_argument('--skip-export', action='store_true',
                        help='Skip dataset export, only train')
    args = parser.parse_args()

    base_dir = os.path.dirname(__file__)

    if not args.skip_export:
        print("=" * 70)
        print("EXPORTING DATASETS")
        print("=" * 70)
        print("\nTraining dataset:")
        export_dataset(os.path.join(base_dir, 'v3_full_train_data'), num_per_class=300, seed=42)
        print("\nEvaluation dataset:")
        export_dataset(os.path.join(base_dir, 'v3_full_eval_data'), num_per_class=150, seed=777)

    if not args.export_only:
        train()
