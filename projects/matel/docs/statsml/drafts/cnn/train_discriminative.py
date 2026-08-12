"""
CNN Shape Classification v3-full: DISCRIMINATIVE (softmax) with Grayscale + Band.

Same architecture as v3-full generative but with a single softmax head.
Picks exactly one winner per input (mutually exclusive classes).

Input: 64x64 single-channel grayscale image (bars=1.0, band=0.5, bg=0.0)

Classes (11): bell, right_skew, left_skew, heavy_tail, bimodal, multimodal,
              u_shaped, spike, descending, ascending, zero_inflated

Reports top-1 and top-2 accuracy.
"""

import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader
import os
import csv

# Import generators and rendering from the generative script
import sys
sys.path.insert(0, os.path.dirname(__file__))
from train_generative_v3_full import (
    CLASSES, NUM_CLASSES, IMG_SIZE, NUM_BINS, SAMPLE_SIZES,
    CLASS_GENERATORS, BROAD_GROUPS, render_grayscale_with_band,
    gen_bell, gen_right_skew, gen_left_skew, gen_heavy_tail,
    gen_bimodal, gen_multimodal, gen_u_shaped,
    gen_spike, gen_descending, gen_ascending, gen_zero_inflated,
)


# === Dataset: standard multi-class (one-hot) ===

class MultiClassDataset(Dataset):
    """Standard dataset: one class label per sample."""

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
                self.images.append(img)
                self.labels.append(class_idx)

        self.images = np.array(self.images, dtype=np.float32)
        self.labels = np.array(self.labels, dtype=np.int64)

        perm = rng.permutation(len(self.images))
        self.images = self.images[perm]
        self.labels = self.labels[perm]

    def __len__(self):
        return len(self.images)

    def __getitem__(self, idx):
        image = torch.tensor(self.images[idx]).unsqueeze(0)  # (1, 64, 64)
        label = torch.tensor(self.labels[idx])
        return image, label


# === Model: Shared Backbone + Single Softmax Head ===

class ShapeCNNDiscriminativeV3(nn.Module):
    """
    Single-channel 64x64 → CNN backbone → single FC → softmax over 11 classes.
    Mutually exclusive: picks exactly one winner.
    """
    def __init__(self, num_classes=NUM_CLASSES):
        super().__init__()
        self.num_classes = num_classes

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

        self.classifier = nn.Sequential(
            nn.Flatten(),
            nn.Linear(128 * 4 * 4, 256),
            nn.ReLU(),
            nn.Dropout(0.4),
            nn.Linear(256, 128),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(128, num_classes),
        )

    def forward(self, x):
        x = self.backbone(x)
        return self.classifier(x)

    def predict_probs(self, x):
        logits = self.forward(x)
        return torch.softmax(logits, dim=1)


# === Training ===

def train():
    print("=" * 70)
    print("CNN Shape Classification v3-full — DISCRIMINATIVE (Softmax)")
    print("  11 classes, single softmax head, mutually exclusive")
    print("=" * 70)
    print(f"Classes ({NUM_CLASSES}): {CLASSES}")
    print(f"Image: {IMG_SIZE}x{IMG_SIZE} single-channel grayscale + SE band")
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

    model = ShapeCNNDiscriminativeV3(NUM_CLASSES).to(device)
    num_params = sum(p.numel() for p in model.parameters() if p.requires_grad)
    print(f"Model parameters: {num_params:,}")
    print()

    # === Training Data ===
    print("Generating training data (5000 per class)...")
    train_ds = MultiClassDataset(num_per_class=5000, seed=42, augment=True)
    print(f"  Training samples: {len(train_ds)}")

    val_ds = MultiClassDataset(num_per_class=1000, seed=999, augment=False)
    print(f"  Validation samples: {len(val_ds)}")

    train_loader = DataLoader(train_ds, batch_size=128, shuffle=True, num_workers=0)
    val_loader = DataLoader(val_ds, batch_size=256, shuffle=False, num_workers=0)

    criterion = nn.CrossEntropyLoss()
    optimizer = optim.Adam(model.parameters(), lr=0.001, weight_decay=1e-4)
    scheduler = optim.lr_scheduler.CosineAnnealingLR(optimizer, T_max=25)

    num_epochs = 25
    best_val_acc = 0.0
    best_state = None

    print(f"\n{'Epoch':>5} | {'Train Loss':>10} | {'Val Loss':>9} | {'Top-1':>6} | {'Top-2':>6} | {'Best':>5}")
    print("-" * 58)

    for epoch in range(1, num_epochs + 1):
        model.train()
        train_loss = 0.0
        total = 0

        for images, labels in train_loader:
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
        top1_correct = 0
        top2_correct = 0

        with torch.no_grad():
            for images, labels in val_loader:
                images, labels = images.to(device), labels.to(device)
                logits = model(images)
                loss = criterion(logits, labels)
                val_loss += loss.item() * images.size(0)
                val_total += images.size(0)

                # Top-1
                _, pred = logits.max(1)
                top1_correct += pred.eq(labels).sum().item()

                # Top-2
                _, top2_pred = logits.topk(2, dim=1)
                for i in range(labels.size(0)):
                    if labels[i] in top2_pred[i]:
                        top2_correct += 1

        val_loss /= val_total
        top1_acc = top1_correct / val_total * 100
        top2_acc = top2_correct / val_total * 100

        is_best = top1_acc > best_val_acc
        if is_best:
            best_val_acc = top1_acc
            best_state = {k: v.clone() for k, v in model.state_dict().items()}

        scheduler.step()
        print(f"{epoch:5d} | {train_loss:10.4f} | {val_loss:9.4f} | {top1_acc:5.1f}% | {top2_acc:5.1f}% | {'*' if is_best else ''}")

    if best_state:
        model.load_state_dict(best_state)
    print(f"\nTraining complete. Best top-1: {best_val_acc:.1f}%")

    # Save
    save_path = os.path.join(os.path.dirname(__file__), 'shape_cnn_discriminative_v3_full.pth')
    torch.save(model.state_dict(), save_path)
    print(f"Model saved to {save_path} ({num_params:,} params)")

    # === Evaluation ===
    print("\n" + "=" * 70)
    print("EVALUATION — Per-Class Accuracy")
    print("=" * 70)

    model.eval()
    rng = np.random.default_rng(777)

    n_test = 300
    correct_per_class = np.zeros(NUM_CLASSES)
    top2_per_class = np.zeros(NUM_CLASSES)
    total_per_class = np.zeros(NUM_CLASSES)

    # Confusion matrix
    confusion = np.zeros((NUM_CLASSES, NUM_CLASSES))

    for true_idx, class_name in enumerate(CLASSES):
        generators = CLASS_GENERATORS[class_name]
        for _ in range(n_test):
            gen_fn = rng.choice(generators)
            n = int(rng.choice([200, 400, 800, 1500, 3000]))
            data = gen_fn(rng, n)
            img = render_grayscale_with_band(data)
            t = torch.tensor(img).unsqueeze(0).unsqueeze(0).to(device)
            with torch.no_grad():
                logits = model(t)
                probs = torch.softmax(logits, dim=1).cpu().numpy().squeeze()

            pred = np.argmax(probs)
            top2 = np.argsort(probs)[-2:]

            confusion[true_idx, pred] += 1
            total_per_class[true_idx] += 1
            if pred == true_idx:
                correct_per_class[true_idx] += 1
            if true_idx in top2:
                top2_per_class[true_idx] += 1

    print(f"\n{'Class':<14s} | {'Top-1':>7s} | {'Top-2':>7s} | {'Most Confused With':>20s}")
    print("-" * 60)

    for i, class_name in enumerate(CLASSES):
        top1 = correct_per_class[i] / total_per_class[i] * 100
        top2 = top2_per_class[i] / total_per_class[i] * 100
        # Most confused: highest off-diagonal
        conf_row = confusion[i].copy()
        conf_row[i] = 0
        confused_idx = np.argmax(conf_row)
        confused_pct = conf_row[confused_idx] / total_per_class[i] * 100
        print(f"{class_name:<14s} | {top1:6.1f}% | {top2:6.1f}% | {CLASSES[confused_idx]} ({confused_pct:.1f}%)")

    overall_top1 = correct_per_class.sum() / total_per_class.sum() * 100
    overall_top2 = top2_per_class.sum() / total_per_class.sum() * 100
    print(f"\n{'OVERALL':<14s} | {overall_top1:6.1f}% | {overall_top2:6.1f}%")

    # Confusion matrix
    print(f"\nConfusion Matrix (rows=true, cols=predicted, counts out of {n_test}):")
    print(f"{'':14s}", end='')
    for c in CLASSES:
        print(f" {c[:5]:>5s}", end='')
    print()
    for i, true_name in enumerate(CLASSES):
        print(f"{true_name:<14s}", end='')
        for j in range(NUM_CLASSES):
            val = int(confusion[i, j])
            marker = '*' if i == j else ' '
            print(f" {val:4d}{marker}", end='')
        print()

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
        print(f"\n  {'Feature':<22s} | {'Top-1':>25s} | {'Top-2':>25s} | {'Confidence':>5s}")
        print("  " + "-" * 85)

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
                probs = model.predict_probs(t).cpu().numpy().squeeze() * 100

            ranked = np.argsort(probs)[::-1]
            t1 = f"{CLASSES[ranked[0]]} ({probs[ranked[0]]:.0f}%)"
            t2 = f"{CLASSES[ranked[1]]} ({probs[ranked[1]]:.0f}%)"
            conf = probs[ranked[0]]
            print(f"  {col_name:<22s} | {t1:>25s} | {t2:>25s} | {conf:4.0f}%")

    except FileNotFoundError:
        print("  (Ames dataset not found)")

    # Adult
    print("\n" + "=" * 70)
    print("REAL DATA — Adult Census")
    print("=" * 70)

    try:
        adult_path = '<PATH-TO-ADULT-UCI-KAGGLE-DATASET>/adult_train.csv'
        with open(adult_path, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)
            rows = [r for r in reader if len(r) == len(header)]

        print(f"\n  {'Feature':<22s} | {'Top-1':>25s} | {'Top-2':>25s} | {'Confidence':>5s}")
        print("  " + "-" * 85)

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
                probs = model.predict_probs(t).cpu().numpy().squeeze() * 100

            ranked = np.argsort(probs)[::-1]
            t1 = f"{CLASSES[ranked[0]]} ({probs[ranked[0]]:.0f}%)"
            t2 = f"{CLASSES[ranked[1]]} ({probs[ranked[1]]:.0f}%)"
            conf = probs[ranked[0]]
            print(f"  {col_name:<22s} | {t1:>25s} | {t2:>25s} | {conf:4.0f}%")

    except FileNotFoundError:
        print("  (Adult dataset not found)")

    # === Agreement with generative model ===
    print("\n" + "=" * 70)
    print("AGREEMENT: Discriminative vs Generative (v3-full)")
    print("=" * 70)

    try:
        from train_generative_v3_full import ShapeCNNGenerativeV3Full
        gen_model_path = os.path.join(os.path.dirname(__file__), 'shape_cnn_generative_v3_full.pth')
        gen_model = ShapeCNNGenerativeV3Full(NUM_CLASSES).to(device)
        gen_model.load_state_dict(torch.load(gen_model_path, map_location=device, weights_only=True))
        gen_model.eval()

        agree = 0
        total = 0
        disagree_examples = []

        rng2 = np.random.default_rng(555)
        for true_idx, class_name in enumerate(CLASSES):
            generators = CLASS_GENERATORS[class_name]
            for _ in range(100):
                gen_fn = rng2.choice(generators)
                n = int(rng2.choice([400, 800, 1500, 3000]))
                data = gen_fn(rng2, n)
                img = render_grayscale_with_band(data)
                t = torch.tensor(img).unsqueeze(0).unsqueeze(0).to(device)

                with torch.no_grad():
                    disc_probs = model.predict_probs(t).cpu().numpy().squeeze()
                    gen_scores = gen_model.predict_scores(t).cpu().numpy().squeeze()

                disc_pred = np.argmax(disc_probs)
                gen_pred = np.argmax(gen_scores)
                total += 1

                if disc_pred == gen_pred:
                    agree += 1
                elif len(disagree_examples) < 10:
                    disagree_examples.append((class_name, CLASSES[disc_pred], CLASSES[gen_pred]))

        print(f"\n  Agreement: {agree}/{total} ({agree/total*100:.1f}%)")
        if disagree_examples:
            print(f"\n  Sample disagreements (true → disc / gen):")
            for true, disc, gen in disagree_examples[:8]:
                print(f"    {true:<14s} → disc: {disc:<14s} / gen: {gen}")

    except Exception as e:
        print(f"  (Could not load generative model: {e})")

    print(f"\nDone. Discriminative model: {save_path}")
    print("Output: softmax probabilities (sum to 100%, mutually exclusive)")


if __name__ == '__main__':
    train()
