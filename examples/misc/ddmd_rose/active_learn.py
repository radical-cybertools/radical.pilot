import asyncio
import random
from pathlib import Path
import numpy as np
import pickle
import argparse
from typing import Union

# Global datasets
UNLABELED_DATA = []
LABELED_DATA = []
LABELS = {}

async def load_model(model_filename: Union[str, Path]):
    """Load a model from a pickle file in a thread."""
    try:
        return await asyncio.to_thread(
            lambda: pickle.load(open(model_filename, 'rb'))
        )
    except (OSError, pickle.UnpicklingError) as e:
        print(f"⚠ Unable to load model from {model_filename}: {e}")
        return None

async def train_model(labeled_data, labels):
    """Simulate model training."""
    await asyncio.sleep(0.5)
    print(f"Trained model on {len(labeled_data)} samples")
    return "model"

async def get_uncertainty_scores(model, unlabeled_data):
    """Simulate computing uncertainty scores for unlabeled samples."""
    await asyncio.sleep(0.2)
    return {i: random.random() for i in range(len(unlabeled_data))}

async def label_data(samples):
    """Simulate labeling process (e.g., human annotation or simulation)."""
    await asyncio.sleep(0.3)
    return {s: random.choice([0, 1]) for s in samples}

def load_unlabeled_data(train_dir: str):
    """Load all .npz files from train_dir into UNLABELED_DATA."""
    global UNLABELED_DATA
    train_dir = Path(train_dir)
    if not train_dir.is_dir():
        raise ValueError(f"Train directory {train_dir} does not exist")
    
    UNLABELED_DATA = []
    for file in train_dir.iterdir():
        if file.is_file() and file.suffix == ".npz":
            data = np.load(file)
            X = data["X"]
            UNLABELED_DATA.extend([x for x in X])
    print(f"Loaded {len(UNLABELED_DATA)} unlabeled samples from {train_dir}")

def move_labeled_to_train_al(train_al_dir: str, sample_indices, unlabeled_data_snapshot, labels_snapshot):
    """
    Move newly labeled data to train_al_dir for next training iteration.

    Args:
        train_al_dir: directory to store labeled data
        sample_indices: indices of the newly labeled samples in the original UNLABELED_DATA snapshot
        unlabeled_data_snapshot: the UNLABELED_DATA list at the start of iteration
        labels_snapshot: dictionary of labels for the newly labeled samples
    """
    train_al_dir = Path(train_al_dir)
    train_al_dir.mkdir(parents=True, exist_ok=True)
    
    for idx in sample_indices:
        sample_file = train_al_dir / f"sample_{idx}.npz"
        X = unlabeled_data_snapshot[idx]
        y = labels_snapshot[idx]
        np.savez(sample_file, X=X, y=y)
    print(f"Moved {len(sample_indices)} labeled samples to {train_al_dir}")


async def active_learning_loop(model_filename: str,
                               train_dir: str,
                               train_al_dir: str,
                               iterations=5, batch_size=5)-> None:
    global UNLABELED_DATA, LABELED_DATA, LABELS

    for it in range(iterations):
        print(f"\n=== Iteration {it+1} ===")

        # Load model
        model = await load_model(model_filename)

        # Load unlabeled data
        load_unlabeled_data(train_dir)

        # Get uncertainty scores for unlabeled data
        scores = await get_uncertainty_scores(model, UNLABELED_DATA)

        # Select top uncertain samples
        most_uncertain = sorted(scores, key=scores.get, reverse=True)[:batch_size]
        print(f"Selected samples for labeling: {most_uncertain}")

        # Label them
        new_labels = await label_data(most_uncertain)

        # Take a snapshot of current UNLABELED_DATA
        unlabeled_snapshot = UNLABELED_DATA.copy()
        new_labels = await label_data(most_uncertain)

        # Update datasets
        for s, lbl in new_labels.items():
            LABELS[s] = lbl
            LABELED_DATA.append(UNLABELED_DATA[s])
        UNLABELED_DATA = [x for i, x in enumerate(UNLABELED_DATA) if i not in most_uncertain]

        # Move newly labeled data using the snapshot
        move_labeled_to_train_al(train_al_dir, list(new_labels.keys()), unlabeled_snapshot, new_labels)


    print("\nFinal labeled dataset size:", len(LABELED_DATA))
    #print("Labels:", LABELS)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Select batch for the next training iteration.")
    parser.add_argument('--model_filename', required=True, help='Path to model weights (pickle file)')
    parser.add_argument('--train_dir', type=str, help='Path to all available training data')
    parser.add_argument('--train_al_dir', type=str, help='Path to training data selected for next AL iteration')
    args = parser.parse_args()

    asyncio.run(active_learning_loop(args.model_filename, args.train_dir, args.train_al_dir))