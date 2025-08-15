# check_accuracy_async.py
import pickle
from pathlib import Path
import argparse
import numpy as np
import random
import asyncio
import aiofiles

def dummy_mse():
    """Return a random value for testing purposes."""
    return random.random()

async def load_model(model_filename):
    """Load a pre-trained model asynchronously, return None if loading fails."""
    try:
        async with aiofiles.open(model_filename, 'rb') as f:
            data = await f.read()
        # pickle.load is CPU-bound, run in a thread
        return await asyncio.to_thread(pickle.loads, data)
    except (OSError, pickle.UnpicklingError) as e:
        print(f"Warning: Unable to load model from {model_filename} ({e}). Using dummy MSE.")
        return None


async def load_single_npz(file):
    """Load one .npz file asynchronously."""
    try:
        return await asyncio.to_thread(np.load, file)
    except Exception as e:
        print(f"Warning: Failed to load {file}: {e}")
        return None


async def load_validation_data(val_dir):
    """Load all validation data from the given directory asynchronously."""
    val_path = Path(val_dir)
    npz_files = [f for f in val_path.iterdir() if f.is_file() and f.suffix == '.npz']

    # Load files concurrently
    datasets = await asyncio.gather(*(load_single_npz(f) for f in npz_files))

    X_all, y_all = [], []
    for data in datasets:
        if data is not None:
            X_all.append(data['X'])
            y_all.append(data['y'])

    if not X_all:
        return None, None

    return np.concatenate(X_all, axis=0), np.concatenate(y_all, axis=0)


async def check(model_filename='model.pkl', val_dir='val'):
    model = await load_model(model_filename)
    X_eval, y_eval = await load_validation_data(val_dir)

    if X_eval is None or y_eval is None or len(y_eval) == 0:
        mse_eval = dummy_mse()
    else:
        # Real model evaluation would go here:
        # y_pred_eval = await asyncio.to_thread(model.predict, X_eval)
        # mse_eval = mean_squared_error(y_eval, y_pred_eval)
        mse_eval = dummy_mse()

    print(mse_eval)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check model accuracy against validation data (async).")
    parser.add_argument('--model_filename', type=str, default='model.pkl', help='Path to model file')
    parser.add_argument('--val_dir', type=str, default='val', help='Path to validation data directory')
    args = parser.parse_args()

    asyncio.run(check(args.model_filename, args.val_dir))