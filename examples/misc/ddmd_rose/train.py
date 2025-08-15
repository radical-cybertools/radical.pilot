# train_async.py
import asyncio
from pathlib import Path
import pickle
from sklearn.linear_model import LinearRegression
import argparse
import numpy as np
import random
import shutil
from asyncio import to_thread

VAL_SPLIT = 0.5
MIN_TRAIN_SIZE = 10
MIN_NUM_TO_PREDICT = 1

async def async_iterdir(path: Path):
    """Run Path.iterdir() in a separate thread to avoid blocking."""
    return await to_thread(list, path.iterdir())

async def check_sim_dir(sim_output_dir: Path):
    files = []
    count = 0
    for sim_dir in await async_iterdir(sim_output_dir):
        count += 1
        if count == MIN_TRAIN_SIZE:
            return True
        if sim_dir.is_dir():
            files += [f.name for f in await async_iterdir(sim_dir)]

    return False

async def data_loading(sim_output_dir: Path, train_dir: Path, val_dir: Path):
    await asyncio.to_thread(train_dir.mkdir, parents=True, exist_ok=True)
    await asyncio.to_thread(val_dir.mkdir, parents=True, exist_ok=True)

    for sim_dir in await async_iterdir(sim_output_dir):
        if not sim_dir.is_dir():
            continue

        files = [f.name for f in await async_iterdir(sim_dir)]
        if not files:
            continue

        random.shuffle(files)
        val_subset = int(len(files) * VAL_SPLIT)
        train_files = files[val_subset:]
        val_files = files[:val_subset]

        # Move files asynchronously (threaded because shutil is blocking)
        for filename in train_files:
            await to_thread(shutil.move, sim_dir / filename, train_dir / filename)
        for filename in val_files:
            await to_thread(shutil.move, sim_dir / filename, val_dir / filename)

async def train(model_filename='model.pkl', sim_output_dir='sim_output', 
                train_dir='train_data', val_dir='val_data'):

    train_dir = Path(train_dir)
    val_dir = Path(val_dir)
    sim_output_dir = Path(sim_output_dir)

    while True:
        data_ready = await check_sim_dir(sim_output_dir)
        if data_ready:
            break
        await asyncio.sleep(1)

    await data_loading(sim_output_dir, train_dir, val_dir)

    try:
        model = await to_thread(pickle.load, open(model_filename, 'rb'))
    except:
        model = LinearRegression()

    X_all, y_all = [], []

    count = 0
    for file in await async_iterdir(train_dir):
        count += 1
        # This condition helps avoid long execution times when there are too many files to iterate through.
        if count == MIN_TRAIN_SIZE:
            break
        if file.is_file():
            print(f'Using data from {file} for training')
            try:
                data = await to_thread(np.load, file)
                X_labeled = data['X']
                y_labeled = data['y']
                X_all.append(X_labeled)
                y_all.append(y_labeled)
            except Exception as e:
                print(f"Skipping {file}: {e}")

    if y_all:
        X_combined = np.concatenate(X_all, axis=0)
        y_combined = np.concatenate(y_all, axis=0)
        if len(y_combined) > MIN_NUM_TO_PREDICT:
            await to_thread(model.fit, X_combined, y_combined)
            await to_thread(pickle.dump, model, open(model_filename, 'wb'))
            print(f"Model saved to {model_filename}")

    #Run this to extend execution time
    i = 0
    for _ in range(10000):
        i += 1

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Async Training Script")
    parser.add_argument('--model_filename', type=str, help='Path to store model weights')
    parser.add_argument('--train_dir', type=str, help='Path to training data')
    parser.add_argument('--sim_output_dir', type=str, help='Path to simulation output data')
    parser.add_argument('--val_dir', type=str, help='Path to validation data')

    args = parser.parse_args()
    asyncio.run(train(args.model_filename, args.sim_output_dir, args.train_dir, args.val_dir))