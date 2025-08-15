# predict_async_limited.py
import pickle
import random
import json
import numpy as np
from pathlib import Path
from sklearn.metrics import mean_squared_error
import argparse
from typing import Dict, Union
import asyncio
from asyncio import to_thread

# Control how many files to load in parallel (tune for HPC)
MAX_CONCURRENT_FILE_LOADS = 50

async def async_iterdir(path: Path):
    """Run Path.iterdir() in a separate thread to avoid blocking."""
    return await to_thread(list, path.iterdir())

async def load_model(model_filename: Union[str, Path]):
    """Load a model from a pickle file in a thread."""
    try:
        return await asyncio.to_thread(
            lambda: pickle.load(open(model_filename, 'rb'))
        )
    except (OSError, pickle.UnpicklingError) as e:
        print(f"⚠ Unable to load model from {model_filename}: {e}")
        return None


async def evaluate_npz_file(file: Path, model, sem: asyncio.Semaphore) -> float:
    """Evaluate a single .npz file and return its MSE."""
    async with sem:  # limit concurrent file access
        try:
            data = await asyncio.to_thread(np.load, file)
            X_eval = data['X']
            y_eval = data['y']
        except (OSError, KeyError) as e:
            print(f"⚠ Skipping corrupt file {file}: {e}")
            return None

        if len(y_eval) == 0:
            return None

        # Uncomment for real model prediction
        # y_pred_eval = await asyncio.to_thread(model.predict, X_eval)
        # mse_eval = mean_squared_error(y_eval, y_pred_eval)
        mse_eval = random.random()  # placeholder
        return mse_eval


async def evaluate_simulation(sim_dir: Path, model, sem: asyncio.Semaphore) -> float:
    """Evaluate a single simulation directory asynchronously."""
    tasks = []
    for file in sim_dir.iterdir():
        if file.is_file() and file.suffix == ".npz":
            tasks.append(evaluate_npz_file(file, model, sem))

    mses = await asyncio.gather(*tasks)
    mses = [m for m in mses if m is not None]
    return float(np.mean(mses)) if mses else float('nan')


async def predict(
    model_filename: str,
    sim_output_dir: str,
    output_file: str
) -> None:
    """Run prediction on all available simulations asynchronously."""
    model = await load_model(model_filename)
   
    sim_output_dir = Path(sim_output_dir)
    results: Dict[str, float] = {}

    sem = asyncio.Semaphore(MAX_CONCURRENT_FILE_LOADS)  # limit concurrency

    tasks = []
    for sim_dir in await async_iterdir(sim_output_dir):
        if not sim_dir.is_dir():
            continue

        files = [f.name for f in await async_iterdir(sim_dir)]
        if not files:
            continue
        sim_tag = sim_dir.name
        sim_dir = sim_output_dir / sim_tag
        if sim_dir.is_dir():
            tasks.append((sim_tag, evaluate_simulation(sim_dir, model, sem)))

    # Run simulations concurrently
    eval_results = await asyncio.gather(*(task for _, task in tasks))
    for (sim_tag, _), mse in zip(tasks, eval_results):
        results[sim_tag] = mse

    print(f"\nPrediction completed. Saving results to {output_file}")
    try:
        await asyncio.to_thread(
            lambda: json.dump(results, open(output_file, 'w'), indent=2)
        )
    except OSError as e:
        print(f"⚠ Failed to write results to {output_file}: {e}")


def main():
    global MAX_CONCURRENT_FILE_LOADS

    parser = argparse.ArgumentParser(description="Run predictions on simulation data (async + concurrency limit).")
    parser.add_argument('--model_filename', required=True, help='Path to model weights (pickle file)')
    parser.add_argument('--sim_output_dir', required=True, help='Path to simulation output data')
    parser.add_argument('--output_file', required=True, help='Path to save prediction results')
    parser.add_argument('--max_concurrent', type=int, default=MAX_CONCURRENT_FILE_LOADS, help='Max concurrent file loads')
    args = parser.parse_args()

    MAX_CONCURRENT_FILE_LOADS = args.max_concurrent

    asyncio.run(
        predict(args.model_filename, args.sim_output_dir, args.output_file)
    )


if __name__ == "__main__":
    main()