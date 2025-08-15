# sim_async.py
import argparse
from pathlib import Path
import numpy as np
import asyncio
import aiofiles

def complicated_function(x: np.ndarray) -> np.ndarray:
    """Complex mathematical function to simulate a process."""
    return (
        0.3 * np.sin(1.5 * np.pi * x**2)
        + 0.2 * np.cos(2 * np.pi * x**3)
        + 0.5 * np.exp(-0.5 * x)
        + 0.1 * np.tanh(0.2 * (x - 0.5))
        + 0.3 * (x**3)
    )

async def simulate_one(output_file: Path):
    """Run a single simulation iteration asynchronously."""
    X = np.random.uniform(low=0.0, high=1.0, size=(500, 1))

    # Run CPU-heavy loop in a thread to avoid blocking event loop
    def run_math():
        y = 0
        for _ in range(1000):
            y += complicated_function(X)
        return y

    y = await asyncio.to_thread(run_math)

    # Save results asynchronously
    np_bytes = await asyncio.to_thread(np.savez_compressed, output_file, X=X, y=y)

    print(f"Saved simulation to {output_file}")


async def run_simulation(input_dir: str, output_dir: str, sim_tag: str) -> None:
    """Run the simulation and save results."""
    print(f"Simulation will read from {input_dir}")

    output_sim_dir = Path(output_dir) / sim_tag
    output_sim_dir.mkdir(parents=True, exist_ok=True)

    tasks = []
    for i in range(150):
        output_file = output_sim_dir / f"{sim_tag}_{i}.npz"
        tasks.append(simulate_one(output_file))

    # Run up to N simulations concurrently
    await asyncio.gather(*tasks)

    print(f"Simulation completed. Results saved in {output_sim_dir}")


def main():
    parser = argparse.ArgumentParser(description="Run a simulation (async)")
    parser.add_argument('--input_dir', type=str, required=True, help='Path to input file')
    parser.add_argument('--output_dir', type=str, required=True, help='Path to simulation output directory')
    parser.add_argument('--sim_tag', type=str, required=True, help='Simulation tag')
    args = parser.parse_args()

    asyncio.run(run_simulation(args.input_dir, args.output_dir, args.sim_tag))

if __name__ == "__main__":
    main()