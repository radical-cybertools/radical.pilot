# sim.py
import time
import argparse
from pathlib import Path
import numpy as np
import random

def complicated_function(x):
    return (
        0.3 * np.sin(1.5 * np.pi * x**2) +
        0.2 * np.cos(2 * np.pi * x**3) +
        0.5 * np.exp(-0.5 * x) +
        0.1 * np.tanh(0.2 * (x - 0.5)) +
        0.3 * (x**3)
        )

def sim(input_path='input.file', output_path='output.dir', sim_tag=0):

    print(f'Simulation will read from {input_path}')
    output_sim_path = Path(output_path, sim_tag)
    if not output_sim_path.is_dir():
        output_sim_path.mkdir(parents=True, exist_ok=True)
    
    for i in range(150):
        
        X = np.random.uniform(low=0.0, high=1.0, size=500)
        y = complicated_function(X)
        X = X.reshape(-1, 1)
        output_file = f'{output_sim_path}/r{sim_tag}_{i}.npz'
        np.savez(output_file, X=X, y=y)
        print(f'Saved simulation {i} to {output_file}')
        t = random.randint(100,150)
        time.sleep(t)

    #print(f'Simulation completed and all results saved to {output_dir}')
    #return


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Simulation argument parser")
    parser.add_argument('--input_path', type=str, help='Path to Input file')
    parser.add_argument('--output_path', type=str, help='Path to simulation output data')
    parser.add_argument('--sim_tag', help='Simulation tag')
    args = parser.parse_args()

    sim(args.input_path, args.output_path, args.sim_tag) 