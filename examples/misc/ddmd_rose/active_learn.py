# active_learn.py
import argparse
import json
from pathlib import Path
import random
import shutil

VAL_SPLIT = 0.2
MIN_TRAIN_SIZE = 1

def active_learn(sim_output_path='sim_output', registered_sims_filename='registered_sims.json', train_path='train_data', val_path='val_data'):

    train_path = Path(train_path)
    if not train_path.is_dir():
        train_path.mkdir(parents=True, exist_ok=True)
    val_path = Path(train_path)
    if not val_path.is_dir():
        val_path.mkdir(parents=True, exist_ok=True)

    with open(registered_sims_filename, 'r') as f:
        sim_inds = json.load(f)
    train_num = 0
    val_num = 0

    #Parent directory with subdirs from each simulation
    sim_output_path = Path(sim_output_path)

    #for dir in sim_output_path.iterdir():
    for sim_ind in sim_inds:
        sim_results = Path(sim_output_path / sim_ind)

        #Collect data from directory sim_1, sim2 etc
        if sim_results.is_dir():
        
            files = [f for f in sim_results.iterdir()]
            if len(files) == 0:
                continue
            random.shuffle(files)

            #Split all files for current simulation into train and val subsets
            val_subset = int(len(files) * VAL_SPLIT)
            train_files = files[val_subset:]
            val_files = files[:val_subset]
            for filename in train_files:
                source_file = sim_results / filename
                target_file = train_path / filename
                shutil.move(source_file, target_file)
                train_num += 1
            for filename in val_files:
                source_file = sim_results / filename
                target_file = val_path / filename
                shutil.move(source_file, target_file)
                val_num += 1
    
    if train_num > MIN_TRAIN_SIZE and val_num > 0:
        print('start_train')
    else:
        print('not_yet')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Active Learning argument parser")
    parser.add_argument('--sim_output_path', type=str, help='Path to simulation output data')
    parser.add_argument('--train_path', type=str, help='Path to training data')
    parser.add_argument('--val_path', type=str, help='Path to validation data')
    parser.add_argument('--registered_sims_filename', type=str, help='Path to store registered simulation tags')
    args = parser.parse_args()

    active_learn(args.sim_output_path, args.registered_sims_filename, args.train_path, args.val_path)