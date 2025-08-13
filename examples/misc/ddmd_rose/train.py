# train.py
from pathlib import Path
import pickle
from sklearn.linear_model import LinearRegression
import argparse
import numpy as np
import random
import shutil
import json
VAL_SPLIT = 0.5
MIN_TRAIN_SIZE = 1
MIN_NUM_TO_PREDICT = 1

def data_loading(sim_output_path='sim_output', registered_sims_filename='registered_sims.json', 
                 train_path='train_data', val_path='val_data'):

    train_path = Path(train_path)
    if not train_path.is_dir():
        train_path.mkdir(parents=True, exist_ok=True)
    val_path = Path(val_path)
    if not val_path.is_dir():
        val_path.mkdir(parents=True, exist_ok=True)

    if not Path(registered_sims_filename).is_file():
        return False
    
    with open(registered_sims_filename, 'r') as f:
        sim_inds = json.load(f)
    train_num = 0
    val_num = 0

    #Parent directory with subdirs from each simulation
    sim_output_path = Path(sim_output_path)

    #for dir in sim_output_path.iterdir():
    for sim_ind in sim_inds:
        sim_results = Path(sim_output_path / sim_ind)

        print(f'using data from {sim_results}')

        #Collect data from directory sim_1, sim2 etc
        if sim_results.is_dir():
        
            files = [f.name for f in sim_results.iterdir()]
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
    
    print(f'Will use {train_num} files for training and {val_num} for validation')
    if train_num > MIN_TRAIN_SIZE and val_num > 0:
        return True
    else:
        return False

def train(model_filename='model.pkl', sim_output_path='sim_output', 
          registered_sims_filename='registered_sims.json', 
        train_path='train_data', val_path='val_data'):

    while True:
        data_ready = data_loading(sim_output_path=sim_output_path, 
                        registered_sims_filename=registered_sims_filename,
                        train_path=train_path, val_path=val_path)
        if data_ready:
            break

    try:
        # Load model pre-trained at prev AL iteration
        with open(model_filename, 'rb') as f:
            model = pickle.load(f)
    except:
        # Train a simple linear regression model
        model = LinearRegression()

    train_path = Path(train_path)
    X_all = []
    y_all = []

    train_path = Path(train_path)
    if not train_path.is_dir():
        train_path.mkdir(parents=True, exist_ok=True)

    for file in train_path.iterdir():
        if file.is_file():
            print(f'Using data from {file} for training')

            data = np.load(file)
            X_labeled = data['X']
            y_labeled = data['y']

            X_all.append(X_labeled)
            y_all.append(y_labeled)

    if len(y_all) > 0:
        X_combined = np.concatenate(X_all, axis=0)
        y_combined = np.concatenate(y_all, axis=0)
        if len(y_labeled) > MIN_NUM_TO_PREDICT:
            model.fit(X_combined, y_combined)

            # Save the trained model
            with open(model_filename, 'wb') as f:
                pickle.dump(model, f)
        
            print(f"Model saved to {model_filename}")

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Training argument parser")
    parser.add_argument('--model_filename', type=str, help='Path to store model weights')
    parser.add_argument('--train_path', type=str, help='Path to training data')
    parser.add_argument('--sim_output_path', type=str, help='Path to simulation output data')
    parser.add_argument('--val_path', type=str, help='Path to validation data')
    parser.add_argument('--registered_sims_filename', type=str, help='Path to store registered simulation tags')

    args = parser.parse_args()

    train(args.model_filename, args.sim_output_path, args.registered_sims_filename, args.train_path, args.val_path)  # Running the training task