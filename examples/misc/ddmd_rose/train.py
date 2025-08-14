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
MIN_TRAIN_SIZE = 10
MIN_NUM_TO_PREDICT = 1

def check_sim_results(sim_output_path, registered_sims_filename):
    files = []
    
    if not Path(registered_sims_filename).is_file():
        return False
    
    with open(registered_sims_filename, 'r') as f:
        sim_inds = json.load(f)

    for sim_ind in sim_inds:
        sim_results = Path(sim_output_path / sim_ind)
        if sim_results.exists():
            files= files + [f.name for f in sim_results.iterdir()]
    
    if len(files) > MIN_TRAIN_SIZE:
        print(f'Will use {len(files)} files for training')
        return True
    else:
        return False

def data_loading(sim_output_path, registered_sims_filename, 
                 train_path, val_path):

    train_path = Path(train_path)
    if not train_path.exists():
        train_path.mkdir(parents=True, exist_ok=True)
    val_path = Path(val_path)
    if not val_path.exists():
        val_path.mkdir(parents=True, exist_ok=True)
    
    with open(registered_sims_filename, 'r') as f:
        sim_inds = json.load(f)

    #Parent directory with subdirs from each simulation
    sim_output_path = Path(sim_output_path)

    #for dir in sim_output_path.iterdir():
    for sim_ind in sim_inds:
        sim_results = Path(sim_output_path / sim_ind)

        #Collect data from directory sim_1, sim2 etc
        if sim_results.exists():
        
            files1 = [f for f in sim_results.iterdir()]
            
            files = [f.name for f in sim_results.iterdir()]
            if len(files) == 0:
                continue
            random.shuffle(files)
            print(f'using data from {sim_results}')
            print(files1)

            #Split all files for current simulation into train and val subsets
            val_subset = int(len(files) * VAL_SPLIT)
            train_files = files[val_subset:]
            val_files = files[:val_subset]
            for filename in train_files:
                print('\nmoving', filename)
                source_file = sim_results / filename
                target_file = train_path / filename
                shutil.move(source_file, target_file)
            for filename in val_files:
                print('moving', filename)
                source_file = sim_results / filename
                target_file = val_path / filename
                shutil.move(source_file, target_file)

def train(model_filename='model.pkl', sim_output_path='sim_output', 
          registered_sims_filename='registered_sims.json', 
        train_path='train_data', val_path='val_data'):


    train_path = Path(train_path)
    val_path = Path(val_path)
    sim_output_path = Path(sim_output_path)

    while True:
        data_ready = check_sim_results(sim_output_path, registered_sims_filename)
        if data_ready:
            break

    data_loading(sim_output_path=sim_output_path, 
                        registered_sims_filename=registered_sims_filename,
                        train_path=train_path, val_path=val_path)
    try:
        # Load model pre-trained at prev AL iteration
        with open(model_filename, 'rb') as f:
            model = pickle.load(f)
    except:
        # Train a simple linear regression model
        model = LinearRegression()

    
    X_all = []
    y_all = []

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