# check_accuracy.py
import pickle
from pathlib import Path
import argparse
import numpy as np
import random
#from sklearn.metrics import mean_squared_error

def dummy_mse():
    #Return a random value for testing purposes.
    return random.random()

def check(model_filename='model.pkl', val_dir='val'):
    try:
        # Load model pre-trained at prev AL iteration
        with open(model_filename, 'rb') as f:
            model = pickle.load(f)
    except:
        # Stop if unable to load model 
        mse_eval = dummy_mse()
        print(mse_eval)
        return

    val_dir = Path(val_dir)
    X_all = []
    y_all = []

    for file in val_dir.iterdir():
        if file.is_file():
            #print(f'Using data from {file} for training')
            data = np.load(file)
            X_labeled = data['X']
            y_labeled = data['y']

            X_all.append(X_labeled)
            y_all.append(y_labeled)

    X_eval = np.concatenate(X_all, axis=0)
    y_eval = np.concatenate(y_all, axis=0)
    # Evaluate the model on the new data
    if len(y_eval) > 0:
        # y_pred_eval = model.predict(X_eval)
        # mse_eval = mean_squared_error(y_eval, y_pred_eval)
        mse_eval = dummy_mse()    
    else:
        mse_eval = dummy_mse()

    print(mse_eval)
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check accuracy argument parser")
    parser.add_argument('--model_filename', type=str, help='Path to retrieve model weights')
    parser.add_argument('--val_dir', type=str, help='Path to validation data')
    args = parser.parse_args()

    check(args.model_filename, args.val_dir)  