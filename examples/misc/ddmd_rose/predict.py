# predict.py
import pickle
import random
import json
import numpy as np
from pathlib import Path
from sklearn.metrics import mean_squared_error
import argparse

threshold = 0.75

def predict(model_filename='model.pkl', sim_output_path='sim_output', registered_sims_filename=['sim_0'], output_file='results.json'):

    try:
        # Load model
        with open(model_filename, 'rb') as f:
            model = pickle.load(f)
    except:
        #raise RuntimeError("Unable to read {model_filename}")
        #return
        pass

    results = {}
    sim_output_path = Path(sim_output_path)
    with open(registered_sims_filename, 'r') as f:
        sim_inds = json.load(f)

    for sim_ind in sim_inds:
        sim_dir = Path(sim_output_path / sim_ind)
        if sim_dir.is_dir():
            mses = []

            for file in sim_dir.iterdir():
                if file.is_file():
                    #print(f'Using data from {file} for training')
                    data = np.load(file)
                    X_eval = data['X']
                    y_eval = data['y']

                    if len(y_eval) > 0:
                        #y_pred_eval = model.predict(X_eval)
                        #mse_eval = mean_squared_error(y_eval, y_pred_eval)
                        mse_eval = random.random()
                        mses.append(mse_eval)
                        #print(f'{dir} - mse={results[dir]}')
                    else:
                        sim_dir.rmdir()
            results[sim_dir.name] = np.mean(mses)
    print(f'\n\nPrediction completed. Saving results to {output_file}.')
    with open(output_file, 'w') as f:
        json.dump(results, f)



if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Prediction argument parser")
    parser.add_argument('--model_filename', type=str, help='Path to retrieve model weights')
    parser.add_argument('--sim_output_path', type=str, help='Path to simulation output data')
    parser.add_argument('--registered_sims_filename', type=str, help='Path to store registered simulation tags')
    parser.add_argument('--output_file', type=str, help='Path to simulation output data')
    args = parser.parse_args()

    predict(args.model_filename, args.sim_output_path, args.registered_sims_filename, args.output_file)  # Running the training task