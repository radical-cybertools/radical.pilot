# train.py
from pathlib import Path
import pickle
from sklearn.linear_model import LinearRegression
import argparse
import numpy as np

MIN_NUM_TO_PREDICT = 1

def train(model_filename='model.pkl', train_path='train'):

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
            #print(f'Using data from {file} for training')

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
    args = parser.parse_args()

    train(args.model_filename, args.train_path)  # Running the training task