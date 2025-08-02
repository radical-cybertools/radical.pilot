# active_learn.py
import argparse
import time

VAL_SPLIT = 0.2
MIN_TRAIN_SIZE = 1

def active_learn():
    #sim_output_path='sim_output', registered_sims_filename='registered_sims.json', train_path='train_data', val_path='val_data'):
    print('starting active_learn')
    time.sleep(20)
    print('finished active_learn')

if __name__ == "__main__":
    # parser = argparse.ArgumentParser(description="Active Learning argument parser")
    # parser.add_argument('--sim_output_path', type=str, help='Path to simulation output data')
    # parser.add_argument('--train_path', type=str, help='Path to training data')
    # parser.add_argument('--val_path', type=str, help='Path to validation data')
    # parser.add_argument('--registered_sims_filename', type=str, help='Path to store registered simulation tags')
    # args = parser.parse_args()

    #active_learn(args.sim_output_path, args.registered_sims_filename, args.train_path, args.val_path)
    active_learn()