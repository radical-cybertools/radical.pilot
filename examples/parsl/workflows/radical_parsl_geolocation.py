import os 
import parsl
import radical.pilot as rp
from parsl import File
from parsl.config import Config
from parsl.app.app import python_app, bash_app
from radical.pilot.agent.executing.parsl_rp import RADICALExecutor as RADICALExecutor

parsl.set_stream_logger()

config = Config(
         executors=[RADICALExecutor(
                        label = 'RADICALExecutor',
                        resource = 'xsede.comet_ssh_funcs',
                        login_method = 'gsissh',
                        project = 'unc100',
                        partition = 'gpu', 
                        walltime = 60,
                        managed = True,
                        max_tasks = 192,
                        gpus = 32)
                        ],
strategy= None,
usage_tracking=True)

parsl.load(config)

@python_app
def sift(gpu_id, nproc, ngpus)-> str:
    import os
    import subprocess
    os.system("export LD_LIBRARY_PATH=/oasis/projects/nsf/unc100/aymen/anaconda3/lib:$LD_LIBRARY_PATH")
    proc = subprocess.Popen("CUDA_VISIBLE_DEVICES={0} $HOME/RADICAL/integration_usecases/geolocation/CudaSift/cudasift"
                            " $HOME/RADICAL/integration_usecases/geolocation/CudaSift/msg-1-fc-40.jpg"
                            " 2000 2000 2000 2000 $HOME/RADICAL/integration_usecases/geolocation/CudaSift/msg-1-fc-40-1.jpg"
                            " 2000 2000 2000 2000".format(gpu_id), shell = True, stdout=subprocess.PIPE)
    match_path = str(proc.stdout.readlines()[-1].split()[0], 'utf-8')
    return match_path

@python_app
def ransac(sift_matches_file, nproc):  #python function has no ptype
    import csv
    import cv2
    import numpy as np
    from matplotlib import pyplot as plt
    
    MIN_MATCH_COUNT = 10
    
    img1 = cv2.imread('/home/aymen/RADICAL/integration_usecases/geolocation/CudaSift/msg-1-fc-40.jpg',0)    # queryImage
    img2 = cv2.imread('/home/aymen/RADICAL/integration_usecases/geolocation/CudaSift/msg-1-fc-40-1.jpg',0)   # trainImage

    data_pt1 = []
    data_pt2 = []
    print(sift_matches_file)
    with open(sift_matches_file, 'r') as csvfile:
         csv_reader = csv.reader(csvfile, delimiter=',')
         csv_reader.__next__()
         for lines in csv_reader:
             pt1 = (float(lines[0]), float(lines[1]))
             pt2 = (float(lines[7]), float(lines[8]))
             data_pt1.append(pt1)
             data_pt2.append(pt2)
    
    src_pts = np.float32(data_pt1).reshape(-1,1,2)
    dst_pts = np.float32(data_pt2).reshape(-1,1,2)

    M, mask     = cv2.findHomography(src_pts, dst_pts, cv2.RANSAC,5.0)
    matchesMask = mask.ravel().tolist()

    h,w  = img1.shape
    pts  = np.float32([ [0,0],[0,h-1],[w-1,h-1],[w-1,0] ]).reshape(-1,1,2)
    dst  = cv2.perspectiveTransform(pts,M)

    return True
    

sift_results   = []
ransac_results = []
num_images     = 64
num_gpus       = 4

# submit image matching tasks [(we will generate a task of num_images * num_gpus) *2]
for i in range(num_images):
    for j in range(num_gpus):
        sift_results.append(sift(gpu_id=j ,nproc=1, ngpus=1)) # GPU 0
        sift_results.append(sift(gpu_id=j ,nproc=1, ngpus=1)) # GPU 0

# print each job status, they will now be finished
print ("Job Status: {}".format([r.done() for r in sift_results]))

for i in range(len(sift_results)):
    ransac_results.append(ransac(sift_results[i], nproc=1))

# wait for all ransac apps to complete
[r.result() for r in ransac_results]

# print each job status, they will now be finished
print ("Job Status: {}".format([r.done() for r in ransac_results]))
