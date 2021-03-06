`
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
                        resource = 'local.localhost_funcs',
                        login_method = 'local',
                        project = '',
                        partition = '',
                        walltime = 30,
                        managed = True,
                        max_tasks = 1)
                        ],
strategy= None,
usage_tracking=True)

parsl.load(config)

@python_app
def sift(ptype, nproc)-> str:
    import subprocess as sp
    cmd  = '$HOME/cuda_sift/cudasift'
    args = [img1,x1,y1,img2,x2,y2]
    output_file = sp.Popen(cmd, args, stdout = PIPE, stderr= PIPE)
    return output_file

@python_app
def ransac(sift_matches_files:str):  #python function has no ptype
    import csv
    import numpy as np
    import cv2
    from matplotlib import pyplot as plt
    
    MIN_MATCH_COUNT = 10
    
    img1 = cv2.imread('~/integration_usecases/geolocation/CudaSift/msg-1-fc-40.jpg',0)   # queryImage
    img2 = cv2.imread('~/integration_usecases/geolocation/CudaSift/msg-1-fc-40.jpg',0)   # trainImage
    
    data_pt1 = []
    data_pt2 = []
    good = data_pt1
    DESIRED_COLUMNS = ('x1','y1','x2','y2')
    with open(sift_matches_files, 'r') as csvfile:
         csv_reader = csv.reader(csvfile, delimiter=',')
         csv_reader.__next__()
         for lines in csv_reader:
             pt1 = (float(lines[0]), float(lines[1]))
             pt2 = (float(lines[7]), float(lines[8]))
             data_pt1.append(pt1)
             data_pt2.append(pt2)
    
    src_pts = np.float32(data_pt1).reshape(-1,1,2)
    dst_pts = np.float32(data_pt2).reshape(-1,1,2)
    
    M, mask = cv2.findHomography(src_pts, dst_pts, cv2.RANSAC,5.0)
    matchesMask = mask.ravel().tolist()
    
    print(img1)
    h,w = img1.shape
    pts = np.float32([ [0,0],[0,h-1],[w-1,h-1],[w-1,0] ]).reshape(-1,1,2)
    dst = cv2.perspectiveTransform(pts,M)
    img2 = cv2.polylines(img2,[np.int32(dst)],True,255,3, cv2.LINE_AA)
    

results  = []
out_file = "/home/aymen/mathma_{0}".format(0)
results.append(mathma(10, nproc=1))


# wait for all apps to complete
[r.result() for r in results]

# print each job status, they will now be finished
print ("Job Status: {}".format([r.done() for r in results]))
