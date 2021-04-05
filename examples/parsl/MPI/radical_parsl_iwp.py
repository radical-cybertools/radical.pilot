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
                        resource = 'xsede.comet_ssh_funcs_mpirun',
                        login_method = 'gsissh',
                        project = 'unc100',
                        partition = 'gpu', 
                        walltime = 60,
                        managed = True,
                        max_tasks = 24,
                        gpus = 4)
                        ],
strategy= None,
usage_tracking=True)



parsl.load(config)

@python_app
def run_iwp(work,ngpus=int,
                 ptype=str,
                 nproc=int):

    import sys
    sys.path.append("/oasis/projects/nsf/unc100/aymen/IWP/local_dir/")
    import  iwp_run as iwp
    import iwp_divideimg as divide
    import iwp_inferenceimg as inference
    import iwp_stitchshpfile as stitch

    input_img_name             = work
    input_img_path             = r"/oasis/projects/nsf/unc100/aymen/IWP/local_dir/datasets/polygon/input_img/WV02_20100707232723_10300100_10km_test1.tif"
    worker_divided_img_subroot = r"/oasis/projects/nsf/unc100/aymen/IWP/local_dir/datasets/polygon/divided_img/WV02_20100707232723_10300100_10km_test1/"
    worker_output_shp_subroot  = r"/oasis/projects/nsf/unc100/aymen/IWP/local_dir/datasets/polygon/output_shp/WV02_20100707232723_10300100_10km_test1/"
    worker_finaloutput_subroot = r"/oasis/projects/nsf/unc100/aymen/IWP/local_dir/datasets/polygon/final_shp/WV02_20100707232723_10300100_10km_test1/"
    crop_size = 360

    x_resolution, y_resolution = divide.divide_image(input_img_path,
                                                     worker_divided_img_subroot,
                                                     crop_size)

    print('Divide Image is Done')

    
    POLYGON_DIR  = r"/oasis/projects/nsf/unc100/aymen/IWP/local_dir/datasets/polygon/"
    weights_path = r"/oasis/projects/nsf/unc100/aymen/IWP/local_dir/datasets/logs/ice_wedge_polygon20180823T1403/mask_rcnn_ice_wedge_polygon_0008.h5"
    inference.inference_image(worker_divided_img_subroot,
                              POLYGON_DIR,
                              weights_path,
                              worker_output_shp_subroot,
                              x_resolution,
                              y_resolution)

    print('Inference Image is Done')

    
    stitch.stitch_shapefile(worker_output_shp_subroot,
                            input_img_name,
                            worker_finaloutput_subroot)

    print('Stich Image is Done')

    
    return True

results = []
work    = ['WV02_20100707232723_10300100_10km_test1.tif']

num_nodes = 1  # Every IWP task takes 1 GPU Node (4 GPUs and 24 CPU cores) 

for i in range(num_nodes):

    results.append(run_iwp(work, ngpus=4,ptype=rp.MPI_FUNC, nproc=24))

# wait for all apps to complete
[r.result() for r in results]

# print each job status, they will now be finished
print ("Job Status: {}".format([r.done() for r in results]))
