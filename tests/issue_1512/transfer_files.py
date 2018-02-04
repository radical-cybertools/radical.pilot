import os
import tempfile
import saga          as rs
from time import time


def transfer_file(src_files,target_dir,resource):

    # Remote transfering happens by using a saga session
    # Here we create one. We are adding  as connecting context
    # gsissh
    ctx = rs.Context("gsissh")
    session = rs.Session()
    session.add_context(ctx)

    # Setting up the remote filesystem connection. `remote_dir` now is a 
    target_connection = resource+':2222/'
    remote_dir = saga.filesystem.Directory('gsisftp://'+ target,
                                               session=session)

    # Create the remote directory with all the parents
    remote_dir.make_dir(target_dir, flags=rs.filesystem.CREATE_PARENTS)

    # Do the actual transfer.
    start_time = time()
    for src_file in src_files:
        target_file = target_dir + src_file.split('/')[-1]
        remote_dir.copy(src_file, target_file, flags=copy_flags)

    end_time = time()

    print(end_time-start_time)
