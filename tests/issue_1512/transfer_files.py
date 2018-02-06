import os
import tempfile
import saga  as rs
import radical.utils as ru
from time import time
import glob
import argparse
from radical.pilot.staging_directives import complete_url
import tarfile

def transfer_file(src_files,target_dir,resource,tar):


    # Remote transfering happens by using a saga session
    # Here we create one. We are adding  as connecting context
    # gsissh
    #ctx = rs.Context("gsissh")
    session = rs.Session()
    #session.add_context(ctx)

    # Setting up the remote filesystem connection. `remote_dir` now is a 
    target_connection = resource+':2222/'
    remote_dir = rs.filesystem.Directory('gsisftp://'+ target_connection,
                                               session=session)

    # Create the remote directory with all the parents
    remote_dir.make_dir(target_dir, flags=rs.filesystem.CREATE_PARENTS)
    tgt_dir = 'gsisftp://' + target_connection + target_dir
    # Do the actual transfer.
    start_time = time()

    if tar:
        SrcFiles = ['tartest.tar']
        tar = tarfile.open(SrcFiles[0],"w")
        for filename in src_files:
            tar.add(filename)
        tar.close()
    else:
        SrcFiles = src_files

    for src_file in SrcFiles:
        src_filename = src_file.split('/')[-1]
        src_context = {'pwd'      : os.path.dirname(os.path.abspath(src_file)),
                       'unit'     : tgt_dir, 
                       'pilot'    : tgt_dir,
                       'resource' : tgt_dir}
        tgt_context = {'pwd'      : tgt_dir,
                       'unit'     : tgt_dir, 
                       'pilot'    : tgt_dir, 
                       'resource' : tgt_dir}
        src = complete_url(src_filename,src_context,ru.get_logger('transfer.files'))
        tgt = complete_url(src_filename,tgt_context,ru.get_logger('transfer.files'))
        
        #target_file = target_dir + src_file.split('/')[-1]
        remote_dir.copy(src,tgt, flags=rs.filesystem.CREATE_PARENTS)

    end_time = time()

    print(end_time-start_time)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('srcfolder',help='Folder which contains the files to be transfered')
    parser.add_argument('resource', help="Target Resource that support GSISFTP")
    parser.add_argument('tgtfolder', help="Target folder. Should be the whole path.")
    parser.add_argument('--tar',help='Transfer without tarball or with',default=False,action="store_true")
    args = parser.parse_args()
    
    TansferFiles = glob.glob(args.srcfolder+'/*')

    transfer_file(TansferFiles,args.tgtfolder,args.resource,args.tar)
