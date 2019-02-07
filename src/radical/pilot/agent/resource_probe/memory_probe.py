from subprocess import Popen, PIPE

if __name__ == '__main__':

    torque_nodefile = os.environ.get('PBS_NODEFILE')
    # Parse PBS the nodefile
    torque_nodes = [line.strip() for line in open(torque_nodefile)]
    node_dict = dict()
    for node in torque_nodes:
        if node not in node_dict.keys():
            node_dict[node] = 0
        # cmd = [ 'ssh %s'%node, 'module load python/2.7.7-anaconda',
        #         'python -c "import psutil; print psutil.virtual_memory()[1]"']
        cmd = ['echo test']
        proc = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
        out, err = proc.communicate()
        node_dict[node] = float(out)
