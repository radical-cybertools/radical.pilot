import os
import sys
from stat import *
from subprocess import *
import shutil
from string import Template
import xmlrpclib
import api
import time
from random import choice
import bliss.saga as saga

"""
Job States

* Unknown
  Not part of the SPEC...

* New
  This state identies a newly constructed job instance which has not yet run.
  This state corresponds to the BES state Pending. This state is initial.

* Running     
  The run() method has been invoked on the job, either explicitly or implicitly.
  This state corresponds to the BES state Running. This state is initial.

* Done    
  The synchronous or asynchronous operation has finished successfully. It
  corresponds to the BES state Finished. This state is final.

* Canceled    
  The asynchronous operation has been canceled, i.e. cancel() has been called on
  the job instance. It corresponds to the BES state Canceled. This state is final.

* Failed  
  The synchronous or asynchronous operation has finished unsuccessfully. It
  corresponds to the BES state Failed. This state is final.

* Suspended   
  Suspended identifies a job instance which has been suspended. This state
  corresponds to the BES state Suspend. 

"""



def get_uuid():
    wd_uuid=""
    if sys.version_info < (2, 5):
        uuid_str = os.popen("/usr/bin/uuidgen").read()
        wd_uuid += uuid_str.rstrip("\n")
            
        #preparation for fail-safe exit
        #sys.stderr.write('Incompatible Python version found! Please use Python 2.5 or higher with BigJob!') 
        #sys.exit(-1)
    else:   
        import uuid
        wd_uuid += str(uuid.uuid1())
    return wd_uuid

class PilotComputeService(api.PilotComputeService):
    
    def __init__(self, master_host, master_port):

        print "Connecting to DIANE master on %s:%s ..." % (master_host, master_port)
        try:
            self.rpcproxy = xmlrpclib.ServerProxy('http://%s:%s/' % (master_host, master_port),
                verbose=False, allow_none=True)
        except Exception, err:
            print 'ERROR:', err
            sys.exit(-1)

        self.compute_pilots = []


    def __del__(self):
        # TODO: kill the right master, not the most recent
        #print 'Killing the master'
        #import os
        #os.system("diane-master-ping kill")
        pass

    def create_pilot(self, pilot_compute_description, context=None):
        try:
            d = pilot_compute_description.copy()
            if 'number_of_processes' not in d:
                d['number_of_processes'] = 1
            if 'working_directory' not in d:
                d['working_directory'] = '.'
            id = self.rpcproxy.create_pilot(d)
        except Exception, err:
            print 'ERROR:', err
            sys.exit()
        pc = PilotCompute(id)
        pc.pcs = self

        self.compute_pilots.append(pc)

        return pc

    def get_state_detail(self): 
        # TODO
        print 'get_state_detail'
        return "running"
    
    def cancel(self):        
        # TODO: kill master? agents?
        pass
                    
    def _submit_su(self, sud):
        id = get_uuid()
        d = sud.copy()
        su = self.rpcproxy.submit_schedunit(d, id)
        return id

    def _get_su_metric(self, id, metric):
        value = self.rpcproxy.get_su_metric(id, metric)
        return value

    def _get_su_state(self, id):
        state = self.rpcproxy.get_su_state(id)
        return state

    def _get_su_state_detail(self, id):        
        detail = self.rpcproxy.get_su_state_detail(id)
        return detail

    def list_pilots(self):
        loc_p = self.compute_pilots
        rem_p = self.rpcproxy.list_pilots()

        ret = []

        for r in rem_p:
            # find previously used object
            found = False
            for l in loc_p:
                if r == l.id:
                    print 'INFO: Re-used PC found'
                    found = True
                    ret.append(l)
            if found == False:
                #print 'WARNING: No previous PC found!'
                pc = PilotCompute(r)
                pc.pcs = self
                ret.append(pc)
                    
        return ret

    def _get_pc_state_detail(self, id):
        detail = self.rpcproxy.get_pc_state_detail(id)
        return detail

    def _get_pc_state(self, id):
        state = self.rpcproxy.get_pc_state(id)
        return state

    def _list_sus(self):
        # CUs == SUs for now ...
        l = self.rpcproxy.list_sus()
        return l
    
    def _list_taskq_hist(self):
        l = self.rpcproxy.list_taskq_hist()
        return l

    def wait(self):

        # find pending jobs
        l = self._list_sus()
        not_yet = True
        while not_yet:
            not_yet = False
            for s in l:
                state = self._get_su_state(s)
                if state not in ['Done', 'Failed', 'Canceled']:
                    not_yet = True
            time.sleep(1)

class PilotCompute(api.PilotCompute):

    def __init__(self, id=None):
        if id:
            self.id = id

    def get_state(self):
        state = self.pcs._get_pc_state(self.id)
        return state

    def get_state_detail(self):
        detail = self.pcs._get_pc_state_detail(self.id)
        return detail

class ComputeDataService(api.ComputeDataService):

    def __init__(self):
        self.pcs = None
        self.pds = None
        self.cus = []
        self.start_time = time.time()

    def cancel(self):
        self.stop_time = time.time()

    def add_pilot_compute_service(self, pcs):
        if self.pcs:
            print 'ERROR: Can only hold one PilotComputeService currently.'
        else:
            pcs.cds = self
            self.pcs = pcs

    def add_pilot_data_service(self, pds):

        if self.pds:
            print 'ERROR: Can only hold one PilotDataService currently.'
        else:
            pds.cds = self
            self.pds = pds

    def submit_compute_unit(self, cud):
        cu = ComputeUnit()
        cu.description = cud
        cud['dead_on_arrival'] = False
        cu.cds = self
        input_du = None

        if 'input_data' in cud:

            # used by output_data
            if cud['input_data']:
                input_du = cud['input_data'][0]

            l = []
            for du in cud['input_data'] :

                # Check if the input DU is actually alive
                if du.get_state() == 'Done':
                    print 'INFO: got a failed DU as input, cancelling CU'
                    cud['dead_on_arrival'] = True
                    
                # TODO: Need better way of selecting PD
                if not hasattr(du, 'pd'):
                    print 'INFO: DU has no PD yet, adding to one'
                    choice(self.pds.list_pilots()).add_data_unit(du)
                # Build list of input uris based on du's
                for item in du.list_data_unit_items():
                    if not '*' in item:
                        if du.static:
                            l.append(os.path.join(du.pd.service_url,
                                    'static', item))
                        else:
                            item_basename = os.path.basename(item)
                            l.append(os.path.join(du.pd.service_url, du.id,
                                    item_basename))
                    else:
                        raise('input dus should not have wildcards?')

            cud['input_data'] = l

        if 'output_data' in cud:
            l = []
            for du in cud['output_data'] :
                
                if du.static:
                    raise 'ERROR: Output DU should not be static'

                # Assume that if the input DU is static, the output DU
                # can go anywhere, otherwise use the same as the input
                if input_du:
                    if not input_du.static:
                        pd = input_du.pd
                    else:
                        pd = choice(self.pds.list_pilots())
                else:
                    # there was no input DU, select one random
                    pd = choice(self.pds.list_pilots())

                pd.add_data_unit(du)

                # set the cu that will generate this du.
                # this dependency is used to determine if a du is done later.
                du.cu = cu

                du.state = 'Pending'
                for item in du.list_data_unit_items():
                    item_basename = os.path.basename(item)
                    l.append(os.path.join(du.pd.service_url, du.id,
                            item_basename))

            cud['output_data'] = l

        cu.id = self.pcs._submit_su(cud)

        self.cus.append(cu)
        return cu

    def submit_data_unit(self, dud):
        du = self.pds._submit_du(dud)
        return du

    def wait(self):

        # wait for pending compute units
        self.pcs.wait()

        # wait for pending data units
        self.pds.wait()

    def list_compute_units(self):
        l = self.pcs._list_sus()

        r = []

        for id in l:
            # find previously used object
            found = False
            for c in self.cus:
                if c.id == id:
            #        print 'INFO: Re-used CU found'
                    found = True
                    r.append(c)
            #if found == False:
            #    print 'WARNING: No previous CU found!'

        return r

        
    def list_data_units(self):
        # XXX: return du.ids or dus?
        return [ lll.id for ll in self.pds._list_dus() for lll in ll]


class ComputeUnit(api.ComputeUnit):

    def __init__(self):
        pass

    def get_state(self):
        state = self.cds.pcs._get_su_state(self.id)
        return state


    def get_state_detail(self):        
        detail = self.cds.pcs._get_su_state_detail(self.id)
        return detail


    def list_metrics(self):
        return [ 
            'AFTERDOWNLOAD',
            'BEFOREUPLOAD',
            'CREAM_JOBID',
            'DEFAULTSE',
            'DOWNLOAD',
            'ERRNO',
            'EXECUTION',
            'HOSTNAME',
            'SITE_NAME',
            'START',
            'STOP',
            'TOTAL',
            'UPLOAD' ]


    def get_metric(self, metric):
        value = self.cds.pcs._get_su_metric(self.id, metric)
        return value
        


class DataUnit(api.DataUnit):

    def __init__(self, data_unit_description=None, static=False):
        self.id = get_uuid()
        self.items = []
        self.static = static

        if data_unit_description:
            self.description = data_unit_description

            for f in self.description['file_urls']:
                self.items.append(f)

            if self.static:
                self.state = 'Running'
            else:
                self.state = 'New'


    def wait(self):

        while True:
            try:
                state = self.get_state()
            except Exception, e:
                # TODO
                #raise 'Could not get state of DU:', e
                state = 'Unknown'
            if state not in ['Pending', 'New', 'Unknown']:
                break
            time.sleep(1)

    def list_data_unit_items(self):
        return self.items

    def get_state(self):
        # New => Initialized
        # Pending => Files are synchronized with a pilot store
        # Running => PD is in sync with all replicas
        # Done => Terminated

        if self.state == 'Done' or self.state == 'Running' \
                or self.state == 'New':
            return self.state

        # Still pending ...
        print 'du:get_state()'

        if hasattr(self, 'cu'):

            cu_state = self.cu.get_state()

            if cu_state == 'New' or cu_state == 'Running':
                print 'cu is new or not running, du state stays Pending'

            elif cu_state == 'Done':
                print 'cu is done, du state becomes Running'

                add_list = []
                del_list = []
                for file_url in self.list_data_unit_items():

                    if not '*' in file_url:
                        print 'this is not a wildcard'
                        continue
                    else:
                        print 'this is indeed a wildcard'
                    d = os.path.join(self.pd.service_url, self.id)
                    mydir = saga.filesystem.Directory(d)
                    for entry in mydir.list():
                        add_list.append(entry)

                    del_list.append(file_url)

                self.items = [x for x in self.items if x not in del_list]
                self.items += add_list

                self.state = 'Running'

            elif cu_state == 'Canceled' or cu_state == 'Failed' \
                    or cu_state == 'Suspended':   
                print 'cu failed, du state becomes Done'
                self.state = 'Done'

        return self.state

    def split(self, chunks=None, size=1):

        if chunks is not None and size != 1:
            raise('print either specify chunks or size, not both!')
        
        ret = []
        nr_items = len(self.list_data_unit_items())
        
        pd_dir = saga.filesystem.Directory(self.pd.service_url)

        for item in self.list_data_unit_items():
            chunk = DataUnit(data_unit_description={'file_urls': [item]})

            # Associate chunks with same PD as parent
            chunk.pd = self.pd

            chunk.state = 'Pending'

            src = os.path.join(self.pd.service_url, self.id, item)
            f = saga.filesystem.File(src)

            pd_dir.make_dir(chunk.id)

            dst = os.path.join(self.pd.service_url, chunk.id, item)
            print 'Moving %s -> %s' % (src, dst)
            succeed = False
            try:
                t_begin = time.time()
                f.move(dst)
                succeed = True
            except:
                print 'ERROR: move failed'
            t_end = time.time()
            self.pd.pds.transfer_hist.append((t_begin, t_end, 'split',
                    succeed, src, dst))

            chunk.state = 'Running'
        
            ret.append(chunk)

        pd_dir.remove(self.id)

        self.state = 'Done'

        return ret


    def export(self, dest_path):

        for item in self.list_data_unit_items():

            item_url = os.path.join(self.pd.service_url, self.id, item)
            dest_url = os.path.join(dest_path, item)

            print 'transfer: %s <- %s' % (dest_url, item_url)

            r = saga.filesystem.File(item_url)
            succeed = False
            try:
                t_begin = time.time()
                r.copy(dest_url)
                succeed = True
            except:
                print 'ERROR: export failed'
            t_end = time.time()
            self.pd.pds.transfer_hist.append((t_begin, t_end, 'export',
                    succeed, item_url, dest_url))


class PilotData(api.PilotData):

    def __init__(self, description):
        self.id = get_uuid()
        self.service_url = description['service_url']
        self.du_list = []

    def _list_dus(self):
        return self.du_list

    def add_data_unit(self, du):
        du.pd = self
        self.du_list.append(du)

    def _add_du(self, du):
        du.state = 'Pending'

        for file_url in du.list_data_unit_items():

            file_basename = os.path.basename(file_url)

            pd_url = os.path.join(self.service_url, du.id, file_basename)
            print 'transfer: %s -> %s' % (file_url, pd_url)

            r = saga.filesystem.File(file_url)
            succeed = False
            try:
                t_begin = time.time()
                r.copy(pd_url)
                succeed = True
                du.state = 'Running'
            except:
                du.state = 'Failed'
                # TODO: what other logic should be applied here?
            t_end = time.time()
            self.pds.transfer_hist.append((t_begin, t_end, '_add_du',
                    succeed, file_url, pd_url))

        du.pd = self

        self.du_list.append(du)
        

    def wait(self):
        # find pending data transfers
        for d in self._list_dus():
            d.wait()


class PilotDataService(api.PilotDataService):

    def __init__(self):
        self.pd_list = []
        self.pilot_index = 0

        self.transfer_hist = []



    def create_pilot(self, pilot_data_description):
        pd = PilotData(pilot_data_description)
        pd.pds = self
        self.pd_list.append(pd)
        return pd


    def list_pilots(self):
        return self.pd_list
        

    def _list_dus(self):
        l = []
        for p in self.list_pilots():
            l.append(p._list_dus())
        return l

    def _list_transfer_hist(self):
        return self.transfer_hist

    def _submit_du(self, dud):
        du = DataUnit(dud)
        du.pds = self

        # if index is higher than number of pilots, then wrap
        if self.pilot_index >= len(self.list_pilots()):
            self.pilot_index = 0

        # select pd based on last used
        pd = self.list_pilots()[self.pilot_index]
        # select next 
        self.pilot_index += 1

        pd._add_du(du)
        return du


    def wait(self):
        # find pending data transfers
        l = self.list_pilots()
        for p in l:
            p.wait()
