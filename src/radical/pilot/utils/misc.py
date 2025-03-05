
# pylint: disable=global-statement

import os
import time

from typing import List, Union

import radical.utils as ru

# max number of t out/err chars to push to tail
MAX_IO_LOGLENGTH = 1024

# cache resource configs
_rcfgs = None


# ------------------------------------------------------------------------------
#
class FastTypedDict(ru.TypedDict):

    _deep = False


# ------------------------------------------------------------------------------
#
def tail(txt: str, maxlen: int = MAX_IO_LOGLENGTH) -> str:

    # shorten the given string to the last <n> characters, and prepend
    # a notification.  This is used to keep logging information in mongodb
    # manageable(the size of mongodb documents is limited).

    if not txt:
        return ''

    if len(txt) > maxlen:
        return "[... CONTENT SHORTENED ...]\n%s" % txt[-maxlen:]
    else:
        return txt


# ------------------------------------------------------------------------------
#
def get_rusage() -> str:

    import resource

    self_usage  = resource.getrusage(resource.RUSAGE_SELF)
    child_usage = resource.getrusage(resource.RUSAGE_CHILDREN)

    rtime = time.time()
    utime = self_usage.ru_utime  + child_usage.ru_utime
    stime = self_usage.ru_stime  + child_usage.ru_stime
    rss   = self_usage.ru_maxrss + child_usage.ru_maxrss

    return "real %3f sec | user %.3f sec | system %.3f sec | mem %.2f kB" \
         % (rtime, utime, stime, rss)


# ------------------------------------------------------------------------------
#
def create_tar(tgt: str, dnames: List[str]) -> None:
    '''
    Create a tarball on the file system which contains all given directories
    '''

    uid   = os.getuid()
    gid   = os.getgid()
    mode  = 16893
    mtime = int(time.time())

    fout  = open(tgt, 'wb')

    def rpad(s, size):
        return s + (size - len(s)) * '\0'

    def write_dir(path):
        data  = rpad(path, 100) \
              + rpad('%o' % mode,   8) \
              + rpad('%o' % uid,    8) \
              + rpad('%o' % gid,    8) \
              + rpad('%o' % 0,     12) \
              + rpad('%o' % mtime, 12) \
              + 8 * '\0' + '5'
        cksum = 256 + sum(ord(h) for h in data)
        data  = rpad(data  , 512)
        data  = data  [:-364] + '%06o\0' % cksum + data[-357:]
        fout.write(ru.as_bytes(data))

    for dname in dnames:
        write_dir(dname)

    fout.close()


# ------------------------------------------------------------------------------
#
def get_resource_configs() -> ru.Config:
    '''
    Return all resource configurations used by `radical.pilot`.

    Configurations for the individual resources are organized as sites and
    resources:

       - cfgs = get_resource_configs()
       - sites = cfgs.keys()
       - for site in sites: resource_names = cfgs[site].keys()

    Returns:

        :obj:`radical.utils.Config`: the resource configurations
    '''

    global _rcfgs

    if not _rcfgs:
        _rcfgs = ru.Config('radical.pilot.resource', name='*', expand=False)

    # return a deep copy
    return ru.Config(from_dict=_rcfgs)


# ------------------------------------------------------------------------------
#
def get_resource_config(resource: str) -> Union[None, ru.Config]:
    '''
    For the given resource label, return the resource configuration used by
    `radical.pilot`.


    Args:
        resource (:obj:`str`): resource label for which to return the cfg

    Returns:

        :obj:`radical.utils.Config`: the resource configuration

        The method returns `None` if no resource config is found for the
        specified resource label.
    '''

    # populate cache
    if not _rcfgs:
        get_resource_configs()

    site, host = resource.split('.', 1)

    if site not in _rcfgs:
        return None

    if host not in _rcfgs[site]:
        return None

    # return a deep copy
    return ru.Config(cfg=_rcfgs[site][host])


# ------------------------------------------------------------------------------
#
def get_resource_fs_url(resource: str,
                        schema  : str = None) -> Union[None, ru.Url]:
    '''
    For the given resource label, return the contact URL of the resource's file
    system.  This corresponds to the `filesystem_endpoint` setting in the
    resource config.

    For example,
    `rs.filesystem.directory(get_resource_fs_url(...)).change_dir('/')`
    is equivalent to the base ``endpoint:///`` URL available for use
    in a `staging_directive`.

    Args:
        resource (:obj:`str`): resource label for which to return the url

        schema (:obj:`str`, optional): access schema to use for resource
            access.  Defaults to the default access schema as defined in the
            resources config files.

    Returns:

        :obj:`radical.utils.Url`: the file system URL

        The method returns `None` if no resource config is found for the
        specified resource label and access schema.
    '''

    rcfg = get_resource_config(resource)

    if not schema:
        schema = rcfg['default_schema']

    # return a deep copy
    return ru.Url(rcfg['schemas'][schema]['filesystem_endpoint'])


# ------------------------------------------------------------------------------
#
def get_resource_job_url(resource: str,
                         schema  : str = None) -> Union[None, ru.Url]:
    '''
    For the given resource label, return the contact URL of the resource's job
    manager.


    Args:
        resource (:obj:`str`): resource label for which to return the url

        schema (:obj:`str`, optional): access schema to use for resource
            access.  Defaults to the default access schema as defined in the
            resources config files.

    Returns:

        :obj:`radical.utils.Url`: the job manager URL

        The method returns `None` if no resource config is found for the
        specified resource label and access schema.
    '''

    rcfg = get_resource_config(resource)

    if not schema:
        schema = rcfg['default_schema']

    # return a deep copy
    return ru.Url(rcfg.schemas[schema]['job_manager_endpoint'])


# ------------------------------------------------------------------------------
#
def convert_slots_to_new(slots, log=None):

    from ..resource_config import Slot, RO

    if not slots:
        return slots

    new_slots = list()
    for slot in slots:

        # check slot type: no need to convert if it is already new (>=1)
        if slot.get('version', 0) >= 1:
            new_slots.append(slot)
            continue

        cores = slot['cores']
        if cores:
            if isinstance(cores[0], RO):
                pass
            elif isinstance(cores[0], int):
                cores = [RO(index=i, occupation=1.0)
                         for i in slot['cores']]
            elif isinstance(cores[0], dict):
                cores = list()
                for ro in slot['cores']:
                    i = ro['index']
                    o = ro['occupation']
                    cores.append(RO(index=i, occupation=o))
            else:
                cores = [RO(index=i, occupation=o)
                         for i,o in slot['cores']]


        gpus = slot['gpus']
        if gpus:
            if isinstance(gpus[0], RO):
                pass
            elif isinstance(gpus[0], int):
                gpus  = [RO(index=i, occupation=1.0)
                         for i in slot['gpus']]
            elif isinstance(gpus[0], dict):
                gpus = list()
                for ro in slot['gpus']:
                    i = ro['index']
                    o = ro['occupation']
                    gpus.append(RO(index=i, occupation=o))
            else:
                gpus  = [RO(index=i, occupation=o)
                         for i,o in slot['gpus']]

        new_slot = Slot(cores=cores,
                        gpus=gpus,
                        lfs=slot['lfs'],
                        mem=slot['mem'],
                        node_index=slot['node_index'],
                        node_name=slot['node_name'])
        new_slots.append(new_slot)

    return new_slots


# ------------------------------------------------------------------------------
#
def convert_slots_to_old(slots, log=None):

    if not slots:
        return slots

    old_slots = list()
    for slot in slots:

        # if slots do not have a version field, they are old
        if not slot.get('version'):
            old_slots.append(slot)
            continue

        cores = slot['cores']
        if cores:
            if isinstance(cores[0], int):
                cores = [[c] for c in cores]
            elif isinstance(cores[0], dict):
                cores = [[ro['index']] for ro in cores]
            else:
                cores = [[ro.index] for ro in cores]

        gpus = slot['gpus']
        if gpus:
            if isinstance(gpus[0], int):
                gpus = [[c] for c in gpus]
            elif isinstance(gpus[0], dict):
                gpus = [[ro['index']] for ro in gpus]
            else:
                gpus = [[ro.index] for ro in gpus]

        old_slot = {'cores'     : cores,
                    'gpus'      : gpus,
                    'lfs'       : slot.get('lfs', 0),
                    'mem'       : slot.get('mem', 0),
                    'node_index': slot['node_index'],
                    'node_name' : slot['node_name']}
        old_slots.append(old_slot)

    return old_slots


# ------------------------------------------------------------------------------

