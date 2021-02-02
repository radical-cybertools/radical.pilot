
"""

    This is an example which shows how to access Amazon EC2 clouds via the
    RADICAL-SAGA resource package and RADICAL-Pilot.  We use SAGA to create
    a VM, which is then used as a ComputeResource (CR) for RADICAL-Pilot.

    In order to run this example, you need to set the following environment
    variables:

    * EC2_ACCESS_KEY:     your Amazon EC2 ID
    * EC2_SECRET_KEY:     your Amazon EC2 KEY
    * EC2_SSH_KEYPAIR_ID: name of ssh keypair within EC2
    * EC2_SSH_KEYPAIR:    your ssh keypair to use to access the VM, e.g.,
                          /home/username/.ssh/id_rsa_ec2
"""


import os
import sys
import time

import radical.saga  as rs
import radical.utils as ru
import radical.pilot as rp

dh = ru.DebugHelper()


# we use a reporter class for nicer output
report = ru.Reporter(name='radical.pilot')
report.title('Getting Started (RP version %s)' % rp.version)


# ------------------------------------------------------------------------------
#
def main():
    """
    We first use the RADIAL-SAGA resource package to start a CR.
    On that CR we run a pilot.
    On that pilot we run the workload.
    The we shut things down in inverse order.
    """

    cr    = None
    pilot = None

    try:
        report.header('startup')
        cr     = start_cr()
        pilot  = start_pilot(cr)
        run_workload (pilot)

    except Exception as e:
        print('Exception: %s' % e)
        raise

    finally:
        report.header('shutdown')
        stop_pilot   (pilot)
        stop_cr      (cr)

    return


# ------------------------------------------------------------------------------
#
def start_cr():
    """
    We use SAGA to start a VM (called Compute Resource (cr) in this context)
    """

    # In order to connect to EC2, we need an EC2 ID and KEY. We read those
    # from the environment.
    ec2_ctx = rs.Context('EC2')
    ec2_ctx.user_id  = os.environ['EC2_ACCESS_KEY']
    ec2_ctx.user_key = os.environ['EC2_SECRET_KEY']

    # The SSH keypair we want to use the access the EC2 VM. If the keypair is
    # not yet registered on EC2 saga will register it automatically.  This
    # context specifies the key for VM startup, ie. the VM will be configured to
    # accept this key
    ec2keypair_ctx = rs.Context('EC2_KEYPAIR')
    ec2keypair_ctx.token    = os.environ['EC2_KEYPAIR_ID']
    ec2keypair_ctx.user_key = os.environ['EC2_KEYPAIR']
    ec2keypair_ctx.user_id  = 'admin'  # the user id on the target VM

    # We specify the *same* ssh key for ssh access to the VM.  That now should
    # work if the VM go configured correctly per the 'EC2_KEYPAIR' context
    # above.
    ssh_ctx = rs.Context('SSH')
    ssh_ctx.user_id  = 'admin'
    ssh_ctx.user_key = os.environ['EC2_KEYPAIR']

    session = rs.Session(False)  # FALSE: don't use other (default) contexts
    session.contexts.append(ec2_ctx)
    session.contexts.append(ec2keypair_ctx)
    session.contexts.append(ssh_ctx)

    cr  = None  # compute resource handle
    rid = None  # compute resource ID
    try:

        # ----------------------------------------------------------------------
        #
        # reconnect to VM (ID given in ARGV[1])
        #
        if len(sys.argv) > 1:

            rid = sys.argv[1]

            # reconnect to the given resource
            print('reconnecting to %s' % rid)
            cr = rs.resource.Compute(id=rid, session=session)
            print('reconnected  to %s' % rid)
            print("  state : %s (%s)" % (cr.state, cr.state_detail))


        # ----------------------------------------------------------------------
        #
        # start a new VM
        #
        else:

            # start a VM if needed
            # in our session, connect to the EC2 resource manager
            rm = rs.resource.Manager("ec2://aws.amazon.com/", session=session)

            # Create a resource description with an image and an OS template,.
            # We pick a small VM and a plain Ubuntu image...
            cd = rs.resource.ComputeDescription()
            cd.image    = 'ami-e6eeaa8e'    # plain debain wheezy
            cd.template = 'Small Instance'

            # Create a VM instance from that description.
            cr = rm.acquire(cd)

            print("\nWaiting for VM to become active...")


        # ----------------------------------------------------------------------
        #
        # use the VM
        #
        # Wait for the VM to 'boot up', i.e., become 'ACTIVE'
        cr.wait(rs.resource.ACTIVE)

        # Query some information about the newly created VM
        print("Created VM: %s"      %  cr.id)
        print("  state   : %s (%s)" % (cr.state, cr.state_detail))
        print("  access  : %s"      %  cr.access)

        # give the VM some time to start up comlpetely, otherwise the subsequent
        # job submission might end up failing...
        time.sleep(60)

        return cr


    except Exception as e:
        # Catch all other exceptions
        print("An Exception occured: %s " % e)
        raise


# --------------------------------------------------------------------------
#
def stop_cr(cr):

    if not cr:
        return

    cr.destroy()
    print("\nDestroyed VM: %s" % cr.id)
    print("  state : %s (%s)" % (cr.state, cr.state_detail))


# --------------------------------------------------------------------------
#
def start_pilot(cr=None):
    """
    In order to start a pilot on the newly created CR, we need to define
    a resource description for that CR.  To do so, we programatically create
    a clone of the local.localhost description, and replace the job submission
    URL with an ssh:// URL pointing to the CR.
    """

    if not cr:
        class _CR (object):
            def __init__(self):
                self.access = 'ssh://remote.host.net:1234/'
        cr = _CR()

    # get the local resource config
    session = rp.Session()
    cfg = session.get_resource_config('local.localhost')

    # create a new config based on the local one, and add it back
    new_cfg = rp.ResourceConfig('ec2.vm', cfg)
    new_cfg.schemas = ['ssh']
    new_cfg['ssh']['job_manager_endpoint'] = cr.access
    new_cfg['ssh']['filesystem_endpoint']  = cr.access

    # the new config needs to make sure we can bootstrap on the VM
    new_cfg['pre_bootstrap_0'] = ['sudo apt-get update',
            'sudo apt-get install -y python-virtualenv python-dev dnsutils bc']
    session.add_resource_config(new_cfg)

    # use the *same* ssh key for ssh access to the VM
    ssh_ctx = rs.Context('SSH')
    ssh_ctx.user_id  = 'admin'
    ssh_ctx.user_key = os.environ['EC2_KEYPAIR']
    session.contexts.append(ssh_ctx)

    # submit a pilot to it.
    pd = rp.PilotDescription()
    pd.resource      = 'ec2.vm'
    pd.runtime       = 10
    pd.cores         = 1
    pd.exit_on_error = True,

    pmgr = rp.PilotManager(session=session)
    return pmgr.submit_pilots(pd)


# --------------------------------------------------------------------------
#
def run_workload(pilot):

    report.header('submit tasks')

    # Register the Pilot in a TaskManager object.
    tmgr = rp.TaskManager(session=pilot.session)
    tmgr.add_pilots(pilot)

    # Create a workload of Tasks.
    # Each task runs '/bin/date'.

    n = 128   # number of tasks to run
    report.info('create %d task description(s)\n\t' % n)

    tds = list()
    for i in range(0, n):

        # create a new Task description, and fill it.
        # Here we don't use dict initialization.
        td = rp.TaskDescription()
        # trigger an error now and then
        if not i % 10: td.executable = '/bin/data'  # does not exist
        else         : td.executable = '/bin/hostname'

        tds.append(td)
        report.progress()
    report.ok('>>ok\n')

    # Submit the previously created Task descriptions to the
    # PilotManager. This will trigger the selected scheduler to start
    # assigning Tasks to the Pilots.
    tasks = tmgr.submit_tasks(tds)

    # Wait for all tasks to reach a final state (DONE, CANCELED or FAILED).
    report.header('gather results')
    tmgr.wait_tasks()

    report.info('\n')
    for task in tasks:
        if task.state == rp.FAILED:
            report.plain('  * %s: %s, exit: %3s, err: %s'
                    % (task.uid, task.state[:4],
                       task.exit_code, task.stderr.strip()[-35:]))
            report.error('>>err\n')
        else:
            report.plain('  * %s: %s, exit: %3s, out: %s'
                    % (task.uid, task.state[:4],
                        task.exit_code, task.stdout.strip()[:35]))
            report.ok('>>ok\n')


    report.header()


# --------------------------------------------------------------------------
#
def stop_pilot(pilot):

    if not pilot:
        return

    pilot.pilot_manager.cancel_pilots(pilot.uid)
    print("\nCancel Pilot: %s" % pilot.uid)


# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    main()
    sys.exit(0)


# ------------------------------------------------------------------------------

