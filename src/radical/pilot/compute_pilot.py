
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import copy
import time
import saga

import radical.utils as ru

from . import utils     as rpu
from . import states    as rps
from . import constants as rpc
from . import types     as rpt

from .staging_directives import expand_staging_directive
from .staging_directives import TRANSFER, COPY, LINK, MOVE, STAGING_AREA


# ------------------------------------------------------------------------------
#
class ComputePilot(object):
    """
    A ComputePilot represent a resource overlay on a local or remote resource.

    .. note:: A ComputePilot cannot be created directly. The factory method
              :meth:`radical.pilot.PilotManager.submit_pilots` has to be used instead.

                **Example**::

                      pm = radical.pilot.PilotManager(session=s)

                      pd = radical.pilot.ComputePilotDescription()
                      pd.resource = "local.localhost"
                      pd.cores    = 2
                      pd.runtime  = 5 # minutes

                      pilot = pm.submit_pilots(pd)
    """
    
    # --------------------------------------------------------------------------
    # In terms of implementation, a Pilot is not much more than a dict whose
    # content are dynamically updated to reflect the state progression through
    # the PMGR components.  As a Pilot is always created via a PMGR, it is
    # considered to *belong* to that PMGR, and all activities are actually
    # implemented by that PMGR.
    #
    # Note that this implies that we could create Pilots before submitting them
    # to a PMGR, w/o any problems. (FIXME?)
    # --------------------------------------------------------------------------


    # --------------------------------------------------------------------------
    #
    def __init__(self, pmgr, descr):

        # 'static' members
        self._descr = descr.as_dict()
        self._pmgr  = pmgr

        # initialize state
        self._session    = self._pmgr.session
        self._uid        = ru.generate_id('pilot.%(counter)04d', ru.ID_CUSTOM)
        self._state      = rps.NEW
        self._state_hist = [[rps.NEW, rpu.timestamp()]]
        self._log        = pmgr._log
        self._log_msgs   = []
        self._stdout     = None
        self._stderr     = None
        self._sandbox    = None

        # sanity checks on description
        for check in ['resource', 'cores', 'runtime']:
            if not self._descr.get(check):
                raise ValueError("ComputePilotDescription needs '%s'" % check)


    # -------------------------------------------------------------------------
    #
    @staticmethod
    def create(pmgr, descr):
        """ 
        PRIVATE: Create a new compute pilot (in NEW state)
        """

        return ComputePilot(pmgr=pmgr, descr=descr)


    # --------------------------------------------------------------------------
    #
    def __repr__(self):

        return self.as_dict()


    # --------------------------------------------------------------------------
    #
    def __str__(self):

        return str(self.as_dict())


    # --------------------------------------------------------------------------
    #
    def as_dict(self):
        """
        Returns a Python dictionary representation of the object.
        """
        ret = {
            'session':         self.session.uid,
            'pmgr':            self.pmgr.uid,
            '_id':             self.uid,  # for component...
            'uid':             self.uid,
            'state':           self.state,
            'state_history':   self.state_history,
            'log':             self.log,
            'stdout':          self.stdout,
            'stderr':          self.stderr,
            'resource':        self.resource,
            'sandbox':         self.sandbox,
            'description':     copy.deepcopy(self.description)
        }
        return ret


    # --------------------------------------------------------------------------
    #
    @property
    def session(self):
        """
        Returns the pilot's session.

        **Returns:**
            * A :class:`Session`.
        """

        return self._session


    # --------------------------------------------------------------------------
    #
    @property
    def pmgr(self):
        """
        Returns the pilot's manager.

        **Returns:**
            * A :class:`PilotManager`.
        """

        return self._pmgr


    # --------------------------------------------------------------------------
    #
    @property
    def uid(self):
        """
        Returns the pilot's unique identifier.

        The uid identifies the pilot within a :class:`PilotManager`.

        **Returns:**
            * A unique identifier (string).
        """
        return self._uid


    # --------------------------------------------------------------------------
    #
    @property
    def state(self):
        """
        Returns the current state of the pilot.

        **Returns:**
            * state (string enum)
        """

        return self._state


    # --------------------------------------------------------------------------
    #
    @property
    def state_history(self):
        """
        Returns the complete state history of the pilot.

        **Returns:**
            * list of tuples [[state, timestamp]]
        """

        return copy.deepcopy(self._state_hist)


    # --------------------------------------------------------------------------
    #
    @property
    def log(self):
        """
        Returns a list of human readable [timestamp, string] tuples describing
        various events during the pilot's lifetime.  Those strings are not
        normative, only informative!

        **Returns:**
            * log (list of [timestamp, string] tuples)
        """

        return copy.deepcopy(self._log_msgs)


    # --------------------------------------------------------------------------
    #
    @property
    def stdout(self):
        """
        Returns a snapshot of the pilot's STDOUT stream.

        If this property is queried before the pilot has reached
        'DONE' or 'FAILED' state it will return None.

        .. warning: This can be inefficient.  Output may be incomplete and/or
           filtered.

        **Returns:**
            * stdout (string)
        """

        return self._stdout


    # --------------------------------------------------------------------------
    #
    @property
    def stderr(self):
        """
        Returns a snapshot of the pilot's STDERR stream.

        If this property is queried before the pilot has reached
        'DONE' or 'FAILED' state it will return None.

        .. warning: This can be inefficient.  Output may be incomplete and/or
           filtered.

        **Returns:**
            * stderr (string)
        """

        return self._stderr


    # --------------------------------------------------------------------------
    #
    @property
    def resource(self):
        """
        Returns the resource tag of this pilot.

        **Returns:**
            * A resource tag (string)
        """

        return self._descr.get('resource')


    # --------------------------------------------------------------------------
    #
    @property
    def sandbox(self):
        """
        Returns the full sandbox URL of this pilot, if that is already
        known, or 'None' otherwise.

        **Returns:**
            * A URL (radical.utils.Url).
        """

        return self._sandbox


    # --------------------------------------------------------------------------
    #
    @property
    def description(self):
        """
        Returns the description the pilot was started with, as a dictionary.

        **Returns:**
            * description (dict)
        """

        return copy.deepcopy(self._descr)


    # --------------------------------------------------------------------------
    #
    def register_callback(self, cb_func, cb_data=None):
        """
        Registers a callback function that is triggered every time the
        pilot's state changes.

        All callback functions need to have the same signature::

            def cb_func(obj, state)

        where ``object`` is a handle to the object that triggered the callback
        and ``state`` is the new state of that object.  If 'cb_data' is given,
        then the 'cb_func' signature changes to 

            def cb_func(obj, state, cb_data)

        and 'cb_data' are passed along.

        """
        self._pmgr.register_callback(self.uid, rpt.PILOT_STATE, cb_func, cb_data)


    # --------------------------------------------------------------------------
    #
    def wait(self, state=None, timeout=None):
        """
        Returns when the pilot reaches a specific state or
        when an optional timeout is reached.

        **Arguments:**

            * **state** [`list of strings`]
              The state(s) that pilot has to reach in order for the
              call to return.

              By default `wait` waits for the pilot to reach a **final**
              state, which can be one of the following:

              * :data:`radical.pilot.states.DONE`
              * :data:`radical.pilot.states.FAILED`
              * :data:`radical.pilot.states.CANCELED`

            * **timeout** [`float`]
              Optional timeout in seconds before the call returns regardless
              whether the pilot has reached the desired state or not.  The
              default value **None** never times out.  """

        if not state:
            states = rps.FINAL
        if not isinstance(state, list):
            states = [state]
        else:
            states = state


        if self.state in rps.FINAL:
            # we will never see another state progression.  Raise an error
            # (unless we waited for this)
            if self.state in states:
                return

            # FIXME: do we want a raise here, really?  This introduces a race,
            #        really, on application level
            # raise RuntimeError("can't wait on a pilot in final state")
            return self.state

        start_wait = time.time()
        while self.state not in states:

            time.sleep(0.1)

            if timeout and (timeout <= (time.time() - start_wait)):
                break

        return self.state


    # --------------------------------------------------------------------------
    #
    def cancel(self):
        """
        Cancel the pilot.
        """
        
        self._pmgr.cancel_pilot(self.uid)


    # --------------------------------------------------------------------------
    #
    def stage_in(self, directives):
        """Stages the content of the staging directive into the pilot's
        staging area"""

        # Wait until we can assume the pilot directory to be created
        # # FIXME: can't we create it ourself?
        if self.state == rps.NEW:
            self.wait(state=[rps.PMGR_LAUNCHING_PENDING, rps.PMGR_LAUNCHING,
                             rps.PMGR_ACTIVE_PENDING,    rps.PMGR_ACTIVE])
        elif self.state in rps.FINAL:
            raise Exception("Pilot already finished, no need to stage anymore!")

        # Iterate over all directives
        for directive in expand_staging_directive(directives):

            # TODO: respect flags in directive

            src_url = saga.Url(directive['source'])
            action = directive['action']

            # Convert the target url into a SAGA Url object
            tgt_url = saga.Url(directive['target'])
            # Create a pointer to the directory object that we will use
            tgt_dir_url = tgt_url

            if tgt_url.path.endswith('/'):
                # If the original target was a directory (ends with /),
                # we assume that the user wants the same filename as the source.
                tgt_filename = os.path.basename(src_url.path)
            else:
                # Otherwise, extract the filename and update the directory
                tgt_filename = os.path.basename(tgt_dir_url.path)
                tgt_dir_url.path = os.path.dirname(tgt_dir_url.path)

            # Handle special 'staging' scheme
            if tgt_dir_url.scheme == 'staging':

                # We expect a staging:///relative/path/file.txt URI,
                # as hostname would have unclear semantics currently.
                if tgt_dir_url.host:
                    raise Exception("hostname not supported with staging:// scheme")

                # Remove the leading slash to get a relative path from the staging area
                rel_path = os.path.relpath(tgt_dir_url.path, '/')

                # Now base the target directory relative of the sandbox and staging prefix
                tgt_dir_url = saga.Url(os.path.join(self.sandbox, STAGING_AREA, rel_path))

            # Define and open the staging directory for the pilot
            # We use the target dir construct here, so that we can create
            # the directory if it does not yet exist.
            target_dir = saga.filesystem.Directory(tgt_dir_url, flags=saga.filesystem.CREATE_PARENTS)

            if action == LINK:
                # TODO: Does this make sense?
                #log_message = 'Linking %s to %s' % (source, abs_target)
                #os.symlink(source, abs_target)
                self._log.error("action 'LINK' not supported on pilot level staging")
                raise ValueError("action 'LINK' not supported on pilot level staging")
            elif action == COPY:
                # TODO: Does this make sense?
                #log_message = 'Copying %s to %s' % (source, abs_target)
                #shutil.copyfile(source, abs_target)
                self._log.error("action 'COPY' not supported on pilot level staging")
                raise ValueError("action 'COPY' not supported on pilot level staging")
            elif action == MOVE:
                # TODO: Does this make sense?
                #log_message = 'Moving %s to %s' % (source, abs_target)
                #shutil.move(source, abs_target)
                self._log.error("action 'MOVE' not supported on pilot level staging")
                raise ValueError("action 'MOVE' not supported on pilot level staging")
            elif action == TRANSFER:
                log_message = 'Transferring %s to %s' % (src_url, os.path.join(str(tgt_dir_url), tgt_filename))
                self._log.info(log_message)
                # Transfer the source file to the target staging area
                target_dir.copy(src_url, tgt_filename)
            else:
                raise Exception('Action %s not supported' % action)

# ------------------------------------------------------------------------------

