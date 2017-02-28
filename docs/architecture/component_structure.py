
# ==============================================================================
#
class Component(mp.Process):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        # initialize the Process base class for later fork.
        mp.Process.__init__(self, name=self.uid)


    # --------------------------------------------------------------------------
    #
    def initialize_common(self):

        # can be overloaded
        self._log.debug('initialize_common (NOOP)')


    # --------------------------------------------------------------------------
    #
    def _initialize_common(self):

        self.initialize_common()


    # --------------------------------------------------------------------------
    #
    def _initialize_parent(self):

        self._is_parent  = True
        self._initialize_common()
        self.initialize_parent()


    # --------------------------------------------------------------------------
    #
    def initialize_parent(self):

        # can be overloaded
        self._log.debug('initialize_parent (NOOP)')


    # --------------------------------------------------------------------------
    #
    def _initialize_child(self):

        self._is_parent = False
        self._initialize_common()
        self.initialize_child()


    # --------------------------------------------------------------------------
    #
    def initialize_child(self):

        # can be overloaded
        self._log.debug('initialize_child (NOOP)')


    # --------------------------------------------------------------------------
    #
    def _finalize_common(self):

        self.finalize_common()

        # signal all threads to terminate
        # collect the threads


    # --------------------------------------------------------------------------
    #
    def finalize_common(self):

        # can be overloaded
        self._log.debug('finalize_common (NOOP)')


    # --------------------------------------------------------------------------
    #
    def _finalize_parent(self):

        self.finalize_parent()
        self._finalize_common()


    # --------------------------------------------------------------------------
    #
    def finalize_parent(self):

        # can be overloaded
        self._log.debug('finalize_parent (NOOP)')


    # --------------------------------------------------------------------------
    #
    def _finalize_child(self):

        self.finalize_child()
        self._finalize_common()


    # --------------------------------------------------------------------------
    #
    def finalize_child(self):

        # can be overloaded
        self._log.debug('finalize_child (NOOP)')


    # --------------------------------------------------------------------------
    #
    def start(self, spawn=True):

        # this method will fork the child process

        if spawn:
            mp.Process.start(self)  # fork happens here

        # this is now the parent process context
        self._initialize_parent()


    # --------------------------------------------------------------------------
    #
    def stop(self):

        if self._is_parent:

            # signal the child -- if one exists
            if self.has_child:
                self.terminate()
                self.join()

            self._finalize_parent()

        else:

            # we don't call the finalizers here, as this could be a thread.
            # the child exits here.  This is caught in the run loop, which will
            # then call the finalizers in the finally clause, before calling
            # stop() itself, then to exit the main thread.
            sys.exit()


    # --------------------------------------------------------------------------
    #
    def join(self, timeout=None):

        # we only really join when the component child process has been started
        # this is basically a wait(2) on the child pid.
        if self.pid:
            mp.Process.join(self, timeout)


    # --------------------------------------------------------------------------
    #
    def run(self):

        try:

            # this is now the child process context
            self._initialize_child()

            while True:

                for name in self._inputs:

                    things = input.get_nowait(1000) # timeout in microseconds

                    # sort into things/state buckets, by machig
                    for state,things in buckets.iteritems():
                        self._workers[state](things)
        except Exception as e:
            # error in communication channel or worker
            self._log.exception('loop exception')
        
        except SystemExit:
            # normal shutdown from self.()stop()
            self._log.info("loop exit")
        
        except KeyboardInterrupt:
            # a thread caused this by calling ru.cancel_main_thread()
            self._log.info("loop cancel")
        
        except:
            # any other signal or interrupt.
            self._log.exception('loop interrupted')
        
        finally:

            self._finalize_child()
            self.stop()


# ------------------------------------------------------------------------------

