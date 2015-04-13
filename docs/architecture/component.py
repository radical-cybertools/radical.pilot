

# ------------------------------------------------------------------------------
#
class component (threading.Thread):

    # --------------------------------------------------------------------------
    #
    def __init__ (input_queue, output_even_queue, output_odd_queue, update_queue):
    
        self._input_queue       = input_queue
        self._output_even_queue = output_even_queue
        self._output_odd_queue  = output_odd_queue
        self._update_queue      = update_queue  
        self._terminate         = threading.Event()


    # --------------------------------------------------------------------------
    #
    def stop():

        self._terminate.set()


    # --------------------------------------------------------------------------
    #
    def run():

        try:

            while not self._terminate:

                command, data = self._input_queue.get ()

                if command == COMMAND_CANCEL :
                    self.stop()

                else command == COMMAND_OPERATION :

                    for cu in rpu.blowup (cu=data, 'component_name'):
                        profile (uid=cu.uid, 'get', 'input queue to component')
                        self.operation (cu)

                else:
                    print 'unknown command %s - ignored' % command


        except:
            print ('component failed')
            raise


    # --------------------------------------------------------------------------
    #
    def operation (cu):

        try :

            # set state to the one this component is operating on, and
            # send state update
            cu.state = COMPONENT_OPERATION
            for _cu in blowup (cu, 'update_queue'):
                profile (uid=cu.uid, 'put', 'component to update_queue (COMPONENT_OPERATION)')
                self._update_queue.put (_cu)
           
            # hard work...
            cu.counter += 1

            # next operation/component may depend on some property (state detail)
            if not cu.counter % 2:

                # set the state...
                cu.state = PENDING_EVEN
                
                # ... push state update ...
                for _cu in blowup (cu, 'pending_output_even'):
                    profile (uid=cu.uid, 'put', 'component to update_queue (PENDING_EVEN)')
                    self._update_queue.put (_cu)

                # ... and push toward next component.
                for _cu in blowup (cu, 'output_queue_even'):
                    profile (uid=cu.uid, 'put', 'component to output_queue_even')
                    self._output_queue_even.put (_cu)

            else:

                # set the state...
                cu.state = PENDING_ODD

                # ... push state update ...
                for _cu in blowup (cu, 'pending_output_odd'):
                    profile (uid=cu.uid, 'put', 'component to update_queue (PENDING_ODD)')
                    self._update_queue.put (_cu)

                # ... and push toward next component.
                for _cu in blowup (cu, 'output_queue_odd'):
                    profile (uid=cu.uid, 'put', 'component to output_queue_odd')
                    self._output_queue_odd.put (_cu)


        except Exception as e:

            # on failuer, just push a state update -- don't advance the unit
            cu.state = FAILED
            for _cu in blowup (cu, 'update_queue'):
                profile (uid=cu.uid, 'put', 'component to oudate_queue (FAILED)')
                self._update_queue.put (_cu)

#
# ------------------------------------------------------------------------------


