
# A 'Component' represents the fundamental architectural element in RP.  We
# define it as:
#
#   Component: a software element which manages a specific state of a specific
#              stateful entity.
#
# This somewhat generic definition is best illustrated by some examples.  Before
# doing so, we need to clarify the notion of 'state' though.
#
# RP is semantically a system which manages various types of short-lived
# entities, most prominently of 'pilot' and 'units'.  Those entities follow
# a strict state model during their lifetimes.  For example, a pilot starts its
# live in a NEW state, waits for bein launched (PMGR_LAUNCHING_PENDING),
# eventually gets launched (PMGR_LAUNCHING), waits to get active on the target
# resource (PMGR_ACTIVE_PENDING), gets active eventually (PMGR_ACTIVE), and ends
# in one of 3 possible final states (DONE, FAILED or CANCELED), depending on how
# its termination was triggered.
#
# At any point in time, the pilot is *owned* by some software component which
# ensures its transition to the next state in that model.  Now, RP's software
# architecture is such that, with very few exceptions, there is exactly one
# software component responsible for each state, which results in the above
# definition.
#
# The exceptions are the following:
#   - pilot managers and unit managers are responsible for *all* initial and
#     final states
#   - all '*_PENDING' states are owned by communication bridges, and signify
#     that the respecitive entities are in transition from one component to the
#     next.
#
# With those rules in mind, we can now give examples for components:
#
#   - the `pmgr.PilotLaunching` component manages pilots in
#     `PMGR_PILOT_LAUNCHING` state, ie. it launches pilots onto target resources
#   - the `umgr.StagingInput` component manages units in the
#     `UMGR_STAGING_INPUT` state, ie. it stages input files for compute units
#   - the `AGENT_EXECUTING` component manages units in `AGENT_EXECUTING` state,
#     ie. it executes units.
#
# This makes a RP 'Component' extremely simple, at least conceptually.  It
# basically reduces to the following semantic load (here rendered as
# a function):
#
#   def agent_executing(unit):
#
#     # make sure we got a unit which is ready for execution
#     assert(unit.state == rp.AGENT_EXECUTING_PENDING)
#
#     # advance unit state
#     unit.state = rp.AGENT_EXECUTING
#
#     # do the deed
#     os.system(unit.command)
#
#     # we are done executing -- push unit to the next component
#     unit.state = rp.AGENT_STAGING_OUTPUT_PENDING
#     return unit
#
#
# In practice, a RP Component is more complex -- but that is really only owed to
# the desire for performance, scalability, traceability, and manageability of
# the component.  Towards those metrics, Components differ in the following way:
#
#   - base performance:
#
#     Some state transitions can take a long time (file transfer being an
#     obvious example).  To increase throughput of units, we *must* employ some
#     form of concurrent execution, so that delays in file staging do not stall,
#     for example, unit execution.
#
#     Components thus run in separate processes (python is bad at threading),
#     they are rendered as classes which inherit multiprocessing.Process.
#
#
#   - scalability
#
#     The resulting pipeline of state transitions will be limited in throughput
#     by the slowest component: if input staging is limited to 1 unit/sec, we 
#     can only ever execute at most 1 unit/sec.  To be able to scale beyond
#     that, we allow many component instances of the same time to be
#     concurrently active.  For example, 2 input staging components with
#     1 unit/sec each can serve 1 executing component operating at 2 units/sec.
#
#     Multiple component processes of the same type can run in parallel, and be
#     distributed.  
#
#     (Note: To load balance entities between the component instances, we use 
#            queue based communication channels with a request/subscriber scheme
#            on the reading end, which results in a first-come-first-served
#            semantics.
#
#
#   - non-linear state transitions, component management
#
#     Entities can always fail, and enter a final state early.  Some state
#     transitions can be empty, and be skipped for optimization (e.g. a unit
#     having no files to stage).  Those actions can lead to state transitions
#     out of the expected linear state model.
#
#     Additionally, components must be able to handle entity cancelation
#     requests in any state, and must themself react to management commands
#     (e.g. for termination).
#
#     The Components are rendered as classes, where the base class
#     (rp.utils.Component) performs those management activities:
#
#       - bootstrapping
#       - connecting and watching incoming communication channels
#       - reacting on management commands
#       - reacting on cancelation requests
#       - moving entities out toward downstream communication channels
#
#
#   - scalability
#
#     RP is expected to scale to large numbers of components.  As RP is now
#     rendered as a component network, where components are distributed and
#     connected over queue-like communication channels, the latency of those
#     communication channels becomes important.  
#
#     The Component base class centrally manages two means of latency hiding
#     techniques: buling and caching.
#
#     bulking: 
#
#     instead of pushing one entity at a time though an communication channel,
#     RP can push several entities in a bulk, incurring the latency cost only
#     once.  The Component base class is responsibly for managing the
#     appropriate bulk size and structure.
#
#     caching:
#
#     entities can be pre-fetched from the communication channel while the
#     component is actually still working on the previous components.
#     Similarly, pushing out entities to downstream components can in principle
#     be delayed until the component is operating on the next entitity.  The
#     component base class can centrally implement those optimizations (but at
#     this point in time does not do pre-fetching).
#
#
#  - profiling
#
#    The component base class can centrally profile the arrival and departure of
#    entities to/from Components, and can also centrally and uniformely profile
#    all state transitions.  This is specifically important as different
#    specialized components of the same type thus remain comparable.  For
#    example, the AgentExecuting component on a Cray differs from that on
#    a local machine -- but the performance can easily be compared due to
#    centralized and localized profiling.  
#
#    Note that component implementations can still add local and specialized
#    profiling information.
#
#
#  - traceability
#
#    Several elements in RP are designed to react on state changes of the RP
#    entities.  Specifically the RP API is expected to expose state changes in
#    a timely manner, but other elements, such as the unit scheduler, do also
#    need state change notifications.
#
#    As the comonent base class is already managing the entity state
#    transitions, it also pushes state notifications to the repsective
#    communication channels (which have pubsub emantics).
#
#
#  - correctness
#
#    Since all component instances formally declare their state transition
#    repsonsibilities to the component base class, the base class can enforce
#    and ensure semantically and causally valid state transitions, for all
#    entities, in all places, with no overhead.
#
#
# With all those elements in mind, we end up  with a structure like this:
#
#   class Component(multiprocessing.Process):
#
#     def __init__(self):
#
#       multiprocessing.Process.__init__(self)
#
#       self._input_states  = list()
#       self._output_states = list()
#
#       self._inputs      = dict()
#       self._outputs     = dict()
#       self._transitions = dict()
#
#
#     def register_input(self, state, input):
#       # register an input channel for entities in a specific state
#       assert(state not in self._inputs)
#       self._inputs[state] = input
#
#       
#     def register_output(self, state, output):
#       # register an input channel for entities in a specific state
#       assert(state not in self._outputs)
#       self._outputs[state] = output
#
#
#     def register_transition(self, state, transition):
#       # register a worker routine which transitions an entity from one state
#       # to the mext
#       assert(state not in self._transitions)
#       self._transitions[state] = transition
#
#
#     def run(self):
#       # this is the child process
#
#       while True:
#         
#         entities = None
#         for input in self._inputs:
#           entities += input.get_nowait()
#
#         if not entities:
#           # nothing to do, wait a bit
#           time.sleep(1)
#           continue
#
#         # got some entities - work on them
#         for entity in entities:
#
#           assert(entity.state in self._inputs)
#           assert(entity.state in self._transitions)
#           self._transitions[entity.state](entity)
#           assert(entity.state in self._outputs)
#           self._outputs[entity.state].push(entity)
#       
#
#


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
            # child stops here
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

                    if not things:
                        # nothing to do right now
                        # we could now tend to houskeeping ops etc
                        time.sleep(0.1)
                        continue

                    # make sure inputs have the same state, and its the one we
                    # want
                    states = set([thing.state for thing in things])
                    assert(len(states) == 1)
                    assert(states[0] == rp.AGENT_EXECUTING_PENDING)

                    # advance state
                    self.advance(things, rp.AGENT_EXECUTING, push=False)

                    # call the component worker routine for that state
                    self._work(things))

                    # advance state and push out to next component
                    self.advance(things, rp.AGENT_STAGING_OUTPUT_PENDING, push=True))

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

