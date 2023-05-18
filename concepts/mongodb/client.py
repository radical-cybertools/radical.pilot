
import time

import radical.utils       as ru
import radical.pilot       as rp
import radical.pilot.utils as rpu

class ClientInput(rpu.Component):

    def __init__(self, session, cfg):
        
        self.session = session
        self.cfg     = cfg
        # {'name' : 'client_input'}
        rpu.Component.__init__(self, 'client_input', self.cfg)


    def initialize_child(self):

        self.declare_input ('CLIENT_INPUT_PENDING', 'CLIENT_INPUT_QUEUE', self.work)
     #  self.declare_output('AGENT_INPUT_PENDING',  'AGENT_INPUT_QUEUE')
        self.declare_chown ('AGENT_INPUT_PENDING',  'AGENT_INPUT_QUEUE', owner='AGENT')

        self.declare_publisher('state',   'CLIENT_STATE_PUBSUB')
        self.declare_publisher('command', 'CLIENT_COMMAND_PUBSUB')

        self.publish('command', {'cmd' : 'alive',
                                 'arg' : self.cname})

    def work(self, msg):

        self.advance(msg, 'CLIENT_INPUT',        publish=True, push=False)
        self.advance(msg, 'AGENT_INPUT_PENDING', publish=True, push=True)


class AgentInput(rpu.Component):

    def __init__(self, session, cfg):
        
        self.session = session
        self.cfg     = cfg
      # cfg['name']  = 'agent_input'
        rpu.Component.__init__(self, 'agent_input', self.cfg)


    def initialize_child(self):

        self.declare_input ('AGENT_INPUT_PENDING',  'AGENT_INPUT_QUEUE', self.work)
        self.declare_output('AGENT_OUTPUT_PENDING', 'AGENT_OUTPUT_QUEUE')

        self.declare_publisher('state',   'AGENT_STATE_PUBSUB')
        self.declare_publisher('command', 'AGENT_COMMAND_PUBSUB')

        self.publish('command', {'cmd' : 'alive',
                                 'arg' : self.cname})

    def work(self, msg):

        self.advance(msg, 'AGENT_INPUT',          publish=True, push=False)
        self.advance(msg, 'AGENT_OUTPUT_PENDING', publish=True, push=True)


class AgentOutput(rpu.Component):

    def __init__(self, session, cfg):
        
        self.session = session
        self.cfg     = cfg
      # {'name' : 'agent_output'}
        rpu.Component.__init__(self, 'agent_output', self.cfg)


    def initialize_child(self):

        self.declare_input ('AGENT_OUTPUT_PENDING',  'AGENT_OUTPUT_QUEUE', self.work)
     #  self.declare_output('CLIENT_OUTPUT_PENDING', 'CLIENT_OUTPUT_QUEUE')
        self.declare_chown ('CLIENT_OUTPUT_PENDING', 'CLIENT_OUTPUT_QUEUE', owner='CLIENT')

        self.declare_publisher('state',   'AGENT_STATE_PUBSUB')
        self.declare_publisher('command', 'AGENT_COMMAND_PUBSUB')

        self.publish('command', {'cmd' : 'alive',
                                 'arg' : self.cname})

    def work(self, msg):

        self.advance(msg, 'AGENT_OUTPUT',          publish=True, push=False)
        self.advance(msg, 'CLIENT_OUTPUT_PENDING', publish=True, push=True)



class ClientOutput(rpu.Component):

    def __init__(self, session, cfg):
        
        self.session = session
        self.cfg     = cfg
      # {'name' : 'client_output'}
        rpu.Component.__init__(self, 'client_output', self.cfg)


    def initialize_child(self):

        self.declare_input('CLIENT_OUTPUT_PENDING', 'CLIENT_OUTPUT_QUEUE', self.work)

        self.declare_publisher('state',   'CLIENT_STATE_PUBSUB')
        self.declare_publisher('command', 'CLIENT_COMMAND_PUBSUB')

        self.publish('command', {'cmd' : 'alive',
                                 'arg' : self.cname})


    def work(self, msg):

        self.advance(msg, 'CLIENT_OUTPUT', publish=True, push=False)
        self.advance(msg, 'CLIENT_DONE',   publish=True, push=False)



class Client(rpu.Component):

    def __init__(self):

        self.cfg      = {'owner' : 'client'}
        self.session  = rp.Session()

        # start bridges
        self._b_input   = rpu.Queue.create(rpu.QUEUE_ZMQ, 'CLIENT_INPUT_QUEUE',    rpu.QUEUE_BRIDGE)
        self._b_output  = rpu.Queue.create(rpu.QUEUE_ZMQ, 'CLIENT_OUTPUT_QUEUE',   rpu.QUEUE_BRIDGE)
        self._b_state   = rpu.Queue.create(rpu.QUEUE_ZMQ, 'CLIENT_STATE_PUBSUB',   rpu.PUBSUB_BRIDGE)
        self._b_command = rpu.Queue.create(rpu.QUEUE_ZMQ, 'CLIENT_COMMAND_PUBSUB', rpu.PUBSUB_BRIDGE)

        self.cfg['bridge_addresses'] = {
                'CLIENT_INPUT_QUEUE'    : { 'in'  : self._b_input.bridge_in,
                                            'out' : self._b_input.bridge_out},
                'CLIENT_OUTPUT_QUEUE'   : { 'in'  : self._b_output.bridge_in,
                                            'out' : self._b_output.bridge_out},
                'CLIENT_STATE_PUBSUB'   : { 'in'  : self._b_state.bridge_in,
                                            'out' : self._b_state.bridge_out},
                'CLIENT_COMMAND_PUBSUB' : { 'in'  : self._b_command.bridge_in,
                    'out' : self._b_command.bridge_out}}

        # start components
        self._c_input  = ClientInput (session=self.session, cfg=self.cfg)
        self._c_output = ClientOutput(session=self.session, cfg=self.cfg)

        self._c_input.start()
        self._c_output.start()

        # start worker
        self._w_update = rp.worker.Update(cfg=self.cfg)

        rpu.Component.__init__(self, 'client', self.cfg)


    def close(self):

        self.publish('command', 'CLOSE')

        self._w_update.stop()

        self._c_input.stop()
        self._c_output.stop()

        self._b_input.stop()
        self._b_output.stop()
        self._b_state.stop()
        self._b_command.stop()


    def initialize(self):

        self.declare_output('CLIENT_INPUT_PENDING', 'CLIENT_INPUT_QUEUE')

        self.declare_publisher('state',   'CLIENT_STATE_PUBSUB')
        self.declare_publisher('command', 'CLIENT_COMMAND_PUBSUB')

        self.declare_subscriber('state', self.state_cb)


    def state_cb(self, topic, msg):

        print msg


    def submit(self):

        thing = {'type'  : 'thing',
                 'uid'   : ru.generate_id('thing.'),
                 'state' : 'NEW'}

        self.advance(thing, 'CLIENT_INPUT_PENDING', publish=True, push=True)


if __name__ == '__main__':

    client = Client()
    client.submit()
    time.sleep(3)
    client.close()


