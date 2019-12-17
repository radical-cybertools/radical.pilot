

import radical.utils as ru

import spec_attribs  as a


class Cfg(ru.Config):

    def __init__(self, from_dict=None):

        ru.Config.__init__(self, from_dict)

        if from_dict:
            for k, v in from_dict.items:
                self[k] = v


