
import radical.utils as ru


class RUD(ru.DictMixin):

    # --------------------------------------------------------------------------
    #
    # first level definitions should be implemented by the sub-class
    #
    def __init__(self):

        self._data = dict()

    def __getitem__(self, key):
        return self._data[key]

    def __setitem__(self, key, value):
        self._data[key] = value

    def __delitem__(self, key):
        del(self._data[key])

    def keys(self):
        return self._data.keys()



