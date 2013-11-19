from saga.job import Description

def as_list(obj):
    """Converts single items to a single-entry list and leaves lists as lists.
    """
    if obj is None:
        return None
    elif type(obj) is not list:
        obj_list = []
        obj_list.append(obj)
    else:
        obj_list = obj
    return obj_list
