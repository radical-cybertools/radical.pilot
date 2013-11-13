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

def create_saga_job_description(pilot_desc, resource_desc):
    """Creates a SAGA job descritpion from a PilotDescription
    and from a resource configuration. 
    """
    jd = Description()
    jd.executable = "/bin/sleep"
    jd.arguments  = ["60"]

    # First we initialize the job description with whatever 
    # we find in the resource configuration.


    # Now we add attributes from the pilot description. 
    # Resource configuration attributes get overwritten if 
    # defined in the pilot description as well.

    return jd