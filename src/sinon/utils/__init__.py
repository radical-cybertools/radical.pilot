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

def create_saga_job_description(pilot_uid, pilot_desc, resource_desc):
    """Creates a SAGA job descritpion from a PilotDescription
    and from a resource configuration. 
    """

    jd = Description()
    jd.working_directory = "/N/u/oweidner/saga-pilot/"
    jd.executable = "/N/u/oweidner/saga-pilot/bootsrap-and-run-agent-%s" % pilot_uid
    jd.output = "saga-pilot-agent-%s.out" % pilot_uid
    jd.error  = "saga-pilot-agent-%s.err" % pilot_uid
    jd.arguments  = ["-r", "host", "-u", "123"]

    # First we initialize the job description with whatever 
    # we find in the resource configuration.


    # Now we add attributes from the pilot description. 
    # Resource configuration attributes get overwritten if 
    # defined in the pilot description as well.

    return jd