

import saga.utils as su

# ------------------------------------------------------------------------------
#
class PluginManager (su.PluginManager) :
    """ 
    The TROY plugin management and loading utility.

    The plugin manater allows to manage plugins of a specific type, such as
    'WorkloadScheduler', 'OverlayScheduler', 'Planner', etc.  For that type, the
    manager can search for installed plugins, list and describe plugins found,
    load plugins, and instantiate the plugin for further use in the TROY code
    base.

    Example::

       pm = troy.PluginManager ('planner')

       for plugin_name in pm.list () :
           print plugin_name
           print pm.describe (plugin_name)

        default_plugin = pm.load ('default')

        default_plugin.init (app_description)
        (overlay_description, workload_description) = default_plugin.run ()

    The plugins are expected to follow a specific naming and coding schema to be
    recognized by the plugin manager.  The naming schema is:

        troy.plugins.[type].plugin_[type]_[name].py

    i.e. for the code example above: `troy.plugins.planner.plugin_planner_default.py`

    The plugin code consists of two parts:  a plugin description, and a plugin
    class.  The description is a module level dictionary named
    `PLUGIN_DESCRIPTION`, the plugin class must be named `PLUGIN_CLASS`, and
    must have a class constructor `__init__(*args, **kwargs)` to create plugin
    instances for further use within TROY.  

    At this point, we leave the definition of the exact plugin signature open,
    but expect that to be strictly defined per plugin type in the future.
    """


    #---------------------------------------------------------------------------
    # 
    def __init__ (self, ptype) :
        """
        ptype: type of plugins to manage
        """
        su.PluginManager.__init__ (self, 'sinon', ptype)


# ------------------------------------------------------------------------------
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

