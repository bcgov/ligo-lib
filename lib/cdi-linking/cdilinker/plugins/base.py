'''
Forked from : `A Simple Plugin Framework <http://martyalchin.com/2008/jan/10/simple-plugin-framework/>`__ by
Marty Alchin.
'''

PLUGINS_GROUP = 'bcgov.linking.plugins'
PLUGINS = None


class PluginMount(type):
    # Holds a dictionary of available plugins instances to support Singleton plugins.
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(PluginMount, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

    def __init__(cls, name, bases, attrs):
        if not hasattr(cls, 'plugins'):
            # This branch only executes when processing the mount point itself.
            # So, since this is a new plugin type, not an implementation, this
            # class shouldn't be registered as a plugin. Instead, it sets up a
            # list where plugins can be registered later.
            cls.plugins = []
        else:
            # This must be a plugin implementation, which should be registered.
            # Simply appending it to the list is all that's needed to keep
            # track of it later.
            print 'Adding new plugin class: {0} type: {1}.'.format(cls.name , cls.type)
            cls.plugins.append(cls)


class AlgorithmProvider:
    """
    Singleton Mount point for plugins that provide comparison/matching algorithms for linking/De-Duplication jobs.
    Plugins implementing this reference should provide the following properties:

    name :  The name of the algorithm(key)
    title:  A short description of the algorithm
    type :  'DTR' for Deterministic / 'PRB' for Probabilistic algorithms 'TSF' for Transformations
    tags : A list of strings that can be used to search and filter algorithms.
    args :  A list of required named parameters, this does not include main parameter(s).
            For example, for the forlllowing Levenshtein strings similarity algorithm,
            the only required parameter is max_edits:
            levenshtein(x, y, max_edits=2)

    """

    __metaclass__ = PluginMount

    def apply(self, args):
        raise NotImplementedError

def load_plugins():
    '''
    Loads all available plugins for bcgov-linking module.
    :return: List of available plugins
    '''
    from pkg_resources import iter_entry_points
    plugins = []
    for entry_point in iter_entry_points(group=PLUGINS_GROUP, name=None):
        plugins.append(entry_point.load())

    return plugins


if PLUGINS is None:
    PLUGINS = load_plugins()
