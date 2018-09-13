from typing import ValuesView
from importlib import import_module


class NoSuchProcess(KeyError):
    pass


class ProcessRegistry:
    """ Registry class for processes
    """
    def __init__(self):
        self.registry = {}

    def register(self, process):
        identifier = process.identifier
        if identifier in self.registry:
            raise Exception(f'Process {identifier} is already registered.')
        self.registry[identifier] = process

    @property
    def processes(self) -> ValuesView:
        """ Get an iterator of all registered processes.
        """
        return self.registry.values()

    def get_process(self, identifier):
        """ Get the registered process
        """
        try:
            return self.registry[identifier]
        except KeyError:
            raise NoSuchProcess(identifier)


REGISTRY = None

def load_process_registry(config):
    global REGISTRY
    if REGISTRY is None:
        REGISTRY = ProcessRegistry()
        for process_path in config.process_config.locations:
            module_path, object_name = process_path.split(':', 1)
            module = import_module(module_path)
            REGISTRY.register(getattr(module, object_name).__process_wrapper__)

    return REGISTRY
