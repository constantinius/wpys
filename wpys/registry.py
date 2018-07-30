from uuid import uuid4


class ProcessRegistry:
    def __init__(self):
        self.registry = {}

    def register(self, process):
        identifier = process.identifier
        if identifier in self.registry:
            raise Exception(f'Process {identifier} is already registered.')
        self.registry[identifier] = process

    @property
    def processes(self):
        return self.registry.values()

    def get_process(self, identifier):
        return self.registry[identifier]
