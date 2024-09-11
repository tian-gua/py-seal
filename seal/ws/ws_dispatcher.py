class WSDispatcher:

    def __init__(self):
        self.registry = {}

    def dispatch(self, path):
        return self.registry.get(path)

    def register(self, path, func):
        self.registry[path] = func


dispatcher = WSDispatcher()
