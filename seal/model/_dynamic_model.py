class DynamicModels:
    def __init__(self):
        self._models = {}

    def register(self, name, model):
        self._models[name] = model

    def get(self, name):
        return self._models[name]

    def has(self, name):
        return name in self._models


dynamic_models = DynamicModels()
