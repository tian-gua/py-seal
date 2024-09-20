import yaml


class Configuration:

    def __init__(self):
        self.config_dict = {}

    def load(self, path: str):
        with open(path, 'r') as f:
            self.config_dict = yaml.load(f, Loader=yaml.FullLoader)

    def get_conf(self, *keys):
        conf = self.config_dict
        for key in keys:
            conf = conf.get(key)
            if not conf:
                raise ValueError(f'config not found: {keys}')
        return conf

    def get_conf_default(self, *keys, default=None):
        conf = self.config_dict
        for key in keys:
            conf = conf.get(key)
            if conf is None:
                return default
        return conf
