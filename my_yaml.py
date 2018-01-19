#-*- encoding: utf-8 -*-
import ruamel.yaml


class MyYAML(object):
    def __init__(self, path='my.yaml'):
        self.path = path

    def __del__(self):
        pass

    def get_ini(self):
        with open(self.path, 'r') as f:
            return ruamel.yaml.load(
                stream=f, Loader=ruamel.yaml.RoundTripLoader)

    def set_ini(self, data):
        with open(self.path, 'w') as f:
            ruamel.yaml.dump(
                data, stream=f, Dumper=ruamel.yaml.RoundTripDumper,
                default_flow_style=False, allow_unicode=True)
