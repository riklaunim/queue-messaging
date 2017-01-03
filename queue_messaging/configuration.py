from collections import namedtuple


Configuration = namedtuple(
    'Configuration',
    ['TOPIC', 'SUBSCRIPTION', 'DEAD_LETTER_TOPIC', 'PUBSUB_EMULATOR_HOST',
     'MESSAGE_TYPES'],
)


class Factory:
    def __init__(self, config_dict):
        self.config_dict = config_dict

    def create(self) -> Configuration:
        return Configuration(
            self.config_dict.get('TOPIC'),
            self.config_dict.get('SUBSCRIPTION'),
            self.config_dict.get('DEAD_LETTER_TOPIC'),
            self.config_dict.get('PUBSUB_EMULATOR_HOST'),
            self.config_dict['MESSAGE_TYPES'],
        )