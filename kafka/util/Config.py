import ConfigParser

class KafkaConfig(object):
    def __init__(self):
        self.KafkaConfigs = KafkaConfig.initKafkaConfig()

    @staticmethod
    def initKafkaConfig():
        config = ConfigParser.ConfigParser()
        cp = config.read('./config/config.ini')

        host = config.get('kafkaconf','host')
        port = config.get('kafkaconf','port')
        auth = config.get('kafkaconf','auth')

        return [host,port,auth]
