from configobj import ConfigObj
import os
import logging

class Config(object):

    @staticmethod
    def initAllConfig():
        config = ConfigObj(os.path.dirname(os.path.abspath(__file__)) + "/../config/config.ini", encoding='UTF8')
        return config
        
logging.basicConfig(level=logging.ERROR,#控制台打印的日志级别
                    filename= os.path.dirname(os.path.abspath(__file__)) + '/../log/application.log',
                    filemode='a',
                    format=
                    '%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'
                    #日志格式
                    )