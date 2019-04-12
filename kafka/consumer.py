#coding=utf-8

from util.ConfigObj import Config
from pykafka import KafkaClient
import json
import logging
import time
import os


class consumerKafka(object):
    def __init__(self,cfg):
        self.name = cfg['kafkaconf']['topic']
        self.group = cfg['kafkaconf']['group']
        self.hosts = cfg['kafkaconf']['brokers']
        self.zookeepers = cfg['kafkaconf']['zookeepers']
        #kafka配置文件
        self.cfgs = cfg
        self.conn = None
        self.topic = None
        self.consumer = None
        # self.thread = None
    def open(self):
        if self.conn is None:
            self.conn = KafkaClient(hosts=self.hosts,zookeeper_hosts=self.zookeepers,socket_timeout_ms=10000)
            # self.conn = KafkaClient(socket_timeout_ms=10000,hosts=self.hosts)
            self.topic = self.conn.topics[self.name]
    def close(self):
        if self.consumer:
            self.consumer.stop()
    def consume(self):
        print("consumer start")
        if self.consumer is None:
            # self.consumer = self.topic.get_simple_consumer(auto_commit_enable=True)
            self.consumer = self.topic.get_simple_consumer()

        # print(json.dumps(self.consumer))
        # for message in self.consumer:
        #     if message is not None:
        #         print(message.offset)
        #         print(message.value.decode())
        msg = self.consumer.consume(True)
        print("消费 :{} {}".format(msg.value.decode(), str(msg.offset)))
        print("consumer end")
    def consumeBalanced(self):
        # print("consumer balanced start")
        if self.consumer is None:
            self.consumer = self.topic.get_balanced_consumer(consumer_group=self.group,
                                                             auto_commit_enable=True,
                                                             auto_commit_interval_ms=2000,
                                                             #auto_offset_reset=OffsetType.LATEST,
                                                             #reset_offset_on_start=True,
                                                             #auto_offset_reset=OffsetType.LATEST,
                                                             zookeeper_connection_timeout_ms=60000,
                                                             consumer_timeout_ms=-1,
                                                             rebalance_backoff_ms=10000,
                                                             rebalance_max_retries=10,
                                                             zookeeper_connect=self.zookeepers
                                                            )
            offset_list = self.consumer.held_offsets
            print("当前消费者分区offset情况{}".format(offset_list))  # 消费者拥有的分区offset的情况
        msg = self.consumer.consume(True)
        print("{}消费 : {} {}".format(os.getpid(),msg.value.decode(),str(msg.offset)))
        # print("consumer balanced end")
if __name__ == '__main__':
    cfg = Config.initAllConfig()
    cs = consumerKafka(cfg)
    cs.open()
    print(json.dumps(cs.cfgs))
    # print(json.dumps(cs.topic))
    print("consumer try")
    try:
        while True:
            try:
                time.sleep(1)
                # print("consumer start")
                # cs.consume()
                cs.consumeBalanced()
            except Exception as e:
                logging.exception(e)
                continue
    except KeyboardInterrupt:
        print("interrupt")
    finally:
        print("finally")
        pass

    # print(json.dumps(cfg))
    # print("abc")


