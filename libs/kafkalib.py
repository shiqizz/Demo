# -*- coding: utf-8 -*-

import sys
import time
import json

from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.errors import KafkaError

KAFAKA_ADDRESS = "test-server.aihuoshi.net:9092"
KAFAKA_TOPIC = "tzcmc-backflow-company"


class Kafka_producer():
    '''
    生产模块：根据不同的key，区分消息
    '''

    def __init__(self, **configs):
        self.producer = KafkaProducer(**configs)

    def sendjsondata(self, topic, params=None, key=None, headers=None, partition=None, timestamp_ms=None):
        try:
            if isinstance(params, dict):
                parmas_message = json.dumps(params)
            else:
                parmas_message = params
            producer = self.producer
            producer.send(topic, key=key, value=parmas_message.encode('utf-8'),
                          headers=None, partition=None, timestamp_ms=None)
            producer.flush()
        except KafkaError as e:
            print(e)

    def close(self):
        producer = self.producer
        producer.close()


class Kafka_consumer():
    '''
    消费模块: 通过不同groupid消费topic里面的消息
    '''

    def __init__(self, *topics, **configs):
        self.consumer = KafkaConsumer(*topics, **configs)

    def consume_data(self):
        try:
            for message in self.consumer:
                yield message
        except KeyboardInterrupt as e:
            print(e)


def main(xtype, group, key):
    '''
    测试consumer和producer
    '''
    if xtype == "p":
        # 生产模块
        producer = Kafka_producer(bootstrap_servers=KAFAKA_ADDRESS)
        print("===========> producer:", producer)
        producer.sendjsondata(KAFAKA_TOPIC, params='test', key=key)
        # for _id in range(100):
        #     params = '{"msg" : "%s"}' % str(_id)
        #     producer.sendjsondata(KAFAKA_TOPIC, params=params, key=key)
        #     time.sleep(1)

    if xtype == 'c':
        # 消费模块
        consumer = Kafka_consumer(KAFAKA_TOPIC, bootstrap_servers=KAFAKA_ADDRESS, group_id=group)
        print("===========> consumer:", consumer)
        message = consumer.consume_data()
        for msg in message:
            print('msg---------------->', msg)
            print('key---------------->', msg.key)
            print('offset---------------->', msg.offset)


if __name__ == '__main__':
    xtype = "c"
    group = None
    key = None
    main(xtype, group, key)
