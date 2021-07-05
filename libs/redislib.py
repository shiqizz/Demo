# -*- coding: utf-8 -*-

"""
    author : luoyu
    date : 2019-02-15
"""

from redis import ConnectionPool, Redis
from base.options import config
import json

# from django.conf import settings

REDIS_MAX_CONNECTION = 50
REDIS_HOST = config.db.redis.host
REDIS_PORT = config.db.redis.port
REDIS_DB = config.db.redis.db
REDIS_PASSWORD = config.db.redis.password


def singleton(cls):
    instances = {}

    def _singleton(connection_pool=None):
        if connection_pool is not None:
            key = json.dumps(connection_pool.connection_kwargs, sort_keys=True)
            if key not in instances:
                instances[key] = cls(connection_pool=connection_pool)
            return instances[key]
        else:
            if cls not in instances:
                instances[cls] = cls()
            return instances[cls]
    return _singleton


# 单例
@singleton
class RedisTool(object):
    def __init__(self, connection_pool=None):
        if connection_pool is not None:
            self.__connection_pool = connection_pool
        else:
            if config.db.redis_auth:
                self.__connection_pool = ConnectionPool(max_connections=REDIS_MAX_CONNECTION,
                                                        host=REDIS_HOST,
                                                        port=REDIS_PORT,
                                                        db=REDIS_DB,
                                                        password=REDIS_PASSWORD)
            else:
                self.__connection_pool = ConnectionPool(max_connections=REDIS_MAX_CONNECTION,
                                                        host=REDIS_HOST,
                                                        port=REDIS_PORT,
                                                        db=REDIS_DB)
        self.__redis = self.get_redis()

    def __call__(self):
        return self.__redis

    def set(self, key, value):
        self.__redis.set(key, value)

    def get(self, key):
        value = self.__redis.get(key)
        if value:
            return value.decode()
        else:
            return value

    # 保存一条记录，默认添加到最前面
    def save_key_value(self, key, value, append=False):
        if append:
            self.__redis.rpush(key, value)
        else:
            self.__redis.lpush(key, value)

    # 保存多条记录，默认添加到最前面
    def save_key_values(self, key, value_list, append=False):
        if not isinstance(value_list, list) and not isinstance(value_list, tuple):
            return
        if len(value_list) <= 0:
            return
        if append:
            self.__redis.rpush(key, *value_list)
        else:
            self.__redis.lpush(key, *value_list)

    # 获取一条记录
    def get_key_value(self, key, index):
        value = self.__redis.lindex(key, index)
        return value.decode

    # 获取多条记录
    def get_key_values(self, key, start=0, end=-1):
        values = self.__redis.lrange(key, start, end)
        return values

    # 获取键对应记录集的长度
    def get_key_values_length(self, key):
        length = self.__redis.llen(key)
        return length

    # 删除一条记录
    def delete_key_value(self, key, value):
        self.__redis.lrem(key, value)

    # 删除多条记录
    def delete_key_values(self, key, value_list):
        for value in value_list:
            self.__redis.lrem(key, value)

    # 删除一个key
    def delet_key(self, key):
        self.__redis.delete(key)

    # key是否存在
    def has_key(self, key):
        return self.__redis.exists(key)

    # 创建一个key,并添加值(如果key存在，则先删除再创建)
    def create_key_values(self, key, value_list, append=False):
        if self.has_key(key):
            self.delet_key(key)
        self.save_key_values(key, value_list, append)

    # 获得一个redis实例
    def get_redis(self):
        return Redis(connection_pool=self.__connection_pool)

    # 创建一个key-value
    def set_redis_key_value(self, key, value):
        self.__redis.set(key, value)

    # 创建一个key-value
    def setKeyValueExpir(self, key, value, time):
        self.__redis.setex(key, time, value)

    # 创建一个Hash
    def setDict(self, name, key, value):
        self.__redis.hset(name, key, value)

    # 获取一个hash-value
    def getDictKeyValue(self, name, key):
        return self.__redis.hget(name, key).decode()

    # 是否存在一个hash-key
    def hasDict(self, name, key):
        return self.__redis.hexists(name, key)

    # 删除一个hash-key
    def delDictKey(self, name, key):
        self.__redis.hdel(name, key)


if __name__ == '__main__':
    redis =RedisTool()
    redis.get("aaa")
