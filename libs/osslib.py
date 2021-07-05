# -*- coding: utf-8 -*-
import oss2
from base.options import apollo_config


def singleton(cls):
    """
    单例装饰器
    """
    _instance = {}

    def _singleton(*args, **kwargs):
        if cls not in _instance:
            _instance[cls] = cls(*args, **kwargs)
        return _instance[cls]

    return _singleton


@singleton
class OssBucket:

    def __init__(self):
        self.endpoint = apollo_config.get_value('oss.endpoint')
        self.access_key_id = apollo_config.get_value('oss.access_key_id')
        self.access_key_secret = apollo_config.get_value('oss.access_key_secret')
        self.bucket_name = apollo_config.get_value('oss.bucket_name')
        self.__bucket = self.get_bucket()

    def __call__(self, *args, **kwargs):
        return self.__bucket

    def get_bucket(self):
        auth = oss2.Auth(self.access_key_id, self.access_key_secret)
        bucket = oss2.Bucket(auth, self.endpoint, self.bucket_name)
        return bucket

    def put_object(self, key, data):
        self.__bucket.put_object(key, data)
