# -*- coding: utf-8 -*-

"""
    author : luoyu
    date : 2019-02-15
"""

from elasticsearch import Elasticsearch
from elasticsearch import helpers
from base.options import config


def singleton(cls, *args, **kw):
    instances = {}

    def _singleton(hosts=None, **kwargs):
        if hosts not in instances:
            instances[hosts] = cls(hosts=hosts, **kwargs)
        return instances[hosts]

    return _singleton


# 单例
@singleton
class ESTool(object):
    def __init__(self, hosts=None, http_auth=None):
        if hosts is not None:
            self.__hosts = hosts
        else:
            self.__hosts = config.db.elasticsearch.host
        self.__es = self.get_es(http_auth)

    def __call__(self):
        return self.__es

    def search(self, **es_search_options):
        es_result = self.__es.search(**es_search_options)
        return es_result

    def scroll_search(self, **es_search_options):
        es_result = self._get_scroll_search(**es_search_options)
        return es_result

    def get_result_list(self, es_result):
        final_result = []
        for item in es_result:
            final_result.append(item)
        return final_result

    def _get_scroll_search(self, body, index, scroll='24h', doc_type='index', timeout=180):
        es_result = helpers.scan(
            client=self.__es,
            query=body,
            scroll=scroll,
            index=index,
            doc_type=doc_type,
            request_timeout=timeout,
            timeout="24h",
            size=int(1e4)
        )
        return es_result

    # 获得一个es实例
    def get_es(self, http_auth=None):
        if http_auth is not None:
            return Elasticsearch(self.__hosts, http_auth=http_auth)
        return Elasticsearch(self.__hosts)


if __name__ == '__main__':
    def set_search_optional():
        # 检索选项
        es_search_options = dict(index="dm_map_edb_sql",
                                 body={
                                     "query": {
                                         "match_all": {}
                                     }
                                 })
        return es_search_options


    final_results = ESTool().search(**set_search_optional())
    print(final_results)
