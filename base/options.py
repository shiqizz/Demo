#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
@author: luoyu
@contact: 532275035@qq.com
@software: PyCharm
@file: options.py
@time: 2020/3/30 15:40
"""
import os
from base import configlib
from libs.httpclient import RequestsClient


def get_base_config():
    root_path = configlib.root_path
    os.chdir(root_path + '/configs')
    cfg = configlib.Config('base.icfg')
    cfg.addNamespace(configlib)
    os.chdir(root_path)
    return cfg

def load_config():
    try:
        return RequestsClient().get(url=config_url)
    except Exception as e:
        print('load configs from configs center error!{}'.format(e))
        return None


config = get_base_config()
PROFILE = os.environ.get('PROFILE')
print('using env: %s ' % PROFILE)
config_server_url=getattr(config, 'apollo_config_server_url_%s' % PROFILE) if PROFILE else config.apollo_config_server_url_dev
config_url = "{}/configfiles/json/{}/default/application".format(config_server_url, config.apollo_app_id)
apollo_config = load_config()

# from pyapollo import ApolloClient
# apollo_config = ApolloClient(app_id=configs.apollo_app_id,
#                              cluster=configs.apollo_cluster,
#                              config_server_url=getattr(configs, 'apollo_config_server_url_%s' % PROFILE)
#                              if PROFILE else configs.apollo_config_server_url_dev)
# apollo_config.start()


