#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
@author: luoyu
@contact: 532275035@qq.com
@software: PyCharm
@file: loglib.py
@time: 2020/3/30 15:40
"""
import os
import logging
import logging.config
import sys
from base.options import config
import pathlib


log_path = config.path.log

root_path = pathlib.Path(config.root_path)
if not os.path.exists(log_path):
    os.makedirs(log_path)
try:
    logging.config.fileConfig(root_path / "configs"/"logging.configs")
except Exception as e:
    print(e)
    print("get logger error")


class Log(object):
    def __init__(self):
        self.lineno = sys._getframe(1).f_lineno
        self.co_filename = sys._getframe(1).f_code.co_filename
        self.name = '.'.join(pathlib.Path(self.co_filename).parts).replace('.'.join(root_path.parts), '', 1)[1:]

    def _get_logger(self, f_logger=None):
        try:
            if f_logger is None:
                f_logger = 'root'
            logger = logging.getLogger('{f_logger}.{name}'.format(f_logger=f_logger, name=self.name))
            handlers = set(logger.handlers+logger.parent.handlers)
            log_level = config.console_log_level.upper()
            for handler in handlers:
                if type(handler) == logging.StreamHandler:
                    handler.setLevel(log_level)
            return logger
        except Exception as e:
            print(e)
            print("get logger error")

    def debug(self, msg, *args, **kwargs):
        logger = self._get_logger('debug')
        lineno = sys._getframe(1).f_lineno
        msg = "%s[line %s] %s" % (self.name, lineno, msg)
        logger.debug(msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        logger = self._get_logger('root')
        lineno = sys._getframe(1).f_lineno
        msg = "%s[line %s] %s" % (self.name, lineno, msg)
        logger.info(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        logger = self._get_logger('error')
        lineno = sys._getframe(1).f_lineno
        msg = "%s[line %s] %s" % (self.name, lineno, msg)
        logger.error(msg, *args, **kwargs)



