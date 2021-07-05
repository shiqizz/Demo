# -*- coding: utf-8 -*-


class MasterSlaveDBRouter(object):
    """数据库主从读写分离路由"""

    def db_for_read(self, model, **hints):
        """读数据库"""
        # if model._meta.app_label == 'inx_general':
        #     return 'public'
        return 'default'

    def db_for_write(self, model, **hints):
        """写数据库"""
        # if model._meta.app_label == 'inx_general':
        #     return 'public'
        return 'default'

    def allow_relation(self, obj1, obj2, **hints):
        """Allow any relation between apps that use the same database."""
        return None

    def allow_syncdb(self, db, model):
        """Make sure that apps only appear in the related database."""
        return None

    def allow_migrate(self, db, app_label, model=None, **hints):
        """
        Make sure the auth app only appears in the 'auth_db'
        database.
        """
        return None

