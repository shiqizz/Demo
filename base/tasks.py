# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals
from bootstrap import celery_app


@celery_app.task
def run_request_task(request_info):
    '''
    推送到任务队列
    :param request_info: Request的字典 例{"method":"GET","url":"","params":"","data":"","headers":{"Content-Type":"application/json"}}
    :return:
    '''
    r = request_info
    return r
#
# from datetime import timedelta
# celery_app.conf.beat_schedule.update(
#     {
#         'add-every-30-seconds': {
#             'task': 'base.tasks.run_request_task',
#             'schedule': timedelta(seconds=5),
#             'args': (16,)
#         },
#     })
