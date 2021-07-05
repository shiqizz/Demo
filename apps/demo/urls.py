# -*- coding: utf-8 -*-

from django.urls import path
from rest_framework.urlpatterns import format_suffix_patterns
from .views import DemoList, DemoDetail

urlpatterns = [
    path('', DemoList.as_view()),
    path('<path:pk>', DemoDetail.as_view()),
    ]

urlpatterns = format_suffix_patterns(urlpatterns)
