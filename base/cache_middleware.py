# -*- coding: utf-8 -*-
"""
    alter by: zzzy
    alter on 2021-06-04
"""

import json
from django.core.cache import cache
from django.middleware.cache import CacheMiddleware
from base.utils import md5


class CustomCacheMiddleware(CacheMiddleware):

    def get_cache_key(self, request):
        parameter = request.GET if request.method == 'GET' else request.body if not request.POST else request.POST
        if isinstance(parameter, bytes):
            parameter = json.loads(parameter) if parameter else dict()

        parameter = dict(parameter)
        parameter['path'], parameter['method'] = request.path, request.method
        sign_string = json.dumps(parameter, sort_keys=True, separators=(',', ':'))

        cache_key = md5(sign_string)
        return cache_key

    def process_request(self, request, *args, **kwargs):
        if request.method in ('GET', 'POST'):
            cache_key = self.get_cache_key(request)

            if cache.has_key(cache_key):
                response = self.cache.get(cache_key)
                return response

    def process_response(self, request, response):
        cache_key = self.get_cache_key(request)

        if hasattr(response, 'data'):
            if response.data.get('code') == 200:
                self.cache.set(cache_key, response, timeout=60)

        return response
