# Create your views here.
from rest_framework.views import APIView
from base import utils
from bootstrap import exceptions
from rest_framework.response import Response
from rest_framework.pagination import PageNumberPagination, _positive_int, InvalidPage
from collections import OrderedDict
from django.http import QueryDict
from django.http import HttpResponse
import types
from rest_framework.views import exception_handler as origin_exception_handler
from django.core.cache import cache
from .permissions import permissions
import time
from libs.elasticsearchlib import ESTool
from base.options import apollo_config
from django.db import connection
from libs.httpclient import RequestsClient
from rest_framework.exceptions import NotFound


def get(self, request, *args, **kwargs):
    try:
        self._get(request, *args, **kwargs)
    except exceptions.ApiRawResponseException as e:
        return Response(e)
    except Exception as e:
        self.set_result(str(e))
    finally:
        if isinstance(self.result, HttpResponse):
            return self.result
        else:
            return Response(self.result)


def put(self, request, *args, **kwargs):
    try:
        self._put(request, *args, **kwargs)
    except exceptions.ApiRawResponseException as e:
        return Response(e)
    except Exception as e:
        self.set_result(str(e))
    finally:
        if isinstance(self.result, HttpResponse):
            return self.result
        else:
            return Response(self.result)


def post(self, request, *args, **kwargs):
    try:
        self._post(request, *args, **kwargs)
    except exceptions.ApiRawResponseException as e:
        return Response(e)
    except Exception as e:
        self.set_result(str(e))
    finally:
        if isinstance(self.result, HttpResponse):
            return self.result
        else:
            return Response(self.result)


def delete(self, request, *args, **kwargs):
    try:
        self._delete(request, *args, **kwargs)
    except exceptions.ApiRawResponseException as e:
        return Response(e)
    except Exception as e:
        self.set_result(str(e))
    finally:
        if isinstance(self.result, HttpResponse):
            return self.result
        else:
            return Response(self.result)


def exception_handler(exc, context):
    """
    重新定义错误返回报文
    """
    origin_res = origin_exception_handler(exc, context)
    if origin_res:
        if isinstance(exc.detail, (list, dict)):
            pass
        else:
            origin_res.data = {
                'code': exc.status_code,
                'msg_code': exc.get_codes(),
                'msg': exc.detail}
            return origin_res
    return None


class BaseAPIView(APIView):
    owner_field = 'user'

    def __init__(self):
        self.result = utils.init_response_data()
        if hasattr(self, '_get'):
            self.get = types.MethodType(get, self)
        if hasattr(self, '_post'):
            self.post = types.MethodType(post, self)
        if hasattr(self, '_put'):
            self.put = types.MethodType(put, self)
        if hasattr(self, '_delete'):
            self.delete = types.MethodType(delete, self)
        super(BaseAPIView, self).__init__()

    def set_result(self, e=None):
        self.result = utils.reset_response_data(0, str(e))

    def update_request_data(self, data):
        _data = self.request.data
        if isinstance(_data, QueryDict):
            _mutable = _data._mutable
            _data._mutable = True
            _data.update(data)
            _data._mutable = _mutable
        else:
            _data.update(data)
        return self.request

    def update_request_query_params(self, data):
        _data = self.request.query_params
        if isinstance(_data, QueryDict):
            _mutable = _data._mutable
            _data._mutable = True
            _data.update(data)
            _data._mutable = _mutable
        else:
            _data.update(data)
        return self.request


class PagePagination(PageNumberPagination):
    page_size_query_param = "page_size"

    def get_paginated_response(self, data):
        pager = {
            "page_size": self.page.paginator.per_page,
            "max_page": self.page.paginator.num_pages,
            "page": self.page.number,
            "enable": True,
            "has_more": self.page.has_next(),
            "length": self.page.paginator.count
        }
        return Response(OrderedDict([
            ('data', data),
            ('pager', pager)
        ]))


class ZFZPagePagination(PageNumberPagination):
    page_size_query_param = "size"

    def get_paginated_response(self, data):
        count = self.page.paginator.count
        return Response(OrderedDict([
            ('list', data),
            ('count', count)
        ]))


class THPagePagination(PageNumberPagination):
    page_size_query_param = 'size'

    def paginate_queryset(self, queryset, request, view=None):
        page_size = self.get_page_size(request)
        if not page_size:
            return None

        paginator = self.django_paginator_class(queryset, page_size)
        page_number = request.data.get(self.page_query_param, 1)
        if page_number in self.last_page_strings:
            page_number = paginator.num_pages

        try:
            self.page = paginator.page(page_number)
        except InvalidPage as exc:
            msg = self.invalid_page_message.format(
                page_number=page_number, message=str(exc)
            )
            raise NotFound(msg)

        if paginator.num_pages > 1 and self.template is not None:
            self.display_page_controls = True

        self.request = request
        return list(self.page)

    def get_page_size(self, request):
        if self.page_size_query_param:
            try:
                size = request.query_params.get(self.page_size_query_param)
                if not size:
                    size = request.data.get(self.page_size_query_param)
                return _positive_int(
                    size,
                    strict=True,
                    cutoff=self.max_page_size
                )
            except (KeyError, ValueError):
                pass

        return self.page_size

    def get_paginated_response(self, data):
        count = self.page.paginator.count
        return Response(OrderedDict([
            ('list', data),
            ('count', count)
        ]))


class HealthzView(BaseAPIView):
    permission_classes = (permissions.AllowAny,)

    def _get(self, request, *args, **kwargs):
        passkey = 'local_passkey'
        if request.query_params.get('passkey') != passkey:
            self.result = self.handle_exception(exceptions.AuthenticationFailed('Unauthorized'))
            return
        status = 200
        # redis
        redis_status = 'OK'
        start_time = time.time()
        try:
            cache.set('healthz_verify', '1')
            cache.get('healthz_verify')
            cache.delete('healthz_verify')
        except:
            redis_status = 'FAIL'
            status = 500
        end_time1 = time.time()
        # es
        es_status = 'OK'
        try:
            es = ESTool(hosts=apollo_config.get('sz_drango_sfz_graph_es.host'),
                        http_auth=(apollo_config.get('sz_drango_sfz_graph_es.user'),
                                   apollo_config.get('sz_drango_sfz_graph_es.password')))
            es_search_options = dict(index='companyn01',
                                     _source=["_name"],
                                     size=1
                                     )
            body = {
                "query": {
                    "bool": {}
                }
            }
            es_search_options['body'] = body
            es.search(**es_search_options)
        except:
            es_status = 'FAIL'
            status = 500
        end_time2 = time.time()
        # pg
        pgsql_status = 'OK'
        try:
            with connection.cursor() as cursor:
                cursor.execute('select 1', [])
                cursor.fetchall()
        except:
            pgsql_status = 'FAIL'
            status = 500
        end_time3 = time.time()
        # 图查询服务
        body = {
            "subs": [
                {
                    "tablename": 'companyn01',
                    "query": {
                        "size": 1,
                        "from": 0,
                        "_source": ["_name"],
                        "query": {
                            "bool": {
                                'must': [
                                    {
                                        "match_all": {}
                                    }
                                ]
                            }
                        }
                    }
                }
            ]
        }
        host = apollo_config['sfz-graph-api'] if apollo_config.get(
            'sfz-graph-api') else 'https://graph-api.aihuoshi.net'
        url = '{}/graph_query/company_relationship'.format(host)
        graph_status = 'OK'
        try:
            res = RequestsClient().postXml(xml=body, url=url)
            _ = res['data']
        except:
            graph_status = 'FAIL'
            status = 500
        end_time4 = time.time()
        # sfz-data
        host = apollo_config.get('sfz.data.api') if apollo_config.get(
            'sfz.data.api') else 'https://sfz-data-api.aihuoshi.net'
        url = '{}/v1/business_information/search_enterprises?name={}'.format(host, '火石创造')
        xml = {}
        sfz_data_status = 'OK'
        try:
            res = RequestsClient().postXml(xml=xml, url=url)
            _ = res['data']
        except:
            sfz_data_status = 'FAIL'
            status = 500
        end_time5 = time.time()
        result = {"redis": redis_status,
                  "redis-time": int((end_time1 - start_time) * 1000),
                  "es": es_status,
                  "es-time": int((end_time2 - end_time1) * 1000),
                  "pgsql": pgsql_status,
                  "pgsql-time": int((end_time3 - end_time2) * 1000),
                  "graph": graph_status,
                  "graph-time": int((end_time4 - end_time3) * 1000),
                  "sfz_data": sfz_data_status,
                  "sfz_data-time": int((end_time5 - end_time4) * 1000)}
        if status == 500:
            self.result = Response(status=500, data=result)
        else:
            self.result = result
