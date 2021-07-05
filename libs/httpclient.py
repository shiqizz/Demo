# coding:utf-8
"""
Created on 2014-11-24
"""

import json
import threading
import requests

try:  # py3
    import urllib.request as urllib2
    from urllib.parse import quote
except ImportError:  # py2
    import urllib2
    from urllib import quote

try:
    from cStringIO import StringIO
except ImportError:
    from io import StringIO
    from io import BytesIO


class Singleton(object):
    """单例模式"""

    _instance_lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, "_instance"):
            with cls._instance_lock:
                if not hasattr(cls, "_instance"):
                    impl = cls.configure() if hasattr(cls, "configure") else cls
                    instance = super(Singleton, cls).__new__(impl)
                    if not isinstance(instance, cls):
                        instance.__init__(*args, **kwargs)
                    cls._instance = instance
        return cls._instance


class UrllibClient(object):
    """使用urlib2发送请求"""

    def get(self, url, second=30):
        return self.postXml(None, url, second)

    def postXml(self, xml, url, second=30):
        """不使用证书"""
        try:
            data = urllib2.urlopen(url, xml, timeout=second).read()
        except TypeError:
            data = urllib2.urlopen(url, xml.encode(), timeout=second).read()
        return data

    def postXmlSSL(self, xml, url, second=30):
        """使用证书"""
        raise TypeError("please use CurlClient")


class RequestsClient(object):
    """使用Requests发送请求"""
    config = None

    def __init__(self, config=None):
        self.config = config

    def filter(self, data):
        if isinstance(data, (list, tuple, dict)):
            data = json.dumps(data, ensure_ascii=False)
        return data.encode()

    def get(self, url, headers=None, second=30):
        if not headers:
            headers = {'Content-Type': 'application/json; charset=utf-8'}
        elif 'Content-Type' not in headers:
                headers['Content-Type'] = 'application/json; charset=utf-8'
        r = requests.get(url, headers=headers, timeout=second)
        try:
            return r.json()
        except:
            return r.content.decode()

    def postXml(self, xml, url, headers=None, second=30):
        """不使用证书"""
        xml = self.filter(xml)
        if not headers:
            headers = {'Content-Type': 'application/json; charset=utf-8'}
        elif 'Content-Type' not in headers:
                headers['Content-Type'] = 'application/json; charset=utf-8'
        r = requests.post(url, headers=headers, data=xml, timeout=second)
        try:
            return r.json()
        except:
            return r.content.decode()

    def postXmlSSL(self, xml, url, headers=None, second=30, cert=True, post=True):
        """使用证书"""
        xml = self.filter(xml)
        if not headers:
            headers = {'Content-Type': 'application/json; charset=utf-8'}
        elif 'Content-Type' not in headers:
                headers['Content-Type'] = 'application/json; charset=utf-8'
        r = requests.post(url, data=xml, headers=headers, timeout=second,
                              cert=(self.config.SSLCERT_PATH, self.config.SSLKEY_PATH))
        try:
            return r.json()
        except:
            return r.content.decode()


class HttpClient(Singleton):
    @classmethod
    def configure(cls):
        return RequestsClient

