# -*- coding: utf-8 -*-
#
# @author: Daemon Wang
# Created on 2016-03-02
#
import base64
import os
import random
import time
import datetime
import json

import traceback
import string
import hashlib
import urllib
import html
from concurrent import futures
import zipfile
import re

from base.options import config
import uuid
import math
from math import *
import shutil
import platform
import calendar
import decimal


def get_root_path():
    from base.configlib import root_path
    return root_path


def find_modules(modules_dir):
    try:
        return [f[:-3] for f in os.listdir(modules_dir)
                if not f.startswith('_') and f.endswith('.py')]
    except OSError:
        return []


def get_random_num(length, mode='string'):
    if mode == 'string':
        return ''.join([(string.ascii_letters + string.digits)[x] for x in random.sample(range(0, 62), length)])
    elif mode == 'number':
        return ''.join([string.digits[x] for x in random.sample(range(0, 10), length)])


def md5(s):
    m = hashlib.md5()
    m.update(s.encode())
    return m.hexdigest()


def sha1(s):
    m = hashlib.sha1()
    m.update(s.encode())
    return m.hexdigest()


def generate_password(password, loginname):
    m = hashlib.md5()
    m.update((str(password) + str(loginname)).encode())
    res = m.hexdigest()
    return res


def get_uuid():
    return uuid.uuid1()


# 驼峰转蛇形
def tf_to_sx(camel_name):
    table_name = ""
    for i, s in enumerate(camel_name):
        if s.isupper() and i != 0:
            table_name += "_" + s.lower()
        else:
            table_name += s.lower()
    return table_name


# 蛇形转驼峰
def sx_to_tf(camel_name):
    table_name = ""
    toupper = False
    for i, s in enumerate(camel_name):
        if i == 0 or (toupper is True):
            table_name += s.upper()
            toupper = False
        elif s == "_":
            toupper = True
        else:
            table_name += s.lower()
            toupper = False
    return table_name


# 验证参数
def validate(required=None, **kwargs):
    msg = list(filter(lambda x: x if x not in kwargs else None, required))
    if msg:
        raise ValueError("%s 是必填字段" % msg)


def get_current_time(format_type='%Y-%m-%d %H:%M:%S'):
    _format = '%Y-%m-%d %H:%M:%S'
    if format_type == 'date':
        _format = '%Y-%m-%d'
    elif format_type == 'datetime2':
        _format = '%Y-%m-%d %H:%M:%S.%f'
    elif format_type == 'directory_date':
        _format = '%Y/%m/%d'
    else:
        _format = format_type
    return datetime.datetime.now().strftime(_format)


# 获取当前utc时间
def get_utc_now():
    return datetime.datetime.utcnow()


# 获取当前时间
def get_now():
    return datetime.datetime.now()


# 生成0000-00-00时间
def get_default_time():
    return datetime.datetime(1, 1, 1, 0, 0, 0)


# 格式化错误信息
def format_error():
    return traceback.format_exc()


def html_encode(str):
    return html.escape(str)


# 计算分页信息
def count_page(length, page, page_size=15):
    if page is None:
        page = 1
    if page_size is None:
        page_size = 15

    page = int(page)
    page_size = int(page_size)
    length = int(length)
    if length == 0:
        return {
            "page_size": page_size,
            "max_page": 1,
            "page": 1,
            "enable": True,
            "has_more": False,
            "length": 0
        }
    max_page = int(math.ceil(float(length) / page_size))
    if page >= max_page:
        has_more = False
    else:
        has_more = True
    pager = {
        "page_size": page_size,
        "max_page": max_page,
        "page": page,
        "enable": True,
        "has_more": has_more,
        "length": length
    }
    return pager


# 计算分页信息
def cut_list(l, page_size=15):
    l2 = []
    length = len(l)
    max_page = int(math.ceil(float(length) / page_size))
    for i in range(max_page):
        if i < max_page - 1:
            _l = l[i * page_size:i * page_size + page_size]
        else:
            _l = l[i * page_size:length]
        l2.append(_l)
    return l2


# 将两个list合成字典
def list_to_dict(list1, list2):
    return dict(zip(list1[::], list2))


# 获取请求Host
def get_request_host(request):
    return request.headers.get_list('HOST')[0]


def zip_folder(foldername, zip_name):
    filelist = []
    if os.path.isfile(foldername):
        filelist.append(foldername)
    else:
        for root, dirs, files in os.walk(foldername):
            for name in files:
                filelist.append(os.path.join(root, name))

    zf = zipfile.ZipFile(zip_name, "w", zipfile.zlib.DEFLATED)
    for tar in filelist:
        arcname = tar[len(foldername):]
        zf.write(tar, arcname)
    zf.close()


def get_concurrent_pool(n=4):
    return futures.ThreadPoolExecutor(n)


def init_response_data():
    result = {"code": 200, "msg": "success", "data": None}
    return result


def reset_response_data(code, e=None):
    result = init_response_data()
    if code == 1:
        result["msg"] = "success"
    elif code == -1:
        result["msg"] = "token invalidate"
    else:
        try:
            error = json.loads(e)
            result["msg"] = error["sub_msg"] or "error"
            result["code"] = error["code"]
        except:
            result["msg"] = e or "error"
            result["code"] = 10000
    if config.debug:
        result["error_msg"] = format_error()
        try:
            print(format_error())
        except:
            pass
    else:
        result["error_msg"] = ""
        msg = format_error()
    return result


def return_error(msg, code=10000):
    error = {"msg": msg, "code": code}
    raise ValueError(json.dumps(error))


# 创建目录
def mkdir(path):
    path = path.strip()
    is_exist = os.path.exists(path)
    if not is_exist:
        os.makedirs(path)
    else:
        pass
    return path


def save_file(path, file_name, data):
    if data is None:
        return
    mkdir(path)
    if not path.endswith("/"):
        path = path + "/"
    file = open(path + file_name, "wb")
    file.write(data)
    file.flush()
    file.close()


def check_email(email):
    return re.match("^\w+([-+.]\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*$", email) is not None


def check_mobile(mobile):
    return re.match("^1\d{10}$", mobile) is not None


def is_chinese(string):
    pattern = re.compile(u'[\u4e00-\u9fa5]+')
    return pattern.search(string)


def str_to_img(uri, string, url=None):
    if url is None:
        url = '/static/ftp/resource/' + uri
    # string = string.replace("data:image/jpeg;base64,","")
    # missing_padding = 4 - len(string) % 4
    # if missing_padding:
    #     string += '=' * missing_padding
    img_data = base64.b64decode(string)
    path_url = get_root_path() + url
    if not os.path.exists(os.path.dirname(path_url)):
        os.makedirs(os.path.dirname(path_url))
    f = open(path_url, "wb")
    f.write(img_data)
    f.close()
    return url


def convert(data):
    # 字节转str
    if isinstance(data, bytes):
        return data.decode()
    if isinstance(data, dict):
        return dict(map(convert, data.items()))
    if isinstance(data, (tuple, list)):
        return map(convert, data)
    return data


def get_status_code(err=None):
    if str(err) in ("Permission denied", "用户权限不足"):
        return 403
    else:
        return 401


# 富文本流处理
def format_multieditor(html, host):
    html = urllib.parse.unquote(html)
    pattern = re.compile(
        r"<img\b[^<>]*?\bsrc[\s\t\r\n]*=[\s\t\r\n]*[""']?[\s\t\r\n]*(?P<imgurl>[^\s\t\r\n""'<>]*)[^<>]*?/?[\s\t\r\n]*>")
    re_script = re.compile('<\s*script[^>]*>[^<]*<\s*/\s*script\s*>', re.I)  # Script
    re_style = re.compile('<\s*style[^>]*>[^<]*<\s*/\s*style\s*>', re.I)  # style
    re_blank = re.compile('\n+')
    html = re_script.sub('', html)  # 去掉SCRIPT
    html = re_style.sub('', html)  # 去掉style
    html = re_blank.sub('', html)  # 去掉空行
    match = pattern.findall(html)
    if len(match) == 0:
        return html
    new_match = []
    for m in match:
        if "data:image" in m:
            base64_data = m.split(",")[1]
            url = host + str_to_img('multieditor/%s.png' % get_uuid(), base64_data)
            new_match.append('"%s"' % url)
        else:
            new_match.append(m)
    for i in list(range(len(match))):
        new_html = html.replace(match[i], new_match[i])
    return new_html


def calcDistance(Lat_A, Lng_A, Lat_B, Lng_B):
    """
    input Lat_A 纬度A
    input Lng_A 经度A
    input Lat_B 纬度B
    input Lng_B 经度B
    output distance 距离(km)
    """
    ra = 6378.140  # 赤道半径 (km)
    rb = 6356.755  # 极半径 (km)
    flatten = (ra - rb) / ra  # 地球扁率
    try:
        rad_lat_A = radians(Lat_A)
        rad_lng_A = radians(Lng_A)
        rad_lat_B = radians(Lat_B)
        rad_lng_B = radians(Lng_B)
        pA = atan(rb / ra * tan(rad_lat_A))
        pB = atan(rb / ra * tan(rad_lat_B))
        xx = acos(sin(pA) * sin(pB) + cos(pA) * cos(pB) * cos(rad_lng_A - rad_lng_B))
        if xx == 0:
            distance = 0
        else:
            c1 = (sin(xx) - xx) * (sin(pA) + sin(pB)) ** 2 / cos(xx / 2) ** 2
            c2 = (sin(xx) + xx) * (sin(pA) - sin(pB)) ** 2 / sin(xx / 2) ** 2
            dr = flatten / 8 * (c1 - c2)
            distance = ra * (xx + dr)
    except:
        distance = 1000000000000000000
    return int(distance * 1000)


def to_datetime(date):
    '''
    将对象转成datetime.datetime类型
    :param date:datetime 或者str类型
    :return:
    '''
    if isinstance(date, (datetime.date, datetime.datetime)):
        return datetime.datetime.strptime(date.strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
    elif isinstance(date, str):
        try:
            d = datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
        except Exception as e:
            try:
                d = datetime.datetime.strptime(date, "%Y-%m-%d")
            except:
                raise ValueError("日期格式有误")
        return d


def get_datetime_range(date):
    """
    获取一个日期的范围
    :param date: <datetime.datetime> 或者str类型
    :return:
    """
    date = to_datetime(date)
    start_time = date.replace(hour=0, minute=0, second=0)
    end_time = start_time + datetime.timedelta(days=1)
    return start_time, end_time


def get_months(date, months):
    if isinstance(date, str):
        date = to_datetime(date)
    month = date.month - 1 + months
    year = date.year + month // 12
    month = month % 12 + 1
    day = min(date.day, calendar.monthrange(year, month)[1])
    date = date.replace(year=year, month=month, day=day)
    return date


def clean_cache():
    _root = get_root_path()
    cacheDir = _root + "/static/temp"
    if os.path.exists(cacheDir):
        try:
            shutil.rmtree(cacheDir)
        except:
            pass


def currentPaltform():
    if 'Windows' in platform.system():
        return 'Windows'
    elif 'Linux' in platform.system():
        return 'Linux'
    else:
        return 'Unknown'


def connect_success():
    if currentPaltform() == 'Windows':
        exit_code = os.system('ping www.baidu.com')
    else:
        exit_code = os.system('ping -c 1 www.baidu.com')
    if exit_code:
        return False
    else:
        return True


def generate_tree(t, l):
    if not l:
        return t
    if isinstance(l, tuple):
        l = list(l)
    name = l.pop(0)
    if name:
        has_name = False
        for _t in t:
            if _t["name"] == name:
                has_name = True
                children = _t["children"]
                break
        if has_name is False:
            children = []
            _t = dict(name=name, children=children)
            t.append(_t)
        generate_tree(children, l)
    return t


def object_hook(obj):
    res = str(obj)
    return res



def inverted_replace(str, old, new, n=1):
    """
    倒叙替换
    :param str: 需要替换的字符串
    :param old: 替换的元素
    :param new: 新的元素
    :param n: 替换次数
    :return: 返回结果
    """
    str = str[::-1]
    str = str.replace(old[::-1], new[::-1], n)
    str = str[::-1]
    return str


def to_str(bytes_or_str):
    if isinstance(bytes_or_str, bytes):
        value = bytes_or_str.decode('utf-8')
    else:
        value = bytes_or_str
    return value    # isinstance of str

def to_bytes(bytes_or_str):
    if isinstance(bytes_or_str, str):
        value = bytes_or_str.encode('utf-8')
    else:
        value = bytes_or_str
    return value    # isinstance of bytes
