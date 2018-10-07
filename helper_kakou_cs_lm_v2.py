# -*- coding: utf-8 -*-
import json

import requests
from requests.auth import HTTPBasicAuth


class Kakou(object):
    def __init__(self, **kwargs):
        self.base_path = 'http://{0}:{1}{2}'.format(
            kwargs['host'], kwargs['port'], kwargs['path'])
        self.headers = {
            'content-type': 'application/json',
            'apikey': kwargs['apikey']
        }

        self.status = False


    def get_kakou(self, st, et, page=1, per_page=1000):
        """根据ID范围获取卡口信息"""
        url = '%s/cltx?q={"page":%s,"per_page":%s,"st":"%s","et":"%s"}' % (
            self.base_path, page, per_page, st, et)
        #print(url)
        try:
            r = requests.get(url, headers=self.headers)
            if r.status_code == 200:
                return json.loads(r.text)
            else:
                self.status = False
                raise Exception('url: {url}, status: {code}, {text}'.format(
                    url=url, code=r.status_code, text=r.text))
        except Exception as e:
            self.status = False
            raise

    def get_kakou_by_id(self, _id):
        """根据ID范围获取卡口信息"""
        url = '{0}/cltx/{1}'.format(self.base_path, _id)
        try:
            r = requests.get(url, headers=self.headers)
            if r.status_code == 200:
                return json.loads(r.text)
            else:
                self.status = False
                raise Exception('url: {url}, status: {code}, {text}'.format(
                    url=url, code=r.status_code, text=r.text))
        except Exception as e:
            self.status = False
            raise

    def get_maxtime(self):
        """根据最大过车时间"""
        url = '{0}/maxtime'.format(self.base_path)
        try:
            r = requests.get(url, headers=self.headers)
            if r.status_code == 200:
                return json.loads(r.text)
            else:
                self.status = False
                raise Exception('url: {url}, status: {code}, {text}'.format(
                    url=url, code=r.status_code, text=r.text))
        except Exception as e:
            self.status = False
            raise
