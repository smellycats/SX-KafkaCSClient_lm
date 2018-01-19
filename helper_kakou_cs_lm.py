# -*- coding: utf-8 -*-
import json

import requests
from requests.auth import HTTPBasicAuth


class Kakou(object):
    def __init__(self, **kwargs):
        self.host = kwargs['host']
        self.port = kwargs['port']

        self.headers = {'content-type': 'application/json'}

        self.status = False


    def get_kakou(self, st, et, page=1, per_page=1000):
        """根据ID范围获取卡口信息"""
        url = 'http://%s:%s/cltx?q={"page":%s,"per_page":%s,"st":"%s","et":"%s"}' % (
            self.host, self.port, page, per_page, st, et)
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
        url = 'http://{0}:{1}/cltx/{3}'.format(self.host, self.port, _id)
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

