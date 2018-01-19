# -*- coding: utf-8 -*-
import json
import urllib

import requests


class ConsulAPI(object):
    def __init__(self):
        self.host = '127.0.0.1'
        self.port = 8500
        self.headers = {'content-type': 'application/json'}
       
        self.path = ''

        self.status = False

    def put_session(self, ttl=30, name="union-watchdog-lock"):
        """创建session"""
        url = 'http://{0}:{1}/v1/session/create'.format(self.host, self.port)
        data = {
            "LockDelay": "15s",
            "Name": name,
            "Behavior": "release",
            "TTL": "{0}s".format(ttl)
        }
        try:
            r = requests.put(url, data=json.dumps(data))
            if r.status_code == 200:
                return json.loads(r.text)
            else:
                self.status = False
                raise Exception('url: {url}, status: {code}, {text}'.format(
                    url=url, code=r.status_code, text=r.text))
        except Exception as e:
            self.status = False
            raise

    def del_session(self, uuid):
        """删除session"""
        url = 'http://{0}:{1}/v1/session/destroy/{2}'.format(
            self.host, self.port, uuid)
        try:
            r = requests.put(url)
            if r.status_code == 200:
                return json.loads(r.text)
            else:
                self.status = False
                raise Exception('url: {url}, status: {code}, {text}'.format(
                    url=url, code=r.status_code, text=r.text))
        except Exception as e:
            self.status = False
            raise

    def renew_session(self, uuid):
        """续约session"""
        url = 'http://{0}:{1}/v1/session/renew/{2}'.format(
            self.host, self.port, uuid)
        try:
            r = requests.put(url)
            if r.status_code == 200:
                return json.loads(r.text)
            elif r.status_code == 404:
                return None
            else:
                self.status = False
                raise Exception('url: {url}, status: {code}, {text}'.format(
                    url=url, code=r.status_code, text=r.text))
        except Exception as e:
            self.status = False
            raise

    def get_service(self, service):
        """获取服务"""
        url = 'http://{0}:{1}/v1/catalog/service/{2}'.format(
            self.host, self.port, service)
        try:
            r = requests.get(url)
            if r.status_code == 200:
                return json.loads(r.text)
            else:
                self.status = False
                raise Exception('url: {url}, status: {code}, {text}'.format(
                    url=url, code=r.status_code, text=r.text))
        except Exception as e:
            self.status = False
            raise

    def get_health(self, service):
        """获取服务状态"""
        url = 'http://{0}:{1}/v1/health/checks/{2}'.format(
            self.host, self.port, service)
        try:
            r = requests.get(url)
            if r.status_code == 200:
                return json.loads(r.text)
            else:
                self.status = False
                raise Exception('url: {url}, status: {code}, {text}'.format(
                    url=url, code=r.status_code, text=r.text))
        except Exception as e:
            self.status = False
            raise

    def get_lock(self, uuid, data):
        """获取锁成功返回True,失败返回False,session过期返回500错误"""
        url = 'http://{0}:{1}/v1/kv/{2}lock?acquire={3}'.format(
            self.host, self.port, self.path, uuid)
        try:
            r = requests.put(url, data=data)
            if r.status_code == 200:
                return json.loads(r.text)
            elif r.status_code == 500:
                return None
            else:
                self.status = False
                raise Exception('url: {url}, status: {code}, {text}'.format(
                    url=url, code=r.status_code, text=r.text))
        except Exception as e:
            self.status = False
            raise

    def get_id(self):
        """获取电话号码"""
        url = 'http://{0}:{1}/v1/kv/{2}id'.format(
            self.host, self.port, self.path)
        try:
            r = requests.get(url)
            if r.status_code == 200:
                return json.loads(r.text)
            else:
                self.status = False
                raise Exception('url: {url}, status: {code}, {text}'.format(
                    url=url, code=r.status_code, text=r.text))
        except Exception as e:
            self.status = False
            raise

    def put_id(self, _id, cas):
        """获取卡口地点"""
        url = 'http://{0}:{1}/v1/kv/{2}id?cas={3}'.format(
            self.host, self.port, self.path, cas)
        try:
            r = requests.put(url, data=str(_id))
            if r.status_code == 200:
                return json.loads(r.text)
            else:
                self.status = False
                raise Exception('url: {url}, status: {code}, {text}'.format(
                    url=url, code=r.status_code, text=r.text))
        except Exception as e:
            self.status = False
            raise

    def get_lost(self):
        """获取电话号码"""
        url = 'http://{0}:{1}/v1/kv/{2}lost'.format(
            self.host, self.port, self.path)
        try:
            r = requests.get(url)
            if r.status_code == 200:
                return json.loads(r.text)
            else:
                self.status = False
                raise Exception('url: {url}, status: {code}, {text}'.format(
                    url=url, code=r.status_code, text=r.text))
        except Exception as e:
            self.status = False
            raise

    def put_lost(self, data):
        """获取卡口地点"""
        url = 'http://{0}:{1}/v1/kv/{2}lost'.format(
            self.host, self.port, self.path)
        try:
            r = requests.put(url, data=data)
            if r.status_code == 200:
                return json.loads(r.text)
            else:
                self.status = False
                raise Exception('url: {url}, status: {code}, {text}'.format(
                    url=url, code=r.status_code, text=r.text))
        except Exception as e:
            self.status = False
            raise

