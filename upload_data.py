# -*- coding: utf-8 -*-
import time
import json
import base64
import socket

import arrow

import helper
from helper_kakou_cs_lm_v2 import Kakou
from helper_consul import ConsulAPI
from helper_kafka import KafkaData
from my_yaml import MyYAML
from my_logger import *


debug_logging('/home/logs/error.log')
logger = logging.getLogger('root')


class UploadData(object):
    def __init__(self):
        # 配置文件
        self.my_ini = MyYAML('/home/my.yaml').get_ini()
        self.flag_ini = MyYAML('/home/flag.yaml')

        # request方法类
        self.kk = None
        self.ka = KafkaData(**dict(self.my_ini['kafka']))
        self.con = ConsulAPI()
        self.con.path = dict(self.my_ini['consul'])['path']
	
        # ID上传标记
        self.kk_name = dict(self.my_ini['kakou'])['name']
        self.step = dict(self.my_ini['kakou'])['step']
        self.kkdd = dict(self.my_ini['kakou'])['kkdd']

        self.uuid = None                    # session id
        self.session_time = time.time()     # session生成时间戳
        self.ttl = dict(self.my_ini['consul'])['ttl']               # 生存周期
        self.lock_name = dict(self.my_ini['consul'])['lock_name']   # 锁名

        self.local_ip = socket.gethostbyname(socket.gethostname())  # 本地IP
        self.maxid = 0

        self.id_flag = self.flag_ini.get_ini()['id']

    def get_id2(self):
        """获取上传id"""
        r = self.con.get_id()[0]
        return base64.b64decode(r['Value']).decode(), r['ModifyIndex']

    def get_id(self):
        """获取上传id"""
        return self.id_flag

    def set_id2(self, _id, modify_index):
        """设置ID"""
        if self.con.put_id(_id, modify_index):
            print(_id)

    def set_id(self, _id, msg=''):
        """设置ID"""
        self.id_flag = _id
        self.flag_ini.set_ini({'id': _id})
        print(self.id_flag)
        logger.info('{0} {1}'.format(_id, msg))

    def get_lost(self):
        """获取未上传数据id列表"""
        r = self.con.get_lost()[0]
        return json.loads(base64.b64decode(r['Value']).decode()), r['ModifyIndex']

    def post_lost_data(self):
        """未上传数据重传"""
        lost_list, modify_index = self.get_lost()
        if len(lost_list) == 0:
            return 0
        t = arrow.now('PRC').format('YYYY-MM-DD HH:mm:ss')
        for i in lost_list:
            value = {'timestamp': t, 'message': i['message']}
            self.ka.produce_info(key='{0}_{0}'.format(self.kk_name, i['message']['id']), value=json.dumps(value))
            print('lost={0}'.format(i['message']['id']))
        self.ka.flush()
        lost_list = []
        if len(self.ka.lost_msg) > 0:
            for i in self.ka.lost_msg:
                lost_list.append(json.loads(i.value()))
            self.ka.lost_msg = []
        self.con.put_lost(json.dumps(lost_list))
        return len(lost_list)

    def post_info(self):
        """上传数据"""
        #t, modify_index = self.get_id()
        t = self.get_id()
        st = arrow.get(t).replace(seconds=1).format('YYYY-MM-DD HH:mm:ss')
        et = arrow.get(t).replace(hours=2).format('YYYY-MM-DD HH:mm:ss')
        #et = arrow.now('PRC').replace(minutes=30).format('YYYY-MM-DD HH:mm:ss')
        info = self.kk.get_kakou(st, et, 1, self.step+1)
        #print(info['total_count'])
        # 如果查询数据为0
        if info['total_count'] == 0:
            mt = self.kk.get_maxtime()['maxtime']
            if arrow.get(et).timestamp < arrow.get(mt).timestamp:
                self.set_id(et)
            return 0

        t = arrow.now('PRC').format('YYYY-MM-DD HH:mm:ss')
        for i in info['items']:
            i['cllx'] = 'X99'
            i['csys'] = 'Z'
            i['hpzl'] = helper.hphm2hpzl(i['hphm'], i['hpys_id'], i['hpzl'])
            value = {'timestamp': t, 'message': i}
            self.ka.produce_info(key='{0}_{0}'.format(self.kk_name, i['id']), value=json.dumps(value))
        self.ka.flush()
        #if len(self.ka.lost_msg) > 0:
        #    lost_list = []
        #    for i in self.ka.lost_msg:
        #        lost_list.append(json.loads(i.value()))
        #    self.ka.lost_msg = []
        #    self.con.put_lost(json.dumps(lost_list))
        # 设置最新ID
        #self.set_id(info['items'][0]['jgsj'], modify_index)
        self.set_id(info['items'][0]['jgsj'])
        return info['total_count']

    def get_lock(self):
        """获取锁"""
        p = False
        if self.uuid is None:
            self.uuid = self.con.put_session(self.ttl, self.lock_name)['ID']
            self.session_time = time.time()
            p = True
        # 大于一定时间间隔则更新session
        t = time.time() - self.session_time
        if t > (self.ttl - 5):
            self.con.renew_session(self.uuid)
            self.session_time = time.time()
            p = True
        l = self.con.get_lock(self.uuid, self.local_ip)
        if p:
            print(self.uuid, l)
        # session过期
        if l == None:
            self.uuid = None
            return False
        return l


    def main_loop(self):
        while 1:
            #time.sleep(1)
            #if not self.get_lock():
            #    time.sleep(2)
            #    continue
            if self.kk is not None and self.kk.status:
                try:
                    #m = self.post_lost_data()
                    #if m > 0:
                    #    time.sleep(0.5)
                    #    continue
                    n = self.post_info()
                    if n < self.step:
                        time.sleep(0.5)
                except Exception as e:
                    logger.exception(e)
                    time.sleep(15)
            else:
                try:
                    if self.kk is None or not self.kk.status:
                        s = self.con.get_useful_service('kong')
                        if s is None:
                            time.sleep(5)
                            continue
                        self.kk = Kakou(**{'host':s['host'], 'port':s['port'], 'path':dict(self.my_ini['kakou'])['path'], 'apikey':dict(self.my_ini['kakou'])['apikey']})
                        self.kk.status = True
                except Exception as e:
                    logger.error(e)
                    time.sleep(1)
        
