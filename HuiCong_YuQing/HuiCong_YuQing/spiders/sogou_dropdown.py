# -*- coding: utf-8 -*-
import scrapy
import re
import traceback
import pymongo
import cx_Oracle
from scrapy import log
from HuiCong_YuQing.items import HuicongYuqingItem
from scrapy.conf import settings
import urllib
import pdb
import copy
import string
import json
import random

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import os
os.system('export LANG=zh_CN.GB18030')
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.ZHS16GBK'

class BaiduDropdownSpider(scrapy.Spider):
    name = "sogou_dropdown"

    def __init__(self, *args, **kwargs):
        super(BaiduDropdownSpider, self).__init__(*args, **kwargs)
        try:
            connstr = "%s/%s@%s/%s" % (
                    settings['ORACLE_SERVER_USERNAME'],
                    settings['ORACLE_SERVER_PASSWORD'],
                    settings['ORACLE_SERVER_ADDR'],
                    settings['ORACLE_SERVER_DBNAME'])
            self.oracleFetchConn = cx_Oracle.connect(connstr)
        except Exception, err:
            self.logger.error("Manager fetch Connection Oracle Error!: %s" % (err,))

        cr = self.oracleFetchConn.cursor()
        exesql = settings['GET_HUICONG_YUQING_KEYWORD_SQL'].format('sogou_dropdown'.upper(), settings['SELECT_STEP'])
        ret = cr.execute(exesql)
        self.start_urls = ret.fetchall()
        cr.close()

    def start_requests(self):
        #pdb.set_trace()
        for index, keyword, page_max in  self.start_urls:
            keyword = keyword.decode('GBK')
            item = HuicongYuqingItem()
            item['index'] = index
            item['keyword'] = keyword
            item['source'] = 'sogou_dropdown'
            item['has_no'] = ''
            cur_url = 'https://www.sogou.com/suggnew/ajajjson?type=web&ori=yes'
            keyword = keyword.encode('gb18030')
            url = cur_url + '&key=%s' % (keyword)
            meta = {'item': item, 'dont_retry': True}
            self.log('insert new keyword=%s index=%d' %
                     (keyword, index), level=log.INFO)
            yield scrapy.Request(url=url,
                        callback=self.parse,
                        meta=meta,
                        headers = {
                            'Host' : 'www.sogou.com',
                            'Accept' : '*/*',
                            'User-Agent' : random.choice(settings['USER_AGENT']),
                        },
                        dont_filter=True)

    def parse(self, response):
        #pdb.set_trace()
        item = response.meta['item']

        if response.status != 200:
            self.log('fetch failed! keyword=%s index=%d status=%d jump_url=%s' %
                     (item['keyword'], item['index'], item['status'], response.headers.get('Location', '')), level=log.WARNING)
            if response.status == 302 and response.headers.get('Location', '').find('vcode') > 0:
                ##block
                return
            else:
                yield item

        contents = response.body_as_unicode()
        dropdown = re.findall('(?<=\[")(.*?)(?=",\[)', contents, re.S)
        item['dropdown'] = [iss for iss in dropdown]
        # print 'keyword: %s, dropdown: %s' % (item['keyword'], item['dropdown'])
        # dropdowns = contents['result']
        # for content in dropdowns:
            # item['dropdown'] = content['word']

            # if not item['dropdown']:
                # continue
            
            # # print 'keyword: %s, dropdown: %s' % (item['keyword'], item['dropdown'])
        yield item

    def closed(self, reason):
        self.oracleFetchConn.close()
