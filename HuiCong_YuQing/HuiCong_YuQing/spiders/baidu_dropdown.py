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
import random
import json

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import os
os.system('export LANG=zh_CN.GB18030')
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.ZHS16GBK'

class BaiduRelatedSpider(scrapy.Spider):
    name = "baidu_dropdown"

    def __init__(self, *args, **kwargs):
        super(BaiduRelatedSpider, self).__init__(*args, **kwargs)
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
        exesql = settings['GET_HUICONG_YUQING_KEYWORD_SQL'].format('baidu_dropdown'.upper(), settings['SELECT_STEP'])
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
            item['source'] = 'baidu_dropdown'
            item['has_no'] = ''
            cur_url = 'http://www.baidu.com/s?'
            keyword = keyword.encode('gb18030')
            url = cur_url + 'wd=%s' % (keyword)
            meta = {'item': item, 'dont_retry': True}
            self.log('insert new keyword=%s index=%d' %
                     (keyword, index), level=log.INFO)
            yield scrapy.Request(url=url, callback=self.parse, meta=meta, dont_filter=True)

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
        dropdowns = re.findall('(?<=http://suggestion.baidu.com/)(.*?)(?=su)', contents, re.S)
        for iss in dropdowns:
            item['dropdown'] = iss

            yield item

    def closed(self, reason):
        self.oracleFetchConn.close()
