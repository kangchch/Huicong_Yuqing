# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy.conf import settings
from scrapy.exceptions import DropItem
import logging
import sys 
import pdb 
import cx_Oracle
import re
import os

os.system('export LANG=zh_CN.GB18030')
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.ZHS16GBK'

class HuicongYuqingPipeline(object):
    def __init__(self):
        self.logger = logging.getLogger('HuiCongYuQing')
        self.oracleConn = None
        try:
            connstr = "%s/%s@%s/%s" % (
                    settings['ORACLE_SERVER_USERNAME'],
                    settings['ORACLE_SERVER_PASSWORD'],
                    settings['ORACLE_SERVER_ADDR'],
                    settings['ORACLE_SERVER_DBNAME'])
            self.oracleConn = cx_Oracle.connect(connstr)
        except Exception, err:
            self.logger.error("Connection Oracle Error!: %s" % (err,))


    def process_item(self, item, spider):

        if not item:
            return
        if u'\u8d34\u5427404' in item.get(u'title', u''):
            return
        #pdb.set_trace()

        # keywordset = re.split(u'[\r\n\t ]+', item['keyword'])
        keywordset = settings.get('JUDGE_KEYWORDS', ())
        hit_keywords = []
        for keyword in keywordset:
            if item.get(u'page_url', u'').find('hc360.com') > 0:
                continue

            if keyword in item.get(u'related', u'') or keyword in item.get(u'dropdown', u'') or keyword in item.get(u'title', u'') or keyword in item.get(u'content', u'') or keyword in item.get(u'introduce', u''):
                hit_keywords.append(keyword)

            if keyword in item.get(u'introduce', u'') or keyword in item.get(u'title', u''):
                item['has_no'] = 'has'

        if not hit_keywords:
            return item

        result_item = {}
        result_item['index'] = item['index']
        result_item['keyword'] = item['keyword']
        result_item['judge_keywords'] = ','.join(hit_keywords)
        result_item['page_url'] = item.get(u'page_url', u'')
        result_item['title'] = item.get(u'title', u'')
        result_item['source'] = item['source']
        result_item['snapshoot_url'] = item.get(u'snapshoot_url', u'')
        result_item['tieba_name'] = item.get(u'tieba_name', u'')
        result_item['baidu_zhidao_id'] = item.get(u'baidu_zhidao_id', u'')
        result_item['post_time'] = item.get(u'post_time', u'')
        ## add
        result_item['has_no'] = item['has_no']
        result_item['media_name'] = item.get(u'media_name', u'')
        result_item['page_index'] = item.get('page_index', u'')
        result_item['ranking'] = item.get('ranking', u'')
        result_item['introduce'] = item.get(u'introduce', u'')
        result_item['related'] = item.get(u'related', u'')
        result_item['dropdown'] = item.get(u'dropdown', u'')
        self.insertOracle(result_item)

    def insertOracle(self, item):
        cr = self.oracleConn.cursor()
        exesql = settings['INSERT_SQL'].format(
                    keyword = item['keyword'],
                    judge_keywords = item['judge_keywords'],
                    page_url = item['page_url'],
                    snapshoot_url = item['snapshoot_url'],
                    title = item['title'],
                    source = item['source'],
                    tieba_name = item['tieba_name'],
                    zhidao_id = item['baidu_zhidao_id'],
                    post_time = item['post_time'],
                    has_no = item['has_no'],
                    media_name = item['media_name'],
                    page_index = item['page_index'],
                    ranking = item['ranking'],
                    introduce = item['introduce'],
                    related= item['related'],
                    dropdown= item['dropdown'],
               )

        try:
            cr.execute(exesql)
            self.oracleConn.commit()
            cr.close()
            self.updateOracle(item)
        except Exception, err:
            # pass
            self.logger.error(u'{0:s} ERROR!'.format(exesql))
    def updateOracle(self, item):
        cr = self.oracleConn.cursor()
        exesql = settings['UPDATE_HUICONG_YUQING_KEYWORD_TBL_SQL'].format(item['source'], item['index'])
        try:
            cr.execute(exesql)
            self.oracleConn.commit()
            cr.close()
        except Exception, err:
            # pass
            self.logger.error(u'{0:s} ERROR!'.format(exesql))


