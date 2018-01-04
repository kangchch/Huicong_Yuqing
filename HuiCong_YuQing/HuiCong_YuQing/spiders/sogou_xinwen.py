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
import random
import string


class SogouXinWenSpider(scrapy.Spider):
    name = "sogou_xinwen"

    def __init__(self, *args, **kwargs):
        super(SogouXinWenSpider, self).__init__(*args, **kwargs)
        #self.start_urls = [
        #        (1, u'慧聪网 骗子', 20),
        #        (2, u'慧聪网 骗局', 11),
        #        (3, u'慧聪网 虚假', 6),
        #        (4, u'慧聪网 垃圾', 7)]
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
        exesql = settings['GET_HUICONG_YUQING_KEYWORD_SQL'].format('sogou_xinwen'.upper(), settings['SELECT_STEP'])
        ret = cr.execute(exesql)
        self.start_urls = ret.fetchall()
        cr.close()

    def start_requests(self):
        #pdb.set_trace()
        for index, keyword, page_max in  self.start_urls:
            pn_max = page_max
            page_index = 0
            keyword = keyword.decode('GBK')
            for pn in range(1, pn_max + 1):
                page_index = page_index + 1
                item = HuicongYuqingItem()
                item['index'] = index
                item['keyword'] = keyword
                item['source'] = 'sogou_xinwen' 
                item['page_index'] = page_index
                item['has_no'] = 'no'
                # cur_url = 'https://www.baidu.com/s?wd=%s&pn=%d' % (urllib.quote(keyword.encode('gb18030')), pn)
                cur_url = 'http://news.sogou.com/news?'
                data = {'query': keyword.encode('gb18030'),
                        'page': pn}
                url = cur_url + urllib.urlencode(data)
                meta = {'pn': pn, 'item': item, 'dont_retry': True}
                self.log('insert new keyword=%s pn=%d index=%d' %
                         (keyword, pn, index), level=log.INFO)
                yield scrapy.Request(url=url, callback=self.parse, meta=meta, dont_filter=True)

    def parse(self, response):
        #pdb.set_trace()
        item = response.meta['item']
        pn = response.meta['pn']

        if response.status != 200:
            self.log('fetch failed! keyword=%s index=%d status=%d jump_url=%s' %
                     (item['keyword'], item['index'], item['status'], response.headers.get('Location', '')), level=log.WARNING)
            if response.status == 302 and response.headers.get('Location', '').find('vcode') > 0:
                ##block
                return
            else:
                yield item
        #div[@class="vrwrap"]
        handles = response.xpath('//div[@class="vrwrap"]')
        for cur_index, handle in enumerate(handles):
            hot_url = handle.xpath(u'./div/div/a[contains(text(), "\u5feb\u7167")]/@href').extract()
            hot_url = hot_url[0] if hot_url else ''
            page_baidu_url = handle.xpath('./div/h3[@class="vrTitle"]/a/@href').extract()
            page_baidu_url = page_baidu_url[0] if page_baidu_url else ''

            introduces = handle.xpath('.//span').extract()
            introduce = u''.join(introduces[0]) if introduces else u''
            com = re.compile(u'<span id="summary_|">|" "|</span>|<em>|</em>|<!--red_beg-->|<!--red_end-->|<span class="bg_imgnews_text', re.S)
            introduce = re.sub(com, '', introduce)
            introduce = introduce.strip(string.digits)
            media_names= handle.xpath(".//p[@class='news-from']/text()").extract()
            media_name = media_names[0].split()[0] if media_names else ''
            # print 'keyword: %s, introduce: %s, media_name: %s' % (item['keyword'], introduce, media_name)
            if not page_baidu_url:
                continue
            item = copy.deepcopy(response.meta['item'])
            item['snapshoot_url'] = hot_url
            item['ranking'] = cur_index
            item['introduce'] = introduce
            item['media_name'] = media_name
            
            yield scrapy.Request(url=page_baidu_url, callback=self.parse_page, meta={'item' : item}, dont_filter=True)


    def parse_page(self, response):
        #pdb.set_trace()
        item = response.meta['item']
        if response.status == 200:
            title = response.xpath("//title/text()").extract()
            title = title[0] if title else ''
            item['title'] = title.strip()
        item['page_url'] = response.url
        item['content'] = response.body_as_unicode()
        return item

