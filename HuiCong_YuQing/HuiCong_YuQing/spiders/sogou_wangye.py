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


class SogouWangYeSpider(scrapy.Spider):
    name = "sogou_wangye"

    def __init__(self, *args, **kwargs):
        super(SogouWangYeSpider, self).__init__(*args, **kwargs)
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
        exesql = settings['GET_HUICONG_YUQING_KEYWORD_SQL'].format('sogou_wangye'.upper(), settings['SELECT_STEP'])
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
                item['source'] = 'sogou_wangye'
                item['page_index'] = page_index
                # cur_url = 'https://www.baidu.com/s?wd=%s&pn=%d' % (urllib.quote(keyword.encode('gb18030')), pn)
                cur_url = 'http://www.sogou.com/web?'
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
            #pdb.set_trace()
            hot_url = handle.xpath(u'./div//a[contains(text(), "\u5feb\u7167")]/@href').extract()
            hot_url = hot_url[0] if hot_url else ''
            page_baidu_url = handle.xpath('./h3[@class="vrTitle"]/a/@href').extract()
            page_baidu_url = page_baidu_url[0] if page_baidu_url else ''

            introduces = handle.xpath('//p[@class="str_info"]').extract()
            introduce = u''.join(introduces[0]) if introduces else u''
            com = re.compile(u'<p class="str_info">|<em>|</em>|</div>|" "|<!--red_beg-->|<!--red_end-->|</p>', re.S)
            introduce = re.sub(com, '', introduce.replace('\r\n', ''))
            if u'display:none' in introduce:
                item['introduce'] = u''
            else:
                item['introduce'] = introduce
            # print 'keyword: %s, introduce: %s' % (item['keyword'], item['introduce'])

            if not page_baidu_url:
                continue
            item = copy.deepcopy(response.meta['item'])
            item['snapshoot_url'] = hot_url
            item['ranking'] = cur_index
            
            yield scrapy.Request(url=page_baidu_url, callback=self.parse_page, meta={'item' : item}, dont_filter=True)

        #div[@class="rb_\d"]
        handles = response.xpath('//div[@class="rb"]')
        for cur_index, handle in enumerate(handles):
            #pdb.set_trace()
            hot_url = handle.xpath(u'./div/a[contains(text(), "\u5feb\u7167")]/@href').extract()
            hot_url = hot_url[0] if hot_url else ''
            page_baidu_url = handle.xpath('./h3[@class="pt"]/a/@href').extract()
            page_baidu_url = page_baidu_url[0] if page_baidu_url else ''

            introduces = handle.xpath('./div[@class="ft"]').extract()
            introduce = u''.join(introduces[0]) if introduces else u''
            com = re.compile(u'<div class="ft" id="cacheresult_summary_|">|<!--summary_beg-->|<!--summary_end-->|<!--red_beg-->|<!--red_end-->|<em>|</em>|</div>|" "|</div>', re.S)
            introduce = re.sub(com, '', introduce)
            ## digits 移除字符串开头的数字
            item['introduce'] = introduce.replace(' ', '').strip(string.digits)
            # print 'keyword: %s, introduce: %s' % (item['keyword'], item['introduce'])
            if not page_baidu_url:
                continue
            item = copy.deepcopy(response.meta['item'])
            item['snapshoot_url'] = hot_url
            item['ranking'] = cur_index
            
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

    def closed(self, reason):
        self.oracleFetchConn.close()
