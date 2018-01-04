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
import urlparse


class XinWen360Spider(scrapy.Spider):
    name = "so_xinwen"

    def __init__(self, *args, **kwargs):
        super(XinWen360Spider, self).__init__(*args, **kwargs)
        #pdb.set_trace()
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
            self.logger.error("Manager fetch aligoUrl Connection Oracle Error!: %s" % (err,))

        cr = self.oracleFetchConn.cursor()
        exesql = settings['GET_HUICONG_YUQING_KEYWORD_SQL'].format('so_xinwen'.upper(), settings['SELECT_STEP'])
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
                item['source'] = u'so_xinwen'
                item['page_index'] = page_index
                # cur_url = 'https://www.baidu.com/s?wd=%s&pn=%d' % (urllib.quote(keyword.encode('gb18030')), pn)
                cur_url = 'http://news.so.com/ns?'
                data = {'q': keyword.encode('gb18030'),
                        'pn': pn}
                url = cur_url + urllib.urlencode(data)
                meta = {'pn': pn, 'item': item, 'dont_retry': True}
                self.log('insert new keyword=%s pn=%d index=%d' %
                         (keyword, pn, index), level=log.INFO)
                yield scrapy.Request(url=url,
                        callback=self.parse,
                        meta=meta,
                        headers = {
                            'Host' : 'news.so.com',
                            'Upgrade-Insecure-Requests' : '1', 
                            'Accept' : 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                            'User-Agent' : random.choice(settings['USER_AGENT']), 
                        },
                        dont_filter=True)

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
        handles = response.xpath('//ul[@class="result"]/li')
        for cur_index, handle in enumerate(handles):
            #pdb.set_trace()
            hot_url = handle.xpath(u'./p[@class="res-linkinfo"]/a[contains(text(), "\u5feb\u7167")]/@href').extract()
            hot_url = hot_url[0] if hot_url else ''
            page_baidu_url = handle.xpath('./h3/a/@href').extract()
            page_baidu_url = page_baidu_url[0] if page_baidu_url else ''

            media_names = handle.xpath(".//span[@class='sitename']/text()").extract()
            media_name = media_names[0] if media_names else ''

            introduces = handle.xpath('./p[@class="content"]').extract()
            introduce = u''.join(introduces[0]) if introduces else u''
            com = re.compile('<p class="content">|<em>|</em>|</p>|" "', re.S)
            introduce = re.sub(com, '', introduce)
            if u'group-img-link' in introduce:
                item['introduce'] = u''
            else:
                item['introduce'] = introduce

            if not page_baidu_url:
                continue
            item = copy.deepcopy(response.meta['item'])
            item['snapshoot_url'] = hot_url
            item['ranking'] = cur_index
            item['media_name'] = media_name

            # print "keyword: %s, introduce: %s, media_name: %s" % (item['keyword'], item['introduce'], media_name)
            yield scrapy.Request(url=page_baidu_url, callback=self.parse_page, meta={'item' : item}, dont_filter=True)

    def parse_middle_page(self, response):
        content = response.body_as_unicode()
        final_url = re.findall('replace\("(.*?)"\)', content)
        if not final_url:
            return
        else:
            final_url = final_url[0]

        yield scrapy.Request(url = final_url,
                callback = self.parse_page,
                meta = {'item' : copy.deepcopy(response.meta['item'])},
                headers = {
                    'User-Agent' : random.choice(settings['USER_AGENT']),
                    'Referer' : response.url,
                    'Host' : urlparse.urlparse(final_url).netloc,
                },
                dont_filter = True)

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

