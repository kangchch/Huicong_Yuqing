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


class BaiduTiebaSpider(scrapy.Spider):
    name = "baidu_tieba"

    def __init__(self, *args, **kwargs):
        super(BaiduTiebaSpider, self).__init__(*args, **kwargs)
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
        exesql = settings['GET_HUICONG_YUQING_KEYWORD_SQL'].format('baidu_tieba'.upper(), settings['SELECT_STEP'])
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
                item['source'] = 'baidu_tieba'
                item['page_index'] = page_index
                item['has_no'] = 'no'
                # cur_url = 'https://www.baidu.com/s?wd=%s&pn=%d' % (urllib.quote(keyword.encode('gb18030')), pn)
                cur_url = 'http://tieba.baidu.com/f/search/res?'
                data = {'qw': keyword.encode('gb18030'),
                        'pn': pn}
                url = cur_url + urllib.urlencode(data)
                meta = {'pn': pn, 'item': item, 'dont_retry': True}
                self.log('insert new keyword=%s pn=%d index=%d' %
                         (keyword, pn, index), level=log.INFO)
                yield scrapy.Request(url=url,
                        callback=self.parse,
                        meta=meta,
                        headers = {
                            'Host' : 'tieba.baidu.com',
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

        handles = response.xpath(u'//div[@class="s_post_list"]/div')
        for cur_rank, handle in enumerate(handles):
            #pdb.set_trace()
            # hot_url = handle.xpath(".//div[@class='f13']/a[@class='m']/@href").extract()
            # hot_url = hot_url[0] if hot_url else ''
            page_baidu_url = handle.xpath(u'./span[@class="p_title"]/a/@href').extract()
            page_baidu_url = page_baidu_url[0] if page_baidu_url else ''
            if not page_baidu_url:
                continue
            tieba_name = handle.xpath(u'./a[@class="p_forum"]/font/text()').extract()
            tieba_name = tieba_name[0] if tieba_name else u''
            if not tieba_name:
                continue

            introduces = handle.xpath("div[@class='p_content']").extract()
            introduce = u''.join(introduces[0]) if introduces else u''
            com = re.compile('<div class="p_content">|<em>|</em>|</div>|" "', re.S)
            introduce = re.sub(com, '', introduce)
            item = copy.deepcopy(response.meta['item'])
            # item['snapshoot_url'] = hot_url
            item['tieba_name'] = tieba_name
            item['ranking'] = cur_rank
            item['introduce'] = introduce
            # print 'keyword: %s, introduce: %s' % (item['keyword'], introduce)

            if not page_baidu_url.startswith('https'):
                page_baidu_url = u'https://tieba.baidu.com' + page_baidu_url
            elif not page_baidu_url.startswith('http'):
                page_baidu_url = u'http://tieba.baidu.com' + page_baidu_url

            yield scrapy.Request(url=page_baidu_url,
                    callback=self.parse_page,
                    meta={'item' : item},
                    headers = {
                            'Host' : 'tieba.baidu.com',
                            'Upgrade-Insecure-Requests' : '1', 
                            'Referer' : response.url,
                            'Accept' : 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                            'User-Agent' : random.choice(settings['USER_AGENT']), 
                    },
                    dont_filter=True)

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
