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
import re


class BaiduZhiDaoSpider(scrapy.Spider):
    name = "baidu_zhidao"

    def __init__(self, *args, **kwargs):
        super(BaiduZhiDaoSpider, self).__init__(*args, **kwargs)
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
        exesql = settings['GET_HUICONG_YUQING_KEYWORD_SQL'].format('baidu_zhidao'.upper(), settings['SELECT_STEP'])
        ret = cr.execute(exesql)
        self.start_urls = ret.fetchall()
        cr.close()

    def start_requests(self):
        #pdb.set_trace()
        for index, keyword, page_max in  self.start_urls:
            pn_max = 10 * page_max
            page_index = 0
            keyword = keyword.decode('GBK')
            for pn in range(0, pn_max, 10):
                page_index = page_index + 1
                item = HuicongYuqingItem()
                item['index'] = index
                item['keyword'] = keyword
                item['source'] = 'baidu_zhidao'
                item['page_index'] = page_index
                item['has_no'] = 'no'
                # cur_url = 'https://www.baidu.com/s?wd=%s&pn=%d' % (urllib.quote(keyword.encode('gb18030')), pn)
                cur_url = 'https://zhidao.baidu.com/search?'
                data = {'word': keyword.encode('gb18030'),
                        'pn': pn}
                url = cur_url + urllib.urlencode(data)
                meta = {'pn': pn, 'item': item, 'dont_retry': True}
                self.log('insert new keyword=%s pn=%d index=%d' %
                         (keyword, pn, index), level=log.INFO)
                yield scrapy.Request(url=url,
                        callback=self.parse,
                        meta=meta,
                        headers = {
                            'Host' : 'zhidao.baidu.com',
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

        handles = response.xpath('//div[@class="list"]/dl')
        for cur_rank, handle in enumerate(handles):
            page_baidu_url = handle.xpath("./dt/a/@href").extract()
            page_baidu_url = page_baidu_url[0].split('?')[0] if page_baidu_url else ''
            # introduces = handle.xpath('./dd').extract()
            introduces = handle.xpath('./dd[contains(@class, "dd answer")]').extract()
            introduce = u''.join(introduces[0]) if introduces else u''
            com = re.compile(u'<dd class="dd answer"><i class="i-answer-text">|<dd class="dd summary"><i class="i-answer-text">|</i>|<em>|</em>|</dd>|" "|', re.S)
            introduce = re.sub(com, '', introduce)
            # print 'keyword: %s,  introduce: %s' % (item['keyword'], introduce)
            if not page_baidu_url:
                continue
            item = copy.deepcopy(response.meta['item'])
            item['snapshoot_url'] = u''
            item['ranking'] = cur_rank
            item['introduce'] = introduce
            yield scrapy.Request(url=page_baidu_url,
                    callback=self.parse_page,
                    meta={'item' : item},
                    headers = {
                        'Host' : 'zhidao.baidu.com',
                        'Upgrade-Insecure-Requests' : '1', 
                        'Accept' : 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                        'User-Agent' : random.choice(settings['USER_AGENT']), 
                        'Referer' : response.url,
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
        baidu_zhidao_id = response.xpath('//a[@alog-action="qb-ask-uname"]/text()').extract()
        item['baidu_zhidao_id']= baidu_zhidao_id[0] if baidu_zhidao_id else u''
        post_time = response.xpath('//span[@class="grid-r f-aid pos-time answer-time f-pening"]/text()').extract()
        if not post_time:
            post_time = response.xpath('//span[@class="pos-time"]/text()').extract()
        post_time = post_time[0] if post_time else u''
        item['post_time'] = re.sub(u'[\u4e00-\u9fa5]', u'', post_time)
        item['content'] = response.body_as_unicode()
        return item

