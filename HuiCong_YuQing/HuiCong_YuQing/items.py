# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class HuicongYuqingItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()

    index = scrapy.Field()
    keyword = scrapy.Field()
    judge_keywords = scrapy.Field()
    title = scrapy.Field()
    content = scrapy.Field()
    snapshoot_url = scrapy.Field()
    page_url = scrapy.Field()
    source = scrapy.Field()
    status = scrapy.Field()
    tieba_name  = scrapy.Field()
    page_index = scrapy.Field() ## 页数
    is_title_contain_keywords = scrapy.Field()
    is_content_contain_keywords = scrapy.Field()
    baidu_zhidao_id = scrapy.Field()
    post_time = scrapy.Field()

    ##add 
    ranking = scrapy.Field() ## 排名
    introduce = scrapy.Field() ## 简介
    has_no = scrapy.Field() ## 标题简介是否包含判定词
    media_name = scrapy.Field() ## 媒体名称
    related = scrapy.Field()
    dropdown = scrapy.Field()
