#!/bin/bash
cd /app/HuiCong_YuQing/HuiCong_YuQing
rm *.log nohup.out -rf
scrapy crawl baidu_wangye  --logfile Log_baidu_wangye.log
scrapy crawl baidu_xinwen  --logfile Log_baidu_xinwen.log
scrapy crawl baidu_zhidao  --logfile Log_baidu_zhidao.log
scrapy crawl baidu_wenku  --logfile Log_baidu_wenku.log
scrapy crawl baidu_tieba  --logfile Log_baidu_tieba.log
scrapy crawl sogou_wangye  --logfile Log_sogou_wangye.log
scrapy crawl sogou_xinwen  --logfile Log_sogou_xinwen.log
scrapy crawl so_wangye  --logfile Log_so_wangye.log
scrapy crawl so_xinwen  --logfile Log_so_xinwen.log
scrapy crawl baidu_related 
scrapy crawl baidu_dropdown
scrapy crawl so_related
scrapy crawl so_dropdown
scrapy crawl sogou_related
scrapy crawl sogou_dropdown
