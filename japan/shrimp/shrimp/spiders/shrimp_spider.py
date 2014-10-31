#!/usr/bin/env python

from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector
from scrapy.contrib.spiders import CrawlSpider, Rule
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor

from shrimp.items import ShrimpItem

class ShrimpSpider(CrawlSpider):
    name = "shrimp"
    allowed_domains = ["shrimp2.pixnet.net"]
    start_urls = ["http://shrimp2.pixnet.net/blog/"]

    #rules = (Rule (SgmlLinkExtractor(allow=('[\d]+',),
    #       restrict_xpaths=('//a[@class="next"]',)),
    #       callback='parse_item', 
    #       follow=True),
    #)
    #allow=(r'http://shrimp2.pixnet.net/blog/\d+',)
    #'//a[@class="next"]/@href'
    def __init__(self):
        self._rules = (Rule (SgmlLinkExtractor(
								restrict_xpaths=('//a[@class="next"]',)
							),
							callback=self.parse_item,
							follow=True),
        )

    #def parse(self, response):
    #    pass
        #print "\n".join(response.xpath("//a[@class='next']/@href").extract())

        '''
        hxs = HtmlXPathSelector(response)
        article = hxs.select('//li/h2')
        #items = []
        for i in article:
            item = ShrimpItem()
            item['title']=i.select('a/text()').extract()
            #items.append(item)
            yield item
        #return items
        '''

    def parse_item(self, response):
        #print response.url
        #print "\n".join(response.xpath("//a[@class='next']/@href").extract())
        article = response.xpath('//li/h2')
        for i in article:
            item = ShrimpItem()
            item['title']=i.select('a/text()').extract()
            yield item
