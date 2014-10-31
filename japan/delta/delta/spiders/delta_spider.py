#!/usr/bin/env python
#Objectives: use POST method to post info to the form that is on delta.com

from scrapy.item import Item, Field
from scrapy.http import FormRequest
from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector

from delta.items import DeltaItem

class DeltaSpider(BaseSpider):
    name = "delta"
    allowed_domains = ["delta.com"]
    start_urls = ["http://www.delta.com"]

    def parse(self, response):
        yield FormRequest.from_response(response,
                                        formname='flightSearchForm',
                                        formdata={
                                                  'departureCity[0]' : 'JFK',
                                                  'destinationCity[0]' : 'SFO',
                                                  'departureDate[0]' : '09.20.2014',
                                                  'departureDate[1]' : '09.25.2014'
                                                 },
                                        callback=self.parse)
    def parse(self, response):
        #print response.status
        #sites = response.xpath('//')
        #items = []
        #for i in sites:
        #    items = DeltaItem()
        #    item['title'] = i.select('text()').extract()
        #    item['link'] = i.select('text()').extract()
        #    item['desc'] = i.select('text()').extract()
        #    yield item
        print 'THIS IS PRINTOUT', response.url, response.status
