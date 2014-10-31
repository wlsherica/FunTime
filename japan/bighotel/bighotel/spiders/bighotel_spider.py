#!/usr/bin/env python

from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector
from scrapy.http import FormRequest, Request

from bighotel.items import BighotelItem

class BighotelSpider(BaseSpider):
    name = 'bighotel'
    allowed_domains = ["ihg.com"]
    start_urls = ["http://www.ihg.com/holidayinn/hotels/us/en/reservation"]

    def parse(self, response):
        yield FormRequest(url=response.url, 
                                        #formname='', 
                                        formdata={'closedSearch':'false',
                                                  'modifySearch':'resmodule',
                                                  'pageName':'reservation',
                                                  'parentController':'',
                                                  'includedView':'resmodule',
                                                  'showSearchInterstitial':'false',
                                                  'suggestiveHotelCode':'',
                                                  'hotelCode':'',
                                                  'destination':'Los Angeles, CA, United States',
                                                  'callGeocoderAPI':'',
                                                  'geoCode.latitude':'0.0',
                                                  'geoCode.longitude':'0.0',
                                                  'checkInDt':'Dec-22-2014',
                                                  'checkOutDt':'Dec-23-2014',
                                                  'adultsCount':'2',
                                                  'childrenCount':'0', 
                                                  'roomsCount':'1',
                                                  'ratePreference':'6CBARC',
                                                  'roomPreference':'3',
                                                  'corporateId':'',
                                                  'smokingPreference':'3',
                                                  'iataNumber':'',
                                                  'iataNumberFromURL':'false',
                                                  'brandCodes':'hi',
                                                  '_brandCodes':'on',
                                                  'brandCodes':'ex',
                                                  '_brandCodes':'on',
                                                  'brandCodes':'rs',
                                                  '_brandCodes':'on',
                                                  'brandCodes':'cv',
                                                  '_brandCodes':'on',
                                                  'brandCodes':'ic',
                                                  '_brandCodes':'on',
                                                  'brandCodes':'cp',
                                                  '_brandCodes':'on',
                                                  'brandCodes':'in',
                                                  '_brandCodes':'on',
                                                  'brandCodes':'vn',
                                                  '_brandCodes':'on',
                                                  'brandCodes':'sb',
                                                  '_brandCodes':'on',
                                                  'brandCodes':'cw',
                                                  '_brandCodes':'on',
                                                  'brandCodes':'ul',
                                                  '_brandCodes':'on',
                                                  'brandCodes':'6c',
                                                  '_brandCodes':'on',
                                                  'currentBrand':'hi',
                                                  'alongARoute':'false',
                                                  'resultsOrder':'BRAND',
                                                  '_amenities':'on',
                                                  '_amenities':'on',
                                                  '_amenities':'on',
                                                  '_amenities':'on',
                                                  '_amenities':'on',
                                                  '_amenities':'on',
                                                  '_amenities':'on',
                                                  '_amenities':'on',
                                                  '_amenities':'on'
                                                 }, 
                                        callback=self.parse_items)
    def parse_items(self, response):
        info = response.xpath('//title') #//div/h3
        print 'hello...', response.url, info
        #items = []
        #for i in info:
        #    item = BighotelItem()
        #    item['room_type'] = i.select('a[@class="detailsLink"]/text()').extract()
        #    yield item 
