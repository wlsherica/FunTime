#!/usr/bin/env python

#need to define: name, start_urls, parse()
#plz create .py under folder spider

from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector

from jpHotel.items import JphotelItem

class HotelSpider(BaseSpider):
   name = "HotelSpider"
   start_urls = [
       "https://hotel.reservation.jp/superhotel/eng/reservation/3.asp?ne_hotel=049&ne_reserv_d=3&ne_reserv_ym=2007/11/01"
    ]

   def parse(self, response):
       hxs = HtmlXPathSelector(response)
       mn = hxs.select('//option[contains(@value,"2015")]')
       #mn_info = mn.select('text()').extract()

       items = []

       for i in mn:
           item = JphotelItem()
           item['YearMonth'] = i.select('text()').extract()
           items.append(item)
       return items
       #filename = response.url.split("/")[-2]
       #open(filename, 'wb').write(response.body)

