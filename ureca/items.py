# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy.contrib.loader.processor import TakeFirst


class UrecaItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


class Foursquare(scrapy.Item):
    # define the fields for your item here like:
    venue_id = scrapy.Field(output_processor=TakeFirst())
    name = scrapy.Field(output_processor=TakeFirst())
    raw_data = scrapy.Field(output_processor=TakeFirst())
    depth = scrapy.Field(output_processor=TakeFirst())
    is_child_parsed = scrapy.Field(output_processor=TakeFirst())
    pass
