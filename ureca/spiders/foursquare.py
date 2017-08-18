# -*- coding: utf-8 -*-
import pdb
import json

from scrapy import Spider, Request
from scrapy.loader import ItemLoader
from ureca.items import Foursquare


sql_update_parsed = '''
UPDATE `parsed_data` SET parsed=true WHERE venue_id=%s;
'''


class FoursquareSpider(Spider):
    name = 'foursquare'
    allowed_domains = ['foursquare.com']

    # Fetch the first url (which is NTU) and spread from there
    def start_requests(self):
        # TODO: make it the smallest depth and parsed = False trus di FOR
        venue = (
            'https://api.foursquare.com/v2/venues/4b442617f964a5200af225e3',
            '4b442617f964a5200af225e3',
        )
        yield Request(
            url=venue[0],
            callback=lambda x, venue_id=venue[1]: self.parse(x, venue_id),
        )
        yield self.update_complete_parse(venue=venue)

    def parse(self, response, venue_id):
        # todo: cek dulu ada ga di db
        pdb.set_trace()
        response_data = json.loads(response.body.decode('utf-8'))
        loader = ItemLoader(item=Foursquare(), response=response)


    def update_complete_parse(self, venue):
        self.cursor.execute(sql_update_parsed, [venue[1], ])
