# -*- coding: utf-8 -*-
import pdb
import json
import pymongo

from scrapy import Spider, Request
from scrapy.loader import ItemLoader
from ureca.items import Foursquare


sql_update_parsed = '''
UPDATE `parsed_data` SET parsed=true WHERE venue_id=%s;
'''

def generate_url_venue_detail(venue_id):
    return 'https://api.foursquare.com/v2/venues/{}?client_id=LFZ3GZTEM34B1NTKJMZHNRWICA1GF5CWGJBR5UP503OU00WK&client_secret=EBAFOC3RUESHDWM4XUKCSOCSCUTZ32FN2C2YFRDSAIJT2HEF&v=20170818'.format(venue_id)

def generate_url_next_venues(venue_id):
    return 'https://api.foursquare.com/v2/venues/{}/nextvenues?client_id=LFZ3GZTEM34B1NTKJMZHNRWICA1GF5CWGJBR5UP503OU00WK&client_secret=EBAFOC3RUESHDWM4XUKCSOCSCUTZ32FN2C2YFRDSAIJT2HEF&v=20170818'.format(venue_id)

def generate_url_explore_venues(latitude, longitude):
    return 'https://api.foursquare.com/v2/venues/explore?ll={},{}&client_id=LFZ3GZTEM34B1NTKJMZHNRWICA1GF5CWGJBR5UP503OU00WK&client_secret=EBAFOC3RUESHDWM4XUKCSOCSCUTZ32FN2C2YFRDSAIJT2HEF&v=20170818'.format(latitude, longitude)

class FoursquareSpider(Spider):
    name = 'foursquare'
    allowed_domains = ['foursquare.com']
    MAX_DEPTH = 20

    def __init__(self):
		self.mongo_host = 'localhost'
		self.mongo_port = 27017
		self.mongo_db = 'ureca'
		self.collection_name = 'parsed_data_raw'
		self.client = pymongo.MongoClient(self.mongo_host, self.mongo_port)
		self.db = self.client[self.mongo_db]
		self.collection = self.db[self.collection_name]

    def closed(self, reason):
        self.client.close()
        print('reason: ', reason)

    # Fetch the first url (which is NTU) and spread from there
    def start_requests(self):
        # TODO: make it the smallest depth and parsed = False trus di FOR
        venue = (
            generate_url_venue_detail('4b442617f964a5200af225e3'),
            '4b442617f964a5200af225e3',
        )
        yield Request(
            url=venue[0],
            callback=lambda x, depth=1, venue_id=venue[1]: self.parse(x, depth, venue_id),
        )

    def parse(self, response, depth, venue_id):
        # TODO: validate if the data is not in the db
        if depth < self.MAX_DEPTH:
            try:
                response_data = json.loads(response.body.decode('utf-8'))
                venue = response_data['response']['venue']
                loader = ItemLoader(item=Foursquare(), response=response)
                loader.add_value('venue_id', venue['id'])
                loader.add_value('name', venue['name'])
                loader.add_value('is_child_parsed', False)
                loader.add_value('raw_data', venue)
                loader.add_value('depth', depth)
                yield loader.load_item()
                if depth + 1 < self.MAX_DEPTH:
                    yield Request(
                        generate_url_next_venues(venue_id),
                        callback=lambda x, depth=depth, venue_id=venue_id: self.parse_next_venues(x, depth, venue_id),
                    )
                    if 'location' in venue and 'lat' in venue['location'] and 'lng' in venue['location']:
                        yield Request(
                            generate_url_explore_venues(venue['location']['lat'], venue['location']['lng']),
                            callback=lambda x, depth=depth, venue_id=venue_id: self.parse_explore_venues(x, depth, venue_id),
                        )
                    yield self.update_complete_parse(venue_id=venue_id)

            except Exception as e:
                pass

    def parse_explore_venues(self, response, depth, venue_id):
        try:
            response_data = json.loads(response.body.decode('utf-8'))
            venue_groups = response_data['response']['groups']
            for venue_group in venue_groups:
                for item in venue_group['items']:
                    yield Request(
                        generate_url_venue_detail(item['venue']['id']),
                        callback=lambda x, depth=depth + 1, venue_id=item['venue']['id']: self.parse(x, depth, venue_id)
                    )
        except Exception as e:
            pass

    def parse_next_venues(self, response, depth, venue_id):
        try:
            response_data = json.loads(response.body.decode('utf-8'))
            next_venues = response_data['response']['nextVenues']
            for item in next_venues['items']:
                yield Request(
                    generate_url_venue_detail(item['id']),
                    callback=lambda x, depth=depth + 1, venue_id=item['id']:self.parse(x, depth, venue_id)
                )
        except Exception as e:
            pass

    def update_complete_parse(self, venue_id):
    	self.collection.update_one({ 'venue_id': venue_id }, { '$set': { 'is_child_parsed': True } }, upsert=False)
