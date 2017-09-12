# -*- coding: utf-8 -*-
# import pdb
import json
import pymongo
# import logging

from scrapy import Spider, Request
from scrapy.loader import ItemLoader
from scrapy.exceptions import CloseSpider
from ureca.items import Foursquare
import time
import datetime


sql_update_parsed = '''
UPDATE `parsed_data` SET parsed=true WHERE venue_id=%s;
'''
# NTU 4b442617f964a5200af225e3
# google 40870b00f964a5209bf21ee3


def generate_url_venue_detail(venue_id):
    return 'https://api.foursquare.com/v2/venues/{}?client_id=LFZ3GZTEM34B1NTKJMZHNRWICA1GF5CWGJBR5UP503OU00WK&client_secret=EBAFOC3RUESHDWM4XUKCSOCSCUTZ32FN2C2YFRDSAIJT2HEF&v=20170818'.format(venue_id)


def generate_url_next_venues(venue_id):
    return 'https://api.foursquare.com/v2/venues/{}/nextvenues?client_id=LFZ3GZTEM34B1NTKJMZHNRWICA1GF5CWGJBR5UP503OU00WK&client_secret=EBAFOC3RUESHDWM4XUKCSOCSCUTZ32FN2C2YFRDSAIJT2HEF&v=20170818'.format(venue_id)


def generate_url_explore_venues(latitude, longitude):
    return 'https://api.foursquare.com/v2/venues/explore?ll={},{}&client_id=LFZ3GZTEM34B1NTKJMZHNRWICA1GF5CWGJBR5UP503OU00WK&client_secret=EBAFOC3RUESHDWM4XUKCSOCSCUTZ32FN2C2YFRDSAIJT2HEF&v=20170818'.format(latitude, longitude)


class FoursquareSpider(Spider):
    name = 'foursquare'
    allowed_domains = ['foursquare.com']
    handle_httpstatus_list = [403]
    MAX_DEPTH = 20000000

    def __init__(self):
        self.mongo_uri = 'mongodb://kevin:kevin@155.69.149.160/geodata'
        self.mongo_db = 'geodata'
        self.collection_name = 'us_raw'
        self.log_collection_name = 'log_parser'

        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        self.collection = self.db[self.collection_name]
        self.log_collection = self.db[self.log_collection_name]

    def update_log(self):
        elapsed_time = time.time() - self.latest_log_time
        self.logger.info('elapsed time: {}'.format(elapsed_time))
        if elapsed_time > 60:
        	self.logger.info('insert log_collection')
            count = self.collection.count()
            self.log_collection.insert({
                'elapsed_time': elapsed_time,
                'date_time': datetime.now(),
                'item_parsed': count
            })
            self.latest_log_time = time.time()

    def closed(self, reason):
    	elapsed_time = time.time() - self.latest_log_time
    	self.logger.info('stopped, elapsed time: {}'.format(elapsed_time))
        count = self.collection.count()
        self.log_collection.insert({
            'elapsed_time': elapsed_time,
            'date_time': datetime.now(),
            'item_parsed': count
        })
        self.client.close()
        self.logger.info('reason: {}'.format(reason))

    # Fetch the first url (which is NTU) and spread from there
    def start_requests(self):
        self.latest_log_time = time.time()
        # TODO: make it the smallest depth and parsed = False trus di FOR
        self.collection.create_index([('depth', pymongo.ASCENDING)], background=True)
        venue_list = self.collection \
            .find({
                '$or': [
                    {'is_child_next_venue_parsed': False},
                    {'is_child_explore_parsed': False}
                ]
            }) \
            .sort('depth', 1)
        venue_count = venue_list.count()
        for venue_obj in venue_list:
            venue = (
                generate_url_venue_detail(venue_obj['venue_id']),
                venue_obj['venue_id'],
            )
            self.logger.info(venue_obj['name'])
            venue_raw_obj = venue_obj['raw_data']
            if (venue_obj['is_child_explore_parsed'] is False and
                    'location' in venue_raw_obj and
                    'lat' in venue_raw_obj['location'] and
                    'lng' in venue_raw_obj['location']):
                yield Request(
                    generate_url_explore_venues(
                        venue_raw_obj['location']['lat'],
                        venue_raw_obj['location']['lng']
                    ),
                    callback=lambda x, depth=venue_obj['depth'], venue_id=venue_obj['venue_id']: self.parse_explore_venues(x, depth, venue_id),
                )
            if venue_obj['is_child_next_venue_parsed'] is False:
                yield Request(
                    generate_url_next_venues(venue_obj['venue_id']),
                    callback=lambda x, depth=venue_obj['depth'], venue_id=venue_obj['venue_id']: self.parse_next_venues(x, depth, venue_id),
                )
        if venue_count == 0:
            venue_id_list = self.db['todo'].find({})
            for venue_obj in venue_id_list:
                venue = (
                    generate_url_venue_detail(venue_obj['venue_id']),
                    venue_obj['venue_id'],
                )
            yield Request(
                url=venue[0],
                callback=lambda x, depth=1, venue_id=venue[1]: self.parse(x, depth, venue_id),
            )

    def parse(self, response, depth, venue_id):
        self.update_log()
        self.logger.info('PARSE {}'.format(venue_id))
        # TODO: validate if the data is not in the db
        if response.status == 403:
            raise CloseSpider('Bandwith exceeded')
        count_venue_parsed = self.collection.find({
                'venue_id': venue_id,
                'is_child_next_venue_parsed': True,
                'is_child_explore_parsed': True
            }).count()

        if count_venue_parsed > 0:
            return
        if depth < self.MAX_DEPTH:
            try:
                response_data = json.loads(response.body.decode('utf-8'))
                venue = response_data['response']['venue']

                count_venue = self.collection \
                    .find({'venue_id': venue_id}) \
                    .count()
                if count_venue == 0:
                    loader = ItemLoader(item=Foursquare(), response=response)
                    loader.add_value('venue_id', venue['id'])
                    loader.add_value('name', venue['name'])
                    loader.add_value('is_child_next_venue_parsed', False)
                    loader.add_value('is_child_explore_parsed', False)
                    loader.add_value('raw_data', venue)
                    loader.add_value('depth', depth)
                    yield loader.load_item()

                if depth + 1 < self.MAX_DEPTH:
                    if 'location' in venue and 'lat' in venue['location'] and 'lng' in venue['location']:
                        yield Request(
                            generate_url_explore_venues(
                                venue['location']['lat'],
                                venue['location']['lng']
                            ),
                            callback=lambda x, depth=depth, venue_id=venue_id: self.parse_explore_venues(x, depth, venue_id),
                        )
                    yield Request(
                        generate_url_next_venues(venue_id),
                        callback=lambda x, depth=depth, venue_id=venue_id: self.parse_next_venues(x, depth, venue_id),
                    )
            except Exception:
                pass

    def parse_explore_venues(self, response, depth, venue_id):
        self.update_log()
        if response.status == 403:
            raise CloseSpider('Bandwith exceeded')
        try:
            response_data = json.loads(response.body.decode('utf-8'))
            venue_groups = response_data['response']['groups']
            for venue_group in venue_groups:
                for item in venue_group['items']:
                    count_venue_parsed = self.collection.find(
                        {'venue_id': item['venue']['id']}).count()
                    if count_venue_parsed == 0:
                        yield Request(
                            generate_url_venue_detail(item['venue']['id']),
                            callback=lambda x, depth=depth + 1, venue_id=item['venue']['id']: self.parse(x, depth, venue_id)
                        )
            yield self.update_complete_parse_explore(venue_id)
        except Exception:
            pass

    def parse_next_venues(self, response, depth, venue_id):
        self.update_log()
        if response.status == 403:
            raise CloseSpider('Bandwith exceeded')
        try:
            response_data = json.loads(response.body.decode('utf-8'))
            next_venues = response_data['response']['nextVenues']
            for item in next_venues['items']:
                yield Request(
                    generate_url_venue_detail(item['id']),
                    callback=lambda x, depth=depth + 1, venue_id=item['id']: self.parse(x, depth, venue_id)
                )
            yield self.update_complete_parse_next(venue_id)
        except Exception:
            pass

    def update_complete_parse_explore(self, venue_id):
        self.update_log()
        self.collection.update_one(
            {'venue_id': venue_id},
            {'$set': {'is_child_explore_parsed': True}},
            upsert=False
        )

    def update_complete_parse_next(self, venue_id):
        self.update_log()
        self.collection.update_one(
            {'venue_id': venue_id},
            {'$set': {'is_child_next_venue_parsed': True}},
            upsert=False
        )
