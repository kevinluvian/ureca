# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo
import pdb


class UrecaPipeline(object):
    def process_item(self, item, spider):
        return item


class MongoPipeline(object):
    collection_name = 'us_raw'

    def __init__(self):
        self.mongo_uri = 'mongodb://kevin:kevin@155.69.149.160/geodata'
        self.mongo_db = 'geodata'

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        self.collection = self.db[self.collection_name]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        self.collection.insert(dict(item))
        return item
