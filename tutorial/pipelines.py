# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from kafka import KafkaProducer


class TutorialPipeline(object):
    topic_name = 'scrapy_items'

    def __init__(self, kafka_uri):
        self.kafka_uri = kafka_uri

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            kafka_uri=crawler.setting.get('KAFKA_URI')
        )

    def open_spider(self, spider):
        self.client = KafkaProducer(bootstrap_servers=self.kafka_uri)

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        return item
