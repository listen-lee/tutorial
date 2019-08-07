# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from kafka import KafkaProducer
import json
import logging


class TutorialPipeline(object):
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    def __init__(self, kafka_uri, kafka_topic):
        self.kafka_uri = kafka_uri
        self.kafka_topic = kafka_topic

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            kafka_uri=crawler.settings.get('KAFKA_URI'),
            kafka_topic=crawler.settings.get('KAFKA_TOPIC')
        )

    def open_spider(self, spider):
        logging.debug("open_spider, kafka_uri: [%s]", self.kafka_uri)
        self.client = KafkaProducer(bootstrap_servers=self.kafka_uri)

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        item_json = json.dumps(item, default=lambda x: x.__dict__)
        future = self.client.send(self.kafka_topic, bytes(item_json, encoding='utf8'))
        result = future.get(60)
        logging.debug('result:[%s]', result)
        return item
