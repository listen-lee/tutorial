import scrapy
from scrapy.loader import ItemLoader
from tutorial.items import TutorialItem


class AuthorSpider(scrapy.Spider):
    name = 'author-items'
    start_urls = ['http://quotes.toscrape.com/']

    def parse(self, response):
        # follow link to author pages
        for href in response.css('.author + a::attr(href)'):
            yield response.follow(href, self.parse_author)
        # follow pagination link
        for href in response.css('li.next a::attr(href)'):
            yield response.follow(href, self.parse)

    def parse_author(self, response):
        def extract_with_css(query):
            return response.css(query).get(default='').strip()

        l = ItemLoader(item=TutorialItem(), response=response)
        l.add_css('h3.author-title::text')
        l.add_css('.author-born-date::text')
        l.add_css('.author-description::text')
        return l.load_item()
        # yield {
        #     'name': extract_with_css('h3.author-title::text'),
        #     'birthday': extract_with_css('.author-born-date::text'),
        #     'bio': extract_with_css('.author-description::text')
        # }
