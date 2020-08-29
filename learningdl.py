#!/usr/bin/env python3

import logging
from scrapy import Spider, Request, Item, Field, logformatter
from scrapy.crawler import CrawlerProcess
# from scrapy.loader import ItemLoader
from scrapy.exceptions import CloseSpider, DropItem
from pathlib import Path
from datetime import datetime, timedelta, timezone
from itemadapter import ItemAdapter
from pprint import pprint


class LearningDLSpider(Spider):
    name = "learningdl"
    MAX_TIMEDELTA = timedelta(days=2)  #<<<<====CHANGE HERE
    start_urls = (
        'https://learningdl.net/category/ebooks-tutorials/technical/',
        # 'https://learningdl.net/?s=cybrary',
        # 'https://learningdl.net/?s=sans',
    )
    allowed_domains = ["learningdl.net"]
    custom_settings = {
        'ITEM_PIPELINES': {
            'learningdl.RapidgatorPipeline': 1,
            'learningdl.UdemyBlackListPipeline': 10
        },
        'LOG_LEVEL': 'INFO',
        'LOG_FORMATTER': 'learningdl.PoliteLogFormatter',
        'FEEDS': {
            'data_%(name)s_%(time)s_.json': {
                'format': 'jsonlines',
                'encoding': 'utf8',
                'store_empty': False,
                'fields': None,
                'indent': 0,
            },
            Path('data_%(name)s_%(time)s_.csv'): {
                'format': 'csv',
                'fields': ["title", "links", "date", "url", "id"],
            },
        }
    }

    def __init__(self, *args, **kwargs):
        super(LearningDLSpider, self).__init__(*args, **kwargs)
        self.__now = datetime.now(timezone.utc)
        "start_time':"
        "'finish_time':"
        self.logger.setLevel('INFO')
        self.article = 0
        self.article_parsed = 0
        self.too_old_article = 0
        """ 10 articles/page """
        self.too_old_nb_limit = 20

    def start_requests(self):
        pprint(self.settings.__dict__)
        self.logger.info("### Start URLs: {}".format(self.start_urls))
        for url in self.start_urls:
            yield Request(url=url, callback=self.parse)

    def parse(self, response):
        """  parse list of articles  """
        self.logger.debug("response.url : {}".format(response.url))
        articles_xpath = '/html/body/div/div/div/main/article'
        for article in response.xpath(articles_xpath):
            date = article.xpath('header/p/time/@datetime').extract_first()
            self.logger.debug("### ARTICLE DATE: {}".format(date))
            article_date = datetime.strptime(date, '%Y-%m-%dT%H:%M:%S%z')
            self.logger.debug("### ARTICLE DATE python: {}".format(article_date))

            delta = self.__now - article_date
            self.logger.info("### ARTICLE DELTA TIME: {}".format(delta))
            if delta <= self.MAX_TIMEDELTA:
                article_link = article.xpath('header/h2/a/@href').extract_first()
                if article_link is None:
                    article_link = article.xpath('div[@class="entry-content"]/p/a[@class="more-link"]/@href').extract_first()
                self.logger.debug("### ARTICLE LINK: {}".format(article_link))
                self.article += 1
                yield Request(article_link, callback=self.parse_item, meta={'article_date': date})
            else:
                self.too_old_article += 1
                if self.too_old_article > self.too_old_nb_limit:
                    raise CloseSpider('Too OLD Content, no need to check older post')

        next_page = response.xpath('//li[@class="pagination-next"]/a/@href').extract_first()
        self.logger.debug("### NEXT_PAGE URLs: {}".format(next_page))
        if next_page is not None:
            yield Request(next_page, callback=self.parse)

    @staticmethod
    def remove_bad_char(ss):
        return ss.replace('"', '').replace('–', '')

    def parse_item(self, response):
        """   parse article """
        item = Article(
            url=response.request.url,
            date=response.meta['article_date'],
            title=self.remove_bad_char(response.xpath('//article/header/h1/text()').extract_first()),
            id=response.xpath('//article//@class').re_first(r'post-(\d+)'),
            author=response.xpath('//span[@itemprop="author"]/a/span/text()').extract_first(),
            lang=response.xpath('//article/div[@class="entry-content"]//text()').re_first(r'(.*) \| Size\: .+'),
            size=response.xpath('//article/div[@class="entry-content"]//text()').re_first(r'.* \| Size\: (.+)'),
            cat=response.xpath('//article/div[@class="entry-content"]//text()').re_first(r'(?:Genre|Category)\: (.+)'),
            desc="".join(response.xpath('//div[@style="text-align:center;"]/following-sibling::*/text()').extract()),
            links=response.xpath('//a[@class="autohyperlink"]/@href').extract()
        )
        # self.logger.debug("### ARTICLE Item: {}".format(item))
        # self.article_parsed += 1
        yield item

#     def parse_item(self, response):
#         item = ItemLoader(Article(), response)
#         item.add_value('url' ,response.request.url)
#         item.add_value('date', response.meta['article_date'])
#         item.add_xpath('links', '//a[@class="autohyperlink"]/@href')
#         item.add_xpath('id', '//article//@class')
#         item.add_xpath('author', '//span[@itemprop="author"]/a/span/text()')
#         item.add_xpath('lang', '//article/div[@class="entry-content"]//text()')
#         item.add_xpath('size', '//article/div[@class="entry-content"]//text()')
#         item.add_xpath('cat', '//article/div[@class="entry-content"]//text()')
#         item.add_xpath('desc', '//article/div[@class="entry-content"]//text()')
#         yield item.load_item()
#
#     class ProductLoader(ItemLoader):
#         #default_input_processor = TakeFirst()
#         default_output_processor = TakeFirst()
#
#         url_in = MapCompose(str.title)
#         url_out = Join()
#
#         date_in = MapCompose(str.strip)
#         url_out = Join()
#
# @dataclass
# class Article(Item):
#     url: Optional[str] = field(default=None)
#     id: Optional[int] = field(default=None)
#     title: Optional[str] = field(default=None)
#     date: Optional[str] = field(default=None)
#     author: Optional[str] = field(default=None)
#     lang: Optional[str] = field(default=None)
#     size: Optional[str] = field(default=None)
#     cat: Optional[str] = field(default=None)
#     desc: Optional[str] = field(default=None)
#     links: Optional[str] = field(default=None)


class Article(Item):
    url = Field()
    id = Field()
    title = Field()
    date = Field()
    author = Field()
    lang = Field()
    size = Field()
    cat = Field()
    desc = Field()
    links = Field()


class RapidgatorPipeline:
    """ remove all not rapidgator links """
    DOMAIN = "rapidgator.net"

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        if adapter.get('links'):
            adapter['links'] = list(filter(lambda x: self.DOMAIN in x, adapter['links']))
            return item
        else:
            raise DropItem("Missing rapidgator link %s" % item)


class UdemyBlackListPipeline:
    """ remove Udemy tutorial """
    BLACKLIST = "UDEMY"

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        if adapter.get('title'):
            if self.BLACKLIST.upper() in adapter['title'].upper():
                spider.logger.debug("### Ignoring Udemy Tutoial: {}".format(item))
                raise DropItem("Ignoring {} Tutorial: \t {}".format(self.BLACKLIST, item['title']))
            else:
                return item


class PoliteLogFormatter(logformatter.LogFormatter):
    def dropped(self, item, exception, response, spider):
        return {
            # 'level': logging.DEBUG,
            # 'msg': logformatter.DROPPEDMSG,
            'level': logging.INFO,
            'msg': "Dropped: {} :".format(exception),
            'exception': exception,
            'args': {
                'exception': exception,
                'item': item,
            }
        }


if __name__ == "__main__":
    process = CrawlerProcess(
        {'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'}
    )
    process.crawl(LearningDLSpider)
    # the script will block here until the crawling is finished
    process.start()
