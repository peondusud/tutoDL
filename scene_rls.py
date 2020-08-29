#!/usr/bin/env python3
from scrapy import Spider, Request, Item, Field, logformatter
from scrapy.crawler import CrawlerProcess
# from scrapy.loader import ItemLoader
from scrapy.exceptions import CloseSpider, DropItem
from pathlib import Path
from datetime import datetime, timedelta
from itemadapter import ItemAdapter
from pprint import pprint


class SceneRlsSpider(Spider):
    name = "scene_rls"
    MAX_TIMEDELTA = timedelta(days=10)  #<<<<====CHANGE HERE
    start_urls = (
        'http://apps.scene-rls.net/?cat=51',
        'http://apps.scene-rls.net/?cat=52',
    )
    allowed_domains = ["scene-rls.net"]
    custom_settings = {
        'ITEM_PIPELINES': {
            'scene_rls.LearningPlatformBlackListPipeline': 10,
            'scene_rls.WarezGroupsFilterPipeline': 20,
            'scene_rls.KeyWordBlackListPipeline': 30,
            'scene_rls.RapidgatorPipeline': 80
        },
        'LOG_LEVEL': 'INFO',
        'LOG_FORMATTER': 'learningdl.PoliteLogFormatter',
        'ROBOTSTXT_OBEY': False,
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
                'fields': ["title", "links"],
            },
        }
    }

    def __init__(self, *args, **kwargs):
        super(SceneRlsSpider, self).__init__(*args, **kwargs)
        self.__now = datetime.now()
        self.logger.setLevel('INFO')
        # self.logger.setLevel('DEBUG')
        self.article = 0
        self.too_old_article = 0
        # 10 articles/page
        self.too_old_nb_limit = 20

    def start_requests(self):
        pprint(self.settings.__dict__)
        self.logger.info("### Start URLs: {}".format(self.start_urls))
        for url in self.start_urls:
            yield Request(url=url, callback=self.parse)

    def parse(self, response):
        """ parse articles """
        self.logger.debug("response.url : {}".format(response.url))
        for article in response.xpath('//div[@class="post"]'):
            tmp_date = article.xpath('div[@class="postContent"]/p[@style="text-align: center;"]//text()').re_first(r'Published on: (.+)')
            article_date = datetime.strptime(tmp_date, '%b %d, %Y @ %H:%M')
            delta = self.__now - article_date
            self.logger.debug("### ARTICLE DELTA TIME: {}".format(delta))
            if delta <= self.MAX_TIMEDELTA:
                item = Article()
                item['url'] = article.xpath('div[@class="postHeader"]/h2[@class="postTitle"]/a/@href').extract_first()
                item['id'] = response.xpath('//article//@class').re_first(r'post-(\d+)')
                item['title'] = article.xpath('div[@class="postHeader"]/h2[@class="postTitle"]/a/text()').extract_first()
                item['cat'] = response.xpath('div[@class="postHeader"]/div[class="postSubTitle"]/span[@class="postCategories"]/a/@rel').extract_first()
                item['size'] = article.xpath('div[@class="postContent"]/p[@style="text-align: center;"]//text()').re_first(r'([\.\d]+ \wB)')
                item['date'] = article_date
                item['links'] = article.xpath('div[@class="postContent"]/h2[@style="text-align: center;"]//a/@href').extract()
                item['tags'] = article.xpath('div[@class="postFooter"]/span[@class="postTags"]//a/text()').extract()
                # self.logger.info("### ARTICLE LINK: {}".format(item['title']))
                self.logger.debug("### ARTICLE LINK: {}".format(item))
                self.article += 1
                yield item
            else:
                self.too_old_article += 1
                if self.too_old_article > self.too_old_nb_limit:
                    raise CloseSpider('Too OLD Content, no need too check older post')

        next_page = response.xpath('//span[@id="olderEntries"]/a/@href').extract_first()
        self.logger.debug("### NEXT_PAGE URLs: {}".format(next_page))
        if next_page is not None:
            yield Request(next_page, callback=self.parse)


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
    tags = Field()
    links = Field()


class RapidgatorPipeline:
    """ remove all not rapidgator links """
    DOMAIN = "rapidgator.net"

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        if adapter.get('links'):
            adapter['links'] = list(filter(lambda x: self.DOMAIN in x, adapter['links']))
            spider.logger.info("{}\t{}".format(self.DOMAIN, adapter['title']))
            return item
        else:
            raise DropItem("Missing rapidgator link {}".format(item))


class LearningPlatformBlackListPipeline:
    """ remove remove tutorial from learning platform or that contains badkeyword """
    blacklist_learning_platform = ["Kelbyone", "Groove3", "Producertech", "CreativeLive", "Ask Video", "Udemy"]

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        if adapter.get('title'):
            for blacklistLearnPlatform in self.blacklist_learning_platform:
                if blacklistLearnPlatform.upper() in adapter['title'].upper():
                    raise DropItem("Ignoring {} Tutorial: \t{}".format(blacklistLearnPlatform, item['title']))
            return item


class KeyWordBlackListPipeline:
    """ remove remove tutorial from learning platform or that contains badkeyword """
    blacklist_keyword = ["keygen", "KeyMaker", "Incl Cracked", "Incl Keygen", "incl Patch", "registration code", "MULTILANGUAGE", "-ROBOTS", "-TRUMP", "StarryNight", "WAV-EXPANSION ", "ISO-SOFTiMAGE", "-DawgFather"]

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        if adapter.get('title'):
            for keyword in self.blacklist_keyword:
                if keyword.upper() in adapter['title'].upper():
                    raise DropItem("Ignoring {} keyword: \t{}".format(keyword, item['title']))
            return item


class WarezGroupsFilterPipeline:
    """ remove tutorial from certain warez groups """
    whitelst_wrz_grps = ["ADSR", "APoLLo", "BiFiSO", "BooKWoRM", "CONSORTiUM", "ELOHiM", "EXPANSION", "iLST", "iNKiSO", "JGTiSO", "KNiSO", "LiBRiCiDE", "NOLEDGE", "QUASAR", "REBAR", "RiDWARE", "RPBISO", "SKiLLUP", "SOFTiMAGE", "SoSISO" "STM", "TUTOR", "ViGOROUS", "XCODE", "XQZT", "ZH"]
    blklst_wrz_grps = ["3ARLY", "6581", "ACTiVATED", "ALiAS", "AMPED", "APEX", "B4tman", "BLiZZARD", "BRD", "CaviaR", "CiO", "CLASS", "CLC", "CODEX", "Cracked", "CRD", "CSE-V", "CYGiSO", "CYGNUS", "Darbujan", "DARKSiDERS", "DARKZER0", "DELiGHT", "DEViANCE", "DINOByTES", "DVN", "DVT", "ENGiNE", "EViLiSO", "EXPANSION", "F4CG", "FAiRLiGHT", "FALLEN", "FFF", "HOODLUM", "HR", "iNCiDENT", "iND", "LAXiTY", "LiGHTFORCE", "LND", "MAGNiTUDE", "MASCHiNE", "MAZE", "Mephisto", "NAViGON", "NGEN", "Orion", "OUTLAWS", "PARADOX", "PH", "Playable", "PLAZA", "QUARTEX", "R2R", "Ratiborus", "RAZOR", "Razor1911", "RELOADED", "rG", "rGPDA", "RINDVIEH", "RiTUEL", "RoZ", "SiMPLEX", "SKIDROW", "SOFTiMAGE", "SoSISO", "SSM", "SUXXORS", "TiNYiSO", "Unleashed", "V.R", "VACE", "VENOM", "ViTALiTY", "WEB0DAY", "WEBiSO", "WiiERD", "XFORCE"]

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        if adapter.get('title'):
            # warezFroup_regex = "^.+-(.*)$"
            tt = adapter['title'].split('-')[-1].strip()
            for wrzGrp_allow in self.whitelst_wrz_grps:
                if wrzGrp_allow in tt:
                    return item
            for wrzGrp_ban in self.blklst_wrz_grps:
                # if wrzGrp_ban in tt and wrzGrp_ban in adapter['title']:
                if wrzGrp_ban in adapter['title']:
                    spider.logger.debug("### Ignoring {} Tutoial: \t{}".format(wrzGrp_ban, item))
                    raise DropItem("Ignoring {} Tutorial: \t{}".format(wrzGrp_ban, item['title']))
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


def test():
    s = [
        ("A Cloud Guru Kubernetes Fundamentals-SKiLLUP", True),
        ("A Short Hike v1 7 7 Linux-rG", False),
        ("Addison Wesley Professional Continuous Encryption on AWS The DevSecOps on AWS Series REPACK-XCODE", True),
        ("Ask Video Arturia V 103 The Buchla Easel V Explored TUTORiAL-ADSR", True),
        ("Ask Video Behringer 101 DeepMind 12 Explained and Explored TUTORiAL-ADSR", True),
        ("CLOUD ACADEMY ALIBABA FUNDAMENTALS ELASTIC COMPUTE SERVICE ECS-STM", True),
        ("CLOUD ACADEMY CLASSES-STM", True),
        ("Combit Relationship Manager v10 0 Enterprise MULTILANGUAGE-CYGNUS", False),
        ("CreativeLive Sony A9 Fast Start TUTORIAL-SoSISO", True),
        ("Daz3D Sci-fi Police Officer Textures for Genesis 8 Males ISO-SOFTiMAGE", False),
        ("GRAPHISOFT ARCHICAD V24 3008 INT-ENGiNE", False),
        ("Gumroad HDR Caustics ISO-SOFTiMAGE", False),
        ("Head First PMP: A Learners Companion to Passing the Project Management Professional Exam 4th Edition PDF", True),
        ("INE CCIE Service Provider v5 Exam Review-iLST", True),
        ("Kaizen Software Asset Manager 2019 Enterprise Edition v3 1 1003 0 Incl Keygen-AMPED", False),
        ("Kelbyone Using Light to Bring Emotion into Your Images-BooKWoRM", True),
        ("Launcher v4 0 Multilanguage-LAXiTY", False),
        ("Linkedin Learning PHP for WordPress Online Class-ZH", True),
        ("MASTERCLASS – Brandon McMillan Teaches Dog Training 1080p WebRip 10Bit H265-DawgFather", True),
        ("McAfee Client Proxy v3 10 16TH BIRTHDAY-DVT", False),
        ("Pluralsight com Building APEX Applications with Different Data Formats-ELOHiM", True),
        ("Pluralsight com Getting Started with Software Development Using Cisco DevNet 2020-ELOHiM", True),
        ("Pluralsight Play By Play Everything You Always Wanted To Know About Salesforce Logs But Were Afraid To Ask-REBAR", True),
        ("PortSwigger Burp Suite Professional v2020 7-WEB0DAY", False),
        ("Serato DJ Pro Suite v2 3 8 CSE-V", False),
        ("Sitepoint com Learn Database and Security Techniques with PHP-iNKiSO", True),
        ("Sitepoint com Learn the Principles of Object-Oriented Programming in PHP-iNKiSO", True),
        ("Sitepoint com PHP and MySQL Programming Principles-iNKiSO", True),
        ("Skillshare How to use Ansible to automate deployment of ELK stack 7 x-XCODE", True),
        ("Skillshare NLP Master Guide To Achieving Extraordinary Results-ViGOROUS", True),
        ("Skillshare Zend Framework 3 for beginners Learn to master the PHP framework ZF3 to make web applications-XCODE", True),
        ("Soundbox Jackin House and Tech WAV-EXPANSION", False),
        ("StoneRivereLearning DevOps Fundamentals Gain Solid Understanding-CONSORTiUM", True),
        ("StudiolinkedVST Mediocre Kit Drum Pro Expansion-6581", False),
        ("Tornado Driver-DARKZER0", False),
        ("Udemy Learn Django 2 for Beginners BOOKWARE-SOFTiMAGE", True),
        ("WinImage Pro v10 0 Win64 Incl Keygen-FALLEN", False)
        ]

    a = WarezGroupsFilterPipeline()
    for line in iter(s.splitlines()):
        print(a.process_item({'title': line}))


if __name__ == "__main__":
    process = CrawlerProcess(
        {'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'}
    )
    process.crawl(SceneRlsSpider)
    # the script will block here until the crawling is finished
    process.start()
