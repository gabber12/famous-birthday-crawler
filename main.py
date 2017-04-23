import asyncio
import aiohttp
import os
import sys
import bs4
import scrapy
from pymongo import MongoClient

sem = asyncio.Semaphore(5)
urls = []

def get_seed_urls():
    sem = asyncio.Semaphore(5)
    loop = asyncio.get_event_loop()
    f = asyncio.wait([fetch_random_profiles() for i in range(1000)])
    loop.run_until_complete(f)

@asyncio.coroutine
def get(*args, **kwargs):
    response = yield from aiohttp.request('GET', *args, **kwargs)
    return response.headers

async def append_url(url):
    f = open('hello.urls', 'a')
    f.write(url+"\n")
    f.close()

@asyncio.coroutine
def fetch_random_profiles():
    url = 'http://www.famousbirthdays.com/s/randomizer.php'
    with (yield from sem):
        page = yield from get(url, compress=True, allow_redirects=False)
        yield from append_url(page['Location'])

def init_seed_url():
    with open('hello.urls') as f:
        urls = f.readlines()
        return urls

# Scraper
class Spider(scrapy.Spider):
    name = 'birth'
    client = MongoClient(os.environ['DB_1_PORT_27017_TCP_ADDR'])
    start_urls = ['https://blog.scrapinghub.com']
    def start_requests(self):
        print("Started Seeding")
        get_seed_urls()
        print("Seeding complete - Urls ",len(urls))
        with open('hello.urls') as f:
            urls = f.readlines()
            for url in urls:
                yield scrapy.Request(url=url.strip(), callback=self.parse)
    def parse(self, response):
        res = {}
        res['url'] =  response.request.url
        res['full_name'] = response.css('h1::text').extract_first().strip()
        res['first_name'] = res['full_name'].split(" ")[0]
        res['popularity'] = response.css('.rank-no::text').extract_first().strip()[1]
        res['birthday'] = response.xpath('/html/body/div[1]/div[1]/div/div/div[3]/div/div/div[2]/div/div[1]/div/div[1]/div/a[1]/span[1]/text()').extract_first().strip()
        res['birthday'] += " "+response.xpath('/html/body/div[1]/div[1]/div/div/div[3]/div/div/div[2]/div/div[1]/div/div[1]/div/a[2]/text()').extract_first().strip()
        res['category'] = response.xpath('/html/body/div[1]/div[1]/div/div/div[3]/div/div/div[1]/div/div[1]/h1/div/a/text()').extract_first().strip()
        res['birth_place'] = "".join(response.xpath('/html/body/div[1]/div[1]/div/div/div[3]/div/div/div[2]/div/div[2]/div/div[2]/div/a/text()').extract()).strip()

        keys = response.css('.bio h2::text').extract()
        for key, val in zip(keys, response.css('.bio p::text').extract()):
            res[key] = val

        for anchors in response.css('.also-viewed .row a::attr(href)').extract():
            yield scrapy.Request(url = anchors, callback=self.parse)

        if self.client.bottr_web_1.bottr_db_1.profiles.find({'url':res['url']}).count() > 0:
            self.client.bottr_web_1.bottr_db_1.profiles.insert_one(res)
        yield res
