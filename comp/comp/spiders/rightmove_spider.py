'''
Created on Sep 11, 2018
@author: kosta
'''
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.http.request import Request
from datetime import datetime, timedelta
import re
import time
import sys
import json
from string import ascii_lowercase
import requests
from xml.dom import minidom
import os


class Rightmove(scrapy.Spider):
    name = 'rightmove_spider'
    def __init__(self):
        
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
        url = 'https://www.rightmove.co.uk/sitemap_agents.xml'
        resp = requests.get(url, headers=headers)
        data = resp.text
        xmldoc = minidom.parseString(data)
        itemlist = xmldoc.getElementsByTagName('loc')
        file = open('agents.txt','w') 
        for item in itemlist:
           if item.firstChild:
               if str(item.firstChild.data).startswith('https://www.rightmove.co.uk/'):
                   file.write(item.firstChild.data.strip())
                   file.write('\n')
        file.close()
        with open('agents.txt') as f:
            self.start_urls = [one_line.replace('%0A','') if '%0A' in one_line else one_line.strip() for one_line in list(reversed(list(f.readlines()))) ]
            
    def parse(self, response):
        current_url = response.url
        check_microsite_resale = response.xpath('.//div[@id="resale-microsite"]')
        check_microsite_lettings = response.xpath('.//div[@id="lettings-microsite"]')
        microsite = False
        if check_microsite_resale:
            microsite = True
        elif check_microsite_lettings:
            microsite = True
            
        if 'https://www.rightmove.co.uk/estate-agents/' in response.url:
            if '-Lettings' in response.url:
                next_page_url = response.xpath('.//div[starts-with(@class, "propertyavailable clearfix ")]/a/@href').extract_first()
                if next_page_url:
                    yield scrapy.Request(next_page_url, callback = self.parse_one_page_for_rent, meta= {'microsite':microsite})
                buttonProperty = response.xpath('.//a[@id="view-properties-button"]/@href').extract_first()
                if buttonProperty:
                    next_page_url ='https://www.rightmove.co.uk'+ buttonProperty
                    yield scrapy.Request(next_page_url, callback = self.parse_one_page_for_rent, meta= {'microsite':microsite})
            else:
                property_urls = response.xpath('.//div[starts-with(@class, "propertyavailable clearfix ")]/h3/text()').extract()
                if property_urls:
                    if 'Properties for sale' and not 'Properties to rent' in property_urls:
                        next_page_url = response.xpath('.//div[starts-with(@class, "propertyavailable clearfix ")]/a/@href').extract_first()
                        yield scrapy.Request(next_page_url, callback=self.parse_one_page_for_sale, meta= {'microsite':microsite})
                    elif 'Properties to rent' and not 'Properties for sale' in property_urls:
                        next_page_url = response.xpath('.//div[starts-with(@class, "propertyavailable clearfix ")]/a/@href').extract_first()
                        yield scrapy.Request(next_page_url, callback=self.parse_one_page_for_rent, meta= {'microsite':microsite})
                    elif 'Properties for sale' and 'Properties to rent' in property_urls:
                        rent_url = response.xpath('.//div[@class="propertyavailable clearfix dual renting"]/a/@href').extract_first()
                        next_page_url = response.xpath('.//div[@class="propertyavailable clearfix dual buying"]/a/@href').extract_first()
                        yield scrapy.Request(next_page_url, callback=self.parse_one_page_for_sale, meta={'rent_url':rent_url, 'microsite':microsite})
                else:    
                    buttonProperty = response.xpath('.//a[@id="view-properties-button"]/@href').extract_first()
                    if buttonProperty:
                        next_page_url = 'https://www.rightmove.co.uk' + buttonProperty
                        yield scrapy.Request(next_page_url, callback = self.parse_one_page_for_sale, meta= {'microsite':microsite})
                    
        elif 'https://www.rightmove.co.uk/overseas-property/' in response.url:
            property_urls = response.xpath('.//div[starts-with(@class, "propertyavailable clearfix ")]/h3/text()').extract()
            if property_urls:
                next_page_url = response.xpath('.//div[starts-with(@class, "propertyavailable clearfix ")]/a/@href').extract_first()
                yield scrapy.Request(next_page_url, callback = self.parse_one_page_for_sale, meta={'over_url':response.url, 'microsite':microsite})
            
            else:
                next_page_url = response.url
                yield scrapy.Request(next_page_url, callback = self.parse_one_page_for_sale, meta={'over_url':response.url, 'microsite':microsite})
    
    def parse_one_page_for_sale(self, response):
        item = {}
        searchResult = response.xpath('.//h1[@class="searchTitle-heading"]/text()').extract_first()
        if searchResult:
            checkCategory = searchResult.split(',')[0]
            if 'Properties For Sale' in checkCategory:
                unique_name = checkCategory.split(' by')[1].strip() + ' '+ searchResult.split(',')[1].strip()
                number_sale = response.xpath('.//span[@class="searchHeader-resultCount"]/text()').extract_first()
                
                if 'over_url' in response.meta.keys():
                    item['type'] = 1
                else:
                    item['type'] = 0
                microsite = response.meta['microsite']
                item['microsite'] = microsite
                item['url'] = unique_name
                item['sale'] = int(number_sale.replace(',',''))
                if 'rent_url' in response.meta.keys():
                    rent = response.meta['rent_url']
                    yield scrapy.Request(rent, callback = self.parse_one_page_for_rent, meta = {'url':unique_name , 'sale': number_sale, 'microsite':microsite})
                else:
                    item['rent'] = 0
                    item['datetime'] = datetime.now()
                    agent_url = response.xpath('.//div[@id="searchSidebar-agentInformation"]/div/div/a/@href').extract_first()
                    if agent_url:
                        agent_url = 'https://www.rightmove.co.uk' + response.xpath('.//div[@id="searchSidebar-agentInformation"]/div/div/a/@href').extract_first()
                    else:
                        agent_url = ''
                    item['agent_url'] = agent_url.replace('#ram', '').strip()
                    yield item
            elif 'Commercial Properties For Sale' in checkCategory:
                if ' in ' in checkCategory:
                    unique_name = checkCategory.split(' in ')[1].strip()+' '+searchResult.split(',')[1].strip()
                else:
                    unique_name = checkCategory.split(' by ')[1].strip()+' ' +searchResult.split(',')[1].strip()
                number_sale = response.xpath('.//span[@class="searchHeader-resultCount"]/text()').extract_first()
                microsite = response.meta['microsite']
                item['microsite'] = microsite
                item['url'] = unique_name
                item['sale'] = int(number_sale.replace(',',''))
                if 'rent_url' in response.meta.keys():
                    rent = response.meta['rent_url']
                    yield scrapy.Request(rent, callback = self.parse_one_page_for_rent, meta = {'url':unique_name , 'sale': number_sale, 'microsite':microsite})
                else:
                    item['rent'] = 0
                    item['type'] = 2
                    item['datetime'] = datetime.now()
                    agent_url = response.xpath('.//div[@id="searchSidebar-agentInformation"]/div/div/a/@href').extract_first()
                    if agent_url:
                        agent_url = 'https://www.rightmove.co.uk' + response.xpath('.//div[@id="searchSidebar-agentInformation"]/div/div/a/@href').extract_first()
                    else:
                        agent_url = ''
                    item['agent_url'] = agent_url.replace('#ram', '').strip()
                    yield item
            
    def parse_one_page_for_rent(self, response):
        
        item = {}
        searchResult = response.xpath('.//h1[@class="searchTitle-heading"]/text()').extract_first()
        if searchResult:
            checkCategory = searchResult.split(',')[0]
            if 'Properties To Rent' in checkCategory:
                number_rent = response.xpath('.//span[@class="searchHeader-resultCount"]/text()').extract_first()
                if 'url' and 'sale' in response.meta.keys():
                    unique_name_meta = response.meta['url']
                    number_sale_meta = response.meta['sale']
                    microsite = response.meta['microsite']
                    item['microsite'] = microsite
                    item['url'] = unique_name_meta
                    item['sale'] = int(number_sale_meta.replace(',',''))
                    item['rent'] = int(number_rent.replace(',',''))
                    item['type'] = 0
                    item['datetime'] = datetime.now()
                    agent_url = response.xpath('.//div[@id="searchSidebar-agentInformation"]/div/div/a/@href').extract_first()
                    if agent_url:
                        agent_url = 'https://www.rightmove.co.uk' + response.xpath('.//div[@id="searchSidebar-agentInformation"]/div/div/a/@href').extract_first()
                    else:
                        agent_url = ''
                    item['agent_url'] = agent_url.replace('#ram', '').strip()
                    yield item
                else:
                    if ' in ' in checkCategory:
                        unique_name = checkCategory.split(' in ')[1].strip() +' '+ searchResult.split(',')[1].strip()
                    else:
                        unique_name = checkCategory.split(' by ')[1].strip() +' '+ searchResult.split(',')[1].strip()
                    microsite = response.meta['microsite']
                    item['microsite'] = microsite
                    item['type'] = 0
                    item['url'] = unique_name
                    item['sale'] = 0
                    item['rent'] = int(number_rent.replace(',',''))
                    item['datetime'] = datetime.now()
                    agent_url = response.xpath('.//div[@id="searchSidebar-agentInformation"]/div/div/a/@href').extract_first()
                    if agent_url:
                        agent_url = 'https://www.rightmove.co.uk' + response.xpath('.//div[@id="searchSidebar-agentInformation"]/div/div/a/@href').extract_first()
                    else:
                        agent_url = ''
                    item['agent_url'] = agent_url.replace('#ram', '').strip()
                    yield item
                    
            elif 'Commercial Properties To Let' in checkCategory:
                
                number_rent = response.xpath('.//span[@class="searchHeader-resultCount"]/text()').extract_first()
                if 'url' and 'sale' in response.meta.keys():
                    unique_name_meta = response.meta['url']
                    number_sale_meta = response.meta['sale']
                    microsite = response.meta['microsite']
                    item['microsite'] = microsite
                    item['url'] = unique_name_meta
                    item['sale'] = int(number_sale_meta)
                    item['rent'] = int(number_rent.replace(',',''))
                    item['type'] = 2
                    item['datetime'] = datetime.now()
                    agent_url = response.xpath('.//div[@id="searchSidebar-agentInformation"]/div/div/a/@href').extract_first()
                    if agent_url:
                        agent_url = 'https://www.rightmove.co.uk' + response.xpath('.//div[@id="searchSidebar-agentInformation"]/div/div/a/@href').extract_first()
                    else:
                        agent_url = ''
                    item['agent_url'] = agent_url.replace('#ram', '').strip()
                    yield item
                else:
                    if ' in ' in checkCategory:
                        unique_name = checkCategory.split(' in ')[1].strip() +' '+ searchResult.split(',')[1].strip()
                    else:
                        unique_name = checkCategory.split(' by ')[1].strip() +' '+ searchResult.split(',')[1].strip()
                    microsite = response.meta['microsite']
                    item['microsite'] = microsite
                    item['url'] = unique_name
                    item['sale'] = 0
                    item['rent'] = int(number_rent.replace(',',''))
                    item['type'] = 2
                    item['datetime'] = datetime.now()
                    agent_url = response.xpath('.//div[@id="searchSidebar-agentInformation"]/div/div/a/@href').extract_first()
                    if agent_url:
                        agent_url = 'https://www.rightmove.co.uk' + response.xpath('.//div[@id="searchSidebar-agentInformation"]/div/div/a/@href').extract_first()
                    else:
                        agent_url = ''
                    item['agent_url'] = agent_url.replace('#ram', '').strip()
                    yield item       
                
if __name__ == '__main__':
    process = CrawlerProcess()
    process.crawl(Rightmove)
    process.start()