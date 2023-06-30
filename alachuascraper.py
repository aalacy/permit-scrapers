import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.crawler import CrawlerProcess
from scrapy.http import FormRequest
from scrapy.selector import Selector
import selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import Firefox, Chrome, ChromeOptions, FirefoxProfile
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.support.ui import Select
import os
import pdb
import requests
import json
import time
import urllib.parse as urlparse

from util import Util
from logger import logger

myutil = Util('Alachua')

class AlachuaSpider(CrawlSpider):
	name = 'Alachua'
	allowed_domains = ["www.citizenserve.com"]
	page = 1

	meta = {
		"proxy": "95.211.175.167:13150"
	}
	
	base_url = 'https://www.citizenserve.com/Portal/PortalController?Action=showSearchPage&ctzPagePrefix=Portal_&installationID=318&original_iid=0&original_contactID=0'
	basedir = os.path.abspath(os.path.dirname(__file__))

	def __init__(self):
		super(AlachuaSpider, self).__init__()

		options = FirefoxOptions()
		profile = FirefoxProfile()
		options.add_argument('--no-sandbox')
		options.add_argument('--disable-dev-shm-usage')
		options.add_argument('--headless')
		profile.set_preference("permissions.default.image", 2)
		path = f"{self.basedir}/data/geckodriver"
		firefox = Firefox(executable_path=path, options=options, firefox_profile=profile)

		self.driver = firefox

	def get_headers(self):
		return {
			'user-agent': myutil._get_ua(),
			'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
			'accept-language': 'en-US,en;q=0.5',
			'accept-encoding': 'gzip, deflate, br',
			'referer': 'https://www2.citizenserve.com/Portal/PortalController',
		}

	def start_requests(self):
		request = scrapy.Request(url=self.base_url, callback=self.parse)
		request.meta['url'] = self.base_url
		yield request

	def parse(self, response):
		try:
			url = response.meta['url']
			if url:
				self.driver.get(url)
				Select(self.driver.find_element_by_id('filetype')).select_by_value('Permit')
				WebDriverWait(self.driver, 3).until(EC.presence_of_element_located((By.XPATH,'//div[@id="submitRow"]//button'))).click()
				time.sleep(3)

			# pagination
			no_result = False
			response = None
			while True:
				time.sleep(1)
				# no result
				response = Selector(text=self.driver.page_source)
				class_name = response.xpath('//span[@id="waitTillRecords"]/following-sibling::a[last()]//i/@class').get()
				if class_name and class_name.strip() == 'icon-arrow-left':
					no_result = True
					break

				# check <a> tag
				links = response.xpath("//div[@id='resultContent']//table//tbody/tr")
				print(len(links), ' links ---------')
				if links:
					break

			# details
			headers = myutil._strip_list(response.xpath("//div[@id='resultContent']//table//thead/tr/th/text()").getall())
			trs = response.xpath("//div[@id='resultContent']//table//tbody/tr")
			res = []
			for tr in trs:
				values = myutil.strip_list1([el.css('::text').get() for el in tr.xpath('.//td')])
				item = {
					'permit_city': self.name,
				}
				item.update(dict(zip(headers, values)))
				myutil._normalizeKeys(item)
				res.append(item)

			if res:
				myutil._save_to_mongo_bulk(data=res)

			# click next page
			WebDriverWait(self.driver, 3).until(EC.presence_of_element_located((By.XPATH,'//span[@id="waitTillRecords"]/following-sibling::a[last()]'))).click()
			time.sleep(1)

			if no_result:
				self.driver.close()
				print('=====finished')
				return
			else:
				# pass
				request = scrapy.Request(url=self.base_url, dont_filter=True,  callback=self.parse)
				request.meta['url'] = ''
				yield request

		except Exception as err:
			logger.warning(str(err))

	def parse_detail(self, response):
		# summary
		summary = myutil._strip_list(response.xpath("//div[@class='configspace']/div[@class='row'][1]//font[1]//text()").getall())
		keys = [] 
		values = [] 
		for i in range(0, len(summary)): 
			if i % 2: 
				values.append(summary[i]) 
			else : 
				keys.append(summary[i])

		item = dict(zip(keys, values))
		item['detail_link'] = response.url
		item['permit_city'] = self.name

		# permit tab
		keys = myutil._strip_list(response.xpath('//div[@id="permit"]//div[@class="col-md-3"]//font//text()').getall())[1:]
		values = myutil.strip_list1(response.xpath('//div[@id="permit"]//div[@class="col-md-9"]//font//text()').getall())[1:]
		item['permit'] = dict(zip(keys, values))
		permit_number = myutil._valid(response.xpath('//div[@id="permit"]//div[@class="row"][1]//div[@class="col-md-9"]//font//text()').get())
		item['permit'].update({ 'permit #:': permit_number })

		# go to reviews tab
		params = response.url.split('?')[1].split('&')[1:]
		url = f"{response.url.split('?')[0]}?Action=listPermitReview&{'&'.join(params)}"
		request = scrapy.Request(url=url, headers=self.get_headers(), callback=self.parse_review)
		request.meta['item'] = item
		yield request

	def parse_review(self, response):
		item = response.meta['item']
		item['reviews'] = myutil.parse_table_tab('reviews', response)

		# go to documents
		params = response.url.split('?')[1].split('&')[1:]
		url = f"{response.url.split('?')[0]}?Action=listDocuments&{'&'.join(params)}"
		request = scrapy.Request(url=url, headers=self.get_headers(), callback=self.parse_documents)
		request.meta['item'] = item
		yield request

	def parse_documents(self, response):
		item = response.meta['item']

		item['documents'] = myutil.parse_table_tab('documents', response)

		# go to inspections
		params = response.url.split('?')[1].split('&')[1:]
		url = f"{response.url.split('?')[0]}?Action=listInspections&{'&'.join(params)}"
		request = scrapy.Request(url=url, headers=self.get_headers(), callback=self.parse_inspections)
		request.meta['item'] = item
		yield request

	def parse_inspections(self, response):
		item = response.meta['item']
		item['inspections'] = myutil.parse_table_tab('inspections', response)

		myutil._normalizeKeys(item)

		myutil._save_to_mongo(data=item)

if __name__ == '__main__':
	c = CrawlerProcess({
		'USER-AGENT': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1',
		'DOWNLOAD_DELAY': '.4',
		'COOKIES_ENABLED': 'true',
		'CONCURRENT_REQUESTS_PER_DOMAIN': '30',
		'CONCURRENT_REQUESTS_PER_IP': '30',
		'DOWNLOADER_MIDDLEWARES': {
			'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
		}
	})
	c.crawl(AlachuaSpider)
	c.start()