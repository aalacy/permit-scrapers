import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.crawler import CrawlerProcess
from scrapy.http import FormRequest
from scrapy.selector import Selector
import selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
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
import random
import urllib
import copy

from util import Util
from logger import logger

myutil = Util('Broward')


class BrowardSpider(CrawlSpider):
	name = 'Broward'
	allowed_domains = ["dpepp.broward.org"]
	permit_prefix = 10

	meta = {
		"proxy": "95.211.175.167:13150"
	}
	
	base_url = 'https://dpepp.broward.org/BCS/Default.aspx?PossePresentation=SearchForMasterPermit'
	
	basedir = os.path.abspath(os.path.dirname(__file__))

	def __init__(self):
		super(BrowardSpider, self).__init__()

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
			'referer': 'https://dpepp.broward.org/BCS/Default.aspx?PossePresentation=SearchForMasterPermit',
		}

	def my_send_keys(self, element, value):
		for a in value:
			time.sleep(random.randint(1,100) / 1000)
			element.send_keys(a)

	def start_requests(self):
		yield scrapy.Request(url=self.base_url, callback=self.parse)

	def parse(self, response):
		try:
			self.driver.get(self.base_url)
			search = WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.ID, 'PermitNumber_23768337_S0')))
			self.my_send_keys(search, str(self.permit_prefix))
			search.send_keys(Keys.RETURN)
			time.sleep(1)
			no_result = False
			response = None
			while True:
				time.sleep(1)
				# no result
				response = Selector(text=self.driver.page_source)
				img = response.xpath('//img[@id="errorImage"]')
				if img:
					no_result = True
					break

				# check <a> tag
				links = response.xpath('//div[@id="ctl00_cphPaneBand_pnlPaneBand"]/table//tr//a')
				logger.info('++ wait for page loading ++')
				print(len(links), ' links ---------')
				if links:
					break

			trs = response.xpath('//div[@id="ctl00_cphPaneBand_pnlPaneBand"]/table//tr')
			for tr in trs[1:]:
				href = myutil._valid(tr.xpath('.//td[1]//a/@href').get())
				permit_number = myutil._valid(tr.xpath('.//td[2]//text()').get())
				type = myutil._valid(tr.xpath('.//td[3]//text()').get())
				status = myutil._valid(tr.xpath('.//td[4]//text()').get())
				issue_date = myutil._valid(tr.xpath('.//td[5]//text()').get())
				contractor = myutil._valid(tr.xpath('.//td[6]//text()').get())
				item = dict(
					detail_link=href,
					permit_number=permit_number,
					type=type,
					status=status,
					issue_date=issue_date,
					contractor=contractor
				)
				request = scrapy.Request(url=href, headers=self.get_headers(), callback=self.parse_detail)
				request.meta['item'] = item
				yield request

			self.permit_prefix += 1
			time.sleep(1)

			if no_result:
				self.driver.close()
				return
			else:
				yield scrapy.Request(url=self.base_url, dont_filter=True,  callback=self.parse)
		except Exception as err:
			logger.warning(str(err))

	def parse_detail(self, response):
		tbodies = response.xpath('//div[@id="ctl00_cphPaneBand_pnlPaneBand"]//span/table//table/tbody')
		details = []
		for tbody in tbodies:
			permit_number = myutil._valid(tbody.xpath('.//td[2]//text()').get())
			permit = myutil._valid(tbody.xpath('.//td[3]//text()').get())
			type = myutil._valid(tbody.xpath('.//td[4]//text()').get())
			status = myutil._valid(tbody.xpath('.//td[5]//text()').get())
			issue_date = myutil._valid(tbody.xpath('.//td[6]//text()').get())
			contractor = myutil._valid(tbody.xpath('.//td[7]//text()').get())
			unpaid_fees = myutil._valid(tbody.xpath('.//td[8]//text()').get())
			select_to_pay = myutil._valid(tbody.xpath('.//td[9]//text()').get())
			details.append(
				dict(
					permit_number=permit_number,
					permit=permit,
					type=type,
					status=status,
					issue_date=issue_date,
					contractor=contractor,
					unpaid_fees=unpaid_fees,
					select_to_pay=select_to_pay
				)
			)

		item = copy.deepcopy(response.meta['item'])
		item['details'] = details
		item['permit_city'] = self.name

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
	c.crawl(BrowardSpider)
	c.start()