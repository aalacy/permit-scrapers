import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.crawler import CrawlerProcess
from scrapy.http import FormRequest, JsonRequest
from scrapy.selector import Selector
import selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import Firefox, Chrome, ChromeOptions, FirefoxProfile
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.support.ui import Select
from datetime import datetime
import os
import pdb
import json
from requests.utils import requote_uri

from util import Util
from logger import logger

myutil = Util('Broward')

class LauderhillSpider(CrawlSpider):
	name = 'Lauderhill'
	allowed_domains = ["aca-prod.accela.com"]
	page = 1

	meta = {
		"proxy": "http://95.211.175.167:13150"
	}
	
	domain = 'https://aca-prod.accela.com'
	base_url = 'http://egov.lauderhill-fl.gov/eGovPlus83/permit/perm_status.aspx'
	basedir = os.path.abspath(os.path.dirname(__file__))
	
	def __init__(self):
		super(LauderhillSpider, self).__init__()		

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
			'accept-encoding': 'gzip, deflate',
			'origin': 'http://egov.lauderhill-fl.gov',
			'referer': self.base_url,
			'Upgrade-Insecure-Requests': '1',
			'content-type': 'application/x-www-form-urlencoded'
		}

	def get_formdata(self, option):
		return {
			"permit_no": "",
			"permtype": option,
			"parcel_id": "",
			"house_num": "",
			"street": "",
			"perm_status": "SEARCH"
		}

	def _parse_table(self, table):
		res = []
		try:
			labels = table.xpath('.//td[@class="tbl_hd"]/text()').getall()
			trs = table.xpath('.//tr')
			for tr in trs[1:]:
				data = myutil._strip_list(tr.xpath('.//td//text()').getall())
				if data:
					res.append(dict(zip(labels, data)))
		except Exception as err:
			logger.warning(str(err))

		return res

	def _parse_table1(self, table):
		res = []
		try:
			labels = table.xpath('.//th[@class="tbl_hd"]/text()').getall()
			trs = table.xpath('.//tr')
			for tr in trs:
				data = myutil._strip_list(tr.xpath('.//td//text()').getall())
				if data and data :
					res.append(dict(zip(labels, data)))
		except Exception as err:
			logger.warning(str(err))

		return res

	def start_requests(self):
		self.driver.get(self.base_url)
		WebDriverWait(self.driver, 5).until(EC.presence_of_element_located((By.XPATH,'//select[@name="permtype"]')))
		response = Selector(text=self.driver.page_source)
		options = response.xpath('//select[@name="permtype"]/option/@value').extract()
		for option in options:
			if not option:
				continue

			self.driver.get(self.base_url)
			WebDriverWait(self.driver, 5).until(EC.presence_of_element_located((By.XPATH,'//select[@name="permtype"]')))
			Select(self.driver.find_element_by_name('permtype')).select_by_value(option)
			WebDriverWait(self.driver, 5).until(EC.presence_of_element_located((By.NAME,'perm_status'))).click()
			WebDriverWait(self.driver, 5).until(EC.presence_of_element_located((By.XPATH,'//table[@class="search_results"]')))

			response = Selector(text=self.driver.page_source)

			links = response.xpath('//table[@class="search_results"]//td[1]/a[@class="search_dtl"]/@href').getall()
			for link in links:
				if link:
					url = f'http://egov.lauderhill-fl.gov/eGovPlus83/permit{link[1:]}'
					yield scrapy.Request(url=url, headers=self.get_headers(), callback=self.parse_detail, meta=self.meta)

		self.driver.close()

	def parse_detail(self, response):
		summary_table = response.xpath('//table[@class="page_body"]//table[@class="search_results"][1]')
		summary_title, item = myutil._parse_table_search_results(summary_table)

		# permit info
		permit_tables = response.xpath('//div[@id="permit_info"]//table[@class="search_results"]')
		for table in permit_tables:
			title, data = myutil._parse_table_search_results(table)
			item.update({ title: data })

		# plan reviews
		item['plan_info'] = self._parse_table(response.xpath(f'//div[@id="plan_info"]//table[@class="search_results"]'))

		# inspections
		item['inspection_info'] = self._parse_table1(response.xpath(f'//div[@id="inspection_info"]//table[@class="search_results"]'))

		# fees
		item['fee_info'] = self._parse_table1(response.xpath(f'//div[@id="fee_info"]//table[@id="DBStyleTable1"]'))
		item['fee_info'] += myutil._parse_table_search_results(response.xpath(f'//div[@id="fee_info"]//table[@class="search_results"]'))

		# contractors
		item['contractors'] = {}
		tables = response.xpath('//div[@id="contractor_info"]//table[@class="search_results" and not(@cellspacing="0")]')
		if len(tables) > 0:
			title, data = myutil._parse_table_search_results(tables[0])
			item['contractors']['general_contractor'] = data
		if len(tables) > 1:
			item['contractors']['subcontractors_professionals'] = self._parse_table1(tables[1].xpath('.//table[@class="search_results" and @cellspacing="0"]'))

		item['permit_city'] = self.name
		myutil._normalizeKeys(item)

		myutil._save_to_mongo(data=item)


if __name__ == '__main__':
	c = CrawlerProcess({
		'USER-AGENT': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1',
		'DOWNLOAD_DELAY': '3',
		'COOKIES_ENABLED': 'true',
		'CONCURRENT_REQUESTS_PER_DOMAIN': '10',
		'CONCURRENT_REQUESTS_PER_IP': '10',
		'DOWNLOADER_MIDDLEWARES': {
			'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
		}
	})
	c.crawl(LauderhillSpider)
	c.start()
	