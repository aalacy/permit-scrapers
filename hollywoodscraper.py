import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.crawler import CrawlerProcess
from scrapy.http import FormRequest, JsonRequest
from scrapy.selector import Selector
from datetime import datetime
import os
import pdb
import requests
import json
from requests.utils import requote_uri

from util import Util
from logger import logger

myutil = Util('Broward', 'Hollywood')

class HollywoodSpider(CrawlSpider):
	name = 'Hollywood'
	allowed_domains = ["apps.hollywoodfl.org"]
	page = 1

	meta = {
		"proxy": "37.48.118.90:13042"
	}
	
	domain = 'https://apps.hollywoodfl.org'
	base_url = 'http://apps.hollywoodfl.org/building/PermitStatus.aspx'
	
	def __init__(self):
		super(HollywoodSpider, self).__init__()		

		self.proxies = {
			'http': "37.48.118.90:13042",
			'https': "83.149.70.159:13042"
		}

	def start_requests(self):
		headers = {'USER-AGENT': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1'}
		yield scrapy.Request(url=self.base_url, dont_filter=True, headers=headers, callback=self.show_neighbor)

	def show_neighbor(self, response):
		yield FormRequest(self.base_url, formdata=self.get_formdata(response), headers=self.get_headers(self.base_url), callback=self.parse_neighbor)

	def get_headers(self, url):
		return {
			'user-agent': myutil._get_ua(),
			'accept': '*/*',
			'accept-language': 'en-US,en;q=0.5',
			'accept-encoding': 'gzip, deflate, br',
			'origin': 'https://acaweb.brevardcounty.us',
			'referer': self.base_url,
			'content-type': 'application/x-www-form-urlencoded; charset=utf-8',
		}

	def get_formdata(self, response, event_target='lnkBtnNeighborhoodSearch', lstNeighborhoods1=''):
		EVENTARGUMENT = ''
		VIEWSTATE = myutil._valid(myutil._get_viewstate(response))
		VIEWSTATEGENERATOR = myutil._valid(myutil._get_viewstategenerator(response))
		EVENTVALIDATION = myutil._valid(myutil._get_eventvalidation(response))
		formdata = {
			'__EVENTTARGET': event_target,
			'__EVENTARGUMENT': EVENTARGUMENT,
			'__VIEWSTATE': VIEWSTATE,
			'__VIEWSTATEGENERATOR': VIEWSTATEGENERATOR,
			'__EVENTVALIDATION': EVENTVALIDATION,
		}
		if lstNeighborhoods1:
			formdata['lstNeighborhoods1'] = ''
		elif lstNeighborhoods1 != -1:
			formdata['lstNeighborhoods'] = ''
			formdata['txtAddress'] = ''

		return formdata

	def parse_event_target(self, _event_target):
		return _event_target.split("javascript:__doPostBack('")[1].split("'")[0]

	def parse_neighbor(self, response):
		options = response.xpath('//select[@id="lstNeighborhoods1"]/option/@value').extract()
		for option in options:
			if not option:
				continue

			formdata = self.get_formdata(response, event_target='cmdNeighborhood', lstNeighborhoods1=option)
			request = FormRequest(self.base_url, dont_filter=True, formdata=formdata, headers=self.get_headers(response.url), callback=self.parse_temp)
			request.meta['option'] = option

			yield request

	def parse_temp(self, response):
		keyword = '+'.join(response.meta['option'].split(' '))
		url = f'http://apps.hollywoodfl.org/building/psGatherParms.aspx?Neighborhood={keyword}'
		request = scrapy.Request(url=url, dont_filter=True, headers=self.get_headers(response.url), callback=self.parse_pagination)
		request.meta['page'] = 1
		request.meta['keyword'] = keyword
		yield request

	def parse_pagination(self, response):
		page = int(response.meta['page'])
		trs = response.xpath("//table[@id='dgPropertiesList']//tr")
		event_target = None
		if trs:
			href = trs[-1].xpath('.//a[2]/@href').get()
			if href:
				event_target = self.parse_event_target(href)
			headers = myutil._strip_list(trs[0].xpath(".//text()").extract())
			for tr in trs[1:-1]:
				values = myutil._strip_list(tr.xpath(".//text()").extract())
				item = dict(zip(headers, values))
				_event_target = self.parse_event_target(tr.xpath(".//a/@href").get())
				formdata = self.get_formdata(response, event_target=_event_target, lstNeighborhoods1=-1)
				if _event_target:
					try:
						request = FormRequest(response.url, dont_filter=True, formdata=formdata, headers=self.get_headers(response.url), callback=self.parse_temp_detail)
						request.meta['item'] = item
						yield request
					except Exception as err:
						pdb.set_trace()
						logger.warning(f'{err}{event_target}')
				else:
					myutil._save_to_mongo(item)

		if page > 1 and len(trs) < 2:
			logger.warning(f'end page {page}')
			return

		if event_target:
			formdata = self.get_formdata(response, event_target=event_target, lstNeighborhoods1=-1)
			headers = self.get_headers(response.url)

			request = FormRequest(url=response.url, dont_filter=True, headers=headers, formdata=formdata, callback=self.parse_pagination)
			request.meta['page'] = page+1
			yield request

	def parse_temp_detail(self, response):
		trs = response.xpath('//table[@id="dgPermitsList"]//tr')
		for tr in trs[1:]:
			href = tr.xpath('.//a/@href').get()
			if href:
				event_target = self.parse_event_target(href)
				formdata = self.get_formdata(response, event_target=event_target, lstNeighborhoods1=-1)
				headers = self.get_headers(response.url)
				process_number = myutil._valid(tr.xpath('.//td[2]//text()').get())
				permit_number = myutil._valid(tr.xpath('.//td[3]//text()').get())
				url = ''
				if process_number:
					url = f'http://apps.hollywoodfl.org/building/psApplData.aspx?ProcNum={process_number}'
				elif permit_number:
					url = f'http://apps.hollywoodfl.org/building/psApplData.aspx?PermNum={permit_number}'

				if url:
					request = FormRequest(url=url, dont_filter=True, headers=headers, formdata=formdata, callback=self.parse_detail)
					request.meta['item'] = response.meta['item']
					yield request

	def parse_detail(self, response):
		tables = response.xpath("//table[@id='tblSearchTabs']/following-sibling::table/tr/td/table")
		item = {}
		item['permit_city'] = self.name
		item['permit_details'] = myutil._parse_label_text(tables[0])
		item['site_information'] = myutil._parse_label_text(tables[1].xpath('.//tr[2]'))

		for table in tables[2:-1]:
			item.update(myutil._parse_label_text_arr(table))

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
	c.crawl(HollywoodSpider)
	c.start()
	