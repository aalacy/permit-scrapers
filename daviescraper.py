import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.crawler import CrawlerProcess
from scrapy.http import FormRequest
import os
import requests
import base64
import json
from uuid import uuid1
from lxml import etree
import pdb

from util import Util
from logger import logger

myutil = Util('Broward', 'Davie')

class DavieSpider(CrawlSpider):
	name = 'Davie'

	meta = {
		"proxy": "37.48.118.90:13042"
	}
	
	base_url = 'https://esuite.davie-fl.gov/eSuite.Permits/AdvancedSearchPage/AdvancedSearch.aspx?permitNumber=&permitType=-1&serviceAddress='

	data = {}

	def __init__(self):
		super(DavieSpider, self).__init__()		

		self.session = requests.Session()
		self.proxies = {
			'http': "37.48.118.90:13042",
			'https': "83.149.70.159:13042"
		}
		# self.session.proxies = self.proxies
		self.session.mount('https://', requests.adapters.HTTPAdapter(pool_connections=1000000, max_retries=3))

	def start_requests(self):
		yield scrapy.Request(url=self.base_url, callback=self.parse_type)
		# yield scrapy.Request(url=url, callback=self.parse, meta=self.meta, headers=self.headers)

	def parse_type(self, response):
		options = response.xpath('//select[@id="ctl00_ctl00_Content_DefaultContent_ddlPermitType"]/option')
		for option in options:
			value = option.xpath('.//@value').extract_first()
			if value == '-1':
				continue

			name = option.xpath('.//text()').extract_first()
			url = f'https://esuite.davie-fl.gov/eSuite.Permits/AdvancedSearchPage/AdvancedSearch.aspx?permitNumber=&permitType={value}&serviceAddress='

			request = scrapy.Request(url=url, callback=self.parse_type_pagination)
			request.meta['type'] = value
			request.meta['name'] = name
			yield request

	def parse_type_pagination(self, response):
		pages = response.xpath("//div[@class='advancedSearchContainer']/a")
		for _page in pages:
			page = _page.xpath('.//text()').get()
			if not page.isnumeric():
				continue
			referer = self.base_url
			url = f'https://esuite.davie-fl.gov/eSuite.Permits/AdvancedSearchPage/AdvancedSearch.aspx?page={page}'
			EVENTTARGET = _page.xpath('.//@id').get()
			VIEWSTATE = myutil._get_viewstate(response)
			VIEWSTATEGENERATOR = myutil._get_viewstategenerator(response)
			PREVIOUSPAGE = myutil._get_previouspage(response)
			EVENTVALIDATION = myutil._get_eventvalidation(response)
			action = myutil._get_form_action(response)
			formdata = {
				'__EVENTTARGET': EVENTTARGET,
				'__EVENTARGUMENT': '',
				'__LASTFOCUS': '',
				'__VIEWSTATE': VIEWSTATE,
				'__VIEWSTATEGENERATOR': VIEWSTATEGENERATOR,
				'__PREVIOUSPAGE': PREVIOUSPAGE,
				'__EVENTVALIDATION': EVENTVALIDATION,
				'ctl00$ctl00$Content$DefaultContent$ddlPermitType': response.meta['type'],
				'ctl00$ctl00$Content$DefaultContent$txtPermitNumber': '',
				'ctl00$ctl00$Content$DefaultContent$txtServiceAddress': '',
				'ctl00$ctl00$Content$DefaultContent$txtServiceAddress_TextBoxWatermarkExtender_ClientState': '',
				'ctl00$ctl00$Content$DefaultContent$ddlNoOFRows': '50',
				'hiddenInputToUpdateATBuffer_CommonToolkitScripts': '1'
			}

			headers = {
				'user-agent': myutil._get_ua(),
				'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
				'accept-language': 'en-US,en;q=0.9',
				'referer': referer,
				'content-type': 'application/x-www-form-urlencoded',
				'origin': 'https://esuite.davie-fl.gov',
				'upgrade-insecure-requests': '1',
				'sec-fetch-dest': 'document',
				'sec-fetch-mode': 'navigate',
				'sec-fetch-site': 'same-origin',
				'sec-fetch-user': '?1'
			}
			request = FormRequest(url=url, formdata=formdata, headers=headers, callback=self.parse_content)
			request.meta['type'] = response.meta['type']
			request.meta['name'] = response.meta['name']
			yield request

	def parse_content(self, response):
		logger.info('--- parse content')
		# headers = myutil._strip_list(response.xpath('//table[@class="table-data"]/thead/tr//text()').extract())
		trs = response.xpath('//table[@class="table-data"]//tr')
		x = 0
		for tr in trs:
			if x == 0:
				x += 1
				continue
			permit_number = ' '.join(myutil._strip_list(tr.xpath('.//td[1]/div[@style="visibility:visible"]//text()').extract()))
			app_number = ' '.join(myutil._strip_list(tr.xpath('.//td[2]/div[@style="visibility:visible"]//text()').extract()))
			status = myutil._valid(tr.xpath('.//td[3]//text()').get())
			addr = myutil._valid(tr.xpath('.//td[4]//text()').get())

			_url = tr.xpath('.//td[1]/div[@style="visibility:visible"]/a/@href').get()
			if not _url:
				logger.warning(f'{response.url} has no detail')
				continue

			url = f'https://esuite.davie-fl.gov/eSuite.Permits{_url[2:]}'
			request = scrapy.Request(url=url, callback=self.parse_detail)
			request.meta['permit_number'] = permit_number
			request.meta['app_number'] = app_number
			request.meta['status'] = status
			request.meta['addr'] = addr
			request.meta['type'] = response.meta['type']
			request.meta['name'] = response.meta['name']

			yield request

	def parse_detail(self, response):
		logger.info('--- parse detail')
		# permit summary
		permit_summary = myutil._parse_summary_table(response, 'contractorPermitDetailsPermitSummary', 'Permit Summary')

		# Payment Summary
		payment_summary = myutil._parse_summary_table(response, 'contractorPermitDetailsPermitSummary', 'Payment Summary')

		# Location
		location = myutil._parse_summary_table(response, 'contractorPermitLocationSection', 'Location')

		# Permit details
		permit_details = myutil._parse_summary_table(response, 'contractorPermitDetailsSection', 'Permit Details')

		_type = '_'.join(myutil._strip_list(response.meta['name'].replace('-', '').split(' ')))
		data = {
			'permit_city': self.name,
			'type': _type,
			'permit_number': response.meta['permit_number'],
			'application_number': response.meta['app_number'],
			'status': response.meta['status'],
			'address': response.meta['addr'],
			'detail_url': response.url,
			'permit_summary': permit_summary,
			'payment_summary': payment_summary,
			'location': location,
			'permit_details': permit_details
		}

		myutil._normalizeKeys(data)

		myutil._save_to_mongo(data=data)


if __name__ == '__main__':
	c = CrawlerProcess({
		'USER_AGENT': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.131 Safari/537.36',
		'DOWNLOAD_DELAY': '.4',
		'COOKIES_ENABLED': 'True',
		'CONCURRENT_REQUESTS_PER_DOMAIN': '30',
		'CONCURRENT_REQUESTS_PER_IP': '30'
	})
	c.crawl(DavieSpider)
	c.start()
