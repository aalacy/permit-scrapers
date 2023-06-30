import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.crawler import CrawlerProcess
from scrapy.http import FormRequest
import os
import pdb
import requests
import json

from util import Util
from logger import logger

myutil = Util('Broward', 'Pembroke_Pines')

class PembrokePinesSpider(CrawlSpider):
	name = 'Pembroke Pines'
	allowed_domains = ["ppines.com"]
	page = 0
	page_cnt = -1

	# handle_httpstatus_list = [500]

	meta = {
		"proxy": "37.48.118.90:13042"
	}
	
	base_url = 'https://services.ppines.com/jobsearch/JobSearch2.aspx'

	def __init__(self):
		super(PembrokePinesSpider, self).__init__()		

		self.proxies = {
			'http': "37.48.118.90:13042",
			'https': "83.149.70.159:13042"
		}

	def get_headers(self):
		return {
			'user-agent': myutil._get_ua(),
			'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
			'accept-language': 'en-US,en;q=0.5',
			'accept-encoding': 'gzip, deflate, br',
			'origin': 'https://services.ppines.com',
			'referer': 'https://services.ppines.com/jobsearch/JobSearch2.aspx',
			'content-type': 'application/x-www-form-urlencoded',
			'upgrade-insecure-requests': '1'
		}

	def start_requests(self):
		request = scrapy.Request(url=self.base_url, dont_filter=True, headers=self.get_headers(), callback=self.parse_pagination)
		request.meta['page'] = 0
		yield request

	def parse_pagination(self, response):
		page = int(response.meta['page'])
		_page = ''
		if page < 10:
			_page = f'0{page}'
		elif page % 10 == 0:
			_page = '10'
		elif page > 10 and page % 10 != 0:
			_page = f'0{page % 10}'

		trs = response.xpath("//table[@id='DataGrid1']//tr")
		print(len(trs), 'pagination ============ page', page)
		if trs:
			headers = myutil._strip_list(trs[0].xpath(".//text()").extract())
			for tr in trs[1:-1]:
				values = myutil._strip_list(tr.xpath(".//text()").extract())
				appnum = tr.xpath(".//a/text()").get()
				url = f'https://services.ppines.com/fire/building/InspectSchedule/bzpublic/planreviewstart.aspx?appnum={appnum}'
				request = scrapy.Request(url=url, dont_filter=True, headers=self.get_headers(), callback=self.parse_detail)
				request.meta['item'] = dict(zip(headers, values))

				try:
					yield request
				except Exception as err:
					logger.warning(f'======= error {str(err)}')

		if page > 0:
			# logger.info(f'=====**********======= PAGE {page}')
			cnt = response.xpath('//span[@id="lblmess"]/text()').get()
			if cnt:
				try:
					total_cnt = cnt.split('Search')[0].strip()
					self.page_cnt = int(int(total_cnt)/200) + 1
				except:
					pass
			if page > self.page_cnt:
				print('>>>>>>>>>>> no more pagination')
				return

		EVENTTARGET = f'DataGrid1$ctl204$ctl{_page}'
		VIEWSTATE = myutil._valid(myutil._get_viewstate(response))
		EVENTVALIDATION = myutil._valid(myutil._get_eventvalidation(response))	
		VIEWSTATEGENERATOR = myutil._valid(myutil._get_viewstategenerator(response))	
		formdata = {
			'__EVENTARGUMENT': '',
			'__VIEWSTATE': VIEWSTATE,
			'__EVENTVALIDATION': EVENTVALIDATION,
			'__VIEWSTATEGENERATOR': VIEWSTATEGENERATOR,
			'housnumtxbx': '',
			'ddlstrdir': "",
			'strnamtxbx': '',
			'ddlstrtyp': '',
			'unittxbx': '',
			'ddlhousnum': "S",
			'ddlstrnam': "C",
			'ddlunit': "S",
			'ddlorder': "Address",
			'ddlmax': '0'
		}

		if page == 0:
			formdata['UtilSearch'] = "Start+Search"
		else:
			formdata['__EVENTTARGET'] = EVENTTARGET

		try:
			request = FormRequest(url=self.base_url, dont_filter=True, formdata=formdata, headers=self.get_headers(), callback=self.parse_pagination)
			request.meta['page'] = page + 1
			yield request
		except Exception as err:
			logger.warning(str(err))
		
	def parse_detail(self, response):
		item = response.meta['item']

		# job
		item['job_info'] = myutil._parse_table_with_tbody(response, 'DataGrid4')
		
		# plan review
		item['plan_review'] = myutil._parse_table_with_tbody(response, 'DataGrid2')

		item['detail_url'] = response.url
		item['permit_city'] = self.name

		myutil._normalizeKeys(item)

		myutil._save_to_mongo(data=item)


if __name__ == '__main__':
	c = CrawlerProcess({
		'USER_AGENT': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.131 Safari/537.36',
		'DOWNLOAD_DELAY': '.4',
		'COOKIES_ENABLED': 'True',
		'CONCURRENT_REQUESTS_PER_DOMAIN': '30',
		'CONCURRENT_REQUESTS_PER_IP': '30',
		'DOWNLOADER_MIDDLEWARES': {
		    'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 400,
		}
	})
	c.crawl(PembrokePinesSpider)
	c.start()
