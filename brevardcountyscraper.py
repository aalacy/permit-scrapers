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

from util import Util
from logger import logger

myutil = Util('Brevard', 'Brevard')

class BrevardSpider(CrawlSpider):
	name = 'Brevard'
	allowed_domains = ["acaweb.brevardcounty.us"]
	page = 1

	meta = {
		"proxy": "37.48.118.90:13042"
	}
	
	domain = 'https://acaweb.brevardcounty.us'
	base_url = 'https://acaweb.brevardcounty.us/CitizenAccess/Cap/CapHome.aspx?module=Building&TabName=Building&TabList=HOME|0|Building|1|Development|2|Enforce|3|CurrentTabIndex|1'
	
	def __init__(self):
		super(BrevardSpider, self).__init__()		

		self.proxies = {
			'http': "37.48.118.90:13042",
			'https': "83.149.70.159:13042"
		}

	def start_requests(self):
		request = scrapy.Request(url=self.base_url, headers=self.get_headers(), callback=self.parse_pagination)
		request.meta['page'] = 1
		yield request

	def get_headers(self):
		return {
			'user-agent': myutil._get_ua(),
			'accept': '*/*',
			'accept-language': 'en-US,en;q=0.5',
			'accept-encoding': 'gzip, deflate, br',
			'origin': 'https://acaweb.brevardcounty.us',
			'referer': self.base_url,
			'content-type': 'application/x-www-form-urlencoded; charset=utf-8',
		}	

	def parse_pagination(self, response):
		trs = response.xpath("//table[@id='ctl00_PlaceHolderMain_dgvPermitList_gdvPermitList']//tr")
		if trs:
			headers = myutil._strip_list(trs[2].xpath(".//text()").extract())
			for tr in trs[3:-3]:
				values = myutil._strip_list(tr.xpath(".//text()").extract())
				item = dict(zip(headers, values))
				_link = tr.xpath(".//a/@href").get()
				detail_link = f'{self.domain}{_link}'
				item['detail_link'] = detail_link
				if _link:
					try:
						res = requests.get(detail_link)
						self.parse_detail(Selector(text=res.text), item)
					except Exception as err:
						logger.warning(f'{err}{detail_link}')
				else:
					myutil._save_to_mongo(data=item)

		if self.page > 1 and len(trs) < 2:
			logger.warning(f'end page {self.page}')
			return

		script_manager1 = ''
		EVENTTARGET = 'ctl00$PlaceHolderMain$btnNewSearch'
		ACA_CS_FIELD = myutil._get_ACA_CS_FIELD(response)
		VIEWSTATE = myutil._valid(myutil._get_viewstate(response))
		VIEWSTATEGENERATOR = myutil._valid(myutil._get_viewstategenerator(response))
		EVENTARGUMENT = ''
		LASTFOCUS = ''
		AjaxControlToolkitCalendarCssLoaded = ''
		end_date = datetime.now().strftime('%m/%d/%Y')
		
		if self.page < 10:
			_page = f'0{self.page+1}'
		else:
			_page = self.page+1

		if self.page == 1:
			script_manager1 = 'ctl00$PlaceHolderMain$updatePanel|ctl00$PlaceHolderMain$btnNewSearch'
		else:
			script_manager1 = f'ctl00$PlaceHolderMain$dgvPermitList$updatePanel|ctl00$PlaceHolderMain$dgvPermitList$gdvPermitList$ctl13$ctl{_page}'
			EVENTTARGET = f'ctl00$PlaceHolderMain$dgvPermitList$gdvPermitList$ctl13$ctl{_page}'
			try:
				hidden_txt = response.body.decode('utf8')
				VIEWSTATE = myutil._split_hidden_text(hidden_txt, '__VIEWSTATE')
				VIEWSTATEGENERATOR = myutil._split_hidden_text(hidden_txt, '__VIEWSTATEGENERATOR')
				ACA_CS_FIELD = myutil._split_hidden_text(hidden_txt, 'ACA_CS_FIELD')
			except:
				pass

		formdata = {
			"ctl00$ScriptManager1": script_manager1,
			"ACA_CS_FIELD": ACA_CS_FIELD,
			"__EVENTTARGET": EVENTTARGET,
			"__EVENTARGUMENT": EVENTARGUMENT,
			"__LASTFOCUS": LASTFOCUS,
			"__VIEWSTATE": VIEWSTATE,
			"__VIEWSTATEGENERATOR": VIEWSTATEGENERATOR,
			"txtSearchCondition": "Search...",
			"ctl00$HeaderNavigation$hdnShoppingCartItemNumber": "",
			"ctl00$HeaderNavigation$hdnShowReportLink": "Y",
			"ctl00$PlaceHolderMain$addForMyPermits$collection": "rdoNewCollection",
			"ctl00$PlaceHolderMain$addForMyPermits$txtName": "name",
			"ctl00$PlaceHolderMain$addForMyPermits$txtDesc": "",
			"ctl00$PlaceHolderMain$generalSearchForm$txtGSPermitNumber": "",
			"ctl00$PlaceHolderMain$generalSearchForm$txtGSProjectName": "",
			"ctl00$PlaceHolderMain$generalSearchForm$ddlGSPermitType": "",
			"ctl00$PlaceHolderMain$generalSearchForm$txtGSStartDate": "01/01/1986",
			"ctl00$PlaceHolderMain$generalSearchForm$txtGSStartDate_ext_ClientState": "",
			"ctl00$PlaceHolderMain$generalSearchForm$txtGSEndDate": end_date,
			"ctl00$PlaceHolderMain$generalSearchForm$txtGSEndDate_ext_ClientState": "",
			"ctl00$PlaceHolderMain$generalSearchForm$txtGSNumber$ChildControl0": "",
			"ctl00$PlaceHolderMain$generalSearchForm$txtGSNumber$ctl00_PlaceHolderMain_generalSearchForm_txtGSNumber_ChildControl0_watermark_exd_ClientState": "",
			"ctl00$PlaceHolderMain$generalSearchForm$txtGSNumber$ChildControl1": "",
			"ctl00$PlaceHolderMain$generalSearchForm$txtGSNumber$ctl00_PlaceHolderMain_generalSearchForm_txtGSNumber_ChildControl1_watermark_exd_ClientState": "",
			"ctl00$PlaceHolderMain$generalSearchForm$ddlGSDirection": "",
			"ctl00$PlaceHolderMain$generalSearchForm$txtGSStreetName": "",
			"ctl00$PlaceHolderMain$generalSearchForm$ddlGSStreetSuffix": "",
			"ctl00$PlaceHolderMain$generalSearchForm$txtGSUnitNo": "",
			"ctl00$PlaceHolderMain$generalSearchForm$txtGSParcelNo": "",
			"ctl00$PlaceHolderMain$generalSearchForm$ddlGSLicenseType": "",
			"ctl00$PlaceHolderMain$generalSearchForm$txtGSLicenseNumber": "",
			"ctl00$PlaceHolderMain$generalSearchForm$txtGSBusiName": "",
			"ctl00$PlaceHolderMain$hfASIExpanded": "",
			"ctl00$PlaceHolderMain$txtHiddenDate": "",
			"ctl00$PlaceHolderMain$txtHiddenDate_ext_ClientState": "",
			"ctl00$PlaceHolderMain$hfGridId": "",
			"ctl00$HDExpressionParam": "",
			"__ASYNCPOST": "true",
			"_get__AjaxControlToolkitCalendarCssLoaded": AjaxControlToolkitCalendarCssLoaded,
			"": ""
		}

		headers = self.get_headers()

		self.page += 1

		try:
			yield FormRequest(self.base_url, formdata=formdata, headers=headers, callback=self.parse_pagination)
		except Exception as err:
			logger.warning(err)

	def _build_app_info(self, names, keys, values):
		temp = []
		app_info = []
		_name = names.pop()
		for x in range(len(keys)):
			temp.append({ keys[x]: values[x] })
		app_info.append({
			_name: temp
		})

		return app_info

	def _parse_related_contact(self, response, span_id):
		detail = []
		try:
			tds = response.xpath(f"//span[@id='{span_id}']/table//td[@class='MoreDetail_BlockContent']")
			for td in tds:
				name = td.css("h2::text").get()
				arr = []
				spans = td.css("ul span")
				for span in spans:
					key = span.attrib['class']
					value = span.css('::text').get()
					if value:
						arr.append({
							key: value
						})

				divs = td.css('ul div')
				for div in divs:
					key = div.attrib['class']
					value = div.css('::text').get()
					if value:
						arr.append({
							key: value
						})

				if name:
					detail.append({
						name: arr
					})
		except Exception as err:
			logger.warning(str(err))
			
		return detail

	def _parse_app_info(self, response):
		blocks = response.xpath("//span[@id='ctl00_PlaceHolderMain_PermitDetailList1_tbASIList']/table//tr[@id='trASIList']/td[@class='MoreDetail_BlockContent']/div/div")
		names = []
		keys = []
		values = []
		app_info = []
		for block in blocks:
			name = block.css("div.MoreDetail_ItemTitle::text").get()
			if name:
				if names:
					app_info += self._build_app_info(names, keys, values)
					names = []
					keys = [] 
					values = []

				names.append(name)

			key = block.css("div.MoreDetail_ItemColASI.MoreDetail_ItemCol1 span::text").get()
			if key:
				keys.append(key)

			value = block.css("div.MoreDetail_ItemColASI.MoreDetail_ItemCol2 span::text").get()
			if value:
				values.append(value)

		# manage last name, key & value
		if names:
			app_info += self._build_app_info(names, keys, values)

		return app_info

	def _parse_additional_info_table(self, response):
		blocks = response.xpath(f"//span[@id='ctl00_PlaceHolderMain_PermitDetailList1_tbASITList']/table//td[@class='MoreDetail_BlockContent']//div[@class='MoreDetail_Item']/div")
		keys = response.xpath(f"//span[@id='ctl00_PlaceHolderMain_PermitDetailList1_tbASITList']/table//td[@class='MoreDetail_BlockContent']//div[@class='MoreDetail_Item']/div[contains(@class, 'MoreDetail_ItemCol1')]//text()").extract()
		values = response.xpath(f"//span[@id='ctl00_PlaceHolderMain_PermitDetailList1_tbASITList']/table//td[@class='MoreDetail_BlockContent']//div[@class='MoreDetail_Item']/div[contains(@class, 'MoreDetail_ItemCol2')]//text()").extract()
		return dict(zip(keys, values))

	def parse_detail(self, response, item):
		# record detail
		record_details = {}

		location = ' '.join(myutil._strip_list(response.xpath("//table[@id='tbl_worklocation']//text()").extract()))
		record_details['location'] = location

		application_details = []
		try:
			tds = response.xpath("//table[@id='ctl00_PlaceHolderMain_PermitDetailList1_TBPermitDetailTest']//td[@class='td_parent_left']")
			for td in tds:
				name = myutil._valid(td.xpath(".//h1//text()").get())
				value = ''.join(myutil._strip_list(td.xpath(".//table//td//text()").extract()))
				application_details.append({
					name: value
				})
		except Exception as err:
			logger.warning(str(err))

		record_details['application_details'] = application_details

		# more details
		more_details = []
		
		# related contacts
		related_contact = self._parse_related_contact(response, 'ctl00_PlaceHolderMain_PermitDetailList1_tbRCList')
		more_details.append({
			'related_contact': related_contact
		})

		# additional information
		additional_info = myutil._parse_keys_values_with_span(response, 'ctl00_PlaceHolderMain_PermitDetailList1_tbADIList')

		more_details.append({
			'additional_info': additional_info
		})

		app_info = self._parse_app_info(response)

		more_details.append({
			'application_info': app_info
		})

		# additional information table
		additional_info_table = self._parse_additional_info_table(response)
		more_details.append({
			'additional_info_table': additional_info_table
		})

		# parcel info
		parcel = myutil._parse_keys_values_with_div(response, 'ctl00_PlaceHolderMain_PermitDetailList1_tbParcelList')

		more_details.append({
			'parcel': parcel
		})

		record_details['more_details'] = more_details

		item['record_details'] = record_details
		item['permit_city'] = self.name

		try:
			myutil._normalizeKeys(item)
		except Exception as err:
			logger.warning(str(err))

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
	c.crawl(BrevardSpider)
	c.start()
	