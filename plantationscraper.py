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

myutil = Util('Broward', 'Plantation')

class PlantationSpider(CrawlSpider):
	name = 'Plantation'
	allowed_domains = ["aca.plantation.org"]
	page = 1

	meta = {
		"proxy": "37.48.118.90:13042"
	}
	
	base_url = 'https://aca.plantation.org/CitizenAccess/Cap/CapHome.aspx?module=Building&TabName=Building&TabList=Home%7C0%7CBuilding%7C1%7CLandscape%7C2%7CEnforcement%7C3%7CPlanning%7C4%7CCityClerk%7C5%7CCurrentTabIndex%7C1'
	
	def __init__(self):
		super(PlantationSpider, self).__init__()		

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
			'origin': 'https://aca.plantation.org',
			'referer': self.base_url,
			'content-type': 'application/x-www-form-urlencoded; charset=utf-8',
		}

	def parse_pagination(self, response):
		page = int(response.meta['page'])
		trs = response.xpath("//table[@id='ctl00_PlaceHolderMain_dgvPermitList_gdvPermitList']//tr")
		if trs:
			headers = myutil._strip_list(trs[2].xpath(".//text()").extract())
			for tr in trs[3:-3]:
				values = myutil._strip_list(tr.xpath(".//text()").extract())
				item = dict(zip(headers, values))
				detail_link = f'https://aca.plantation.org{tr.xpath(".//a/@href").get()}'
				item['detail_link'] = detail_link

				try:
					res = requests.get(detail_link)
					yield self.parse_detail(Selector(text=res.text), item)
				except Exception as err:
					logger.warning(f'{err}{detail_link}')

		if page > 1 and len(trs) < 2:
			logger.warning(f'end page {page}')
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
		
		if page < 10:
			_page = f'0{page+1}'
		else:
			_page = page+1

		if page == 1:
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

		try:
			request = FormRequest(self.base_url, formdata=formdata, headers=headers, callback=self.parse_pagination)
			request.meta['page'] = page + 1
			yield request
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

	def parse_detail(self, response, item):
		# item = response.meta['item']

		work_location = ' '.join(myutil._strip_list(response.xpath("//table[@id='tbl_worklocation']//text()").extract()))
		item['work_location']: work_location

		# record detail
		record_details = []
		tds = response.xpath("//table[@id='ctl00_PlaceHolderMain_PermitDetailList1_TBPermitDetailTest']//td[@class='td_parent_left']")
		for td in tds:
			name = myutil._valid(td.xpath(".//h1//text()").get())
			value = ''.join(myutil._strip_list(td.xpath(".//table//td//text()").extract()))
			record_details.append({
				name: value
			})

		# more details
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

		record_details.append({
			'application_info': app_info
		})

		# parcel info
		keys = response.xpath("//span[@id='ctl00_PlaceHolderMain_PermitDetailList1_tbParcelList']//table//tr[@id='trParcelList']//div[contains(@class, 'MoreDetail_ItemCol')]//h2//text()").extract()
		values = response.xpath("//span[@id='ctl00_PlaceHolderMain_PermitDetailList1_tbParcelList']//table//tr[@id='trParcelList']//div[contains(@class, 'MoreDetail_ItemCol')]//div//text()").extract()
		parcel_info = dict(zip(keys, values))

		record_details.append({
			'parcel_info': parcel_info
		})

		item['record_details'] = record_details

		item['permit_city'] = self.name

		myutil._normalizeKeys(item)

		myutil._save_to_mongo(data=item)

if __name__ == '__main__':
	c = CrawlerProcess({
		'USER_AGENT': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1',
		'DOWNLOAD_DELAY': '.4',
		'COOKIES_ENABLED': 'true',
		'CONCURRENT_REQUESTS_PER_DOMAIN': '30',
		'CONCURRENT_REQUESTS_PER_IP': '30',
		'DOWNLOADER_MIDDLEWARES': {
			'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
		}
	})
	c.crawl(PlantationSpider)
	c.start()