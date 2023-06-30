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

myutil = Util('Broward', 'FortLauderdale')

class FortLauderdaleSpider(CrawlSpider):
	name = 'Fort Lauderdale'
	allowed_domains = ["aca-prod.accela.com"]
	page = 1

	meta = {
		"proxy": "37.48.118.90:13042"
	}
	
	domain = 'https://aca-prod.accela.com'
	base_url = 'https://aca-prod.accela.com/FTL/Cap/CapHome.aspx?ShowMyPermitList=Y&SearchType=ByPermit&module=Permits'
	
	def __init__(self):
		super(FortLauderdaleSpider, self).__init__()		

		self.proxies = {
			'http': "37.48.118.90:13042",
			'https': "83.149.70.159:13042"
		}

	def start_requests(self):
		headers = {'USER-AGENT': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1'}
		yield scrapy.Request(url=self.base_url, dont_filter=True, headers=headers, callback=self.parse_temp)

	def parse_temp(self, response):
		options = response.xpath('//select[@id="ctl00_PlaceHolderMain_generalSearchForm_ddlGSLicenseType"]/option/@value').extract()
		for option in options:
			if not option:
				continue

			headers = {'USER-AGENT': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1'}
			request = scrapy.Request(url=self.base_url, dont_filter=True, headers=headers, callback=self.parse_pagination)
			request.meta['option'] = option
			request.meta['page'] = 1
			yield request

	def parse_pagination(self, response):
		logger.info(f'=====*** {response.meta["option"]} page {response.meta["page"]} ***=====')
		page = int(response.meta['page'])
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
			"__VIEWSTATEENCRYPTED": "",
			"ctl00$HeaderNavigation$hdnShoppingCartItemNumber": "",
			"ctl00$HeaderNavigation$hdnShowReportLink": "N",
			"ctl00$PlaceHolderMain$addForMyPermits$collection": "rdoNewCollection",
			"ctl00$PlaceHolderMain$addForMyPermits$txtName": "name",
			"ctl00$PlaceHolderMain$addForMyPermits$txtDesc": "",
			"ctl00$PlaceHolderMain$generalSearchForm$txtGSPermitNumber": "",
			"ctl00$PlaceHolderMain$generalSearchForm$ctl00_PlaceHolderMain_generalSearchForm_txtGSPermitNumber_watermark_exd_ClientState": "",
			"ctl00$PlaceHolderMain$generalSearchForm$ddlGSPermitType": "",
			"ctl00$PlaceHolderMain$generalSearchForm$ddlGSLicenseType": response.meta['option'],
			"ctl00$PlaceHolderMain$generalSearchForm$txtGSLicenseNumber": "",
			"ctl00$PlaceHolderMain$generalSearchForm$txtGSFirstName": "",
			"ctl00$PlaceHolderMain$generalSearchForm$txtGSLastName": "",
			"ctl00$PlaceHolderMain$generalSearchForm$txtGSBusiName": "",
			"ctl00$PlaceHolderMain$generalSearchForm$txtGSNumber$ChildControl0": "",
			"ctl00$PlaceHolderMain$generalSearchForm$txtGSNumber$ctl00_PlaceHolderMain_generalSearchForm_txtGSNumber_ChildControl0_watermark_exd_ClientState": "",
			"ctl00$PlaceHolderMain$generalSearchForm$txtGSNumber$ChildControl1": "",
			"ctl00$PlaceHolderMain$generalSearchForm$txtGSNumber$ctl00_PlaceHolderMain_generalSearchForm_txtGSNumber_ChildControl1_watermark_exd_ClientState": "",
			"ctl00$PlaceHolderMain$generalSearchForm$ddlGSDirection": "",
			"ctl00$PlaceHolderMain$generalSearchForm$txtGSStreetName": "",
			"ctl00$PlaceHolderMain$generalSearchForm$ddlGSStreetSuffix": "",
			"ctl00$PlaceHolderMain$generalSearchForm$txtGSUnitNo": "",
			"ctl00$PlaceHolderMain$generalSearchForm$txtGSCity": "",
			"ctl00$PlaceHolderMain$generalSearchForm$ddlGSState$State1": "",
			"ctl00$PlaceHolderMain$generalSearchForm$txtGSAppZipSearchPermit": "",
			"ctl00$PlaceHolderMain$generalSearchForm$txtGSAppZipSearchPermit_ZipFromAA": "0",
			"ctl00$PlaceHolderMain$generalSearchForm$txtGSAppZipSearchPermit_zipMask": "",
			"ctl00$PlaceHolderMain$generalSearchForm$txtGSAppZipSearchPermit_ext_ClientState": "",
			"ctl00$PlaceHolderMain$generalSearchForm$txtGSParcelNo": "",
			"ctl00$PlaceHolderMain$hfASIExpanded": "",
			"ctl00$PlaceHolderMain$txtHiddenDate": "",
			"ctl00$PlaceHolderMain$txtHiddenDate_ext_ClientState": "",
			"ctl00$PlaceHolderMain$hfGridId": "",
			"ctl00$HDExpressionParam": "",
			"Submit": "Submit",
			"__ASYNCPOST": "true",
		}

		headers = {
			'user-agent': myutil._get_ua(),
			'accept': '*/*',
			'accept-language': 'en-US,en;q=0.5',
			'accept-encoding': 'gzip, deflate, br',
			'origin': 'https://aca-prod.accela.com',
			'referer': self.base_url,
			'Sec-Fetch-Dest': 'empty',
			'Sec-Fetch-Mode': 'cors',
			'Sec-Fetch-Site': 'same-origin',
			'content-type': 'application/x-www-form-urlencoded; charset=utf-8',
			'cookie': ' ACA_USER_PREFERRED_CULTURE=en-US; ACA_COOKIE_SUPPORT_ACCESSSIBILITY=False; _pendo_accountId.08c27448-9075-481d-584f-0c00aac03d50=WESTON; _pendo_visitorId.08c27448-9075-481d-584f-0c00aac03d50=31BCA02094EB78126A517B206A88C73CFA9EC6F704C7030D18212CACE820F025F00BF0EA68DBF3F3A5436CA63B53BF7BF80AD8D5DE7D8359D0B7FED9DBC3AB99; _pendo_meta.08c27448-9075-481d-584f-0c00aac03d50=1655508987; LASTEST_REQUEST_TIME=1606471192271; .ASPXANONYMOUS=677tppTQ_k7oiVJ5XpBiQDPneHOI89iDg7H_aIlZurHfG2W6GOV96wjpx4nidaw8G5EoaHlX12mJKL2Dnvo-tA_EUkcQdnOBK9XreZ2sl8GSSLijiS1OQocQzM1j0_W6NDHiI-OCWgEw5ZzyT6L87T1mqjQH1ZrE8w703BTYTX_DwfL1tg8SKJs7wnqCKwIy0; ACA_SS_STORE=1e0cvj4f2jkb4ccclo0q5cg1; ACA_CS_KEY=9626b2394ca04e2cabf7ce1a8e06a040; ApplicationGatewayAffinity=35ba0760a33cd116cc9372b0d2fe1709aa8ae394d6c5ba58e596480d41a2c1e4; ApplicationGatewayAffinityCORS=35ba0760a33cd116cc9372b0d2fe1709aa8ae394d6c5ba58e596480d41a2c1e4'
		}

		try:
			request = FormRequest(self.base_url, formdata=formdata, headers=headers, callback=self.parse_pagination)
			request.meta['option'] = response.meta['option']
			request.meta['page'] = page+1
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
		logger.info('======= parse detail ==========')
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

		# valuation
		valuation = myutil._parse_keys_values_with_span(response, 'ctl00_PlaceHolderMain_PermitDetailList1_tbADIList')
		
		more_details.append({
			'valuation': valuation
		})

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
	c.crawl(FortLauderdaleSpider)
	c.start()
	