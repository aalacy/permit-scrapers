import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.crawler import CrawlerProcess
from scrapy.http import FormRequest
import os
import pdb

from util import Util
from logger import logger

myutil = Util('Broward', 'Lighthose_Point')

class LighthousePointSpider(CrawlSpider):
	name = 'Lighthouse Point'
	page = 1

	meta = {
		"proxy": "37.48.118.90:13042"
	}
	
	base_url = 'https://services.ppines.com/jobsearch/JobSearch2.aspx'

	def __init__(self):
		super(LighthousePointSpider, self).__init__()		

		self.proxies = {
			'http': "37.48.118.90:13042",
			'https': "83.149.70.159:13042"
		}

	def start_requests(self):
		request = scrapy.Request(url=self.base_url, callback=self.parse_type_pagination)
		request.meta['page'] = 1
		yield request

	def parse_type_pagination(self, response):
		# parse content
		page = int(response.meta['page'])
		logger.info(f'--- parse content page {page}')
		divs = response.xpath('//div[@class="search-result-item"]//a/@onclick').extract()
		for div in divs:
			_uuid = div.split('Detail/')[1].split("'")[0].strip()
			url = f'https://ci-lighthousepoint-fl.smartgovcommunity.com/PermittingPublic/PermitDetailPublic/Index/{_uuid}?_conv=1'

			yield scrapy.Request(url=url, callback=self.parse_detail)

		if page > 1 and not divs:
			return

		formdata = {
			"_conv": "1",
			"query": "",
			"search_listState": {"Filter":[{"$or":[{"$and":[{"key":"Module","op":"=","val":"Permitting"}]},{"$and":[{"key":"Module","op":"=","val":"Licensing"}]}]},{"key":"Status.ProcessState","op":"!=","val":"Cancelled"},{"key":"CaseType.PublicPortalSearchable","op":"=","val":"True"},{"key":"Occurrence","op":"=","val":"0"},{"$or":[{"key":"CaseNumber","op":"LIKE","val":"%"},{"key":"SiteAddress.Street1","op":"LIKE","val":"%"},{"$and":[{"key":"PrimaryContact.Contact.DisplayName","op":"LIKE","val":"%"},{"key":"CaseType.ContactPortalVisibility.IsPortalPublic","op":"=","val":"True"}]},{"$and":[{"key":"PrimaryContractor.Contact.DisplayName","op":"LIKE","val":"%"},{"key":"CaseType.ContactPortalVisibility.IsPortalPublic","op":"=","val":"True"}]}]}],"Sort":[{"key":"StatusDate","op":"DESC"}]},
			"_applicationSearchPage": str(page),
			"ILS-Ajax": "Y"
		}

		headers = {
			'user-agent': myutil._get_ua(),
			'accept': 'text/plain, */*; q=0.01',
			'accept-language': 'en-US,en;q=0.5',
			'referer': 'https://ci-lighthousepoint-fl.smartgovcommunity.com/ApplicationPublic/ApplicationSearch',
			'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
			'origin': 'https://ci-lighthousepoint-fl.smartgovcommunity.com'
		}
		url = 'https://ci-lighthousepoint-fl.smartgovcommunity.com/ApplicationPublic/ApplicationSearch/SearchPage'
		request = FormRequest(url=url, formdata=formdata, headers=headers, callback=self.parse_type_pagination)

		request.meta['page'] = page + 1
		yield request

	def parse_detail(self, response):
		logger.info('******** parse detail')
		try:
			# permit summary
			_title = myutil._strip_list(response.xpath("//div[@class='min-template-content-title']//text()").extract())
			_address = myutil._strip_list(response.xpath('//address//text()').extract())
			address = ' '.join(myutil._strip_list(_address[:2]))
			parcel_number = myutil._valid(_address[-1])
			status = myutil._valid(response.xpath("//div[@class='span1']/span[2]/text()").get())
			keys = myutil._strip_list(response.xpath("//div[contains(@class, 'span2 permit-date-container')]//div/span[1]/text()").extract())
			values = myutil._strip_list(response.xpath("//div[contains(@class, 'span2 permit-date-container')]//div/span[2]/text()").extract())
			date_info = dict(zip(keys, values))

			keys = myutil._strip_list(response.xpath("//div[contains(@class, 'span2 permit-inspections-container')]/div[contains(@class, 'span2 alert-label-centered')]/div/span[1]/text()").extract())
			values = myutil._strip_list(response.xpath("//div[contains(@class, 'span2 permit-inspections-container')]/div[contains(@class, 'span2 alert-label-centered')]/div/span[2]/text()").extract())
			summary_inspections = dict(zip(keys, values))

			total_fees = response.xpath("//div[contains(@class, 'permit-detail-payment-container')]/span[@class='dollars']/text()").get() + '.' + response.xpath("//div[contains(@class, 'permit-detail-payment-container')]/span[@class='cents']/text()").get()

			project = myutil._valid(response.xpath("//div[contains(@class, 'span9')]/text()").extract()[1])

			contacts = myutil._valid(' '.join(myutil._strip_list(response.xpath("//section[@id='contacts-section']//table[@class='permit-detail-table']//text()").extract())))

			trs = response.xpath("//section[@id='contractor-section']//table//tr")
			contractors = []
			for tr in trs:
				key = myutil._valid(tr.xpath(".//td[1]//text()").get())
				value = ' '.join(myutil._strip_list(tr.xpath(".//td[2]//text()").extract()))
				contractors.append({key: value})

			trs = response.xpath("//section[@id='parcel-section']//table//tbody//tr")
			parcels = []
			for tr in trs:
				parcel = myutil._valid(tr.xpath(".//td[1]//text()").get())
				owner = myutil._valid(tr.xpath(".//td[2]//text()").get())
				address = ' '.join(myutil._strip_list(tr.xpath(".//td[3]//text()").extract()))
				parcels.append({
					'parcel': parcel,
					'owner': owner,
					'address': address
				})

			submittals = myutil._parse_table_in_section(response, 'submittal-section')

			approval_steps = myutil._parse_table_in_section(response, 'steps-section')

			fees = myutil._parse_table_in_section(response, 'fees-section')

			inspections = self._parse_inspections(response)

			data = {
				'permit_city': self.name,
				'title': _title[0],
				'type': _title[1],
				'parcel_number': parcel_number,
				'address': address,
				'status': status,
				'date_info': date_info,
				'summary_inspections': summary_inspections,
				'total_fees': total_fees,
				'project': project,
				'contacts': contacts,
				'contractors': contractors,
				'parcels': parcels,
				'submittals': submittals,
				'approval_steps': approval_steps,
				'fees': fees,
				'inspections': inspections,
				'detail_url': response.url,
			}

			myutil._normalizeKeys(data)

			myutil._save_to_mongo(data=data)
		except Exception as E:
			logger.warning(str(E))

	def _parse_inspections(self, response):
		res = []
		try:
			table = response.xpath(f"//section[@id='inspection-section']//table")
			# headers
			headers = ['_', 'Completed On']
			
			# values
			value_trs = table.xpath('.//tr')[1:]
			for tr in value_trs:
				values = myutil._strip_list(tr.xpath('.//text()').extract())
				res.append(dict(zip(headers, values)))
		except Exception as E:
			logger.warning(str(E))

		return res


if __name__ == '__main__':
	c = CrawlerProcess({
		'USER_AGENT': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.131 Safari/537.36',
		'DOWNLOAD_DELAY': '.4',
		'COOKIES_ENABLED': 'True',
		'CONCURRENT_REQUESTS_PER_DOMAIN': '30',
		'CONCURRENT_REQUESTS_PER_IP': '30'
	})
	c.crawl(LighthousePointSpider)
	c.start()
