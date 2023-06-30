import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.crawler import CrawlerProcess
from scrapy.http import FormRequest, JsonRequest
import os
import pdb
import requests
import json

from util import Util
from logger import logger

myutil = Util('Broward', 'sunrise')

class SunriseSpider(CrawlSpider):
	name = 'Sunrise'
	allowed_domains = ["energov.sunrisefl.gov"]
	page = 1

	meta = {
		"proxy": "37.48.118.90:13042"
	}
	
	base_url = 'https://energov.sunrisefl.gov/EnerGov_Prod/SelfService/	SunriseFL%20Prod#/search?m=2&ps=10&pn=1&em=True'
	
	page_url = 'https://energov.sunrisefl.gov/EnerGov_Prod/selfservice/api/energov/search/search'

	def __init__(self):
		super(SunriseSpider, self).__init__()		

		self.proxies = {
			'http': "37.48.118.90:13042",
			'https': "83.149.70.159:13042"
		}

	def start_requests(self):
		request = scrapy.Request(url=self.base_url, callback=self.parse_pagination)
		request.meta['page'] = 1
		yield request

	def build_payload(self, page):
		return {
				"Keyword":"",
				"ExactMatch":'true',
				"SearchModule":'1',
				"FilterModule":'2',
				"SearchMainAddress":'false',
				"PlanCriteria":{"PlanNumber":'null',"PlanTypeId":'null',"PlanWorkclassId":'null',"PlanStatusId":'null',"ProjectName":'null',"ApplyDateFrom":'null',"ApplyDateTo":'null',"ExpireDateFrom":'null',"ExpireDateTo":'null',"CompleteDateFrom":'null',"CompleteDateTo":'null',"Address":'null',"Description":'null',"SearchMainAddress":'false',"ContactId":'null',"ParcelNumber":'null',"TypeId":'null',"WorkClassIds":'null',"ExcludeCases":'null',"PageNumber":'0',"PageSize":'0',"SortBy":'null',"SortAscending":'false'},
				"PermitCriteria":{"PermitNumber":'null',"PermitTypeId":'null',"PermitWorkclassId":'null',"PermitStatusId":'null',"ProjectName":'null',"IssueDateFrom":'null',"IssueDateTo":'null',"Address":'null',"Description":'null',"ExpireDateFrom":'null',"ExpireDateTo":'null',"FinalDateFrom":'null',"FinalDateTo":'null',"ApplyDateFrom":'null',"ApplyDateTo":'null',"SearchMainAddress":'false',"ContactId":'null',"TypeId":'null',"WorkClassIds":'null',"ParcelNumber":'null',"ExcludeCases":'null',"PageNumber":'0',"PageSize":'0',"SortBy":"PermitNumber.keyword","SortAscending":'false'},
				"InspectionCriteria":{"Keyword":'null',"ExactMatch":'false',"Complete":'null',"InspectionNumber":'null',"InspectionTypeId":'null',"InspectionStatusId":'null',"RequestDateFrom":'null',"RequestDateTo":'null',"ScheduleDateFrom":'null',"ScheduleDateTo":'null',"Address":'null',"SearchMainAddress":'false',"ContactId":'null',"TypeId":[],"WorkClassIds":[],"ParcelNumber":'null',"DisplayCodeInspections":'false',"ExcludeCases":[],"ExcludeFilterModules":[],"PageNumber":'0',"PageSize":'0',"SortBy":'null',"SortAscending":'false'},
				"CodeCaseCriteria":{"CodeCaseNumber":'null',"CodeCaseTypeId":'null',"CodeCaseStatusId":'null',"ProjectName":'null',"OpenedDateFrom":'null',"OpenedDateTo":'null',"ClosedDateFrom":'null',"ClosedDateTo":'null',"Address":'null',"ParcelNumber":'null',"Description":'null',"SearchMainAddress":'false',"RequestId":'null',"ExcludeCases":'null',"ContactId":'null',"PageNumber":'0',"PageSize":'0',"SortBy":'null',"SortAscending":'false'},
				"RequestCriteria":{"RequestNumber":'null',"RequestTypeId":'null',"RequestStatusId":'null',"ProjectName":'null',"EnteredDateFrom":'null',"EnteredDateTo":'null',"DeadlineDateFrom":'null',"DeadlineDateTo":'null',"CompleteDateFrom":'null',"CompleteDateTo":'null',"Address":'null',"ParcelNumber":'null',"SearchMainAddress":'false',"PageNumber":'0',"PageSize":'0',"SortBy":'null',"SortAscending":'false'},
				"BusinessLicenseCriteria":{"LicenseNumber":'null',"LicenseTypeId":'null',"LicenseClassId":'null',"LicenseStatusId":'null',"BusinessStatusId":'null',"LicenseYear":'null',"ApplicationDateFrom":'null',"ApplicationDateTo":'null',"IssueDateFrom":'null',"IssueDateTo":'null',"ExpirationDateFrom":'null',"ExpirationDateTo":'null',"SearchMainAddress":'false',"CompanyTypeId":'null',"CompanyName":'null',"BusinessTypeId":'null',"Description":'null',"CompanyOpenedDateFrom":'null',"CompanyOpenedDateTo":'null',"CompanyClosedDateFrom":'null',"CompanyClosedDateTo":'null',"LastAuditDateFrom":'null',"LastAuditDateTo":'null',"ParcelNumber":'null',"Address":'null',"TaxID":'null',"DBA":'null',"ExcludeCases":'null',"TypeId":'null',"WorkClassIds":'null',"ContactId":'null',"PageNumber":'0',"PageSize":'0',"SortBy":'null',"SortAscending":'false'},
				"ProfessionalLicenseCriteria":{"LicenseNumber":'null',"HolderFirstName":'null',"HolderMiddleName":'null',"HolderLastName":'null',"HolderCompanyName":'null',"LicenseTypeId":'null',"LicenseClassId":'null',"LicenseStatusId":'null',"IssueDateFrom":'null',"IssueDateTo":'null',"ExpirationDateFrom":'null',"ExpirationDateTo":'null',"ApplicationDateFrom":'null',"ApplicationDateTo":'null',"Address":'null',"MainParcel":'null',"SearchMainAddress":'false',"ExcludeCases":'null',"TypeId":'null',"WorkClassIds":'null',"ContactId":'null',"PageNumber":'0',"PageSize":'0',"SortBy":'null',"SortAscending":'false'},
				"LicenseCriteria":{"LicenseNumber":'null',"LicenseTypeId":'null',"LicenseClassId":'null',"LicenseStatusId":'null',"BusinessStatusId":'null',"ApplicationDateFrom":'null',"ApplicationDateTo":'null',"IssueDateFrom":'null',"IssueDateTo":'null',"ExpirationDateFrom":'null',"ExpirationDateTo":'null',"SearchMainAddress":'false',"CompanyTypeId":'null',"CompanyName":'null',"BusinessTypeId":'null',"Description":'null',"CompanyOpenedDateFrom":'null',"CompanyOpenedDateTo":'null',"CompanyClosedDateFrom":'null',"CompanyClosedDateTo":'null',"LastAuditDateFrom":'null',"LastAuditDateTo":'null',"ParcelNumber":'null',"Address":'null',"TaxID":'null',"DBA":'null',"ExcludeCases":'null',"TypeId":'null',"WorkClassIds":'null',"ContactId":'null',"HolderFirstName":'null',"HolderMiddleName":'null',"HolderLastName":'null',"MainParcel":'null',"PageNumber":'0',"PageSize":'0',"SortBy":'null',"SortAscending":'false'},
				"PlanSortList":[{"Key":"relevance","Value":"Relevance"},{"Key":"PlanNumber.keyword","Value":"Plan Number"},{"Key":"ProjectName.keyword","Value":"Project"},{"Key":"MainAddress","Value":"Address"},{"Key":"ApplyDate","Value":"Apply Date"}],
				"PermitSortList":[{"Key":"relevance","Value":"Relevance"},{"Key":"PermitNumber.keyword","Value":"Permit Number"},{"Key":"ProjectName.keyword","Value":"Project"},{"Key":"MainAddress","Value":"Address"},{"Key":"IssueDate","Value":"Issued Date"},{"Key":"FinalDate","Value":"Finalized Date"}],
				"InspectionSortList":[{"Key":"relevance","Value":"Relevance"},{"Key":"InspectionNumber.keyword","Value":"Inspection Number"},{"Key":"MainAddress","Value":"Address"},{"Key":"ScheduledDate","Value":"Schedule Date"},{"Key":"RequestDate","Value":"Request Date"}],
				"CodeCaseSortList":[{"Key":"relevance","Value":"Relevance"},{"Key":"CaseNumber.keyword","Value":"Code Case Number"},{"Key":"ProjectName.keyword","Value":"Project"},{"Key":"MainAddress","Value":"Address"},{"Key":"OpenedDate","Value":"Opened Date"},{"Key":"ClosedDate","Value":"Closed Date"}],
				"RequestSortList":[{"Key":"relevance","Value":"Relevance"},{"Key":"RequestNumber.keyword","Value":"Request Number"},{"Key":"ProjectName.keyword","Value":"Project Name"},{"Key":"MainAddress","Value":"Address"},{"Key":"EnteredDate","Value":"Date Entered"},{"Key":"CompleteDate","Value":"Completion Date"}],
				"LicenseSortList":[{"Key":"relevance","Value":"Relevance"},{"Key":"LicenseNumber.keyword","Value":"License Number"},{"Key":"CompanyName.keyword","Value":"Company Name"},{"Key":"AppliedDate","Value":"Applied Date"},{"Key":"MainAddress","Value":"Address"}],
				"ExcludeCases":'null',
				"SortOrderList":[{"Key":'true',"Value":"Ascending"},{"Key":'false',"Value":"Descending"}],
				"PageNumber": str(page),
				"PageSize":'100',
				"SortBy":"PermitNumber.keyword",
				"SortAscending":'true'
			}

	def build_headers(self):
		return {
				'user-agent': myutil._get_ua(),
				'accept':'application/json, text/plain, */*',
				'accept-language': 'en-US,en;q=0.5',
				'accept-encoding': 'gzip, deflate, br',
				'content-type': 'application/json;charset=utf-8',
				'tenantId': '1',
				'tenantName': 'SunriseFL Prod',
				'Tyler-TenantUrl': 'SunriseFL Prod',
				'Tyler-Tenant-Culture': 'en-US',
				'origin': 'https://energov.sunrisefl.gov',
				'referer': 'https://energov.sunrisefl.gov/EnerGov_Prod/SelfService/SunriseFL%20Prod'
			}

	def parse_pagination(self, response):
		page = int(response.meta['page'])
		if page > 1:
			if response.status == 200:
				data = json.loads(response.body)
				if len(data['Result']['EntityResults']) == 0:
					return

				_temp = myutil._normalize_keys_list(data['Result']['EntityResults'])
				new_temp = []
				for dd in _temp:
					dd['permit_city'] = self.name
					new_temp.append(dd)
				myutil._save_to_mongo_bulk(data=new_temp)

		payload = self.build_payload(page)
		headers = self.build_headers()
		request = scrapy.Request(
			url=self.page_url,
			callback=self.parse_pagination,
			headers=headers, 
			method="POST", 
			body=json.dumps(payload)
		)
		request.meta['page'] = page + 1
		yield request

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
	c.crawl(SunriseSpider)
	c.start()