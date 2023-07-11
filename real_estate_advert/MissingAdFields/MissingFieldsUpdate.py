from .scrollfilter import scroll_filtered_docs
import settings,time,concurrent.futures
from real_estate_advert.pap.scraper import PapScraper
from real_estate_advert.seloger.scraperv3 import SelogerScraper
from real_estate_advert.paruvendu.scraperv2 import UpdateId as paruvenduUpdateFields
# Define query and scroll parameters
query = {"query": {"match_all": {}}}
scroll_size = 10
index_name = settings.ES_COMMON_TOPIC
doc_type = "_doc"
portals = settings.MissingAdFieldsPortals
# Define scroll filter to filter records where postal_code and status field does not exist
scroll_filter = {"query":{"bool": {"must_not": [{"exists": {"field": "postal_code"}}, {"exists": {"field": "status"}},{"exists": {"field": "@timestamp"}},{"exists": {"field": "title"}}]}}}


def updateMissinFieldByPortal(portalName):
    filterConf = scroll_filter.copy()
    filterConf["query"]["bool"]["must"] =[
        {
            "match":{
                "website": portalName
            }
        }
    ]
    AdObj = scroll_filtered_docs( index_name, filterConf, scroll_size)
    nextad = next(AdObj)
    if nextad:
        adsli = []
        if portalName == "pap.fr":
            scraperClass =  PapScraper({})
            scraper =scraperClass.updateId
        elif portalName == "seloger.com":
            scraperClass = SelogerScraper({},maxtry=True,asyncsize=5)
            scraper = scraperClass.updateId
        else:
            return 0
    for d in scroll_filtered_docs( index_name, filterConf, scroll_size):
        adsli.append(d["id"])
        if len(adsli)>=scroll_size:
            scraper(adsli)
            adsli=[]
    scraper(adsli)
    if scraperClass:scraperClass.__del__()
    print("done")
def updateMissinField():
    # updateMissinFieldByPortal("pap.fr")
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(portals)) as excuter:
        futures = excuter.map(updateMissinFieldByPortal,portals)
