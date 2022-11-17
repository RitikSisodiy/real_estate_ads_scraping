import os
import time
from real_estate_advert.leboncoin.ad import Ad as leboncoinAd
from real_estate_advert.leboncoin.scraperv4 import updateLebonCoin,leboncoinAdScraper
from real_estate_advert.paruvendu.scraperv2 import main_scraper as ParuvenduScraper
from real_estate_advert.gensdeconfiance.scraper import main_scraper as gensdeconfianceScraper
from real_estate_advert.paruvendu.scraperv2 import UpdateParuvendu
from real_estate_advert.pap.scraper2 import pap_scraper as PapScraper
from real_estate_advert.pap.scraper2 import UpdatePap
from real_estate_advert.bienci.scraper import main_scraper as bienciScraper
from real_estate_advert.bienci.scraper import UpdateBienci
from real_estate_advert.seloger.scraperv3 import main_scraper as selogerScraper
from real_estate_advert.logicImmo.logicImmo import main_scraper as LogicImmoScraper
from real_estate_advert.lefigaro.scraper import main_scraper as LefigaroScrapper
from real_estate_advert.ouestfrance.scraper import main_scraper as OuestFranceScrapper
from real_estate_advert.avendrealouer.scraper import main_scraper as avendrealouerScrapper
from real_estate_advert.green_acres.scraper import main_scraper as greenacresrScrapper
from celery import Celery
from celery.schedules import crontab
from settings import *


celery_app = Celery(TaskQueue, backend=CeleryBackend, broker=CeleryBroker)
celery_app.config_from_object(__name__)


@celery_app.task(name="real estate")
def real_estate_task(payload):
    print("Task start ================> ")
    print("payload : ", payload)
    obj = leboncoinAd()
    obj.visit_url()

    # Scraping task obj start here

    print("Task End ================> ")
import traceback
@celery_app.task(name="bienci task")
def scrap_bienci_task(payload):
    print("Task start ================> ")
    print("payload : ", payload)
    try:
        data = bienciScraper(payload)
    
    except Exception as e:
        print(traceback.format_exc())
        print("Exception ================> ",e)
        data= "exception"
    # Scraping task obj start here
    print("Task End ================> ")
    return data
@celery_app.task(name="real estate logic-immo")
def scrap_logicimmo_task(payload):
    print("Task start ================> ")
    print("payload : ", payload)
    try:
        data = LogicImmoScraper(payload)
    
    except Exception as e:
        print(traceback.format_exc())
        print("Exception ================> ",e)
        data= "exception"
    # Scraping task obj start here
    print("Task End ================> ")
    return data
@celery_app.task(name="real estate Lefigaro")
def scrap_lefigaro_task(payload):
    print("Task start ================> ")
    print("payload : ", payload)
    try:
        data = LefigaroScrapper(payload)
    except Exception as e:
        print(traceback.format_exc())
        print("Exception ================> ",e)
        data= "exception"
    # Scraping task obj start here
    print("Task End ================> ")
    return data

@celery_app.task(name="real estate avendrealouer")
def scrap_avendrealouer_task(payload):
    print("Task start ================> ")
    print("payload : ", payload)
    try:
        data = avendrealouerScrapper(payload)
    except Exception as e:
        print(traceback.format_exc())
        print("Exception ================> ",e)
        data= "exception"
    # Scraping task obj start here
    print("Task End ================> ")
    return data
@celery_app.task(name="real estate OuestFranceScrapper")
def scrap_OuestFranceScrapper_task(payload):
    print("Task start ================> ")
    print("payload : ", payload)
    try:
        data = OuestFranceScrapper(payload)
    except Exception as e:
        print(traceback.format_exc())
        print("Exception ================> ",e)
        data= "exception"
    # Scraping task obj start here
    print("Task End ================> ")
    return data
@celery_app.task(name="real estate gensdeconfianceScraper")
def scrap_gensdeconfiance_task(payload):
    print("Task start ================> ")
    print("payload : ", payload)
    try:
        data = gensdeconfianceScraper(payload)
    except Exception as e:
        print(traceback.format_exc())
        print("Exception ================> ",e)
        data= "exception"
    # Scraping task obj start here
    print("Task End ================> ")
    return data


@celery_app.task(name="real estate leboncoin")
def scrape_leboncoin_task(payload):
    print("Task start ================> ")
    print("payload : ", payload)
    try:leboncoinAdScraper(payload)
    except Exception as e:
        traceback.print_exc()
        print("Exception ================> ",e)
    # Scraping task obj start here
    print("Scraper 3 ")

    print("Task End ================> ")

@celery_app.task(name="real estate fetch leboncoin latest ad")
def update_leboncoin_ads():
    print("Task start ================> ")
    try:leboncoinAdScraper({
        "real_state_type":"Updated/Latest Ads"
    })
    except Exception as e:
        traceback.print_exc()
        print("Exception ================> ",e)
    print("Task End ================> ")
@celery_app.task(name="real estate fetch paruvedu latest ad")
def update_paruvendu_ads():
    print("Task start ================> ")
    try:UpdateParuvendu()
    except Exception as e:
        traceback.print_exc()
        print("Exception ================> ",e)
    print("Task End ================> ")

@celery_app.task(name="real estate fetch pap latest ad")
def update_pap_ads():
    print("Task start ================> ")
    try:UpdatePap()
    except Exception as e:
        traceback.print_exc()
        print("Exception ================> ",e)
    print("Task End ================> ")
@celery_app.task(name="real estate fetch Bienci latest ad")
def update_Bienci_ads():
    print("Task start ================> ")
    try:
        data = UpdateBienci()
    except Exception as e:
        traceback.print_exc()
        print("Exception ================> ",e)
        data = "exeption"
    print("Task End ================> ")
    print(data)
    return data
@celery_app.task(name="real estate fetch logicImmo latest ad")
def update_logicImmo_ads():
    print("Task start ================> ")
    try:
        data = LogicImmoScraper({},update=True)
    except Exception as e:
        traceback.print_exc()
        print("Exception ================> ",e)
        data = "exeption"
    print("Task End ================> ")
    print(data)
    return data
@celery_app.task(name="real estate fetch lefigaro latest ad")
def update_lefigaro_ads():
    print("Task start ================> ")
    try:
        data = LefigaroScrapper({"real_state_type":"Updated/Latest Ads"})
    except Exception as e:
        traceback.print_exc()
        print("Exception ================> ",e)
        data = "exeption"
    print("Task End ================> ")
    print(data)
    return data
@celery_app.task(name="real estate fetch avendrealouer latest ad")
def update_avendrealouer_ads():
    print("Task start ================> ")
    try:
        data = avendrealouerScrapper({"real_state_type":"Updated/Latest Ads"})
    except Exception as e:
        traceback.print_exc()
        print("Exception ================> ",e)
        data = "exeption"
    print("Task End ================> ")
    print(data)
    return data
@celery_app.task(name="real estate fetch seloger latest ad")
def update_seloger_ads():
    print("Task start ================> ")
    try:selogerScraper({},update=True)
    except Exception as e:
        traceback.print_exc()
        print("Exception ================> ",e)
    print("Task End ================> ")


@celery_app.task(name="real estate pap")
def scrape_pap_task(payload):
    print("Task start ================> ")
    print("payload : ", payload)
    PapScraper(payload)

    # Scraping task obj start here

    print("Task End ================> ")
@celery_app.task(name="real estate seloger")
def scrape_seloger_task(payload):
    print("Task start ================> ")
    print("payload : ", payload)
    selogerScraper(payload)

    # Scraping task obj start here

    print("Task End ================> ")



@celery_app.task(name="real estate paruvendu")
def scrape_paruvendu_task(payload):
    print("Task start ================> ")
    print("payload : ", payload)
    try:
        ParuvenduScraper(payload)
    except Exception as e:
        traceback.print_exc()
        print("Exception ==============>", e)
    # Scraping task obj start here

    print("Task End ================> ")
@celery_app.task(name="real estate OuestFrance")
def update_OuestFranceScrapper_ads():
    print("Task start ================> ")
    try:OuestFranceScrapper({},update=True)
    except Exception as e:
        traceback.print_exc()
        print("Exception ================> ",e)
    print("Task End ================> ")
@celery_app.task(name="real estate gensdeconfiance")
def update_gensdeconfianceScrapper_ads():
    print("Task start ================> ")
    try:gensdeconfianceScraper({},update=True)
    except Exception as e:
        traceback.print_exc()
        print("Exception ================> ",e)
    print("Task End ================> ")

@celery_app.task(name="real estate green-acres")
def scrap_greenacres_task(payload):
    print("Task start ================> ")
    print("payload : ", payload)
    try:
        greenacresrScrapper(payload)
    except Exception as e:
        traceback.print_exc()
        print("Exception ==============>", e)
    # Scraping task obj start here

    print("Task End ================> ")



# 4 website 1 Core = 4 Core CPU


@celery_app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    print("rnnnint periodic tasks")
    # Calls update_leboncoin_ads in every 20 minutes
    sender.add_periodic_task(20*60, update_leboncoin_ads.s(), name='update leboncoin ads in every 20 minuts')
    # Calls update_peruvendu_ads in every 20 minutes
    sender.add_periodic_task(20*60, update_paruvendu_ads.s(), name='update paruvendu ads every 20 minuts')
    # # Calls update_pap_ads in every 20 minutes
    sender.add_periodic_task(20*60, update_pap_ads.s(), name='update pap ads every 20 minuts')
    # Calls update_seloger_ads in every 20 minutes
    sender.add_periodic_task(20*60, update_seloger_ads.s(), name='update seloger ads every 20 minuts')
    # Calls update_Bienci_ads in every 20 minutes
    sender.add_periodic_task(20*60, update_Bienci_ads.s(), name='update Bienci ads every 20 minuts')
    # Calls update_logicImmo_ads in every 20 minutes
    sender.add_periodic_task(20*60, update_logicImmo_ads.s(), name='update logicImmo ads every 20 minuts')
    # Calls update_logicImmo_ads in every 20 minutes
    sender.add_periodic_task(20*60, update_lefigaro_ads.s(), name='update lefigaro ads every 20 minuts')
    # Calls update_logicImmo_ads in every 20 minutes
    sender.add_periodic_task(20*60, update_avendrealouer_ads.s(), name='update avendrealouer ads every 20 minuts')
    # Calls update_logicImmo_ads in every 20 minutes
    sender.add_periodic_task(20*60, update_OuestFranceScrapper_ads.s(), name='update OuestFranceScrapper ads every 20 minuts')
    # Calls update_logicImmo_ads in every 20 minutes
    sender.add_periodic_task(20*60, update_gensdeconfianceScrapper_ads.s(), name='update gensdeconfiance ads every 20 minuts')
