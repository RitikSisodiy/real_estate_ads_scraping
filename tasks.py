import os
import time
from real_estate_advert.leboncoin.ad import Ad as leboncoinAd
from real_estate_advert.leboncoin.scraperv4 import updateLebonCoin
from real_estate_advert.paruvendu.scraperv2 import main_scraper as ParuvenduScraper
from real_estate_advert.paruvendu.scraperv2 import UpdateParuvendu
from real_estate_advert.pap.scraperf import pap_scraper as PapScraper
from real_estate_advert.pap.scraperf import UpdatePap
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


@celery_app.task(name="real estate leboncoin")
def scrape_leboncoin_task(payload):
    print("Task start ================> ")
    print("payload : ", payload)

    # Scraping task obj start here
    print("Scraper 3 ")
    time.sleep(100)

    print("Task End ================> ")

@celery_app.task(name="real estate fetch leboncoin latest ad")
def update_leboncoin_ads():
    print("Task start ================> ")
    try:updateLebonCoin()
    except Exception as e:print("Exception ================> ",e)
    print("Task End ================> ")
@celery_app.task(name="real estate fetch paruvedu latest ad")
def update_paruvendu_ads():
    print("Task start ================> ")
    try:UpdateParuvendu()
    except Exception as e:print("Exception ================> ",e)
    print("Task End ================> ")

@celery_app.task(name="real estate fetch pap latest ad")
def update_pap_ads():
    print("Task start ================> ")
    try:UpdatePap()
    except Exception as e:print("Exception ================> ",e)
    print("Task End ================> ")


@celery_app.task(name="real estate pap")
def scrape_pap_task(payload):
    print("Task start ================> ")
    print("payload : ", payload)
    PapScraper(payload)

    # Scraping task obj start here

    print("Task End ================> ")


@celery_app.task(name="real estate paruvendu")
def scrape_paruvendu_task(payload):
    print("Task start ================> ")
    print("payload : ", payload)
    try:
        ParuvenduScraper(payload)
    except Exception as e:
        print("Exception ==============>", e)
    # Scraping task obj start here

    print("Task End ================> ")


@celery_app.task(name="real estate")
def scrape_seloger_task(payload):
    print("Task start ================> ")
    print("payload : ", payload)

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
    # Calls update_pap_ads in every 20 minutes
    sender.add_periodic_task(20*60, update_pap_ads.s(), name='update pap ads every 20 minuts')
