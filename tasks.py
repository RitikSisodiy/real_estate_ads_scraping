import os
import time
from real_estate_advert.leboncoin.ad import Ad as leboncoinAd
from real_estate_advert.paruvendu.scraperv1 import main_scraper as ParuvenduScraper
from real_estate_advert.pap.scraper import pap_scraper as PapScraper

from celery import Celery
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


@celery_app.task(name="real estate pap")
def scrape_pap_task(payload):
    print("Task start ================> ")
    print("payload : ", payload)
    PapScraper()

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