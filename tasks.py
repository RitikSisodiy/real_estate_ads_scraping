import os
import time
from real_estate_advert.leboncoin.ad import Ad as leboncoinAd
from real_estate_advert.paruvendu.scraper import main_scraper as ParuvenduScraper
from celery import Celery

celery_app = Celery("tasks", backend="rpc://", broker=f"pyamqp://guest:guest@localhost:5672")
celery_app.config_from_object(__name__)


# Rabbit MQ server setting
# celery_app.conf.broker_url = os.environ.get("celery_app_BROKER_URL", "amqp://guest:guest@localhost:5672//")

# Redis Broker Setting
# celery_app.conf.broker_url = os.environ.get("celery_app_BROKER_URL", "redis://localhost:6379")
# celery_app.conf.result_backend = os.environ.get("celery_app_RESULT_BACKEND", "redis://localhost:6379")


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

    print("Scraper 2 ")
    time.sleep(100)

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