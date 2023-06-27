import os
import dotenv
dotenv.load_dotenv()
"""

APP settings
"""
title = "Web Scrapping Server "
API_V1_STR = "/api/v1"
DEBUG = True

""" Kafka Publisher Configurations """

BROKER_IPS = os.getenv("BROKER_IPS") and os.getenv("BROKER_IPS").split(",") 
TIMEOUT = 60
CRAWLER_SOCKS_PROXY_HOST = '127.0.0.1'
"""Celery configurations """

# redis server setting

TaskQueue = "tasks"
CeleryBackend = 'redis://localhost:6379/0'
CeleryBroker = f'redis://localhost:6379/0'

# s3 Client 
REGION_NAME=os.getenv("REGION_NAME")
BUCKET_NAME=os.getenv("BUCKET_NAME")

# ES Client

ES_HOSTS = os.getenv("ES_HOSTS").split(",")
ES_USER =  os.getenv("ES_USER")
ES_PASSWORD  = os.getenv("ES_PASSWORD")
ES_COMMON_TOPIC = "search-common-ads-data_alias"

# Proxy
PROXY = os.getenv("PROXY")

# KAFKA TOPICS
KAFKA_COMMON_PATTERN = "common-ads-data_v1"
KAFKA_COMMON_ES_INDEX = "search-common-ads-data_alias"
KAFKA_AVENDREALOUER = "avendrealouer-data_v1"
KAFKA_BIENICI="bienici_data_v1"
KAFKA_GENSDECONFIANCE = "gensdeconfiance_data_v1"
KAFKA_LEBONCOIN = "leboncoin-data_v1"
KAFKA_LEFIGARO = "lefigaro-data_v1"
KAFKA_LOGICIMMO = "logicImmo_data_v1"
KAFKA_OUESTFRANCE = "ouestfrance-immo-v1"
KAFKA_PAP = "pap_data_v1"
KAFKA_PARUVENDU = "paruvendu-data_v1"
KAFKA_SELOGER = "seloger_data_v1"
KAFKA_DELETE = "Delete_doc_es"

MissingAdFieldsPortals = {
    "pap.fr",
    "seloger.com"
}