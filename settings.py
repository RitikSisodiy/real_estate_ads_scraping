import os
"""

APP settings
"""
title = "Web Scrapping Server "
API_V1_STR = "/api/v1"
DEBUG = True

""" Kafka Publisher Configurations """

BROKER_IPS = [f"10.8.0.27:9091",f"10.8.0.27:9092", f"10.8.0.27:9093"]
TIMEOUT = 60
CRAWLER_SOCKS_PROXY_HOST = '127.0.0.1'
"""Celery configurations """

# redis server setting

TaskQueue = "tasks"
CeleryBackend = 'redis://localhost:6379/0'
CeleryBroker = f'redis://localhost:6379/0'

# s3 Client 
REGION_NAME="" 
BUCKET_NAME=""

# ES Client

ES_HOSTS = ["https://node-1.kifwat.net:9200","https://node-3.kifwat.net:9200","https://node-4.kifwat.net:9200"]
ES_USER = "elastic"
ES_PASSWORD  = "p1a9tYGpvMxyHpj-_Fsx"

# Proxy
PROXY = os.getenv("Proxy")

# KAFKA TOPICS
KAFKA_COMMON_PATTERN = "common-ads-data_v1"
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