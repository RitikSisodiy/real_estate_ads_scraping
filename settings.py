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

# Rabbit MQ server setting

TaskQueue = "tasks"
CeleryBackend = 'redis://localhost:6379/0'
CeleryBroker = f'redis://localhost:6379/0'

