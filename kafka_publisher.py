from kafka import KafkaProducer
import json
from settings import *

from fileinput import filename
from aiokafka import AIOKafkaProducer
import asyncio

import sys

# filename=sys.argv[1]
async def send_one(topic,data,producer):
    # Get cluster layout and initial topic/partition leadership information
    
    try:
        # Produce message
        # data = json.dumps(data)
        # msg = str().encode('utf-8')
        msg = bytes(data, 'utf-8')
        await producer.send_and_wait(topic, msg)
        print("uploaded")
    except Exception as e:
        await send_one(topic,data,producer)
    finally:
        # Wait for all pending messages to be delivered or expire.
        pass
class AsyncKafkaTopicProducer:  
    def __init__(self):
        self.producer = AIOKafkaProducer(bootstrap_servers=BROKER_IPS)
    async def start(self):
        await self.producer.start()

    async def kafka_producer_async(self,topic,data):
    # Get cluster layout and initial topic/partition leadership information
        try:
            print("tring")
            # Produce message
            # data = json.dumps(data)
            # msg = str().encode('utf-8')
            msg = bytes(data, 'utf-8')
            print("converted")
            await self.producer.send_and_wait(topic, msg)
            print("uploaded")
        # except Exception as e:
        #     await send_one(topic,data)
        finally:
            print("passed")
            # Wait for all pending messages to be delivered or expire.
            pass
    async def close(self):
        await self.producer.stop()
class KafkaTopicProducer(object):
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=BROKER_IPS , api_version = (0,3,3))

    @staticmethod
    def success(metadata):
        print(metadata.topic)
        return True

    @staticmethod
    def error(exception):
        print(exception)
        return False

    def kafka_producer_sync(self, topic="topic", data={}):
        """
        :param topic:
        :param data:
        :return:
        """
        try:
            data = json.dumps(data)
            msg = str(data).encode('utf-8')
            future = self.producer.send(topic, msg)
            #result = future.get(timeout=TIMEOUT)
            print("KAFKA STREAMING :============= >> ", topic)
        except Exception as e:
            print(e)
        finally:
            self.producer.flush()

    def kafka_producer_async(self, topic="paruvendu-data", data={}):
        """
        :param topic:
        :param data:
        :return:
        """
        try:

            data = json.dumps(data)
            msg = str(data).encode('utf-8')
            self.producer.send(topic, msg).add_callback(self.success).add_errback(self.error)
        except Exception as e:
            print(e)
        finally:
            self.producer.flush()

# How to use this producer.
