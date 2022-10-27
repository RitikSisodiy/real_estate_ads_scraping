from asyncio import tasks
from fileinput import filename
import json
from aiokafka import AIOKafkaProducer
import asyncio
import traceback    
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

# producer = KafkaTopicProducer()
async def main():
    producer = AIOKafkaProducer(
        bootstrap_servers=[f"10.8.0.27:9091",f"10.8.0.27:9092", f"10.8.0.27:9093"])
    await producer.start()
    with open(filename,'r') as file:
        data = file.readlines()
    tasks = []
    count = 0
    totl = len(data)
    for da in data:
        # da  = json.loads(da)
        print('appending')
        tasks.append(asyncio.ensure_future(send_one(topic="leboncoin-data_v1", data=da,producer=producer)))
        # producer.kafka_producer_sync(topic="leboncoin-data_v1", data=da)
        count+=1
        if len(tasks)==1000 or count == totl:
            await asyncio.gather(*tasks)
            tasks = []
    await producer.stop()
async def PushData(data,producer=None):
    if not producer:
        producer = AIOKafkaProducer(bootstrap_servers=[f"10.8.0.27:9091",f"10.8.0.27:9092", f"10.8.0.27:9093"])
    await producer.start()
class AsyncKafkaTopicProducer:
    def __init__(self) -> None:
        pass
        # self.producer = asyncio.run(self.statProducer())
        # print(self.producer,"producer started")
    async def statProducer(self):
        self.producer = AIOKafkaProducer(bootstrap_servers=[f"10.8.0.27:9091",f"10.8.0.27:9092", f"10.8.0.27:9093"])
        await self.producer.start()
        # return producer
    async def stopProducer(self):
        await self.producer.stop()
    async def send_one(self,topic,data):
    # Get cluster layout and initial topic/partition leadership information
        try:
            # Produce message
            # data = json.dumps(data)
            # msg = str().encode('utf-8')
            msg = bytes(data, 'utf-8')
            await self.producer.send_and_wait(topic, msg)
            print("uploaded")
        except Exception as e:
            print(e)
            traceback.print_exc()
            input()
            await self.send_one(topic,data)
        finally:
            # Wait for all pending messages to be delivered or expire.
            pass
    async def TriggerPushDataList(self,topic,data):
        tasks = []
        await self.statProducer()
        for da in data:
            da = json.dumps(da)
            tasks.append(asyncio.ensure_future(self.send_one(topic,da)))
        await asyncio.gather(*tasks)
        await self.stopProducer()
    def PushDataList(self,topic,data):
        asyncio.run(self.TriggerPushDataList(topic,data))
# asyncio.run(main())

# def PushDataOnKafka(data):
