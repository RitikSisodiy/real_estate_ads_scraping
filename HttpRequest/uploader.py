from fileinput import filename
import json
from aiokafka import AIOKafkaProducer
import asyncio
import traceback    
from .getImageHash import ImageHash
import settings
# import sys,os
# from .s3Client import S3
# filename=sys.argv[1]
if not settings.BROKER_IPS:
    raise ValueError("Please set the BROKER_IPS  variable in settings")
bootstrap_server = settings.BROKER_IPS
async def send_one(topic,data,producer):
    # Get cluster layout and initial topic/partition leadership information
    
    try:
        # Produce message
        # data = json.dumps(data)
        # msg = str().encode('utf-8')
        msg = bytes(data, 'utf-8')
        await producer.send_and_wait(topic, msg)
        # print("uploaded")
    except Exception as e:
        await send_one(topic,data,producer)
    finally:
        # Wait for all pending messages to be delivered or expire.
        pass

# producer = KafkaTopicProducer()
async def uploadAdImages(ad,s3client):
    imgs = ad.get("images_url")
    if not imgs:
        return ad
    uploadPath = ad.get("website") or "other"
    uploadPath = "portals/"+uploadPath
    ad["images_url"] = await s3client.bulkUrlUpload(ad["images_url"],uploadPath=uploadPath)
    return ad
async def bulkuploadAdImages(ads,s3client):
    tasks = []
    result = []
    maxlength = 50
    for ad in ads:
        tasks.append(asyncio.ensure_future(uploadAdImages(ad,s3client)))
        if len(tasks)>=maxlength:
            result+=await asyncio.gather(*tasks)
            tasks = []
    result += await asyncio.gather(*tasks)
    return result
async def main():
    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap_server)
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
        producer = AIOKafkaProducer(bootstrap_servers=bootstrap_server)
    await producer.start()
class AsyncKafkaTopicProducer:
    def __init__(self) -> None:
        self.start = False
        pass
        # self.producer = asyncio.run(self.statProducer())
        # print(self.producer,"producer started")
    async def statProducer(self):
        self.producer = AIOKafkaProducer(bootstrap_servers=bootstrap_server)
        await self.producer.start()
        self.start = True
        # return producer
    async def stopProducer(self):
        await self.producer.stop()
        self.start = False
    async def send_one(self,topic,data,retry=0):
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
            retry+=1
            if retry<10:await self.send_one(topic,data,retry)
        finally:
            # Wait for all pending messages to be delivered or expire.
            pass
    async def send_one_v1(self,producer,topic,data,retry=0):
    # Get cluster layout and initial topic/partition leadership information
        try:
            # Produce message
            # data = json.dumps(data)
            # msg = str().encode('utf-8')
            msg = bytes(data, 'utf-8')
            await producer.send_and_wait(topic, msg)
            print("uploaded")
        except Exception as e:
            print(e)
            traceback.print_exc()
            retry+=1
            if retry<10:await self.send_one_v1(producer,topic,data,retry)
        finally:
            # Wait for all pending messages to be delivered or expire.
            pass
    async def TriggerPushDataList(self,topic,data):
        tasks = []
        await self.statProducer()
        hashimage = ImageHash()
        # s3client = S3(os.getenv("BUCKET_NAME"))
        # data = await bulkuploadAdImages(data,s3client)
        tasks = []
        datas = []
        for da in data:
            tasks.append(asyncio.ensure_future(hashimage.getHashByUrl(da)))
             # If the number of tasks reaches "size"
            if len(tasks)>50:
                # Wait for all tasks to complete
                datas += await asyncio.gather(*tasks)
                # Reset the list of tasks
                tasks = [] 
        if tasks:
            datas += await asyncio.gather(*tasks)
            tasks = []
        for da in datas:
            da = json.dumps(da)
            tasks.append(asyncio.ensure_future(self.send_one(topic,da)))
        await asyncio.gather(*tasks)
        await self.stopProducer()
        # await s3client.close()
    async def TriggerPushDataList_v1(self,topic,data):
        tasks = []
        producer = AIOKafkaProducer(bootstrap_servers=bootstrap_server)
        await producer.start()
        # s3client = S3(os.getenv("BUCKET_NAME"))
        # data = await bulkuploadAdImages(data,s3client)
        for da in data:
            da = json.dumps(da)
            tasks.append(asyncio.ensure_future(self.send_one_v1(producer,topic,da)))
        await asyncio.gather(*tasks)
        await producer.stop()
        # await s3client.close()
    def PushDataList(self,topic,data):
        datali = []
        for da in data:
            if da:
                if data.get("price") and data.get("area"):
                    try:data["price_m2"] = float(data.get("price")) / float(data.get("area"))
                    except:pass
                da.append(da)
        asyncio.run(self.TriggerPushDataList(topic,data))
    def PushDataList_v1(self,topic,data):
        data = [da for da in data if da]
        asyncio.run(self.TriggerPushDataList_v1(topic,data))
# asyncio.run(main())

# def PushDataOnKafka(data):
