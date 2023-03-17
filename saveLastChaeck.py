from HttpRequest.uploader import AsyncKafkaTopicProducer
producer = AsyncKafkaTopicProducer()
commonIdUpdate = "common-ads-portal-lastcheck"
def saveLastCheck(website,nowtime):
    data = {
        "website":website,
        "lastcheck":nowtime
    }
    producer.PushDataList(commonIdUpdate,[data])