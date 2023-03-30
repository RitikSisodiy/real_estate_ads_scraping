from HttpRequest.uploader import AsyncKafkaTopicProducer
import requests,time
import settings
from elasticsearch import Elasticsearch
commonIdUpdate = "common-ads-portal-lastcheck"
commonIndex = "search-common-ads-data_v2"
cred = {
    "hosts":settings.ES_HOSTS,
    "http_auth":(settings.ES_USER, settings.ES_PASSWORD)
}
session = requests.session()
# This function is used to check the number of messages behind the Kafka topic consumer group for a given topic.
def checkBehindMessages(topic,session):
    url = f"https://kafka.kifwat.net/api/clusters/kifwat-kafka/topics/{topic}/consumer-groups"
    r = session.get(url)
    data = r.json()
    data = data[0]["messagesBehind"]
    print(url,data)
    return data
commonIdUpdate = "common-ads-portal-lastcheck"
# This function is used to update the last check time of all ads for a given website.
def saveLastCheck(website,nowtime):
    producer = AsyncKafkaTopicProducer()
    try:
        # creating an update query to mark all ads with a last_check time less than nowtime as inactive
        update_query = {
            "script": {
                "source": "ctx._source.available=false",
                "lang": "painless"
            },
            "query":{
                "bool":{
                    "must":[
                        {
                            "match": {
                                "website": website
                            }
                        },
                        {
                            "match": {
                                "available": True
                            }
                        },
                        {
                            "range": {
                                "last_checked": {
                                    "lte": nowtime
                                }
                            }
                        }
                    ]
                    
                }
            }
        }
        # checking the messages behind the Kafka topic consumer group before updating the ads
        data = {
            "website":website,
            "lastcheck":nowtime,
        }
        producer.PushDataList(commonIdUpdate,[data])
        while True:
            topic = f"activeid-{website}"
            if checkBehindMessages(topic,session)<=0:break
            time.sleep(10)
        # updating the ads in Elasticsearch using the update_by_query API
        while True:
            try:
                es = Elasticsearch(**cred)
                response = es.update_by_query(index=commonIndex, body=update_query,wait_for_completion=False,conflicts="proceed")
                es.close()
                break
            except:pass
            finally:es.close()

        # pushing a message to the common-ads-portal-lastcheck Kafka topic with the updated last check time information   
        data["task_id"] =response['task']
        producer.PushDataList(commonIdUpdate,[data])
    except Exception as e:
        data = {
            "portal":"website",
            "nowtime":nowtime,
            "error":str(e)
        }
        producer.PushDataList(commonIdUpdate,[data])