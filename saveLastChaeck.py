from HttpRequest.uploader import AsyncKafkaTopicProducer
producer = AsyncKafkaTopicProducer()
import requests,time
from elasticsearch import Elasticsearch
commonIdUpdate = "common-ads-portal-lastcheck"
commonIndex = "search-common-ads-data_v2"
cred = {
    "hosts":["https://node-1.kifwat.net:9200","https://node-3.kifwat.net:9200","https://node-4.kifwat.net:9200"],
    "http_auth":('elastic', 'p1a9tYGpvMxyHpj-_Fsx')
}
session = requests.session()
def checkBehindMessages(topic,session):
    url = f"https://kafka.kifwat.net/api/clusters/kifwat-kafka/topics/{topic}/consumer-groups"
    r = session.get(url)
    data = r.json()
    data = data[0]["messagesBehind"]
    print(url,data)
    return data
commonIdUpdate = "common-ads-portal-lastcheck"
def saveLastCheck(website,nowtime):
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
    while True:
        topic = f"activeid-{website}"
        if checkBehindMessages(topic,session)<=0:break
        time.sleep(10)
    while True:
        try:
            es = Elasticsearch(**cred)
            response = es.update_by_query(index=commonIndex, body=update_query,wait_for_completion=False)
            es.close()
            break
        except:pass
        finally:es.close()
        
    data = {
        "website":website,
        "lastcheck":nowtime,
         "task_id" :response['task']
    }
    producer.PushDataList(commonIdUpdate,[data])
   