from kafka import KafkaProducer
import json
from settings import *



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
            result = future.get(timeout=TIMEOUT)
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

# if __name__ == '__main__':
#     producer = KafkaTopicProducer()
#     producer.kafka_producer_sync("topic", {"Test":123})
#
