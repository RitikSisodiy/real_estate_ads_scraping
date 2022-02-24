from kafka import KafkaProducer
import json

""" Kafka Producer Configurations """

BROKER_IPS = [f"10.100.103.101:6667 "]
TIMEOUT = 60

""" Kafka Topic  Producer Configurations """


class KafkaTopicProducer(object):
    def __init__(self):
        try:
            self.producer = KafkaProducer(bootstrap_servers=BROKER_IPS)
        except Exception as e:
            print("KAFKA ERROR >>>>>>> ", e)

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

    def kafka_producer_async(self, topic="topic", data={}):
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

# # How to use this producer.
#
# if __name__ == '__main__':
#     producer = KafkaTopicProducer()
#     producer.kafka_producer_sync("topic", "Data")
