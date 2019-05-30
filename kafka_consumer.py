from kafka import KafkaConsumer

class KafkaConsumerAPI:
    def __init__(self,bl,topic):
        self.broker_list = []
        self.broker_list.append(bl)
        self.topic = topic
        self.consumer = KafkaConsumer(self.topic,auto_offset_reset='earliest',bootstrap_servers=self.broker_list)

    def read_messages(self):
        for message in self.consumer:
            # message value and key are raw bytes -- decode if necessary!
            # e.g., for unicode: `message.value.decode('utf-8')`
            print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                                 message.offset, message.key,
                                                 message.value))