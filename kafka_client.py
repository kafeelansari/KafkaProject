from kafka.admin import KafkaAdminClient,NewTopic

class KafkaAdmin:
    def __init__(self):
        self.admin = KafkaAdminClient(bootstrap_servers='localhost:9092')

    def create_topic(self,topic,rf,partitions):
        topics = []
        topics.append(NewTopic(name=topic,num_partitions=partitions,replication_factor=rf))
        for i in topics:
            print(i)
        self.admin.create_topics(new_topics=topics)
        print("Topic {} created successfully".format(topic))

    def validate_topic(self,topic_name):
        topics1 = []
        par=1
        rf=1
        topics1.append(NewTopic(name=topic_name, num_partitions=par, replication_factor=rf))
        result = self.admin.create_topics(new_topics=topics1,validate_only=True)
        print(result)
