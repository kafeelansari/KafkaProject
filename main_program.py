from kafka_client import KafkaAdmin
from kafka_producer import KafkaProducerAPI
from kafka_consumer import KafkaConsumerAPI


def main():
    kadmin = KafkaAdmin()
    topic_name = "test"
    #kadmin.create_topic(topic_name,1,1)
    #kadmin.validate_topic(topic_name)

    kprod = KafkaProducerAPI('localhost:9092','test')
    kprod.send_messages("/home/sylar/Desktop/SKH_dataset/kafka/urls.txt")


    kcons = KafkaConsumerAPI(bl='localhost:9092',topic='test')
    kcons.read_messages()


if __name__=="__main__":
    main()
