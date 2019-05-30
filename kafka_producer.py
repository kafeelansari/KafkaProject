from kafka import KafkaProducer


class KafkaProducerAPI:
    def __init__(self,bootstrap_server,topic):
        self.broker_list =[]
        self.broker_list.append(bootstrap_server)
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers=self.broker_list)

    def read_file(self,file_path_to_read,type='local'):
        if type == 'local':
            file = open(file_path_to_read,"r")
            contents = file.readlines()
            file.close()
        return contents

    def send_messages(self,file_path):
        conts = self.read_file(file_path , 'local')
        for i in conts:
            self.producer.send(self.topic,str.encode(i))






