from kafka import KafkaProducer
import json
import time


class ProducerServer(KafkaProducer):

    def __init__(self, input_file, topic, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic

    # TODO we're generating a dummy data
    def generate_data(self):
        count = 0
        with open(self.input_file, 'r') as file:
            data = json.load(file)
            print(len(data))
            for line in data:
                message = self.dict_to_binary(line)
                # TODO send the correct data
                self.send(self.topic, message)
                count = count+1
                if count % 30000 == 0:
                    print("Event published = ", count)
#                 time.sleep(1)
        print("Number of call-message = ", count)

    # TODO fill this in to return the json dictionary to binary
    def dict_to_binary(self, json_dict):
        return json.dumps(json_dict).encode("utf-8")