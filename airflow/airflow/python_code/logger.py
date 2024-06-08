import json
import datetime
import kafka 


class Logger:
    time_stamp = f'run_{datetime.datetime.now().strftime("%Y%m%d%H%M%S")}'
    
    def __init__(self) -> None:
        self.logger_producer = kafka.KafkaProducer(bootstrap_servers='kafka1:19091',  api_version=(0,10,5))

    def kafka_logger(self, message, function):
        """Will send to topic 'run_<timestamp>' in kafka to monitor execution
        message (string): The message to be sent to kafka topic
        """
        try:
            message_dict = {'run': Logger.time_stamp , 'function': function, 'message': message}
            self.logger_producer.send('logging', json.dumps(message_dict).encode('utf-8'))
        except Exception as err:
            message_dict = dict(err)
            self.logger_producer.send('logging', json.dumps(message_dict).encode('utf-8'))
        finally:
            self.logger_producer.flush()
            self.logger_producer.close()


