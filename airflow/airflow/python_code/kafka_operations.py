import kafka.errors
from  requests import Session
import json
import kafka 
import pandas as pd
import os
from logger import Logger

class KafkaData:

    def __init__(self):
        self.session = Session()
        self.jheaders = {'content-type': 'application/json'}
        self.producer = kafka.KafkaProducer(bootstrap_servers='kafka1:19091',  api_version=(0,10,5))
        self.consumer = kafka.KafkaConsumer('data', bootstrap_servers='kafka1:19091',  auto_offset_reset='earliest', api_version=(0,10,5), consumer_timeout_ms= 6000)




    def produce_data(self):
        """
        Takes flight records from gov.il database loads the data to kafka topic and returns the flight records a dictionary 
        :return Flights records as python dictionary
        """
        try:
            r = self.session.get('https://data.gov.il/api/3/action/datastore_search?resource_id=e83f763b-b7d7-479e-b172-ae981ddc6de5', headers=self.jheaders)
            if r.status_code == 200:
                json_res = json.loads(r.text)
                flight_records = json_res.get('result').get('records')
                r = flight_records
                Logger().kafka_logger(f"Pass - GET from data.gov successful", 'KafkaData.produce_data')
            else:
                Logger().kafka_logger(f"Fail - Status code is {r.status_code} - error {r.content}", 'KafkaData.produce_data')

            temp = json.dumps(flight_records).encode('utf-8')  # Encode as bytes
            self.producer.send('data', temp)
            Logger().kafka_logger(f'Success - Sent flights data to data topic', 'KafkaData.produce_data')

        except Exception as err:
            Logger().kafka_logger(f'Error - {err}', 'KafkaData.produce_data')
            r = err
        finally:
            self.producer.flush()
            self.producer.close()

        return r
    
    def consume_data(self, csv_file):
        """Ingests data from kafak topic 'data' and returns the data as json to be used by pandas to analyze
        csv_file (string): The path to the csv file to be concatenated with the new data pulled from API
        """
        # Continuously poll for messages
        try:
            messages = self.consumer
            last_message = list(messages)[-1]
            r = ""
            cur_dir = os.getcwd()
            if last_message is not None and bool(last_message):
                last_message = last_message.value.decode('utf-8')
                df = pd.DataFrame(json.loads(last_message))
                old_csv = f'{cur_dir}/df_temp.csv'
                if os.path.exists(old_csv):
                    old_df = pd.read_csv(old_csv, sep= '\t')
                    r = pd.concat([old_df, df])
                    if not r.empty:
                        os.remove(old_csv)
                        if not os.path.exists(old_csv):
                            os.rename(csv_file, old_csv)
                            Logger().kafka_logger(f'Success - Renamed {old_csv} to {csv_file}', 'KafkaData.consume_data')
                        else:
                            Logger().kafka_logger(f'Fail - {FileExistsError}', 'KafkaData.consume_data')
                            raise FileExistsError
                else:
                    r = df
                    df.to_csv(f'{cur_dir}/df_temp.csv', sep= '\t')
                    Logger().kafka_logger(f'Success - Created def_temp.csv in {cur_dir}', 'KafkaData.consume_data')
            else:                
                Logger().kafka_logger(f'Fail - No message were pulled from {last_message.topic}', 'KafkaData.consume_data')
        except Exception as err:
            Logger().kafka_logger(f'Fail - {err}', 'KafkaData.consume_data')
            raise err
        finally:
            self.consumer.close()
        return r