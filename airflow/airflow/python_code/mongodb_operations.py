from requests import Session
import json
from logger import Logger
from pandas import DataFrame



class MongoDbOps: 

    def __init__(self) -> None:
        self.url = "https://eu-central-1.aws.data.mongodb-api.com/app/data-wlooxrj/endpoint/data/v1/action"
        self.session = Session()
        self.headers = {
        'Content-Type': 'application/json',
        'Access-Control-Request-Headers': '*',
        'api-key': '<api_key>',
        'Accept': 'application/json'
        }

    def insert_many(self, data_frame):
        """
        Insert many documents to flight_records collection in maxim_company db
        """

        end_point = "insertMany"
        data_frame: DataFrame = data_frame
        json_df = json.loads(data_frame.to_json(orient='records'))
        try:
            payload = json.dumps({
            "dataSource": "naya-final-proj-maxim",
            "database": "<db_name>",
            "collection": "flight_records",
            "documents": json_df
            })

            response = self.session.post(f'{self.url}/{end_point}', headers=self.headers, data=payload)
            if response.status_code == 200 or response.status_code == 201:
                Logger().kafka_logger("Pass - Successfuly inserted documents to flights_records collection", 'MongoDbOps.insert_many')
            else:
                Logger().kafka_logger(f"Fail - Failed to insert documents to flight_recorde with code {str(response.status_code)} and {response.content.decode('utf-8')}  ", 'MongoDbOps.insert_many')
        except Exception as err:
            Logger().kafka_logger(f"Fail - {err}", 'MongoDbOps.insert_many')


