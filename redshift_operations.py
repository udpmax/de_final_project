from sqlalchemy import create_engine
from sqlalchemy import text
import pandas as pd
from logger import Logger




class RedshiftOps:

    def __init__(self, data_frame) -> None:
        self.engine = create_engine('redshift://<user>:<password>@<endpoit_redshift>', 
                                   echo=False, pool_size=10)
        self.data_frame: pd = data_frame

    

    def send_data(self, if_exists='append'):
        """Create a connection to Redshift cluster with sqlalchemy and append by default the clean data frame consumed from Kafka
            possible stirngs for if_exists: "fail", "replace", "append"
        Returns:
            string: If Pass, nothing is returned. In case of Fail Exception is returned.
        """
        r = ""
        try:
            with self.engine.begin() as connection:
                self.data_frame.to_sql(name='tlv_flights', con=connection, if_exists=if_exists, index=False)
                Logger().kafka_logger(f'Pass - Successfuly converted dataframe to sql and upload to Redshift', 'RedshiftOps.send_data')
        except Exception as err:
            r = err
            Logger().kafka_logger(f'Pass - {err}', 'RedshiftOps.send_data')            
    
        return r 
    
    def read_data(self, sql_query):
        """Create a connection to Redshift cluster with sqlalchemy and append by default the clean data frame consumed from Kafka
           possible stirngs for if_exists: "fail", "replace", "append"
        Returns:
            string: If Pass, nothing is returned. In case of Fail Exception is returned.
        """
        result = ""
        try:
          with self.engine.connect() as conn:
            result = conn.execute(text(sql_query))
            Logger().kafka_logger(f'Pass - Sent {sql_query} to Redshift tlv_flights db', 'RedshiftOps.read_data')            
        except Exception as err:
            result = err
            Logger().kafka_logger(f'Fail - {err}', 'RedshiftOps.read_data')            

        return result
