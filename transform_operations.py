import json
import pandas as pd
import boto3
from botocore.exceptions import ClientError
import datetime
import os
from logger import Logger


class TransformOp:

    def __init__(self) -> None:
        pass

    def json_to_csv(self, data_set):
        """Input as python dict/json convert to csv and save to csv file 

        Args:
            data_set (dict): Python Dict as input to convert to csv
            return: tuple with csv and gzip of the dataset
        """
        try:
            df = pd.read_json(json.dumps(data_set))
            time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            cur_dir = os.getcwd()
            df.to_csv(f'{cur_dir}/data_{time}.csv', sep= '\t')
            df.to_csv(f'{cur_dir}/data_{time}.gz',  sep= '\t', compression='gzip')
            r = (f'{cur_dir}/data_{time}.csv',
                  f'{cur_dir}/data_{time}.gz')
            Logger().kafka_logger(f'Pass - Converted data frame to csv and gz', 'TransformOp.json_to_csv')
        except Exception as err:
            r = err
            Logger().kafka_logger(f'Fail - {err}', 'TransformOp.json_to_csv')

        return r

    def copy_to_s3_bucket(self, csv_path):
        """Get the path to the csv file and copy the file to s3 bucket as raw file 

        Args:
            csv_path (str): path to the created csv file
        """
        # Upload the file
        s3_client = boto3.client('s3', 
                                 aws_access_key_id='<access_key_id>', 
                                 aws_secret_access_key= '<secret_key>')
        try:
            s3_client.upload_file(csv_path, '<s3_bucket>', csv_path.split('/')[-1])
            os.remove(csv_path)
            Logger().kafka_logger(f'Pass - Successfuly upload {csv_path} to maxim-final-proj-s3', 'TransformOp.copy_to_s3_bucket')
            
        except ClientError as err:
            Logger().kafka_logger(f'Fail - {err}', 'TransformOp.copy_to_s3_bucket')
            return False
        
        return True


