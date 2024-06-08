import json
from IPython.display import display
import pandas as pd
from logger import Logger

class DataAnalysis:

    def __init__(self, kaf_data) -> None:
        self.df: pd = kaf_data


    def clean_and_transform(self):
        """Clean (reindex, rename columns, convert datatypes, drop not neaded columns) data Frame and return the new transformed data frame

        Returns:
            Data: DataFrame without duplicates
        """
        try:
            droped_c_df: pd = self.df.drop(columns=['CHLOC1TH', 'CHLOC1CH', 'CHRMINH', 'Unnamed: 0', '_id'])
            droped_c_df.reset_index(inplace=True, drop=True)
            renamed_c_df = droped_c_df.rename(columns={'CHOPER': 'operator_code', 
                                                    'CHOPERD': 'operator_name',
                                                    'CHFLTN': 'flight_number', 
                                                    'CHSTOL': 'schedualed_time', 
                                                    'CHPTOL': 'actual_time',
                                                    'CHAORD': 'area',
                                                    'CHLOC1': 'airport_code',
                                                    'CHLOC1D': 'airport_name',
                                                    'CHLOC1T': 'city_name',
                                                    'CHLOCCT':  'country_name',
                                                    'CHTERM': 'terminal',
                                                    'CHCINT': 'check_in_counter',
                                                    'CHCKZN': 'check_in_zone',
                                                    'CHRMINE': 'flight_status'
                                                    })
            converted_datatypes_df: pd = renamed_c_df.astype(
                                                    {'operator_code': 'string', 
                                                    'operator_name': 'string',
                                                    'flight_number': 'string', 
                                                    'schedualed_time': 'datetime64[ns]', 
                                                    'actual_time': 'datetime64[ns]',
                                                    'area': 'string',
                                                    'airport_code': 'string',
                                                    'airport_name': 'string',
                                                    'city_name': 'string',
                                                    'country_name':  'string',
                                                    'terminal': 'string',
                                                    'check_in_counter': 'string',
                                                    'check_in_zone': 'string',
                                                    'flight_status': 'string'
                                                    })
            
            r = converted_datatypes_df.drop_duplicates(inplace=True)
            Logger().kafka_logger('Pass - Clean and transformed data with Pandas', 'DataAnalysis.clean_and_transform')
        except Exception as err:
            r = err
            Logger().kafka_logger(f'Fail - {err}', 'DataAnalysis.clean_and_transform')

        return converted_datatypes_df
    
    def el_al_on_time_flight(self, clean_df):
        """
        Get the Clean and data and return new data frame with el-al flights that were not late
        return: DataFrame 
        Args:
            clean_df (DataFrame): Clean data frame as returned from the clean_and_transform function
        """
        try:
            only_el_al: pd= (clean_df[clean_df['operator_code'] == 'LY']) 
            r: pd = only_el_al[only_el_al['actual_time'] <= only_el_al['schedualed_time']]
            Logger().kafka_logger('Pass - Created new data frame with el-al flights which are not late', 'DataAnalysis.el_al_on_time_flight')
        except Exception as err:
            r = err
            Logger().kafka_logger(f'Fail - {err}', 'DataAnalysis.el_al_on_time_flight')
        return r