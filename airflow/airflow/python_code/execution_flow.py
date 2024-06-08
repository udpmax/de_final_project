from kafka_operations import *
from transform_operations import *
from data_analysis import *
from redshift_operations import *
from mongodb_operations import *


if __name__ == '__main__':
    os.chdir('/opt/airflow/python_code/')
    kafka_class = KafkaData()
    transform_ops = TransformOp()
    data = kafka_class.produce_data()
    csv_file = transform_ops.json_to_csv(data)
    transform_ops.copy_to_s3_bucket(csv_file[1])
    data_analysis_df  = kafka_class.consume_data(csv_file[0])
    data_analysis = DataAnalysis(data_analysis_df)
    clean_df = data_analysis.clean_and_transform()
    el_al_df = data_analysis.el_al_on_time_flight(clean_df)
    RedshiftOps(clean_df).send_data()
    MongoDbOps().insert_many(el_al_df)




    

