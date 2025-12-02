import time
import sys
import os
sys.path.append('/opt/spark')

from realtime_processor.db.writer import write_to_postgres
from realtime_processor.kafka.consumer import connect_to_kafka, create_selection_df_from_kafka
from realtime_processor.spark.connection import create_spark_connection
from realtime_processor.spark.transform import transform_dvc_data

if __name__ == "__main__":

    print("Real-Time Data Processing Application Started ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))


    spark_conn = create_spark_connection()
    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        print("Printing Schema of spark_df: ")

        spark_df.printSchema()

        selection_df = create_selection_df_from_kafka(spark_df)

        transformed_df = transform_dvc_data(selection_df)

        transformed_df.writeStream \
            .foreachBatch(lambda batch_df, batch_id: write_to_postgres(batch_df, batch_id, "pharmaceutical_data_staging")) \
            .start() \
            .awaitTermination()