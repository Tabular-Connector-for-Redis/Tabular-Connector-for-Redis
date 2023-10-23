import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import requests


class RedisLoader(DataFrame):
    def __init__(self, tbl_name: str, host_name: str, port: str, data: DataFrame):
        super().__init__(data._jdf, data.sparkSession)

        self.table = tbl_name
        self.host_name = host_name
        self.port = port
        self.df = data

        self.end_point = (
            f"http://{self.host_name}:{self.port}/api/v1/schema/{tbl_name}/load"
        )

    def load_redis(self):
        csv_str = self.df.toPandas().to_csv(index=False)
        res_code = requests.post(
            url=self.end_point, data=csv_str, headers={"Content-Type": "text/csv"}
        )
        return res_code


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()

    # Test DataFrame Creation
    cols = ["col2_string", "col1_int", "col3_string", "col4_int"]
    data = [["AMP", "1234", "APAC", 1234], ["ANZ", "4564", "APAC", 5678]]
    df = spark.createDataFrame(data, cols)

    # Initialize RedisLoader class by passing target table name, host name, port and data frame,
    # that needs to be loaded in REDIS and then call load_redis() method
    # Pass table_name, host_name and port as command line parameters with spark-submit
    table_name = sys.argv[1]
    host_name = sys.argv[2]
    port = sys.argv[3]

    redis = RedisLoader(table_name, host_name, port, df)
    res = redis.load_redis()
    if res.status_code >= 300:
        logging.log(logging.ERROR, f"Failed to load table : {table_name} to redis")
        sys.exit(-1)
    logging.log(logging.INFO, f"Table : {table_name} loaded")
