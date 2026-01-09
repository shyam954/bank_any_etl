import logging
from dependencies.logging_utils import get_logger
from dependencies.error_utils import exception_handler
from pyspark.sql.window import *
from pyspark.sql.functions import *

class DataOutputWriter:

    def __init__(self, spark, config,env):
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.local_base_path = config["local_processed_path"]
        self.s3_output_path=config["s3_output_path"]
        self.env=env

    # Customers

    def write_customers(self, df):
        self.logger.info("Writing customers data")
        if self.env.lower() =="local":
            df.write \
            .mode("overwrite") \
            .partitionBy("branch_code") \
            .parquet(f"{self.local_base_path}customers/")
        elif self.env.lower() =="s3":
            df.write \
            .mode("overwrite") \
            .partitionBy("branch_code") \
            .parquet(f"{self.s3_output_path}customers/")

    # Transactions
  
    def write_transactions(self, df):
        self.logger.info("Writing transactions data")
        if self.env.lower() =="local":
            df.write \
            .mode("overwrite") \
            .partitionBy("txn_year", "txn_month") \
            .parquet(f"{self.local_base_path}transactions/")

        elif self.env.lower() =="s3":
            df.write \
            .mode("overwrite") \
            .partitionBy("txn_year", "txn_month") \
            .parquet(f"{self.s3_output_path}transactions/")
     
    # Loans
 
    def write_loans(self, df):
        self.logger.info("Writing loans data")
        if self.env.lower() =="local":
            df.write \
            .mode("overwrite") \
            .partitionBy("status") \
            .parquet(f"{self.local_base_path}loans/")
        elif self.env.lower() =="s3":
            df.write \
            .mode("overwrite") \
            .partitionBy("status") \
            .parquet(f"{self.s3_output_path}loans/")
 
    # Analytics Outputs
   
    def write_analytics(self, df, dataset_name):
        self.logger.info(f"Writing analytics dataset: {dataset_name}")
        if self.env.lower() =="local":
            df.write \
            .mode("overwrite") \
            .parquet(f"{self.local_base_path}/analytics/{dataset_name}")

        elif self.env.lower() =="s3":
            df.write \
            .mode("overwrite") \
            .parquet(f"{self.s3_output_path}/analytics/{dataset_name}")
  
    # Optional: Redshift
   
    def write_to_redshift(self, df, table_name):
        self.logger.info(f"Writing {table_name} to Redshift")

        df.write \
            .format("jdbc") \
            .option("url", self.config["redshift_jdbc_url"]) \
            .option("dbtable", table_name) \
            .option("user", self.config["redshift_user"]) \
            .option("password", self.config["redshift_pwd"]) \
            .option("driver", "com.amazon.redshift.jdbc.Driver") \
            .mode("overwrite") \
            .save()


