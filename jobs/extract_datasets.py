from dependencies.logging_utils import get_logger
from dependencies.error_utils import exception_handler

class data_ingestion:

    def __init__(self,spark,config,env):
        self.spark=spark
        self.env=env
        self.config=config

    @exception_handler(get_logger("extract_customers"))
    def extract_customers(self,schema):
        logger = get_logger("extract_customers")
        if self.env.lower() == "local":
            path = self.config["local_customers"]
            logger.info(f"Reading customer data from {path}")

            cdf = self.spark.read.format("csv").option("header",True).schema(schema).load(path)
            logger.info(f"Loaded {cdf.count()} records from customers.csv")
            return cdf
        elif  self.env.lower() == "s3":
            path = self.config["s3_raw_customers"]
            logger.info(f"Reading customer data from {path}")

            cdf = self.spark.read.format("csv").option("header",True).schema(schema).load(path)
            logger.info(f"Loaded {cdf.count()} records from customers.csv")
            return cdf

       


    @exception_handler(get_logger("extract_transactions"))
    def extract_transactions(self,schema):
        logger = get_logger("extract_transactions")
        if self.env.lower() =="local":
            path = self.config["local_transactions"]
            logger.info(f"Reading transactions data from {path}")

            tdf = self.spark.read.format("csv").option("header",True).schema(schema).load(path)

            logger.info(f"Loaded {tdf.count()} records from transactions.csv")
            return tdf
        elif self.env.lower() =="s3":
            path = self.config["s3_raw_transactions"]
            logger.info(f"Reading transactions data from {path}")

            tdf = self.spark.read.format("csv").option("header",True).schema(schema).load(path)

            logger.info(f"Loaded {tdf.count()} records from transactions.csv")
            return tdf



        

    @exception_handler(get_logger("extract_loans"))
    def extract_loans(self,schema):
        logger = get_logger("extract_loans")


        if self.env.lower() =="local":
            path = self.config["local_loans"]
            logger.info(f"Reading loans data from {path}")

            ldf = self.spark.read.format("csv").option("header",True).schema(schema).load(path)

            logger.info(f"Loaded {ldf.count()} records from loans.csv")
            return ldf
        elif self.env.lower() =="s3":
            path = self.config["s3_raw_loans"]
            logger.info(f"Reading loans data from {path}")

            ldf = self.spark.read.format("csv").option("header",True).schema(schema).load(path)

            logger.info(f"Loaded {ldf.count()} records from loans.csv")
            return ldf