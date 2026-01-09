from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType,DoubleType

class manual_schema:
    def customers_schema ():
        cust_schema = StructType([
            StructField("customer_id", IntegerType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("date_of_birth", StringType(), True),
            StructField("email", StringType(), True),
            StructField("account_open_date", StringType(), True),
            StructField("kyc_status", StringType(), True),
            StructField("branch_code", StringType(), True)
        ])
        return cust_schema


    def transactions_schema():
        trans_schema = StructType([
            StructField("txn_id", StringType(), True),
            StructField("customer_id", IntegerType(), True),
            StructField("txn_timestamp", StringType(), True),
            StructField("txn_amount", DoubleType(), True),
            StructField("txn_type", StringType(), True),
            StructField("device_id", StringType(), True),
            StructField("location", StringType(), True),
            StructField("status", StringType(), True)
        ])
        return trans_schema



    def loan_schema():
        loans_schema = StructType([
            StructField("loan_id", StringType(), True),
            StructField("customer_id", IntegerType(), True),
            StructField("opening_date", StringType(), True),
            StructField("principal_amount", DoubleType(), True),
            StructField("interest_rate", DoubleType(), True),
            StructField("status", StringType(), True)
        ])
        return loans_schema