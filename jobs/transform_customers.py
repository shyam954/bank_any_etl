from pyspark.sql.functions import *
from pyspark.sql.window import *
from dependencies.logging_utils import get_logger
from dependencies.error_utils import exception_handler

class Transformations_on_customers:
    
    def __init__(self):
        self.logger = get_logger("transform_customers")

    
    def transform_customers (self,df):

        self.logger.info("correcting date types")
        cust_date_type_change=df.withColumn("date_of_birth",to_date(col("date_of_birth"),"yyyy-MM-dd"))\
            .withColumn("account_open_date",to_date(col("account_open_date"),"yyyy-MM-dd"))
    
        self.logger.info("Standardizing string columns")
        sdf = cust_date_type_change \
        .withColumn("first_name", initcap(trim("first_name"))) \
        .withColumn("last_name", initcap(trim("last_name"))) \
        .withColumn("email", lower(trim("email"))) \
        .withColumn("branch_code", upper(trim("branch_code"))) \
        .withColumn("kyc_status", upper(trim("kyc_status")))

        #Email validation flag
        email_regex = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
        self.logger.info("Validating email format")
        edf = sdf.withColumn(
        "is_valid_email",
        when(col("email").rlike(email_regex), lit(1)).otherwise(lit(0)) )

        #Derived columns
        self.logger.info("Removing duplicate customers")
        ddf = edf .withColumn(
            "age",
            floor(datediff(current_date(),col("date_of_birth")) / 365)
        ) \
        .withColumn(
            "tenure_years",
            floor(datediff(current_date(), col("account_open_date")) / 365)
        ) \
        .withColumn(
            "kyc_verified_flag",
            when(col("kyc_status") == "VERIFIED",lit(1)).otherwise(lit(0))
        )
        # Deduplication (latest account per customer)
        self.logger.info("Removing duplicate customers")
        window_spec = Window.partitionBy("customer_id") \
                        .orderBy(col("account_open_date").desc())

        wdf = ddf.withColumn("rn", row_number().over(window_spec)) \
           .filter(col("rn") == 1) \
           .drop("rn")
        # Data quality filters
        self.logger.info("Applying data quality filters")
        fdf = wdf.filter(
        col("customer_id").isNotNull() &
        col("branch_code").isNotNull()
         )
        self.logger.info("Customer data transformation completed successfully")
        return fdf


    def kyc_verified_customers(self,df):
        # Flag and filter only verified KYC customers
        df_clean = df.withColumn("kyc_status", upper(col("kyc_status")))
        df_clean = df_clean.filter(col("kyc_status") == "VERIFIED")
        return df_clean
    
    def final_customers(self,df):
        cleaned_customers=self.transform_customers(df)
        verified_customers=self.kyc_verified_customers(cleaned_customers)
        return verified_customers



 




























