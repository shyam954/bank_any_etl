
from pyspark.sql.functions import *
from pyspark.sql.window import *
from dependencies.logging_utils import get_logger
from dependencies.error_utils import exception_handler

class Transformations_on_loans:
    def __init__(self):
        self.logger = get_logger("transform_loans")

    def clean_loans(self,ldf):
        """
        Clean loan dataset:
        - Convert opening_date to DateType
        - Validate principal_amount and interest_rate
        """
        self.logger.info("Starting loan data cleaning")

        df = (
            ldf
            .withColumn("opening_date", to_date(col("opening_date"), "yyyy-MM-dd"))
            .withColumn("principal_amount_valid", when(col("principal_amount") > 0, lit(1)).otherwise(lit(0)))
            .withColumn("interest_rate_valid", when(col("interest_rate") > 0, lit(1)).otherwise(lit(0)))
        )

        self.logger.info("Loan data cleaning completed")
        return df


    def enrich_loans(self,df):
        """
        Add derived columns:
        - loan_tenure_days
        - loan_status_flag (DEFAULTED=1, ACTIVE/CLOSED=0)
        - monthly_repayment_estimate
        """
        self.logger.info("Starting loan enrichment")

        enriched_df = (
            df
            .withColumn("loan_tenure_days", datediff(current_date(), col("opening_date")))
            .withColumn("loan_status_flag", when(col("status") == "DEFAULTED", 1).otherwise(0))
            .withColumn(
                "monthly_repayment_estimate",
                round((col("principal_amount") * (1 + col("interest_rate")/100)) / 12, 2)
            )
        )

        self.logger.info("Loan enrichment completed")
        return enriched_df


    def customer_loan_metrics(self,df):
        """
        Aggregate loan metrics per customer:
        - total principal
        - total interest (estimated)
        - active loans count
        """
        self.logger.info("Computing customer-level loan metrics")

        metrics_df = df.groupBy("customer_id").agg(
            sum("principal_amount").alias("total_principal"),
            sum(col("principal_amount") * col("interest_rate") / 100).alias("total_interest"),
            sum(when(col("status") == "ACTIVE", 1).otherwise(0)).alias("active_loans_count")
        )

        self.logger.info("Customer-level loan metrics computed")
        return metrics_df


    def transform_loans(self,ldf):
        """
        Complete loan transformation pipeline:
        - Clean
        - Enrich
        - Aggregate
        Returns a dictionary of DataFrames
        """
        self.logger.info("Loan transformation pipeline started")

        cleaned_df = self.clean_loans(ldf)
        enriched_df = self.enrich_loans(cleaned_df)
        customer_metrics_df = self.customer_loan_metrics(enriched_df)

        self.logger.info("Loan transformation pipeline completed")

        return {
            "loans_enriched": enriched_df,
            "customer_metrics": customer_metrics_df
        }



 