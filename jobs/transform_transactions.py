from pyspark.sql.functions import *
from dependencies.logging_utils import get_logger
from dependencies.error_utils import exception_handler


class Transformations_On_transactions:
    def __init__(self):
        self.logger = get_logger("transform_transactions")

    
    def clean_transactions(self,tdf):
        self.logger.info("Starting transaction data cleaning")

        df = (
            tdf
            .withColumn(
                "txn_timestamp",
                to_timestamp(col("txn_timestamp"), "yyyy-MM-dd HH:mm:ss")
            )
            .withColumn(
                "txn_type",
                upper(trim(col("txn_type")))
            )
            .withColumn(
                "txn_type",
                when(col("txn_type").isin("DEPOSIT", "WITHDRAWAL", "TRANSFER"),
                    col("txn_type"))
                .otherwise("UNKNOWN")
            )
            .withColumn(
                "is_valid_amount",
                when(col("txn_amount") > 0, lit(1)).otherwise(lit(0))
            )
        )

        self.logger.info("Transaction cleaning completed")
        return df


    def enrich_transactions(self,df, high_value_threshold=5000):
        self.logger.info("Starting transaction enrichment")

        enriched_df = (
            df
            .withColumn("txn_date", to_date(col("txn_timestamp")))
            .withColumn("txn_year", year(col("txn_timestamp")))
            .withColumn("txn_month", month(col("txn_timestamp")))
            .withColumn("txn_week", weekofyear(col("txn_timestamp")))
            .withColumn(
                "txn_category",
                when(col("txn_amount") >= high_value_threshold, "HIGH_VALUE")
                .otherwise("NORMAL")
            )
        )

        self.logger.info("Transaction enrichment completed")
        return enriched_df



    def customer_transaction_metrics(self,df):
        self.logger.info("Computing customer-level transaction metrics")

        metrics_df = df.groupBy("customer_id").agg(
            count("*").alias("total_transactions"),
            sum("txn_amount").alias("total_amount"),
            avg("txn_amount").alias("average_amount"),
            sum(
                when(col("status") == "FAILED", 1).otherwise(0)
            ).alias("failed_transactions_count")
        )

        self.logger.info("Customer transaction metrics computation completed")
        return metrics_df




    def transaction_time_aggregates(self,df):
        self.logger.info("Computing time-based transaction aggregates")

        daily_df = df.groupBy("txn_date").agg(
            count("*").alias("daily_txn_count"),
            sum("txn_amount").alias("daily_total_amount")
        )

        monthly_df = df.groupBy("txn_year", "txn_month").agg(
            count("*").alias("monthly_txn_count"),
            sum("txn_amount").alias("monthly_total_amount")
        )

        self.logger.info("Time-based aggregation completed")
        return daily_df, monthly_df


    def transform_transactions(self,tdf):
        self.logger.info("Transaction transformation pipeline started")

        cleaned_df = self.clean_transactions(tdf)
        enriched_df = self.enrich_transactions(cleaned_df)

        customer_metrics_df = self.customer_transaction_metrics(enriched_df)
        daily_df, monthly_df = self.transaction_time_aggregates(enriched_df)

        self.logger.info("Transaction transformation pipeline completed")

        return {
            "transactions_enriched": enriched_df,
            "customer_metrics": customer_metrics_df,
            "daily_summary": daily_df,
            "monthly_summary": monthly_df
        }









