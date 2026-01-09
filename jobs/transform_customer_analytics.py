from pyspark.sql.functions import *
from pyspark.sql.window import *
from dependencies.logging_utils import get_logger
from dependencies.error_utils import exception_handler

class CustomerAnalytics:
    def __init__(self):
        self.logger = get_logger("transform_loans")


    def join_datasets(self,customers_df, transactions_df, loans_df):
        
        self.logger.info("Joining datasets")

        # Aggregate transactions per customer first
        txn_agg = transactions_df.groupBy("customer_id").agg(
            count("*").alias("total_transactions"),
            sum("txn_amount").alias("total_txn_amount"),
            sum(when(col("txn_type") == "DEPOSIT", col("txn_amount")).otherwise(0)).alias("total_deposits"),
            sum(when(col("txn_type") == "WITHDRAWAL", col("txn_amount")).otherwise(0)).alias("total_withdrawals"),
            sum(when(col("status") == "FAILED", 1).otherwise(0)).alias("failed_transactions"))

        # Aggregate loans per customer
        loan_agg = loans_df.groupBy("customer_id").agg(
            sum("principal_amount").alias("total_loan_principal"),
            sum("monthly_repayment_estimate").alias("total_monthly_repayment"),
            sum(when(col("loan_status_flag") == 1, 1).otherwise(0)).alias("defaulted_loans_count"),
            sum(when(col("status") == "ACTIVE", 1).otherwise(0)).alias("active_loans_count")
        )

        # Join all datasets
        joined_df = customers_df.join(txn_agg, "customer_id", "left")\
                                .join(loan_agg, "customer_id", "left")

        self.logger.info("Datasets joined successfully")
        return joined_df


    def calculate_customer_metrics(self,df):
        
        self.logger.info("Calculating customer-level metrics")

        df_metrics = df.withColumn(
            "customer_risk_score",
            col("defaulted_loans_count") * 0.7 + col("failed_transactions") * 0.3
        ).withColumn(
            "customer_profitability",
            col("total_deposits") - col("total_withdrawals") + col("total_monthly_repayment")
        )



        # Identify top 10% high-value customers by transaction + loan amount
        #window = Window.orderBy(col("total_txn_amount") + col("total_loan_principal").desc())


        window = Window.orderBy((coalesce(col("total_txn_amount"), lit(0.0)) +
     coalesce(col("total_loan_principal"), lit(0.0))).desc())
        
        df_metrics = df_metrics.withColumn(
            "percent_rank",
            percent_rank().over(window)
        ).withColumn(
            "high_value_customer",
            when(col("percent_rank") >= 0.9, 1).otherwise(0)
        ).drop("percent_rank")

        self.logger.info("Customer-level metrics calculation completed")
        return df_metrics


    def transform_customer_analytics(self,customers_df, transactions_df, loans_df):
        
        self.logger.info("Customer analytics pipeline started")

        joined_df = self.join_datasets(customers_df, transactions_df, loans_df)
        final_df = self.calculate_customer_metrics(joined_df)

        self.logger.info("Customer analytics pipeline completed")
        return final_df





