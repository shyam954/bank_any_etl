from jobs.extract_datasets import data_ingestion as D 
from dependencies.schemas import manual_schema as MS
from dependencies.spark_utils import spark_session as SS
import json


from jobs.transform_customers import Transformations_on_customers
from jobs.transform_loans import Transformations_on_loans
from jobs.transform_transactions import Transformations_On_transactions
from jobs.transform_customer_analytics import CustomerAnalytics
from DataOutputWriter.writer import DataOutputWriter

def run_etl():
    # Load config
    with open("/Users/shyamsundarreddysureddy/Desktop/banking_project/configs/etl_config.json", "r") as f:
        config = json.load(f)

    #  Initialize Spark session with S3 nad local support 
    gs = SS.get_spark("local",app_name="BankETLApp")

    # Extract Customers
    cs = MS.customers_schema()
    data_pull = D(gs, config, "local")
    customers_df = data_pull.extract_customers(cs)

    # Transform Customers
    customer_transformer = Transformations_on_customers()
    customers_enriched = customer_transformer.final_customers(customers_df)

    # Extract Transactions 
    ts = MS.transactions_schema()
    transactions_df = data_pull.extract_transactions(ts)

    # Transform Transactions
    transaction_transformer = Transformations_On_transactions()
    transactions_transformed = transaction_transformer.transform_transactions(transactions_df)
    transactions_enriched = transactions_transformed["transactions_enriched"]

    # Extract Loans
    ls = MS.loan_schema()
    loans_df = data_pull.extract_loans(ls)

    # Transform Loans
    loan_transformer = Transformations_on_loans()
    loans_transformed = loan_transformer.transform_loans(loans_df)
    loans_enriched = loans_transformed["loans_enriched"]

    # Customer Analytics
    analytics_obj = CustomerAnalytics()
    customer_metrics_df = analytics_obj.transform_customer_analytics(
        customers_enriched, transactions_enriched, loans_enriched
    )

    # Write all datasets to S3 or local 
    writer = DataOutputWriter(gs, config, "local")
    writer.write_customers(customers_enriched)
    writer.write_transactions(transactions_enriched)
    writer.write_loans(loans_enriched)
    writer.write_analytics(customer_metrics_df, "customer_metrics")

    print("ETL pipeline completed successfully!")

# Entry point
if __name__ == "__main__":
    run_etl()
