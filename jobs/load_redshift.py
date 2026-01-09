"""def load_to_redshift(df, table_name, config):
    df.write \
        .format("jdbc") \
        .option("url", config["redshift_jdbc_url"]) \
        .option("dbtable", table_name) \
        .option("user", config["redshift_user"]) \
        .option("password", config["redshift_pwd"]) \
        .mode("overwrite") \
        .save()"""



from pyspark.sql.functions import *
from jobs.extract_datasets import data_ingestion as D 
from dependencies.schemas import manual_schema as MS
from dependencies.spark_utils import spark_session as SS
import json
from pyspark.sql.window import *
from dependencies.logging_utils import get_logger
from dependencies.error_utils import exception_handler
from jobs.transform_customers import Transformations_on_customers
from jobs.transform_loans import Transformations_on_loans as tl
from jobs.transform_transactions import Transformations_On_transactions as tt




# --- Read config from JSON file ---
with open("configs/etl_config.json", "r") as f:
     config = json.load(f)


gs =SS.get_spark()

cs =MS.customers_schema()
print(cs)
"""c=D.extract_customers(gs,config,cs)





obj_1=Transformations_on_customers()
r=obj_1.final_customers(c)
r.show()
"""