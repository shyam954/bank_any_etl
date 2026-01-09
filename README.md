Banking ETL Pipeline with PySpark
A production-grade, modular ETL pipeline for banking data processing with support for both local development and AWS S3 deployment.
Show Image
Show Image
Show Image
 Features

Dual Environment Support: Seamlessly switch between local and AWS S3 execution
Modular Architecture: Class-based design for easy maintenance and extension
Banking Domain Logic: KYC verification, transaction analysis, loan risk assessment
Smart Partitioning: Optimized data storage with Parquet format
Comprehensive Transformations: Data cleaning, enrichment, and aggregation
Customer Analytics: 360-degree customer view with risk scoring
Production Ready: Error handling, logging, and data quality checks

📋 Table of Contents

Project Structure
Prerequisites
Installation
Configuration
Usage
Data Pipeline
AWS Deployment
Testing
Troubleshooting
Contributing

📁 Project Structure
banking_project/
├── configs/
│   ├── __init__.py
│   └── etl_config.json              # Environment configurations
├── data/
│   ├── processed/                    # Output location (local)
│   ├── customers.csv                 # Sample customer data
│   ├── loans.csv                     # Sample loan data
│   └── transactions.csv              # Sample transaction data
├── data_creation/
│   └── generating_data.py            # Synthetic data generator
├── dependencies/
│   ├── __init__.py
│   ├── error_utils.py                # Exception handling decorators
│   ├── logging_utils.py              # Centralized logging utilities
│   ├── schemas.py                    # Manual schema definitions
│   └── spark_utils.py                # Spark session with S3 support
├── jobs/
│   ├── __init__.py
│   ├── extract_datasets.py           # Data ingestion (local & S3)
│   ├── transform_customers.py        # Customer transformations
│   ├── transform_loans.py            # Loan transformations
│   ├── transform_transactions.py     # Transaction transformations
│   └── transform_customer_analytics.py  # Analytics aggregations
├── DataOutputWriter/
│   └── writer.py                     # Write to local/S3/Redshift
├── tests/
│   └── test_transformations.py       # Unit tests
├── scripts/
│   └── deploy_to_glue.sh             # AWS Glue deployment script
├── main_pipeline_orc.py              # Main orchestrator
├── requirements.txt                  # Python dependencies
└── README.md                         # This file
 Prerequisites

Python: 3.8 or higher
Java: JDK 8 or 11 (required for PySpark)
AWS Account: (Optional) For S3 and Glue deployment
AWS CLI: (Optional) Configured with credentials

Check Prerequisites
bash# Check Python version
python --version

# Check Java version
java -version

# Check AWS CLI (optional)
aws --version
Installation
1. Clone the Repository
bashgit clone https://github.com/yourusername/banking_project.git
cd banking_project
2. Create Virtual Environment
bash# Create virtual environment
python -m venv banking_project_env

# Activate virtual environment
# On macOS/Linux:
source banking_project_env/bin/activate

# On Windows:
banking_project_env\Scripts\activate
3. Install Dependencies
bashpip install -r requirements.txt
requirements.txt:
pyspark==3.3.2
faker==18.13.0
pandas==2.0.3
boto3==1.28.25
⚙️ Configuration
1. Update Configuration File
Edit configs/etl_config.json with your paths:
json{
    "s3_output_path": "s3a://banking-output/results/",
    "s3_raw_customers": "s3a://banking--data/customers_data/",
    "local_customers": "/absolute/path/to/banking_project/data/customers.csv",
    
    "s3_raw_transactions": "s3a://banking--data/transactions_data/",
    "local_transactions": "/absolute/path/to/banking_project/data/transactions.csv",
    
    "s3_raw_loans": "s3a://banking--data/loans_data/",
    "local_loans": "/absolute/path/to/banking_project/data/loans.csv",
    
    "local_processed_path": "/absolute/path/to/banking_project/data/processed/",
    
    "redshift_jdbc_url": "jdbc:redshift://your-cluster.amazonaws.com:5439/bankdw",
    "redshift_user": "etl_user",
    "redshift_pwd": "secure_pwd_here",
    "log_level": "INFO"
}
2. Generate Sample Data
bashcd data_creation
python generating_data.py
cd ..
This creates three CSV files in the data/ directory:

customers.csv (1,000 records)
transactions.csv (5,000 records)
loans.csv (500 records)

 Usage
Running Locally
bash# Make sure you're in the project root directory
python main_pipeline_orc.py
Expected Output:
================================================================================
 BANKING ETL PIPELINE - STARTING
================================================================================

 PHASE 1: DATA EXTRACTION
--------------------------------------------------------------------------------
[2025-01-09 10:30:15] INFO - extract_customers: Reading customer data from /path/to/customers.csv
[2025-01-09 10:30:16] INFO - extract_customers: Loaded 1000 records from customers.csv
✓ Extraction phase completed

 PHASE 2: DATA TRANSFORMATION
--------------------------------------------------------------------------------
✓ Transformed 850 customer records
✓ Transformed 4998 transaction records
✓ Transformed 500 loan records
✓ Created analytics for 850 customers
✓ Transformation phase completed

 PHASE 3: DATA LOADING
--------------------------------------------------------------------------------
✓ Customers written
✓ Transactions written
✓ Loans written
✓ Analytics written

================================================================================
 ETL PIPELINE COMPLETED SUCCESSFULLY!
================================================================================

 FINAL RECORD COUNTS:
  • Customers: 850
  • Transactions: 4,998
  • Loans: 500
  • Customer Analytics: 850
================================================================================
Switching to S3 Mode
In main_pipeline_orc.py, change these lines:
python# Change environment from 'local' to 's3'
gs = SS.get_spark("s3", app_name="BankETLApp")  # Line 16
data_pull = D(gs, config, "s3")                  # Line 24
writer = DataOutputWriter(gs, config, "s3")      # Line 62
 Data Pipeline
Pipeline Flow
┌─────────────────┐
│  Raw Data (CSV) │
└────────┬────────┘
         │
         ▼
┌─────────────────────────┐
│  EXTRACT PHASE          │
│  - Customers (Schema)   │
│  - Transactions         │
│  - Loans                │
└────────┬────────────────┘
         │
         ▼
┌─────────────────────────────────────┐
│  TRANSFORM PHASE                    │
│  - Data Cleaning                    │
│  - Standardization                  │
│  - Feature Engineering              │
│  - KYC Filtering                    │
│  - Temporal Enrichment              │
│  - Customer Analytics               │
└────────┬────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────┐
│  LOAD PHASE                         │
│  - Partitioned Parquet Files        │
│  - Local/S3 Storage                 │
│  - Optional: Redshift               │
└─────────────────────────────────────┘
Transformations Applied
Customers:

Date type corrections (date_of_birth, account_open_date)
String standardization (initcap, upper, lower)
Email validation with regex
Age and tenure calculations
KYC verification filtering
Deduplication (latest record per customer)

Transactions:

Timestamp parsing
Transaction type validation
Temporal features (year, month, week)
High-value categorization (> $5,000)
Customer-level aggregations
Daily and monthly summaries

Loans:

Date parsing
Amount validation
Monthly repayment estimation
Loan tenure calculation
Default status flagging
Customer loan portfolio metrics

Customer Analytics:

360-degree customer view
Risk scoring (defaults + failed transactions)
Profitability metrics
High-value customer identification (top 10%)

☁️ AWS Deployment
Prerequisites
bash# Configure AWS credentials
aws configure

# Create S3 buckets
aws s3 mb s3://banking--data
aws s3 mb s3://banking-output
Upload Data to S3
bash# Upload raw data
aws s3 cp data/customers.csv s3://banking--data/customers_data/
aws s3 cp data/transactions.csv s3://banking--data/transactions_data/
aws s3 cp data/loans.csv s3://banking--data/loans_data/

# Verify uploads
aws s3 ls s3://banking--data/ --recursive
Deploy to AWS Glue
bash# Make script executable
chmod +x scripts/deploy_to_glue.sh

# Deploy
./scripts/deploy_to_glue.sh
Manual Glue Job Creation
bash# Package dependencies
zip -r banking_etl.zip jobs/ dependencies/ DataOutputWriter/ configs/

# Upload to S3
aws s3 cp banking_etl.zip s3://banking-etl-scripts/
aws s3 cp main_pipeline_orc.py s3://banking-etl-scripts/

# Create Glue job
aws glue create-job \
    --name banking-etl-pipeline \
    --role arn:aws:iam::YOUR_ACCOUNT:role/AWSGlueServiceRole \
    --command '{
        "Name": "glueetl",
        "ScriptLocation": "s3://banking-etl-scripts/main_pipeline_orc.py",
        "PythonVersion": "3"
    }' \
    --default-arguments '{
        "--extra-py-files": "s3://banking-etl-scripts/banking_etl.zip",
        "--TempDir": "s3://banking-output/temp/",
        "--enable-metrics": "true"
    }' \
    --glue-version "4.0" \
    --number-of-workers 5 \
    --worker-type "G.1X"
 Testing
Run Unit Tests
bashpython -m pytest tests/ -v
Run Specific Test
bashpython tests/test_transformations.py
Add Data Quality Checks
Add to main_pipeline_orc.py:
pythondef validate_data_quality(df, dataset_name, min_records=1):
    """Validate data quality"""
    record_count = df.count()
    assert record_count >= min_records, f"{dataset_name} has insufficient records"
    return True

# Use in pipeline:
validate_data_quality(customers_enriched, "Customers", min_records=100)
 Troubleshooting
Common Issues
1. Java Not Found
Error: JAVA_HOME is not set
Solution:
bash# macOS
export JAVA_HOME=$(/usr/libexec/java_home)

# Linux
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk

# Add to ~/.bashrc or ~/.zshrc for persistence
2. PySpark Memory Issues
Error: Java heap space
Solution: Increase Spark memory in spark_utils.py:
python.config("spark.driver.memory", "4g") \
.config("spark.executor.memory", "4g")
3. S3 Access Denied
Error: Access Denied (Service: Amazon S3)
Solution:
bash# Check AWS credentials
aws sts get-caller-identity

# Ensure IAM role has S3 permissions
aws iam attach-role-policy \
    --role-name AWSGlueServiceRole \
    --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
4. Module Not Found
ModuleNotFoundError: No module named 'jobs'
Solution: Ensure you're running from project root and __init__.py files exist:
bashtouch jobs/__init__.py
touch dependencies/__init__.py
touch DataOutputWriter/__init__.py
5. Path Issues in Config
Error: File not found
Solution: Use absolute paths in etl_config.json:
bash# Get absolute path
pwd
# Use full path: /Users/username/Desktop/banking_project/data/...
 Output Data Structure
Local Output
data/processed/
├── customers/
│   ├── branch_code=B001/
│   │   └── part-00000-xxx.parquet
│   ├── branch_code=B002/
│   │   └── part-00000-xxx.parquet
│   └── branch_code=B003/
│       └── part-00000-xxx.parquet
├── transactions/
│   ├── txn_year=2024/
│   │   ├── txn_month=1/
│   │   ├── txn_month=2/
│   │   └── ...
│   └── txn_year=2025/
│       └── ...
├── loans/
│   ├── status=ACTIVE/
│   ├── status=CLOSED/
│   └── status=DEFAULTED/
└── analytics/
    └── customer_metrics/
        └── part-00000-xxx.parquet
Reading Output Data
pythonfrom pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadResults").getOrCreate()

# Read customers
customers = spark.read.parquet("data/processed/customers/")
customers.show()

# Read transactions for specific month
jan_txns = spark.read.parquet("data/processed/transactions/txn_year=2024/txn_month=1/")
jan_txns.show()

# Read customer analytics
analytics = spark.read.parquet("data/processed/analytics/customer_metrics/")
analytics.show()
 Security Best Practices

Never commit credentials: Use environment variables or AWS Secrets Manager
Use IAM roles: For AWS resources instead of access keys
Encrypt data: Enable S3 encryption and use SSL for Redshift
Implement least privilege: Grant minimum necessary permissions
Rotate credentials: Regularly update passwords and access keys

Using Environment Variables
pythonimport os

# In writer.py, replace:
"password": os.environ.get("REDSHIFT_PASSWORD")

# Set in terminal:
export REDSHIFT_PASSWORD="your_secure_password"
📈 Performance Optimization
For Large Datasets
python# In spark_utils.py, add configurations:
.config("spark.sql.shuffle.partitions", "200") \
.config("spark.default.parallelism", "100") \
.config("spark.sql.adaptive.enabled", "true") \
.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
Partition Tuning
python# Adjust partition columns based on query patterns
# High cardinality: txn_year, txn_month
# Low cardinality: branch_code, status
 Contributing
Contributions are welcome! Please follow these steps:

Fork the repository
Create a feature branch (git checkout -b feature/amazing-feature)
Commit your changes (git commit -m 'Add amazing feature')
Push to the branch (git push origin feature/amazing-feature)
Open a Pull Request

Development Guidelines

Follow PEP 8 style guide
Add docstrings to all functions
Write unit tests for new features
Update documentation

👥 Authors

--Shyam Reddy

🙏 Acknowledgments

