from pyspark.sql import SparkSession

class spark_session:
    @staticmethod
    def get_spark(env,app_name="BankETLApp"):

        if env.lower() == "s3":
            return (
                SparkSession.builder
                .appName(app_name)
                # dynamic overwrite for partitions
                .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
                # S3A filesystem
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                # AWS credentials from ~/.aws/credentials
                .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
                # add Hadoop AWS and AWS SDK JARs
                .config("spark.jars.packages",
                        "org.apache.hadoop:hadoop-aws:3.3.4,"
                        "com.amazonaws:aws-java-sdk-bundle:1.12.262")
                .getOrCreate()
            )
        elif env.lower() == "local":
            return SparkSession.builder.appName(app_name).getOrCreate()
            

 