from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

def init_spark():
    spark = SparkSession.builder \
            .appName("Ecommerce-Analytics-Bronze-Ingestion") \
            .master("local[*]") \
            .config("spark.driver.memory", "6g") \
            .config("spark.executor.memory", "6g") \
            .config("spark.sql.shuffle.partitions", "200") \
            .getOrCreate()
    
    return spark

def ingest_raw_to_bronze():
    spark = init_spark()

    raw_data = "data/raw_data/01-log-tracking.csv"
    bronze_output = "data/bronze/"

    df = spark.read.csv(raw_data, header=True)

    df.show(1)
    df.printSchema()

    #cast structure fields into correct business and mathematical types
    standardize_df = df \
    .withColumn("event_time", to_timestamp(col("event_time"))) \
    .withColumn("event_type", col("event_type").cast("string")) \
    .withColumn("product_id", col("product_id").cast("long")) \
    .withColumn("category_id", col("category_id").cast("long")) \
    .withColumn("category_code", col("category_code").cast("string")) \
    .withColumn("brand", col("brand").cast("string")) \
    .withColumn("price", col("price").cast("double")) \
    .withColumn("user_id", col("user_id").cast("long")) \
    .withColumn("user_session", col("user_session").cast("string"))

    total_row = standardize_df.count()
    print("Total row: ", total_row)

    # Checking for missing values
    for column in standardize_df.columns:
        null_count = standardize_df.filter(col(column).isNull()).count()
        print(f'Column {column}: {null_count} null values')
    
    # Save to Bronze folder in parquet format
    standardize_df.write.mode('overwrite').format('parquet').save(bronze_output)
    spark.stop()

if __name__ == "__main__":
    ingest_raw_to_bronze()