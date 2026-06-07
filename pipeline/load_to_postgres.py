from pyspark.sql import SparkSession
import os
import urllib.request
def download_jar(version='42.7.3'):
    jar_name = f"postgresql-{version}.jar"
    target_path = f'jars/{jar_name}'
    if not os.path.exists('jars'):
        os.makedirs('jars')

    if not os.path.exists(target_path):
        print(f"""File {jar_name} hasn't available yet. Auto download into jars/...""")
        maven_url = f"https://repo1.maven.org/maven2/org/postgresql/postgresql/{version}/{jar_name}"

        try:
            urllib.request.urlretrieve(maven_url, target_path)
            print(f"Download successfully: {target_path}")
        except Exception as e:
            print(f"Cannot download jar file: {e}")
            return None
    return target_path

def init_spark():
    jar_path = download_jar()

    spark = SparkSession.builder \
        .appName("Ecommerce-Analytics-Load-To-Postgres") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "200")
    
    if jar_path:
        spark = spark.config("spark.jars", jar_path)
        print(f"Spark session loading jar from: {jar_path}")

    return spark.getOrCreate()

def load_to_postgres():
    spark = init_spark()

    gold_path = "data/gold"

    jdbc_url = "jdbc:postgresql://localhost:5432/log_tracking_db"

    db_properties = {
        "user": "postgres",
        "password": "dinhhieu21",
        "driver": "org.postgresql.Driver",
        "batchsize": "50000"
    }

    data_paths = {
        "sales_trend": f"{gold_path}/sales_trend",
        "brand_analysis": f"{gold_path}/brand_analysis",
        "rfm_segmented": f"{gold_path}/rfm_segmented"
    }

    for table_name, data_path in data_paths.items():
        print(f'Reading data from {table_name}')
        df = spark.read.parquet(data_path)
        print(f'Writing table {table_name} into postgreSQL')
        df.write.jdbc(
            url=jdbc_url,
            table=table_name,
            mode="overwrite",
            properties=db_properties
        )
        print(f'Writing table {table_name} into db successfully')

    spark.stop()

if __name__ == '__main__':
    load_to_postgres()