from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum, max, to_date, count, expr, datediff, lit

def init_spark():
    spark = SparkSession.builder \
        .appName('Ecommerce-Analytic-Exploratory-Gold') \
        .master('local[*]') \
        .config('spark.driver.memory', '6g') \
        .config('spark.executor.memory', '6g') \
        .config('spark.sql.shuffle.partitions', '200') \
        .getOrCreate()
    
    return spark

def exploratory_to_gold():
    spark  = init_spark()

    data_path = 'data/silver'
    output_path = 'data/gold'
    users = spark.read.parquet(f'{data_path}/users')
    products = spark.read.parquet(f'{data_path}/products')
    categories = spark.read.parquet(f'{data_path}/categories')
    events = spark.read.parquet(f'{data_path}/events')

    sales_trend = events.groupBy(to_date(col('event_time').alias('event_date'))) \
                        .agg(
                            sum(when(col('event_type') == 'view', 1).otherwise(0).alias('total_views')),
                            sum(when(col('event_type') == 'cart', 1).otherwise(0).alias('total_carts')),
                            sum(when(col('event_type') == 'purchase', 1).otherwise(0).alias('total_purchases')),
                            sum(when(col('event_type') == 'purchase', col('captured_price')).otherwise(0.0).alias('total_revenue')),
                        )
    
    print('Sales trend: ')
    sales_trend.printSchema()
    sales_trend.show(5)

    brand_analysis = events.filter(col('event_type') == 'purchase') \
                    .join(products, 'product_id', 'inner') \
                    .groupBy('brand') \
                    .agg(
                        count('product_id').alias('total_orders_sold'),
                        sum('captured_price').alias('total_brand_revenue')
                    ).orderBy(col('total_brand_revenue').desc())
    
    print('Brand Analysis: ')
    brand_analysis.printSchema()
    brand_analysis.show(5)


    max_sub_date = events.select(max('event_time')).collect()[0][0]
    rfm_base = events.filter(col('event_type') == 'purchase') \
                    .groupBy('user_id') \
                    .agg(
                        datediff(lit(max_sub_date), max('event_time')).alias('recency'),
                        count('user_session').alias('frequency'),
                        sum('captured_price').alias('monetary')
                    ).orderBy('monetary')
    
    rfm_segmented = rfm_base.withColumn(
        "segment",
        expr("""
            CASE    
                WHEN recency <= 7 AND frequency >= 10 THEN 'Champions (VIP)'
                WHEN recency <= 14 AND frequency >= 3 THEN 'Loyal Customers'
                WHEN recency <= 21 AND recency <= 1 THEN 'At Risk'
                ELSE 'Lost'
            END 
        """)
    )

    print("::: Writing data into Gold Layer...")
    sales_trend.write.mode("overwrite").format("parquet").save(output_path)
    brand_analysis.write.mode("overwrite").format("parquet").save(output_path)
    rfm_segmented.write.mode("overwrite").format("parquet").save(output_path)
    
    print("::: Successfully")
    spark.stop()

if __name__ == "__main__":
    exploratory_to_gold()