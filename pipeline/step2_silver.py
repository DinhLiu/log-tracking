from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, when, try_element_at, lit

def init_spark():
	spark = SparkSession.builder \
		.appName('Ecommerce-Analytics-Silver-Normalizations') \
		.master('local[*]') \
		.config('spark.driver.memory', '6g') \
		.config('spark.executor.memory', '6g') \
		.config('spark.sql.shuffle.partitions', '200') \
		.getOrCreate()
	return spark

def normalize_to_silver():
	spark = init_spark()

	bronze_path = 'data/bronze/'
	silver_path = 'data/silver/'
	df = spark.read.parquet(bronze_path)

	users_df = df.select('user_id').distinct().filter(col('user_id').isNotNull())
	category_parsed = df.select('category_id', 'category_code').distinct() \
			.filter(col('category_id').isNotNull()) \
			.withColumn('code_parts', split(col('category_code'), '\.'))

	
	categories_df = category_parsed.withColumn(
		"main_category", try_element_at(col("code_parts"), lit(1))
	).withColumn(
		"sub_category", try_element_at(col("code_parts"), lit(2))
	).withColumn(
		"product_type", try_element_at(col("code_parts"), lit(3))
	).drop("code_parts")

	products_df = df.select(
		'product_id', 'brand', 'category_id'
	).distinct().filter(col('product_id').isNotNull())

	events_df = df.select(
		'event_time', 'event_type', 'product_id', 'user_id', 'user_session', col('price').alias('captured_price')
	).filter(col('user_id').isNotNull() & col('product_id').isNotNull())

	users_df.write.mode('overwrite').format('parquet').save(f'{silver_path}/users/')
	products_df.write.mode('overwrite').format('parquet').save(f'{silver_path}/products/')
	categories_df.write.mode('overwrite').format('parquet').save(f'{silver_path}/categories/')
	events_df.write.mode('overwrite').format('parquet').save(f'{silver_path}/events/')
	
	print(f'Users table: {users_df.count():,} rows')
	print(f'Products table: {products_df.count():,} rows')
	print(f'Categories table: {categories_df.count():,} rows')
	print(f'Events table: {events_df.count():,} rows')
	spark.stop()

if __name__ == '__main__':
	normalize_to_silver()
