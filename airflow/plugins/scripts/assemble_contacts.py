import argparse
import functools

from pyspark.sql import SparkSession
from pyspark.sql.types import (
	StructType, StringType, StructField
)
from pyspark.sql.functions import lit

contact_schema = StructType([
	StructField("identifier", StringType()),
	StructField("name", StringType()),
	StructField("address_1", StringType()),
	StructField("address_2", StringType()),
	StructField("address_3", StringType()),
	StructField("address_4", StringType()),
	StructField("city", StringType()),
	StructField("state_province", StringType()),
	StructField("zip_code", StringType()),
	StructField("country_code", StringType()),
	StructField("contact_name", StringType()),
	StructField("comm_number_qualifier", StringType()),
	StructField("comm_number", StringType())
])
contacts = ['consignee', 'notifyparty', 'shipper']

def create_spark_session():
	spark = SparkSession \
		.builder \
		.getOrCreate()

	return spark

def get_contact_dataframe(spark, name, local_run=False):
	df = spark.read \
		.option("header", True) \
		.option("escape", '"') \
		.option("enforceSchema", True) \
		.option("schema", contact_schema) \
		.csv(f"{'.' if local_run else ''}/input/ams/*/*/ams__{name}_*__*.csv")

	return df.withColumn('contact_type', lit(name))

def process_contact_data(spark, local_run=False):
	contact_dfs = list(map(
		lambda name: get_contact_dataframe(spark, name, local_run), 
		contacts
	))
	contact_df = functools.reduce(lambda a,b: a.union(b), contact_dfs)

	contact_df.repartition(1).write.mode('overwrite').format("csv") \
		.option("header", True) \
		.option("escape", '"') \
		.save(f"{'.' if local_run else ''}/output/contact/")

def main(local_run):
	spark = create_spark_session()
	process_contact_data(spark, local_run)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('-l', '--local', action='store_true')
	args = parser.parse_args()

	main(args.local)
