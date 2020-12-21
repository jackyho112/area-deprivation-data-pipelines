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

def get_contact_dataframe(spark, name, input_dir):
	df = spark.read \
		.option("header", True) \
		.option("escape", '"') \
		.option("enforceSchema", True) \
		.option("schema", contact_schema) \
		.csv(f"{input_dir}/ams/*/*/ams__{name}_*__*.csv")

	return df.withColumn('contact_type', lit(name))

def process_contact_data(spark, input_dir, ouput):
	contact_dfs = list(map(
		lambda name: get_contact_dataframe(spark, name, input_dir), 
		contacts
	))
	contact_df = functools.reduce(lambda a,b: a.union(b), contact_dfs)

	contact_df.repartition(1).write.mode('overwrite').format("csv") \
		.option("header", True) \
		.option("escape", '"') \
		.save(f"{ouput}/contact/")

def main(input_dir, output):
	spark = create_spark_session()
	process_contact_data(spark, input_dir, output)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('-i', '--input', default='/input')
	parser.add_argument('-o', '--output', default='/output')
	args = parser.parse_args()

	main(args.input, args.output)
