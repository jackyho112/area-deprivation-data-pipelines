import argparse

from pyspark.sql import SparkSession
from pyspark.sql.types import (
	StructType, StructField, StringType, IntegerType, DateType
)

header_schema = StructType([
    StructField("identifier", StringType()),
    StructField("carrier_code", StringType()),
    StructField("vessel_country_code", StringType()),
    StructField("vessel_name", StringType()),
    StructField("port_of_unlading", StringType()),
    StructField("estimated_arrival_date", DateType()),
    StructField("foreign_port_of_lading_qualifier", StringType()),
    StructField("foreign_port_of_lading", StringType()),
    StructField("manifest_quantity", IntegerType()),
    StructField("manifest_unit", StringType()),
    StructField("weight", IntegerType()),
    StructField("weight_unit", StringType()),
    StructField("record_status_indicator", StringType()),
    StructField("place_of_receipt", StringType()),
    StructField("port_of_destination", StringType()),
    StructField("foreign_port_of_destination_qualifier", StringType()),
    StructField("foreign_port_of_destination", StringType()),
    StructField("conveyance_id_qualifier", StringType()),
    StructField("conveyance_id", StringType()),
    StructField("mode_of_transportation", StringType()),
    StructField("actual_arrival_date", DateType())
])

bill_schema = StructType([
    StructField("identifier", StringType()),
    StructField("master_bol_number", StringType()),
    StructField("house_bol_number", StringType()),
    StructField("sub_house_bol_number", StringType()),
    StructField("voyage_number", StringType()),
    StructField("bill_type_code", StringType()),
    StructField("manifest_number", IntegerType()),
    StructField("trade_update_date", DateType()),
    StructField("run_date", DateType())
])

def create_spark_session():
	spark = SparkSession \
		.builder \
		.getOrCreate()

	return spark

def process_header_data(spark, local_run=False):
	header = spark.read \
		.option("header", True) \
		.option("escape", '"') \
		.csv(
			f"{'.' if local_run else ''}/input/ams/*/*/ams__header_*__*.csv",
			schema=header_schema,
			enforceSchema=True
		)

	bill = spark.read \
		.option("header", True) \
		.option("escape", '"') \
		.csv(
			f"{'.' if local_run else ''}/input/ams/*/*/ams__billgen_*__*.csv",
			schema=bill_schema,
			enforceSchema=True
		)

	header_full = header.join(bill, ['identifier'], how='left')

	contact_df.repartition(1).write.mode('overwrite').format("csv") \
		.option("header", True) \
		.option("escape", '"') \
		.save(f"{'.' if local_run else ''}/output/header/")

def main(local_run):
	spark = create_spark_session()
	process_header_data(spark, local_run)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('-l', '--local', action='store_true')
	args = parser.parse_args()

	main(args.local)
