import argparse

from pyspark.sql import SparkSession

kept_header_cols = [
    "identifier",
    "carrier_code",
    "vessel_country_code",
    "vessel_name",
    "port_of_unlading",
    "estimated_arrival_date",
    "foreign_port_of_lading_qualifier",
    "foreign_port_of_lading",
    "manifest_quantity",
    "manifest_unit",
    "weight",
    "weight_unit",
    "record_status_indicator",
    "place_of_receipt",
    "port_of_destination",
    "foreign_port_of_destination_qualifier",
    "foreign_port_of_destination",
    "conveyance_id_qualifier",
    "conveyance_id",
    "mode_of_transportation",
    "actual_arrival_date",
]

kept_bill_cols = [
    "identifier",
    "master_bol_number",
    "house_bol_number",
    "sub_house_bol_number",
    "voyage_number",
    "bill_type_code",
    "manifest_number",
    "trade_update_date",
    "run_date"
]

def create_spark_session():
	spark = SparkSession \
		.builder \
		.getOrCreate()

	return spark

def process_header_data(spark, local_run=False):
	header = spark.read \
		.option("header", True) \
		.option("escape", '"') \
        .option("inferSchema", True) \
		.csv(f"{'.' if local_run else ''}/input/ams/*/*/ams__header_*__*.csv") \
		.select(*kept_header_cols)

	bill = spark.read \
		.option("header", True) \
		.option("escape", '"') \
        .option("inferSchema", True) \
		.csv(f"{'.' if local_run else ''}/input/ams/*/*/ams__billgen_*__*.csv") \
		.select(*kept_bill_cols)

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
