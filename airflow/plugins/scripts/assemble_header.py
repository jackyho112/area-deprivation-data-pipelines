import argparse

from pyspark.sql import SparkSession

header_cols = [
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

bill_cols = [
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

def process_header_data(spark, input_dir, output):
	header = spark.read \
		.option("header", True) \
		.option("escape", '"') \
        .option("inferSchema", True) \
		.csv(f"{input_dir}/ams/*/*/ams__header_*__*.csv") \
		.select(*header_cols)

	bill = spark.read \
		.option("header", True) \
		.option("escape", '"') \
        .option("inferSchema", True) \
		.csv(f"{input_dir}/ams/*/*/ams__billgen_*__*.csv") \
		.select(*bill_cols)

	header_full = header.join(bill, ['identifier'], how='left')

	header_full.repartition(1).write.mode('overwrite').format("csv") \
		.option("header", True) \
		.option("escape", '"') \
		.save(f"{output}/header/")

def main(input_dir, output):
	spark = create_spark_session()
	process_header_data(spark, input_dir, output)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', default='/input')
    parser.add_argument('-o', '--output', default='/ouput')
    args = parser.parse_args()

    main(args.input, args.ouput)

