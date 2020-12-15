import argparse

from pyspark.sql import SparkSession
from pyspark.sql.types import (
	StructType, StructField, StringType
)

cargo_desc_schema = StructType([
	StructField("identifier", StringType()),
	StructField("container_number", StringType()),
	StructField("description_sequence_number", StringType()),
	StructField("piece_count", StringType()),
	StructField("description_text", StringType())
])

hazmat_schema = StructType([
	StructField("identifier", StringType()),
	StructField("container_number", StringType()),
	StructField("hazmat_sequence_number", StringType()),
	StructField("hazmat_code", StringType()),
	StructField("hazmat_class", StringType()),
	StructField("hazmat_code_qualifier", StringType()),
	StructField("hazmat_contact", StringType()),
	StructField("hazmat_page_number", StringType()),
	StructField("hazmat_flash_point_temperature", StringType()),
	StructField("hazmat_flash_point_temperature_negative_ind", StringType()),
	StructField("hazmat_flash_point_temperature_unit", StringType()),
	StructField("hazmat_description", StringType())
])

hazmat_class_schema = StructType([
	StructField("identifier", StringType()),
	StructField("container_number", StringType()),
	StructField("hazmat_sequence_number", StringType()),
	StructField("hazmat_classification", StringType())
])

schema_map = {
	'cargodesc': cargo_desc_schema,
	'hazmat': hazmat_schema,
	'hazmatclass': hazmat_class_schema
}

def create_spark_session():
	spark = SparkSession \
		.builder \
		.getOrCreate()

	return spark

def create_temp_view(spark, name, schema, local_run=False):
	df = spark.read \
		.option("header", True) \
		.option("escape", '"') \
		.csv(f"{'.' if local_run else ''}/input/ams/*/*/ams__{name}_*__*.csv")
		.select(*schema.fieldNames())

	df.createOrReplaceTempView(name)

def process_cargo_data(spark, local_run=False):
	for name, schema in schema_map.items():
		create_temp_view(spark, name, schema, local_run)

	cargo_table = spark.sql("""
		SELECT 
			c.identifier,
			c.container_number,
			c.description_sequence_number AS sequence_number,
			c.piece_count,
			c.description_text AS description,
			h.hazmat_code,
			(CASE 
				WHEN (h.hazmat_class IS NOT NULL) THEN h.hazmat_class
				ELSE hc.hazmat_classification
			END) AS hazmat_class,
			h.hazmat_code_qualifier,
			h.hazmat_contact,
			h.hazmat_page_number,
			h.hazmat_flash_point_temperature,
			h.hazmat_flash_point_temperature_negative_ind,
			h.hazmat_flash_point_temperature_unit,
			h.hazmat_description
		FROM cargodesc AS c
		LEFT JOIN hazmat AS h
		ON 
			c.identifier = h.identifier AND 
			c.container_number = h.container_number AND 
			c.description_sequence_number = h.hazmat_sequence_number
		LEFT JOIN hazmatclass AS hc
		ON
			c.identifier = hc.identifier AND 
			c.container_number = hc.container_number AND 
			c.description_sequence_number = hc.hazmat_sequence_number
	""")

	cargo_table.repartition(1).write.mode('overwrite').format("csv") \
		.option("header", True) \
		.option("escape", '"') \
		.save(f"{'.' if local_run else ''}/output/cargo/")

def main(local_run):
	spark = create_spark_session()
	process_cargo_data(spark, local_run)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('-l', '--local', action='store_true')
	args = parser.parse_args()

	main(args.local)
