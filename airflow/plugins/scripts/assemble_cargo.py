import argparse

from pyspark.sql import SparkSession
from pyspark.sql.types import (
	StructType, StructField, StringType
)

cargo_desc_cols = [
	"identifier",
	"container_number",
	"description_sequence_number",
	"piece_count",
	"description_text"
]

hazmat_cols = [
	"identifier",
	"container_number",
	"hazmat_sequence_number",
	"hazmat_code",
	"hazmat_class",
	"hazmat_code_qualifier",
	"hazmat_contact",
	"hazmat_page_number",
	"hazmat_flash_point_temperature",
	"hazmat_flash_point_temperature_negative_ind",
	"hazmat_flash_point_temperature_unit",
	"hazmat_description",
]

hazmat_class_cols = [
	"identifier",
	"container_number",
	"hazmat_sequence_number",
	"hazmat_classification",
]

schema_map = {
	'cargodesc': cargo_desc_cols,
	'hazmat': hazmat_cols,
	'hazmatclass': hazmat_class_cols
}

def create_spark_session():
	spark = SparkSession \
		.builder \
		.getOrCreate()

	return spark

def create_temp_view(spark, name, columns, local_run=False):
	df = spark.read \
		.option("header", True) \
		.option("escape", '"') \
		.option("inferSchema", True) \
		.csv(f"{'.' if local_run else ''}/input/ams/*/*/ams__{name}_*__*.csv") \
		.select(*columns)

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
