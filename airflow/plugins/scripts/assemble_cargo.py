import argparse

from pyspark.sql import SparkSession

dataset_names = ['cargodesc', 'hazmat', 'hazmatclass']

def create_spark_session():
	spark = SparkSession \
		.builder \
		.getOrCreate()

	return spark

def create_temp_view(spark, name, input_dir):
	df = spark.read \
		.option("header", True) \
		.option("escape", '"') \
		.option("inferSchema", True) \
		.csv(f"{input_dir}/ams/*/*/ams__{name}_*__*.csv")

	df.createOrReplaceTempView(name)

def process_cargo_data(spark, input_dir, output):
	for name in dataset_names:
		create_temp_view(spark, name, input_dir)

	cargo_table = spark.sql("""
		SELECT 
			c.identifier,
			c.container_number,
			c.description_sequence_number AS sequence_number,
			c.piece_count,
			c.description_text AS description,
			h.hazmat_code,
			(CASE 
				WHEN (hc.hazmat_classification IS NOT NULL) THEN hc.hazmat_classification
				ELSE h.hazmat_class
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
		WHERE c.identifier IS NOT NULL AND c.container_number IS NOT NULL
	""")

	cargo_table.repartition(1).write.mode('overwrite').format("csv") \
		.option("header", True) \
		.option("escape", '"') \
		.save(f"{output}/cargo/")

def main(input_dir, output):
	spark = create_spark_session()
	process_cargo_data(spark, input_dir, output)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('-i', '--input', default='/input')
	parser.add_argument('-o', '--output', default='/output')
	args = parser.parse_args()

	main(args.input, args.output)
