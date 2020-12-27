import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

container_cols = [
	"identifier",
	"container_number",
	"equipment_description_code",
	"container_length",
	"container_height",
	"container_width",
	"container_type",
	"load_status",
	"type_of_service",
]

def create_spark_session():
	spark = SparkSession \
		.builder \
		.getOrCreate()

	return spark

def process_container_data(spark, input_dir, output):
	container = spark.read \
		.option("header", True) \
		.option("escape", '"') \
        .option("inferSchema", True) \
		.csv(f"{input_dir}/ams/*/*/ams__container_*__*.csv") \
		.select(*container_cols) \
		.where(col('identifier').isNotNull() & col('container_number').isNotNull())

	mark = spark.read \
		.option("header", True) \
		.option("escape", '"') \
        .option("inferSchema", True) \
		.csv(f"{input_dir}/ams/*/*/ams__marksnumbers_*__*.csv") \

	container_full = container.join(mark, ['identifier', 'container_number'], how='left')

	container_full.repartition(1).write.mode('overwrite').format("csv") \
		.option("header", True) \
		.option("escape", '"') \
		.save(f"{output}/container/")

def main(input_dir, output):
	spark = create_spark_session()
	process_container_data(spark, input_dir, output)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('-i', '--input', default='/input')
	parser.add_argument('-o', '--output', default='/output')
	args = parser.parse_args()

	main(args.input, args.output)
