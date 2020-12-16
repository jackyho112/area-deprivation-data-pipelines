import argparse

from pyspark.sql import SparkSession

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

def process_container_data(spark, local_run):
	container = spark.read \
		.option("header", True) \
		.option("escape", '"') \
        .option("inferSchema", True) \
		.csv(f"{'.' if local_run else ''}/input/ams/*/*/ams__container_*__*.csv") \
		.select(*container_cols)

	mark = spark.read \
		.option("header", True) \
		.option("escape", '"') \
        .option("inferSchema", True) \
		.csv(f"{'.' if local_run else ''}/input/ams/*/*/ams__marksnumbers_*__*.csv") \

	container_full = container.join(mark, ['identifier', 'container_number'], how='left')

	container_full.repartition(1).write.mode('overwrite').format("csv") \
		.option("header", True) \
		.option("escape", '"') \
		.save(f"{'.' if local_run else ''}/output/container/")

def main(local_run):
	spark = create_spark_session()
	process_container_data(spark, local_run)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('-l', '--local', action='store_true')
	args = parser.parse_args()

	main(args.local)