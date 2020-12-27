from pyspark.sql import SparkSession
from pyspark.sql.functions import isnull, when, count, col

dataset_names = ['cargo', 'contact', 'container', 'header']
row_fullness_checks = {
	'cargo': ['identifier', 'container_number'],
	'contact': ['identifier', 'contact_type'],
	'container': ['identifier', 'container_number'],
	'header': ['identifier']
}

def create_spark_session():
	spark = SparkSession \
		.builder \
		.getOrCreate()

	return spark

def check_for_empty_data(df, name):
	if contact.count() > 0:
		return []
	else:
		return [f"The {name} dataframe is empty."]

def check_row_fullness(df, name, rows):
	row_selects = list(map(
		lambda x: count(when(isnull(col(x)), col(x))).alias(x)
	))

	collected_rows = df.select(row_selects).collect()[0]

	messages = []
	for idx, c in enumerate(collected_rows):
		if c != 0:
			messages.append(f"The {rows[idx]} row has {c} empty values.")

	return messages

def get_dataframe(spark, name):
	return spark.read \
		.option("header", True) \
		.option("escape", '"') \
		.option("inferSchema", True) \
		.csv(f"/output/{name}/*.csv") \

def check_data(spark):
	messages = []

	datasets = list(map(
		lambda x: (x, get_dataframe(spark, x)),
		dataset_names
	))

	for (name, dataset) in datasets:
		data_messages = []
		data_messages += check_for_empty_data(dataset, name)

		if len(data_messages) > 0:
			messages += data_messages
		else:
			messages += check_row_fullness(dataset, name, row_fullness_checks[name])

	if len(messages) > 0:
		raise Exception(' '.join(messages))

def main():
	spark = create_spark_session()
	check_data(spark)

if __name__ == "__main__":
	main()
