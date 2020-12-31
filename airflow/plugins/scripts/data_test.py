from pyspark.sql import SparkSession
from pyspark.sql.functions import isnull, when, count, col

dataset_names = ['cargo', 'contact', 'container', 'header']
col_fullness_checks = {
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
	"""
	Check whether a dataframe is empty
	
	Parameters
	----------
	df : Spark dataframe
		The dataframe to check
	name : str
		The dataframe name

	Returns
	-------
	list (str)
		Error messages
	"""
	if df.count() > 0:
		return []
	else:
		return [f"The {name} dataframe is empty."]

def check_col_fullness(df, name, cols):
	"""
	Check whether certain dataframe columns have any empty row
	
	Parameters
	----------
	df : Spark dataframe
		The dataframe to check
	name : str
		The dataframe name
	cols : list (str)
		The columns to check for

	Returns
	-------
	list (str)
		Error messages
	"""
	col_selects = list(map(
		lambda x: count(when(isnull(col(x)), col(x))).alias(x),
		cols
	))

	collected_cols = df.select(col_selects).collect()[0]

	messages = []
	for idx, c in enumerate(collected_cols):
		if c != 0:
			messages.append(f"The {cols[idx]} column from {name} has {c} empty values.")

	return messages

def get_dataframe(spark, name):
	"""
	Get a dataframe in the output folder
	
	Parameters
	----------
	spark : Spark session
		Current Spark session
	name : str
		The data name

	Returns
	-------
	Spark dataframe 
		The Spark dataframe
	"""
	return spark.read \
		.option("header", True) \
		.option("escape", '"') \
		.option("inferSchema", True) \
		.csv(f"/output/{name}/*.csv")

def check_data(spark):
	"""
	Check data integrety of several datasets in the output folder. 
	Raise an Exception if there is any error.
	
	Parameters
	----------
	spark : Spark session
		Current Spark session
	"""
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
			messages += check_col_fullness(dataset, name, col_fullness_checks[name])

	if len(messages) > 0:
		raise Exception(' '.join(messages))

def main():
	spark = create_spark_session()
	check_data(spark)

if __name__ == "__main__":
	main()
