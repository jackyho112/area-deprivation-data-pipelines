import argparse
import os
import functools

from pyspark.sql import SparkSession
from pyspark.sql.types import (
	TimestampType, StructType, StringType, IntegerType, FloatType, StructField
)
from pyspark.sql.functions import lit

contact_schema = StructType([
    StructField("identifier", StringType()),
    StructField("name", StringType()),
    StructField("address_1", StringType()),
    StructField("address_2", StringType()),
    StructField("address_3", StringType()),
    StructField("address_4", StringType()),
    StructField("city", StringType()),
    StructField("state_province", StringType()),
    StructField("zip_code", StringType()),
    StructField("country_code", StringType()),
    StructField("contact_name", StringType()),
    StructField("comm_number_qualifier", StringType()),
    StructField("comm_number", StringType())
])

contacts = ['consignee', 'notifyparty', 'shipper']

def create_spark_session():
    spark = SparkSession \
        .builder \
        .getOrCreate()

    return spark

def get_file_path(year, revision, name):
	return f"../data/ams/{year}/{revision}/ams__{name}_{year}__{revision}.csv"

def get_contact_dataframe(spark, year, revision, name):
	df = spark.read \
		.option("header", True) \
	    .option("escape", '"') \
	    .csv(
	    	get_file_path(year, revision, name),
	        schema=contact_schema,
	        enforceSchema=True
	    )

	return df.withColumn('contact_type', lit(name))

def process_contact_data(spark, year, revision):
	contact_dfs = list(map(
		lambda name: get_contact_dataframe(spark, year, revision, name), 
		contacts
	))
	contact_df = functools.reduce(lambda a,b: a.union(b), contact_dfs)

	contact_df.repartition(1).write.mode('overwrite').format("csv") \
	    .option("header", True) \
	    .option("escape", '"') \
	    .save("../data/contact")

def main(year, revision):
    spark = create_spark_session()

    process_contact_data(spark, year, revision)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="""
    	This script is going to assemble contacts out of export data for a certain year
    """)

    parser.add_argument("-y", "--year", help="year of exports")
    args = parser.parse_args()
    year = args.year or '2020'

    available_revisions = [f.name for f in os.scandir('../data/ams/' + year) if f.is_dir()]
    available_revisions.sort(reverse=True)

    main(year, available_revisions[0])

