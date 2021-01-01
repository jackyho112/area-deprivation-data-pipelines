# US import data pipeline

The source is [Amazon Data Exchange](https://aws.amazon.com/marketplace/pp/US-Imports-Automated-Manifest-System-AMS-Shipments/prodview-stk4wn3mbhx24). It contains data for US imports from 2018 to 2020. I assume it will also contain 2021 data when it becomes available. 

## Input data

Each year's data contains 11 datasets for header, bill, cargo description, hazmat, hazmat class, tariff, cosignee, notified party, shipper, container, and mark number.

For information on these datasets and their columns, refer to this [notebook](https://github.com/jackyho112/us-import-data-pipelines/blob/main/notebooks/exploration.ipynb). You can also refer to the Amazon Data Exchange [overview page](https://aws.amazon.com/marketplace/pp/US-Imports-Automated-Manifest-System-AMS-Shipments/prodview-stk4wn3mbhx24#offers).

## Output data

This data pipeline outputs four datasets in CSV files aggregating each year of import data. Here are the table schemas (from Spark) and how they are assembled.

Fact Table: 

Bill table -

From joining the header and bill dataset

![bill table schema](imgs/bill-table-schema.png)

## Running Airflow

In the Airflow folder,

to start the scheduler:
```bash
airflow scheduler
```

to start the web sever:
```bash
airflow webserver -p 8080
```

For more information on Airflow, go to its [site](http://airflow.apache.org/docs/stable/)
