# from airflow.hooks.postgres_hook import PostgresHook
# from airflow.models import BaseOperator
# from airflow.utils.decorators import apply_defaults
# from airflow.contrib.hooks.aws_hook import AwsHook

# class LoadRawToS3Operator(BaseOperator):
# 	ui_color = '#80BD9E'

#     @apply_defaults
#     def __init__(
#     	self,
# 		bucket_name,
# 		sql_query=[],
# 		*args, 
# 		**kwargs
# 	):

#         super(LoadRawToS3Operator, self).__init__(*args, **kwargs)
#         self.redshift_conn_id = redshift_conn_id
#         self.sql_query = sql_query

#     def execute(self, context):
#         redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
#         for query in self.sql_query:
#             self.log.info("Running Analytics query :  {}".format(query))
#             redshift_hook.run(self.sql_query)
#             self.log.info("Query ran successfully!!")