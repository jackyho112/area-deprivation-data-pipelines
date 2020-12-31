from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook

class ClearS3OutputOperator(BaseOperator):
	ui_color = '#80BD9E'

	@apply_defaults
	def __init__(
		self,
		bucket_name,
		*args, 
		**kwargs
	):
		"""
		Parameters
		----------
		bucket_name : str
			The bucket name to delete the output folder in
		"""
		super(ClearS3OutputOperator, self).__init__(*args, **kwargs)
		self.bucket_name = bucket_name

	def execute(self, context):
		"""
		Delete the output folder in a bucket

		Parameters
		----------
		context : dict
			The Airflow context
		"""
		s3 = S3Hook()

		bucket = s3.get_bucket(self.bucket_name)
		bucket.objects.filter(Prefix="output/").delete()