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
		super(ClearS3OutputOperator, self).__init__(*args, **kwargs)
		self.bucket_name = bucket_name

	def execute(self, context):
		s3 = S3Hook()

		bucket = s3.get_bucket(self.bucket_name)
		bucket.objects.filter(Prefix="output/").delete()