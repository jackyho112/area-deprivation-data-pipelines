from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.S3_hook import S3Hook
from helpers import EntitledAssets

class LoadScriptsToS3Operator(BaseOperator):
	ui_color = '#80BD9E'

	@apply_defaults
	def __init__(
		self,
		bucket_name,
		script_paths,
		*args, 
		**kwargs
	):
		super(LoadRawToS3Operator, self).__init__(*args, **kwargs)
		self.bucket_name = bucket_name
		self.script_paths = script_paths
		self.folder_key = folder_key

	def execute(self, context):
		s3 = S3Hook()

		for path in self.script_paths:
			s3.load_file(
				filename='../scripts/' + path,
				bucket_name=self.bucket_name,
				replace=True,
				key='scripts/' + path.split('/')[-1]
			)