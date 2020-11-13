from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
import os

class LoadScriptsToS3Operator(BaseOperator):
	ui_color = '#80BD9E'

	@apply_defaults
	def __init__(
		self,
		bucket_name,
		folder_key='scripts/',
		*args, 
		**kwargs
	):
		super(LoadScriptsToS3Operator, self).__init__(*args, **kwargs)
		self.bucket_name = bucket_name
		self.folder_key = folder_key

	def execute(self, context):
		s3 = S3Hook()

		os.chdir(os.path.dirname(os.path.abspath(__file__)))
		available_scripts = [f.path for f in os.scandir('../scripts') if '.py' in f.name]

		for script in available_scripts:
			s3.load_file(
				filename=script,
				bucket_name=self.bucket_name,
				replace=True,
				key=self.folder_key + script.split('/')[-1]
			)