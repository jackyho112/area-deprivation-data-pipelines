from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
from airflow.exceptions import AirflowException

class CheckForBucketOperator(BaseOperator):
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
	        The bucket name to check for availability
	    """
		super(CheckForBucketOperator, self).__init__(*args, **kwargs)
		self.bucket_name = bucket_name

	def execute(self):
		"""
		Check whether the S3 bucket is available
		"""
		s3 = S3Hook()
		bucket_exists = s3.check_for_bucket(self.bucket_name)

		if not bucket_exists:
			raise AirflowException(f"The bucket {bucket_name} does not exist!")

		return True