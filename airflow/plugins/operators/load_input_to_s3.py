from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from helpers import EntitledAssets

class LoadInputToS3Operator(BaseOperator):
	ui_color = '#80BD9E'

	@apply_defaults
	def __init__(
		self,
		dataset_id,
		bucket_name, 
		region_name,
		*args, 
		**kwargs
	):
		super(LoadInputToS3Operator, self).__init__(*args, **kwargs)
		self.dataset_id = dataset_id
		self.bucket_name = bucket_name
		self.region_name = region_name

	def execute(self, context):
		ds_client = AwsHook().get_client_type('dataexchange', region_name=self.region_name)
		api = EntitledAssets(data_exchange_client=ds_client)
		dataset_id = self.dataset_id

		dataset_revision_res = api.list_dataset_revisions(dataset_id)
		latest_revision_id = dataset_revision_res['Revisions'][0]['Arn'].split('/')[-1]
		revision_assets = api.get_all_dataset_asset_infos(dataset_id, latest_revision_id)

		api.export_assets(revision_assets, self.bucket_name)