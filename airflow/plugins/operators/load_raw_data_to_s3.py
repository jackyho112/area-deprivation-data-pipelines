from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import Variable
from helpers import EntitledAssets

class LoadRawToS3Operator(BaseOperator):
	ui_color = '#80BD9E'

	@apply_defaults
	def __init__(self, *args, **kwargs):
		super(LoadRawToS3Operator, self).__init__(*args, **kwargs)

	def execute(self, context):
		ds_client = AwsHook().get_client_type('dataexchange')
		api = EntitledAssets(data_exchange_client=ds_client)
		dataset_id = Variable.get('data_exchange_dataset_id')

		dataset_revision_res = api.list_dataset_revisions(dataset_id)
		latest_revision_id = dataset_revision_res['Revisions'][0]['Arn'].split('/')[-1]
		revision_assets = api.get_all_dataset_asset_infos(dataset_id, latest_revision_id)

		api.export_assets(revision_assets, Variable.get('s3_raw_data_bucket'))