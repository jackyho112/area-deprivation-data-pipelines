import time

# Reference: https://github.com/aws-samples/aws-dataexchange-api-samples#python
class EntitledAssets():
	def __init__(self, data_exchange_client):
	    """
	    Parameters
	    ----------
	    data_exchange_client : AWS Data Exchange client
	        An AWS Data Exchange client from the AWS library or Airflow AWS hook
	    """
		self.data_exchange_client = data_exchange_client

	def get_dataset_info(self, dataset_id):
	    """
	    Get the Data Exchange dataset info
	    
	    Parameters
	    ----------
	    dataset_id : str
	        The dataset id

	    Returns
	    -------
	    dict
	        The dataset info
	    """
		return self.data_exchange_client.get_data_set(DataSetId=dataset_id)

	def list_dataset_revisions(self, dataset_id):
	    """
	    Get the Data Exchange dataset revisions
	    
	    Parameters
	    ----------
	    dataset_id : str
	        The dataset id

	    Returns
	    -------
	    dict
	        The dataset revisions
	    """
		return self.data_exchange_client.list_data_set_revisions(DataSetId=dataset_id)

	def get_all_dataset_asset_infos(self, dataset_id, revision_id):
	    """
	    Get the Data Exchange dataset revision assets' info
	    
	    Parameters
	    ----------
	    dataset_id : str
	        The dataset id
	    revision_id : str
	        The revision id

	    Returns
	    -------
	    list (dict)
	        Dataset revision assets
	    """
		assets = []

		client = self.data_exchange_client
		res = client.list_revision_assets(DataSetId=dataset_id, RevisionId=revision_id)
		next_token = res.get('NextToken')
		
		assets += res.get('Assets')
		while next_token:
			res = client.list_revision_assets(
				DataSetId=dataset_id,
				RevisionId=revision_id,
				NextToken=next_token
			)
			assets += res.get('Assets')
			next_token = res.get('NextToken')
			
		return assets

	def export_assets(self, assets, bucket, folder='input/'):
	    """
	    Export assets to a bucket
	    
	    Parameters
	    ----------
	    assets : list (dict)
	        Info regarding the assets
	    bucket : str
	        The bucket name
	    folder : str
	    	The bucket key to put assets in

	    Returns
	    -------
	    Bool
	    """
		asset_destinations = []
		client = self.data_exchange_client

		for asset in assets:
			asset_destinations.append({
				"AssetId": asset.get('Id'),
				"Bucket": bucket,
				"Key": folder + asset.get('Name')
			})

		job = client.create_job(Type='EXPORT_ASSETS_TO_S3', Details={
			"ExportAssetsToS3": {
				"RevisionId": asset.get("RevisionId"), 
				"DataSetId": asset.get("DataSetId"),
				"AssetDestinations": asset_destinations
			}
		})

		job_id = job.get('Id')
		client.start_job(JobId=job_id)

		while True:
			job = client.get_job(JobId=job_id)

			if job.get('State') == 'COMPLETED':
				break
			elif job.get('State') == 'ERROR':
				raise Exception("Job {job_id} failed to complete - {message}".format(
					job_id=job_id, 
					message=job.get('Errors')[0].get('Message'))
				)

			time.sleep(30)

		return True
