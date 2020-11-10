from airflow.plugins_manager import AirflowPlugin

import operators

class USImportPlugin(AirflowPlugin):
    name = "us_import_plugin"
    operators = [
    ]