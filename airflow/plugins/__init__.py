from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators

class USImportPlugin(AirflowPlugin):
    name = "us_import_plugin"

    operators = [
		operators.LoadInputToS3Operator,
		operators.LoadScriptsToS3Operator
    ]