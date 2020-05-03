from airflow.plugins_manager import AirflowPlugin
import operators
import helpers


# Defining the plugin class
class SparkifyPlugin(AirflowPlugin):
    name = "sparkify_plugin"
    operators = [
        operators.StageToRedshiftOperator,
        operators.LoadFactOperator,
        operators.LoadDimensionOperator,
        operators.DataQualityOperator
    ]
    helpers = [
        helpers.SqlQueries,
        helpers.DataChecks,
    ]

